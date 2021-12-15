mod aws;

use argh::FromArgs;
use async_compression::{
	tokio::{bufread::ZstdDecoder, write::ZstdEncoder}, Level
};
use futures::{
	future::{try_join_all, Fuse}, FutureExt, StreamExt
};
use pin_project::pin_project;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::StaticProvider;
use rusoto_s3::S3Client;
use serde::Deserialize;
use std::{
	collections::BTreeMap, env, error::Error, fs::{self, read_to_string}, future::Future, io, iter, path::PathBuf, pin::Pin, sync::Arc, task::{Context, Poll}, time::SystemTime
};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};
use walkdir::WalkDir;

const WEB_SAFE: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_~";

#[derive(Deserialize, Debug)]
struct AwsConfig {
	access_key_id: String,
	secret_access_key: String,
	#[serde(with = "serde_with::rust::display_fromstr")]
	region: Region,
}

#[derive(Deserialize, Debug)]
struct Config {
	bucket: String,
}

#[derive(FromArgs, Debug)]
/// command
struct Cmd {
	#[argh(subcommand)]
	cmd: Cmd_,
}

#[derive(FromArgs, Debug)]
#[argh(subcommand)]
enum Cmd_ {
	Save(CmdSave),
	Uncache(CmdLoad),
}

#[derive(FromArgs, Debug)]
/// save cache to s3
#[argh(subcommand, name = "save")]
struct CmdSave {
	#[argh(positional)]
	/// keys
	keys: Vec<String>,
}

#[derive(FromArgs, Debug)]
/// load cache from s3
#[argh(subcommand, name = "load")]
struct CmdLoad {
	#[argh(positional)]
	/// keys. will try from left to right
	keys: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
	env::set_var("RUST_BACKTRACE", "1");
	dotenv::dotenv().ok();

	let cmd: Cmd = argh::from_env();

	let aws_config: AwsConfig = envy::prefixed("AWS_").from_env().unwrap();
	let config: Config = envy::prefixed("KACHE_").from_env().unwrap();

	let http_client = Arc::new(HttpClient::new().expect("failed to create request dispatcher"));
	let creds = StaticProvider::new(aws_config.access_key_id, aws_config.secret_access_key, None, None);
	let s3_client = &S3Client::new_with(http_client.clone(), creds.clone(), aws_config.region);

	let bucket = &config.bucket;

	let target_dir = PathBuf::from("./target"); // TODO: actually deduce
	let cargo_home = home::cargo_home().unwrap();

	match cmd.cmd {
		Cmd_::Save(CmdSave { keys }) => {
			println!("saving cache to: {keys:?}");
			let id: [u8; 16] = rand::random();
			let id = base_x::encode(WEB_SAFE, &id);
			let info_path = PathBuf::from(".kache-info");
			let (id, cutoff) = match info_path.metadata() {
				Ok(info) => {
					let parent_id = read_to_string(info_path).unwrap();
					let parent_id = parent_id.trim_end_matches('\n');
					(format!("{parent_id}:{id}"), Some(info.modified().unwrap()))
				}
				Err(_) => (id, None),
			};
			println!("packing {id}");
			let tar = tar(iter::empty()
				.chain(walk(PathBuf::from("cargo").join("bin"), cargo_home.join("bin"), cutoff))
				.chain(walk(PathBuf::from("cargo").join("registry/index"), cargo_home.join("registry/index"), cutoff))
				.chain(walk(PathBuf::from("cargo").join("registry/cache"), cargo_home.join("registry/cache"), cutoff))
				.chain(walk(PathBuf::from("cargo").join("git/db"), cargo_home.join("git/db"), cutoff))
				.chain(walk(PathBuf::from("target"), target_dir, cutoff)));
			aws::upload(s3_client, bucket.clone(), format!("blobs/{}.tar.zst", id), String::from("application/zstd"), Some(31536000), tar).await?;
			let id = &id;
			let _: Vec<()> = try_join_all(keys.into_iter().map(|key| async move {
				let key = format!("keys/{}.txt", key);
				aws::upload(s3_client, bucket.clone(), key, String::from("text/plain"), Some(600), id.as_bytes()).await
			}))
			.await?;
			println!("finished saving cache");
		}
		Cmd_::Uncache(CmdLoad { keys }) => {
			println!("fetching cache from: {keys:?}");
			for key in keys {
				let key = format!("keys/{}.txt", key);
				if let Ok(mut id_) = aws::download(s3_client, bucket.clone(), key.clone()).await {
					// Assumption: if this id is listed in s3 under this cache key then the underlying blobs *must* still exist.
					let mut id = String::new();
					let _ = id_.read_to_string(&mut id).await?;
					println!("cache hit: {key} -> {id}");
					let ids = id.match_indices(':').map(|(index, _)| &id[..index]).chain(std::iter::once(id.as_ref()));
					for id in ids {
						let blob_id = format!("blobs/{}.tar.zst", id);
						println!("unpacking {blob_id}");
						let tar = aws::download(s3_client, bucket.clone(), blob_id).await?;
						untar(
							vec![
								(PathBuf::from("cargo").join("bin"), cargo_home.join("bin")),
								(PathBuf::from("cargo").join("registry/index"), cargo_home.join("registry/index")),
								(PathBuf::from("cargo").join("registry/cache"), cargo_home.join("registry/cache")),
								(PathBuf::from("cargo").join("git/db"), cargo_home.join("git/db")),
								(PathBuf::from("target"), target_dir.clone()),
							]
							.into_iter()
							.collect(),
							tar,
						)
						.await?;
					}
					fs::write(".kache-info", id).unwrap();
					println!("Finished unpacking cache");
					return Ok(());
				}
			}
			println!("cache miss :(");
		}
	}
	Ok(())
}

fn walk(slug: PathBuf, base: PathBuf, cutoff: Option<SystemTime>) -> impl Iterator<Item = (PathBuf, PathBuf, PathBuf)> {
	WalkDir::new(&base).sort_by(|a, b| a.file_name().cmp(b.file_name())).into_iter().filter_map(move |entry| {
		let entry = entry.unwrap();
		let t = entry.file_type();
		let path = entry.path().strip_prefix(&base).unwrap().to_owned();
		let non_empty = path.components().next().is_some();
		if non_empty
			&& (t.is_file() || t.is_symlink() || t.is_dir())
			&& cutoff.map(|cutoff| cutoff < entry.metadata().unwrap().modified().unwrap()).unwrap_or(true)
		{
			Some((slug.clone(), base.clone(), path))
		} else {
			None
		}
	})
}

fn tar(paths: impl Iterator<Item = (PathBuf, PathBuf, PathBuf)>) -> impl AsyncRead {
	let (writer, reader) = async_pipe::pipe();
	let task = async move {
		// create a .tar.zsd
		// FIXME: switch to a threaded implementation using std::io::Write, rather than async-compression,
		// and set the encoder to be multithreaded:
		// https://docs.rs/zstd/0.9.0+zstd.1.5.0/zstd/stream/write/struct.Encoder.html#method.multithread
		// This may allow us to get the compression speeds promised by these benchmarks:
		// https://community.centminmod.com/threads/round-4-compression-comparison-benchmarks-zstd-vs-brotli-vs-pigz-vs-bzip2-vs-xz-etc.18669/
		let tar_ = ZstdEncoder::with_quality(writer, Level::Fastest);
		let mut tar_ = tokio_tar::Builder::new(tar_);
		tar_.mode(tokio_tar::HeaderMode::Complete);

		// add resources (check for docker images) to tar
		for (slug, base, relative) in paths {
			tar_.append_path_with_name(base.join(&relative), slug.join(&relative)).await.unwrap();
		}

		// flush writers
		let mut tar_ = tar_.into_inner().await.unwrap();
		tar_.shutdown().await.unwrap();
		let _tar = tar_.into_inner();
	};

	AlsoPollFuture { reader, task: task.fuse() }
}

async fn untar(slugs: BTreeMap<PathBuf, PathBuf>, tar: impl AsyncBufRead) -> Result<(), io::Error> {
	tokio::pin!(tar);
	let tar = ZstdDecoder::new(tar);
	let tar = tokio::io::BufReader::with_capacity(16 * 1024 * 1024, tar);

	let mut entries = tokio_tar::Archive::new(tar).entries().unwrap();

	while let Some(entry) = entries.next().await {
		let mut entry = entry.unwrap();
		let path = entry.path().unwrap().into_owned();

		let path = apply_transform(path, &slugs);

		let parent = path.parent().unwrap();
		let _ = fs::create_dir_all(&parent);
		entry.unpack(&path).await.map(drop).unwrap_or_else(|err| match err.kind() {
			io::ErrorKind::PermissionDenied if path.exists() => {
				println!("permission denied writing to preexisting {path:?} - skipping")
			}
			_ => {
				let metadata = path.metadata();
				let parent_metadata = parent.metadata();
				panic!("Could not unpack {path:?}: {err:?}\nmetadata: {metadata:?}\nparent_metadata: {parent_metadata:?}")
			}
		});
	}

	Ok(())
}

fn apply_transform(path: PathBuf, slugs: &BTreeMap<PathBuf, PathBuf>) -> PathBuf {
	// Iterate in reverse order. This should visit more specific slugs first.
	for (slug, base) in slugs.iter().rev() {
		if let Ok(path) = path.strip_prefix(slug) {
			return base.join(path);
		}
	}
	panic!("{path:?} does not match anything in {slugs:?}");
}

#[pin_project]
struct AlsoPollFuture<R, F> {
	#[pin]
	reader: R,
	#[pin]
	task: Fuse<F>,
}
impl<R, F> AsyncRead for AlsoPollFuture<R, F>
where
	R: AsyncRead,
	F: Future<Output = ()>,
{
	fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
		let self_ = self.project();
		let _ = self_.task.poll(cx);
		self_.reader.poll_read(cx, buf)
	}
}
impl<R, F> AsyncBufRead for AlsoPollFuture<R, F>
where
	R: AsyncBufRead,
	F: Future<Output = ()>,
{
	fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
		let self_ = self.project();
		let _ = self_.task.poll(cx);
		self_.reader.poll_fill_buf(cx)
	}
	fn consume(self: Pin<&mut Self>, amt: usize) {
		self.project().reader.consume(amt)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	#[ignore]
	async fn loopback() -> Result<(), Box<dyn Error>> {
		let target_dir = PathBuf::from(env::var("TARGET_DIR").unwrap());
		let cargo_home = PathBuf::from(env::var("CARGO_HOME").unwrap());

		let paths = walk(PathBuf::from("cargo"), cargo_home, None).chain(walk(PathBuf::from("target"), target_dir, None));
		let tar = tar(paths);
		untar(
			vec![(PathBuf::from("cargo"), PathBuf::from("/tmp/cargo")), (PathBuf::from("target"), PathBuf::from("/tmp/target"))]
				.into_iter()
				.collect(),
			tokio::io::BufReader::with_capacity(16 * 1024 * 1024, tar),
		)
		.await?;

		// rsync -avnc --delete target/ target2
		// https://unix.stackexchange.com/questions/57305/rsync-compare-directories/351112#351112
		Ok(())
	}
}
