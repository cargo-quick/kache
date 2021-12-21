mod aws;

use argh::FromArgs;
use async_compression::{
    tokio::{bufread::ZstdDecoder, write::ZstdEncoder},
    Level,
};
use futures::{
    future::{try_join_all, Fuse},
    FutureExt, StreamExt,
};
use pin_project::pin_project;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::StaticProvider;
use rusoto_s3::S3Client;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    env,
    error::Error,
    fs::{self, read_to_string},
    future::Future,
    io, iter, panic,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::SystemTime,
};
use tokio::{
    io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf},
    process,
};
use tokio_tar::EntryType;
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

#[derive(Deserialize)]
struct Crates {
    installs: HashMap<String, Install>,
}

#[derive(Deserialize)]
struct Install {
    bins: Vec<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env::set_var("RUST_BACKTRACE", "1");
    dotenv::dotenv().ok();

    let cmd: Cmd = argh::from_env();

    let aws_config: AwsConfig = envy::prefixed("AWS_").from_env().unwrap();
    let config: Config = envy::prefixed("KACHE_").from_env().unwrap();

    let http_client = Arc::new(HttpClient::new().expect("failed to create request dispatcher"));
    let creds = StaticProvider::new(
        aws_config.access_key_id,
        aws_config.secret_access_key,
        None,
        None,
    );
    let s3_client = &S3Client::new_with(http_client.clone(), creds.clone(), aws_config.region);

    let bucket = &config.bucket;

    let target_dir = PathBuf::from("./target"); // TODO: actually deduce
    let cargo_home = home::cargo_home().unwrap();
    let mut docker_dir = shiplift::Docker::new()
        .info()
        .await
        .ok()
        .map(|info| PathBuf::from(info.docker_root_dir));
    if docker_dir.is_some() && cfg!(target_os = "macos") {
        docker_dir = Some(
            home::home_dir()
                .unwrap()
                .join("Library/Containers/com.docker.docker/Data/vms"),
        );
    }
    let crates: Crates =
        serde_json::from_slice(&fs::read(cargo_home.join(".crates2.json")).unwrap()).unwrap();
    let cargo_bins = crates
        .installs
        .values()
        .flat_map(|install| &install.bins)
        .filter(|bin| bin.file_stem().unwrap() != "kache")
        .collect::<BTreeSet<_>>();

    stop_docker(|| async move {
        match cmd.cmd {
            Cmd_::Save(CmdSave { keys }) => {
                println!("saving cache to: {:?}", keys);
                let id: [u8; 16] = rand::random();
                let id = base_x::encode(WEB_SAFE, &id);
                let info_path = PathBuf::from(".kache-info");
                let (id, cutoff) = match info_path.metadata() {
                    Ok(info) => {
                        let parent_id = read_to_string(info_path).unwrap();
                        let parent_id = parent_id.trim_end_matches('\n');
                        (
                            format!("{}:{}", parent_id, id),
                            Some(info.modified().unwrap()),
                        )
                    }
                    Err(_) => (id, None),
                };
                println!("packing {}", id);
                let tar = tar(iter::empty()
                    .chain(walk(
                        PathBuf::from("cargo").join(".crates.toml"),
                        cargo_home.join(".crates.toml"),
                        cutoff,
                    ))
                    .chain(walk(
                        PathBuf::from("cargo").join(".crates2.json"),
                        cargo_home.join(".crates2.json"),
                        cutoff,
                    ))
                    .chain(cargo_bins.into_iter().flat_map(|bin| {
                        walk(
                            PathBuf::from("cargo").join("bin").join(&bin),
                            cargo_home.join("bin").join(bin),
                            cutoff,
                        )
                    }))
                    .chain(walk(
                        PathBuf::from("cargo").join("registry/index"),
                        cargo_home.join("registry/index"),
                        cutoff,
                    ))
                    .chain(walk(
                        PathBuf::from("cargo").join("registry/cache"),
                        cargo_home.join("registry/cache"),
                        cutoff,
                    ))
                    .chain(walk(
                        PathBuf::from("cargo").join("git/db"),
                        cargo_home.join("git/db"),
                        cutoff,
                    ))
                    .chain(walk(PathBuf::from("target"), target_dir, cutoff))
                    .chain(
                        docker_dir
                            .as_ref()
                            .map(|docker_dir| {
                                walk(PathBuf::from("docker"), docker_dir.clone(), cutoff)
                            })
                            .into_iter()
                            .flatten(),
                    ));
                aws::upload(
                    s3_client,
                    bucket.clone(),
                    format!("blobs/{}.tar.zst", id),
                    String::from("application/zstd"),
                    Some(31536000),
                    tar,
                )
                .await?;
                let id = &id;
                let _: Vec<()> = try_join_all(keys.into_iter().map(|key| async move {
                    let key = format!("keys/{}.txt", key);
                    aws::upload(
                        s3_client,
                        bucket.clone(),
                        key,
                        String::from("text/plain"),
                        Some(600),
                        id.as_bytes(),
                    )
                    .await
                }))
                .await?;
                println!("finished saving cache");
            }
            Cmd_::Uncache(CmdLoad { keys }) => {
                println!("fetching cache from: {:?}", keys);
                for key in keys {
                    let key = format!("keys/{}.txt", key);
                    if let Ok(mut id_) = aws::download(s3_client, bucket.clone(), key.clone()).await
                    {
                        // Assumption: if this id is listed in s3 under this cache key then the underlying blobs *must* still exist.
                        let mut id = String::new();
                        let _ = id_.read_to_string(&mut id).await?;
                        println!("cache hit: {} -> {}", key, id);
                        let ids = id
                            .match_indices(':')
                            .map(|(index, _)| &id[..index])
                            .chain(std::iter::once(id.as_ref()));
                        for id in ids {
                            let blob_id = format!("blobs/{}.tar.zst", id);
                            println!("unpacking {}", blob_id);
                            let tar = aws::download(s3_client, bucket.clone(), blob_id).await?;
                            untar(
                                [
                                    (
                                        PathBuf::from("cargo").join(".crates.toml"),
                                        cargo_home.join(".crates.toml"),
                                    ),
                                    (
                                        PathBuf::from("cargo").join(".crates2.json"),
                                        cargo_home.join(".crates2.json"),
                                    ),
                                    (PathBuf::from("cargo").join("bin"), cargo_home.join("bin")),
                                    (
                                        PathBuf::from("cargo").join("registry/index"),
                                        cargo_home.join("registry/index"),
                                    ),
                                    (
                                        PathBuf::from("cargo").join("registry/cache"),
                                        cargo_home.join("registry/cache"),
                                    ),
                                    (
                                        PathBuf::from("cargo").join("git/db"),
                                        cargo_home.join("git/db"),
                                    ),
                                    (PathBuf::from("target"), target_dir.clone()),
                                ]
                                .into_iter()
                                .chain(docker_dir.as_ref().map(|docker_dir| {
                                    (PathBuf::from("docker"), docker_dir.clone())
                                }))
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
    })
    .await?
}

async fn stop_docker<F, O>(f: impl FnOnce() -> F) -> Result<O, Box<dyn Error>>
where
    F: Future<Output = O>,
{
    if cfg!(windows) {
        let mut docker_stop = process::Command::new("powershell");
        docker_stop.args(["-command", "Stop-Service docker -Force"]);
        let status = docker_stop.status().await?;
        if !status.success() {
            return Err(format!("{:?}", status).into());
        }
    } else if cfg!(target_os = "linux") {
        let mut docker_stop = process::Command::new("sudo");
        docker_stop.args(["systemctl", "stop", "docker"]);
        let status = docker_stop.status().await?;
        if !status.success() {
            return Err(format!("{:?}", status).into());
        }
    }
    let ret = panic::AssertUnwindSafe(f()).catch_unwind().await;
    if cfg!(windows) {
        let mut docker_stop = process::Command::new("powershell");
        docker_stop.args(["-command", "Start-Service docker"]);
        let status = docker_stop.status().await?;
        if !status.success() {
            return Err::<O, _>(format!("{:?}", status).into());
        }
    } else if cfg!(target_os = "linux") {
        let mut docker_stop = process::Command::new("sudo");
        docker_stop.args(["systemctl", "start", "docker"]);
        let status = docker_stop.status().await?;
        if !status.success() {
            return Err::<O, _>(format!("{:?}", status).into());
        }
    }
    match ret {
        Ok(ret) => Ok(ret),
        Err(e) => panic::resume_unwind(e),
    }
}

fn walk(
    slug: PathBuf,
    base: PathBuf,
    cutoff: Option<SystemTime>,
) -> impl Iterator<Item = (PathBuf, PathBuf, Option<PathBuf>)> {
    WalkDir::new(&base)
        .sort_by(|a, b| a.file_name().cmp(b.file_name()))
        .into_iter()
        .filter_map(move |entry| {
            let entry = entry.unwrap();
            let t = entry.file_type();
            let path = entry.path().strip_prefix(&base).unwrap().to_owned();
            let non_empty = path.components().next().is_some();
            let path = non_empty.then(|| path);
            if (t.is_file() || t.is_symlink() || t.is_dir())
                && cutoff
                    .map(|cutoff| cutoff < entry.metadata().unwrap().modified().unwrap())
                    .unwrap_or(true)
            {
                Some((slug.clone(), base.clone(), path))
            } else {
                None
            }
        })
}

fn tar(paths: impl Iterator<Item = (PathBuf, PathBuf, Option<PathBuf>)>) -> impl AsyncRead {
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
        tar_.follow_symlinks(false);

        // add resources to tar
        for (mut slug, mut base, relative) in paths {
            if let Some(relative) = relative {
                base = base.join(&relative);
                slug = slug.join(&relative);
            }
            tar_.append_path_with_name(base.clone(), slug)
                .await
                .unwrap_or_else(|e| panic!("can't tar {}: {:?}", base.display(), e));
        }

        // flush writers
        let mut tar_ = tar_.into_inner().await.unwrap();
        tar_.shutdown().await.unwrap();
        let _tar = tar_.into_inner();
    };

    AlsoPollFuture {
        reader,
        task: task.fuse(),
    }
}

async fn untar(slugs: BTreeMap<PathBuf, PathBuf>, tar: impl AsyncBufRead) -> Result<(), io::Error> {
    tokio::pin!(tar);
    let tar = ZstdDecoder::new(tar);
    let tar = tokio::io::BufReader::with_capacity(16 * 1024 * 1024, tar);

    let mut entries = tokio_tar::Archive::new(tar).entries().unwrap();

    let mut mtimes = BTreeMap::new();

    while let Some(entry) = entries.next().await {
        let mut entry = entry.unwrap();
        let path = entry.path().unwrap().into_owned();

        let path = apply_transform(path, &slugs);

        let parent = path.parent().unwrap();
        let _ = fs::create_dir_all(&parent);
        if let Ok(metadata) = fs::metadata(&path) {
            let mut perms = metadata.permissions();
            perms.set_readonly(false);
            let _ = fs::set_permissions(&path, perms);
        }
        let _ = fs::remove_file(&path);
        match entry.header().entry_type() {
            EntryType::Directory => {
                let _ = mtimes.insert(path.clone(), entry.header().mtime().unwrap());
            }
            _ => {
                let _ = fs::remove_dir_all(&path);
            }
        }
        entry.unpack(&path).await.map(drop).unwrap_or_else(|err| {
            panic!(
                "Could not unpack {:?}: {:?}\nmetadata: {:?}\nparent_metadata: {:?}",
                path,
                err,
                path.metadata(),
                parent.metadata()
            )
        });
    }

    for (path, mtime) in mtimes.into_iter().rev() {
        filetime::set_file_mtime(
            path,
            filetime::FileTime::from_unix_time(mtime.try_into().unwrap(), 0),
        )
        .unwrap();
    }

    Ok(())
}

fn apply_transform(path: PathBuf, slugs: &BTreeMap<PathBuf, PathBuf>) -> PathBuf {
    // Iterate in reverse order. This should visit more specific slugs first.
    for (slug, base) in slugs.iter().rev() {
        if let Ok(path) = path.strip_prefix(slug) {
            let mut base = base.clone();
            let non_empty = path.components().next().is_some();
            if non_empty {
                base = base.join(path);
            }
            return base;
        }
    }
    panic!("{:?} does not match anything in {:?}", path, slugs);
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
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
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

        let paths = walk(PathBuf::from("cargo"), cargo_home, None).chain(walk(
            PathBuf::from("target"),
            target_dir,
            None,
        ));
        let tar = tar(paths);
        untar(
            vec![
                (PathBuf::from("cargo"), PathBuf::from("/tmp/cargo")),
                (PathBuf::from("target"), PathBuf::from("/tmp/target")),
            ]
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
