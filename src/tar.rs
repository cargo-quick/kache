use async_compression::{
    tokio::{bufread::ZstdDecoder, write::ZstdEncoder},
    Level,
};
use futures::{future::Fuse, FutureExt, StreamExt};
use pin_project::pin_project;
use std::{
    collections::BTreeMap,
    fs::{self},
    future::Future,
    io,
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
    time::SystemTime,
};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWriteExt, ReadBuf};
use tokio_tar::EntryType;
use walkdir::WalkDir;

pub struct FilePathInfo {
    slug: PathBuf,
    base: PathBuf,
    relative: Option<PathBuf>,
}

pub fn walk(
    slug: PathBuf,
    base: PathBuf,
    cutoff: Option<SystemTime>,
) -> Result<impl Iterator<Item = FilePathInfo>, io::Error> {
    let mut walk = WalkDir::new(&base)
        .sort_by(|a, b| a.file_name().cmp(b.file_name()))
        .into_iter()
        .peekable();
    let entry = walk.peek().unwrap();
    if let Err(e) = entry {
        if e.io_error()
            .map_or(false, |e| e.kind() == io::ErrorKind::NotFound)
        {
            let e = walk.next().unwrap().err().unwrap();
            return Err(e.into_io_error().unwrap());
        }
    }
    Ok(walk.filter_map(move |entry| {
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
            Some(FilePathInfo {
                slug: slug.clone(),
                base: base.clone(),
                relative: path,
            })
        } else {
            None
        }
    }))
}

pub fn tar(paths: impl Iterator<Item = FilePathInfo>) -> impl AsyncRead {
    let (writer, reader) = async_pipe::pipe();
    let task = async move {
        // create a .tar.zsd
        // FIXME: switch to a threaded implementation using std::io::Write, rather than
        // async-compression, and set the encoder to be multithreaded:
        // https://docs.rs/zstd/0.9.0+zstd.1.5.0/zstd/stream/write/struct.Encoder.html#method.multithread
        // This may allow us to get the compression speeds promised by these benchmarks:
        // https://community.centminmod.com/threads/round-4-compression-comparison-benchmarks-zstd-vs-brotli-vs-pigz-vs-bzip2-vs-xz-etc.18669/
        let tar_ = ZstdEncoder::with_quality(writer, Level::Fastest);
        let mut tar_ = tokio_tar::Builder::new(tar_);
        tar_.mode(tokio_tar::HeaderMode::Complete);
        tar_.follow_symlinks(false);

        // add resources to tar
        for FilePathInfo {
            mut slug,
            mut base,
            relative,
        } in paths
        {
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

pub async fn untar(
    slugs: BTreeMap<PathBuf, PathBuf>,
    tar: impl AsyncBufRead,
) -> Result<(), io::Error> {
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
        // https://github.com/rust-lang/rust/blob/e100ec5bc7cd768ec17d75448b29c9ab4a39272b/src/bootstrap/clean.rs#L98-L114
        if let Ok(metadata) = fs::symlink_metadata(&path) {
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
                path.symlink_metadata(),
                parent.symlink_metadata()
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
