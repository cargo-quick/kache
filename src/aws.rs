#![allow(clippy::must_use_candidate)]

use bytes::{Bytes, BytesMut};
use futures::{stream, Stream, StreamExt, TryStreamExt};
use md5::{Digest, Md5};
use rusoto_core::{HttpClient, RusotoError};
use rusoto_credential::StaticProvider;
use rusoto_s3::{
    CompleteMultipartUploadRequest, CompletedMultipartUpload, CompletedPart,
    CreateMultipartUploadRequest, GetObjectRequest, HeadObjectRequest, ListObjectsV2Error,
    ListObjectsV2Output, ListObjectsV2Request, Object, S3Client, UploadPartRequest, S3 as _,
};
use std::{error::Error, future::Future, io, str, time::Duration};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt};

pub type S3 = S3Client;
pub type Region = rusoto_core::Region;

/// Convenience method to create an S3 client
pub fn s3_new(access_key_id: &str, secret_access_key: &str, region: Region) -> S3 {
    S3Client::new_with(
        HttpClient::new().expect("failed to create request dispatcher"),
        StaticProvider::new(access_key_id.to_owned(), secret_access_key.to_owned(), None, None),
        region,
    )
}

/// # Panics
/// TODO: panic docs
///
/// # Errors
/// TODO: error docs
#[allow(clippy::too_many_lines)] // TODO
pub async fn upload(
    s3_client: &S3, bucket: String, key: String, content_encoding: Option<String>,
    content_type: String, cache_control: Option<u64>, read: impl AsyncRead,
) -> Result<(), Box<dyn Error>> {
    let pb = &indicatif::ProgressBar::new(0).with_finish(indicatif::ProgressFinish::AndLeave);
    pb.set_style(
        indicatif::ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})").unwrap()
            .progress_chars("#>-")
    );

    let part_size = 16 * 1024 * 1024;
    assert!((5_242_880..5_368_709_120).contains(&part_size)); // S3's min and max 5MiB and 5GiB
    let parallelism = 10;

    let upload_id = &rusoto_retry(|| async {
        s3_client
            .create_multipart_upload(CreateMultipartUploadRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                content_encoding: content_encoding.clone(),
                content_type: Some(content_type.clone()),
                cache_control: Some(cache_control.map_or_else(
                    || String::from("no-store, must-revalidate"),
                    |secs| format!("public, max-age={}", secs),
                )),
                ..CreateMultipartUploadRequest::default()
            })
            .await
    })
    .await?
    .upload_id
    .unwrap();

    tokio::pin!(read);

    let (bucket, key) = (&bucket, &key);

    // TODO: if only 1 part:
    //  let _ = rusoto_retry(|| async {
    //      let mut tar = tar.try_clone().await.unwrap();
    //      let _ = tar.rewind().await.unwrap();
    //      let tar = pb.wrap_async_read(tar);
    //      s3_client
    //          .put_object(PutObjectRequest {
    //              bucket: bucket.clone(),
    //              key: key.clone(),
    //              cache_control: Some(format!("public, max-age={}", 31536000)),
    //              content_type: Some(String::from("application/zstd")),
    //              ..Default::default()
    //          })
    //          .await
    //  })
    //  .await
    //  .unwrap();

    let e_tags = futures::stream::unfold(read, |mut read| async move {
        let mut buf = BytesMut::with_capacity(part_size);
        loop {
            let n = match read.read_buf(&mut buf).await {
                Ok(n) => n,
                Err(e) => return Some((Err(e), read)),
            };
            assert!(buf.len() <= part_size);
            if n == 0 || buf.len() == part_size {
                break;
            }
        }
        pb.inc_length(buf.len().try_into().unwrap());
        (!buf.is_empty()).then(|| (Ok::<_, io::Error>(buf.freeze()), read))
    })
    .enumerate()
    .map(|(i, buf): (usize, Result<Bytes, _>)| async move {
        let buf = &buf?;
        assert!(i < 10_000); // S3's max
        Ok::<_, io::Error>(
            rusoto_retry(|| async {
                let buf = buf.clone();
                let buf_len = buf.len();
                let ret = s3_client
                    .upload_part(UploadPartRequest {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        upload_id: upload_id.clone(),
                        part_number: (i + 1).try_into().unwrap(),
                        content_md5: Some(base64::encode(
                            &Md5::new().chain_update(&buf).finalize(),
                        )),
                        body: Some(rusoto_core::ByteStream::new_with_size(
                            stream::once(async { Ok(buf) }),
                            buf_len,
                        )),
                        ..UploadPartRequest::default()
                    })
                    .await;
                pb.inc(buf_len.try_into().unwrap());
                ret
            })
            .await
            .unwrap()
            .e_tag
            .unwrap(),
        )
    })
    .buffered(parallelism)
    .try_collect::<Vec<String>>()
    .await?;

    let _ = rusoto_retry(|| async {
        s3_client
            .complete_multipart_upload(CompleteMultipartUploadRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id: upload_id.clone(),
                multipart_upload: Some(CompletedMultipartUpload {
                    parts: Some(
                        e_tags
                            .iter()
                            .enumerate()
                            .map(|(i, e_tag)| CompletedPart {
                                part_number: Some((i + 1).try_into().unwrap()),
                                e_tag: Some(e_tag.clone()),
                            })
                            .collect(),
                    ),
                }),
                ..CompleteMultipartUploadRequest::default()
            })
            .await
    })
    .await?;

    Ok(())
}

/// # Panics
/// TODO: panic docs
///
/// # Errors
/// TODO: error docs
#[allow(clippy::too_many_lines)] // TODO
pub async fn download(
    s3_client: &S3, bucket: String, key: String,
) -> Result<impl AsyncBufRead + '_, io::Error> {
    let head = rusoto_retry(|| async {
        s3_client
            .head_object(HeadObjectRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                part_number: Some(1),
                ..HeadObjectRequest::default()
            })
            .await
    })
    .await
    .map_err(|e| io::Error::new(io::ErrorKind::NotFound, e.to_string()))?;

    let (part_size, parts): (u64, Option<u64>) = (
        head.content_length.unwrap().try_into().unwrap(),
        head.parts_count.map(|x| x.try_into().unwrap()),
    );

    let length: u64 = if head.parts_count.is_some() {
        rusoto_retry(|| async {
            s3_client
                .head_object(HeadObjectRequest {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    part_number: None,
                    ..HeadObjectRequest::default()
                })
                .await
        })
        .await
        .unwrap()
        .content_length
        .unwrap()
        .try_into()
        .unwrap()
    } else {
        part_size
    };

    assert!(
        part_size * (parts.unwrap_or(1) - 1) < length && length <= part_size * parts.unwrap_or(1)
    );

    let pb = indicatif::ProgressBar::new(length).with_finish(indicatif::ProgressFinish::AndLeave);
    pb.set_style(
        indicatif::ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})").unwrap()
            .progress_chars("#>-")
    );

    if let Some(parts) = parts {
        let parallelism = 20;

        let body = tokio_util::io::StreamReader::new(
            futures::stream::iter((0..parts).map(move |i| {
                let (pb, bucket, key) = (pb.clone(), bucket.clone(), key.clone());

                // This is the part of the code that does the actual downloading.
                async move {
                    let range = part_size * i..(part_size * (i + 1)).min(length);
                    let cap: usize = (range.end - range.start).try_into().unwrap();

                    'async_read: loop {
                        let body = rusoto_retry(|| async {
                            s3_client
                                .get_object(GetObjectRequest {
                                    bucket: bucket.clone(),
                                    key: key.clone(),
                                    part_number: Some((i + 1).try_into().unwrap()),
                                    ..GetObjectRequest::default()
                                })
                                .await
                        })
                        .await
                        .unwrap()
                        .body
                        .unwrap()
                        .into_async_read();

                        let mut body = pb.wrap_async_read(body);
                        let mut buf = BytesMut::with_capacity(cap);

                        while buf.len() != cap {
                            let _bytes = match body.read_buf(&mut buf).await {
                                Ok(bytes) => bytes,
                                Err(_e) => continue 'async_read,
                            };
                            assert!(buf.len() <= cap);
                        }
                        break Ok::<_, io::Error>(buf);
                    }
                }
            }))
            .buffered(parallelism),
        );

        Ok(tokio_util::either::Either::Left(body))
    } else {
        let body = rusoto_retry(|| async {
            s3_client
                .get_object(GetObjectRequest {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    ..GetObjectRequest::default()
                })
                .await
        })
        .await
        .unwrap()
        .body
        .unwrap()
        .into_async_read();

        let body = pb.wrap_async_read(body);
        let body = tokio::io::BufReader::with_capacity(16 * 1024 * 1024, body);

        Ok(tokio_util::either::Either::Right(body))
    }
}

/// Wraps [Future] which returns a [Result<_, RusottoError>] and automatically retries it if some
/// transient errors are found.
///
/// # Panics
/// Should never panic if the passed in function does not.
///
/// # Errors
/// The errors that are not caught are passed on transparently.
pub async fn rusoto_retry<F, U, T, E>(mut f: F) -> Result<T, RusotoError<E>>
where
    F: FnMut() -> U,
    U: Future<Output = Result<T, RusotoError<E>>>,
{
    loop {
        match f().await {
            Err(RusotoError::HttpDispatch(e)) => {
                println!("Got transient error: {:?}. Retrying.", e);
            }
            Err(RusotoError::Unknown(response))
                // backblaze gives us 501 when you give it options it doesn't support
                if matches!(response.status.as_u16(), 500 | 502 | 503 | 504)
                    || (response.status == 403
                        && str::from_utf8(&response.body)
                            .unwrap()
                            .contains("RequestTimeTooSkewed")) => {}
            res => break res,
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

type StreamItem = Result<ListObjectsV2Output, RusotoError<ListObjectsV2Error>>;

/// This stream repeatedly runs the same request, taking care of adjusting the
/// [ListObjectsV2Request::continuation_token] as appropriate.
fn stream_list_objects_v2(
    request: ListObjectsV2Request, s3_client: &S3Client,
) -> impl Stream<Item = StreamItem> + '_ {
    type State<'c> = Option<(ListObjectsV2Request, &'c S3Client)>;

    async fn retrieve_list_fragment(state: State<'_>) -> Option<(StreamItem, State<'_>)> {
        let (mut request, s3_client) = state?;

        let response = rusoto_retry(|| s3_client.list_objects_v2(request.clone())).await;

        match response {
            Ok(output) if output.next_continuation_token.is_some() => {
                request.continuation_token = output.next_continuation_token.clone();
                Some((Ok(output), Some((request, s3_client))))
            }
            Ok(output) => Some((Ok(output), None)),
            Err(error) => Some((Err(error), None)),
        }
    }

    stream::unfold(Some((request, s3_client)), retrieve_list_fragment)
}

/// Returns all [Object]s in a bucket.
///
/// # Panics
/// Should never panic.
///
/// # Errors
/// Errors are for failed paginated requests, the iterator will terminate at the first failed
/// request (or when the bucket has been exausted).
pub async fn list_objects(
    bucket: &str, s3_client: &S3Client,
) -> Result<impl Iterator<Item = Object>, RusotoError<ListObjectsV2Error>> {
    let responses: Vec<Result<ListObjectsV2Output, _>> = stream_list_objects_v2(
        ListObjectsV2Request { bucket: bucket.to_string(), ..ListObjectsV2Request::default() },
        s3_client,
    )
    .collect()
    .await;

    let mut objects = vec![];

    for response in responses {
        let output = response?;
        // API unclear, but we would expect an empty bucket to
        // return a `Some([])` instead of a `None`.
        let contents = output.contents.unwrap();
        objects.push(contents);
    }

    Ok(objects.into_iter().flatten())
}
