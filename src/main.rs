mod aws;
mod docker;
mod tar;

use argh::FromArgs;
use futures::{future::try_join_all, stream, StreamExt};
use rusoto_s3::S3Client;
use serde::{de, Deserialize};
use serde_with::DeserializeAs;
use std::{
    collections::{BTreeSet, HashMap},
    env,
    error::Error,
    fs,
    iter,
    path::PathBuf,
    str::FromStr,
};
use tokio::io::AsyncReadExt;

use docker::stop_docker;
use tar::{tar, untar, walk};
use aws::Region;

const WEB_SAFE: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_~";

#[derive(Deserialize, Debug)]
struct AwsConfig {
    access_key_id: String,
    secret_access_key: String,
    #[serde(default, deserialize_with = "deserialize_region")]
    region: Option<Region>,
    #[serde(default, with = "serde_with::rust::string_empty_as_none")]
    endpoint: Option<String>,
}

fn deserialize_region<'de, D>(deserializer: D) -> Result<Option<Region>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    serde_with::NoneAsEmptyString::deserialize_as(deserializer).and_then(|str: Option<String>| {
        Ok(match str {
            Some(str) => Some(Region::from_str(&str).map_err(de::Error::custom)?),
            None => None,
        })
    })
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
    Load(CmdLoad),
}

#[derive(FromArgs, Debug)]
/// save cache to s3
#[argh(subcommand, name = "save")]
struct CmdSave {
    /// overwrite the contents of this key, even if it already exists.
    /// Note that if you repeatedly pull from a key and re-push to it then you will
    /// gradually accumulate cruft, so you will want a strategy to stop cruft from
    /// building up. Rotating your cache key every week is one approach to this problem.
    #[argh(positional)]
    keys: Vec<String>,

    /// only write to this key if it doesn't already exist
    #[argh(option)]
    create: Vec<String>,
}

#[derive(FromArgs, Debug)]
/// load cache from s3
#[argh(subcommand, name = "load")]
struct CmdLoad {
    /// keys to download from. Will try from left to right, and stop when it gets a hit.
    #[argh(positional)]
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
    run().await
}

#[allow(clippy::too_many_lines)]
async fn run() -> Result<(), Box<dyn Error>> {
    if env::var("RUST_BACKTRACE").is_err() {
        env::set_var("RUST_BACKTRACE", "1");
    }
    dotenv::dotenv().ok();

    let cmd: Cmd = argh::from_env();

    let config = &envy::prefixed("KACHE_").from_env::<Config>().unwrap();
    let aws_config: AwsConfig = envy::prefixed("AWS_").from_env().unwrap();

    // Check if a custom endpoint has been provided?
    let region = if let Some(endpoint) = aws_config.endpoint {
        Region::Custom {
            name: aws_config
                .region
                .as_ref()
                .map_or("custom-region", Region::name)
                .to_owned(),
            endpoint,
        }
    } else {
        aws_config.region.expect("need AWS_REGION or AWS_ENDPOINT")
    };

    let s3_client = &aws::s3_new(&aws_config.access_key_id, &aws_config.secret_access_key, region);

    let cwd = PathBuf::from(".");
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
            Cmd_::Save(CmdSave {
                mut keys,
                mut create,
            }) => {
                create = stream::iter(create)
                    .filter(|k| {
                        let k = k.clone();
                        async move { !has_cache_key(s3_client, &config.bucket, &k).await }
                    })
                    .collect()
                    .await;

                keys.extend(create);

                if keys.is_empty() {
                    println!("Nothing to do. All keys already exist.");
                    return Ok(());
                }

                println!("saving cache to: {:?}", keys);
                let id: [u8; 16] = rand::random();
                let id = base_x::encode(WEB_SAFE, &id);
                println!("packing {}", id);

                let tar = tar(iter::empty()
                    .chain(
                        walk(
                            PathBuf::from("cargo").join(".crates.toml"),
                            cargo_home.join(".crates.toml"),
                        )
                        .into_iter()
                        .flatten(),
                    )
                    .chain(
                        walk(
                            PathBuf::from("cargo").join(".crates2.json"),
                            cargo_home.join(".crates2.json"),
                        )
                        .into_iter()
                        .flatten(),
                    )
                    .chain(cargo_bins.into_iter().flat_map(|bin| {
                        walk(
                            PathBuf::from("cargo").join("bin").join(&bin),
                            cargo_home.join("bin").join(bin),
                        )
                        .into_iter()
                        .flatten()
                    }))
                    .chain(
                        walk(
                            PathBuf::from("cargo").join("registry/index"),
                            cargo_home.join("registry/index"),
                        )
                        .into_iter()
                        .flatten(),
                    )
                    .chain(
                        walk(
                            PathBuf::from("cargo").join("registry/cache"),
                            cargo_home.join("registry/cache"),
                        )
                        .into_iter()
                        .flatten(),
                    )
                    .chain(
                        walk(
                            PathBuf::from("cargo").join("git/db"),
                            cargo_home.join("git/db"),
                        )
                        .into_iter()
                        .flatten(),
                    )
                    .chain(walk(PathBuf::from("cwd"), cwd).unwrap())
                    .chain(
                        docker_dir
                            .as_ref()
                            .map(|docker_dir| {
                                walk(PathBuf::from("docker"), docker_dir.clone())
                                    .into_iter()
                                    .flatten()
                            })
                            .into_iter()
                            .flatten(),
                    ));

                aws::upload(
                    s3_client,
                    config.bucket.clone(),
                    format!("blobs/{}.tar.zst", id),
                    None,
                    String::from("application/zstd"),
                    Some(31_536_000),
                    tar,
                )
                .await?;

                let id = &id;
                let _: Vec<()> = try_join_all(keys.into_iter().map(|key| async move {
                    let key = format!("keys/{}.txt", key);
                    aws::upload(
                        s3_client,
                        config.bucket.clone(),
                        key,
                        None,
                        String::from("text/plain"),
                        Some(600),
                        id.as_bytes(),
                    )
                    .await
                }))
                .await?;

                println!("finished saving cache");
            }
            Cmd_::Load(CmdLoad { keys }) => {
                println!("fetching cache from: {:?}", keys);

                for key in keys {
                    if let Ok(id) = get_blob_id(s3_client, &config.bucket, &key).await {
                        // Assumption: if this id is listed in s3 under this cache key then the
                        // underlying blobs *must* still exist.
                        println!("cache hit: {} -> {}", key, id);
                        let ids = id
                            .match_indices(':')
                            .map(|(index, _)| &id[..index])
                            .chain(std::iter::once(id.as_ref()));
                        for id in ids {
                            let blob_id = format!("blobs/{}.tar.zst", id);
                            println!("unpacking {}", blob_id);
                            let tar =
                                aws::download(s3_client, config.bucket.clone(), blob_id).await?;
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
                                    (PathBuf::from("cwd"), cwd.clone()),
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

async fn get_blob_id(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
) -> Result<String, Box<dyn Error>> {
    let key = format!("keys/{}.txt", key);
    let mut id_file = aws::download(s3_client, bucket.to_string(), key).await?;
    let mut id = String::new();
    let _ = id_file.read_to_string(&mut id).await?;
    Ok(id)
}

async fn has_cache_key(s3_client: &S3Client, bucket: &str, key: &str) -> bool {
    get_blob_id(s3_client, bucket, key).await.is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn loopback() -> Result<(), Box<dyn Error>> {
        let target_dir = PathBuf::from(env::var("TARGET_DIR").unwrap());
        let cargo_home = PathBuf::from(env::var("CARGO_HOME").unwrap());

        let paths = walk(PathBuf::from("cargo"), cargo_home)
            .unwrap()
            .chain(walk(PathBuf::from("target"), target_dir).unwrap());
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
