mod aws;
mod docker;
mod tar;

use argh::FromArgs;
use futures::future::try_join_all;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::StaticProvider;
use rusoto_s3::S3Client;
use serde::Deserialize;
use std::{
    collections::{BTreeSet, HashMap},
    env,
    error::Error,
    fs::{self, read_to_string},
    iter,
    path::PathBuf,
    sync::Arc,
};
use tokio::io::AsyncReadExt;

use tar::{tar, untar, walk};

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
    Load(CmdLoad),
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

    let config = &envy::prefixed("KACHE_").from_env::<Config>().unwrap();
    let s3_client = &{
        let aws_config: AwsConfig = envy::prefixed("AWS_").from_env().unwrap();

        let http_client = Arc::new(HttpClient::new().expect("failed to create request dispatcher"));
        let creds = StaticProvider::new(
            aws_config.access_key_id,
            aws_config.secret_access_key,
            None,
            None,
        );
        S3Client::new_with(http_client, creds, aws_config.region)
    };

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

    docker::stop_docker(|| async move {
        match cmd.cmd {
            Cmd_::Save(CmdSave { keys }) => {
                println!("saving cache to: {:?}", keys);
                let id: [u8; 16] = rand::random();
                let id = base_x::encode(WEB_SAFE, &id);
                let info_path = PathBuf::from(".kache-info");
                let (id, cutoff) = match info_path.symlink_metadata() {
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
                    .chain(walk(PathBuf::from("cwd"), cwd, cutoff))
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
                    config.bucket.clone(),
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
                        config.bucket.clone(),
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
            Cmd_::Load(CmdLoad { keys }) => {
                println!("fetching cache from: {:?}", keys);
                for key in keys {
                    let key = format!("keys/{}.txt", key);
                    if let Ok(mut id_) =
                        aws::download(s3_client, config.bucket.clone(), key.clone()).await
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
