use futures::FutureExt;
use std::{error::Error, future::Future, panic, time::Duration};
use tokio::process;

pub(crate) async fn stop_docker<F, O>(f: impl FnOnce() -> F) -> Result<O, Box<dyn Error>>
where
    F: Future<Output = O>,
{
    let has_docker = shiplift::Docker::new().info().await.is_ok();
    if has_docker {
        println!("Stopping docker");
        if cfg!(windows) {
            let mut docker_stop = process::Command::new("powershell");
            docker_stop.args(["-command", "Stop-Service docker"]);
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
            let mut docker_stop = process::Command::new("sudo");
            docker_stop.args(["systemctl", "stop", "docker.socket"]);
            let _status = docker_stop.status().await?;
        } else if cfg!(target_os = "macos") {
            let mut docker_stop = process::Command::new("osascript");
            docker_stop.args(["-e", "quit app \"Docker\""]);
            let status = docker_stop.status().await?;
            if !status.success() {
                return Err(format!("{:?}", status).into());
            }
        } else {
            unimplemented!();
        }
        while shiplift::Docker::new().info().await.is_ok() {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    let ret = panic::AssertUnwindSafe(f()).catch_unwind().await;
    if has_docker {
        println!("Starting docker");
        if cfg!(windows) {
            let mut docker_stop = process::Command::new("powershell");
            docker_stop.args(["-command", "Start-Service docker"]); // TODO: wait till it's listening before returning
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
        } else if cfg!(target_os = "macos") {
            let mut docker_stop = process::Command::new("open");
            docker_stop.args(["-a", "Docker"]);
            let status = docker_stop.status().await?;
            if !status.success() {
                return Err(format!("{:?}", status).into());
            }
        } else {
            unimplemented!();
        }
        while shiplift::Docker::new().info().await.is_err() {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    match ret {
        Ok(ret) => Ok(ret),
        Err(e) => panic::resume_unwind(e),
    }
}
