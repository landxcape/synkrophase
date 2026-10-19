use std::time::Duration;
use tokio::time::sleep;

use crate::daemon::paths::DaemonPaths;
use crate::doctor::report::DoctorReport;

pub async fn apply_repairs(report: &DoctorReport, paths: &DaemonPaths) -> Vec<String> {
    let mut repairs = Vec::new();

    for check in &report.checks {
        if !check.is_fixable {
            continue;
        }

        match check.name.as_str() {
            "IPC Socket" => {
                if paths.socket_path.exists() {
                    paths.clean_socket();
                    paths.clean_pid();
                    repairs.push(format!(
                        "Removed stale IPC socket ({:?}) and reset PID file",
                        paths.socket_path
                    ));
                }
            }
            "Daemon Environment" => {
                if !paths.base_dir.exists() && paths.ensure_dir_exists().is_ok() {
                    repairs.push(format!(
                        "Created daemon configuration directory ({:?})",
                        paths.base_dir
                    ));
                }
            }
            "Media Player" => {
                #[cfg(target_os = "macos")]
                {
                    let launched = std::process::Command::new("open")
                        .arg("-a")
                        .arg("Spotify")
                        .status();

                    match launched {
                        Ok(s) if s.success() => {
                            repairs.push("Launched desktop Spotify application".to_string());
                            sleep(Duration::from_millis(500)).await;
                        }
                        _ => {
                            if let Ok(ms) = std::process::Command::new("open")
                                .arg("-a")
                                .arg("Music")
                                .status()
                                && ms.success()
                            {
                                repairs
                                    .push("Launched desktop Apple Music application".to_string());
                                sleep(Duration::from_millis(500)).await;
                            }
                        }
                    }
                }
                #[cfg(target_os = "linux")]
                {
                    if std::process::Command::new("spotify").spawn().is_ok() {
                        repairs.push("Spawned desktop Spotify process".to_string());
                        sleep(Duration::from_millis(500)).await;
                    }
                }
                #[cfg(target_os = "windows")]
                {
                    if std::process::Command::new("cmd")
                        .args(["/c", "start", "spotify:"])
                        .spawn()
                        .is_ok()
                    {
                        repairs.push("Spawned Spotify URI launcher".to_string());
                        sleep(Duration::from_millis(500)).await;
                    }
                }
                #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
                {
                    // No-op for mock environments
                }
            }
            _ => {}
        }
    }

    repairs
}
