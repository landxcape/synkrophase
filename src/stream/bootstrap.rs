use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::config::DeviceConfig;
use crate::error::{Result, SynkroError};

const YTDLP_RELEASE_URL: &str = "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp";

pub fn ensure_ytdlp(config: &DeviceConfig) -> Result<PathBuf> {
    let path = if let Some(path) = config.ytdlp_path.as_ref().filter(|path| path.exists()) {
        path.clone()
    } else if let Some(path) = find_in_path("yt-dlp") {
        path
    } else {
        let workspace_binary = config.data_dir.join("bin").join("yt-dlp");
        if workspace_binary.exists() {
            workspace_binary
        } else {
            return Err(SynkroError::YtdlpMissing);
        }
    };

    // Try to update in background (non-blocking, don't care if it fails)
    let update_path = path.clone();
    std::thread::spawn(move || {
        let _ = std::process::Command::new(update_path)
            .arg("-U")
            .output();
    });

    Ok(path)
}

pub fn parse_expiry(url: &str) -> Option<u64> {
    let query = url.split_once('?')?.1;
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=')?;
        if key == "expire" {
            return value.parse().ok();
        }
    }
    None
}

pub fn download_ytdlp(target_dir: &Path) -> Result<PathBuf> {
    let response = reqwest::blocking::get(YTDLP_RELEASE_URL)
        .map_err(|err| SynkroError::StreamResolution(err.to_string()))?;
    if !response.status().is_success() {
        return Err(SynkroError::StreamResolution(format!(
            "yt-dlp download failed with status {}",
            response.status()
        )));
    }

    let bytes = response
        .bytes()
        .map_err(|err| SynkroError::StreamResolution(err.to_string()))?;
    install_ytdlp_binary(target_dir, bytes.as_ref())
}

pub fn install_ytdlp_binary(target_dir: &Path, bytes: &[u8]) -> Result<PathBuf> {
    fs::create_dir_all(target_dir).map_err(SynkroError::Network)?;
    let binary_path = target_dir.join("yt-dlp");
    let mut file = fs::File::create(&binary_path).map_err(SynkroError::Network)?;
    file.write_all(bytes).map_err(SynkroError::Network)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut perms = file.metadata().map_err(SynkroError::Network)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&binary_path, perms).map_err(SynkroError::Network)?;
    }

    Ok(binary_path)
}

fn find_in_path(binary_name: &str) -> Option<PathBuf> {
    let paths = env::var_os("PATH")?;
    env::split_paths(&paths)
        .map(|path| path.join(binary_name))
        .find(|candidate| candidate.exists() && is_file(candidate))
}

fn is_file(path: &Path) -> bool {
    path.metadata().map(|meta| meta.is_file()).unwrap_or(false)
}
