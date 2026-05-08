use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use synkrophase::config::DeviceConfig;
use synkrophase::stream::bootstrap::{ensure_ytdlp, install_ytdlp_binary};
use synkrophase::{error::SynkroError, stream::bootstrap};
use uuid::Uuid;

fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("synkro-{name}-{nonce}"));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn config_with_dir(data_dir: PathBuf) -> DeviceConfig {
    DeviceConfig {
        device_id: Uuid::nil(),
        data_dir,
        ytdlp_path: None,
    }
}

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct PathGuard(Option<std::ffi::OsString>);

impl PathGuard {
    fn clear() -> Self {
        let original = std::env::var_os("PATH");
        unsafe { std::env::remove_var("PATH") };
        Self(original)
    }
}

impl Drop for PathGuard {
    fn drop(&mut self) {
        match self.0.take() {
            Some(value) => unsafe { std::env::set_var("PATH", value) },
            None => unsafe { std::env::remove_var("PATH") },
        }
    }
}

#[test]
fn ensure_ytdlp_prefers_configured_path() {
    let data_dir = temp_dir("configured");
    let binary = data_dir.join("custom-yt-dlp");
    fs::write(&binary, "#!/bin/sh\n").unwrap();

    let config = DeviceConfig {
        device_id: Uuid::nil(),
        data_dir,
        ytdlp_path: Some(binary.clone()),
    };

    let resolved = ensure_ytdlp(&config).unwrap();
    assert_eq!(resolved, binary);
}

#[test]
fn ensure_ytdlp_finds_workspace_binary() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _guard = PathGuard::clear();
    let data_dir = temp_dir("workspace");
    let bin_dir = data_dir.join("bin");
    fs::create_dir_all(&bin_dir).unwrap();
    let binary = bin_dir.join("yt-dlp");
    fs::write(&binary, "#!/bin/sh\n").unwrap();

    let resolved = ensure_ytdlp(&config_with_dir(data_dir)).unwrap();
    assert_eq!(resolved, binary);
}

#[test]
fn ensure_ytdlp_errors_when_missing() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _guard = PathGuard::clear();
    let data_dir = temp_dir("missing");

    let err = ensure_ytdlp(&config_with_dir(data_dir)).unwrap_err();
    match err {
        SynkroError::YtdlpMissing => {}
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn parse_expiry_from_googlevideo_urls() {
    let expires_at = bootstrap::parse_expiry(
        "https://rr2---sn.googlevideo.com/videoplayback?expire=1712345678&foo=bar",
    );
    assert_eq!(expires_at, Some(1_712_345_678));
}

#[test]
fn install_ytdlp_binary_writes_executable_file() {
    let data_dir = temp_dir("install");
    let installed = install_ytdlp_binary(&data_dir, b"#!/bin/sh\necho ok\n").unwrap();

    assert_eq!(installed, data_dir.join("yt-dlp"));
    assert!(installed.exists());
}
