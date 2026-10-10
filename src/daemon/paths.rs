use std::fs;
use std::path::PathBuf;

use crate::error::{Result, SynkroError};

/// Manages paths for daemon state, socket, PID file, and log files in `~/.synkrophase/`.
#[derive(Debug, Clone)]
pub struct DaemonPaths {
    pub base_dir: PathBuf,
    pub socket_path: PathBuf,
    pub pid_path: PathBuf,
    pub log_path: PathBuf,
}

impl DaemonPaths {
    pub fn default_paths() -> Result<Self> {
        let home = home::home_dir().ok_or_else(|| {
            SynkroError::Config("Could not locate user home directory".to_string())
        })?;
        let base_dir = home.join(".synkrophase");
        Ok(Self {
            socket_path: base_dir.join("synkro.sock"),
            pid_path: base_dir.join("daemon.pid"),
            log_path: base_dir.join("daemon.log"),
            base_dir,
        })
    }

    pub fn ensure_dir_exists(&self) -> Result<()> {
        if !self.base_dir.exists() {
            fs::create_dir_all(&self.base_dir).map_err(|e| {
                SynkroError::Config(format!(
                    "Failed to create daemon directory {:?}: {e}",
                    self.base_dir
                ))
            })?;
        }
        Ok(())
    }

    pub fn write_pid(&self, pid: u32) -> Result<()> {
        self.ensure_dir_exists()?;
        fs::write(&self.pid_path, pid.to_string()).map_err(|e| {
            SynkroError::Config(format!(
                "Failed to write PID file {:?}: {e}",
                self.pid_path
            ))
        })?;
        Ok(())
    }

    pub fn read_pid(&self) -> Result<Option<u32>> {
        if !self.pid_path.exists() {
            return Ok(None);
        }
        let content = fs::read_to_string(&self.pid_path).map_err(|e| {
            SynkroError::Config(format!("Failed to read PID file {:?}: {e}", self.pid_path))
        })?;
        match content.trim().parse::<u32>() {
            Ok(pid) => Ok(Some(pid)),
            Err(_) => Ok(None),
        }
    }

    pub fn clean_pid(&self) {
        let _ = fs::remove_file(&self.pid_path);
    }

    pub fn clean_socket(&self) {
        let _ = fs::remove_file(&self.socket_path);
    }

    pub fn is_running(&self) -> bool {
        if let Ok(Some(_pid)) = self.read_pid() {
            // Check process existence via kill(pid, 0)
            #[cfg(unix)]
            {
                unsafe { libc::kill(_pid as libc::pid_t, 0) == 0 }
            }
            #[cfg(not(unix))]
            {
                true
            }
        } else {
            false
        }
    }
}
