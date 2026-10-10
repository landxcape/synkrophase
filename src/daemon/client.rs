#[cfg(unix)]
use std::time::Duration;
#[cfg(unix)]
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
#[cfg(unix)]
use tokio::net::UnixStream;
#[cfg(unix)]
use tokio::time::timeout;

#[cfg(unix)]
use crate::daemon::paths::DaemonPaths;
#[cfg(unix)]
use crate::daemon::protocol::{IpcRequest, IpcResponse};
use crate::error::Result;
#[cfg(unix)]
use crate::error::SynkroError;

/// Client helper for dispatching JSON-RPC requests to the active daemon via `synkro.sock`.
pub struct IpcClient;

impl IpcClient {
    /// Attempts to send an IPC command to the daemon. Returns `None` if the daemon is not running.
    pub async fn send_command(
        _method: &str,
        _params: serde_json::Value,
    ) -> Result<Option<serde_json::Value>> {
        #[cfg(not(unix))]
        {
            return Ok(None);
        }

        #[cfg(unix)]
        {
            let paths = match DaemonPaths::default_paths() {
                Ok(p) => p,
                Err(_) => return Ok(None),
            };

            if !paths.is_running() || !paths.socket_path.exists() {
                return Ok(None);
            }

            let stream = match UnixStream::connect(&paths.socket_path).await {
                Ok(s) => s,
                Err(_) => return Ok(None),
            };

            let req = IpcRequest {
                id: Some(1),
                method: _method.to_string(),
                params: _params,
            };

            let mut msg = serde_json::to_string(&req)
                .map_err(|e| SynkroError::Serialization(e.to_string()))?;
            msg.push('\n');

            let (reader, mut writer) = stream.into_split();
            writer
                .write_all(msg.as_bytes())
                .await
                .map_err(|e| SynkroError::Network(std::io::Error::other(e.to_string())))?;

            let mut lines = BufReader::new(reader).lines();
            let line = match timeout(Duration::from_secs(2), lines.next_line()).await {
                Ok(Ok(Some(l))) => l,
                _ => return Ok(None),
            };

            let resp: IpcResponse = serde_json::from_str(&line)
                .map_err(|e| SynkroError::Deserialization(e.to_string()))?;

            if let Some(err) = resp.error {
                return Err(SynkroError::PermissionDenied(err.message));
            }

            Ok(resp.result)
        }
    }
}
