use std::sync::Arc;
#[cfg(unix)]
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::daemon::paths::DaemonPaths;
use crate::daemon::protocol::{DaemonStatusSnapshot, IpcEvent, IpcRequest, IpcResponse};
use crate::error::Result;
#[cfg(unix)]
use crate::error::SynkroError;
use crate::protocol::messages::{PlaybackAction, Role};
use crate::session::engine::{EngineCommand, SynkroEngine};

pub struct IpcServer {
    #[allow(dead_code)]
    paths: DaemonPaths,
    #[allow(dead_code)]
    engine: Arc<SynkroEngine>,
    event_tx: broadcast::Sender<IpcEvent>,
    latest_drift: Arc<std::sync::Mutex<(i64, u8, String)>>,
}

#[allow(dead_code)]
impl IpcServer {
    pub fn new(paths: DaemonPaths, engine: Arc<SynkroEngine>) -> (Self, broadcast::Sender<IpcEvent>) {
        let (event_tx, _) = broadcast::channel(128);
        let latest_drift = Arc::new(std::sync::Mutex::new((
            0,
            1,
            "Locked (<50ms)".to_string(),
        )));
        let server = Self {
            paths,
            engine,
            event_tx: event_tx.clone(),
            latest_drift,
        };
        (server, event_tx)
    }

    pub fn record_drift_update(&self, offset_us: i64, zone: u8, status: String) {
        if let Ok(mut guard) = self.latest_drift.lock() {
            *guard = (offset_us, zone, status.clone());
        }
        let _ = self.event_tx.send(IpcEvent {
            event: "drift_update".to_string(),
            data: serde_json::json!({
                "offset_us": offset_us,
                "zone": zone,
                "status": status,
            }),
        });
    }

    pub async fn run_server(self: Arc<Self>) -> Result<()> {
        #[cfg(unix)]
        {
            self.paths.ensure_dir_exists()?;
            self.paths.clean_socket();

            let listener = UnixListener::bind(&self.paths.socket_path).map_err(|e| {
                SynkroError::Network(std::io::Error::new(
                    std::io::ErrorKind::AddrInUse,
                    format!("Failed to bind domain socket {:?}: {e}", self.paths.socket_path),
                ))
            })?;

            tracing::info!(socket = ?self.paths.socket_path, "IPC domain socket listening");

            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let server = Arc::clone(&self);
                        tokio::spawn(async move {
                            let _ = server.handle_client(stream).await;
                        });
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "IPC accept error");
                        break;
                    }
                }
            }

            self.paths.clean_socket();
        }

        #[cfg(not(unix))]
        {
            tracing::info!("IPC domain socket is currently only supported on Unix systems");
            std::future::pending::<()>().await;
        }

        Ok(())
    }

    #[cfg(unix)]
    async fn handle_client(&self, stream: UnixStream) -> Result<()> {
        let (reader, mut writer) = stream.into_split();
        let mut lines = BufReader::new(reader).lines();
        let mut event_rx: Option<broadcast::Receiver<IpcEvent>> = None;

        loop {
            tokio::select! {
                line_res = lines.next_line() => {
                    match line_res {
                        Ok(Some(line)) => {
                            let line = line.trim();
                            if line.is_empty() {
                                continue;
                            }
                            let req: IpcRequest = match serde_json::from_str(line) {
                                Ok(r) => r,
                                Err(err) => {
                                    let resp = IpcResponse::err(None, 400, format!("Invalid JSON request: {err}"));
                                    let mut out = serde_json::to_string(&resp).unwrap_or_default();
                                    out.push('\n');
                                    let _ = writer.write_all(out.as_bytes()).await;
                                    continue;
                                }
                            };

                            if req.method == "subscribe" {
                                event_rx = Some(self.event_tx.subscribe());
                                let resp = IpcResponse::ok(req.id, serde_json::json!({"subscribed": true}));
                                let mut out = serde_json::to_string(&resp).unwrap_or_default();
                                out.push('\n');
                                let _ = writer.write_all(out.as_bytes()).await;
                                continue;
                            }

                            let resp = self.dispatch_method(&req).await;
                            let mut out = serde_json::to_string(&resp).unwrap_or_default();
                            out.push('\n');
                            if writer.write_all(out.as_bytes()).await.is_err() {
                                break;
                            }
                        }
                        Ok(None) => break, // Client disconnected
                        Err(_) => break,
                    }
                }
                event_res = async {
                    match &mut event_rx {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Ok(event) = event_res {
                        let mut out = serde_json::to_string(&event).unwrap_or_default();
                        out.push('\n');
                        if writer.write_all(out.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn dispatch_method(&self, req: &IpcRequest) -> IpcResponse {
        match req.method.as_str() {
            "status" => {
                let snapshot = self.collect_status_snapshot().await;
                IpcResponse::ok(req.id, serde_json::to_value(snapshot).unwrap_or_default())
            }
            "play" | "resume" => {
                if let Err(e) = self.engine.send_command(EngineCommand::PlaybackAction(PlaybackAction::Play)) {
                    return IpcResponse::err(req.id, 500, e.to_string());
                }
                IpcResponse::ok(req.id, serde_json::json!({"success": true}))
            }
            "pause" => {
                if let Err(e) = self.engine.send_command(EngineCommand::PlaybackAction(PlaybackAction::Pause)) {
                    return IpcResponse::err(req.id, 500, e.to_string());
                }
                IpcResponse::ok(req.id, serde_json::json!({"success": true}))
            }
            "next" => {
                if let Err(e) = self.engine.send_command(EngineCommand::PlaybackAction(PlaybackAction::NextTrack)) {
                    return IpcResponse::err(req.id, 500, e.to_string());
                }
                IpcResponse::ok(req.id, serde_json::json!({"success": true}))
            }
            "prev" => {
                if let Err(e) = self.engine.send_command(EngineCommand::PlaybackAction(PlaybackAction::PreviousTrack)) {
                    return IpcResponse::err(req.id, 500, e.to_string());
                }
                IpcResponse::ok(req.id, serde_json::json!({"success": true}))
            }
            "seek" => {
                let pos_sec = req.params.get("position_sec")
                    .and_then(|v| v.as_f64())
                    .or_else(|| req.params.get("position_ms").and_then(|v| v.as_f64()).map(|ms| ms / 1000.0));
                let Some(pos_sec) = pos_sec else {
                    return IpcResponse::err(req.id, 400, "Missing 'position_sec' parameter");
                };
                let pos_us = (pos_sec * 1_000_000.0) as i64;
                if let Err(e) = self.engine.send_command(EngineCommand::PlaybackAction(PlaybackAction::Seek { target_position_us: pos_us })) {
                    return IpcResponse::err(req.id, 500, e.to_string());
                }
                IpcResponse::ok(req.id, serde_json::json!({"success": true, "position_sec": pos_sec}))
            }
            "volume" => {
                let level = req.params.get("level").and_then(|v| v.as_u64()).map(|l| l as u8);
                if let Err(e) = self.engine.send_command(EngineCommand::SyncVolume(level)) {
                    return IpcResponse::err(req.id, 500, e.to_string());
                }
                IpcResponse::ok(req.id, serde_json::json!({"success": true}))
            }
            "chat" => {
                let Some(msg) = req.params.get("message").and_then(|v| v.as_str()) else {
                    return IpcResponse::err(req.id, 400, "Missing 'message' parameter");
                };
                if let Err(e) = self.engine.send_command(EngineCommand::SendChat(msg.to_string())) {
                    return IpcResponse::err(req.id, 500, e.to_string());
                }
                IpcResponse::ok(req.id, serde_json::json!({"success": true}))
            }
            "transfer" => {
                let Some(target_str) = req.params.get("target").and_then(|v| v.as_str()) else {
                    return IpcResponse::err(req.id, 400, "Missing 'target' UUID parameter");
                };
                let target_uuid = match Uuid::parse_str(target_str) {
                    Ok(u) => u,
                    Err(e) => return IpcResponse::err(req.id, 400, format!("Invalid UUID: {e}")),
                };
                if let Err(e) = self.engine.send_command(EngineCommand::TransferLeadership(target_uuid)) {
                    return IpcResponse::err(req.id, 500, e.to_string());
                }
                IpcResponse::ok(req.id, serde_json::json!({"success": true}))
            }
            "assign_role" => {
                let target_str = req.params.get("target").and_then(|v| v.as_str());
                let role_str = req.params.get("role").and_then(|v| v.as_str());
                let (Some(target_str), Some(role_str)) = (target_str, role_str) else {
                    return IpcResponse::err(req.id, 400, "Missing 'target' or 'role' parameters");
                };
                let target_uuid = match Uuid::parse_str(target_str) {
                    Ok(u) => u,
                    Err(e) => return IpcResponse::err(req.id, 400, format!("Invalid UUID: {e}")),
                };
                let new_role = match role_str.to_lowercase().as_str() {
                    "moderator" => Role::Moderator,
                    "listener" => Role::Listener,
                    _ => return IpcResponse::err(req.id, 400, "Role must be 'Moderator' or 'Listener'"),
                };
                if let Err(e) = self.engine.send_command(EngineCommand::AssignRole { target: target_uuid, new_role }) {
                    return IpcResponse::err(req.id, 500, e.to_string());
                }
                IpcResponse::ok(req.id, serde_json::json!({"success": true}))
            }
            "invite" | "share" => {
                let room_code = self.engine.session().room_code().to_string();
                let is_leader = self.engine.session().is_leader();
                let leader_addr = if is_leader {
                    if let Ok(ip) = local_ip_address::local_ip() {
                        format!("{ip}:{}", self.engine.socket().local_addr().map(|a| a.port()).unwrap_or(0))
                    } else {
                        "127.0.0.1".to_string()
                    }
                } else if let Some(addr) = self.engine.leader_addr() {
                    addr.to_string()
                } else {
                    "unknown".to_string()
                };
                let join_command = format!("synkro join {room_code} --leader-addr {leader_addr}");
                IpcResponse::ok(req.id, serde_json::json!({
                    "room_code": room_code,
                    "leader_addr": leader_addr,
                    "join_command": join_command,
                }))
            }
            "shutdown" => {
                let _ = self.engine.send_command(EngineCommand::Shutdown);
                IpcResponse::ok(req.id, serde_json::json!({"shutting_down": true}))
            }
            _ => IpcResponse::err(req.id, 404, format!("Unknown method '{}'", req.method)),
        }
    }

    async fn collect_status_snapshot(&self) -> DaemonStatusSnapshot {
        let session = self.engine.session();
        let controller = self.engine.controller();
        let playback = controller.get_playback_state().await.unwrap_or_default();
        let (drift_offset_us, _, drift_status) = self.latest_drift.lock().unwrap().clone();

        let track_title = playback.metadata.as_ref().map(|m| m.title.clone());
        let track_artist = playback.metadata.as_ref().and_then(|m| m.artist.clone());
        let track_album = playback.metadata.as_ref().and_then(|m| m.album.clone());
        let duration_sec = playback
            .metadata
            .as_ref()
            .and_then(|m| m.duration_us)
            .map(|us| (us as f64) / 1_000_000.0);

        DaemonStatusSnapshot {
            room_code: session.room_code().to_string(),
            role: session.role(),
            self_id: session.self_id(),
            device_name: session.display_name(&session.self_id()),
            leader_id: Some(session.leader_id()),
            leader_addr: self.engine.leader_addr().map(|a| a.to_string()),
            is_playing: playback.is_playing,
            position_sec: (playback.position_us as f64) / 1_000_000.0,
            track_title,
            track_artist,
            track_album,
            duration_sec,
            clock_offset_us: session.clock_offset(),
            drift_offset_us,
            drift_status,
            peers: session.peer_infos(),
        }
    }
}
