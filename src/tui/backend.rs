use async_trait::async_trait;
use std::sync::Arc;
use uuid::Uuid;

#[cfg(unix)]
use crate::daemon::IpcClient;
#[cfg(unix)]
use crate::daemon::paths::DaemonPaths;
use crate::error::Result;
use crate::protocol::messages::{PlaybackAction, Role};
use crate::session::engine::{EngineCommand, SynkroEngine};

#[async_trait]
pub trait TuiBackend: Send + Sync {
    async fn send_playback_action(&self, action: PlaybackAction) -> Result<()>;
    async fn send_volume(&self, level: Option<u8>) -> Result<()>;
    async fn send_chat(&self, text: String) -> Result<()>;
    async fn transfer_leadership(&self, target: Uuid) -> Result<()>;
    async fn assign_role(&self, target: Uuid, role: Role) -> Result<()>;
    async fn shutdown(&self) -> Result<()>;
    async fn get_invitation_command(&self) -> Result<Option<String>>;
}

pub struct LocalEngineBackend {
    engine: Arc<SynkroEngine>,
}

impl LocalEngineBackend {
    pub fn new(engine: Arc<SynkroEngine>) -> Self {
        Self { engine }
    }

    pub fn engine(&self) -> &SynkroEngine {
        &self.engine
    }
}

#[async_trait]
impl TuiBackend for LocalEngineBackend {
    async fn send_playback_action(&self, action: PlaybackAction) -> Result<()> {
        self.engine
            .send_command(EngineCommand::PlaybackAction(action))
    }

    async fn send_volume(&self, level: Option<u8>) -> Result<()> {
        self.engine.send_command(EngineCommand::SyncVolume(level))
    }

    async fn send_chat(&self, text: String) -> Result<()> {
        self.engine.send_command(EngineCommand::SendChat(text))
    }

    async fn transfer_leadership(&self, target: Uuid) -> Result<()> {
        self.engine
            .send_command(EngineCommand::TransferLeadership(target))
    }

    async fn assign_role(&self, target: Uuid, role: Role) -> Result<()> {
        self.engine.send_command(EngineCommand::AssignRole {
            target,
            new_role: role,
        })
    }

    async fn shutdown(&self) -> Result<()> {
        self.engine.send_command(EngineCommand::Shutdown)
    }

    async fn get_invitation_command(&self) -> Result<Option<String>> {
        let room_code = self.engine.session().room_code().to_string();
        let is_leader = self.engine.session().is_leader();
        let leader_addr = if is_leader {
            if let Ok(ip) = local_ip_address::local_ip() {
                format!(
                    "{ip}:{}",
                    self.engine
                        .socket()
                        .local_addr()
                        .map(|a| a.port())
                        .unwrap_or(0)
                )
            } else {
                "127.0.0.1".to_string()
            }
        } else if let Some(addr) = self.engine.leader_addr() {
            addr.to_string()
        } else {
            "unknown".to_string()
        };
        Ok(Some(format!(
            "synkro join {room_code} --leader-addr {leader_addr}"
        )))
    }
}

#[derive(Default)]
pub struct IpcBackend {
    #[cfg(unix)]
    paths: Option<DaemonPaths>,
}

impl IpcBackend {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(unix)]
    pub fn with_paths(paths: DaemonPaths) -> Self {
        Self { paths: Some(paths) }
    }

    #[cfg(unix)]
    async fn send_ipc(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<Option<serde_json::Value>> {
        if let Some(p) = &self.paths {
            IpcClient::send_command_with_paths(p, method, params).await
        } else {
            IpcClient::send_command(method, params).await
        }
    }
}

#[async_trait]
impl TuiBackend for IpcBackend {
    async fn send_playback_action(&self, action: PlaybackAction) -> Result<()> {
        #[cfg(unix)]
        {
            let (method, params) = match action {
                PlaybackAction::Play => ("play", serde_json::Value::Null),
                PlaybackAction::Pause => ("pause", serde_json::Value::Null),
                PlaybackAction::Seek { target_position_us } => {
                    let sec = (target_position_us as f64) / 1_000_000.0;
                    ("seek", serde_json::json!({ "position_sec": sec }))
                }
                PlaybackAction::NextTrack => ("next", serde_json::Value::Null),
                PlaybackAction::PreviousTrack => ("prev", serde_json::Value::Null),
            };
            let _ = self.send_ipc(method, params).await?;
        }
        #[cfg(not(unix))]
        {
            let _ = action;
        }
        Ok(())
    }

    async fn send_volume(&self, level: Option<u8>) -> Result<()> {
        #[cfg(unix)]
        {
            let params = match level {
                Some(lvl) => serde_json::json!({ "level": lvl }),
                None => serde_json::Value::Null,
            };
            let _ = self.send_ipc("volume", params).await?;
        }
        #[cfg(not(unix))]
        {
            let _ = level;
        }
        Ok(())
    }

    async fn send_chat(&self, text: String) -> Result<()> {
        #[cfg(unix)]
        {
            let _ = self
                .send_ipc("chat", serde_json::json!({ "message": text }))
                .await?;
        }
        #[cfg(not(unix))]
        {
            let _ = text;
        }
        Ok(())
    }

    async fn transfer_leadership(&self, target: Uuid) -> Result<()> {
        #[cfg(unix)]
        {
            let _ = self
                .send_ipc(
                    "transfer",
                    serde_json::json!({ "target": target.to_string() }),
                )
                .await?;
        }
        #[cfg(not(unix))]
        {
            let _ = target;
        }
        Ok(())
    }

    async fn assign_role(&self, target: Uuid, role: Role) -> Result<()> {
        #[cfg(unix)]
        {
            let role_str = match role {
                Role::Leader => "leader",
                Role::Moderator => "moderator",
                Role::Listener => "listener",
            };
            let _ = self
                .send_ipc(
                    "assign_role",
                    serde_json::json!({
                        "target": target.to_string(),
                        "role": role_str,
                    }),
                )
                .await?;
        }
        #[cfg(not(unix))]
        {
            let _ = (target, role);
        }
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        #[cfg(unix)]
        {
            let _ = self.send_ipc("shutdown", serde_json::Value::Null).await?;
        }
        Ok(())
    }

    async fn get_invitation_command(&self) -> Result<Option<String>> {
        #[cfg(unix)]
        {
            if let Ok(Some(res)) = self.send_ipc("invite", serde_json::Value::Null).await
                && let Some(cmd) = res.get("join_command").and_then(|v| v.as_str())
            {
                return Ok(Some(cmd.to_string()));
            }
        }
        Ok(None)
    }
}
