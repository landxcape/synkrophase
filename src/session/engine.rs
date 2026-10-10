use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::cli::args::DEFAULT_LEAD_TIME_US;
use crate::clock::sync::ClockSync;
use crate::controller::MediaController;
use crate::error::Result;
use crate::protocol::messages::{
    Envelope, Message, PlaybackAction, PlaybackIntent, Role, serialize,
};
use crate::session::SessionState;
use crate::sync::scheduler::IntentScheduler;
use crate::tui::AppEvent;

#[derive(Debug, Clone)]
pub enum EngineCommand {
    PlaybackAction(PlaybackAction),
    SendChat(String),
    SyncVolume(Option<u8>),
    TransferLeadership(Uuid),
    AssignRole { target: Uuid, new_role: Role },
    Shutdown,
}

pub struct SynkroEngine {
    session: Arc<SessionState>,
    controller: Arc<dyn MediaController>,
    socket: Arc<UdpSocket>,
    leader_addr: Option<SocketAddr>,
    clock: Arc<ClockSync>,
    scheduler: Arc<IntentScheduler>,
    command_tx: mpsc::UnboundedSender<EngineCommand>,
    event_tx: mpsc::UnboundedSender<AppEvent>,
    abort_handles: std::sync::Mutex<Vec<tokio::task::AbortHandle>>,
}

impl SynkroEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session: Arc<SessionState>,
        controller: Arc<dyn MediaController>,
        socket: Arc<UdpSocket>,
        leader_addr: Option<SocketAddr>,
        clock: Arc<ClockSync>,
        scheduler: Arc<IntentScheduler>,
        event_tx: mpsc::UnboundedSender<AppEvent>,
        abort_handles: Vec<tokio::task::AbortHandle>,
    ) -> (Self, mpsc::UnboundedReceiver<EngineCommand>) {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let engine = Self {
            session,
            controller,
            socket,
            leader_addr,
            clock,
            scheduler,
            command_tx,
            event_tx,
            abort_handles: std::sync::Mutex::new(abort_handles),
        };
        (engine, command_rx)
    }

    pub fn session(&self) -> Arc<SessionState> {
        Arc::clone(&self.session)
    }

    pub fn controller(&self) -> Arc<dyn MediaController> {
        Arc::clone(&self.controller)
    }

    pub fn clock(&self) -> Arc<ClockSync> {
        Arc::clone(&self.clock)
    }

    pub fn scheduler(&self) -> Arc<IntentScheduler> {
        Arc::clone(&self.scheduler)
    }

    pub fn leader_addr(&self) -> Option<SocketAddr> {
        self.leader_addr
    }

    pub fn socket(&self) -> Arc<UdpSocket> {
        Arc::clone(&self.socket)
    }

    pub fn event_sender(&self) -> mpsc::UnboundedSender<AppEvent> {
        self.event_tx.clone()
    }

    pub fn send_command(&self, cmd: EngineCommand) -> Result<()> {
        let _ = self.command_tx.send(cmd);
        Ok(())
    }

    pub async fn execute_command(&self, cmd: EngineCommand) -> Result<()> {
        match cmd {
            EngineCommand::PlaybackAction(action) => {
                self.handle_playback_action(action).await?;
            }
            EngineCommand::SendChat(text) => {
                self.handle_send_chat(text).await?;
            }
            EngineCommand::SyncVolume(vol_opt) => {
                self.handle_sync_volume(vol_opt).await?;
            }
            EngineCommand::TransferLeadership(target_id) => {
                self.handle_transfer_leadership(target_id).await?;
            }
            EngineCommand::AssignRole { target, new_role } => {
                self.handle_assign_role(target, new_role).await?;
            }
            EngineCommand::Shutdown => {}
        }
        Ok(())
    }

    async fn handle_assign_role(&self, target: Uuid, new_role: Role) -> Result<()> {
        let envelope = Envelope {
            sender: self.session.self_id(),
            payload: Message::AssignRole { target, new_role },
        };

        if self.session.is_leader() {
            if let Ok(bytes) = serialize(&envelope) {
                for addr in self.session.peer_socket_addrs() {
                    let _ = self.socket.send_to(&bytes, addr).await;
                }
            }
            if new_role == Role::Leader {
                self.session.demote_from_leader(target);
            } else {
                self.session.apply_role_assignment(target, new_role);
            }
            let target_name = self.session.display_name(&target);
            let _ = self.event_tx.send(AppEvent::Log {
                source: "System".to_string(),
                text: format!("Assigned role [{:?}] to {}", new_role, target_name),
            });
        } else if let Some(addr) = self.leader_addr {
            if let Ok(bytes) = serialize(&envelope) {
                let _ = self.socket.send_to(&bytes, addr).await;
            }
            let target_name = self.session.display_name(&target);
            let _ = self.event_tx.send(AppEvent::Log {
                source: "System".to_string(),
                text: format!("Requested role [{:?}] for {}", new_role, target_name),
            });
        }
        Ok(())
    }

    async fn handle_playback_action(&self, action: PlaybackAction) -> Result<()> {
        let playback_state = self
            .controller
            .get_playback_state()
            .await
            .unwrap_or_default();
        let self_id = self.session.self_id();

        if self.session.is_leader() {
            let now = self.clock.reference_now();
            let target_ref_time = now + DEFAULT_LEAD_TIME_US;
            let title = playback_state.metadata.map(|m| m.title);
            let pos = match action {
                PlaybackAction::Seek { target_position_us } => target_position_us,
                _ => playback_state.position_us,
            };
            let intent = PlaybackIntent {
                action,
                target_ref_time,
                position_us: pos,
                track_title: title,
            };

            let envelope = Envelope {
                sender: self_id,
                payload: Message::Intent(intent.clone()),
            };
            if let Ok(bytes) = serialize(&envelope) {
                for addr in self.session.peer_socket_addrs() {
                    let _ = self.socket.send_to(&bytes, addr).await;
                }
            }
            let _ = self.scheduler.execute_intent(&intent).await;
        } else if let Some(addr) = self.leader_addr {
            let pos = match action {
                PlaybackAction::Seek { target_position_us } => target_position_us,
                _ => playback_state.position_us,
            };
            let intent = PlaybackIntent {
                action,
                target_ref_time: 0,
                position_us: pos,
                track_title: None,
            };
            let envelope = Envelope {
                sender: self_id,
                payload: Message::Intent(intent),
            };
            if let Ok(bytes) = serialize(&envelope) {
                let _ = self.socket.send_to(&bytes, addr).await;
            }
        }
        Ok(())
    }

    async fn handle_send_chat(&self, text: String) -> Result<()> {
        let self_id = self.session.self_id();
        let device_name = self.session.self_info().name;

        if self.session.is_leader() {
            let envelope = Envelope {
                sender: self_id,
                payload: Message::ChatBroadcast {
                    sender: self_id,
                    display_name: device_name.clone(),
                    text: text.clone(),
                },
            };
            if let Ok(bytes) = serialize(&envelope) {
                for addr in self.session.peer_socket_addrs() {
                    let _ = self.socket.send_to(&bytes, addr).await;
                }
            }
            let _ = self.event_tx.send(AppEvent::Log {
                source: device_name,
                text,
            });
        } else if let Some(addr) = self.leader_addr {
            let envelope = Envelope {
                sender: self_id,
                payload: Message::Chat {
                    sender: self_id,
                    name: device_name.clone(),
                    text: text.clone(),
                },
            };
            if let Ok(bytes) = serialize(&envelope) {
                let _ = self.socket.send_to(&bytes, addr).await;
            }
            let _ = self.event_tx.send(AppEvent::Log {
                source: format!("{} (You)", device_name),
                text,
            });
        }
        Ok(())
    }

    async fn handle_sync_volume(&self, target_vol: Option<u8>) -> Result<()> {
        let vol = match target_vol {
            Some(v) => v,
            None => self.controller.get_volume().await.unwrap_or(50),
        };
        let _ = self.controller.set_volume(vol).await;
        let _ = self.event_tx.send(AppEvent::Log {
            source: "System".to_string(),
            text: format!("Volume synced to {}%", vol),
        });

        let envelope = Envelope {
            sender: self.session.self_id(),
            payload: Message::SetVolume {
                volume: vol,
                actor: self.session.self_id(),
            },
        };
        if let Ok(bytes) = serialize(&envelope) {
            if self.session.is_leader() {
                for addr in self.session.peer_socket_addrs() {
                    let _ = self.socket.send_to(&bytes, addr).await;
                }
            } else if let Some(addr) = self.leader_addr {
                let _ = self.socket.send_to(&bytes, addr).await;
            }
        }
        Ok(())
    }

    async fn handle_transfer_leadership(&self, new_leader: Uuid) -> Result<()> {
        let envelope = Envelope {
            sender: self.session.self_id(),
            payload: Message::TransferLeadership { to: new_leader },
        };

        if self.session.is_leader() {
            if let Ok(bytes) = serialize(&envelope) {
                for addr in self.session.peer_socket_addrs() {
                    let _ = self.socket.send_to(&bytes, addr).await;
                }
            }
            self.session.demote_from_leader(new_leader);
            let target_name = self.session.display_name(&new_leader);
            let _ = self.event_tx.send(AppEvent::Log {
                source: "System".to_string(),
                text: format!("Leadership transferred to {}", target_name),
            });
            let elected_env = Envelope {
                sender: self.session.self_id(),
                payload: Message::LeaderElected(new_leader),
            };
            if let Ok(bytes) = serialize(&elected_env) {
                for addr in self.session.peer_socket_addrs() {
                    let _ = self.socket.send_to(&bytes, addr).await;
                }
            }
        } else if let Some(addr) = self.leader_addr {
            if let Ok(bytes) = serialize(&envelope) {
                let _ = self.socket.send_to(&bytes, addr).await;
            }
            let target_name = self.session.display_name(&new_leader);
            let _ = self.event_tx.send(AppEvent::Log {
                source: "System".to_string(),
                text: format!("Requested leadership transfer to {}", target_name),
            });
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        // Send peer left notice if connected
        let leave_env = Envelope {
            sender: self.session.self_id(),
            payload: Message::PeerLeft(self.session.self_id()),
        };
        if let Ok(bytes) = serialize(&leave_env) {
            for addr in self.session.peer_socket_addrs() {
                let _ = self.socket.send_to(&bytes, addr).await;
            }
            if let Some(leader) = self.leader_addr {
                let _ = self.socket.send_to(&bytes, leader).await;
            }
        }

        let mut handles = self.abort_handles.lock().unwrap();
        for handle in handles.drain(..) {
            handle.abort();
        }
        Ok(())
    }
}
