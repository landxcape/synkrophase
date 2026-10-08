use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

use crate::clock::sync::ClockSync;
use crate::config::SyncConfig;
use crate::error::{Result, SynkroError};
use crate::protocol::messages::{Envelope, Message, PeerInfo, deserialize, serialize};
use crate::session::{FollowerSyncRuntime, SessionState};
use crate::sync::controller::{ClockSource, PlaybackControl};
use crate::sync::evaluator::DriftEvaluator;
use crate::sync::scheduler::IntentScheduler;

use rustyline_async::SharedWriter;
use crate::protocol::messages::Role;

pub struct SessionMessageRuntime {
    session: Arc<SessionState>,
    follower_sync: Option<Arc<FollowerSyncRuntime>>,
    playback: Option<Arc<dyn PlaybackControl>>,
    scheduler: Option<Arc<IntentScheduler>>,
    drift_evaluator: Option<Arc<DriftEvaluator>>,
    controller: Option<Arc<dyn crate::controller::MediaController>>,
    clock: Option<Arc<ClockSync>>,
    stdout: Option<SharedWriter>,
    name: String,
}

pub fn print_event(stdout: Option<&SharedWriter>, msg: &str) {
    if let Some(out) = stdout {
        use std::io::Write;
        let mut out = out.clone();
        let _ = writeln!(out, "{}", msg);
    } else {
        println!("{}", msg);
    }
}

impl SessionMessageRuntime {
    pub fn new(session: Arc<SessionState>, stdout: Option<SharedWriter>, name: String) -> Self {
        Self {
            session,
            follower_sync: None,
            playback: None,
            scheduler: None,
            drift_evaluator: None,
            controller: None,
            clock: None,
            stdout,
            name,
        }
    }

    pub fn with_follower_sync(mut self, follower_sync: Arc<FollowerSyncRuntime>) -> Self {
        self.follower_sync = Some(follower_sync);
        self
    }

    pub fn with_playback(mut self, playback: Arc<dyn PlaybackControl>) -> Self {
        self.playback = Some(playback);
        self
    }

    pub fn with_scheduler(mut self, scheduler: Arc<IntentScheduler>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    pub fn with_drift_evaluator(mut self, evaluator: Arc<DriftEvaluator>) -> Self {
        self.drift_evaluator = Some(evaluator);
        self
    }

    pub fn with_controller(mut self, controller: Arc<dyn crate::controller::MediaController>) -> Self {
        self.controller = Some(controller);
        self
    }

    pub fn with_clock(mut self, clock: Arc<ClockSync>) -> Self {
        self.clock = Some(clock);
        self
    }

    fn self_peer_info(&self) -> PeerInfo {
        PeerInfo {
            device_id: self.session.self_id(),
            name: self.name.clone(),
            clock_offset_us: 0,
            last_seen: 0,
            role: self.session.role(),
        }
    }

    fn peer_info_for(sender: Uuid) -> PeerInfo {
        PeerInfo {
            device_id: sender,
            name: "Unknown".into(),
            clock_offset_us: 0,
            last_seen: 0,
            role: Role::Listener,
        }
    }

    pub fn process_message(&self, message: Message) -> Result<()> {
        match self.session.apply_message(message.clone()) {
            Ok(()) => {}
            Err(SynkroError::StaleQueue { .. }) => {}
            Err(err) => return Err(err),
        }

        if let Some(follower_sync) = &self.follower_sync {
            follower_sync.ingest_message(&self.session, &message);
        }
        Ok(())
    }

    pub fn process_envelope(&self, envelope: Envelope) -> Result<()> {
        self.process_message(envelope.payload)
    }

    async fn send_message(
        socket: &UdpSocket,
        sender: Uuid,
        addr: SocketAddr,
        message: Message,
    ) -> Result<()> {
        let envelope = Envelope {
            sender,
            payload: message,
        };
        let bytes = serialize(&envelope)?;
        socket.send_to(&bytes, addr).await?;
        Ok(())
    }

    async fn send_to_peers(&self, socket: &UdpSocket, message: Message) -> Result<()> {
        let envelope = Envelope {
            sender: self.session.self_id(),
            payload: message,
        };
        let bytes = serialize(&envelope)?;
        for addr in self.session.peer_socket_addrs() {
            let _ = socket.send_to(&bytes, addr).await;
        }
        Ok(())
    }

    async fn handle_incoming(
        &self,
        socket: &UdpSocket,
        src: SocketAddr,
        envelope: Envelope,
    ) -> Result<()> {
        if envelope.sender == self.session.self_id() {
            // Allow control messages from same device (CLI use case)
            let allowed = matches!(
                envelope.payload,
                Message::Pause { .. }
                    | Message::Resume { .. }
                    | Message::Play { .. }
                    | Message::QueueProposal(_)
                    | Message::Chat { .. }
            );
            if !allowed {
                return Ok(());
            }
        }

        let sender_role = self.session.get_peer_role(&envelope.sender);

        match envelope.payload {
            Message::JoinRequest { room_code, name } => {
                if room_code != self.session.room_code() {
                    print_event(
                        self.stdout.as_ref(),
                        &format!(
                            "[Warning] Rejected join from {} due to room code mismatch ({} != {})",
                            name,
                            room_code,
                            self.session.room_code()
                        ),
                    );
                    return Ok(());
                }

                // Assign Role: First joiner is Moderator, others are Listeners.
                let assigned_role = if self.session.peer_ids().is_empty() {
                    Role::Moderator
                } else {
                    Role::Listener
                };

                let peer_info = PeerInfo {
                    device_id: envelope.sender,
                    name: name.clone(),
                    clock_offset_us: 0,
                    last_seen: 0,
                    role: assigned_role,
                };

                self.session.record_peer_heartbeat(peer_info.clone(), src);

                if !self.session.is_leader() {
                    return Ok(());
                }

                if self.session.role() >= Role::Moderator {
                    print_event(
                        self.stdout.as_ref(),
                        &format!(
                            "[System] Peer joined: {} (Role: {:?})",
                            self.session.display_name(&envelope.sender),
                            assigned_role
                        ),
                    );
                }

                let current_anchor = self.session.current_anchor.read().unwrap().clone();

                let mut peer_list = self.session.snapshot().peer_list;
                peer_list.push(self.self_peer_info());
                peer_list.sort_by_key(|peer| peer.device_id);

                let queue_state = self.session.queue_snapshot();
                Self::send_message(
                    socket,
                    self.session.self_id(),
                    src,
                    Message::JoinAccepted {
                        peer_list: peer_list.clone(),
                        queue_state: queue_state.clone(),
                        assigned_role,
                        current_anchor,
                    },
                )
                .await?;

                // Notify other peers about the new arrival
                let notification_msg = format!("{} joined the room.", name);
                let notification_env = Envelope {
                    sender: self.session.self_id(),
                    payload: Message::Notification {
                        text: notification_msg,
                    },
                };
                if let Ok(bytes) = serialize(&notification_env) {
                    for (peer_id, entry) in self.session.all_alive_peers() {
                        // Don't send notification to the person who just joined (they got JoinAccepted)
                        // and don't send to ourselves.
                        if peer_id != envelope.sender && peer_id != self.session.self_id() {
                            let _ = socket.send_to(&bytes, entry.addr).await;
                        }
                    }
                }
                Ok(())
            }
            Message::JoinAccepted {
                peer_list,
                queue_state,
                assigned_role,
                current_anchor,
            } => {
                print_event(self.stdout.as_ref(), "[System] Joined room successfully.");
                self.session.accept_join_accepted(
                    envelope.sender,
                    src,
                    peer_list,
                    queue_state,
                    assigned_role,
                );
                if let Some(anchor) = current_anchor {
                    self.session.accept_sync_anchor(anchor.clone());
                    if let Some(follower_sync) = &self.follower_sync {
                        follower_sync.ingest_message(&self.session, &Message::SyncAnchor(anchor));
                    }
                }
                Ok(())
            }
            Message::PeerJoined(_) => {
                // Deprecated in favor of Notification broadcast
                Ok(())
            }
            Message::PeerLeft(peer_id) => {
                if self.session.is_leader() {
                    let peer_name = self.session.display_name(&peer_id);
                    if self.session.role() >= Role::Moderator {
                        print_event(
                            self.stdout.as_ref(),
                            &format!("[System] Peer left: {}", peer_name),
                        );
                    }
                }
                self.session.remove_peer(&peer_id);
                Ok(())
            }
            Message::Heartbeat {
                room_code,
                info,
            } => {
                if room_code != self.session.room_code() {
                    return Ok(());
                }
                self.session.record_peer_heartbeat(info.clone(), src);
                if info.role == Role::Leader {
                    self.session.set_leader_id(envelope.sender);
                }
                Ok(())
            }
            Message::LeaderElected(leader_id) => {
                self.session.set_leader_id(leader_id);
                if self.session.role() >= Role::Moderator {
                    if leader_id == self.session.self_id() {
                        print_event(self.stdout.as_ref(), "[System] You have been elected as the Leader!");
                    } else {
                        let name = self.session.display_name(&leader_id);
                        print_event(self.stdout.as_ref(), &format!("[System] {} is now the Leader.", name));
                    }
                }
                Ok(())
            }
            Message::QueueProposal(command) => {
                if !self.session.is_leader() {
                    return Ok(());
                }

                let role = sender_role.unwrap_or(Role::Listener);
                if role < Role::Moderator {
                    let _ = Self::send_message(
                        socket,
                        self.session.self_id(),
                        src,
                        Message::Notification {
                            text: "Permission Denied: Only Moderators can control the queue.".into(),
                        },
                    )
                    .await;
                    return Ok(());
                }

                if envelope.sender != self.session.self_id() {
                    let role = self.session.get_peer_role(&envelope.sender).unwrap_or(Role::Listener);
                    let mut info = Self::peer_info_for(envelope.sender);
                    info.role = role;
                    self.session.record_peer_heartbeat(info, src);
                }

                let updated = self.session.handle_queue_proposal(command.clone())?;

                let action = match command {
                    crate::protocol::messages::QueueCommand::Add(_) => "queued a track",
                    crate::protocol::messages::QueueCommand::Skip => "skipped the current track",
                    crate::protocol::messages::QueueCommand::Remove { .. } => "removed a track",
                };
                let log_msg = format!(
                    "[System] {} {}",
                    self.session.display_name(&envelope.sender),
                    action
                );
                if self.session.role() >= Role::Moderator {
                    print_event(self.stdout.as_ref(), &log_msg);
                }
                self.send_to_peers(socket, Message::SystemLog(log_msg))
                    .await?;

                if self.session.role() >= Role::Moderator {
                    print_event(
                        self.stdout.as_ref(),
                        &format!(
                            "[System] Queue updated ({} upcoming)",
                            updated.upcoming.len()
                        ),
                    );
                }
                self.send_to_peers(socket, Message::QueueUpdate(updated))
                    .await?;
                Ok(())
            }
            Message::QueueUpdate(update) => {
                if !self.session.is_leader() && envelope.sender != self.session.leader_id() {
                    return Ok(());
                }
                let _ = self.session.accept_queue_update(update);
                Ok(())
            }
            Message::Pause { actor } => {
                if !self.session.is_leader() && envelope.sender != self.session.leader_id() {
                    return Ok(());
                }

                let role = sender_role.unwrap_or(Role::Listener);
                if role < Role::Moderator {
                    if self.session.is_leader() {
                        let _ = Self::send_message(
                            socket,
                            self.session.self_id(),
                            src,
                            Message::Notification {
                                text: "Permission Denied: Only Moderators can control playback."
                                    .into(),
                            },
                        )
                        .await;
                    }
                    return Ok(());
                }

                if self.session.is_leader() && envelope.sender != self.session.self_id() {
                    let role = self.session.get_peer_role(&envelope.sender).unwrap_or(Role::Listener);
                    let mut info = Self::peer_info_for(envelope.sender);
                    info.role = role;
                    self.session.record_peer_heartbeat(info, src);
                }

                if let Some(playback) = &self.playback {
                    playback.pause()?;
                }

                if self.session.role() >= Role::Moderator {
                    print_event(
                        self.stdout.as_ref(),
                        &format!(
                            "[System] Playback paused by {}",
                            self.session.display_name(&actor)
                        ),
                    );
                }

                if self.session.is_leader() {
                    self.send_to_peers(socket, Message::Pause { actor }).await?;
                }
                Ok(())
            }
            Message::Resume { actor } => {
                if !self.session.is_leader() && envelope.sender != self.session.leader_id() {
                    return Ok(());
                }

                let role = sender_role.unwrap_or(Role::Listener);
                if role < Role::Moderator {
                    if self.session.is_leader() {
                        let _ = Self::send_message(
                            socket,
                            self.session.self_id(),
                            src,
                            Message::Notification {
                                text: "Permission Denied: Only Moderators can control playback."
                                    .into(),
                            },
                        )
                        .await;
                    }
                    return Ok(());
                }

                if self.session.is_leader() && envelope.sender != self.session.self_id() {
                    let role = self.session.get_peer_role(&envelope.sender).unwrap_or(Role::Listener);
                    let mut info = Self::peer_info_for(envelope.sender);
                    info.role = role;
                    self.session.record_peer_heartbeat(info, src);
                }

                if let Some(playback) = &self.playback {
                    playback.resume()?;
                }

                if self.session.role() >= Role::Moderator {
                    print_event(
                        self.stdout.as_ref(),
                        &format!(
                            "[System] Playback resumed by {}",
                            self.session.display_name(&actor)
                        ),
                    );
                }

                if self.session.is_leader() {
                    self.send_to_peers(socket, Message::Resume { actor })
                        .await?;
                }
                Ok(())
            }
            Message::Play { actor } => {
                if !self.session.is_leader() && envelope.sender != self.session.leader_id() {
                    return Ok(());
                }

                let role = sender_role.unwrap_or(Role::Listener);
                if role < Role::Moderator {
                    if self.session.is_leader() {
                        let _ = Self::send_message(
                            socket,
                            self.session.self_id(),
                            src,
                            Message::Notification {
                                text: "Permission Denied: Only Moderators can control playback."
                                    .into(),
                            },
                        )
                        .await;
                    }
                    return Ok(());
                }

                if self.session.is_leader() && envelope.sender != self.session.self_id() {
                    let role = self.session.get_peer_role(&envelope.sender).unwrap_or(Role::Listener);
                    let mut info = Self::peer_info_for(envelope.sender);
                    info.role = role;
                    self.session.record_peer_heartbeat(info, src);
                }

                if let Some(playback) = &self.playback {
                    playback.resume()?;
                }

                if self.session.role() >= Role::Moderator {
                    print_event(
                        self.stdout.as_ref(),
                        &format!(
                            "[System] Playback started by {}",
                            self.session.display_name(&actor)
                        ),
                    );
                }

                if self.session.is_leader() {
                    self.send_to_peers(socket, Message::Play { actor }).await?;
                }
                Ok(())
            }
            Message::Skip => {
                let role = sender_role.unwrap_or(Role::Listener);
                if role < Role::Moderator {
                    if self.session.is_leader() {
                        let _ = Self::send_message(
                            socket,
                            self.session.self_id(),
                            src,
                            Message::Notification {
                                text: "Permission Denied: Only Moderators can skip tracks.".into(),
                            },
                        )
                        .await;
                    }
                    return Ok(());
                }

                if let Some(playback) = &self.playback {
                    playback.stop()?;
                }
                Ok(())
            }
            Message::Chat { sender, text, .. } => {
                if envelope.sender != self.session.self_id() {
                    let role = self.session.get_peer_role(&envelope.sender).unwrap_or(Role::Listener);
                    let mut info = Self::peer_info_for(envelope.sender);
                    info.role = role;
                    self.session.record_peer_heartbeat(info, src);
                }
                
                let display_name = self.session.display_name(&sender);
                
                if self.session.is_leader() {
                    // Leader formats and broadcasts to all peers
                    let broadcast = Message::ChatBroadcast {
                        display_name: display_name.clone(),
                        text: text.clone(),
                    };
                    self.send_to_peers(socket, broadcast).await?;
                    // Leader also prints locally
                    print_event(self.stdout.as_ref(), &format!("[{}]: {}", display_name, text));
                }
                Ok(())
            }
            Message::ChatBroadcast { display_name, text } => {
                if envelope.sender != self.session.leader_id() {
                    return Ok(());
                }
                print_event(self.stdout.as_ref(), &format!("[{}]: {}", display_name, text));
                Ok(())
            }
            Message::Notification { text } => {
                if envelope.sender != self.session.leader_id() {
                    return Ok(());
                }
                print_event(self.stdout.as_ref(), &text);
                Ok(())
            }
            Message::Intent(mut intent) => {
                if self.session.is_leader() && envelope.sender != self.session.self_id() {
                    // Follower requested an intent! Leader schedules it for T+100ms on the synced clock
                    let now = if let Some(clock) = &self.clock {
                        clock.reference_now()
                    } else {
                        intent.target_ref_time
                    };
                    intent.target_ref_time = now + 100_000;

                    // If track_title or position is unset, sample the leader's active track
                    if let Some(controller) = &self.controller
                        && let Ok(state) = controller.get_playback_state().await
                    {
                        if intent.track_title.is_none() {
                            intent.track_title = state.metadata.map(|m| m.title);
                        }
                        if matches!(intent.action, crate::protocol::messages::PlaybackAction::Play) && intent.position_us == 0 {
                            intent.position_us = state.position_us;
                        }
                    }

                    // Broadcast scheduled intent to all room peers
                    let broadcast_env = Envelope {
                        sender: self.session.self_id(),
                        payload: Message::Intent(intent.clone()),
                    };
                    if let Ok(bytes) = serialize(&broadcast_env) {
                        for addr in self.session.peer_socket_addrs() {
                            let _ = socket.send_to(&bytes, addr).await;
                        }
                    }
                }

                if let Some(scheduler) = &self.scheduler {
                    let _ = scheduler.execute_intent(&intent).await;
                }
                if let Some(evaluator) = &self.drift_evaluator {
                    evaluator.trigger_immediate();
                }
                Ok(())
            }
            Message::SyncAnchor(anchor) => {
                if !self.session.is_leader() && envelope.sender != self.session.leader_id() {
                    return Ok(());
                }
                self.session.accept_sync_anchor(anchor.clone());
                if let Some(evaluator) = &self.drift_evaluator {
                    evaluator.trigger_immediate();
                }
                if let Some(follower_sync) = &self.follower_sync {
                    follower_sync.ingest_message(&self.session, &Message::SyncAnchor(anchor));
                }
                Ok(())
            }
            message => {
                // Best-effort application message handling. Stale queue updates should not
                // stop the receive loop.
                match self.session.apply_message(message.clone()) {
                    Ok(()) => {}
                    Err(SynkroError::StaleQueue { .. }) => {}
                    Err(err) => return Err(err),
                }

                if let Some(follower_sync) = &self.follower_sync {
                    follower_sync.ingest_message(&self.session, &message);
                }
                Ok(())
            }
        }
    }

    pub async fn run_receive_loop(&self, socket: Arc<UdpSocket>) -> Result<()> {
        let mut buf = [0u8; 8 * 1024];
        loop {
            let (len, src) = socket.recv_from(&mut buf).await?;
            let Ok(envelope) = deserialize(&buf[..len]) else {
                continue;
            };

            // Never let a bad packet kill the runtime loop.
            let _ = self.handle_incoming(&socket, src, envelope).await;
        }
    }
}

pub struct LeaderAnchorBroadcaster {
    session: Arc<SessionState>,
    clock: Arc<dyn ClockSource>,
    playback: Arc<dyn PlaybackControl>,
    controller: Option<Arc<dyn crate::controller::MediaController>>,
    config: SyncConfig,
}

impl LeaderAnchorBroadcaster {
    pub fn new(
        session: Arc<SessionState>,
        clock: Arc<ClockSync>,
        playback: Arc<dyn PlaybackControl>,
        config: SyncConfig,
    ) -> Self {
        Self {
            session,
            clock,
            playback,
            controller: None,
            config,
        }
    }

    pub fn with_controller(mut self, controller: Arc<dyn crate::controller::MediaController>) -> Self {
        self.controller = Some(controller);
        self
    }

    pub async fn build_anchor_envelope(&self, sender: Uuid) -> Result<Envelope> {
        let status = if let Some(controller) = &self.controller {
            if let Ok(state) = controller.get_playback_state().await {
                crate::sync::controller::PlaybackStatus {
                    track_id: state.metadata.map(|m| m.title),
                    position_us: state.position_us,
                    rate: state.rate,
                    is_playing: state.is_playing,
                }
            } else {
                self.playback.status()
            }
        } else {
            self.playback.status()
        };

        let message = self
            .session
            .build_sync_anchor_message(self.clock.reference_now(), &status)?;
        Ok(Envelope {
            sender,
            payload: message,
        })
    }

    pub async fn broadcast_once(&self, socket: &UdpSocket, sender: Uuid) -> Result<usize> {
        if !self.session.is_leader() {
            return Err(SynkroError::NotLeader);
        }

        let envelope = self.build_anchor_envelope(sender).await?;
        let bytes = serialize(&envelope)?;
        let mut sent = 0usize;
        for addr in self.session.peer_socket_addrs() {
            socket.send_to(&bytes, addr).await?;
            sent += 1;
        }
        Ok(sent)
    }

    pub async fn run_broadcast_loop(&self, socket: Arc<UdpSocket>, sender: Uuid) -> Result<()> {
        loop {
            let _ = self.broadcast_once(&socket, sender).await;
            sleep(Duration::from_secs(self.config.anchor_broadcast_secs)).await;
        }
    }
}
