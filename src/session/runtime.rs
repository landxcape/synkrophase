use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

use crate::clock::sync::ClockSync;
use crate::config::SyncConfig;
use crate::error::{Result, SynkroError};
use crate::playback::engine::PlaybackEngine;
use crate::protocol::messages::{Envelope, Message, PeerInfo, deserialize, serialize};
use crate::session::{FollowerSyncRuntime, SessionState};
use crate::sync::controller::ClockSource;

pub struct SessionMessageRuntime {
    session: Arc<SessionState>,
    follower_sync: Option<Arc<FollowerSyncRuntime>>,
    playback: Option<Arc<PlaybackEngine>>,
}

impl SessionMessageRuntime {
    pub fn new(session: Arc<SessionState>) -> Self {
        Self {
            session,
            follower_sync: None,
            playback: None,
        }
    }

    pub fn with_follower_sync(mut self, follower_sync: Arc<FollowerSyncRuntime>) -> Self {
        self.follower_sync = Some(follower_sync);
        self
    }

    pub fn with_playback(mut self, playback: Arc<PlaybackEngine>) -> Self {
        self.playback = Some(playback);
        self
    }

    fn self_peer_info(&self) -> PeerInfo {
        PeerInfo {
            device_id: self.session.self_id(),
            clock_offset_us: 0,
            last_seen: 0,
        }
    }

    fn peer_info_for(sender: Uuid) -> PeerInfo {
        PeerInfo {
            device_id: sender,
            clock_offset_us: 0,
            last_seen: 0,
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

    async fn broadcast_message(
        socket: &UdpSocket,
        sender: Uuid,
        port: u16,
        message: Message,
    ) -> Result<()> {
        let broadcast_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::BROADCAST, port));
        Self::send_message(socket, sender, broadcast_addr, message).await
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
                Message::Pause | Message::Resume | Message::Play | Message::QueueProposal(_)
            );
            if !allowed {
                return Ok(());
            }
        }

        match envelope.payload {
            Message::JoinRequest { room_code } => {
                if !self.session.is_leader() {
                    return Ok(());
                }
                if room_code != self.session.room_code() {
                    return Ok(());
                }

                self.session
                    .record_peer_heartbeat(Self::peer_info_for(envelope.sender), src);

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
                    },
                )
                .await?;

                if let Some(stream) = queue_state
                    .current
                    .as_ref()
                    .and_then(|current| self.session.stream_url_for(&current.id))
                {
                    let _ = Self::send_message(
                        socket,
                        self.session.self_id(),
                        src,
                        Message::StreamUrl(stream),
                    )
                    .await;
                }

                let port = socket.local_addr()?.port();
                let _ = Self::broadcast_message(
                    socket,
                    self.session.self_id(),
                    port,
                    Message::PeerJoined(Self::peer_info_for(envelope.sender)),
                )
                .await;
                Ok(())
            }
            Message::JoinAccepted {
                peer_list,
                queue_state,
            } => {
                self.session
                    .accept_join_accepted(envelope.sender, peer_list, queue_state);
                Ok(())
            }
            Message::PeerJoined(peer) => {
                if peer.device_id != self.session.self_id() {
                    self.session.record_peer_seen(peer);
                }
                Ok(())
            }
            Message::PeerLeft(peer_id) => {
                self.session.remove_peer(&peer_id);
                Ok(())
            }
            Message::Heartbeat {
                room_code,
                is_leader,
            } => {
                if room_code != self.session.room_code() {
                    return Ok(());
                }
                self.session
                    .record_peer_heartbeat(Self::peer_info_for(envelope.sender), src);
                if is_leader {
                    self.session.set_leader_id(envelope.sender);
                }
                Ok(())
            }
            Message::LeaderElected(leader_id) => {
                self.session.set_leader_id(leader_id);
                Ok(())
            }
            Message::QueueProposal(command) => {
                if !self.session.is_leader() {
                    return Ok(());
                }

                if envelope.sender != self.session.self_id() {
                    self.session
                        .record_peer_heartbeat(Self::peer_info_for(envelope.sender), src);
                }

                let updated = self.session.handle_queue_proposal(command)?;
                self.send_to_peers(socket, Message::QueueUpdate(updated))
                    .await?;
                Ok(())
            }
            Message::StreamUrl(stream) => {
                self.session.accept_stream_url(stream.clone());
                if let Some(playback) = &self.playback {
                    let playback = Arc::clone(playback);
                    let url = stream.url.clone();
                    let track_id = stream.track_id.clone();
                    tokio::task::spawn_blocking(move || {
                        let _ = playback.load_and_play(&track_id, &url);
                    });
                }
                Ok(())
            }
            Message::Pause => {
                if envelope.sender != self.session.self_id() {
                    self.session
                        .record_peer_heartbeat(Self::peer_info_for(envelope.sender), src);
                }
                if let Some(playback) = &self.playback {
                    playback.pause()?;
                }
                if self.session.is_leader() {
                    self.send_to_peers(socket, Message::Pause).await?;
                }
                Ok(())
            }
            Message::Resume => {
                if envelope.sender != self.session.self_id() {
                    self.session
                        .record_peer_heartbeat(Self::peer_info_for(envelope.sender), src);
                }
                if let Some(playback) = &self.playback {
                    playback.resume()?;
                }
                if self.session.is_leader() {
                    self.send_to_peers(socket, Message::Resume).await?;
                }
                Ok(())
            }
            Message::Play => {
                if envelope.sender != self.session.self_id() {
                    self.session
                        .record_peer_heartbeat(Self::peer_info_for(envelope.sender), src);
                }
                if let Some(playback) = &self.playback {
                    playback.resume()?;
                }
                if self.session.is_leader() {
                    self.send_to_peers(socket, Message::Play).await?;
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
    playback: Arc<PlaybackEngine>,
    config: SyncConfig,
}

impl LeaderAnchorBroadcaster {
    pub fn new(
        session: Arc<SessionState>,
        clock: Arc<ClockSync>,
        playback: Arc<PlaybackEngine>,
        config: SyncConfig,
    ) -> Self {
        Self {
            session,
            clock,
            playback,
            config,
        }
    }

    pub fn build_anchor_envelope(&self, sender: Uuid) -> Result<Envelope> {
        let message = self
            .session
            .build_sync_anchor_message(self.clock.reference_now(), &self.playback.status())?;
        Ok(Envelope {
            sender,
            payload: message,
        })
    }

    pub async fn broadcast_once(&self, socket: &UdpSocket, sender: Uuid) -> Result<usize> {
        if !self.session.is_leader() {
            return Err(SynkroError::NotLeader);
        }

        let envelope = self.build_anchor_envelope(sender)?;
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
            let _ = self.broadcast_once(&socket, sender).await?;
            sleep(Duration::from_secs(self.config.anchor_broadcast_secs)).await;
        }
    }
}
