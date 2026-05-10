use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::error::{Result, SynkroError};
use crate::playback::engine::PlaybackStatus;
use crate::protocol::messages::{
    Message, PeerInfo, QueueCommand, QueueState, Role, StreamUrl, SyncAnchor,
};
use crate::queue::state::QueueManager;
use crate::stream::resolver::StreamResolver;
use crate::sync::controller::SyncController;

use self::leader::appoint_successor;
use self::peer::{PeerEntry, PeerRegistry};

pub mod discovery;
pub mod leader;
pub mod peer;
pub mod runtime;

pub struct SessionState {
    room_code: String,
    self_id: Uuid,
    self_name: String,
    self_role: RwLock<Role>,
    leader_id: RwLock<Uuid>,
    peers: PeerRegistry,
    queue: QueueManager,
    stream_urls: RwLock<HashMap<String, StreamUrl>>,
    current_anchor: RwLock<Option<SyncAnchor>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSnapshot {
    pub room_code: String,
    pub leader_id: Uuid,
    pub peer_list: Vec<PeerInfo>,
    pub queue_state: QueueState,
}

impl SessionState {
    pub fn new_leader(room_code: String, self_id: Uuid, self_name: String) -> Self {
        Self {
            room_code,
            self_id,
            self_name,
            self_role: RwLock::new(Role::Leader),
            leader_id: RwLock::new(self_id),
            peers: PeerRegistry::new(),
            queue: QueueManager::new(QueueState::default()),
            stream_urls: RwLock::new(HashMap::new()),
            current_anchor: RwLock::new(None),
        }
    }

    pub fn from_join(
        room_code: String,
        self_id: Uuid,
        self_name: String,
        leader_id: Uuid,
        leader_addr: SocketAddr,
        peer_list: Vec<PeerInfo>,
        queue_state: QueueState,
    ) -> Self {
        let session = Self {
            room_code,
            self_id,
            self_name,
            self_role: RwLock::new(Role::Listener),
            leader_id: RwLock::new(leader_id),
            peers: PeerRegistry::new(),
            queue: QueueManager::new(queue_state),
            stream_urls: RwLock::new(HashMap::new()),
            current_anchor: RwLock::new(None),
        };

        for peer in peer_list {
            let addr = if peer.device_id == leader_id {
                leader_addr
            } else {
                SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))
            };
            session.peers.upsert(
                peer,
                addr,
            );
        }

        session
    }

    pub fn is_leader(&self) -> bool {
        self.role() == Role::Leader
    }

    pub fn is_alive(&self, id: &Uuid) -> bool {
        self.peers.is_alive(id)
    }

    pub fn self_id(&self) -> Uuid {
        self.self_id
    }

    pub fn role(&self) -> Role {
        *self.self_role.read().unwrap()
    }

    pub fn set_role(&self, role: Role) {
        *self.self_role.write().unwrap() = role;
    }

    pub fn promote_to_leader(&self) {
        self.set_role(Role::Leader);
        self.set_leader_id(self.self_id);
    }

    pub fn room_code(&self) -> &str {
        &self.room_code
    }

    pub fn leader_id(&self) -> Uuid {
        *self.leader_id.read().unwrap()
    }

    pub fn set_leader_id(&self, leader_id: Uuid) {
        *self.leader_id.write().unwrap() = leader_id;
    }

    pub fn self_info(&self) -> PeerInfo {
        PeerInfo {
            device_id: self.self_id,
            name: self.self_name.clone(),
            clock_offset_us: 0,
            last_seen: 0,
            role: self.role(),
        }
    }

    pub fn peer_ids(&self) -> Vec<Uuid> {
        self.peers.peer_ids()
    }

    pub fn display_name(&self, id: &Uuid) -> String {
        self.peers.display_name(id, self.self_id, &self.self_name)
    }

    pub fn peer_socket_addrs(&self) -> Vec<SocketAddr> {
        self.peers.peer_addrs()
    }

    pub fn all_alive_peers(&self) -> Vec<(Uuid, PeerEntry)> {
        self.peers.all_alive()
    }

    pub fn queue_snapshot(&self) -> QueueState {
        self.queue.snapshot()
    }

    pub fn snapshot(&self) -> SessionSnapshot {
        SessionSnapshot {
            room_code: self.room_code.clone(),
            leader_id: self.leader_id(),
            peer_list: self.peers.peer_infos(),
            queue_state: self.queue.snapshot(),
        }
    }

    pub fn record_peer_heartbeat(&self, peer: PeerInfo, addr: SocketAddr) {
        self.peers.upsert(peer, addr);
    }

    pub fn record_peer_seen(&self, peer: PeerInfo) {
        self.peers.upsert(
            peer,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
        );
    }

    pub fn remove_peer(&self, id: &Uuid) {
        self.peers.remove(id);
    }

    pub fn mark_peer_stale(&self, id: &Uuid, elapsed: Duration) {
        self.peers.mark_stale(id, elapsed);
    }

    pub fn prune_and_appoint(&self, timeout: Duration) -> (Vec<Uuid>, Uuid) {
        let expired = self.peers.prune_expired(timeout);
        let heir = appoint_successor(&self.peers.peer_infos(), &self.self_info());
        (expired, heir)
    }

    pub fn handle_queue_proposal(&self, command: QueueCommand) -> Result<QueueState> {
        if !self.is_leader() {
            return Err(SynkroError::NotLeader);
        }

        self.queue.apply(command)
    }

    pub fn accept_queue_update(&self, update: QueueState) -> Result<()> {
        self.queue.accept_update(update)
    }

    pub fn force_queue_state(&self, update: QueueState) {
        self.queue.force_set(update);
    }

    pub fn accept_join_accepted(
        &self,
        leader_id: Uuid,
        leader_addr: SocketAddr,
        peer_list: Vec<PeerInfo>,
        queue: QueueState,
        assigned_role: Role,
    ) {
        self.set_leader_id(leader_id);
        self.set_role(assigned_role);
        for peer in peer_list {
            if peer.device_id != self.self_id {
                if peer.device_id == leader_id {
                    self.peers.upsert(peer, leader_addr);
                } else {
                    self.record_peer_seen(peer);
                }
            }
        }
        self.peers.remove(&Uuid::nil());
        self.force_queue_state(queue);
    }

    pub async fn resolve_current_track(&self, resolver: &StreamResolver) -> Result<StreamUrl> {
        if !self.is_leader() {
            return Err(SynkroError::NotLeader);
        }

        let track =
            self.queue.snapshot().current.ok_or_else(|| {
                SynkroError::StreamResolution("no current track to resolve".into())
            })?;
        let resolved = resolver.resolve(&track.youtube_url).await?;
        let stream = StreamUrl {
            track_id: track.id.clone(),
            url: resolved.url,
            expires_at: resolved.expires_at.unwrap_or_default(),
        };

        self.accept_stream_url(stream.clone());
        Ok(stream)
    }

    pub fn accept_stream_url(&self, stream: StreamUrl) {
        self.stream_urls
            .write()
            .unwrap()
            .insert(stream.track_id.clone(), stream);
    }

    pub fn stream_url_for(&self, track_id: &str) -> Option<StreamUrl> {
        self.stream_urls.read().unwrap().get(track_id).cloned()
    }

    pub fn stream_message_for(&self, track_id: &str) -> Option<Message> {
        self.stream_url_for(track_id).map(Message::StreamUrl)
    }

    pub fn accept_sync_anchor(&self, anchor: SyncAnchor) {
        *self.current_anchor.write().unwrap() = Some(anchor);
    }

    pub fn latest_sync_anchor(&self) -> Option<SyncAnchor> {
        self.current_anchor.read().unwrap().clone()
    }

    pub fn build_sync_anchor_message(
        &self,
        reference_time: u64,
        playback_status: &PlaybackStatus,
    ) -> Result<Message> {
        if !self.is_leader() {
            return Err(SynkroError::NotLeader);
        }

        Ok(Message::SyncAnchor(SyncAnchor {
            reference_time,
            media_position_us: playback_status.position_us,
            playback_rate: playback_status.rate,
            is_playing: playback_status.is_playing,
        }))
    }

    pub fn apply_message(&self, message: Message) -> Result<()> {
        match message {
            Message::QueueUpdate(update) => self.accept_queue_update(update),
            Message::StreamUrl(stream) => {
                self.accept_stream_url(stream);
                Ok(())
            }
            Message::SyncAnchor(anchor) => {
                self.accept_sync_anchor(anchor);
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

pub struct FollowerSyncRuntime {
    controller: Arc<SyncController>,
    sync_loop_handle: Mutex<Option<JoinHandle<()>>>,
}

impl FollowerSyncRuntime {
    pub fn new(controller: Arc<SyncController>) -> Self {
        Self {
            controller,
            sync_loop_handle: Mutex::new(None),
        }
    }

    pub fn ingest_message(&self, session: &SessionState, message: &Message) {
        if let Message::SyncAnchor(anchor) = message {
            session.accept_sync_anchor(anchor.clone());
            self.controller.set_anchor(anchor.clone());
        }
    }

    pub fn start_sync_loop(&self) -> bool {
        let mut handle = self.sync_loop_handle.lock().unwrap();
        if handle.is_some() {
            return false;
        }

        let controller = Arc::clone(&self.controller);
        *handle = Some(tokio::spawn(async move {
            let _ = controller.run_sync_loop().await;
        }));
        true
    }

    pub fn stop_sync_loop(&self) {
        if let Some(handle) = self.sync_loop_handle.lock().unwrap().take() {
            handle.abort();
        }
    }

    pub fn sync_loop_running(&self) -> bool {
        self.sync_loop_handle.lock().unwrap().is_some()
    }
}
