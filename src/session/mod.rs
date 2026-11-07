use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::error::{Result, SynkroError};
use crate::protocol::messages::{Message, PeerInfo, QueueCommand, QueueState, Role, SyncAnchor};
use crate::queue::state::QueueManager;
use crate::sync::controller::PlaybackStatus;

use self::leader::appoint_successor;
use self::peer::{PeerEntry, PeerRegistry};

pub mod broadcaster;
pub mod discovery;
pub mod engine;
pub mod invitation;
pub mod leader;
pub mod peer;
pub mod runtime;

pub use engine::{EngineCommand, SynkroEngine};

#[derive(Debug, Clone)]
pub struct ExpectedPlayerState {
    pub is_playing: Option<bool>,
    pub position_us: Option<i64>,
    pub set_at: Instant,
    pub timeout: Duration,
}

pub struct SessionState {
    room_code: String,
    self_id: Uuid,
    self_name: String,
    self_role: RwLock<Role>,
    leader_id: RwLock<Uuid>,
    is_join_confirmed: AtomicBool,
    expected_player_state: Mutex<Option<ExpectedPlayerState>>,
    clock_offset_us: AtomicI64,
    peers: PeerRegistry,
    queue: QueueManager,
    current_anchor: RwLock<Option<SyncAnchor>>,
    last_anchor_ref_time: AtomicU64,
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
            is_join_confirmed: AtomicBool::new(true),
            expected_player_state: Mutex::new(None),
            clock_offset_us: AtomicI64::new(0),
            peers: PeerRegistry::new(),
            queue: QueueManager::new(QueueState::default()),
            current_anchor: RwLock::new(None),
            last_anchor_ref_time: AtomicU64::new(0),
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
            is_join_confirmed: AtomicBool::new(false),
            expected_player_state: Mutex::new(None),
            clock_offset_us: AtomicI64::new(0),
            peers: PeerRegistry::new(),
            queue: QueueManager::new(queue_state),
            current_anchor: RwLock::new(None),
            last_anchor_ref_time: AtomicU64::new(0),
        };

        for peer in peer_list {
            let addr = if peer.device_id == leader_id {
                leader_addr
            } else {
                SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))
            };
            session.peers.upsert(peer, addr);
        }

        session
    }

    pub fn expect_playback_state(&self, is_playing: Option<bool>, position_us: Option<i64>) {
        let mut guard = self.expected_player_state.lock().unwrap();
        *guard = Some(ExpectedPlayerState {
            is_playing,
            position_us,
            set_at: Instant::now(),
            timeout: Duration::from_millis(2500),
        });
    }

    pub fn check_and_consume_expected(&self, is_playing: bool, position_us: i64) -> bool {
        let mut guard = self.expected_player_state.lock().unwrap();
        if let Some(expected) = guard.as_ref() {
            if expected.set_at.elapsed() > expected.timeout {
                *guard = None;
                return false;
            }

            let playing_matches = match expected.is_playing {
                Some(exp_playing) => exp_playing == is_playing,
                None => true,
            };

            let position_matches = match expected.position_us {
                Some(exp_pos) => (exp_pos - position_us).abs() < 1_500_000,
                None => true,
            };

            if playing_matches && position_matches {
                *guard = None;
                return true;
            }
        }
        false
    }

    pub fn check_and_consume_expected_play_pause(&self, is_playing: bool) -> bool {
        let mut guard = self.expected_player_state.lock().unwrap();
        if let Some(expected) = guard.as_mut() {
            if expected.set_at.elapsed() > expected.timeout {
                *guard = None;
                return false;
            }

            if let Some(exp_playing) = expected.is_playing
                && exp_playing == is_playing
            {
                expected.is_playing = None;
                if expected.position_us.is_none() {
                    *guard = None;
                }
                return true;
            }
        }
        false
    }

    pub fn check_and_consume_expected_seek(&self, position_us: i64) -> bool {
        let mut guard = self.expected_player_state.lock().unwrap();
        if let Some(expected) = guard.as_mut() {
            if expected.set_at.elapsed() > expected.timeout {
                *guard = None;
                return false;
            }

            if let Some(exp_pos) = expected.position_us
                && (exp_pos - position_us).abs() < 1_500_000
            {
                expected.position_us = None;
                if expected.is_playing.is_none() {
                    *guard = None;
                }
                return true;
            }
        }
        false
    }

    pub fn is_join_confirmed(&self) -> bool {
        self.is_join_confirmed.load(Ordering::Acquire)
    }

    pub fn mark_join_confirmed(&self) {
        self.is_join_confirmed.store(true, Ordering::Release);
    }

    pub fn is_leader(&self) -> bool {
        self.role() == Role::Leader
    }

    pub fn is_alive(&self, id: &Uuid) -> bool {
        if *id == self.self_id {
            return true;
        }
        self.peers.is_alive(id)
    }

    pub fn get_peer_role(&self, id: &Uuid) -> Option<Role> {
        if *id == self.self_id {
            return Some(self.role());
        }
        self.peers.get_role(id)
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
        self.peers.set_role(&self.self_id, Role::Leader);
        self.reset_anchor_watermark();
    }

    pub fn demote_from_leader(&self, new_leader_id: Uuid) {
        self.set_role(Role::Moderator);
        self.set_leader_id(new_leader_id);
        self.peers.set_role(&self.self_id, Role::Moderator);
        self.peers.set_role(&new_leader_id, Role::Leader);
        self.reset_anchor_watermark();
    }

    pub fn can_assign_role(&self, actor_id: Uuid, target_id: Uuid, new_role: Role) -> Result<()> {
        let actor_role = if actor_id == self.self_id {
            self.role()
        } else {
            self.peers.get_role(&actor_id).unwrap_or(Role::Listener)
        };

        let target_role = if target_id == self.self_id {
            self.role()
        } else {
            self.peers.get_role(&target_id).unwrap_or(Role::Listener)
        };

        // Rule 1: Listeners cannot assign any roles
        if actor_role == Role::Listener {
            return Err(SynkroError::PermissionDenied(
                "Listeners cannot assign roles".to_string(),
            ));
        }

        // Rule 2: Cannot assign a role strictly higher than actor's own role
        if new_role > actor_role {
            return Err(SynkroError::PermissionDenied(format!(
                "Actor ({:?}) cannot assign higher role ({:?})",
                actor_role, new_role
            )));
        }

        // Rule 3: Target current role must be strictly less than actor role
        // Exception: Leader transferring leadership to another peer (actor_role == Leader)
        if actor_role != Role::Leader && target_role >= actor_role {
            return Err(SynkroError::PermissionDenied(format!(
                "Actor ({:?}) cannot modify equal or higher rank peer ({:?})",
                actor_role, target_role
            )));
        }

        Ok(())
    }

    pub fn apply_role_assignment(&self, target_id: Uuid, new_role: Role) {
        if target_id == self.self_id {
            self.set_role(new_role);
        }
        self.peers.set_role(&target_id, new_role);
    }

    pub fn room_code(&self) -> &str {
        &self.room_code
    }

    pub fn leader_id(&self) -> Uuid {
        *self.leader_id.read().unwrap()
    }

    pub fn set_leader_id(&self, leader_id: Uuid) {
        let mut current = self.leader_id.write().unwrap();
        if *current != leader_id {
            *current = leader_id;
            self.reset_anchor_watermark();
        }
    }

    pub fn leader_socket_addr(&self) -> Option<std::net::SocketAddr> {
        let leader = self.leader_id();
        self.peers.get_addr(&leader)
    }

    pub fn clock_offset(&self) -> i64 {
        self.clock_offset_us.load(Ordering::Acquire)
    }

    pub fn set_clock_offset(&self, offset_us: i64) {
        self.clock_offset_us.store(offset_us, Ordering::Release);
    }

    pub fn self_info(&self) -> PeerInfo {
        PeerInfo {
            device_id: self.self_id,
            name: self.self_name.clone(),
            clock_offset_us: self.clock_offset(),
            last_seen: 0,
            role: self.role(),
        }
    }

    pub fn peer_ids(&self) -> Vec<Uuid> {
        self.peers.peer_ids()
    }

    pub fn peer_infos(&self) -> Vec<PeerInfo> {
        self.peers.peer_infos()
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
        self.mark_join_confirmed();
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

    /// Ingests a new SyncAnchor if and only if its reference_time is strictly monotonically
    /// increasing relative to previously observed anchors, filtering out late network arrivals.
    pub fn accept_sync_anchor(&self, anchor: SyncAnchor) -> bool {
        let prev = self.last_anchor_ref_time.load(Ordering::Acquire);
        if prev > 0 && anchor.reference_time <= prev {
            return false;
        }
        self.last_anchor_ref_time
            .store(anchor.reference_time.max(1), Ordering::Release);
        *self.current_anchor.write().unwrap() = Some(anchor);
        true
    }

    /// Directly updates the current anchor. Used by the leader node to store its own active
    /// playback anchor for new joiners without passing through network monotonic filtering.
    pub fn set_current_anchor(&self, anchor: SyncAnchor) {
        self.last_anchor_ref_time
            .store(anchor.reference_time.max(1), Ordering::Release);
        *self.current_anchor.write().unwrap() = Some(anchor);
    }

    pub fn last_anchor_ref_time(&self) -> u64 {
        self.last_anchor_ref_time.load(Ordering::Acquire)
    }

    pub fn reset_anchor_watermark(&self) {
        self.last_anchor_ref_time.store(0, Ordering::Release);
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
            track_title: playback_status.track_id.clone(),
        }))
    }

    pub fn apply_message(&self, message: Message) -> Result<()> {
        match message {
            Message::QueueUpdate(update) => self.accept_queue_update(update),
            Message::SyncAnchor(anchor) => {
                self.accept_sync_anchor(anchor);
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leader_join_confirmed_by_default() {
        let leader_id = Uuid::new_v4();
        let session = SessionState::new_leader("ROOM1".into(), leader_id, "Host".into());
        assert!(session.is_join_confirmed());
    }

    #[test]
    fn test_join_unconfirmed_until_accepted() {
        let follower_id = Uuid::new_v4();
        let leader_id = Uuid::new_v4();
        let leader_addr = "127.0.0.1:5871".parse().unwrap();

        let session = SessionState::from_join(
            "ROOM1".into(),
            follower_id,
            "Follower".into(),
            leader_id,
            leader_addr,
            vec![],
            QueueState::default(),
        );

        assert!(!session.is_join_confirmed());

        session.accept_join_accepted(
            leader_id,
            leader_addr,
            vec![],
            QueueState::default(),
            Role::Listener,
        );

        assert!(session.is_join_confirmed());
    }

    #[test]
    fn test_expected_player_state_echo_cancellation() {
        let leader_id = Uuid::new_v4();
        let session = SessionState::new_leader("ROOM1".into(), leader_id, "Host".into());

        // Expect paused state
        session.expect_playback_state(Some(false), None);

        // Matching query consumes the expectation and returns true
        assert!(session.check_and_consume_expected(false, 10_000_000));
        // Second call returns false since expectation was consumed
        assert!(!session.check_and_consume_expected(false, 10_000_000));

        // Expect position seek to 45s
        session.expect_playback_state(None, Some(45_000_000));
        // Position within tolerance matches
        assert!(session.check_and_consume_expected(true, 45_200_000));
        assert!(!session.check_and_consume_expected(true, 45_200_000));

        // Test decoupled play/pause checking regardless of position
        session.expect_playback_state(Some(true), None);
        assert!(session.check_and_consume_expected_play_pause(true));
        assert!(!session.check_and_consume_expected_play_pause(true));

        // Test decoupled seek checking regardless of playing state
        session.expect_playback_state(None, Some(60_000_000));
        assert!(session.check_and_consume_expected_seek(60_500_000));
        assert!(!session.check_and_consume_expected_seek(60_500_000));
    }
}
