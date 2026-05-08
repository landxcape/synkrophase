use crate::protocol::messages::PeerInfo;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Clone)]
pub struct PeerEntry {
    pub info: PeerInfo,
    pub addr: SocketAddr,
    pub last_heartbeat: Instant,
}

pub struct PeerRegistry {
    peers: RwLock<HashMap<Uuid, PeerEntry>>,
}

impl Default for PeerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerRegistry {
    pub fn new() -> Self {
        Self {
            peers: RwLock::new(HashMap::new()),
        }
    }

    pub fn add(&self, id: Uuid, entry: PeerEntry) {
        let mut peers = self.peers.write().unwrap();
        peers.insert(id, entry);
    }

    pub fn upsert(&self, info: PeerInfo, addr: SocketAddr) {
        let mut peers = self.peers.write().unwrap();
        peers.insert(
            info.device_id,
            PeerEntry {
                info,
                addr,
                last_heartbeat: Instant::now(),
            },
        );
    }

    pub fn remove(&self, id: &Uuid) {
        let mut peers = self.peers.write().unwrap();
        peers.remove(id);
    }

    pub fn is_alive(&self, id: &Uuid) -> bool {
        let peers = self.peers.read().unwrap();
        peers.contains_key(id)
    }

    pub fn all_alive(&self) -> Vec<(Uuid, PeerEntry)> {
        let peers = self.peers.read().unwrap();
        peers
            .iter()
            .map(|(id, entry)| (*id, entry.clone()))
            .collect()
    }

    pub fn expired_peers(&self, timeout: Duration) -> Vec<Uuid> {
        let peers = self.peers.read().unwrap();
        let now = Instant::now();
        peers
            .iter()
            .filter(|(_, entry)| now.duration_since(entry.last_heartbeat) > timeout)
            .map(|(id, _)| *id)
            .collect()
    }

    pub fn prune_expired(&self, timeout: Duration) -> Vec<Uuid> {
        let expired = self.expired_peers(timeout);
        let mut peers = self.peers.write().unwrap();
        for id in &expired {
            peers.remove(id);
        }
        expired
    }

    pub fn peer_ids(&self) -> Vec<Uuid> {
        let peers = self.peers.read().unwrap();
        let mut ids: Vec<_> = peers.keys().copied().collect();
        ids.sort();
        ids
    }

    pub fn peer_infos(&self) -> Vec<PeerInfo> {
        let peers = self.peers.read().unwrap();
        let mut infos: Vec<_> = peers.values().map(|entry| entry.info.clone()).collect();
        infos.sort_by_key(|info| info.device_id);
        infos
    }

    pub fn peer_addrs(&self) -> Vec<SocketAddr> {
        let peers = self.peers.read().unwrap();
        peers.values().map(|entry| entry.addr).collect()
    }

    pub fn mark_stale(&self, id: &Uuid, elapsed: Duration) {
        let mut peers = self.peers.write().unwrap();
        if let Some(entry) = peers.get_mut(id) {
            entry.last_heartbeat = Instant::now() - elapsed;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn test_peer_registry_expiry() {
        let registry = PeerRegistry::new();
        let id = Uuid::new_v4();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let info = PeerInfo {
            device_id: id,
            clock_offset_us: 0,
            last_seen: 0,
        };

        registry.add(
            id,
            PeerEntry {
                info,
                addr,
                last_heartbeat: Instant::now() - Duration::from_secs(5),
            },
        );

        let expired = registry.expired_peers(Duration::from_secs(3));
        assert_eq!(expired.len(), 1, "Should have 1 expired peer");
        assert_eq!(expired[0], id);
    }
}
