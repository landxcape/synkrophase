use uuid::Uuid;
use crate::protocol::messages::PeerInfo;

/// Deterministically appoints a successor from the set of live peers and the local peer.
/// Priority is given to the highest Role (descending), then to the numerically lowest UUID (ascending).
pub fn appoint_successor(live_peers: &[PeerInfo], self_info: &PeerInfo) -> Uuid {
    let mut candidates = live_peers.to_vec();
    candidates.push(self_info.clone());

    // Sort by Role (descending), then by device_id (ascending)
    candidates.sort_by(|a, b| {
        b.role.cmp(&a.role)
            .then_with(|| a.device_id.cmp(&b.device_id))
    });

    candidates[0].device_id
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::messages::{Role, PeerInfo};

    #[test]
    fn test_succession_role_priority() {
        let id1 = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let id2 = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();

        let info1 = PeerInfo {
            device_id: id1,
            name: "Listener".into(),
            clock_offset_us: 0,
            last_seen: 0,
            role: Role::Listener,
        };
        let info2 = PeerInfo {
            device_id: id2,
            name: "Moderator".into(),
            clock_offset_us: 0,
            last_seen: 0,
            role: Role::Moderator,
        };

        // Moderator (id2) should win even though id1 is lower UUID
        assert_eq!(appoint_successor(&[info2.clone()], &info1), id2);
    }

    #[test]
    fn test_succession_lowest_uuid_tiebreak() {
        let id1 = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let id2 = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();

        let info1 = PeerInfo {
            device_id: id1,
            name: "Alice".into(),
            clock_offset_us: 0,
            last_seen: 0,
            role: Role::Moderator,
        };
        let info2 = PeerInfo {
            device_id: id2,
            name: "Bob".into(),
            clock_offset_us: 0,
            last_seen: 0,
            role: Role::Moderator,
        };

        // Both are Moderators, lowest UUID (id1) wins
        assert_eq!(appoint_successor(&[info2], &info1), id1);
    }
}
