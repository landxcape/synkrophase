use uuid::Uuid;

/// Deterministically elects a leader from the set of live peers and the local peer.
/// The peer with the numerically lowest UUID is chosen as the leader.
pub fn elect_leader(live_peers: &[Uuid], self_id: Uuid, current_leader: Option<Uuid>) -> Uuid {
    // 1. If current leader is still in the room (or is us), stay with them.
    if let Some(leader) = current_leader {
        if leader == self_id || live_peers.contains(&leader) {
            return leader;
        }
    }

    // 2. Otherwise, fall back to "lowest UUID wins" for deterministic choice.
    let mut candidates = live_peers.to_vec();
    candidates.push(self_id);
    candidates.sort();
    candidates[0]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_election_lowest_uuid_wins() {
        let id1 = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let id2 = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
        let id3 = Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap();

        assert_eq!(elect_leader(&[id2, id3], id1, None), id1, "Self wins if lowest");
        assert_eq!(elect_leader(&[id1, id3], id2, None), id1, "Peer wins if lowest");
    }

    #[test]
    fn test_election_is_sticky() {
        let id1 = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let id2 = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
        
        // id2 is current leader. even if id1 is lower, we stay with id2.
        assert_eq!(elect_leader(&[id2], id1, Some(id2)), id2, "Leader must stay sticky");
    }

    #[test]
    fn test_election_with_single_peer() {
        let self_id = Uuid::new_v4();
        assert_eq!(
            elect_leader(&[], self_id, None),
            self_id,
            "Only peer (self) must win"
        );
    }
}
