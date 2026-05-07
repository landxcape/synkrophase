use uuid::Uuid;

/// Deterministically elects a leader from the set of live peers and the local peer.
/// The peer with the numerically lowest UUID is chosen as the leader.
pub fn elect_leader(live_peers: &[Uuid], self_id: Uuid) -> Uuid {
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

        assert_eq!(elect_leader(&[id2, id3], id1), id1, "Self wins if lowest");
        assert_eq!(elect_leader(&[id1, id3], id2), id1, "Peer wins if lowest");
        assert_eq!(elect_leader(&[id1, id2], id3), id1, "Lowest win regardless of position");
    }

    #[test]
    fn test_election_with_single_peer() {
        let self_id = Uuid::new_v4();
        assert_eq!(elect_leader(&[], self_id), self_id, "Only peer (self) must win");
    }
}
