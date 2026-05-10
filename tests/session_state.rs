use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use synkrophase::playback::engine::PlaybackStatus;
use synkrophase::protocol::messages::{
    Message, PeerInfo, QueueCommand, QueueState, StreamUrl, SyncAnchor, Track,
};
use synkrophase::session::{SessionSnapshot, SessionState};
use uuid::Uuid;

fn track(id: &str) -> Track {
    Track {
        id: id.into(),
        youtube_url: format!("https://youtube.com/watch?v={id}"),
        title: format!("Track {id}"),
        requested_by: Uuid::nil(),
    }
}

fn peer_info(id: Uuid) -> PeerInfo {
    PeerInfo {
        device_id: id,
        name: "TestUser".to_string(),
        clock_offset_us: 0,
        last_seen: 0,
    }
}

#[test]
fn join_accepted_initializes_peer_session() {
    let leader_id = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let self_id = Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap();
    let queue_state = QueueState {
        version: 4,
        current: Some(track("current")),
        upcoming: vec![track("next")],
    };

    let session = SessionState::from_join(
        "ROOM42".into(),
        self_id,
        leader_id,
        "127.0.0.1:8080".parse().unwrap(),
        vec![peer_info(leader_id)],
        queue_state.clone(),
    );

    assert!(!session.is_leader());
    assert_eq!(session.leader_id(), leader_id);
    assert_eq!(session.queue_snapshot(), queue_state);
    assert_eq!(session.peer_ids(), vec![leader_id]);
}

#[test]
fn heartbeat_expiry_re_elects_lowest_live_peer() {
    let old_leader = Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap();
    let self_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let other_peer = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

    let session = SessionState::from_join(
        "ROOM42".into(),
        self_id,
        old_leader,
        addr,
        vec![peer_info(old_leader), peer_info(other_peer)],
        QueueState::default(),
    );

    session.record_peer_heartbeat(peer_info(old_leader), addr);
    session.record_peer_heartbeat(peer_info(other_peer), addr);
    session.mark_peer_stale(&old_leader, Duration::from_secs(5));

    let expired = session.prune_and_elect(Duration::from_secs(3));

    assert_eq!(expired, vec![old_leader]);
    assert_eq!(session.leader_id(), self_id);
    assert!(session.is_leader());
}

#[test]
fn leader_serializes_concurrent_queue_proposals() {
    let leader_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let session = Arc::new(SessionState::new_leader("ROOM42".into(), leader_id));

    let first = Arc::clone(&session);
    let handle_a = thread::spawn(move || {
        first
            .handle_queue_proposal(QueueCommand::Add(track("one")))
            .unwrap();
    });

    let second = Arc::clone(&session);
    let handle_b = thread::spawn(move || {
        second
            .handle_queue_proposal(QueueCommand::Add(track("two")))
            .unwrap();
    });

    handle_a.join().unwrap();
    handle_b.join().unwrap();

    let snapshot = session.queue_snapshot();
    assert_eq!(snapshot.version, 2);
    assert!(snapshot.current.is_some());
    assert_eq!(snapshot.upcoming.len(), 1);

    let mut ids = vec![
        snapshot.current.unwrap().id,
        snapshot.upcoming.into_iter().next().unwrap().id,
    ];
    ids.sort();
    assert_eq!(ids, vec!["one".to_string(), "two".to_string()]);
}

#[test]
fn leader_snapshot_preserves_last_queue_state_for_handoff() {
    let leader_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let session = SessionState::new_leader("ROOM42".into(), leader_id);
    session
        .handle_queue_proposal(QueueCommand::Add(track("one")))
        .unwrap();

    let snapshot = session.snapshot();

    assert_eq!(
        snapshot,
        SessionSnapshot {
            room_code: "ROOM42".into(),
            leader_id,
            peer_list: vec![],
            queue_state: QueueState {
                version: 1,
                current: Some(track("one")),
                upcoming: vec![],
            },
        }
    );
}

#[test]
fn apply_message_updates_stream_and_sync_anchor_state() {
    let self_id = Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap();
    let leader_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let session = SessionState::from_join(
        "ROOM42".into(),
        self_id,
        leader_id,
        "127.0.0.1:8080".parse().unwrap(),
        vec![],
        QueueState::default(),
    );

    let stream = StreamUrl {
        track_id: "track-1".into(),
        url: "https://cdn.example.com/audio".into(),
        expires_at: 1_000,
    };
    let anchor = SyncAnchor {
        reference_time: 10_000,
        media_position_us: 20_000,
        playback_rate: 1.02,
        is_playing: true,
    };

    session
        .apply_message(Message::StreamUrl(stream.clone()))
        .unwrap();
    session
        .apply_message(Message::SyncAnchor(anchor.clone()))
        .unwrap();

    assert_eq!(session.stream_url_for("track-1"), Some(stream));
    assert_eq!(session.latest_sync_anchor(), Some(anchor));
}

#[test]
fn leader_builds_sync_anchor_message_from_playback_status() {
    let leader_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let session = SessionState::new_leader("ROOM42".into(), leader_id);
    let status = PlaybackStatus {
        track_id: Some("track-1".into()),
        stream_url: Some("https://cdn.example.com/audio".into()),
        position_us: 111_000,
        rate: 0.98,
        is_playing: true,
    };

    let message = session.build_sync_anchor_message(123_000, &status).unwrap();
    assert_eq!(
        message,
        Message::SyncAnchor(SyncAnchor {
            reference_time: 123_000,
            media_position_us: 111_000,
            playback_rate: 0.98,
            is_playing: true,
        })
    );
}
