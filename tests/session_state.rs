use std::sync::Arc;
use std::thread;
use std::time::Duration;

use synkrophase::protocol::messages::{
    Message, PeerInfo, QueueCommand, QueueState, SyncAnchor, Track,
};
use synkrophase::session::{SessionSnapshot, SessionState};
use synkrophase::sync::controller::PlaybackStatus;
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
        role: synkrophase::protocol::messages::Role::Listener,
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
        "Self".into(),
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
    let peer_two = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();

    let session = SessionState::from_join(
        "ROOM42".into(),
        self_id,
        "Self".into(),
        old_leader,
        "127.0.0.1:8080".parse().unwrap(),
        vec![peer_info(old_leader), peer_info(peer_two)],
        QueueState::default(),
    );

    thread::sleep(Duration::from_millis(50));
    session.record_peer_heartbeat(peer_info(peer_two), "127.0.0.1:8081".parse().unwrap());

    let (expired, heir) = session.prune_and_appoint(Duration::from_millis(20));

    assert_eq!(expired, vec![old_leader]);
    assert_eq!(heir, self_id);
}

#[test]
fn peer_registry_is_thread_safe_under_concurrent_updates() {
    let session = Arc::new(SessionState::new_leader(
        "ROOM42".into(),
        Uuid::new_v4(),
        "Leader".into(),
    ));

    let p1 = Uuid::new_v4();
    let p2 = Uuid::new_v4();

    let s1 = Arc::clone(&session);
    let h1 = thread::spawn(move || {
        for _ in 0..50 {
            s1.record_peer_heartbeat(peer_info(p1), "127.0.0.1:8001".parse().unwrap());
        }
    });

    let s2 = Arc::clone(&session);
    let h2 = thread::spawn(move || {
        for _ in 0..50 {
            s2.record_peer_heartbeat(peer_info(p2), "127.0.0.1:8002".parse().unwrap());
        }
    });

    h1.join().unwrap();
    h2.join().unwrap();

    let mut ids = session.peer_ids();
    ids.sort();

    let mut expected = vec![p1, p2];
    expected.sort();
    assert_eq!(ids, expected);
}

#[test]
fn queue_proposals_reject_non_leader_callers() {
    let self_id = Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap();
    let session = SessionState::from_join(
        "ROOM42".into(),
        self_id,
        "Self".into(),
        Uuid::new_v4(),
        "127.0.0.1:8080".parse().unwrap(),
        vec![],
        QueueState::default(),
    );

    let result = session.handle_queue_proposal(QueueCommand::Add(track("one")));
    assert!(result.is_err());
}

#[test]
fn queue_proposals_succeed_for_leader() {
    let session = SessionState::new_leader("ROOM42".into(), Uuid::new_v4(), "Leader".into());
    let state = session
        .handle_queue_proposal(QueueCommand::Add(track("one")))
        .unwrap();

    assert_eq!(state.version, 1);
    assert_eq!(state.current.as_ref().map(|t| t.id.as_str()), Some("one"));
}

#[test]
fn concurrent_heartbeats_maintain_distinct_peers() {
    let session = Arc::new(SessionState::new_leader(
        "ROOM42".into(),
        Uuid::new_v4(),
        "Leader".into(),
    ));

    let mut handles = vec![];
    for name in ["one", "two"] {
        let s = Arc::clone(&session);
        handles.push(thread::spawn(move || {
            let info = PeerInfo {
                device_id: Uuid::new_v4(),
                name: name.to_string(),
                clock_offset_us: 0,
                last_seen: 0,
                role: synkrophase::protocol::messages::Role::Listener,
            };
            s.record_peer_heartbeat(info, "127.0.0.1:9000".parse().unwrap());
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let mut names: Vec<_> = session
        .all_alive_peers()
        .into_iter()
        .map(|(_, e)| e.info.name)
        .collect();
    names.sort();
    assert_eq!(names, vec!["one".to_string(), "two".to_string()]);
}

#[test]
fn leader_snapshot_preserves_last_queue_state_for_handoff() {
    let leader_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let session = SessionState::new_leader("ROOM42".into(), leader_id, "Leader".into());
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
fn apply_message_updates_sync_anchor_state() {
    let self_id = Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap();
    let leader_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let session = SessionState::from_join(
        "ROOM42".into(),
        self_id,
        "Self".into(),
        leader_id,
        "127.0.0.1:8080".parse().unwrap(),
        vec![],
        QueueState::default(),
    );

    let anchor = SyncAnchor {
        reference_time: 10_000,
        media_position_us: 20_000,
        playback_rate: 1.02,
        is_playing: true,
    };

    session
        .apply_message(Message::SyncAnchor(anchor.clone()))
        .unwrap();

    assert_eq!(session.latest_sync_anchor(), Some(anchor));
}

#[test]
fn leader_builds_sync_anchor_message_from_playback_status() {
    let leader_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let session = SessionState::new_leader("ROOM42".into(), leader_id, "TestUser".to_string());
    let status = PlaybackStatus {
        track_id: Some("track-1".into()),
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
