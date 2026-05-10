use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use synkrophase::clock::sync::ClockSync;
use synkrophase::config::SyncConfig;
use synkrophase::playback::engine::{PlaybackBackend, PlaybackEngine};
use synkrophase::protocol::messages::{Envelope, Message, QueueCommand, QueueState, deserialize};
use synkrophase::session::SessionState;
use synkrophase::session::runtime::SessionMessageRuntime;
use tokio::net::UdpSocket;
use uuid::Uuid;

#[derive(Default)]
struct MockBackend;

impl PlaybackBackend for MockBackend {
    fn load_and_play(&self, _stream_url: &str) -> synkrophase::error::Result<()> {
        Ok(())
    }
    fn position_us(&self) -> i64 {
        0
    }
    fn set_rate(&self, _rate: f32) -> synkrophase::error::Result<()> {
        Ok(())
    }
    fn seek(&self, _position_us: i64) -> synkrophase::error::Result<()> {
        Ok(())
    }
    fn pause(&self) -> synkrophase::error::Result<()> {
        Ok(())
    }
    fn resume(&self) -> synkrophase::error::Result<()> {
        Ok(())
    }
    fn stop(&self) -> synkrophase::error::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn join_request_gets_join_accepted_and_queue_proposal_broadcasts_update() {
    let cfg = SyncConfig::default();
    let leader_id = Uuid::new_v4();
    let follower_id = Uuid::new_v4();

    let leader_session = Arc::new(SessionState::new_leader("ROOM42".into(), leader_id, "TestUser".to_string()));
    let leader_runtime = SessionMessageRuntime::new(Arc::clone(&leader_session), None, "TestUser".to_string());

    let leader_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    leader_socket.set_broadcast(true).unwrap();
    let leader_addr = leader_socket.local_addr().unwrap();

    // Keep clock sync responder alive to match expected runtime environment.
    let clock_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let clock = Arc::new(ClockSync::new(clock_socket, cfg.clone(), leader_id));
    let _clock_task = tokio::spawn(async move { clock.run_responder().await });

    let _playback = PlaybackEngine::new(Box::new(MockBackend));

    let receive_task = tokio::spawn({
        let socket = Arc::clone(&leader_socket);
        async move { leader_runtime.run_receive_loop(socket).await }
    });

    let follower_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    follower_socket.set_broadcast(true).unwrap();

    // Send JoinRequest to leader.
    let join = Envelope {
        sender: follower_id,
        payload: Message::JoinRequest {
            room_code: "ROOM42".into(),
            name: "Follower".into(),
        },
    };
    let bytes = synkrophase::protocol::messages::serialize(&join).unwrap();
    follower_socket.send_to(&bytes, leader_addr).await.unwrap();

    // Expect JoinAccepted back to follower.
    let mut buf = [0u8; 4096];
    let (len, _src) =
        tokio::time::timeout(Duration::from_secs(2), follower_socket.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
    let accepted = deserialize(&buf[..len]).unwrap();
    match accepted.payload {
        Message::JoinAccepted {
            peer_list,
            queue_state,
        } => {
            assert!(peer_list.iter().any(|p| p.device_id == leader_id));
            assert_eq!(queue_state, QueueState::default());
        }
        other => panic!("expected JoinAccepted, got {other:?}"),
    }

    // Record leader heartbeat so it has a usable addr for proposal reply/broadcast.
    leader_session.record_peer_heartbeat(
        synkrophase::protocol::messages::PeerInfo {
            device_id: follower_id,
            name: "TestUser".to_string(),
            clock_offset_us: 0,
            last_seen: 0,
        },
        follower_socket.local_addr().unwrap(),
    );

    // Send QueueProposal; expect QueueUpdate broadcast (to local broadcast port).
    let proposal = Envelope {
        sender: follower_id,
        payload: Message::QueueProposal(QueueCommand::Skip),
    };
    let bytes = synkrophase::protocol::messages::serialize(&proposal).unwrap();
    follower_socket.send_to(&bytes, leader_addr).await.unwrap();

    // Follower should receive a QueueUpdate soon (could be broadcast).
    let mut got_update = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Ok((len, _))) = tokio::time::timeout(
            Duration::from_millis(200),
            follower_socket.recv_from(&mut buf),
        )
        .await
        {
            let env = deserialize(&buf[..len]).unwrap();
            if matches!(env.payload, Message::QueueUpdate(_)) {
                got_update = true;
                break;
            }
        }
    }
    assert!(got_update, "expected to receive QueueUpdate after proposal");

    receive_task.abort();
}

#[tokio::test]
async fn heartbeats_update_peer_registry_and_election_can_flip_leader() {
    let mut cfg = SyncConfig::default();
    cfg.heartbeat_interval_ms = 10;
    cfg.heartbeat_timeout_ms = 30;

    let id_a = Uuid::parse_str("00000000-0000-0000-0000-000000000010").unwrap();
    let id_b = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();

    let session_a = Arc::new(SessionState::from_join(
        "ROOM42".into(),
        id_a,
        "TestUser".to_string(),
        id_b,
        "127.0.0.1:8080".parse().unwrap(),
        vec![synkrophase::protocol::messages::PeerInfo {
            device_id: id_b,
            name: "TestUser".to_string(),
            clock_offset_us: 0,
            last_seen: 0,
        }],
        QueueState::default(),
    ));
    let runtime_a = SessionMessageRuntime::new(Arc::clone(&session_a), None, "TestUser".to_string());

    let socket_a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    socket_a.set_broadcast(true).unwrap();
    let port = socket_a.local_addr().unwrap().port();

    let receive_task = tokio::spawn({
        let socket = Arc::clone(&socket_a);
        async move { runtime_a.run_receive_loop(socket).await }
    });

    // Send a leader heartbeat from B (simulated) then stop; A should eventually elect itself.
    let sender_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sender_socket.set_broadcast(true).unwrap();
    let leader_hb = Envelope {
        sender: id_b,
        payload: Message::Heartbeat {
            room_code: "ROOM42".into(),
            is_leader: true,
        },
    };
    let bytes = synkrophase::protocol::messages::serialize(&leader_hb).unwrap();
    sender_socket
        .send_to(&bytes, SocketAddr::from(([127, 0, 0, 1], port)))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(cfg.heartbeat_timeout_ms + 10)).await;
    session_a.prune_and_elect(Duration::from_millis(cfg.heartbeat_timeout_ms));
    assert!(
        session_a.is_leader(),
        "A should elect itself after leader timeout"
    );

    receive_task.abort();
}
