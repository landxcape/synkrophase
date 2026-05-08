use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use synkrophase::clock::sync::ClockSync;
use synkrophase::config::SyncConfig;
use synkrophase::error::Result;
use synkrophase::playback::engine::{PlaybackBackend, PlaybackEngine};
use synkrophase::protocol::messages::{
    Envelope, Message, QueueState, StreamUrl, SyncAnchor, deserialize, serialize,
};
use synkrophase::session::runtime::{LeaderAnchorBroadcaster, SessionMessageRuntime};
use synkrophase::session::{FollowerSyncRuntime, SessionState};
use synkrophase::sync::controller::SyncController;
use tokio::net::UdpSocket;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
enum BackendCall {
    Load(String),
    Rate(f32),
    Seek(i64),
    Pause,
    Resume,
    Stop,
}

#[derive(Clone, Default)]
struct MockBackend {
    calls: Arc<Mutex<Vec<BackendCall>>>,
}

impl MockBackend {
    fn calls(&self) -> Vec<BackendCall> {
        self.calls.lock().unwrap().clone()
    }
}

impl PlaybackBackend for MockBackend {
    fn load_and_play(&self, stream_url: &str) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(BackendCall::Load(stream_url.to_string()));
        Ok(())
    }
    fn set_rate(&self, rate: f32) -> Result<()> {
        self.calls.lock().unwrap().push(BackendCall::Rate(rate));
        Ok(())
    }
    fn seek(&self, position_us: i64) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(BackendCall::Seek(position_us));
        Ok(())
    }
    fn pause(&self) -> Result<()> {
        self.calls.lock().unwrap().push(BackendCall::Pause);
        Ok(())
    }
    fn resume(&self) -> Result<()> {
        self.calls.lock().unwrap().push(BackendCall::Resume);
        Ok(())
    }
    fn stop(&self) -> Result<()> {
        self.calls.lock().unwrap().push(BackendCall::Stop);
        Ok(())
    }
}

#[tokio::test]
async fn process_envelope_applies_stream_and_sync_anchor() {
    let session = Arc::new(SessionState::from_join(
        "ROOM42".into(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        vec![],
        QueueState::default(),
    ));
    let clock_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let clock = Arc::new(ClockSync::new(
        clock_socket,
        SyncConfig::default(),
        Uuid::new_v4(),
    ));
    let playback = Arc::new(PlaybackEngine::new(Box::new(MockBackend::default())));
    let controller = Arc::new(SyncController::new(
        clock.clone(),
        playback,
        SyncConfig::default(),
    ));
    let follower = Arc::new(FollowerSyncRuntime::new(controller));

    let runtime = SessionMessageRuntime::new(Arc::clone(&session)).with_follower_sync(follower);
    let envelope = Envelope {
        sender: Uuid::new_v4(),
        payload: Message::SyncAnchor(SyncAnchor {
            reference_time: 5_000,
            media_position_us: 8_000,
            playback_rate: 1.02,
            is_playing: true,
        }),
    };
    runtime.process_envelope(envelope).unwrap();

    let stream_envelope = Envelope {
        sender: Uuid::new_v4(),
        payload: Message::StreamUrl(StreamUrl {
            track_id: "track-1".into(),
            url: "https://cdn.example.com/audio".into(),
            expires_at: 100,
        }),
    };
    runtime.process_envelope(stream_envelope).unwrap();

    assert!(session.latest_sync_anchor().is_some());
    assert!(session.stream_url_for("track-1").is_some());
}

#[tokio::test]
async fn runtime_handles_playback_controls_via_socket() {
    let leader_id = Uuid::new_v4();
    let session = Arc::new(SessionState::new_leader("ROOM42".into(), leader_id));

    let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let addr = socket.local_addr().unwrap();

    let backend = MockBackend::default();
    let playback = Arc::new(PlaybackEngine::new(Box::new(backend.clone())));
    playback.resume().unwrap(); // Start in playing state

    let runtime =
        SessionMessageRuntime::new(Arc::clone(&session)).with_playback(Arc::clone(&playback));

    let runtime_socket = Arc::clone(&socket);
    let handle = tokio::spawn(async move {
        let _ = runtime.run_receive_loop(runtime_socket).await;
    });

    let client_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    // Send Pause
    let env = Envelope {
        sender: Uuid::new_v4(),
        payload: Message::Pause,
    };
    let bytes = serialize(&env).unwrap();
    client_socket.send_to(&bytes, addr).await.unwrap();

    // Give it a moment to process
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!playback.is_playing());
    assert!(backend.calls().contains(&BackendCall::Pause));

    // Send Resume
    let env = Envelope {
        sender: Uuid::new_v4(),
        payload: Message::Resume,
    };
    let bytes = serialize(&env).unwrap();
    client_socket.send_to(&bytes, addr).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(playback.is_playing());
    assert!(backend.calls().contains(&BackendCall::Resume));

    handle.abort();
}

#[tokio::test]
async fn leader_broadcasts_playback_controls_to_peers() {
    let leader_id = Uuid::new_v4();
    let session = Arc::new(SessionState::new_leader("ROOM42".into(), leader_id));

    let leader_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let leader_addr = leader_socket.local_addr().unwrap();

    let peer_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = peer_socket.local_addr().unwrap();

    session.record_peer_heartbeat(
        synkrophase::protocol::messages::PeerInfo {
            device_id: Uuid::new_v4(),
            clock_offset_us: 0,
            last_seen: 0,
        },
        peer_addr,
    );

    let backend = MockBackend::default();
    let playback = Arc::new(PlaybackEngine::new(Box::new(backend)));
    let runtime =
        SessionMessageRuntime::new(Arc::clone(&session)).with_playback(Arc::clone(&playback));

    let runtime_socket = Arc::clone(&leader_socket);
    let handle = tokio::spawn(async move {
        let _ = runtime.run_receive_loop(runtime_socket).await;
    });

    let client_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    // Send Pause to leader from a "CLI" or another peer
    let env = Envelope {
        sender: Uuid::new_v4(),
        payload: Message::Pause,
    };
    let bytes = serialize(&env).unwrap();
    client_socket.send_to(&bytes, leader_addr).await.unwrap();

    // Peer should receive the broadcasted Pause from leader
    let mut buf = [0u8; 2048];
    let (len, _) = tokio::time::timeout(Duration::from_secs(1), peer_socket.recv_from(&mut buf))
        .await
        .expect("timeout waiting for broadcast")
        .unwrap();

    let envelope = deserialize(&buf[..len]).unwrap();
    assert_eq!(envelope.sender, leader_id);
    assert_eq!(envelope.payload, Message::Pause);

    handle.abort();
}

#[tokio::test]
async fn leader_anchor_broadcast_sends_sync_anchor_envelope() {
    let leader_id = Uuid::new_v4();
    let session = Arc::new(SessionState::new_leader("ROOM42".into(), leader_id));
    let peer_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer_addr: SocketAddr = peer_socket.local_addr().unwrap();
    session.record_peer_heartbeat(
        synkrophase::protocol::messages::PeerInfo {
            device_id: Uuid::new_v4(),
            clock_offset_us: 0,
            last_seen: 0,
        },
        peer_addr,
    );

    let clock_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let clock = Arc::new(ClockSync::new(
        clock_socket,
        SyncConfig::default(),
        leader_id,
    ));
    let playback = Arc::new(PlaybackEngine::new(Box::new(MockBackend::default())));
    playback
        .load_and_play("track-1", "https://cdn.example.com/audio")
        .unwrap();
    playback.seek(42_000).unwrap();
    playback.set_rate(0.98).unwrap();

    let broadcaster = LeaderAnchorBroadcaster::new(
        Arc::clone(&session),
        Arc::clone(&clock),
        Arc::clone(&playback),
        SyncConfig::default(),
    );
    let sender_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let sent = broadcaster
        .broadcast_once(&sender_socket, leader_id)
        .await
        .unwrap();
    assert_eq!(sent, 1);

    let mut buf = [0u8; 2048];
    let (len, _) = peer_socket.recv_from(&mut buf).await.unwrap();
    let envelope = deserialize(&buf[..len]).unwrap();
    match envelope.payload {
        Message::SyncAnchor(anchor) => {
            assert_eq!(anchor.media_position_us, 42_000);
            assert_eq!(anchor.playback_rate, 0.98);
            assert!(anchor.is_playing);
        }
        other => panic!("unexpected payload: {other:?}"),
    }
}
