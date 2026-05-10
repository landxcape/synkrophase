use std::sync::{Arc, Mutex};
use std::time::Duration;

use synkrophase::error::Result;
use synkrophase::playback::engine::{PlaybackBackend, PlaybackEngine};
use synkrophase::protocol::messages::{
    Envelope, Message, deserialize, serialize, Role, PeerInfo,
};
use synkrophase::session::runtime::SessionMessageRuntime;
use synkrophase::session::SessionState;
use tokio::net::UdpSocket;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
enum BackendCall {
    Pause,
    Resume,
}

#[derive(Clone, Default)]
struct MockBackend {
    calls: Arc<Mutex<Vec<BackendCall>>>,
}

impl PlaybackBackend for MockBackend {
    fn load_and_play(&self, _stream_url: &str) -> Result<()> { Ok(()) }
    fn position_us(&self) -> i64 { 0 }
    fn set_rate(&self, _rate: f32) -> Result<()> { Ok(()) }
    fn seek(&self, _position_us: i64) -> Result<()> { Ok(()) }
    fn pause(&self) -> Result<()> {
        self.calls.lock().unwrap().push(BackendCall::Pause);
        Ok(())
    }
    fn resume(&self) -> Result<()> {
        self.calls.lock().unwrap().push(BackendCall::Resume);
        Ok(())
    }
    fn stop(&self) -> Result<()> { Ok(()) }
}

#[tokio::test]
async fn leader_broadcasts_notification_on_join() {
    let leader_id = Uuid::new_v4();
    let session = Arc::new(SessionState::new_leader("ROOM42".into(), leader_id, "Leader".to_string()));
    
    let leader_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let leader_addr = leader_socket.local_addr().unwrap();

    let existing_peer_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let existing_peer_addr = existing_peer_socket.local_addr().unwrap();
    let existing_peer_id = Uuid::new_v4();

    session.record_peer_heartbeat(
        PeerInfo {
            device_id: existing_peer_id,
            name: "Existing".to_string(),
            clock_offset_us: 0,
            last_seen: 0,
            role: Role::Listener,
        },
        existing_peer_addr,
    );

    let runtime = SessionMessageRuntime::new(Arc::clone(&session), None, "Leader".to_string());
    let runtime_socket = Arc::clone(&leader_socket);
    let handle = tokio::spawn(async move {
        let _ = runtime.run_receive_loop(runtime_socket).await;
    });

    let new_peer_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let new_peer_id = Uuid::new_v4();
    let join_req = Envelope {
        sender: new_peer_id,
        payload: Message::JoinRequest { room_code: "ROOM42".into(), name: "Newbie".into() },
    };
    new_peer_socket.send_to(&serialize(&join_req).unwrap(), leader_addr).await.unwrap();

    // Existing peer should receive notification
    let mut buf = [0u8; 2048];
    let (len, _) = tokio::time::timeout(Duration::from_millis(500), existing_peer_socket.recv_from(&mut buf))
        .await
        .expect("Timeout waiting for notification")
        .unwrap();

    let envelope = deserialize(&buf[..len]).unwrap();
    assert_eq!(envelope.sender, leader_id);
    if let Message::Notification { text } = envelope.payload {
        assert!(text.contains("Newbie joined the room"));
    } else {
        panic!("Expected Notification, got {:?}", envelope.payload);
    }

    handle.abort();
}

#[tokio::test]
async fn leader_broadcasts_chat_as_chatbroadcast() {
    let leader_id = Uuid::new_v4();
    let session = Arc::new(SessionState::new_leader("ROOM42".into(), leader_id, "Leader".to_string()));
    
    let leader_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let leader_addr = leader_socket.local_addr().unwrap();

    let peer_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let peer_addr = peer_socket.local_addr().unwrap();
    let peer_id = Uuid::new_v4();

    session.record_peer_heartbeat(
        PeerInfo {
            device_id: peer_id,
            name: "Alice".to_string(),
            clock_offset_us: 0,
            last_seen: 0,
            role: Role::Listener,
        },
        peer_addr,
    );

    let runtime = SessionMessageRuntime::new(Arc::clone(&session), None, "Leader".to_string());
    let runtime_socket = Arc::clone(&leader_socket);
    let handle = tokio::spawn(async move {
        let _ = runtime.run_receive_loop(runtime_socket).await;
    });

    // Alice sends chat
    let chat_msg = Envelope {
        sender: peer_id,
        payload: Message::Chat { sender: peer_id, name: "Alice".into(), text: "Hello!".into() },
    };
    peer_socket.send_to(&serialize(&chat_msg).unwrap(), leader_addr).await.unwrap();

    // Alice should receive ChatBroadcast (sent to her registered peer_addr)
    let mut buf = [0u8; 2048];
    let (len, _) = tokio::time::timeout(Duration::from_millis(1000), peer_socket.recv_from(&mut buf))
        .await
        .expect("Timeout waiting for ChatBroadcast")
        .unwrap();

    let envelope = deserialize(&buf[..len]).unwrap();
    assert_eq!(envelope.sender, leader_id);
    assert_eq!(envelope.payload, Message::ChatBroadcast { display_name: "Alice".into(), text: "Hello!".into() });

    handle.abort();
}

#[tokio::test]
async fn leader_enforces_playback_permissions() {
    let leader_id = Uuid::new_v4();
    let session = Arc::new(SessionState::new_leader("ROOM42".into(), leader_id, "Leader".to_string()));
    
    let leader_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let leader_addr = leader_socket.local_addr().unwrap();

    let backend = MockBackend::default();
    let playback = Arc::new(PlaybackEngine::new(Box::new(backend.clone())));
    let runtime = SessionMessageRuntime::new(Arc::clone(&session), None, "Leader".to_string()).with_playback(playback);
    
    let runtime_socket = Arc::clone(&leader_socket);
    let handle = tokio::spawn(async move {
        let _ = runtime.run_receive_loop(runtime_socket).await;
    });

    let listener_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let listener_id = Uuid::new_v4();
    let listener_addr = listener_socket.local_addr().unwrap();
    session.record_peer_heartbeat(
        PeerInfo {
            device_id: listener_id,
            name: "Listener".to_string(),
            clock_offset_us: 0,
            last_seen: 0,
            role: Role::Listener,
        },
        listener_addr,
    );

    // 1. Listener tries to Pause
    let pause_msg = Envelope {
        sender: listener_id,
        payload: Message::Pause { actor: listener_id },
    };
    listener_socket.send_to(&serialize(&pause_msg).unwrap(), leader_addr).await.unwrap();

    // Listener should receive "Permission Denied" Notification
    let mut buf = [0u8; 2048];
    let (len, _) = tokio::time::timeout(Duration::from_millis(1000), listener_socket.recv_from(&mut buf))
        .await
        .expect("Timeout waiting for Notification")
        .unwrap();

    let envelope = deserialize(&buf[..len]).unwrap();
    if let Message::Notification { text } = envelope.payload {
        assert!(text.contains("Permission Denied"));
    } else {
        panic!("Expected Notification, got {:?}", envelope.payload);
    }
    
    // Backend should NOT have been called
    assert!(backend.calls.lock().unwrap().is_empty());

    // 2. Promote to Moderator and try again
    session.record_peer_heartbeat(
        PeerInfo {
            device_id: listener_id,
            name: "Moderator".to_string(),
            clock_offset_us: 0,
            last_seen: 0,
            role: Role::Moderator,
        },
        listener_addr,
    );

    listener_socket.send_to(&serialize(&pause_msg).unwrap(), leader_addr).await.unwrap();
    
    // Give it a moment to process
    tokio::time::sleep(Duration::from_millis(200)).await;
    let calls = backend.calls.lock().unwrap();
    assert_eq!(calls.len(), 1, "Expected 1 backend call, got {:?}", *calls);
    assert_eq!(calls[0], BackendCall::Pause);

    handle.abort();
}
