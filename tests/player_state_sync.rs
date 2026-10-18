use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use uuid::Uuid;

use synkrophase::clock::sync::ClockSync;
use synkrophase::config::SyncConfig;
use synkrophase::controller::MediaController;
use synkrophase::controller::mock::MockMediaController;
use synkrophase::protocol::messages::{
    Envelope, Message, PeerInfo, PlaybackAction, Role, deserialize,
};
use synkrophase::session::SessionState;
use synkrophase::session::broadcaster::LeaderAnchorBroadcaster;
use synkrophase::sync::controller::MediaControllerPlaybackAdapter;

#[tokio::test]
async fn test_leader_native_player_pause_broadcasts_intent_and_anchor() {
    let host_id = Uuid::new_v4();
    let host_session = Arc::new(SessionState::new_leader("ROOM1".into(), host_id, "Host".into()));

    let host_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let follower_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let follower_addr: SocketAddr = follower_socket.local_addr().unwrap();
    let follower_id = Uuid::new_v4();

    // Register follower as an active peer in host's session
    host_session.record_peer_heartbeat(
        PeerInfo {
            device_id: follower_id,
            name: "Follower".into(),
            clock_offset_us: 0,
            last_seen: 0,
            role: Role::Moderator,
        },
        follower_addr,
    );

    let host_clock_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let host_clock = Arc::new(ClockSync::new(
        host_clock_socket,
        SyncConfig::default(),
        host_id,
    ));

    let host_controller = Arc::new(MockMediaController::new());
    // Start with host playing
    host_controller.play().await.unwrap();
    host_controller.seek_to(10_000_000).await.unwrap();

    let host_playback = Arc::new(MediaControllerPlaybackAdapter::new(host_controller.clone()));
    let broadcaster = Arc::new(
        LeaderAnchorBroadcaster::new(
            host_session.clone(),
            host_clock.clone(),
            host_playback,
            SyncConfig::default(),
        )
        .with_controller(host_controller.clone()),
    );

    // Spawn broadcast loop
    let b = broadcaster.clone();
    let s = host_socket.clone();
    let loop_handle = tokio::spawn(async move {
        let _ = b.run_broadcast_loop(s, host_id).await;
    });

    // Allow initial poll loop to establish baseline
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Now, simulate user pausing in Spotify directly!
    host_controller.pause().await.unwrap();

    // The follower should receive a PlaybackAction::Pause Intent!
    let mut received_pause_intent = false;
    let mut buf = [0u8; 4096];
    let start = std::time::Instant::now();

    while start.elapsed() < Duration::from_secs(2) {
        if let Ok(Ok((len, _))) =
            tokio::time::timeout(Duration::from_millis(400), follower_socket.recv_from(&mut buf))
                .await
        {
            if let Ok(Envelope {
                payload: Message::Intent(intent),
                ..
            }) = deserialize(&buf[..len])
            {
                if matches!(intent.action, PlaybackAction::Pause) {
                    received_pause_intent = true;
                    break;
                }
            }
        }
    }

    loop_handle.abort();
    assert!(
        received_pause_intent,
        "Follower should have received PlaybackAction::Pause intent when leader paused native player"
    );
}

#[tokio::test]
async fn test_leader_native_player_resume_broadcasts_play_intent() {
    let host_id = Uuid::new_v4();
    let host_session = Arc::new(SessionState::new_leader("ROOM1".into(), host_id, "Host".into()));

    let host_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let follower_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let follower_addr: SocketAddr = follower_socket.local_addr().unwrap();
    let follower_id = Uuid::new_v4();

    host_session.record_peer_heartbeat(
        PeerInfo {
            device_id: follower_id,
            name: "Follower".into(),
            clock_offset_us: 0,
            last_seen: 0,
            role: Role::Moderator,
        },
        follower_addr,
    );

    let host_clock_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let host_clock = Arc::new(ClockSync::new(
        host_clock_socket,
        SyncConfig::default(),
        host_id,
    ));

    let host_controller = Arc::new(MockMediaController::new());
    // Start paused
    host_controller.pause().await.unwrap();
    host_controller.seek_to(10_000_000).await.unwrap();

    let host_playback = Arc::new(MediaControllerPlaybackAdapter::new(host_controller.clone()));
    let broadcaster = Arc::new(
        LeaderAnchorBroadcaster::new(
            host_session.clone(),
            host_clock.clone(),
            host_playback,
            SyncConfig::default(),
        )
        .with_controller(host_controller.clone()),
    );

    let b = broadcaster.clone();
    let s = host_socket.clone();
    let loop_handle = tokio::spawn(async move {
        let _ = b.run_broadcast_loop(s, host_id).await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Leader unpauses/plays directly in Spotify!
    host_controller.play().await.unwrap();

    let mut received_play_intent = false;
    let mut buf = [0u8; 4096];
    let start = std::time::Instant::now();

    while start.elapsed() < Duration::from_secs(2) {
        if let Ok(Ok((len, _))) =
            tokio::time::timeout(Duration::from_millis(400), follower_socket.recv_from(&mut buf))
                .await
        {
            if let Ok(Envelope {
                payload: Message::Intent(intent),
                ..
            }) = deserialize(&buf[..len])
            {
                if matches!(intent.action, PlaybackAction::Play) {
                    received_play_intent = true;
                    break;
                }
            }
        }
    }

    loop_handle.abort();
    assert!(
        received_play_intent,
        "Follower should have received PlaybackAction::Play intent when leader resumed native player"
    );
}

#[tokio::test]
async fn test_leader_native_player_scrub_broadcasts_seek_intent() {
    let host_id = Uuid::new_v4();
    let host_session = Arc::new(SessionState::new_leader("ROOM1".into(), host_id, "Host".into()));

    let host_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let follower_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let follower_addr: SocketAddr = follower_socket.local_addr().unwrap();
    let follower_id = Uuid::new_v4();

    host_session.record_peer_heartbeat(
        PeerInfo {
            device_id: follower_id,
            name: "Follower".into(),
            clock_offset_us: 0,
            last_seen: 0,
            role: Role::Moderator,
        },
        follower_addr,
    );

    let host_clock_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let host_clock = Arc::new(ClockSync::new(
        host_clock_socket,
        SyncConfig::default(),
        host_id,
    ));

    let host_controller = Arc::new(MockMediaController::new());
    host_controller.play().await.unwrap();
    host_controller.seek_to(10_000_000).await.unwrap();

    let host_playback = Arc::new(MediaControllerPlaybackAdapter::new(host_controller.clone()));
    let broadcaster = Arc::new(
        LeaderAnchorBroadcaster::new(
            host_session.clone(),
            host_clock.clone(),
            host_playback,
            SyncConfig::default(),
        )
        .with_controller(host_controller.clone()),
    );

    let b = broadcaster.clone();
    let s = host_socket.clone();
    let loop_handle = tokio::spawn(async move {
        let _ = b.run_broadcast_loop(s, host_id).await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Leader scrubs timeline forward by 30 seconds directly in Spotify!
    host_controller.seek_to(40_000_000).await.unwrap();

    let mut received_seek_intent = false;
    let mut buf = [0u8; 4096];
    let start = std::time::Instant::now();

    while start.elapsed() < Duration::from_secs(2) {
        if let Ok(Ok((len, _))) =
            tokio::time::timeout(Duration::from_millis(400), follower_socket.recv_from(&mut buf))
                .await
        {
            if let Ok(Envelope {
                payload: Message::Intent(intent),
                ..
            }) = deserialize(&buf[..len])
            {
                if let PlaybackAction::Seek { target_position_us } = intent.action {
                    if target_position_us == 40_000_000 {
                        received_seek_intent = true;
                        break;
                    }
                }
            }
        }
    }

    loop_handle.abort();
    assert!(
        received_seek_intent,
        "Follower should have received PlaybackAction::Seek intent when leader scrubbed timeline"
    );
}

#[tokio::test]
async fn test_moderator_zero_touch_pause_proposes_intent_to_leader() {
    let host_id = Uuid::new_v4();
    let host_session = Arc::new(SessionState::new_leader("ROOM1".into(), host_id, "Host".into()));

    let host_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let host_addr = host_socket.local_addr().unwrap();

    let follower_id = Uuid::new_v4();
    let follower_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let follower_addr = follower_socket.local_addr().unwrap();

    // Register follower as Moderator in host's session
    host_session.record_peer_heartbeat(
        PeerInfo {
            device_id: follower_id,
            name: "Moderator".into(),
            clock_offset_us: 0,
            last_seen: 0,
            role: Role::Moderator,
        },
        follower_addr,
    );

    let follower_session = Arc::new(SessionState::from_join(
        "ROOM1".into(),
        follower_id,
        "Moderator".into(),
        host_id,
        host_addr,
        vec![
            PeerInfo {
                device_id: host_id,
                name: "Host".into(),
                clock_offset_us: 0,
                last_seen: 0,
                role: Role::Leader,
            },
            PeerInfo {
                device_id: follower_id,
                name: "Moderator".into(),
                clock_offset_us: 0,
                last_seen: 0,
                role: Role::Moderator,
            },
        ],
        Default::default(),
    ));
    follower_session.set_role(Role::Moderator);

    let follower_controller = Arc::new(MockMediaController::new());
    follower_controller.play().await.unwrap();

    let follower_clock_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let follower_clock = Arc::new(ClockSync::new(
        follower_clock_socket,
        SyncConfig::default(),
        follower_id,
    ));

    let follower_scheduler = Arc::new(synkrophase::sync::scheduler::IntentScheduler::new(
        follower_clock.clone() as Arc<dyn synkrophase::sync::controller::ClockSource>,
        follower_controller.clone(),
    ));

    let (event_tx, _) = tokio::sync::mpsc::unbounded_channel();
    let (follower_engine, mut cmd_rx) = synkrophase::session::SynkroEngine::new(
        follower_session.clone(),
        follower_controller.clone(),
        follower_socket.clone(),
        Some(host_addr),
        follower_clock.clone(),
        follower_scheduler,
        event_tx,
        vec![],
    );
    let follower_engine = Arc::new(follower_engine);

    // Spawn command consumer
    let f_eng = follower_engine.clone();
    let cmd_task = tokio::spawn(async move {
        while let Some(cmd) = cmd_rx.recv().await {
            let _ = f_eng.execute_command(cmd).await;
        }
    });

    let monitor = synkrophase::daemon::FollowerZeroTouchMonitor::new(follower_engine.clone());
    let monitor_task = tokio::spawn(async move {
        let _ = monitor.run_monitor_loop().await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Moderator pauses local player!
    follower_controller.pause().await.unwrap();

    // Host socket should receive the proposed PlaybackAction::Pause Intent from the Moderator!
    let mut received_proposed_intent = false;
    let mut buf = [0u8; 4096];
    let start = std::time::Instant::now();

    while start.elapsed() < Duration::from_secs(2) {
        if let Ok(Ok((len, _))) =
            tokio::time::timeout(Duration::from_millis(400), host_socket.recv_from(&mut buf)).await
        {
            if let Ok(Envelope {
                sender,
                payload: Message::Intent(intent),
            }) = deserialize(&buf[..len])
            {
                if sender == follower_id && matches!(intent.action, PlaybackAction::Pause) {
                    received_proposed_intent = true;
                    break;
                }
            }
        }
    }

    cmd_task.abort();
    monitor_task.abort();
    assert!(
        received_proposed_intent,
        "Host should have received proposed Pause intent from Moderator"
    );
}

#[tokio::test]
async fn test_listener_intent_rejected_by_leader() {
    let host_id = Uuid::new_v4();
    let host_session = Arc::new(SessionState::new_leader("ROOM1".into(), host_id, "Host".into()));

    let host_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let host_addr = host_socket.local_addr().unwrap();

    let listener_id = Uuid::new_v4();
    let listener_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let listener_addr = listener_socket.local_addr().unwrap();

    // Register peer explicitly as Listener in host's session
    host_session.record_peer_heartbeat(
        PeerInfo {
            device_id: listener_id,
            name: "Listener".into(),
            clock_offset_us: 0,
            last_seen: 0,
            role: Role::Listener,
        },
        listener_addr,
    );

    let host_runtime = Arc::new(
        synkrophase::session::runtime::SessionMessageRuntime::new(
            host_session.clone(),
            "Host".into(),
        ),
    );

    // Spawn host runtime receive loop
    let hr = host_runtime.clone();
    let hs = host_socket.clone();
    let host_task = tokio::spawn(async move {
        let _ = hr.run_receive_loop(hs).await;
    });

    // Listener sends PlaybackAction::Play Intent directly to host
    let unauthorized_intent = Envelope {
        sender: listener_id,
        payload: Message::Intent(synkrophase::protocol::messages::PlaybackIntent {
            action: PlaybackAction::Play,
            target_ref_time: 0,
            position_us: 0,
            track_title: None,
        }),
    };
    let bytes = synkrophase::protocol::messages::serialize(&unauthorized_intent).unwrap();
    listener_socket.send_to(&bytes, host_addr).await.unwrap();

    // Listener should receive Permission Denied notification back from Leader
    let mut received_permission_denied = false;
    let mut buf = [0u8; 4096];
    let start = std::time::Instant::now();

    while start.elapsed() < Duration::from_secs(2) {
        if let Ok(Ok((len, _))) =
            tokio::time::timeout(Duration::from_millis(400), listener_socket.recv_from(&mut buf))
                .await
        {
            if let Ok(Envelope {
                payload: Message::Notification { text },
                ..
            }) = deserialize(&buf[..len])
            {
                if text.contains("Permission Denied: Only Moderators can control playback.") {
                    received_permission_denied = true;
                    break;
                }
            }
        }
    }

    host_task.abort();
    assert!(
        received_permission_denied,
        "Leader should have replied with Permission Denied notification to Listener"
    );
}



