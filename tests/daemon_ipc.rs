#![allow(clippy::collapsible_if)]

#[cfg(unix)]
mod unix_tests {
    use std::fs;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UdpSocket, UnixStream};
    use tokio::time::timeout;
    use uuid::Uuid;

    use synkrophase::clock::sync::ClockSync;
    use synkrophase::config::SyncConfig;
    use synkrophase::controller::mock::MockMediaController;
    use synkrophase::daemon::paths::DaemonPaths;
    use synkrophase::daemon::protocol::{DaemonStatusSnapshot, IpcEvent, IpcRequest, IpcResponse};
    use synkrophase::daemon::IpcServer;
    use synkrophase::protocol::messages::{PeerInfo, Role};
    use synkrophase::session::SessionState;
    use synkrophase::session::engine::SynkroEngine;
    use synkrophase::sync::scheduler::IntentScheduler;
    use synkrophase::tui::AppEvent;

    struct TestHarness {
        server: Arc<IpcServer>,
        #[allow(dead_code)]
        engine: Arc<SynkroEngine>,
        #[allow(dead_code)]
        controller: Arc<MockMediaController>,
        paths: DaemonPaths,
        _server_task: tokio::task::JoinHandle<()>,
    }

    impl Drop for TestHarness {
        fn drop(&mut self) {
            self.paths.clean_socket();
            self.paths.clean_pid();
            let _ = fs::remove_dir_all(&self.paths.base_dir);
        }
    }

    async fn spawn_test_server() -> TestHarness {
        let short_id = &Uuid::new_v4().to_string()[..8];
        let temp_dir = std::path::PathBuf::from(format!("/tmp/synkro-test-{short_id}"));
        fs::create_dir_all(&temp_dir).unwrap();

        let paths = DaemonPaths {
            socket_path: temp_dir.join("synkro.sock"),
            pid_path: temp_dir.join("daemon.pid"),
            log_path: temp_dir.join("daemon.log"),
            base_dir: temp_dir,
        };

        let self_id = Uuid::new_v4();
        let session = Arc::new(SessionState::new_leader("ROOM_TEST".into(), self_id, "TestDevice".into()));
        let controller = Arc::new(MockMediaController::new());
        let clock_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let clock = Arc::new(ClockSync::new(clock_socket, SyncConfig::default(), self_id));
        let scheduler = Arc::new(IntentScheduler::new(clock.clone() as _, controller.clone() as _));
        let session_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

        let (engine, mut command_rx) = SynkroEngine::new(
            session.clone(),
            controller.clone() as _,
            session_socket,
            None,
            clock,
            scheduler,
            event_tx,
            vec![],
        );
        let engine = Arc::new(engine);

        // Command receiver
        {
            let eng = engine.clone();
            tokio::spawn(async move {
                while let Some(cmd) = command_rx.recv().await {
                    let _ = eng.execute_command(cmd).await;
                }
            });
        }

        let (ipc_server, _) = IpcServer::new(paths.clone(), engine.clone());
        let server = Arc::new(ipc_server);

        // Event forwarder to IPC
        {
            let srv = server.clone();
            tokio::spawn(async move {
                while let Some(ev) = event_rx.recv().await {
                    srv.broadcast_app_event(&ev);
                }
            });
        }

        let server_task = {
            let srv = server.clone();
            tokio::spawn(async move {
                let _ = srv.run_server().await;
            })
        };

        // Wait briefly for socket to become available
        for _ in 0..50 {
            if paths.socket_path.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        TestHarness {
            server,
            engine,
            controller,
            paths,
            _server_task: server_task,
        }
    }

    #[tokio::test]
    async fn test_daemon_ipc_status_query() {
        let harness = spawn_test_server().await;
        let stream = UnixStream::connect(&harness.paths.socket_path).await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut lines = BufReader::new(reader).lines();

        let req = IpcRequest {
            id: Some(1),
            method: "status".to_string(),
            params: serde_json::Value::Null,
        };
        let mut line = serde_json::to_string(&req).unwrap();
        line.push('\n');
        writer.write_all(line.as_bytes()).await.unwrap();

        let resp_line = timeout(Duration::from_secs(2), lines.next_line())
            .await
            .expect("timeout")
            .expect("read error")
            .expect("empty response");

        let resp: IpcResponse = serde_json::from_str(&resp_line).unwrap();
        assert_eq!(resp.id, Some(1));
        assert!(resp.error.is_none());

        let snapshot: DaemonStatusSnapshot = serde_json::from_value(resp.result.unwrap()).unwrap();
        assert_eq!(snapshot.room_code, "ROOM_TEST");
        assert_eq!(snapshot.role, Role::Leader);
        assert_eq!(snapshot.device_name, "TestDevice");
    }

    #[tokio::test]
    async fn test_daemon_ipc_event_streaming() {
        let harness = spawn_test_server().await;
        let stream = UnixStream::connect(&harness.paths.socket_path).await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut lines = BufReader::new(reader).lines();

        // Subscribe to events
        let req = IpcRequest {
            id: Some(2),
            method: "subscribe".to_string(),
            params: serde_json::Value::Null,
        };
        let mut line = serde_json::to_string(&req).unwrap();
        line.push('\n');
        writer.write_all(line.as_bytes()).await.unwrap();

        let sub_resp = timeout(Duration::from_secs(2), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let resp: IpcResponse = serde_json::from_str(&sub_resp).unwrap();
        assert_eq!(resp.id, Some(2));

        // Broadcast a log event
        harness.server.broadcast_app_event(&AppEvent::Log {
            source: "Engine".into(),
            text: "Sync locked".into(),
        });

        let event_line = timeout(Duration::from_secs(2), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let ev: IpcEvent = serde_json::from_str(&event_line).unwrap();
        assert_eq!(ev.event, "log");
        assert_eq!(ev.data["source"], "Engine");
        assert_eq!(ev.data["text"], "Sync locked");

        // Broadcast a drift update event
        harness.server.broadcast_app_event(&AppEvent::DriftUpdate(
            15_000,
            1,
            "Locked (<80ms)".to_string(),
        ));

        let event_line2 = timeout(Duration::from_secs(2), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let ev2: IpcEvent = serde_json::from_str(&event_line2).unwrap();
        assert_eq!(ev2.event, "drift_update");
        assert_eq!(ev2.data["offset_us"], 15_000);
        assert_eq!(ev2.data["zone"], 1);

        // Broadcast a peer update event
        let peer = PeerInfo {
            device_id: Uuid::new_v4(),
            name: "Peer1".into(),
            clock_offset_us: 100,
            last_seen: 12345,
            role: Role::Moderator,
        };
        harness.server.broadcast_app_event(&AppEvent::PeerListUpdate(vec![peer.clone()]));

        let event_line3 = timeout(Duration::from_secs(2), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let ev3: IpcEvent = serde_json::from_str(&event_line3).unwrap();
        assert_eq!(ev3.event, "peer_update");
        assert_eq!(ev3.data[0]["name"], "Peer1");
    }

    #[tokio::test]
    async fn test_daemon_ipc_playback_actions() {
        let harness = spawn_test_server().await;
        let stream = UnixStream::connect(&harness.paths.socket_path).await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut lines = BufReader::new(reader).lines();

        // 1. Play
        let req = IpcRequest {
            id: Some(10),
            method: "play".to_string(),
            params: serde_json::Value::Null,
        };
        let mut line = serde_json::to_string(&req).unwrap();
        line.push('\n');
        writer.write_all(line.as_bytes()).await.unwrap();

        let resp_line = timeout(Duration::from_secs(2), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let resp: IpcResponse = serde_json::from_str(&resp_line).unwrap();
        assert_eq!(resp.id, Some(10));
        assert!(resp.error.is_none());

        // 2. Pause
        let req = IpcRequest {
            id: Some(11),
            method: "pause".to_string(),
            params: serde_json::Value::Null,
        };
        let mut line = serde_json::to_string(&req).unwrap();
        line.push('\n');
        writer.write_all(line.as_bytes()).await.unwrap();

        let resp_line = timeout(Duration::from_secs(2), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let resp: IpcResponse = serde_json::from_str(&resp_line).unwrap();
        assert_eq!(resp.id, Some(11));
        assert!(resp.error.is_none());

        // 3. Seek
        let req = IpcRequest {
            id: Some(12),
            method: "seek".to_string(),
            params: serde_json::json!({ "position_sec": 35.5 }),
        };
        let mut line = serde_json::to_string(&req).unwrap();
        line.push('\n');
        writer.write_all(line.as_bytes()).await.unwrap();

        let resp_line = timeout(Duration::from_secs(2), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let resp: IpcResponse = serde_json::from_str(&resp_line).unwrap();
        assert_eq!(resp.id, Some(12));
        assert_eq!(resp.result.unwrap()["position_sec"], 35.5);
    }

    #[tokio::test]
    async fn test_daemon_ipc_client_disconnect_leaves_daemon_alive() {
        let harness = spawn_test_server().await;

        // First client connects, subscribes, and drops
        {
            let stream = UnixStream::connect(&harness.paths.socket_path).await.unwrap();
            let (reader, mut writer) = stream.into_split();
            let mut lines = BufReader::new(reader).lines();

            let req = IpcRequest {
                id: Some(1),
                method: "subscribe".to_string(),
                params: serde_json::Value::Null,
            };
            let mut line = serde_json::to_string(&req).unwrap();
            line.push('\n');
            writer.write_all(line.as_bytes()).await.unwrap();
            let _ = timeout(Duration::from_secs(2), lines.next_line()).await.unwrap();
            // Drop client stream here
        }

        // Second client connects immediately after first client disconnected
        let stream2 = UnixStream::connect(&harness.paths.socket_path).await.unwrap();
        let (reader2, mut writer2) = stream2.into_split();
        let mut lines2 = BufReader::new(reader2).lines();

        let req2 = IpcRequest {
            id: Some(2),
            method: "status".to_string(),
            params: serde_json::Value::Null,
        };
        let mut line2 = serde_json::to_string(&req2).unwrap();
        line2.push('\n');
        writer2.write_all(line2.as_bytes()).await.unwrap();

        let resp_line = timeout(Duration::from_secs(2), lines2.next_line())
            .await
            .expect("timeout")
            .unwrap()
            .unwrap();
        let resp: IpcResponse = serde_json::from_str(&resp_line).unwrap();
        assert_eq!(resp.id, Some(2));
        assert!(resp.error.is_none());
    }

    #[tokio::test]
    async fn test_tui_backend_ipc_dispatch() {
        let harness = spawn_test_server().await;
        harness.paths.write_pid(std::process::id()).unwrap();

        let backend = synkrophase::tui::IpcBackend::with_paths(harness.paths.clone());
        use synkrophase::tui::TuiBackend;

        backend
            .send_playback_action(synkrophase::protocol::messages::PlaybackAction::Play)
            .await
            .unwrap();

        backend
            .send_playback_action(synkrophase::protocol::messages::PlaybackAction::Pause)
            .await
            .unwrap();

        backend
            .send_playback_action(synkrophase::protocol::messages::PlaybackAction::Seek {
                target_position_us: 12_500_000,
            })
            .await
            .unwrap();

        backend.send_volume(Some(75)).await.unwrap();
        backend.send_chat("Test chat from IPC backend".into()).await.unwrap();

        let inv = backend.get_invitation_command().await.unwrap();
        assert!(inv.is_some());
        assert!(inv.unwrap().contains("ROOM_TEST"));
    }

    #[tokio::test]
    async fn test_attached_tui_fails_when_daemon_not_running() {
        let short_id = &Uuid::new_v4().to_string()[..8];
        let temp_dir = std::path::PathBuf::from(format!("/tmp/synkro-test-{short_id}"));
        let paths = DaemonPaths {
            socket_path: temp_dir.join("synkro.sock"),
            pid_path: temp_dir.join("daemon.pid"),
            log_path: temp_dir.join("daemon.log"),
            base_dir: temp_dir,
        };

        let res = synkrophase::tui::run_attached_tui(paths).await;
        assert!(res.is_err());
        let err_msg = res.unwrap_err().to_string();
        assert!(err_msg.contains("No running Synkrophase daemon found"));
    }
}
