use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

#[cfg(unix)]
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader};
use tokio::net::UdpSocket;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::time::sleep;
use uuid::Uuid;

use crate::cli::args::{DaemonCommands, DaemonRunMode};
use crate::cli::handlers::common::{
    create_media_controller, resolve_join_target, run_heartbeat_loop, run_role_manager_loop,
    send_join_request,
};
use crate::clock::sync::ClockSync;
use crate::config::{DeviceConfig, SyncConfig};
use crate::daemon::paths::DaemonPaths;
#[cfg(unix)]
use crate::daemon::protocol::{DaemonStatusSnapshot, IpcRequest, IpcResponse};
use crate::daemon::{FollowerZeroTouchMonitor, IpcServer};
use crate::error::{Result, SynkroError};
use crate::protocol::messages::{QueueState, Role};
use crate::session::broadcaster::LeaderAnchorBroadcaster;
use crate::session::discovery::Discovery;
use crate::session::engine::SynkroEngine;
use crate::session::runtime::SessionMessageRuntime;
use crate::session::SessionState;
use crate::sync::evaluator::DriftEvaluator;
use crate::sync::scheduler::IntentScheduler;

pub async fn handle_daemon_command(
    cmd: DaemonCommands,
    device: DeviceConfig,
    sync_config: SyncConfig,
) -> Result<()> {
    let paths = DaemonPaths::default_paths()?;

    match cmd {
        DaemonCommands::Run { mode } => run_daemon_foreground(mode, device, sync_config, paths).await,
        DaemonCommands::Start { mode } => start_daemon_background(mode, &paths),
        DaemonCommands::Stop => stop_daemon(&paths).await,
        DaemonCommands::Status { json } => status_daemon(&paths, json).await,
        DaemonCommands::Logs { follow } => view_daemon_logs(&paths, follow),
    }
}

async fn run_daemon_foreground(
    mode: DaemonRunMode,
    device: DeviceConfig,
    sync_config: SyncConfig,
    paths: DaemonPaths,
) -> Result<()> {
    if paths.is_running() {
        return Err(SynkroError::Config(
            "Another Synkrophase daemon is already running. Run 'synkro daemon stop' first.".to_string(),
        ));
    }

    paths.write_pid(std::process::id())?;
    tracing::info!(pid = std::process::id(), "Starting Synkrophase daemon in foreground");

    let (engine, ipc_server, abort_handles) = match mode {
        DaemonRunMode::Host {
            room_code,
            clock_port,
            session_port,
        } => {
            setup_host_daemon(device, sync_config, room_code, clock_port, session_port, paths.clone()).await?
        }
        DaemonRunMode::Join {
            room_code,
            leader_addr,
            leader_id,
            leader_clock_port,
            session_port,
        } => {
            setup_join_daemon(
                device,
                sync_config,
                room_code,
                leader_addr,
                leader_id,
                leader_clock_port,
                session_port,
                paths.clone(),
            )
            .await?
        }
    };

    let ipc_task = {
        let srv = Arc::clone(&ipc_server);
        tokio::spawn(async move {
            let _ = srv.run_server().await;
        })
    };

    // Listen for OS signals (SIGINT, SIGTERM)
    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| SynkroError::Network(std::io::Error::other(e.to_string())))?;

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl+C / SIGINT, stopping daemon");
        }
        _ = async {
            #[cfg(unix)]
            {
                sigterm.recv().await;
            }
            #[cfg(not(unix))]
            {
                std::future::pending::<()>().await;
            }
        } => {
            tracing::info!("Received SIGTERM, stopping daemon");
        }
    }

    ipc_task.abort();
    for h in abort_handles {
        h.abort();
    }
    let _ = engine.shutdown().await;

    paths.clean_pid();
    paths.clean_socket();
    tracing::info!("Daemon cleanly stopped");
    Ok(())
}

fn start_daemon_background(mode: DaemonRunMode, paths: &DaemonPaths) -> Result<()> {
    if paths.is_running() {
        println!("Synkrophase daemon is already running (PID: {}).", paths.read_pid()?.unwrap_or_default());
        return Ok(());
    }

    paths.ensure_dir_exists()?;
    let exe = std::env::current_exe().map_err(|e| SynkroError::Config(e.to_string()))?;

    let mut args = vec!["daemon".to_string(), "run".to_string()];
    match mode {
        DaemonRunMode::Host {
            room_code,
            clock_port,
            session_port,
        } => {
            args.push("host".to_string());
            if let Some(r) = room_code {
                args.push(r);
            }
            args.push("--clock-port".to_string());
            args.push(clock_port.to_string());
            args.push("--session-port".to_string());
            args.push(session_port.to_string());
        }
        DaemonRunMode::Join {
            room_code,
            leader_addr,
            leader_id,
            leader_clock_port,
            session_port,
        } => {
            args.push("join".to_string());
            args.push(room_code);
            if let Some(addr) = leader_addr {
                args.push("--leader-addr".to_string());
                args.push(addr.to_string());
            }
            if let Some(id) = leader_id {
                args.push("--leader-id".to_string());
                args.push(id.to_string());
            }
            args.push("--leader-clock-port".to_string());
            args.push(leader_clock_port.to_string());
            args.push("--session-port".to_string());
            args.push(session_port.to_string());
        }
    }

    let log_file = File::options()
        .create(true)
        .append(true)
        .open(&paths.log_path)
        .map_err(|e| SynkroError::Config(format!("Could not open log file: {e}")))?;

    let child = Command::new(exe)
        .args(&args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file.try_clone().unwrap()))
        .stderr(Stdio::from(log_file))
        .spawn()
        .map_err(|e| SynkroError::Config(format!("Failed to spawn daemon process: {e}")))?;

    println!("Synkrophase daemon started in background (PID: {}).", child.id());
    println!("Logs: {:?}", paths.log_path);
    println!("Socket: {:?}", paths.socket_path);
    Ok(())
}

async fn stop_daemon(paths: &DaemonPaths) -> Result<()> {
    if !paths.is_running() {
        println!("No running Synkrophase daemon found.");
        paths.clean_pid();
        paths.clean_socket();
        return Ok(());
    }

    // Try IPC shutdown first
    #[cfg(unix)]
    if let Ok(mut stream) = UnixStream::connect(&paths.socket_path).await {
        let req = IpcRequest {
            id: Some(1),
            method: "shutdown".to_string(),
            params: serde_json::Value::Null,
        };
        let mut msg = serde_json::to_string(&req).unwrap_or_default();
        msg.push('\n');
        let _ = stream.write_all(msg.as_bytes()).await;
        sleep(Duration::from_millis(300)).await;
    }

    if let Ok(Some(pid)) = paths.read_pid() {
        #[cfg(unix)]
        {
            if unsafe { libc::kill(pid as libc::pid_t, 0) == 0 } {
                unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
                sleep(Duration::from_millis(500)).await;
            }
        }
        #[cfg(not(unix))]
        {
            let _ = pid;
        }
    }

    paths.clean_pid();
    paths.clean_socket();
    println!("Synkrophase daemon stopped.");
    Ok(())
}

async fn status_daemon(paths: &DaemonPaths, json: bool) -> Result<()> {
    if !paths.is_running() {
        if json {
            println!("{}", serde_json::json!({"running": false}));
        } else {
            println!("Synkrophase daemon is not running.");
        }
        return Ok(());
    }

    #[cfg(not(unix))]
    {
        if json {
            println!("{}", serde_json::json!({"running": true, "note": "IPC status inspection is only supported on Unix systems"}));
        } else {
            println!("Synkrophase daemon is running (PID: {:?}). IPC socket inspection is only supported on Unix.", paths.read_pid()?);
        }
        Ok(())
    }

    #[cfg(unix)]
    {
        let Ok(mut stream) = UnixStream::connect(&paths.socket_path).await else {
            if json {
                println!("{}", serde_json::json!({"running": false, "error": "Cannot connect to IPC socket"}));
            } else {
                println!("Synkrophase daemon is running (PID: {:?}), but IPC socket is unresponsive.", paths.read_pid()?);
            }
            return Ok(());
        };

        let req = IpcRequest {
            id: Some(1),
            method: "status".to_string(),
            params: serde_json::Value::Null,
        };
    let mut msg = serde_json::to_string(&req).unwrap_or_default();
    msg.push('\n');
    stream.write_all(msg.as_bytes()).await?;

    let (reader, _) = stream.into_split();
    let mut lines = TokioBufReader::new(reader).lines();
    if let Some(line) = lines.next_line().await? {
        let resp: IpcResponse = serde_json::from_str(&line)
            .map_err(|e| SynkroError::Deserialization(e.to_string()))?;
        if let Some(res) = resp.result {
            if json {
                println!("{}", serde_json::to_string_pretty(&res).unwrap_or_default());
            } else {
                let snapshot: DaemonStatusSnapshot = serde_json::from_value(res)
                    .map_err(|e| SynkroError::Deserialization(e.to_string()))?;
                println!("\n=== Synkrophase Daemon Status ===");
                println!("  Room Code:      {}", snapshot.room_code);
                println!("  Role:           {:?}", snapshot.role);
                println!("  Device Name:    {}", snapshot.device_name);
                println!("  Is Playing:     {}", if snapshot.is_playing { "Yes" } else { "No" });
                if let Some(title) = snapshot.track_title {
                    let artist = snapshot.track_artist.unwrap_or_else(|| "Unknown".into());
                    println!("  Track:          {} - {}", title, artist);
                }
                println!("  Position:       {:.2}s", snapshot.position_sec);
                println!("  Clock Offset:   {:+0.2}ms", (snapshot.clock_offset_us as f64) / 1000.0);
                println!("  Drift Status:   {} ({:+0.2}ms)", snapshot.drift_status, (snapshot.drift_offset_us as f64) / 1000.0);
                println!("  Peers Online:   {}", snapshot.peers.len());
                for peer in snapshot.peers {
                    println!("    - {} ({:?}) offset: {:+0.2}ms", peer.name, peer.role, (peer.clock_offset_us as f64) / 1000.0);
                }
                println!();
            }
        }
    }

    Ok(())
    }
}

fn view_daemon_logs(paths: &DaemonPaths, follow: bool) -> Result<()> {
    if !paths.log_path.exists() {
        println!("No log file found at {:?}", paths.log_path);
        return Ok(());
    }

    if follow {
        #[cfg(unix)]
        {
            let _ = Command::new("tail")
                .arg("-f")
                .arg(&paths.log_path)
                .status();
        }
    } else {
        let file = File::open(&paths.log_path).map_err(|e| SynkroError::Config(e.to_string()))?;
        let reader = BufReader::new(file);
        for line in reader.lines().map_while(std::result::Result::ok) {
            println!("{line}");
        }
    }
    Ok(())
}

// Host & Join setup helpers for daemon mode
#[allow(clippy::too_many_arguments)]
async fn setup_host_daemon(
    device: DeviceConfig,
    sync_config: SyncConfig,
    room_code: Option<String>,
    clock_port: u16,
    session_port: u16,
    paths: DaemonPaths,
) -> Result<(Arc<SynkroEngine>, Arc<IpcServer>, Vec<tokio::task::AbortHandle>)> {
    let room = room_code.unwrap_or_else(|| format!("ROOM{}", device.device_id.to_string()[..4].to_uppercase()));
    let clock_socket = UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, clock_port))).await?;
    let session_socket = Arc::new({
        let s = UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, session_port))).await?;
        s.set_broadcast(true)?;
        s
    });

    let clock = Arc::new(ClockSync::new(clock_socket, sync_config.clone(), device.device_id));
    let session = Arc::new(SessionState::new_leader(room.clone(), device.device_id, device.name.clone()));
    let controller = create_media_controller();
    let scheduler = Arc::new(IntentScheduler::new(Arc::clone(&clock) as _, Arc::clone(&controller)));
    let playback: Arc<dyn crate::sync::controller::PlaybackControl> = Arc::new(crate::sync::controller::NoopPlaybackControl::default());
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    let broadcaster = Arc::new(
        LeaderAnchorBroadcaster::new(
            Arc::clone(&session),
            Arc::clone(&clock),
            Arc::clone(&playback),
            sync_config.clone(),
        )
        .with_controller(Arc::clone(&controller)),
    );

    let discovery = Arc::new(Discovery::new()?);
    let _ = discovery.register_session(&room, device.device_id, session_port);

    let clock_task = tokio::spawn({
        let c = Arc::clone(&clock);
        async move { c.run_responder().await }
    });

    let broadcast_task = tokio::spawn({
        let b = Arc::clone(&broadcaster);
        let s = Arc::clone(&session_socket);
        let id = device.device_id;
        async move { b.run_broadcast_loop(s, id).await }
    });

    let heartbeat_task = tokio::spawn({
        let sess = Arc::clone(&session);
        let sock = Arc::clone(&session_socket);
        let id = device.device_id;
        let r = room.clone();
        let cfg = sync_config.clone();
        let clk = Arc::clone(&clock);
        async move { run_heartbeat_loop(sess, sock, id, r, None, cfg, Some(clk)).await }
    });

    let role_task = tokio::spawn({
        let sess = Arc::clone(&session);
        let sock = Arc::clone(&session_socket);
        let id = device.device_id;
        let cfg = sync_config.clone();
        async move { run_role_manager_loop(sess, sock, id, cfg).await }
    });

    let runtime = SessionMessageRuntime::new(Arc::clone(&session), device.name.clone())
        .with_event_tx(event_tx.clone())
        .with_controller(Arc::clone(&controller))
        .with_clock(Arc::clone(&clock))
        .with_scheduler(Arc::clone(&scheduler));

    let recv_task = tokio::spawn({
        let sock = Arc::clone(&session_socket);
        async move { runtime.run_receive_loop(sock).await }
    });

    let abort_handles = vec![
        clock_task.abort_handle(),
        broadcast_task.abort_handle(),
        heartbeat_task.abort_handle(),
        role_task.abort_handle(),
        recv_task.abort_handle(),
    ];

    let (engine, mut command_rx) = SynkroEngine::new(
        Arc::clone(&session),
        Arc::clone(&controller),
        Arc::clone(&session_socket),
        None,
        Arc::clone(&clock),
        Arc::clone(&scheduler),
        event_tx.clone(),
        abort_handles.clone(),
    );
    let engine = Arc::new(engine);

    // Command receiver loop
    {
        let eng = Arc::clone(&engine);
        tokio::spawn(async move {
            while let Some(cmd) = command_rx.recv().await {
                let _ = eng.execute_command(cmd).await;
            }
        });
    }

    let (ipc_server, _) = IpcServer::new(paths, Arc::clone(&engine));
    let ipc_server = Arc::new(ipc_server);

    // Event forwarder to IPC
    {
        let srv = Arc::clone(&ipc_server);
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                if let crate::tui::AppEvent::DriftUpdate(offset, zone, status) = event {
                    srv.record_drift_update(offset, zone, status);
                }
            }
        });
    }

    Ok((engine, ipc_server, abort_handles))
}

#[allow(clippy::too_many_arguments)]
async fn setup_join_daemon(
    device: DeviceConfig,
    sync_config: SyncConfig,
    room_code: String,
    leader_addr: Option<SocketAddr>,
    leader_id: Option<Uuid>,
    leader_clock_port: u16,
    session_port: u16,
    paths: DaemonPaths,
) -> Result<(Arc<SynkroEngine>, Arc<IpcServer>, Vec<tokio::task::AbortHandle>)> {
    let (resolved_leader_addr, resolved_leader_id) =
        resolve_join_target(&room_code, leader_addr, leader_id)?;

    let clock_socket = UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))).await?;
    let clock = Arc::new(ClockSync::new(clock_socket, sync_config.clone(), device.device_id));

    let session = Arc::new(SessionState::from_join(
        room_code.clone(),
        device.device_id,
        device.name.clone(),
        resolved_leader_id,
        resolved_leader_addr,
        vec![crate::protocol::messages::PeerInfo {
            device_id: resolved_leader_id,
            name: "Leader".into(),
            clock_offset_us: 0,
            last_seen: 0,
            role: Role::Leader,
        }],
        QueueState::default(),
    ));

    let leader_clock_addr = SocketAddr::new(resolved_leader_addr.ip(), leader_clock_port);
    if let Ok(offset) = clock.measure_offset(leader_clock_addr).await {
        session.set_clock_offset(offset);
    }

    let session_socket = Arc::new({
        let s = UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, session_port))).await?;
        s.set_broadcast(true)?;
        s
    });

    let controller = create_media_controller();
    let scheduler = Arc::new(IntentScheduler::new(Arc::clone(&clock) as _, Arc::clone(&controller)));
    let drift_evaluator = Arc::new(DriftEvaluator::new(Arc::clone(&clock) as _, Arc::clone(&controller), 50_000));
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    send_join_request(&session_socket, device.device_id, resolved_leader_addr, room_code.clone(), device.name.clone()).await?;

    let clock_task = tokio::spawn({
        let c = Arc::clone(&clock);
        async move { c.run_responder().await }
    });

    let drift_task = tokio::spawn({
        let eval = Arc::clone(&drift_evaluator);
        let sess = Arc::clone(&session);
        async move {
            loop {
                sleep(Duration::from_millis(1000)).await;
                if let Some(anchor) = sess.latest_sync_anchor() {
                    let _ = eval.evaluate_and_reconcile(&anchor).await;
                }
            }
        }
    });

    let clock_refresh_task = tokio::spawn({
        let clk = Arc::clone(&clock);
        let sess = Arc::clone(&session);
        async move {
            loop {
                sleep(Duration::from_secs(10)).await;
                if let Ok(_offset) = clk.measure_offset(leader_clock_addr).await {
                    sess.set_clock_offset(clk.residual_offset());
                }
            }
        }
    });

    let heartbeat_task = tokio::spawn({
        let sess = Arc::clone(&session);
        let sock = Arc::clone(&session_socket);
        let id = device.device_id;
        let r = room_code.clone();
        let cfg = sync_config.clone();
        let clk = Arc::clone(&clock);
        async move { run_heartbeat_loop(sess, sock, id, r, Some(resolved_leader_addr), cfg, Some(clk)).await }
    });

    let role_task = tokio::spawn({
        let sess = Arc::clone(&session);
        let sock = Arc::clone(&session_socket);
        let id = device.device_id;
        let cfg = sync_config.clone();
        async move { run_role_manager_loop(sess, sock, id, cfg).await }
    });

    let runtime = SessionMessageRuntime::new(Arc::clone(&session), device.name.clone())
        .with_event_tx(event_tx.clone())
        .with_controller(Arc::clone(&controller))
        .with_clock(Arc::clone(&clock))
        .with_scheduler(Arc::clone(&scheduler))
        .with_drift_evaluator(Arc::clone(&drift_evaluator));

    let recv_task = tokio::spawn({
        let sock = Arc::clone(&session_socket);
        async move { runtime.run_receive_loop(sock).await }
    });

    let abort_handles = vec![
        clock_task.abort_handle(),
        drift_task.abort_handle(),
        clock_refresh_task.abort_handle(),
        heartbeat_task.abort_handle(),
        role_task.abort_handle(),
        recv_task.abort_handle(),
    ];

    let (engine, mut command_rx) = SynkroEngine::new(
        Arc::clone(&session),
        Arc::clone(&controller),
        Arc::clone(&session_socket),
        Some(resolved_leader_addr),
        Arc::clone(&clock),
        Arc::clone(&scheduler),
        event_tx.clone(),
        abort_handles.clone(),
    );
    let engine = Arc::new(engine);

    // Command receiver loop
    {
        let eng = Arc::clone(&engine);
        tokio::spawn(async move {
            while let Some(cmd) = command_rx.recv().await {
                let _ = eng.execute_command(cmd).await;
            }
        });
    }

    // Follower zero-touch monitor for Moderator local player changes
    let zero_touch_monitor = FollowerZeroTouchMonitor::new(Arc::clone(&engine));
    tokio::spawn(async move {
        let _ = zero_touch_monitor.run_monitor_loop().await;
    });

    let (ipc_server, _) = IpcServer::new(paths, Arc::clone(&engine));
    let ipc_server = Arc::new(ipc_server);

    // Event forwarder to IPC
    {
        let srv = Arc::clone(&ipc_server);
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                if let crate::tui::AppEvent::DriftUpdate(offset, zone, status) = event {
                    srv.record_drift_update(offset, zone, status);
                }
            }
        });
    }

    Ok((engine, ipc_server, abort_handles))
}
