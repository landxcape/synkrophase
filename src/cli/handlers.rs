use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::sleep;
use uuid::Uuid;

use crate::clock::sync::ClockSync;
use crate::config::{DeviceConfig, SyncConfig};
use crate::controller::MediaController;
#[cfg(target_os = "macos")]
use crate::controller::macos::MacOsMediaController;
#[cfg(not(target_os = "macos"))]
use crate::controller::mock::MockMediaController;
use crate::error::{Result, SynkroError};
use crate::protocol::messages::{Envelope, Message, QueueState, serialize};
use crate::session::discovery::Discovery;
use crate::session::runtime::{LeaderAnchorBroadcaster, SessionMessageRuntime};
use crate::session::SessionState;
use crate::sync::controller::{NoopPlaybackControl, PlaybackControl};
use crate::sync::evaluator::DriftEvaluator;
use crate::sync::scheduler::IntentScheduler;
use super::repl::{run_follower_repl, run_host_repl};

pub fn create_media_controller() -> Arc<dyn MediaController> {
    #[cfg(target_os = "macos")]
    {
        Arc::new(MacOsMediaController::new())
    }
    #[cfg(not(target_os = "macos"))]
    {
        Arc::new(MockMediaController::new())
    }
}

pub fn resolve_join_target(
    room_code: &str,
    leader_addr: Option<SocketAddr>,
    leader_id: Option<Uuid>,
) -> Result<(SocketAddr, Uuid)> {
    if let (Some(addr), Some(id)) = (leader_addr, leader_id) {
        return Ok((addr, id));
    }

    let discovery = Discovery::new()?;
    let discovered = discovery.find_sessions()?;
    let found = discovered.into_iter().find(|info| info.room_code == room_code);

    if let Some(info) = found {
        let addr = leader_addr.unwrap_or(info.leader_addr);
        let id = leader_id.unwrap_or(info.leader_id);
        return Ok((addr, id));
    }

    if let Some(addr) = leader_addr {
        return Ok((addr, leader_id.unwrap_or_else(Uuid::new_v4)));
    }

    Err(SynkroError::Network(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("Could not discover room {room_code}"),
    )))
}

pub async fn send_join_request(
    socket: &UdpSocket,
    sender: Uuid,
    leader_addr: SocketAddr,
    room_code: String,
    name: String,
) -> Result<()> {
    let envelope = Envelope {
        sender,
        payload: Message::JoinRequest { room_code, name },
    };
    let bytes = serialize(&envelope)?;
    socket.send_to(&bytes, leader_addr).await?;
    Ok(())
}

pub async fn run_host(
    device: DeviceConfig,
    sync_config: SyncConfig,
    room_code: String,
    clock_port: u16,
    session_port: u16,
    headless: bool,
) -> Result<()> {
    let clock_socket =
        UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, clock_port)))
            .await?;
    let session_socket = Arc::new(
        UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::UNSPECIFIED,
            session_port,
        )))
        .await?,
    );
    session_socket.set_broadcast(true)?;

    let clock = Arc::new(ClockSync::new(
        clock_socket,
        sync_config.clone(),
        device.device_id,
    ));
    let session = Arc::new(SessionState::new_leader(
        room_code.clone(),
        device.device_id,
        device.name.clone(),
    ));

    let controller = create_media_controller();
    let scheduler = Arc::new(IntentScheduler::new(
        Arc::clone(&clock) as Arc<dyn crate::sync::controller::ClockSource>,
        Arc::clone(&controller),
    ));

    let playback: Arc<dyn PlaybackControl> = Arc::new(NoopPlaybackControl::default());

    let discovery = Discovery::new()?;
    discovery.register_session(&room_code, device.device_id, session_port)?;

    let local_ip = local_ip_address::local_ip()
        .unwrap_or(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));

    let clock_task = {
        let clock = Arc::clone(&clock);
        tokio::spawn(async move { clock.run_responder().await })
    };

    let broadcaster = LeaderAnchorBroadcaster::new(
        Arc::clone(&session),
        Arc::clone(&clock),
        Arc::clone(&playback),
        sync_config.clone(),
    )
    .with_controller(Arc::clone(&controller));

    let broadcast_task = tokio::spawn({
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        async move { broadcaster.run_broadcast_loop(socket, sender).await }
    });

    let heartbeat_task = tokio::spawn({
        let session = Arc::clone(&session);
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        let cfg = sync_config.clone();
        let room = room_code.clone();
        async move { run_heartbeat_loop(session, socket, sender, room, None, cfg).await }
    });

    let role_manager_task = tokio::spawn({
        let session = Arc::clone(&session);
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        let cfg = sync_config.clone();
        async move { run_role_manager_loop(session, socket, sender, cfg).await }
    });

    if !headless {
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();

        let runtime = SessionMessageRuntime::new(
            Arc::clone(&session),
            None,
            device.name.clone(),
        )
        .with_event_tx(event_tx.clone())
        .with_playback(Arc::clone(&playback))
        .with_scheduler(Arc::clone(&scheduler))
        .with_controller(Arc::clone(&controller))
        .with_clock(Arc::clone(&clock));

        let receive_task = tokio::spawn({
            let socket = Arc::clone(&session_socket);
            async move { runtime.run_receive_loop(socket).await }
        });

        // Initial welcome log in TUI
        let _ = event_tx.send(crate::tui::AppEvent::Log(
            format!("Room: {room_code} | Session: {session_port} | Clock: {clock_port}"),
            "System".into(),
        ));
        let _ = event_tx.send(crate::tui::AppEvent::Log(
            format!("Join: synkro join {room_code} --leader-addr {local_ip}:{session_port}"),
            "System".into(),
        ));

        let tui_res = crate::tui::run_tui(
            room_code,
            device.name,
            device.device_id,
            crate::protocol::messages::Role::Leader,
            Arc::clone(&session),
            Arc::clone(&controller),
            Arc::clone(&session_socket),
            None,
            Arc::clone(&clock),
            Arc::clone(&scheduler),
            event_rx,
            event_tx,
        )
        .await;

        // Graceful departure notification
        let leave_env = Envelope {
            sender: device.device_id,
            payload: Message::PeerLeft(device.device_id),
        };
        if let Ok(bytes) = serialize(&leave_env) {
            for addr in session.peer_socket_addrs() {
                let _ = session_socket.send_to(&bytes, addr).await;
            }
        }

        let _ = discovery.unregister();
        receive_task.abort();
        clock_task.abort();
        broadcast_task.abort();
        heartbeat_task.abort();
        role_manager_task.abort();

        return tui_res;
    }

    // Headless REPL mode
    let (rl, stdout) = rustyline_async::Readline::new("synkro> ".to_string()).unwrap();
    let runtime = SessionMessageRuntime::new(
        Arc::clone(&session),
        Some(stdout.clone()),
        device.name.clone(),
    )
    .with_playback(Arc::clone(&playback))
    .with_scheduler(Arc::clone(&scheduler))
    .with_controller(Arc::clone(&controller))
    .with_clock(Arc::clone(&clock));

    crate::session::runtime::print_event(
        Some(&stdout),
        &format!(
            "Hosting room {room_code} as leader {} on {local_ip}\n  - Session Port: {session_port}\n  - Clock Port: {clock_port}\n\nJoin with: synkro join {room_code} --leader-addr {local_ip}:{session_port}",
            device.device_id
        ),
    );

    let receive_task = tokio::spawn({
        let socket = Arc::clone(&session_socket);
        async move { runtime.run_receive_loop(socket).await }
    });

    let repl_task = tokio::spawn({
        let session = Arc::clone(&session);
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        let name = device.name.clone();
        let clock = Arc::clone(&clock);
        let controller = Arc::clone(&controller);
        let scheduler = Arc::clone(&scheduler);
        let stdout = stdout.clone();
        async move {
            run_host_repl(
                rl, session, socket, sender, name, clock, controller, scheduler, stdout,
            )
            .await
        }
    });

    tokio::select! {
        res = receive_task => {
            if let Err(err) = res {
                eprintln!("receive task failed: {err}");
            }
        }
        res = clock_task => {
            if let Err(err) = res {
                eprintln!("clock responder failed: {err}");
            }
        }
        res = broadcast_task => {
            if let Err(err) = res {
                eprintln!("broadcast task failed: {err}");
            }
        }
        res = heartbeat_task => {
            if let Err(err) = res {
                eprintln!("heartbeat task failed: {err}");
            }
        }
        res = role_manager_task => {
            if let Err(err) = res {
                eprintln!("role manager failed: {err}");
            }
        }
        res = repl_task => {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => eprintln!("repl loop stopped: {err}"),
                Err(err) => eprintln!("repl task cancelled: {err}"),
            }
        }
        _ = tokio::signal::ctrl_c() => {
            println!("Shutting down host.");
        }
    }

    // Graceful departure notification
    let leave_env = Envelope {
        sender: device.device_id,
        payload: Message::PeerLeft(device.device_id),
    };
    if let Ok(bytes) = serialize(&leave_env) {
        for addr in session.peer_socket_addrs() {
            let _ = session_socket.send_to(&bytes, addr).await;
        }
    }

    let _ = discovery.unregister();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn run_join(
    device: DeviceConfig,
    sync_config: SyncConfig,
    room_code: String,
    leader_addr: Option<SocketAddr>,
    leader_id: Option<Uuid>,
    leader_clock_port: u16,
    session_port: u16,
    headless: bool,
) -> Result<()> {
    let (resolved_leader_addr, resolved_leader_id) =
        resolve_join_target(&room_code, leader_addr, leader_id)?;

    let clock_socket =
        UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))).await?;
    let clock = Arc::new(ClockSync::new(
        clock_socket,
        sync_config.clone(),
        device.device_id,
    ));

    let leader_clock_addr = SocketAddr::new(resolved_leader_addr.ip(), leader_clock_port);
    if let Err(err) = clock.measure_offset(leader_clock_addr).await {
        eprintln!("clock offset measurement failed: {err}");
    }

    let session_socket = Arc::new({
        let socket = UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::UNSPECIFIED,
            session_port,
        )))
        .await?;
        socket.set_broadcast(true)?;
        socket
    });
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
            role: crate::protocol::messages::Role::Leader,
        }],
        QueueState::default(),
    ));

    let controller = create_media_controller();
    let scheduler = Arc::new(IntentScheduler::new(
        Arc::clone(&clock) as Arc<dyn crate::sync::controller::ClockSource>,
        Arc::clone(&controller),
    ));

    let drift_evaluator = Arc::new(DriftEvaluator::new(
        Arc::clone(&clock) as Arc<dyn crate::sync::controller::ClockSource>,
        Arc::clone(&controller),
        50_000,
    ));

    send_join_request(
        &session_socket,
        device.device_id,
        resolved_leader_addr,
        room_code.clone(),
        device.name.clone(),
    )
    .await?;

    let clock_task = {
        let clock = Arc::clone(&clock);
        tokio::spawn(async move { clock.run_responder().await })
    };

    // Sparse background drift check (every 4s)
    let drift_task = tokio::spawn({
        let evaluator = Arc::clone(&drift_evaluator);
        let session = Arc::clone(&session);
        async move {
            loop {
                sleep(Duration::from_secs(4)).await;
                if let Some(anchor) = session.latest_sync_anchor() {
                    let _ = evaluator.evaluate_and_reconcile(&anchor).await;
                }
            }
        }
    });

    let heartbeat_task = tokio::spawn({
        let session = Arc::clone(&session);
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        let cfg = sync_config.clone();
        let room = room_code.clone();
        async move { run_heartbeat_loop(session, socket, sender, room, Some(resolved_leader_addr), cfg).await }
    });

    let role_manager_task = tokio::spawn({
        let session = Arc::clone(&session);
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        let cfg = sync_config.clone();
        async move { run_role_manager_loop(session, socket, sender, cfg).await }
    });

    if !headless {
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();

        let runtime = SessionMessageRuntime::new(
            Arc::clone(&session),
            None,
            device.name.clone(),
        )
        .with_event_tx(event_tx.clone())
        .with_controller(Arc::clone(&controller))
        .with_clock(Arc::clone(&clock))
        .with_scheduler(Arc::clone(&scheduler))
        .with_drift_evaluator(Arc::clone(&drift_evaluator));

        let receive_task = tokio::spawn({
            let socket = Arc::clone(&session_socket);
            async move { runtime.run_receive_loop(socket).await }
        });

        let _ = event_tx.send(crate::tui::AppEvent::Log(
            format!("Joining room {room_code} via {resolved_leader_addr}"),
            "System".into(),
        ));

        let tui_res = crate::tui::run_tui(
            room_code,
            device.name,
            device.device_id,
            crate::protocol::messages::Role::Listener,
            Arc::clone(&session),
            Arc::clone(&controller),
            Arc::clone(&session_socket),
            Some(resolved_leader_addr),
            Arc::clone(&clock),
            Arc::clone(&scheduler),
            event_rx,
            event_tx,
        )
        .await;

        // Graceful departure notification
        let leave_env = Envelope {
            sender: device.device_id,
            payload: Message::PeerLeft(device.device_id),
        };
        if let Ok(bytes) = serialize(&leave_env) {
            for addr in session.peer_socket_addrs() {
                let _ = session_socket.send_to(&bytes, addr).await;
            }
        }

        receive_task.abort();
        clock_task.abort();
        drift_task.abort();
        heartbeat_task.abort();
        role_manager_task.abort();

        return tui_res;
    }

    // Headless REPL mode
    let (rl, stdout) = rustyline_async::Readline::new("synkro> ".to_string()).unwrap();

    let runtime = SessionMessageRuntime::new(
        Arc::clone(&session),
        Some(stdout.clone()),
        device.name.clone(),
    )
    .with_controller(Arc::clone(&controller))
    .with_clock(Arc::clone(&clock))
    .with_scheduler(Arc::clone(&scheduler))
    .with_drift_evaluator(Arc::clone(&drift_evaluator));

    crate::session::runtime::print_event(
        Some(&stdout),
        &format!(
            "Joining room {room_code} as follower {} via leader {resolved_leader_addr}",
            device.device_id
        ),
    );

    let receive_task = tokio::spawn({
        let socket = Arc::clone(&session_socket);
        async move { runtime.run_receive_loop(socket).await }
    });

    let repl_task = tokio::spawn({
        let session = Arc::clone(&session);
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        let name = device.name.clone();
        let leader_addr = resolved_leader_addr;
        let controller = Arc::clone(&controller);
        let clock = Arc::clone(&clock);
        let scheduler = Arc::clone(&scheduler);
        let stdout = stdout.clone();
        async move {
            run_follower_repl(
                rl,
                session,
                socket,
                sender,
                name,
                leader_addr,
                controller,
                clock,
                scheduler,
                stdout,
            )
            .await
        }
    });

    tokio::select! {
        res = receive_task => {
            if let Err(err) = res {
                eprintln!("receive task failed: {err}");
            }
        }
        res = clock_task => {
            if let Err(err) = res {
                eprintln!("clock responder failed: {err}");
            }
        }
        Err(err) = drift_task => {
            eprintln!("drift task failed: {err}");
        }
        res = heartbeat_task => {
            if let Err(err) = res {
                eprintln!("heartbeat loop failed: {err}");
            }
        }
        res = role_manager_task => {
            if let Err(err) = res {
                eprintln!("role manager loop failed: {err}");
            }
        }
        res = repl_task => {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => eprintln!("repl loop stopped: {err}"),
                Err(err) => eprintln!("repl task cancelled: {err}"),
            }
        }
        _ = tokio::signal::ctrl_c() => {
            println!("Leaving room {room_code}.");
        }
    }

    // Graceful departure notification
    let leave_env = Envelope {
        sender: device.device_id,
        payload: Message::PeerLeft(device.device_id),
    };
    if let Ok(bytes) = serialize(&leave_env) {
        for addr in session.peer_socket_addrs() {
            let _ = session_socket.send_to(&bytes, addr).await;
        }
    }

    Ok(())
}

pub async fn run_heartbeat_loop(
    session: Arc<SessionState>,
    socket: Arc<UdpSocket>,
    sender: Uuid,
    room_code: String,
    target_leader: Option<SocketAddr>,
    config: SyncConfig,
) -> Result<()> {
    loop {
        sleep(Duration::from_millis(config.heartbeat_interval_ms)).await;
        let envelope = Envelope {
            sender,
            payload: Message::Heartbeat {
                room_code: room_code.clone(),
                info: session.self_info(),
            },
        };
        let bytes = serialize(&envelope)?;

        // Send to direct leader target if configured
        if let Some(leader_addr) = target_leader {
            let _ = socket.send_to(&bytes, leader_addr).await;
        }

        // Broadcast heartbeat to all registered peer addresses
        for addr in session.peer_socket_addrs() {
            if Some(addr) != target_leader {
                let _ = socket.send_to(&bytes, addr).await;
            }
        }
    }
}

pub async fn run_role_manager_loop(
    session: Arc<SessionState>,
    socket: Arc<UdpSocket>,
    sender: Uuid,
    config: SyncConfig,
) -> Result<()> {
    loop {
        sleep(Duration::from_millis(config.heartbeat_interval_ms)).await;
        let timeout = Duration::from_millis(config.heartbeat_timeout_ms);

        if session.is_leader() {
            let (expired, _) = session.prune_and_appoint(timeout);
            for dead_peer in expired {
                let envelope = Envelope {
                    sender,
                    payload: Message::PeerLeft(dead_peer),
                };
                if let Ok(bytes) = serialize(&envelope) {
                    for addr in session.peer_socket_addrs() {
                        let _ = socket.send_to(&bytes, addr).await;
                    }
                }
            }
        } else {
            // Follower monitoring: check if leader or other peers timed out
            let leader_id = session.leader_id();
            let expired = session.prune_and_appoint(timeout).0;

            if expired.contains(&leader_id) {
                // Leader timed out! Determine new leader deterministically
                let live_infos: Vec<_> = session.all_alive_peers().into_iter().map(|(_, e)| e.info).collect();
                let heir = crate::session::leader::appoint_successor(&live_infos, &session.self_info());

                if heir == session.self_id() {
                    // We won the election!
                    session.promote_to_leader();
                    let envelope = Envelope {
                        sender,
                        payload: Message::LeaderElected(heir),
                    };
                    if let Ok(bytes) = serialize(&envelope) {
                        for addr in session.peer_socket_addrs() {
                            let _ = socket.send_to(&bytes, addr).await;
                        }
                    }
                } else {
                    // Another peer won the election
                    session.set_leader_id(heir);
                }
            }
        }
    }
}

pub async fn run_simple_command(
    device: DeviceConfig,
    room_code: String,
    leader_addr: Option<SocketAddr>,
    message: Message,
) -> Result<()> {
    let (resolved_leader_addr, _) = resolve_join_target(&room_code, leader_addr, None)?;
    let socket =
        UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))).await?;

    let envelope = Envelope {
        sender: device.device_id,
        payload: message,
    };
    let bytes = serialize(&envelope)?;
    socket.send_to(&bytes, resolved_leader_addr).await?;
    Ok(())
}

pub async fn run_queue_display(
    _device: DeviceConfig,
    room_code: String,
    leader_addr: Option<SocketAddr>,
) -> Result<()> {
    let (resolved_leader_addr, _) = resolve_join_target(&room_code, leader_addr, None)?;
    let socket =
        UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))).await?;

    let ephemeral_id = Uuid::new_v4();
    send_join_request(
        &socket,
        ephemeral_id,
        resolved_leader_addr,
        room_code.clone(),
        "QueueViewer".into(),
    )
    .await?;

    let mut buf = [0u8; 8 * 1024];
    match tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut buf)).await {
        Ok(Ok((len, _))) => {
            if let Ok(Envelope {
                payload: Message::JoinAccepted { queue_state, .. },
                ..
            }) = crate::protocol::messages::deserialize(&buf[..len])
            {
                println!("Queue for room {room_code}:");
                if let Some(current) = queue_state.current {
                    println!(
                        "  [PLAYING] {} (ID: {}, requested by {})",
                        current.title, current.id, current.requested_by
                    );
                } else {
                    println!("  [PLAYING] None");
                }
                if queue_state.upcoming.is_empty() {
                    println!("  (Upcoming queue is empty)");
                } else {
                    println!("\n  Upcoming:");
                    for (i, track) in queue_state.upcoming.iter().enumerate() {
                        println!(
                            "  {}. {} (ID: {}, requested by {})",
                            i + 1,
                            track.title,
                            track.id,
                            track.requested_by
                        );
                    }
                }
            }
        }
        _ => println!("Timed out waiting for queue information from leader."),
    }
    Ok(())
}

pub async fn run_sync_status(
    device: DeviceConfig,
    room_code: String,
    leader_addr: Option<SocketAddr>,
) -> Result<()> {
    let (resolved_leader_addr, _) = resolve_join_target(&room_code, leader_addr, None)?;
    let socket =
        UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))).await?;

    let ephemeral_id = Uuid::new_v4();
    send_join_request(
        &socket,
        ephemeral_id,
        resolved_leader_addr,
        room_code.clone(),
        device.name,
    )
    .await?;

    let mut peer_offsets = std::collections::HashMap::new();
    let mut buf = [0u8; 8 * 1024];
    let start = std::time::Instant::now();
    let duration = Duration::from_secs(2);

    while start.elapsed() < duration {
        let remaining = duration.saturating_sub(start.elapsed());
        if let Ok(Ok((len, _))) = tokio::time::timeout(remaining, socket.recv_from(&mut buf)).await
            && let Ok(Envelope {
                payload: Message::JoinAccepted { peer_list, .. },
                ..
            }) = crate::protocol::messages::deserialize(&buf[..len])
        {
            for peer in peer_list {
                peer_offsets.insert(peer.device_id, peer.clock_offset_us);
            }
            break;
        }
    }

    if peer_offsets.is_empty() {
        println!("No peer data collected.");
    } else {
        println!("\n{:<40} {:>12}", "Device ID", "Offset (us)");
        println!("{}", "-".repeat(53));
        let mut sorted: Vec<_> = peer_offsets.into_iter().collect();
        sorted.sort_by_key(|a| a.0);
        for (id, offset) in sorted {
            println!("{:<40} {:>12}", id, offset);
        }
    }
    Ok(())
}

pub async fn run_debug(
    device: DeviceConfig,
    room_code: String,
    leader_addr: Option<SocketAddr>,
) -> Result<()> {
    run_sync_status(device, room_code, leader_addr).await
}
