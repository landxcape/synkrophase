use std::env;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use synkrophase::clock::sync::ClockSync;
use synkrophase::config::{DeviceConfig, SyncConfig};
use synkrophase::controller::MediaController;
#[cfg(target_os = "macos")]
use synkrophase::controller::macos::MacOsMediaController;
#[cfg(not(target_os = "macos"))]
use synkrophase::controller::mock::MockMediaController;
use synkrophase::error::Result;
use synkrophase::protocol::messages::{
    Envelope, Message, PlaybackAction, PlaybackIntent, QueueCommand, QueueState, serialize,
};
use synkrophase::session::discovery::Discovery;
use synkrophase::session::runtime::{LeaderAnchorBroadcaster, SessionMessageRuntime};
use synkrophase::session::{FollowerSyncRuntime, SessionState};
use synkrophase::sync::controller::{NoopPlaybackControl, PlaybackControl, SyncController};
use synkrophase::sync::evaluator::DriftEvaluator;
use synkrophase::sync::scheduler::IntentScheduler;
use tokio::net::UdpSocket;
use tokio::time::sleep;
use uuid::Uuid;

const DEFAULT_CLOCK_PORT: u16 = 5870;
const DEFAULT_SESSION_PORT: u16 = 5871;
const DEFAULT_LEAD_TIME_US: u64 = 100_000; // 100ms dynamic lead time

fn create_media_controller() -> Arc<dyn MediaController> {
    #[cfg(target_os = "macos")]
    {
        Arc::new(MacOsMediaController::new())
    }
    #[cfg(not(target_os = "macos"))]
    {
        Arc::new(MockMediaController::new())
    }
}

#[derive(Parser, Debug)]
#[command(name = "synkro", version, about = "Synchronized LAN playback controller")]
struct Cli {
    /// Optional nickname for this device
    #[arg(long, global = true)]
    pub name: Option<String>,

    /// Use a random, temporary device ID (useful for local testing)
    #[arg(long, global = true)]
    pub ephemeral: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Inspect local media player status and active track info.
    Status,
    /// Start a session and broadcast sync anchors as the leader.
    Host {
        /// Optional room code. If omitted, a code is generated.
        #[arg(long)]
        room_code: Option<String>,
        /// UDP port for clock sync responder.
        #[arg(long, default_value_t = DEFAULT_CLOCK_PORT)]
        clock_port: u16,
        /// UDP port for session traffic.
        #[arg(long, default_value_t = DEFAULT_SESSION_PORT)]
        session_port: u16,
    },
    /// Join an existing session and run follower sync loop.
    Join {
        /// Session room code.
        room_code: String,
        /// Optional leader session address (ip:port). If omitted, mDNS discovery is used.
        #[arg(long)]
        leader_addr: Option<SocketAddr>,
        /// Optional leader UUID for manual join mode.
        #[arg(long)]
        leader_id: Option<Uuid>,
        /// Leader clock sync port.
        #[arg(long, default_value_t = DEFAULT_CLOCK_PORT)]
        leader_clock_port: u16,
        /// Local UDP port for session traffic.
        #[arg(long, default_value_t = DEFAULT_SESSION_PORT)]
        session_port: u16,
    },
    /// Send play intent across the room.
    Play {
        /// Session room code.
        room_code: String,
        /// Optional leader session address (ip:port). If omitted, mDNS discovery is used.
        #[arg(long)]
        leader_addr: Option<SocketAddr>,
    },
    /// Pause playback across the room.
    Pause {
        room_code: String,
        #[arg(long)]
        leader_addr: Option<SocketAddr>,
    },
    /// Resume playback across the room.
    Resume {
        room_code: String,
        #[arg(long)]
        leader_addr: Option<SocketAddr>,
    },
    /// Skip current track across the room.
    Skip {
        room_code: String,
        #[arg(long)]
        leader_addr: Option<SocketAddr>,
    },
    /// Show current queue.
    Queue {
        room_code: String,
        #[arg(long)]
        leader_addr: Option<SocketAddr>,
    },
    /// Show live sync status per peer.
    Sync {
        room_code: String,
        #[arg(long)]
        leader_addr: Option<SocketAddr>,
    },
    /// Verbose sync metrics mode.
    Debug {
        room_code: String,
        #[arg(long)]
        leader_addr: Option<SocketAddr>,
    },
    /// Transfer leadership to another device.
    Transfer {
        room_code: String,
        device_id: String,
        #[arg(long)]
        leader_addr: Option<SocketAddr>,
    },
    /// Send a chat message to a room.
    Chat {
        room_code: String,
        message: String,
        #[arg(long)]
        leader_addr: Option<SocketAddr>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let device = load_device_config(cli.name, cli.ephemeral)?;
    let sync_config = SyncConfig::default();

    match cli.command {
        Commands::Status => {
            let controller = create_media_controller();
            let state = controller.get_playback_state().await?;
            println!("\n=== Local Media Player Status ===");
            println!("  Playing:  {}", if state.is_playing { "Yes" } else { "No" });
            let pos_sec = (state.position_us as f64) / 1_000_000.0;
            println!("  Position: {:.2}s", pos_sec);
            if let Some(meta) = state.metadata {
                println!("  Title:    {}", meta.title);
                if let Some(artist) = meta.artist {
                    println!("  Artist:   {}", artist);
                }
                if let Some(album) = meta.album {
                    println!("  Album:    {}", album);
                }
                if let Some(dur_us) = meta.duration_us {
                    println!("  Duration: {:.2}s", (dur_us as f64) / 1_000_000.0);
                }
            } else {
                println!("  Metadata: (None - no active player found)");
            }
            println!();
            Ok(())
        }
        Commands::Host {
            room_code,
            clock_port,
            session_port,
        } => {
            let room = room_code.unwrap_or_else(|| generated_room_code(device.device_id));
            run_host(device, sync_config, room, clock_port, session_port).await
        }
        Commands::Join {
            room_code,
            leader_addr,
            leader_id,
            leader_clock_port,
            session_port,
        } => {
            run_join(
                device,
                sync_config,
                room_code,
                leader_addr,
                leader_id,
                leader_clock_port,
                session_port,
            )
            .await
        }
        Commands::Play {
            room_code,
            leader_addr,
        } => {
            let controller = create_media_controller();
            let state = controller.get_playback_state().await?;
            let title = state.metadata.map(|m| m.title);
            let intent = PlaybackIntent {
                action: PlaybackAction::Play,
                target_ref_time: 0,
                position_us: state.position_us,
                track_title: title,
            };
            run_simple_command(
                device.clone(),
                room_code,
                leader_addr,
                Message::Intent(intent),
            )
            .await
        }
        Commands::Pause {
            room_code,
            leader_addr,
        } => {
            let intent = PlaybackIntent {
                action: PlaybackAction::Pause,
                target_ref_time: 0,
                position_us: 0,
                track_title: None,
            };
            run_simple_command(
                device.clone(),
                room_code,
                leader_addr,
                Message::Intent(intent),
            )
            .await
        }
        Commands::Resume {
            room_code,
            leader_addr,
        } => {
            run_simple_command(
                device.clone(),
                room_code,
                leader_addr,
                Message::Resume {
                    actor: device.device_id,
                },
            )
            .await
        }
        Commands::Skip {
            room_code,
            leader_addr,
        } => {
            run_simple_command(device, room_code, leader_addr, Message::Skip).await
        }
        Commands::Queue {
            room_code,
            leader_addr,
        } => run_queue_display(device, room_code, leader_addr).await,
        Commands::Sync {
            room_code,
            leader_addr,
        } => run_sync_status(device, room_code, leader_addr).await,
        Commands::Debug {
            room_code,
            leader_addr,
        } => run_debug(device, room_code, leader_addr).await,
        Commands::Transfer {
            room_code,
            device_id,
            leader_addr,
        } => {
            let target_uuid = Uuid::parse_str(&device_id).map_err(|e| {
                synkrophase::error::SynkroError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Invalid target UUID: {e}"),
                ))
            })?;
            run_simple_command(
                device,
                room_code,
                leader_addr,
                Message::TransferLeadership { to: target_uuid },
            )
            .await
        }
        Commands::Chat {
            room_code,
            message,
            leader_addr,
        } => {
            run_simple_command(
                device.clone(),
                room_code,
                leader_addr,
                Message::Chat {
                    sender: device.device_id,
                    name: device.name.clone(),
                    text: message,
                },
            )
            .await
        }
    }
}

async fn run_host(
    device: DeviceConfig,
    sync_config: SyncConfig,
    room_code: String,
    clock_port: u16,
    session_port: u16,
) -> Result<()> {
    let clock_socket = UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::UNSPECIFIED,
        clock_port,
    )))
    .await?;

    let session_socket = Arc::new({
        let socket = UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::UNSPECIFIED,
            session_port,
        )))
        .await?;
        socket.set_broadcast(true)?;
        socket
    });

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
        Arc::clone(&clock) as Arc<dyn synkrophase::sync::controller::ClockSource>,
        Arc::clone(&controller),
    ));

    let playback: Arc<dyn PlaybackControl> = Arc::new(NoopPlaybackControl::default());
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

    let broadcaster = LeaderAnchorBroadcaster::new(
        Arc::clone(&session),
        Arc::clone(&clock),
        Arc::clone(&playback),
        sync_config.clone(),
    )
    .with_controller(Arc::clone(&controller));

    let discovery = Discovery::new()?;
    discovery.register_session(&room_code, device.device_id, session_port)?;

    let local_ip = local_ip_address::local_ip()
        .unwrap_or(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));

    synkrophase::session::runtime::print_event(
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
    let clock_task = {
        let clock = Arc::clone(&clock);
        tokio::spawn(async move { clock.run_responder().await })
    };
    let broadcast_task = tokio::spawn({
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        async move { broadcaster.run_broadcast_loop(socket, sender).await }
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

    let _ = discovery.unregister();
    Ok(())
}

async fn run_join(
    device: DeviceConfig,
    sync_config: SyncConfig,
    room_code: String,
    leader_addr: Option<SocketAddr>,
    leader_id: Option<Uuid>,
    leader_clock_port: u16,
    session_port: u16,
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
        vec![synkrophase::protocol::messages::PeerInfo {
            device_id: resolved_leader_id,
            name: "Leader".into(),
            clock_offset_us: 0,
            last_seen: 0,
            role: synkrophase::protocol::messages::Role::Leader,
        }],
        QueueState::default(),
    ));

    let controller = create_media_controller();
    let scheduler = Arc::new(IntentScheduler::new(
        Arc::clone(&clock) as Arc<dyn synkrophase::sync::controller::ClockSource>,
        Arc::clone(&controller),
    ));

    let drift_evaluator = Arc::new(DriftEvaluator::new(
        Arc::clone(&clock) as Arc<dyn synkrophase::sync::controller::ClockSource>,
        Arc::clone(&controller),
        50_000,
    ));

    let playback: Arc<dyn PlaybackControl> = Arc::new(NoopPlaybackControl::default());
    let follower_sync_controller = Arc::new(SyncController::new(
        Arc::clone(&clock) as Arc<dyn synkrophase::sync::controller::ClockSource>,
        Arc::clone(&playback),
        sync_config.clone(),
    ));
    let follower_sync = Arc::new(FollowerSyncRuntime::new(follower_sync_controller));
    follower_sync.start_sync_loop();

    let (rl, stdout) = rustyline_async::Readline::new("synkro> ".to_string()).unwrap();

    let runtime = SessionMessageRuntime::new(
        Arc::clone(&session),
        Some(stdout.clone()),
        device.name.clone(),
    )
    .with_follower_sync(Arc::clone(&follower_sync))
    .with_playback(Arc::clone(&playback))
    .with_scheduler(Arc::clone(&scheduler))
    .with_drift_evaluator(Arc::clone(&drift_evaluator));

    synkrophase::session::runtime::print_event(
        Some(&stdout),
        &format!(
            "Joining room {room_code} as follower {} via leader {resolved_leader_addr}",
            device.device_id
        ),
    );

    send_join_request(
        &session_socket,
        device.device_id,
        resolved_leader_addr,
        room_code.clone(),
        device.name.clone(),
    )
    .await?;

    let receive_task = tokio::spawn({
        let socket = Arc::clone(&session_socket);
        async move { runtime.run_receive_loop(socket).await }
    });
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
        async move { run_heartbeat_loop(session, socket, sender, room, cfg).await }
    });

    let role_manager_task = tokio::spawn({
        let session = Arc::clone(&session);
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        let cfg = sync_config.clone();
        async move { run_role_manager_loop(session, socket, sender, cfg).await }
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

    Ok(())
}

async fn run_heartbeat_loop(
    session: Arc<SessionState>,
    socket: Arc<UdpSocket>,
    sender: Uuid,
    room_code: String,
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

        if session.is_leader() {
            for addr in session.peer_socket_addrs() {
                let _ = socket.send_to(&bytes, addr).await;
            }
        } else {
            let leader_id = session.leader_id();
            if let Some((_, entry)) = session
                .all_alive_peers()
                .into_iter()
                .find(|(id, _)| *id == leader_id)
            {
                let _ = socket.send_to(&bytes, entry.addr).await;
            }
        }
    }
}

async fn run_role_manager_loop(
    session: Arc<SessionState>,
    socket: Arc<UdpSocket>,
    sender: Uuid,
    config: SyncConfig,
) -> Result<()> {
    loop {
        sleep(Duration::from_millis(config.heartbeat_interval_ms)).await;
        if session.is_leader() {
            let (expired, _) =
                session.prune_and_appoint(Duration::from_millis(config.heartbeat_timeout_ms));
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
        }
    }
}

async fn run_simple_command(
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

async fn run_queue_display(
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
            }) = synkrophase::protocol::messages::deserialize(&buf[..len])
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

async fn run_sync_status(
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
            }) = synkrophase::protocol::messages::deserialize(&buf[..len])
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

async fn run_debug(
    device: DeviceConfig,
    room_code: String,
    leader_addr: Option<SocketAddr>,
) -> Result<()> {
    run_sync_status(device, room_code, leader_addr).await
}

fn resolve_join_target(
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

    Err(synkrophase::error::SynkroError::Network(
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Could not discover room {room_code}"),
        ),
    ))
}

async fn send_join_request(
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

fn load_device_config(name_opt: Option<String>, ephemeral: bool) -> Result<DeviceConfig> {
    let home = home::home_dir().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Could not find home directory",
        )
    })?;
    let data_dir = home.join(".synkrophase");
    fs::create_dir_all(&data_dir)?;

    let name = name_opt
        .or_else(|| env::var("USER").ok())
        .or_else(|| env::var("USERNAME").ok())
        .unwrap_or_else(|| "User".to_string());

    if ephemeral || std::env::var("SYNKRO_EPHEMERAL").is_ok() {
        return Ok(DeviceConfig {
            device_id: Uuid::new_v4(),
            name,
            data_dir,
        });
    }

    let identity_file = data_dir.join("device_id");
    let device_id = if identity_file.exists() {
        let content = fs::read_to_string(&identity_file)?;
        Uuid::parse_str(content.trim()).unwrap_or_else(|_| {
            let id = Uuid::new_v4();
            let _ = fs::write(&identity_file, id.to_string());
            id
        })
    } else {
        let id = Uuid::new_v4();
        let _ = fs::write(&identity_file, id.to_string());
        id
    };

    Ok(DeviceConfig {
        device_id,
        name,
        data_dir,
    })
}

fn generated_room_code(id: Uuid) -> String {
    let compact = id.simple().to_string();
    compact[..6].to_ascii_uppercase()
}

#[allow(clippy::too_many_arguments)]
async fn run_host_repl(
    mut rl: rustyline_async::Readline,
    session: Arc<SessionState>,
    socket: Arc<UdpSocket>,
    sender: Uuid,
    name: String,
    clock: Arc<ClockSync>,
    controller: Arc<dyn MediaController>,
    scheduler: Arc<IntentScheduler>,
    stdout: rustyline_async::SharedWriter,
) -> Result<()> {
    loop {
        let line = rl.readline().await;
        let input = match line {
            Ok(rustyline_async::ReadlineEvent::Line(line)) => line,
            _ => break,
        };

        let input = input.trim();
        if input.is_empty() {
            continue;
        }

        let is_slash = input.starts_with('/');
        let cmd_text = input.strip_prefix('/').unwrap_or(input);
        let parts: Vec<&str> = cmd_text.split_whitespace().collect();

        let is_known_cmd = !parts.is_empty()
            && matches!(
                parts[0],
                "play" | "pause" | "seek" | "status" | "skip" | "queue" | "exit" | "quit" | "help"
            );

        if is_slash || is_known_cmd {
            if parts.is_empty() {
                continue;
            }
            match parts[0] {
                "play" => {
                    let state = controller.get_playback_state().await?;
                    let now = clock.reference_now();
                    let target_ref_time = now + DEFAULT_LEAD_TIME_US;
                    let title = state.metadata.map(|m| m.title);

                    let intent = PlaybackIntent {
                        action: PlaybackAction::Play,
                        target_ref_time,
                        position_us: state.position_us,
                        track_title: title.clone(),
                    };

                    // Broadcast intent immediately across UDP fast lane
                    let envelope = Envelope {
                        sender,
                        payload: Message::Intent(intent.clone()),
                    };
                    if let Ok(bytes) = serialize(&envelope) {
                        for addr in session.peer_socket_addrs() {
                            let _ = socket.send_to(&bytes, addr).await;
                        }
                    }

                    // Execute scheduled intent locally
                    let _ = scheduler.execute_intent(&intent).await;

                    synkrophase::session::runtime::print_event(
                        Some(&stdout),
                        &format!(
                            "[System] Play intent scheduled for {:?} at T+100ms",
                            title.unwrap_or_else(|| "active track".into())
                        ),
                    );
                }
                "pause" => {
                    let state = controller.get_playback_state().await?;
                    let now = clock.reference_now();
                    let target_ref_time = now + DEFAULT_LEAD_TIME_US;

                    let intent = PlaybackIntent {
                        action: PlaybackAction::Pause,
                        target_ref_time,
                        position_us: state.position_us,
                        track_title: None,
                    };

                    let envelope = Envelope {
                        sender,
                        payload: Message::Intent(intent.clone()),
                    };
                    if let Ok(bytes) = serialize(&envelope) {
                        for addr in session.peer_socket_addrs() {
                            let _ = socket.send_to(&bytes, addr).await;
                        }
                    }

                    let _ = scheduler.execute_intent(&intent).await;

                    synkrophase::session::runtime::print_event(
                        Some(&stdout),
                        "[System] Pause intent scheduled at T+100ms",
                    );
                }
                "seek" => {
                    if parts.len() < 2 {
                        synkrophase::session::runtime::print_event(
                            Some(&stdout),
                            "Usage: /seek <seconds> or seek <seconds>",
                        );
                        continue;
                    }
                    if let Ok(sec) = parts[1].parse::<f64>() {
                        let pos_us = (sec * 1_000_000.0) as i64;
                        let now = clock.reference_now();
                        let target_ref_time = now + DEFAULT_LEAD_TIME_US;

                        let intent = PlaybackIntent {
                            action: PlaybackAction::Seek {
                                target_position_us: pos_us,
                            },
                            target_ref_time,
                            position_us: pos_us,
                            track_title: None,
                        };

                        let envelope = Envelope {
                            sender,
                            payload: Message::Intent(intent.clone()),
                        };
                        if let Ok(bytes) = serialize(&envelope) {
                            for addr in session.peer_socket_addrs() {
                                let _ = socket.send_to(&bytes, addr).await;
                            }
                        }

                        let _ = scheduler.execute_intent(&intent).await;

                        synkrophase::session::runtime::print_event(
                            Some(&stdout),
                            &format!("[System] Seek to {:.2}s scheduled at T+100ms", sec),
                        );
                    }
                }
                "status" => {
                    let state = controller.get_playback_state().await?;
                    let mut out = format!(
                        "Player Status: {}\nPosition: {:.2}s\n",
                        if state.is_playing { "Playing" } else { "Paused" },
                        (state.position_us as f64) / 1_000_000.0
                    );
                    if let Some(m) = state.metadata {
                        out.push_str(&format!("Track: {}\n", m.title));
                        if let Some(a) = m.artist {
                            out.push_str(&format!("Artist: {}\n", a));
                        }
                    }
                    synkrophase::session::runtime::print_event(Some(&stdout), out.trim_end());
                }
                "skip" => {
                    if let Ok(updated) =
                        session.handle_queue_proposal(QueueCommand::Skip)
                    {
                        let envelope = Envelope {
                            sender,
                            payload: Message::QueueUpdate(updated),
                        };
                        if let Ok(bytes) = serialize(&envelope) {
                            for addr in session.peer_socket_addrs() {
                                let _ = socket.send_to(&bytes, addr).await;
                            }
                        }
                    }
                }
                "queue" => {
                    let queue_state = session.queue_snapshot();
                    let room_code = session.room_code();
                    let mut out = format!("Queue for room {}:\n", room_code);
                    if let Some(current) = queue_state.current {
                        out.push_str(&format!(
                            "  [PLAYING] {} (ID: {}, requested by {})\n",
                            current.title, current.id, current.requested_by
                        ));
                    } else {
                        out.push_str("  (Nothing playing)\n");
                    }
                    if queue_state.upcoming.is_empty() {
                        out.push_str("  (Upcoming queue is empty)\n");
                    } else {
                        out.push_str("\n  Upcoming:\n");
                        for (i, track) in queue_state.upcoming.iter().enumerate() {
                            out.push_str(&format!(
                                "  {}. {} (ID: {}, requested by {})\n",
                                i + 1,
                                track.title,
                                track.id,
                                track.requested_by
                            ));
                        }
                    }
                    synkrophase::session::runtime::print_event(Some(&stdout), out.trim_end());
                }
                "exit" | "quit" => {
                    synkrophase::session::runtime::print_event(Some(&stdout), "Exiting...");
                    return Ok(());
                }
                _ => {
                    synkrophase::session::runtime::print_event(
                        Some(&stdout),
                        "Available commands: /play, /pause, /seek <sec>, /status, /skip, /queue, /exit",
                    );
                }
            }
        } else {
            let envelope = Envelope {
                sender,
                payload: Message::ChatBroadcast {
                    display_name: name.clone(),
                    text: input.to_string(),
                },
            };
            if let Ok(bytes) = serialize(&envelope) {
                for addr in session.peer_socket_addrs() {
                    let _ = socket.send_to(&bytes, addr).await;
                }
            }
            synkrophase::session::runtime::print_event(
                Some(&stdout),
                &format!("[{name}]: {input}"),
            );
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_follower_repl(
    mut rl: rustyline_async::Readline,
    session: Arc<SessionState>,
    socket: Arc<UdpSocket>,
    sender: Uuid,
    name: String,
    leader_addr: SocketAddr,
    controller: Arc<dyn MediaController>,
    _clock: Arc<ClockSync>,
    _scheduler: Arc<IntentScheduler>,
    stdout: rustyline_async::SharedWriter,
) -> Result<()> {
    loop {
        let line = rl.readline().await;
        let input = match line {
            Ok(rustyline_async::ReadlineEvent::Line(line)) => line,
            _ => break,
        };

        let input = input.trim();
        if input.is_empty() {
            continue;
        }

        let is_slash = input.starts_with('/');
        let cmd_text = input.strip_prefix('/').unwrap_or(input);
        let parts: Vec<&str> = cmd_text.split_whitespace().collect();

        let is_known_cmd = !parts.is_empty()
            && matches!(
                parts[0],
                "play" | "pause" | "resume" | "seek" | "status" | "skip" | "queue" | "exit" | "quit" | "help"
            );

        if is_slash || is_known_cmd {
            if parts.is_empty() {
                continue;
            }
            match parts[0] {
                "play" | "resume" => {
                    let state = controller.get_playback_state().await.unwrap_or_default();
                    let title = state.metadata.map(|m| m.title);
                    let intent = PlaybackIntent {
                        action: PlaybackAction::Play,
                        target_ref_time: 0,
                        position_us: state.position_us,
                        track_title: title,
                    };
                    let envelope = Envelope {
                        sender,
                        payload: Message::Intent(intent),
                    };
                    if let Ok(bytes) = serialize(&envelope) {
                        let _ = socket.send_to(&bytes, leader_addr).await;
                    }
                    synkrophase::session::runtime::print_event(
                        Some(&stdout),
                        "[Follower] Play request forwarded to room leader",
                    );
                }
                "pause" => {
                    let state = controller.get_playback_state().await.unwrap_or_default();
                    let intent = PlaybackIntent {
                        action: PlaybackAction::Pause,
                        target_ref_time: 0,
                        position_us: state.position_us,
                        track_title: None,
                    };
                    let envelope = Envelope {
                        sender,
                        payload: Message::Intent(intent),
                    };
                    if let Ok(bytes) = serialize(&envelope) {
                        let _ = socket.send_to(&bytes, leader_addr).await;
                    }
                    synkrophase::session::runtime::print_event(
                        Some(&stdout),
                        "[Follower] Pause request forwarded to room leader",
                    );
                }
                "seek" => {
                    if parts.len() < 2 {
                        synkrophase::session::runtime::print_event(
                            Some(&stdout),
                            "Usage: /seek <seconds> or seek <seconds>",
                        );
                        continue;
                    }
                    if let Ok(sec) = parts[1].parse::<f64>() {
                        let pos_us = (sec * 1_000_000.0) as i64;
                        let intent = PlaybackIntent {
                            action: PlaybackAction::Seek {
                                target_position_us: pos_us,
                            },
                            target_ref_time: 0,
                            position_us: pos_us,
                            track_title: None,
                        };
                        let envelope = Envelope {
                            sender,
                            payload: Message::Intent(intent),
                        };
                        if let Ok(bytes) = serialize(&envelope) {
                            let _ = socket.send_to(&bytes, leader_addr).await;
                        }
                        synkrophase::session::runtime::print_event(
                            Some(&stdout),
                            &format!("[Follower] Seek to {:.2}s forwarded to room leader", sec),
                        );
                    }
                }
                "status" => {
                    let state = controller.get_playback_state().await?;
                    let mut out = format!(
                        "Player Status: {}\nPosition: {:.2}s\n",
                        if state.is_playing { "Playing" } else { "Paused" },
                        (state.position_us as f64) / 1_000_000.0
                    );
                    if let Some(m) = state.metadata {
                        out.push_str(&format!("Track: {}\n", m.title));
                        if let Some(a) = m.artist {
                            out.push_str(&format!("Artist: {}\n", a));
                        }
                    }
                    synkrophase::session::runtime::print_event(Some(&stdout), out.trim_end());
                }
                "skip" => {
                    let envelope = Envelope {
                        sender,
                        payload: Message::QueueProposal(QueueCommand::Skip),
                    };
                    if let Ok(bytes) = serialize(&envelope) {
                        let _ = socket.send_to(&bytes, leader_addr).await;
                    }
                    synkrophase::session::runtime::print_event(
                        Some(&stdout),
                        "[Follower] Skip request forwarded to room leader",
                    );
                }
                "queue" => {
                    let queue_state = session.queue_snapshot();
                    let room_code = session.room_code();
                    let mut out = format!("Queue for room {}:\n", room_code);
                    if let Some(current) = queue_state.current {
                        out.push_str(&format!(
                            "  [PLAYING] {} (ID: {}, requested by {})\n",
                            current.title, current.id, current.requested_by
                        ));
                    } else {
                        out.push_str("  (Nothing playing)\n");
                    }
                    if queue_state.upcoming.is_empty() {
                        out.push_str("  (Upcoming queue is empty)\n");
                    } else {
                        out.push_str("\n  Upcoming:\n");
                        for (i, track) in queue_state.upcoming.iter().enumerate() {
                            out.push_str(&format!(
                                "  {}. {} (ID: {}, requested by {})\n",
                                i + 1,
                                track.title,
                                track.id,
                                track.requested_by
                            ));
                        }
                    }
                    synkrophase::session::runtime::print_event(Some(&stdout), out.trim_end());
                }
                "exit" | "quit" => {
                    synkrophase::session::runtime::print_event(Some(&stdout), "Exiting...");
                    return Ok(());
                }
                _ => {
                    synkrophase::session::runtime::print_event(
                        Some(&stdout),
                        "Available commands: play, pause, seek <sec>, status, skip, queue, exit",
                    );
                }
            }
        } else {
            let envelope = Envelope {
                sender,
                payload: Message::Chat {
                    sender,
                    name: name.clone(),
                    text: input.to_string(),
                },
            };
            if let Ok(bytes) = serialize(&envelope) {
                let _ = socket.send_to(&bytes, leader_addr).await;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_cli_parsing() {
        // Test Status
        let cli = Cli::try_parse_from(["synkro", "status"]).unwrap();
        assert!(matches!(cli.command, Commands::Status));

        // Test Host
        let cli = Cli::try_parse_from(["synkro", "host", "--room-code", "ABCDEF"]).unwrap();
        match cli.command {
            Commands::Host { room_code, .. } => assert_eq!(room_code, Some("ABCDEF".to_string())),
            _ => panic!("Expected Host command"),
        }

        // Test Pause
        let cli = Cli::try_parse_from(["synkro", "pause", "ROOM12"]).unwrap();
        match cli.command {
            Commands::Pause { room_code, .. } => assert_eq!(room_code, "ROOM12"),
            _ => panic!("Expected Pause command"),
        }

        // Test Transfer
        let cli = Cli::try_parse_from(["synkro", "transfer", "ROOM12", "00000000-0000-0000-0000-000000000001"]).unwrap();
        match cli.command {
            Commands::Transfer {
                room_code,
                device_id,
                ..
            } => {
                assert_eq!(room_code, "ROOM12");
                assert_eq!(device_id, "00000000-0000-0000-0000-000000000001");
            }
            _ => panic!("Expected Transfer command"),
        }
    }
}
