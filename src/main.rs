use std::env;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use synkrophase::clock::sync::ClockSync;
use synkrophase::config::{DeviceConfig, SyncConfig};
use synkrophase::error::Result;
use synkrophase::playback::engine::{PlaybackBackend, PlaybackEngine};
use synkrophase::protocol::messages::{
    Envelope, Message, QueueCommand, QueueState, Track, serialize,
};
use synkrophase::session::discovery::Discovery;
use synkrophase::session::runtime::{LeaderAnchorBroadcaster, SessionMessageRuntime};
use synkrophase::session::{FollowerSyncRuntime, SessionState};
use synkrophase::stream::bootstrap::ensure_ytdlp;
use synkrophase::stream::resolver::StreamResolver;
use synkrophase::sync::controller::{ClockSource, PlaybackControl, SyncController};
use tokio::net::UdpSocket;
use tokio::time::sleep;
use uuid::Uuid;

const DEFAULT_CLOCK_PORT: u16 = 5870;
const DEFAULT_SESSION_PORT: u16 = 5871;

#[derive(Parser, Debug)]
#[command(name = "synkro", about = "Synchronized LAN media playback")]
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
    /// Add a YouTube URL to the queue for a room (leader serializes + resolves stream URL).
    Play {
        /// Session room code.
        room_code: String,
        /// YouTube URL to enqueue.
        url: String,
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
}

#[derive(Default)]
struct NoopPlaybackBackend;

impl PlaybackBackend for NoopPlaybackBackend {
    fn load_and_play(&self, _stream_url: &str) -> Result<()> {
        Ok(())
    }

    fn position_us(&self) -> i64 {
        0
    }

    fn set_rate(&self, _rate: f32) -> Result<()> {
        Ok(())
    }

    fn seek(&self, _position_us: i64) -> Result<()> {
        Ok(())
    }

    fn pause(&self) -> Result<()> {
        Ok(())
    }

    fn resume(&self) -> Result<()> {
        Ok(())
    }

    fn stop(&self) -> Result<()> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let device = load_device_config(cli.name, cli.ephemeral)?;
    let sync_config = SyncConfig::default();

    match cli.command {
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
            url,
            leader_addr,
        } => run_play(device, room_code, url, leader_addr).await,
        Commands::Pause {
            room_code,
            leader_addr,
        } => {
            run_simple_command(
                device.clone(),
                room_code,
                leader_addr,
                Message::Pause {
                    actor: device.device_id,
                },
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
            run_simple_command(
                device,
                room_code,
                leader_addr,
                Message::QueueProposal(QueueCommand::Skip),
            )
            .await
        }
        Commands::Queue {
            room_code,
            leader_addr,
        } => run_queue(device, room_code, leader_addr).await,
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
            let to = Uuid::parse_str(&device_id).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("invalid device_id: {e}"),
                )
            })?;
            run_simple_command(
                device,
                room_code,
                leader_addr,
                Message::TransferLeadership { to },
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
    let playback = Arc::new(build_playback_engine());
    let (rl, stdout) = rustyline_async::Readline::new("synkro> ".to_string()).unwrap();

    let runtime =
        SessionMessageRuntime::new(Arc::clone(&session), Some(stdout.clone()), device.name.clone()).with_playback(Arc::clone(&playback));
    let broadcaster = LeaderAnchorBroadcaster::new(
        Arc::clone(&session),
        Arc::clone(&clock),
        Arc::clone(&playback),
        sync_config.clone(),
    );

    let discovery = Discovery::new()?;
    discovery.register_session(&room_code, device.device_id, session_port)?;

    synkrophase::session::runtime::print_event(
        Some(&stdout),
        &format!(
            "Hosting room {room_code} as leader {} (clock {}, session {})",
            device.device_id, clock_port, session_port
        ),
    );

    let receive_task = tokio::spawn({
        let socket = Arc::clone(&session_socket);
        async move { runtime.run_receive_loop(socket).await }
    });
    let clock_task = tokio::spawn(async move { clock.run_responder().await });
    let anchor_task = tokio::spawn({
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
        async move { run_heartbeat_loop(session, socket, sender, room, true, cfg).await }
    });

    let election_task = tokio::spawn({
        let session = Arc::clone(&session);
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        let cfg = sync_config.clone();
        async move { run_election_loop(session, socket, sender, cfg).await }
    });

    let resolver = match ensure_ytdlp(&device) {
        Ok(path) => Some(StreamResolver::new(path)),
        Err(err) => {
            eprintln!("stream resolver disabled (yt-dlp missing): {err}");
            None
        }
    };
    let stream_task = tokio::spawn({
        let session = Arc::clone(&session);
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        let playback = Arc::clone(&playback);
        let stdout = stdout.clone();
        async move {
            run_stream_distribution_loop(session, socket, sender, resolver, playback, stdout).await
        }
    });

    let repl_task = tokio::spawn({
        let session = Arc::clone(&session);
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        let leader_addr =
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), session_port));
        let rl_stdout = stdout.clone();
        let name = device.name.clone();
        async move { run_repl(session, socket, sender, leader_addr, rl, rl_stdout, name).await }
    });

    tokio::select! {
        res = receive_task => {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => eprintln!("session receive loop stopped: {err}"),
                Err(err) => eprintln!("session receive task cancelled: {err}"),
            }
        }
        res = clock_task => {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => eprintln!("clock responder stopped: {err}"),
                Err(err) => eprintln!("clock responder task cancelled: {err}"),
            }
        }
        res = anchor_task => {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => eprintln!("anchor broadcaster stopped: {err}"),
                Err(err) => eprintln!("anchor broadcaster task cancelled: {err}"),
            }
        }
        res = heartbeat_task => {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => eprintln!("heartbeat loop stopped: {err}"),
                Err(err) => eprintln!("heartbeat task cancelled: {err}"),
            }
        }
        res = election_task => {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => eprintln!("election loop stopped: {err}"),
                Err(err) => eprintln!("election task cancelled: {err}"),
            }
        }
        res = stream_task => {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => eprintln!("stream distribution loop stopped: {err}"),
                Err(err) => eprintln!("stream distribution task cancelled: {err}"),
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
        }],
        QueueState::default(),
    ));
    let playback = Arc::new(build_playback_engine());
    let clock_source: Arc<dyn ClockSource> = clock.clone();
    let playback_control: Arc<dyn PlaybackControl> = playback.clone();
    let controller = Arc::new(SyncController::new(
        clock_source,
        playback_control,
        sync_config.clone(),
    ));
    let follower_sync = Arc::new(FollowerSyncRuntime::new(controller));
    follower_sync.start_sync_loop();

    let (rl, stdout) = rustyline_async::Readline::new("synkro> ".to_string()).unwrap();

    let runtime = SessionMessageRuntime::new(Arc::clone(&session), Some(stdout.clone()), device.name.clone())
        .with_follower_sync(Arc::clone(&follower_sync))
        .with_playback(Arc::clone(&playback));

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
    let clock_task = tokio::spawn(async move { clock.run_responder().await });

    let heartbeat_task = tokio::spawn({
        let session = Arc::clone(&session);
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        let cfg = sync_config.clone();
        let room = room_code.clone();
        async move { run_heartbeat_loop(session, socket, sender, room, false, cfg).await }
    });

    let election_task = tokio::spawn({
        let session = Arc::clone(&session);
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        let cfg = sync_config.clone();
        async move { run_election_loop(session, socket, sender, cfg).await }
    });

    let repl_task = tokio::spawn({
        let session = Arc::clone(&session);
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        let rl_stdout = stdout.clone();
        let name = device.name.clone();
        async move {
            run_repl(
                session,
                socket,
                sender,
                resolved_leader_addr,
                rl,
                rl_stdout,
                name,
            )
            .await
        }
    });

    tokio::select! {
        res = receive_task => {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => eprintln!("session receive loop stopped: {err}"),
                Err(err) => eprintln!("session receive task cancelled: {err}"),
            }
        }
        res = clock_task => {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => eprintln!("clock responder stopped: {err}"),
                Err(err) => eprintln!("clock responder task cancelled: {err}"),
            }
        }
        res = heartbeat_task => {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => eprintln!("heartbeat loop stopped: {err}"),
                Err(err) => eprintln!("heartbeat task cancelled: {err}"),
            }
        }
        res = election_task => {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => eprintln!("election loop stopped: {err}"),
                Err(err) => eprintln!("election task cancelled: {err}"),
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
            println!("Shutting down follower.");
        }
    }

    follower_sync.stop_sync_loop();
    Ok(())
}

fn resolve_join_target(
    room_code: &str,
    leader_addr: Option<SocketAddr>,
    leader_id: Option<Uuid>,
) -> Result<(SocketAddr, Uuid)> {
    if let Some(addr) = leader_addr {
        return Ok((addr, leader_id.unwrap_or(Uuid::nil())));
    }

    let discovery = Discovery::new()?;
    let sessions = discovery.find_sessions()?;
    sessions
        .into_iter()
        .find(|session| session.room_code == room_code)
        .map(|session| (session.leader_addr, session.leader_id))
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("room code {room_code} not found on LAN"),
            )
            .into()
        })
}

async fn send_join_request(
    socket: &UdpSocket,
    self_id: Uuid,
    leader_addr: SocketAddr,
    room_code: String,
    name: String,
) -> Result<()> {
    let envelope = Envelope {
        sender: self_id,
        payload: Message::JoinRequest { room_code, name },
    };
    let bytes = serialize(&envelope)?;
    socket.send_to(&bytes, leader_addr).await?;
    Ok(())
}

fn build_playback_engine() -> PlaybackEngine {
    match PlaybackEngine::new_rodio() {
        Ok(engine) => engine,
        Err(err) => {
            eprintln!("rodio backend unavailable, using noop playback backend: {err}");
            PlaybackEngine::new(Box::new(NoopPlaybackBackend))
        }
    }
}

fn load_device_config(name_opt: Option<String>, ephemeral: bool) -> Result<DeviceConfig> {
    let home = env::var_os("HOME").ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "HOME environment variable not set",
        )
    })?;
    let data_dir = PathBuf::from(home).join(".synkrophase");
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
            ytdlp_path: None,
        });
    }

    let id_path = data_dir.join("device_id");
    let device_id = if id_path.exists() {
        let raw = fs::read_to_string(&id_path)?;
        Uuid::parse_str(raw.trim()).unwrap_or_else(|_| Uuid::new_v4())
    } else {
        let generated = Uuid::new_v4();
        fs::write(&id_path, format!("{generated}\n"))?;
        generated
    };

    Ok(DeviceConfig {
        device_id,
        name,
        data_dir,
        ytdlp_path: None,
    })
}

fn generated_room_code(device_id: Uuid) -> String {
    device_id
        .simple()
        .to_string()
        .chars()
        .take(6)
        .collect::<String>()
        .to_uppercase()
}

async fn run_play(
    device: DeviceConfig,
    room_code: String,
    url: String,
    leader_addr: Option<SocketAddr>,
) -> Result<()> {
    let (resolved_leader_addr, _resolved_leader_id) =
        resolve_join_target(&room_code, leader_addr, None)?;

    let socket =
        UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))).await?;
    socket.set_broadcast(true)?;

    let ephemeral_id = Uuid::new_v4();

    send_join_request(
        &socket,
        ephemeral_id,
        resolved_leader_addr,
        room_code.clone(),
        device.name.clone(),
    )
    .await?;
    let _ = await_join_accepted(&socket, Duration::from_secs(2)).await;

    let track = Track {
        id: Uuid::new_v4().to_string(),
        youtube_url: url,
        title: "Pending".into(),
        requested_by: ephemeral_id,
    };
    let envelope = Envelope {
        sender: ephemeral_id,
        payload: Message::QueueProposal(QueueCommand::Add(track)),
    };
    let bytes = serialize(&envelope)?;
    socket.send_to(&bytes, resolved_leader_addr).await?;
    Ok(())
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
    socket.set_broadcast(true)?;

    let ephemeral_id = Uuid::new_v4();

    send_join_request(
        &socket,
        ephemeral_id,
        resolved_leader_addr,
        room_code.clone(),
        device.name.clone(),
    )
    .await?;
    let _ = await_join_accepted(&socket, Duration::from_secs(1)).await;

    let envelope = Envelope {
        sender: ephemeral_id,
        payload: message,
    };
    let bytes = serialize(&envelope)?;
    socket.send_to(&bytes, resolved_leader_addr).await?;
    Ok(())
}

async fn run_queue(
    device: DeviceConfig,
    room_code: String,
    leader_addr: Option<SocketAddr>,
) -> Result<()> {
    let (resolved_leader_addr, _) = resolve_join_target(&room_code, leader_addr, None)?;
    let socket =
        UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))).await?;
    socket.set_broadcast(true)?;

    let ephemeral_id = Uuid::new_v4();

    println!("Requesting queue state for room {}...", room_code);
    send_join_request(
        &socket,
        ephemeral_id,
        resolved_leader_addr,
        room_code.clone(),
        device.name.clone(),
    )
    .await?;

    let mut buf = [0u8; 8 * 1024];
    match tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut buf)).await {
        Ok(Ok((len, _))) => {
            let envelope = synkrophase::protocol::messages::deserialize(&buf[..len])?;
            if let Message::JoinAccepted { queue_state, .. } = envelope.payload {
                println!("\nQueue for room {}:", room_code);
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
            } else {
                println!("Received unexpected response from leader.");
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
    socket.set_broadcast(true)?;

    let ephemeral_id = Uuid::new_v4();

    println!("Collecting sync status for room {}...", room_code);
    send_join_request(
        &socket,
        ephemeral_id,
        resolved_leader_addr,
        room_code.clone(),
        device.name.clone(),
    )
    .await?;

    let mut peer_offsets = std::collections::HashMap::new();
    let mut buf = [0u8; 8 * 1024];
    let start = std::time::Instant::now();
    let duration = Duration::from_secs(2);

    while start.elapsed() < duration {
        let remaining = duration.saturating_sub(start.elapsed());
        if let Ok(Ok((len, _))) = tokio::time::timeout(remaining, socket.recv_from(&mut buf)).await
        {
            if let Ok(envelope) = synkrophase::protocol::messages::deserialize(&buf[..len]) {
                match envelope.payload {
                    Message::JoinAccepted { peer_list, .. } => {
                        for peer in peer_list {
                            peer_offsets.insert(peer.device_id, peer.clock_offset_us);
                        }
                        break;
                    }
                    Message::Heartbeat { .. } => {
                        // Heartbeat doesn't carry offset, but marks presence.
                        // We already have device_id from envelope.sender.
                    }
                    Message::SyncAnchor(_) => {
                        // Anchors are broadcast by leader.
                    }
                    _ => {}
                }
            }
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
    let (resolved_leader_addr, _) = resolve_join_target(&room_code, leader_addr, None)?;
    let socket =
        UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))).await?;
    socket.set_broadcast(true)?;

    let ephemeral_id = Uuid::new_v4();

    println!(
        "Collecting debug metrics for room {} (3s burst)...",
        room_code
    );
    send_join_request(
        &socket,
        ephemeral_id,
        resolved_leader_addr,
        room_code.clone(),
        device.name.clone(),
    )
    .await?;

    let mut buf = [0u8; 8 * 1024];
    let start = std::time::Instant::now();
    let duration = Duration::from_secs(3);

    println!("\n{:<10} {:<40} {:<20}", "Type", "Sender", "Data");
    println!("{}", "-".repeat(70));

    while start.elapsed() < duration {
        let remaining = duration.saturating_sub(start.elapsed());
        if let Ok(Ok((len, _))) = tokio::time::timeout(remaining, socket.recv_from(&mut buf)).await
        {
            if let Ok(envelope) = synkrophase::protocol::messages::deserialize(&buf[..len]) {
                match envelope.payload {
                    Message::SyncAnchor(anchor) => {
                        println!(
                            "{:<10} {:<40} Pos: {}us, Rate: {:.4}, Playing: {}",
                            "Anchor",
                            envelope.sender,
                            anchor.media_position_us,
                            anchor.playback_rate,
                            anchor.is_playing
                        );
                    }
                    Message::Heartbeat { is_leader, .. } => {
                        println!(
                            "{:<10} {:<40} Role: {}",
                            "Heartbeat",
                            envelope.sender,
                            if is_leader { "Leader" } else { "Follower" }
                        );
                    }
                    Message::JoinAccepted { peer_list, .. } => {
                        println!(
                            "{:<10} {:<40} Peers: {}",
                            "JoinAcc",
                            envelope.sender,
                            peer_list.len()
                        );
                    }
                    Message::QueueUpdate(state) => {
                        println!(
                            "{:<10} {:<40} Queue Ver: {}, Current: {}",
                            "QueueUpd",
                            envelope.sender,
                            state.version,
                            state
                                .current
                                .map(|t| t.title)
                                .unwrap_or_else(|| "None".into())
                        );
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

async fn await_join_accepted(socket: &UdpSocket, timeout: Duration) -> Result<()> {
    let mut buf = [0u8; 8 * 1024];
    let recv = tokio::time::timeout(timeout, socket.recv_from(&mut buf)).await;
    let Ok(Ok((len, _))) = recv else {
        return Ok(());
    };

    let Ok(envelope) = synkrophase::protocol::messages::deserialize(&buf[..len]) else {
        return Ok(());
    };
    if matches!(envelope.payload, Message::JoinAccepted { .. }) {
        return Ok(());
    }
    Ok(())
}

async fn run_stream_distribution_loop(
    session: Arc<SessionState>,
    socket: Arc<UdpSocket>,
    sender: Uuid,
    resolver: Option<StreamResolver>,
    playback: Arc<PlaybackEngine>,
    stdout: rustyline_async::SharedWriter,
) -> Result<()> {
    let Some(resolver) = resolver else {
        loop {
            sleep(Duration::from_secs(1)).await;
        }
    };

    loop {
        let queue = session.queue_snapshot();
        let Some(current) = queue.current else {
            sleep(Duration::from_millis(200)).await;
            continue;
        };

        let current_stream = session.stream_url_for(&current.id);

        let needs_resolve = if let Some(stream) = current_stream {
            if stream.expires_at == 0 {
                false
            } else {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                // Re-resolve if it expires in less than 5 minutes (300 seconds)
                stream.expires_at < now + 300
            }
        } else {
            true // Not resolved yet
        };

        if !needs_resolve {
            let status = playback.status();
            let is_same_track = status.track_id.as_deref() == Some(&current.id);

            if !is_same_track {
                if let Some(stream) = session.stream_url_for(&current.id) {
                    let url = stream.url.clone();
                    let track_id = current.id.clone();
                    let playback_clone = Arc::clone(&playback);
                    let out = stdout.clone();

                    let result = tokio::task::spawn_blocking(move || {
                        playback_clone.load_and_play(&track_id, &url)
                    })
                    .await;

                    if let Ok(Err(e)) = result {
                        let msg = format!("[System] Playback error: {}", e);
                        synkrophase::session::runtime::print_event(Some(&out), &msg);

                        let envelope = Envelope {
                            sender,
                            payload: Message::SystemLog(msg),
                        };
                        if let Ok(bytes) = serialize(&envelope) {
                            for addr in session.peer_socket_addrs() {
                                let _ = socket.send_to(&bytes, addr).await;
                            }
                        }

                        if let Ok(updated) = session.handle_queue_proposal(QueueCommand::Skip) {
                            let queue_update = Envelope {
                                sender,
                                payload: Message::QueueUpdate(updated),
                            };
                            if let Ok(bytes) = serialize(&queue_update) {
                                for addr in session.peer_socket_addrs() {
                                    let _ = socket.send_to(&bytes, addr).await;
                                }
                            }
                        }

                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            }

            sleep(Duration::from_millis(200)).await;
            continue;
        }

        let stream = match session.resolve_current_track(&resolver).await {
            Ok(stream) => stream,
            Err(err) => {
                let msg = format!("[System] Failed to resolve track: {}", err);
                synkrophase::session::runtime::print_event(Some(&stdout), &msg);

                let envelope = Envelope {
                    sender,
                    payload: Message::SystemLog(msg),
                };
                if let Ok(bytes) = serialize(&envelope) {
                    for addr in session.peer_socket_addrs() {
                        let _ = socket.send_to(&bytes, addr).await;
                    }
                }

                // Automatically skip the unresolvable track
                if let Ok(updated) = session.handle_queue_proposal(QueueCommand::Skip) {
                    let queue_update = Envelope {
                        sender,
                        payload: Message::QueueUpdate(updated),
                    };
                    if let Ok(bytes) = serialize(&queue_update) {
                        for addr in session.peer_socket_addrs() {
                            let _ = socket.send_to(&bytes, addr).await;
                        }
                    }
                }

                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let envelope = Envelope {
            sender,
            payload: Message::StreamUrl(stream.clone()),
        };
        let bytes = serialize(&envelope)?;
        for addr in session.peer_socket_addrs() {
            let _ = socket.send_to(&bytes, addr).await;
        }

        let status = playback.status();
        let is_same_track = status.track_id.as_deref() == Some(&current.id);

        if !is_same_track {
            let url = stream.url.clone();
            let track_id = current.id.clone();
            let playback_clone = Arc::clone(&playback);
            let out = stdout.clone();

            let result =
                tokio::task::spawn_blocking(move || playback_clone.load_and_play(&track_id, &url))
                    .await;

            if let Ok(Err(e)) = result {
                let msg = format!("[System] Playback error: {}", e);
                synkrophase::session::runtime::print_event(Some(&out), &msg);

                let envelope = Envelope {
                    sender,
                    payload: Message::SystemLog(msg),
                };
                if let Ok(bytes) = serialize(&envelope) {
                    for addr in session.peer_socket_addrs() {
                        let _ = socket.send_to(&bytes, addr).await;
                    }
                }

                if let Ok(updated) = session.handle_queue_proposal(QueueCommand::Skip) {
                    let queue_update = Envelope {
                        sender,
                        payload: Message::QueueUpdate(updated),
                    };
                    if let Ok(bytes) = serialize(&queue_update) {
                        for addr in session.peer_socket_addrs() {
                            let _ = socket.send_to(&bytes, addr).await;
                        }
                    }
                }

                sleep(Duration::from_secs(1)).await;
                continue;
            }
        }

        sleep(Duration::from_millis(200)).await;
    }
}

async fn run_heartbeat_loop(
    session: Arc<SessionState>,
    socket: Arc<UdpSocket>,
    sender: Uuid,
    room_code: String,
    is_leader: bool,
    config: SyncConfig,
) -> Result<()> {
    let port = socket.local_addr()?.port();
    loop {
        let envelope = Envelope {
            sender,
            payload: Message::Heartbeat {
                room_code: room_code.clone(),
                is_leader,
            },
        };
        let bytes = serialize(&envelope)?;
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::BROADCAST, port));
        let _ = socket.send_to(&bytes, addr).await?;

        for peer_addr in session.peer_socket_addrs() {
            if peer_addr.port() != port {
                let _ = socket.send_to(&bytes, peer_addr).await;
            }
        }

        sleep(Duration::from_millis(config.heartbeat_interval_ms)).await;
    }
}

async fn run_election_loop(
    session: Arc<SessionState>,
    socket: Arc<UdpSocket>,
    sender: Uuid,
    config: SyncConfig,
) -> Result<()> {
    let timeout = Duration::from_millis(config.heartbeat_timeout_ms);
    let port = socket.local_addr()?.port();
    let mut last_leader = session.leader_id();
    loop {
        session.prune_and_elect(timeout);
        let leader = session.leader_id();
        if leader != last_leader {
            last_leader = leader;
            let envelope = Envelope {
                sender,
                payload: Message::LeaderElected(leader),
            };
            let bytes = serialize(&envelope)?;
            let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::BROADCAST, port));
            let _ = socket.send_to(&bytes, addr).await?;

            for peer_addr in session.peer_socket_addrs() {
                if peer_addr.port() != port {
                    let _ = socket.send_to(&bytes, peer_addr).await;
                }
            }
        }
        sleep(Duration::from_millis(config.heartbeat_interval_ms)).await;
    }
}

async fn run_repl(
    session: Arc<SessionState>,
    socket: Arc<UdpSocket>,
    sender: Uuid,
    leader_addr: SocketAddr,
    mut rl: rustyline_async::Readline,
    stdout: rustyline_async::SharedWriter,
    name: String,
) -> Result<()> {
    loop {
        let input = match rl.readline().await {
            Ok(rustyline_async::ReadlineEvent::Line(line)) => {
                rl.add_history_entry(line.clone());
                line
            }
            Ok(rustyline_async::ReadlineEvent::Eof)
            | Ok(rustyline_async::ReadlineEvent::Interrupted) => break,
            Err(_) => break,
        };

        let input = input.trim();
        if input.is_empty() {
            continue;
        }

        if let Some(command_str) = input.strip_prefix('/') {
            let parts: Vec<&str> = command_str.split_whitespace().collect();
            if parts.is_empty() {
                continue;
            }
            match parts[0] {
                "play" => {
                    if parts.len() < 2 {
                        println!("Usage: /play <url>");
                        continue;
                    }
                    let track = Track {
                        id: Uuid::new_v4().to_string(),
                        youtube_url: parts[1].to_string(),
                        title: "Pending".into(),
                        requested_by: sender,
                    };
                    let envelope = Envelope {
                        sender,
                        payload: Message::QueueProposal(QueueCommand::Add(track)),
                    };
                    if let Ok(bytes) = serialize(&envelope) {
                        let _ = socket.send_to(&bytes, leader_addr).await;
                    }
                }
                "pause" => {
                    let envelope = Envelope {
                        sender,
                        payload: Message::Pause { actor: sender },
                    };
                    if let Ok(bytes) = serialize(&envelope) {
                        let _ = socket.send_to(&bytes, leader_addr).await;
                    }
                }
                "resume" => {
                    let envelope = Envelope {
                        sender,
                        payload: Message::Resume { actor: sender },
                    };
                    if let Ok(bytes) = serialize(&envelope) {
                        let _ = socket.send_to(&bytes, leader_addr).await;
                    }
                }
                "skip" => {
                    let envelope = Envelope {
                        sender,
                        payload: Message::QueueProposal(QueueCommand::Skip),
                    };
                    if let Ok(bytes) = serialize(&envelope) {
                        let _ = socket.send_to(&bytes, leader_addr).await;
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
                    out.push_str("\n  Upcoming:\n");
                    if queue_state.upcoming.is_empty() {
                        out.push_str("  (Upcoming queue is empty)\n");
                    } else {
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
                    std::process::exit(0);
                }
                _ => {
                    println!("Unknown command: /{}", parts[0]);
                    println!(
                        "Available commands: /play <url>, /pause, /resume, /skip, /queue, /exit"
                    );
                    println!("Anything else is sent as a chat message.");
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
        let cli = Cli::try_parse_from(["synkro", "transfer", "ROOM12", "device-uuid"]).unwrap();
        match cli.command {
            Commands::Transfer {
                room_code,
                device_id,
                ..
            } => {
                assert_eq!(room_code, "ROOM12");
                assert_eq!(device_id, "device-uuid");
            }
            _ => panic!("Expected Transfer command"),
        }
    }
}
