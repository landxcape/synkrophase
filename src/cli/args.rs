use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use uuid::Uuid;

pub const DEFAULT_CLOCK_PORT: u16 = 5870;
pub const DEFAULT_SESSION_PORT: u16 = 5871;
pub const DEFAULT_LEAD_TIME_US: u64 = 350_000; // 350ms dynamic lead time for pre-dispatch & OS actuation

#[derive(Parser, Debug)]
#[command(
    name = "synkro",
    version,
    about = "Synchronized LAN playback controller"
)]
pub struct Cli {
    /// Optional nickname for this device
    #[arg(long, global = true)]
    pub name: Option<String>,

    /// Use a random, temporary device ID (useful for local testing)
    #[arg(long, global = true)]
    pub ephemeral: bool,

    /// Run in headless background mode without launching the interactive TUI
    #[arg(long, global = true)]
    pub headless: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
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
        /// Run in headless mode without TUI.
        #[arg(long)]
        headless: bool,
        /// Disable automatically copying the join command to system clipboard on launch.
        #[arg(long)]
        no_copy: bool,
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
        /// Run in headless mode without TUI.
        #[arg(long)]
        headless: bool,
    },
    /// Send play intent across the room.
    Play,
    /// Pause playback across the room.
    Pause,
    /// Resume playback across the room.
    Resume,
    /// Skip to next track across the room.
    Next,
    /// Skip to previous track across the room.
    Prev,
    /// Skip current track across the room (alias for next).
    Skip,
    /// Seek timeline position in seconds.
    Seek {
        /// Target position in seconds (e.g. 45.5).
        position_sec: f64,
    },
    /// Sync room volume across all connected peers.
    Volume {
        /// Optional volume level (0-100). If omitted, mirrors leader volume.
        level: Option<u8>,
    },
    /// Show live sync status and drift metrics.
    Sync,
    /// Verbose sync metrics mode.
    Debug,
    /// Share / copy room invitation command to clipboard.
    Share,
    /// Transfer leadership to another peer (by UUID or name prefix).
    Transfer {
        /// Target peer UUID or name prefix.
        peer: String,
    },
    /// Assign a role to a peer (Leader, Moderator, Listener).
    Role {
        /// Target peer UUID or name prefix.
        peer: String,
        /// Role to assign: 'leader', 'moderator', or 'listener'.
        role: String,
    },
    /// Send a chat message to the room.
    Chat {
        /// Message text to broadcast.
        message: String,
    },
    /// Manage the headless background sync daemon.
    Daemon {
        #[command(subcommand)]
        command: DaemonCommands,
    },
}

#[derive(Subcommand, Debug, Clone)]
pub enum DaemonCommands {
    /// Run the daemon in foreground mode (for launchd/systemd or internal spawn).
    Run {
        #[command(subcommand)]
        mode: DaemonRunMode,
    },
    /// Spawn a detached background daemon process.
    Start {
        #[command(subcommand)]
        mode: DaemonRunMode,
    },
    /// Stop a running background daemon.
    Stop,
    /// Query status of a running daemon over IPC.
    Status {
        /// Output status in raw JSON format.
        #[arg(long)]
        json: bool,
    },
    /// Display or tail daemon logs.
    Logs {
        /// Follow log output in real-time.
        #[arg(short, long)]
        follow: bool,
    },
}

#[derive(Subcommand, Debug, Clone)]
pub enum DaemonRunMode {
    /// Host a room as leader in daemon mode.
    Host {
        /// Room code. If omitted, an alphanumeric room code is generated.
        room_code: Option<String>,
        #[arg(long, default_value_t = DEFAULT_CLOCK_PORT)]
        clock_port: u16,
        #[arg(long, default_value_t = DEFAULT_SESSION_PORT)]
        session_port: u16,
    },
    /// Join a room as follower in daemon mode.
    Join {
        /// Session room code.
        room_code: String,
        #[arg(long)]
        leader_addr: Option<SocketAddr>,
        #[arg(long)]
        leader_id: Option<Uuid>,
        #[arg(long, default_value_t = DEFAULT_CLOCK_PORT)]
        leader_clock_port: u16,
        #[arg(long, default_value_t = DEFAULT_SESSION_PORT)]
        session_port: u16,
    },
}
