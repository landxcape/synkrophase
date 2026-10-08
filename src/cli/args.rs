use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use uuid::Uuid;

pub const DEFAULT_CLOCK_PORT: u16 = 5870;
pub const DEFAULT_SESSION_PORT: u16 = 5871;
pub const DEFAULT_LEAD_TIME_US: u64 = 100_000; // 100ms dynamic lead time

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
    /// Skip to next track across the room.
    Next {
        room_code: String,
        #[arg(long)]
        leader_addr: Option<SocketAddr>,
    },
    /// Skip to previous track across the room.
    Prev {
        room_code: String,
        #[arg(long)]
        leader_addr: Option<SocketAddr>,
    },
    /// Skip current track across the room (alias for next).
    Skip {
        room_code: String,
        #[arg(long)]
        leader_addr: Option<SocketAddr>,
    },
    /// Sync room volume across all connected peers.
    Volume {
        room_code: String,
        /// Optional volume level (0-100). If omitted, host volume is mirrored.
        #[arg(long)]
        level: Option<u8>,
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
