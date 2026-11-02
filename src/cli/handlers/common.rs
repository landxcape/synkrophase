use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::sleep;
use uuid::Uuid;

use crate::config::SyncConfig;
use crate::controller::MediaController;
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(target_os = "linux")]
use crate::controller::linux::LinuxMediaController;
#[cfg(target_os = "macos")]
use crate::controller::macos::MacOsMediaController;
use crate::controller::mock::MockMediaController;
#[cfg(target_os = "windows")]
use crate::controller::windows::WindowsMediaController;
use crate::error::{Result, SynkroError};
use crate::protocol::messages::{Envelope, Message, serialize};
use crate::session::SessionState;
use crate::session::discovery::Discovery;

static USE_MOCK_CONTROLLER: AtomicBool = AtomicBool::new(false);

pub fn set_use_mock_controller(enable: bool) {
    USE_MOCK_CONTROLLER.store(enable, Ordering::Release);
}

pub fn create_media_controller() -> Arc<dyn MediaController> {
    if USE_MOCK_CONTROLLER.load(Ordering::Acquire)
        || std::env::var("SYNKRO_MOCK_CONTROLLER").is_ok()
    {
        return Arc::new(MockMediaController::new());
    }

    #[cfg(target_os = "macos")]
    {
        Arc::new(MacOsMediaController::new())
    }
    #[cfg(target_os = "linux")]
    {
        Arc::new(LinuxMediaController::new())
    }
    #[cfg(target_os = "windows")]
    {
        Arc::new(WindowsMediaController::new())
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
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

    if let Some(addr) = leader_addr {
        return Ok((addr, leader_id.unwrap_or_else(Uuid::new_v4)));
    }

    let discovery = Discovery::new()?;
    let found = discovery.find_session_by_room(room_code, Duration::from_millis(2000))?;
    let _ = discovery.shutdown();

    if let Some(info) = found {
        let addr = info.leader_addr;
        let id = leader_id.unwrap_or(info.leader_id);
        return Ok((addr, id));
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

#[allow(clippy::too_many_arguments)]
pub async fn perform_join_handshake(
    socket: &UdpSocket,
    sender: Uuid,
    leader_addr: SocketAddr,
    room_code: String,
    name: String,
    session: &Arc<SessionState>,
    max_attempts: usize,
    interval: Duration,
) -> Result<()> {
    for attempt in 1..=max_attempts {
        send_join_request(socket, sender, leader_addr, room_code.clone(), name.clone()).await?;

        let start = tokio::time::Instant::now();
        while start.elapsed() < interval {
            if session.is_join_confirmed() {
                return Ok(());
            }
            sleep(Duration::from_millis(50)).await;
        }

        if attempt < max_attempts {
            tracing::warn!(
                attempt,
                max_attempts,
                "Retrying join request to leader at {}",
                leader_addr
            );
        }
    }

    if session.is_join_confirmed() {
        Ok(())
    } else {
        Err(SynkroError::Network(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!(
                "Join timed out: host at {leader_addr} did not respond to join request for room '{room_code}'"
            ),
        )))
    }
}

pub async fn run_heartbeat_loop(
    session: Arc<SessionState>,
    socket: Arc<UdpSocket>,
    sender: Uuid,
    room_code: String,
    target_leader: Option<SocketAddr>,
    config: SyncConfig,
    clock: Option<Arc<crate::clock::sync::ClockSync>>,
) -> Result<()> {
    loop {
        sleep(Duration::from_millis(config.heartbeat_interval_ms)).await;
        if let Some(c) = &clock {
            session.set_clock_offset(c.residual_offset());
        }
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
            // Follower monitoring: check if leader or other peers timed out.
            // Never trigger election or self-promotion while still in unconfirmed joining state.
            if !session.is_join_confirmed() {
                continue;
            }

            let leader_id = session.leader_id();
            let expired = session.prune_and_appoint(timeout).0;

            if expired.contains(&leader_id) || !session.is_alive(&leader_id) {
                // Leader timed out or departed! Determine new leader deterministically
                let live_infos: Vec<_> = session
                    .all_alive_peers()
                    .into_iter()
                    .map(|(_, e)| e.info)
                    .collect();
                let heir =
                    crate::session::leader::appoint_successor(&live_infos, &session.self_info());

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
