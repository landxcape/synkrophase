use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

use tokio::net::UdpSocket;
use uuid::Uuid;

use super::common::{resolve_join_target, send_join_request};
use crate::config::DeviceConfig;
use crate::error::Result;
use crate::protocol::messages::{Envelope, Message, serialize};

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
