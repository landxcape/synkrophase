use clap::Parser;
use synkrophase::cli::{
    Cli, Commands, create_media_controller, generated_room_code, load_device_config, run_debug,
    run_host, run_join, run_queue_display, run_simple_command, run_sync_status,
};
use synkrophase::config::SyncConfig;
use synkrophase::error::Result;
use synkrophase::protocol::messages::{Message, PlaybackAction, PlaybackIntent, QueueCommand};

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
            let intent = PlaybackIntent {
                action: PlaybackAction::Play,
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
            let target_uuid = uuid::Uuid::parse_str(&device_id).map_err(|e| {
                synkrophase::error::SynkroError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Invalid device UUID: {e}"),
                ))
            })?;
            run_simple_command(
                device,
                room_code,
                leader_addr,
                Message::TransferLeadership {
                    to: target_uuid,
                },
            )
            .await
        }
        Commands::Chat {
            room_code,
            message,
            leader_addr,
        } => {
            let envelope_msg = Message::Chat {
                sender: device.device_id,
                name: device.name.clone(),
                text: message,
            };
            run_simple_command(device, room_code, leader_addr, envelope_msg).await
        }
    }
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
        let cli = Cli::try_parse_from([
            "synkro",
            "transfer",
            "ROOM12",
            "00000000-0000-0000-0000-000000000001",
        ])
        .unwrap();
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
