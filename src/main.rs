use clap::Parser;
use synkrophase::cli::{
    Cli, Commands, create_media_controller, generated_room_code, load_device_config, run_host,
    run_join,
};
use synkrophase::config::SyncConfig;
use synkrophase::error::Result;

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
            println!(
                "  Playing:  {}",
                if state.is_playing { "Yes" } else { "No" }
            );
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
            headless,
            no_copy,
        } => {
            let room = room_code.unwrap_or_else(|| generated_room_code(device.device_id));
            let is_headless = headless || cli.headless;
            run_host(
                device,
                sync_config,
                room,
                clock_port,
                session_port,
                is_headless,
                no_copy,
            )
            .await
        }
        Commands::Join {
            room_code,
            leader_addr,
            leader_id,
            leader_clock_port,
            session_port,
            headless,
        } => {
            let is_headless = headless || cli.headless;
            run_join(
                device,
                sync_config,
                room_code,
                leader_addr,
                leader_id,
                leader_clock_port,
                session_port,
                is_headless,
            )
            .await
        }
        Commands::Play => {
            send_ipc_or_fail("play", serde_json::Value::Null, "Play intent sent to active session.").await
        }
        Commands::Pause => {
            send_ipc_or_fail("pause", serde_json::Value::Null, "Pause intent sent to active session.").await
        }
        Commands::Resume => {
            send_ipc_or_fail("resume", serde_json::Value::Null, "Resume intent sent to active session.").await
        }
        Commands::Next | Commands::Skip => {
            send_ipc_or_fail("next", serde_json::Value::Null, "Next track intent sent to active session.").await
        }
        Commands::Prev => {
            send_ipc_or_fail("prev", serde_json::Value::Null, "Previous track intent sent to active session.").await
        }
        Commands::Seek { position_sec } => {
            send_ipc_or_fail(
                "seek",
                serde_json::json!({ "position_sec": position_sec }),
                &format!("Seeked to {position_sec:.2}s across session."),
            )
            .await
        }
        Commands::Volume { level } => {
            let params = match level {
                Some(lvl) => serde_json::json!({ "level": lvl }),
                None => serde_json::Value::Null,
            };
            send_ipc_or_fail(
                "volume",
                params,
                &match level {
                    Some(lvl) => format!("Volume set to {lvl}% across session."),
                    None => "Mirrored host volume across session.".to_string(),
                },
            )
            .await
        }
        Commands::Sync => {
            if let Ok(Some(res)) = synkrophase::daemon::IpcClient::send_command("status", serde_json::Value::Null).await
                && let Ok(snapshot) = serde_json::from_value::<synkrophase::daemon::DaemonStatusSnapshot>(res)
            {
                println!("\n=== Synkrophase Session Sync Status ===");
                println!("  Room Code:      {}", snapshot.room_code);
                println!("  Role:           {:?}", snapshot.role);
                println!("  Clock Offset:   {:+0.2}ms", (snapshot.clock_offset_us as f64) / 1000.0);
                println!("  Drift Status:   {} ({:+0.2}ms)", snapshot.drift_status, (snapshot.drift_offset_us as f64) / 1000.0);
                println!("  Peers Online:   {}", snapshot.peers.len());
                for peer in snapshot.peers {
                    println!("    - {} ({:?}) offset: {:+0.2}ms", peer.name, peer.role, (peer.clock_offset_us as f64) / 1000.0);
                }
                println!();
                return Ok(());
            }
            eprintln!("Error: Synkrophase session is not running. Start with 'synkro host' or 'synkro daemon start'.");
            std::process::exit(1);
        }
        Commands::Debug => {
            if let Ok(Some(res)) = synkrophase::daemon::IpcClient::send_command("status", serde_json::Value::Null).await {
                println!("{}", serde_json::to_string_pretty(&res).unwrap_or_default());
                return Ok(());
            }
            eprintln!("Error: Synkrophase session is not running.");
            std::process::exit(1);
        }
        Commands::Share => {
            if let Ok(Some(res)) = synkrophase::daemon::IpcClient::send_command("invite", serde_json::Value::Null).await
                && let Some(cmd) = res.get("join_command").and_then(|v| v.as_str())
            {
                let _ = synkrophase::session::invitation::copy_text_to_clipboard(cmd);
                println!("Room Invitation Command (Copied to clipboard!):\n  {cmd}");
                return Ok(());
            }
            eprintln!("Error: Synkrophase session is not running.");
            std::process::exit(1);
        }
        Commands::Transfer { peer } => {
            send_ipc_or_fail(
                "transfer",
                serde_json::json!({ "target": peer }),
                &format!("Leadership transfer initiated to peer '{peer}'."),
            )
            .await
        }
        Commands::Role { peer, role } => {
            send_ipc_or_fail(
                "assign_role",
                serde_json::json!({ "target": peer, "role": role }),
                &format!("Assigned role '{role}' to peer '{peer}'."),
            )
            .await
        }
        Commands::Chat { message } => {
            send_ipc_or_fail(
                "chat",
                serde_json::json!({ "message": message }),
                "Chat message broadcasted.",
            )
            .await
        }
        Commands::Daemon { command } => {
            synkrophase::cli::handle_daemon_command(command, device, sync_config).await
        }
    }?;

    std::process::exit(0);
}

async fn send_ipc_or_fail(method: &str, params: serde_json::Value, success_msg: &str) -> synkrophase::error::Result<()> {
    match synkrophase::daemon::IpcClient::send_command(method, params).await {
        Ok(Some(_)) => {
            println!("{success_msg}");
            Ok(())
        }
        Ok(None) => {
            eprintln!("Error: Synkrophase is not running. Start a session with 'synkro host' or 'synkro daemon start'.");
            std::process::exit(1);
        }
        Err(err) => {
            eprintln!("IPC Error: {err}");
            std::process::exit(1);
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
        let cli = Cli::try_parse_from(["synkro", "pause"]).unwrap();
        assert!(matches!(cli.command, Commands::Pause));

        // Test Play
        let cli = Cli::try_parse_from(["synkro", "play"]).unwrap();
        assert!(matches!(cli.command, Commands::Play));

        // Test Seek
        let cli = Cli::try_parse_from(["synkro", "seek", "45.5"]).unwrap();
        match cli.command {
            Commands::Seek { position_sec } => {
                assert!((position_sec - 45.5).abs() < f64::EPSILON);
            }
            _ => panic!("Expected Seek command"),
        }

        // Test Transfer
        let cli = Cli::try_parse_from([
            "synkro",
            "transfer",
            "00000000-0000-0000-0000-000000000001",
        ])
        .unwrap();
        match cli.command {
            Commands::Transfer { peer } => {
                assert_eq!(peer, "00000000-0000-0000-0000-000000000001");
            }
            _ => panic!("Expected Transfer command"),
        }

        // Test Daemon Status
        let cli = Cli::try_parse_from(["synkro", "daemon", "status", "--json"]).unwrap();
        match cli.command {
            Commands::Daemon { command: synkrophase::cli::args::DaemonCommands::Status { json } } => {
                assert!(json);
            }
            _ => panic!("Expected Daemon Status command"),
        }

        // Test Daemon Stop
        let cli = Cli::try_parse_from(["synkro", "daemon", "stop"]).unwrap();
        match cli.command {
            Commands::Daemon { command: synkrophase::cli::args::DaemonCommands::Stop } => {}
            _ => panic!("Expected Daemon Stop command"),
        }

        // Test Daemon Start Host
        let cli = Cli::try_parse_from(["synkro", "daemon", "start", "host", "DEMO1"]).unwrap();
        match cli.command {
            Commands::Daemon {
                command: synkrophase::cli::args::DaemonCommands::Start {
                    mode: synkrophase::cli::args::DaemonRunMode::Host { room_code, .. },
                },
            } => {
                assert_eq!(room_code, Some("DEMO1".to_string()));
            }
            _ => panic!("Expected Daemon Start Host command"),
        }
    }
}
