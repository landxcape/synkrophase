#[cfg(unix)]
use std::io::stdout;
#[cfg(unix)]
use std::time::Duration;

#[cfg(unix)]
use crossterm::{
    event::{Event, EventStream},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
#[cfg(unix)]
use futures::StreamExt;
#[cfg(unix)]
use ratatui::{Terminal, backend::CrosstermBackend};
#[cfg(unix)]
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
#[cfg(unix)]
use tokio::net::UnixStream;
#[cfg(unix)]
use tokio::sync::mpsc;

#[cfg(unix)]
use crate::controller::{PlaybackState, TrackMetadata};
use crate::daemon::paths::DaemonPaths;
#[cfg(unix)]
use crate::daemon::protocol::{DaemonStatusSnapshot, IpcEvent, IpcRequest, IpcResponse};
use crate::error::Result;
#[cfg(unix)]
use crate::error::SynkroError;
#[cfg(unix)]
use crate::protocol::messages::Role;
#[cfg(unix)]
use crate::tui::app::{AppEvent, DriftInfo, TuiApp};
#[cfg(unix)]
use crate::tui::backend::IpcBackend;
#[cfg(unix)]
use crate::tui::ui;

pub async fn run_attached_tui(paths: DaemonPaths) -> Result<()> {
    #[cfg(not(unix))]
    {
        let _ = paths;
        Err(crate::error::SynkroError::Config(
            "Daemon mode and attach are currently only supported on Unix systems.".to_string(),
        ))
    }

    #[cfg(unix)]
    {
        if !paths.is_running() {
            return Err(SynkroError::Config(
                "No running Synkrophase daemon found. Start one with 'synkro daemon start host' or 'synkro daemon start join <ROOM>'.".to_string(),
            ));
        }

        let stream = UnixStream::connect(&paths.socket_path).await.map_err(|e| {
            SynkroError::Network(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!(
                    "Failed to connect to Synkrophase daemon socket {:?}: {e}",
                    paths.socket_path
                ),
            ))
        })?;

        let (reader, mut writer) = stream.into_split();
        let mut lines = BufReader::new(reader).lines();

        // 1. Request initial daemon status snapshot
        let req = IpcRequest {
            id: Some(1),
            method: "status".to_string(),
            params: serde_json::Value::Null,
        };
        let mut msg =
            serde_json::to_string(&req).map_err(|e| SynkroError::Serialization(e.to_string()))?;
        msg.push('\n');
        writer
            .write_all(msg.as_bytes())
            .await
            .map_err(|e| SynkroError::Network(std::io::Error::other(e.to_string())))?;

        let status_line =
            match tokio::time::timeout(Duration::from_secs(3), lines.next_line()).await {
                Ok(Ok(Some(line))) => line,
                _ => {
                    return Err(SynkroError::Network(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Timed out waiting for daemon status snapshot",
                    )));
                }
            };

        let status_resp: IpcResponse = serde_json::from_str(&status_line)
            .map_err(|e| SynkroError::Deserialization(e.to_string()))?;
        if let Some(err) = status_resp.error {
            return Err(SynkroError::PermissionDenied(err.message));
        }
        let snapshot: DaemonStatusSnapshot =
            serde_json::from_value(status_resp.result.unwrap_or_default())
                .map_err(|e| SynkroError::Deserialization(e.to_string()))?;

        // 2. Subscribe to real-time events
        let sub_req = IpcRequest {
            id: Some(2),
            method: "subscribe".to_string(),
            params: serde_json::Value::Null,
        };
        let mut sub_msg = serde_json::to_string(&sub_req)
            .map_err(|e| SynkroError::Serialization(e.to_string()))?;
        sub_msg.push('\n');
        writer
            .write_all(sub_msg.as_bytes())
            .await
            .map_err(|e| SynkroError::Network(std::io::Error::other(e.to_string())))?;

        // Wait briefly for subscription response
        let _ = tokio::time::timeout(Duration::from_secs(2), lines.next_line()).await;

        // 3. Initialize TUI application state
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let mut app = TuiApp::new_attached(
            snapshot.room_code.clone(),
            snapshot.device_name.clone(),
            snapshot.self_id,
            snapshot.role,
            event_tx.clone(),
        );
        app.is_leader = matches!(snapshot.role, Role::Leader);
        app.peers = snapshot.peers.clone();
        app.playback = PlaybackState {
            is_playing: snapshot.is_playing,
            position_us: (snapshot.position_sec * 1_000_000.0) as i64,
            rate: 1.0,
            metadata: snapshot.track_title.as_ref().map(|title| TrackMetadata {
                title: title.clone(),
                artist: snapshot.track_artist.clone(),
                album: snapshot.track_album.clone(),
                duration_us: snapshot.duration_sec.map(|s| (s * 1_000_000.0) as u64),
            }),
        };
        app.drift = DriftInfo {
            offset_us: snapshot.drift_offset_us,
            zone: 1,
            status: snapshot.drift_status.clone(),
        };

        // 4. Set up Crossterm terminal
        enable_raw_mode()?;
        let mut stdout_handle = stdout();
        execute!(stdout_handle, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout_handle);
        let mut terminal = Terminal::new(backend)?;

        let tui_backend = IpcBackend::with_paths(paths.clone());

        // 5. Spawn background reader tasks
        let event_tx_stream = event_tx.clone();
        let stream_task = tokio::spawn(async move {
            while let Ok(Some(line)) = lines.next_line().await {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if let Ok(ev) = serde_json::from_str::<IpcEvent>(trimmed) {
                    match ev.event.as_str() {
                        "log" => {
                            let source = ev
                                .data
                                .get("source")
                                .and_then(|v| v.as_str())
                                .unwrap_or("Daemon")
                                .to_string();
                            let text = ev
                                .data
                                .get("text")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();
                            let _ = event_tx_stream.send(AppEvent::Log { source, text });
                        }
                        "drift_update" => {
                            let offset_us = ev
                                .data
                                .get("offset_us")
                                .and_then(|v| v.as_i64())
                                .unwrap_or(0);
                            let zone =
                                ev.data.get("zone").and_then(|v| v.as_u64()).unwrap_or(1) as u8;
                            let status = ev
                                .data
                                .get("status")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();
                            let _ = event_tx_stream
                                .send(AppEvent::DriftUpdate(offset_us, zone, status));
                        }
                        "playback_update" => {
                            if let Ok(state) = serde_json::from_value::<PlaybackState>(ev.data) {
                                let _ = event_tx_stream.send(AppEvent::PlaybackUpdate(state));
                            }
                        }
                        "network_update" => {
                            if let Ok(stats) =
                                serde_json::from_value::<crate::clock::NetworkStats>(ev.data)
                            {
                                let _ = event_tx_stream.send(AppEvent::NetworkUpdate(stats));
                            }
                        }
                        "peer_update" => {
                            let peers_val = ev.data.get("peers").unwrap_or(&ev.data);
                            if let Ok(peers) = serde_json::from_value::<
                                Vec<crate::protocol::messages::PeerInfo>,
                            >(peers_val.clone())
                            {
                                let _ = event_tx_stream.send(AppEvent::PeerListUpdate(peers));
                            }
                        }
                        _ => {}
                    }
                }
            }
            let _ = event_tx_stream.send(AppEvent::Log {
                source: "Daemon".to_string(),
                text: "Connection lost: Daemon terminated or socket closed. Press 'q' to exit."
                    .to_string(),
            });
        });

        let mut reader = EventStream::new();
        let event_tx_keys = event_tx.clone();
        let key_task = tokio::spawn(async move {
            while let Some(Ok(event)) = reader.next().await {
                if let Event::Key(key) = event {
                    let _ = event_tx_keys.send(AppEvent::Key(key));
                }
            }
        });

        let event_tx_tick = event_tx.clone();
        let tick_task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let _ = event_tx_tick.send(AppEvent::Tick);
            }
        });

        // 6. Main attached render & event dispatch loop
        while !app.should_quit {
            terminal.draw(|f| ui::render(f, &app))?;

            if let Some(event) = event_rx.recv().await {
                match event {
                    AppEvent::Tick => {
                        if app.playback.is_playing {
                            app.playback.position_us += 200_000;
                        }
                    }
                    AppEvent::Key(key) => {
                        crate::tui::event::handle_key_event(&mut app, key, &tui_backend).await?;
                    }
                    AppEvent::Log { source, text } => {
                        app.add_log(source, text);
                    }
                    AppEvent::PlaybackUpdate(state) => {
                        app.playback = state;
                    }
                    AppEvent::DriftUpdate(offset_us, zone, status) => {
                        app.drift.offset_us = offset_us;
                        app.drift.zone = zone;
                        app.drift.status = status;
                    }
                    AppEvent::NetworkUpdate(stats) => {
                        app.network = stats;
                    }
                    AppEvent::PeerListUpdate(peers) => {
                        app.update_peer_list(peers);
                    }
                }
            }
        }

        // 7. Cleanup tasks and terminal state
        stream_task.abort();
        key_task.abort();
        tick_task.abort();

        disable_raw_mode()?;
        execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
        terminal.show_cursor()?;

        let pid_str = paths
            .read_pid()
            .ok()
            .flatten()
            .map(|p| format!("PID {p}"))
            .unwrap_or_else(|| "background".to_string());

        if app.should_shutdown_daemon {
            println!("Synkrophase daemon shutdown requested. Daemon stopped.");
        } else {
            println!("Detached from Synkrophase daemon ({pid_str}).");
            println!(
                "Session remains active in background. Run 'synkro attach' to re-open or 'synkro daemon stop' to terminate."
            );
        }

        Ok(())
    }
}
