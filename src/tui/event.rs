use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::app::{InputMode, TuiApp};
use super::backend::TuiBackend;
use crate::error::Result;
use crate::protocol::messages::{PlaybackAction, Role};

pub async fn handle_key_event(
    app: &mut TuiApp,
    key: KeyEvent,
    backend: &dyn TuiBackend,
) -> Result<()> {
    match app.input_mode {
        InputMode::Normal => match key.code {
            KeyCode::Char('q') => {
                app.should_quit = true;
            }
            KeyCode::Esc => {
                if app.show_help {
                    app.show_help = false;
                    app.last_esc_press = None;
                } else if app.log_scroll_offset > 0 {
                    app.reset_log_scroll();
                } else {
                    let exit_label = if app.is_attached { "detach" } else { "exit" };
                    let now = std::time::Instant::now();
                    if let Some(prev) = app.last_esc_press {
                        if now.duration_since(prev) <= std::time::Duration::from_millis(1500) {
                            app.should_quit = true;
                        } else {
                            app.last_esc_press = Some(now);
                            app.add_log(
                                "System".to_string(),
                                format!("Press [Esc] again to {exit_label} (or press [q])"),
                            );
                        }
                    } else {
                        app.last_esc_press = Some(now);
                        app.add_log(
                            "System".to_string(),
                            format!("Press [Esc] again to {exit_label} (or press [q])"),
                        );
                    }
                }
            }
            KeyCode::Up | KeyCode::Char('k') => {
                app.scroll_logs_up(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                app.scroll_logs_down(1);
            }
            KeyCode::PageUp => {
                app.scroll_logs_up(5);
            }
            KeyCode::PageDown => {
                app.scroll_logs_down(5);
            }
            KeyCode::Home => {
                app.scroll_logs_up(app.logs.len());
            }
            KeyCode::End => {
                app.reset_log_scroll();
            }
            KeyCode::Char(' ') => {
                // Toggle play / pause
                let action = if app.playback.is_playing {
                    PlaybackAction::Pause
                } else {
                    PlaybackAction::Play
                };
                backend.send_playback_action(action).await?;
            }
            KeyCode::Left => {
                // Seek -5s
                let new_pos = (app.playback.position_us - 5_000_000).max(0);
                backend
                    .send_playback_action(PlaybackAction::Seek {
                        target_position_us: new_pos,
                    })
                    .await?;
            }
            KeyCode::Right => {
                // Seek +5s
                let new_pos = app.playback.position_us + 5_000_000;
                backend
                    .send_playback_action(PlaybackAction::Seek {
                        target_position_us: new_pos,
                    })
                    .await?;
            }
            KeyCode::Char('n') => {
                backend.send_playback_action(PlaybackAction::NextTrack).await?;
            }
            KeyCode::Char('p') => {
                backend
                    .send_playback_action(PlaybackAction::PreviousTrack)
                    .await?;
            }
            KeyCode::Char('v') => {
                backend.send_volume(None).await?;
            }
            KeyCode::Char('c') => {
                copy_invitation_command(app, backend).await;
            }
            KeyCode::Char('?') | KeyCode::Char('h') => {
                app.show_help = !app.show_help;
            }
            KeyCode::Char('/') | KeyCode::Char('i') => {
                app.input_mode = InputMode::Editing;
                app.input_buffer.clear();
                app.last_esc_press = None;
            }
            _ => {}
        },
        InputMode::Editing => match key.code {
            KeyCode::Esc => {
                app.input_mode = InputMode::Normal;
                app.input_buffer.clear();
                app.last_esc_press = None;
            }
            KeyCode::Enter => {
                let text = app.input_buffer.trim().to_string();
                app.input_buffer.clear();
                app.input_mode = InputMode::Normal;

                if text.is_empty() {
                    return Ok(());
                }

                if let Some(cmd) = text.strip_prefix('/') {
                    handle_slash_command(app, cmd, backend).await?;
                } else {
                    backend.send_chat(text).await?;
                }
            }
            KeyCode::Backspace => {
                app.input_buffer.pop();
            }
            KeyCode::Char(c) => {
                if key.modifiers == KeyModifiers::CONTROL && c == 'c' {
                    app.should_quit = true;
                } else {
                    app.input_buffer.push(c);
                }
            }
            _ => {}
        },
    }
    Ok(())
}

async fn handle_slash_command(
    app: &mut TuiApp,
    cmd: &str,
    backend: &dyn TuiBackend,
) -> Result<()> {
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(());
    }

    match parts[0] {
        "play" | "resume" => {
            backend.send_playback_action(PlaybackAction::Play).await?;
        }
        "pause" => {
            backend.send_playback_action(PlaybackAction::Pause).await?;
        }
        "seek" => {
            if parts.len() > 1
                && let Ok(sec) = parts[1].parse::<f64>()
            {
                let pos_us = (sec * 1_000_000.0) as i64;
                backend
                    .send_playback_action(PlaybackAction::Seek {
                        target_position_us: pos_us,
                    })
                    .await?;
            }
        }
        "next" | "skip" => {
            backend.send_playback_action(PlaybackAction::NextTrack).await?;
        }
        "prev" | "previous" => {
            backend
                .send_playback_action(PlaybackAction::PreviousTrack)
                .await?;
        }
        "volume" | "vol" => {
            let target_vol = if parts.len() > 1 {
                parts[1].parse::<u8>().ok()
            } else {
                None
            };
            backend.send_volume(target_vol).await?;
        }
        "sync" => {
            let role_str = if app.is_leader {
                "Leader"
            } else {
                "Follower"
            };
            let formatted_offset = crate::clock::format_offset_smart(app.drift.offset_us);
            let delay_ms = app
                .controller
                .as_ref()
                .map(|c| c.estimated_actuation_delay_us() / 1000)
                .unwrap_or(0);
            app.add_log(
                "System".to_string(),
                format!(
                    "Clock Sync Health: Offset: {} | Role: {} | Actuation Delay: ~{}ms",
                    formatted_offset,
                    role_str,
                    delay_ms
                ),
            );
        }
        "copy" | "share" | "invitation" | "join-cmd" => {
            copy_invitation_command(app, backend).await;
        }
        "transfer" => {
            if parts.len() < 2 {
                app.add_log(
                    "System".to_string(),
                    "Usage: /transfer <peer_name_or_uuid>".to_string(),
                );
                return Ok(());
            }
            let query = parts[1..].join(" ").to_lowercase();
            // Search amongst other room peers (excluding self)
            let matched_peer = app
                .peers
                .iter()
                .filter(|p| p.device_id != app.self_id)
                .find(|p| {
                    p.device_id.to_string().to_lowercase().starts_with(&query)
                        || p.name.to_lowercase().contains(&query)
                })
                .cloned();

            match matched_peer {
                Some(target) => {
                    backend.transfer_leadership(target.device_id).await?;
                }
                None => {
                    app.add_log(
                        "System".to_string(),
                        format!("No peer matching '{}' found in room", query),
                    );
                }
            }
        }
        "role" => {
            if parts.len() < 3 {
                app.add_log(
                    "System".to_string(),
                    "Usage: /role <peer_name_or_uuid> <leader|moderator|listener>".to_string(),
                );
                return Ok(());
            }
            let target_role_str = parts.last().unwrap().to_lowercase();
            let new_role = match target_role_str.as_str() {
                "leader" | "lead" | "host" => Role::Leader,
                "moderator" | "mod" => Role::Moderator,
                "listener" | "peer" | "user" => Role::Listener,
                _ => {
                    app.add_log(
                        "System".to_string(),
                        format!(
                            "Invalid role '{}'. Choose leader, moderator, or listener.",
                            target_role_str
                        ),
                    );
                    return Ok(());
                }
            };

            let query = parts[1..parts.len() - 1].join(" ").to_lowercase();
            let matched_peer = app
                .peers
                .iter()
                .filter(|p| p.device_id != app.self_id)
                .find(|p| {
                    p.device_id.to_string().to_lowercase().starts_with(&query)
                        || p.name.to_lowercase().contains(&query)
                })
                .cloned();

            match matched_peer {
                Some(target) => {
                    backend.assign_role(target.device_id, new_role).await?;
                }
                None => {
                    app.add_log(
                        "System".to_string(),
                        format!("No peer matching '{}' found in room", query),
                    );
                }
            }
        }
        "shutdown" => {
            backend.shutdown().await?;
            app.should_shutdown_daemon = true;
            app.should_quit = true;
        }
        "help" | "?" => {
            app.show_help = !app.show_help;
        }
        "quit" | "exit" => {
            app.should_quit = true;
        }
        _ => {
            app.add_log(
                "System".to_string(),
                format!("Unknown command: /{cmd} (Type /help for command list)"),
            );
        }
    }
    Ok(())
}

async fn copy_invitation_command(app: &mut TuiApp, backend: &dyn TuiBackend) {
    if let Some(inv) = &app.invitation {
        match inv.copy_to_clipboard() {
            Ok(()) => {
                app.add_log(
                    "System".to_string(),
                    "Join command copied to clipboard!".to_string(),
                );
                return;
            }
            Err(err) => {
                app.add_log(
                    "System".to_string(),
                    format!("Failed to copy to clipboard: {err}"),
                );
                return;
            }
        }
    }

    if let Ok(Some(cmd)) = backend.get_invitation_command().await {
        match crate::session::invitation::copy_text_to_clipboard(&cmd) {
            Ok(()) => {
                app.add_log("System".to_string(), format!("Copied '{cmd}' to clipboard!"));
            }
            Err(err) => {
                app.add_log(
                    "System".to_string(),
                    format!("Failed to copy to clipboard: {err}"),
                );
            }
        }
    } else {
        let cmd = format!("synkro join {}", app.room_code);
        let _ = crate::session::invitation::copy_text_to_clipboard(&cmd);
        app.add_log("System".to_string(), format!("Copied '{cmd}' to clipboard!"));
    }
}
