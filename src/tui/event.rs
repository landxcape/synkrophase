use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::app::{InputMode, TuiApp};
use crate::error::Result;
use crate::protocol::messages::PlaybackAction;
use crate::session::engine::{EngineCommand, SynkroEngine};

pub async fn handle_key_event(
    app: &mut TuiApp,
    key: KeyEvent,
    engine: &SynkroEngine,
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
                } else {
                    let now = std::time::Instant::now();
                    if let Some(prev) = app.last_esc_press {
                        if now.duration_since(prev) <= std::time::Duration::from_millis(1500) {
                            app.should_quit = true;
                        } else {
                            app.last_esc_press = Some(now);
                            app.add_log(
                                "System".to_string(),
                                "Press [Esc] again to exit (or press [q])".to_string(),
                            );
                        }
                    } else {
                        app.last_esc_press = Some(now);
                        app.add_log(
                            "System".to_string(),
                            "Press [Esc] again to exit (or press [q])".to_string(),
                        );
                    }
                }
            }
            KeyCode::Char(' ') => {
                // Toggle play / pause
                let action = if app.playback.is_playing {
                    PlaybackAction::Pause
                } else {
                    PlaybackAction::Play
                };
                engine.send_command(EngineCommand::PlaybackAction(action))?;
            }
            KeyCode::Left => {
                // Seek -5s
                let new_pos = (app.playback.position_us - 5_000_000).max(0);
                engine.send_command(EngineCommand::PlaybackAction(PlaybackAction::Seek {
                    target_position_us: new_pos,
                }))?;
            }
            KeyCode::Right => {
                // Seek +5s
                let new_pos = app.playback.position_us + 5_000_000;
                engine.send_command(EngineCommand::PlaybackAction(PlaybackAction::Seek {
                    target_position_us: new_pos,
                }))?;
            }
            KeyCode::Char('n') => {
                engine.send_command(EngineCommand::PlaybackAction(PlaybackAction::NextTrack))?;
            }
            KeyCode::Char('p') => {
                engine
                    .send_command(EngineCommand::PlaybackAction(PlaybackAction::PreviousTrack))?;
            }
            KeyCode::Char('v') => {
                engine.send_command(EngineCommand::SyncVolume(None))?;
            }
            KeyCode::Char('c') => {
                copy_invitation_command(app);
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
                    handle_slash_command(app, cmd, engine).await?;
                } else {
                    engine.send_command(EngineCommand::SendChat(text))?;
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

async fn handle_slash_command(app: &mut TuiApp, cmd: &str, engine: &SynkroEngine) -> Result<()> {
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(());
    }

    match parts[0] {
        "play" | "resume" => {
            engine.send_command(EngineCommand::PlaybackAction(PlaybackAction::Play))?;
        }
        "pause" => {
            engine.send_command(EngineCommand::PlaybackAction(PlaybackAction::Pause))?;
        }
        "seek" => {
            if parts.len() > 1
                && let Ok(sec) = parts[1].parse::<f64>()
            {
                let pos_us = (sec * 1_000_000.0) as i64;
                engine.send_command(EngineCommand::PlaybackAction(PlaybackAction::Seek {
                    target_position_us: pos_us,
                }))?;
            }
        }
        "next" | "skip" => {
            engine.send_command(EngineCommand::PlaybackAction(PlaybackAction::NextTrack))?;
        }
        "prev" | "previous" => {
            engine.send_command(EngineCommand::PlaybackAction(PlaybackAction::PreviousTrack))?;
        }
        "volume" | "vol" => {
            let target_vol = if parts.len() > 1 {
                parts[1].parse::<u8>().ok()
            } else {
                None
            };
            engine.send_command(EngineCommand::SyncVolume(target_vol))?;
        }
        "sync" => {
            let offset = engine.clock().offset();
            let role_str = if app.is_leader {
                "Leader (Local)"
            } else {
                "Follower"
            };
            let formatted_offset = crate::clock::format_offset_smart(offset);
            app.add_log(
                "System".to_string(),
                format!(
                    "Clock Sync Health: Offset: {} | Role: {} | Actuation Delay: ~{}ms",
                    formatted_offset,
                    role_str,
                    app.controller.estimated_actuation_delay_us() / 1000
                ),
            );
        }
        "copy" | "share" | "invitation" | "join-cmd" => {
            copy_invitation_command(app);
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
                    engine.send_command(EngineCommand::TransferLeadership(target.device_id))?;
                }
                None => {
                    app.add_log(
                        "System".to_string(),
                        format!("No peer matching '{}' found in room", query),
                    );
                }
            }
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

fn copy_invitation_command(app: &mut TuiApp) {
    if let Some(inv) = &app.invitation {
        match inv.copy_to_clipboard() {
            Ok(()) => {
                app.add_log(
                    "System".to_string(),
                    "Join command copied to clipboard!".to_string(),
                );
            }
            Err(err) => {
                app.add_log(
                    "System".to_string(),
                    format!("Failed to copy to clipboard: {err}"),
                );
            }
        }
    } else {
        // Fallback for follower or when invitation is not set
        let cmd = format!("synkro join {}", app.room_code);
        match crate::session::invitation::copy_text_to_clipboard(&cmd) {
            Ok(()) => {
                app.add_log(
                    "System".to_string(),
                    format!("Copied '{cmd}' to clipboard!"),
                );
            }
            Err(err) => {
                app.add_log(
                    "System".to_string(),
                    format!("Failed to copy to clipboard: {err}"),
                );
            }
        }
    }
}
