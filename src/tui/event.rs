use std::net::SocketAddr;
use std::sync::Arc;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use tokio::net::UdpSocket;

use super::app::{InputMode, TuiApp};
use crate::clock::sync::ClockSync;
use crate::error::Result;
use crate::protocol::messages::{
    Envelope, Message, PlaybackAction, PlaybackIntent, QueueCommand, serialize,
};
use crate::sync::scheduler::IntentScheduler;

pub async fn handle_key_event(
    app: &mut TuiApp,
    key: KeyEvent,
    socket: &Arc<UdpSocket>,
    leader_addr: Option<SocketAddr>,
    clock: &Arc<ClockSync>,
    scheduler: &Arc<IntentScheduler>,
) -> Result<()> {
    match app.input_mode {
        InputMode::Normal => match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                app.should_quit = true;
            }
            KeyCode::Char(' ') => {
                // Toggle play / pause
                let action = if app.playback.is_playing {
                    PlaybackAction::Pause
                } else {
                    PlaybackAction::Play
                };
                send_playback_action(app, action, socket, leader_addr, clock, scheduler).await?;
            }
            KeyCode::Left => {
                // Seek -5s
                let new_pos = (app.playback.position_us - 5_000_000).max(0);
                send_playback_action(
                    app,
                    PlaybackAction::Seek {
                        target_position_us: new_pos,
                    },
                    socket,
                    leader_addr,
                    clock,
                    scheduler,
                )
                .await?;
            }
            KeyCode::Right => {
                // Seek +5s
                let new_pos = app.playback.position_us + 5_000_000;
                send_playback_action(
                    app,
                    PlaybackAction::Seek {
                        target_position_us: new_pos,
                    },
                    socket,
                    leader_addr,
                    clock,
                    scheduler,
                )
                .await?;
            }
            KeyCode::Char('/') | KeyCode::Char('i') => {
                app.input_mode = InputMode::Editing;
                app.input_buffer.clear();
            }
            _ => {}
        },
        InputMode::Editing => match key.code {
            KeyCode::Esc => {
                app.input_mode = InputMode::Normal;
                app.input_buffer.clear();
            }
            KeyCode::Enter => {
                let text = app.input_buffer.trim().to_string();
                app.input_buffer.clear();
                app.input_mode = InputMode::Normal;

                if text.is_empty() {
                    return Ok(());
                }

                if let Some(cmd) = text.strip_prefix('/') {
                    handle_slash_command(app, cmd, socket, leader_addr, clock, scheduler).await?;
                } else {
                    // Send chat message
                    send_chat_message(app, text, socket, leader_addr).await?;
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

async fn send_playback_action(
    app: &mut TuiApp,
    action: PlaybackAction,
    socket: &Arc<UdpSocket>,
    leader_addr: Option<SocketAddr>,
    clock: &Arc<ClockSync>,
    scheduler: &Arc<IntentScheduler>,
) -> Result<()> {
    if app.is_leader {
        let now = clock.reference_now();
        let target_ref_time = now + 100_000;
        let title = app.playback.metadata.as_ref().map(|m| m.title.clone());
        let pos = match action {
            PlaybackAction::Seek { target_position_us } => target_position_us,
            _ => app.playback.position_us,
        };

        let intent = PlaybackIntent {
            action,
            target_ref_time,
            position_us: pos,
            track_title: title,
        };

        let envelope = Envelope {
            sender: app.self_id,
            payload: Message::Intent(intent.clone()),
        };
        if let Ok(bytes) = serialize(&envelope) {
            for addr in app.session.peer_socket_addrs() {
                let _ = socket.send_to(&bytes, addr).await;
            }
        }
        let _ = scheduler.execute_intent(&intent).await;
    } else if let Some(addr) = leader_addr {
        let pos = match action {
            PlaybackAction::Seek { target_position_us } => target_position_us,
            _ => app.playback.position_us,
        };
        let intent = PlaybackIntent {
            action,
            target_ref_time: 0,
            position_us: pos,
            track_title: None,
        };
        let envelope = Envelope {
            sender: app.self_id,
            payload: Message::Intent(intent),
        };
        if let Ok(bytes) = serialize(&envelope) {
            let _ = socket.send_to(&bytes, addr).await;
        }
    }
    Ok(())
}

async fn handle_slash_command(
    app: &mut TuiApp,
    cmd: &str,
    socket: &Arc<UdpSocket>,
    leader_addr: Option<SocketAddr>,
    clock: &Arc<ClockSync>,
    scheduler: &Arc<IntentScheduler>,
) -> Result<()> {
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(());
    }

    match parts[0] {
        "play" | "resume" => {
            send_playback_action(
                app,
                PlaybackAction::Play,
                socket,
                leader_addr,
                clock,
                scheduler,
            )
            .await?;
        }
        "pause" => {
            send_playback_action(
                app,
                PlaybackAction::Pause,
                socket,
                leader_addr,
                clock,
                scheduler,
            )
            .await?;
        }
        "seek" => {
            if parts.len() > 1
                && let Ok(sec) = parts[1].parse::<f64>()
            {
                let pos_us = (sec * 1_000_000.0) as i64;
                send_playback_action(
                    app,
                    PlaybackAction::Seek {
                        target_position_us: pos_us,
                    },
                    socket,
                    leader_addr,
                    clock,
                    scheduler,
                )
                .await?;
            }
        }
        "skip" => {
            if app.is_leader {
                if let Ok(updated) = app.session.handle_queue_proposal(QueueCommand::Skip) {
                    let envelope = Envelope {
                        sender: app.self_id,
                        payload: Message::QueueUpdate(updated),
                    };
                    if let Ok(bytes) = serialize(&envelope) {
                        for addr in app.session.peer_socket_addrs() {
                            let _ = socket.send_to(&bytes, addr).await;
                        }
                    }
                }
            } else if let Some(addr) = leader_addr {
                let envelope = Envelope {
                    sender: app.self_id,
                    payload: Message::QueueProposal(QueueCommand::Skip),
                };
                if let Ok(bytes) = serialize(&envelope) {
                    let _ = socket.send_to(&bytes, addr).await;
                }
            }
        }
        "quit" | "exit" => {
            app.should_quit = true;
        }
        _ => {
            app.add_log("System".to_string(), format!("Unknown command: /{cmd}"));
        }
    }
    Ok(())
}

async fn send_chat_message(
    app: &mut TuiApp,
    text: String,
    socket: &Arc<UdpSocket>,
    leader_addr: Option<SocketAddr>,
) -> Result<()> {
    if app.is_leader {
        let envelope = Envelope {
            sender: app.self_id,
            payload: Message::ChatBroadcast {
                sender: app.self_id,
                display_name: app.device_name.clone(),
                text: text.clone(),
            },
        };
        if let Ok(bytes) = serialize(&envelope) {
            for addr in app.session.peer_socket_addrs() {
                let _ = socket.send_to(&bytes, addr).await;
            }
        }
        app.add_log(app.device_name.clone(), text);
    } else if let Some(addr) = leader_addr {
        let envelope = Envelope {
            sender: app.self_id,
            payload: Message::Chat {
                sender: app.self_id,
                name: app.device_name.clone(),
                text: text.clone(),
            },
        };
        if let Ok(bytes) = serialize(&envelope) {
            let _ = socket.send_to(&bytes, addr).await;
        }
        app.add_log(format!("{} (You)", app.device_name), text);
    }
    Ok(())
}
