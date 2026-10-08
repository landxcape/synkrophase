use rustyline_async::SharedWriter;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use uuid::Uuid;

use super::args::DEFAULT_LEAD_TIME_US;
use crate::clock::sync::ClockSync;
use crate::controller::MediaController;
use crate::error::Result;
use crate::protocol::messages::{Envelope, Message, PlaybackAction, PlaybackIntent, serialize};
use crate::session::SessionState;
use crate::sync::scheduler::IntentScheduler;

#[derive(Debug, Clone, PartialEq)]
pub enum ReplCommand {
    Play,
    Pause,
    Resume,
    Seek(f64),
    Next,
    Prev,
    Volume(Option<u8>),
    Status,
    Queue,
    Skip,
    Help,
    Exit,
    Chat(String),
}

impl ReplCommand {
    pub fn parse(input: &str) -> Self {
        let input = input.trim();
        if input.is_empty() {
            return Self::Chat(String::new());
        }

        let is_slash = input.starts_with('/');
        let cmd_text = input.strip_prefix('/').unwrap_or(input);
        let parts: Vec<&str> = cmd_text.split_whitespace().collect();

        if parts.is_empty() {
            return Self::Chat(String::new());
        }

        let is_known = matches!(
            parts[0],
            "play"
                | "pause"
                | "resume"
                | "seek"
                | "next"
                | "prev"
                | "previous"
                | "volume"
                | "vol"
                | "status"
                | "queue"
                | "skip"
                | "help"
                | "exit"
                | "quit"
        );

        if is_slash || is_known {
            match parts[0] {
                "play" => Self::Play,
                "pause" => Self::Pause,
                "resume" => Self::Resume,
                "seek" => {
                    if parts.len() > 1
                        && let Ok(sec) = parts[1].parse::<f64>()
                    {
                        Self::Seek(sec)
                    } else {
                        Self::Help
                    }
                }
                "next" => Self::Next,
                "prev" | "previous" => Self::Prev,
                "volume" | "vol" => {
                    let vol = if parts.len() > 1 {
                        parts[1].parse::<u8>().ok()
                    } else {
                        None
                    };
                    Self::Volume(vol)
                }
                "status" => Self::Status,
                "queue" => Self::Queue,
                "skip" => Self::Skip,
                "help" => Self::Help,
                "exit" | "quit" => Self::Exit,
                _ => Self::Help,
            }
        } else {
            Self::Chat(input.to_string())
        }
    }
}

pub fn print_queue(session: &SessionState, stdout: Option<&SharedWriter>) {
    let queue_state = session.queue_snapshot();
    let room_code = session.room_code();
    let mut out = format!("Queue for room {}:\n", room_code);
    if let Some(current) = queue_state.current {
        out.push_str(&format!(
            "  [PLAYING] {} (ID: {}, requested by {})\n",
            current.title, current.id, current.requested_by
        ));
    } else {
        out.push_str("  (Nothing playing)\n");
    }
    if queue_state.upcoming.is_empty() {
        out.push_str("  (Upcoming queue is empty)\n");
    } else {
        out.push_str("\n  Upcoming:\n");
        for (i, track) in queue_state.upcoming.iter().enumerate() {
            out.push_str(&format!(
                "  {}. {} (ID: {}, requested by {})\n",
                i + 1,
                track.title,
                track.id,
                track.requested_by
            ));
        }
    }
    crate::session::runtime::print_event(stdout, out.trim_end());
}

pub async fn print_status(
    controller: &Arc<dyn MediaController>,
    stdout: Option<&SharedWriter>,
) -> Result<()> {
    let state = controller.get_playback_state().await?;
    let mut out = format!(
        "Player Status: {}\nPosition: {:.2}s\n",
        if state.is_playing {
            "Playing"
        } else {
            "Paused"
        },
        (state.position_us as f64) / 1_000_000.0
    );
    if let Some(m) = state.metadata {
        out.push_str(&format!("Track: {}\n", m.title));
        if let Some(a) = m.artist {
            out.push_str(&format!("Artist: {}\n", a));
        }
    }
    crate::session::runtime::print_event(stdout, out.trim_end());
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn run_host_repl(
    mut rl: rustyline_async::Readline,
    session: Arc<SessionState>,
    socket: Arc<UdpSocket>,
    sender: Uuid,
    name: String,
    clock: Arc<ClockSync>,
    controller: Arc<dyn MediaController>,
    scheduler: Arc<IntentScheduler>,
    stdout: SharedWriter,
) -> Result<()> {
    loop {
        let line = rl.readline().await;
        let input = match line {
            Ok(rustyline_async::ReadlineEvent::Line(line)) => line,
            _ => break,
        };

        match ReplCommand::parse(&input) {
            ReplCommand::Play | ReplCommand::Resume => {
                let state = controller.get_playback_state().await?;
                let now = clock.reference_now();
                let target_ref_time = now + DEFAULT_LEAD_TIME_US;
                let title = state.metadata.map(|m| m.title);

                let intent = PlaybackIntent {
                    action: PlaybackAction::Play,
                    target_ref_time,
                    position_us: state.position_us,
                    track_title: title.clone(),
                };

                let envelope = Envelope {
                    sender,
                    payload: Message::Intent(intent.clone()),
                };
                if let Ok(bytes) = serialize(&envelope) {
                    for addr in session.peer_socket_addrs() {
                        let _ = socket.send_to(&bytes, addr).await;
                    }
                }

                let _ = scheduler.execute_intent(&intent).await;

                crate::session::runtime::print_event(
                    Some(&stdout),
                    &format!(
                        "[System] Play intent scheduled for {:?} at T+100ms",
                        title.unwrap_or_else(|| "active track".into())
                    ),
                );
            }
            ReplCommand::Pause => {
                let state = controller.get_playback_state().await?;
                let now = clock.reference_now();
                let target_ref_time = now + DEFAULT_LEAD_TIME_US;

                let intent = PlaybackIntent {
                    action: PlaybackAction::Pause,
                    target_ref_time,
                    position_us: state.position_us,
                    track_title: None,
                };

                let envelope = Envelope {
                    sender,
                    payload: Message::Intent(intent.clone()),
                };
                if let Ok(bytes) = serialize(&envelope) {
                    for addr in session.peer_socket_addrs() {
                        let _ = socket.send_to(&bytes, addr).await;
                    }
                }

                let _ = scheduler.execute_intent(&intent).await;

                crate::session::runtime::print_event(
                    Some(&stdout),
                    "[System] Pause intent scheduled at T+100ms",
                );
            }
            ReplCommand::Seek(sec) => {
                let pos_us = (sec * 1_000_000.0) as i64;
                let now = clock.reference_now();
                let target_ref_time = now + DEFAULT_LEAD_TIME_US;

                let intent = PlaybackIntent {
                    action: PlaybackAction::Seek {
                        target_position_us: pos_us,
                    },
                    target_ref_time,
                    position_us: pos_us,
                    track_title: None,
                };

                let envelope = Envelope {
                    sender,
                    payload: Message::Intent(intent.clone()),
                };
                if let Ok(bytes) = serialize(&envelope) {
                    for addr in session.peer_socket_addrs() {
                        let _ = socket.send_to(&bytes, addr).await;
                    }
                }

                let _ = scheduler.execute_intent(&intent).await;

                crate::session::runtime::print_event(
                    Some(&stdout),
                    &format!("[System] Seek to {:.2}s scheduled at T+100ms", sec),
                );
            }
            ReplCommand::Next | ReplCommand::Skip => {
                let now = clock.reference_now();
                let target_ref_time = now + DEFAULT_LEAD_TIME_US;

                let intent = PlaybackIntent {
                    action: PlaybackAction::NextTrack,
                    target_ref_time,
                    position_us: 0,
                    track_title: None,
                };

                let envelope = Envelope {
                    sender,
                    payload: Message::Intent(intent.clone()),
                };
                if let Ok(bytes) = serialize(&envelope) {
                    for addr in session.peer_socket_addrs() {
                        let _ = socket.send_to(&bytes, addr).await;
                    }
                }
                let _ = scheduler.execute_intent(&intent).await;
                crate::session::runtime::print_event(
                    Some(&stdout),
                    "[System] Next track scheduled at T+100ms",
                );
            }
            ReplCommand::Prev => {
                let now = clock.reference_now();
                let target_ref_time = now + DEFAULT_LEAD_TIME_US;

                let intent = PlaybackIntent {
                    action: PlaybackAction::PreviousTrack,
                    target_ref_time,
                    position_us: 0,
                    track_title: None,
                };

                let envelope = Envelope {
                    sender,
                    payload: Message::Intent(intent.clone()),
                };
                if let Ok(bytes) = serialize(&envelope) {
                    for addr in session.peer_socket_addrs() {
                        let _ = socket.send_to(&bytes, addr).await;
                    }
                }
                let _ = scheduler.execute_intent(&intent).await;
                crate::session::runtime::print_event(
                    Some(&stdout),
                    "[System] Previous track scheduled at T+100ms",
                );
            }
            ReplCommand::Volume(target_vol) => {
                let vol = match target_vol {
                    Some(v) => v.min(100),
                    None => controller.get_volume().await.unwrap_or(70),
                };
                let _ = controller.set_volume(vol).await;
                let envelope = Envelope {
                    sender,
                    payload: Message::SetVolume {
                        volume: vol,
                        actor: sender,
                    },
                };
                if let Ok(bytes) = serialize(&envelope) {
                    for addr in session.peer_socket_addrs() {
                        let _ = socket.send_to(&bytes, addr).await;
                    }
                }
                crate::session::runtime::print_event(
                    Some(&stdout),
                    &format!("[System] Room volume synced to {}%", vol),
                );
            }
            ReplCommand::Status => {
                let _ = print_status(&controller, Some(&stdout)).await;
            }
            ReplCommand::Queue => {
                print_queue(&session, Some(&stdout));
            }
            ReplCommand::Help => {
                crate::session::runtime::print_event(
                    Some(&stdout),
                    "Available commands: play, pause, seek <sec>, next, prev, vol [0-100], status, exit",
                );
            }
            ReplCommand::Exit => {
                crate::session::runtime::print_event(Some(&stdout), "Exiting...");
                return Ok(());
            }
            ReplCommand::Chat(text) => {
                if text.is_empty() {
                    continue;
                }
                let envelope = Envelope {
                    sender,
                    payload: Message::ChatBroadcast {
                        sender,
                        display_name: name.clone(),
                        text: text.clone(),
                    },
                };
                if let Ok(bytes) = serialize(&envelope) {
                    for addr in session.peer_socket_addrs() {
                        let _ = socket.send_to(&bytes, addr).await;
                    }
                }
                crate::session::runtime::print_event(Some(&stdout), &format!("[{name}]: {text}"));
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn run_follower_repl(
    mut rl: rustyline_async::Readline,
    session: Arc<SessionState>,
    socket: Arc<UdpSocket>,
    sender: Uuid,
    name: String,
    leader_addr: SocketAddr,
    controller: Arc<dyn MediaController>,
    _clock: Arc<ClockSync>,
    _scheduler: Arc<IntentScheduler>,
    stdout: SharedWriter,
) -> Result<()> {
    loop {
        let line = rl.readline().await;
        let input = match line {
            Ok(rustyline_async::ReadlineEvent::Line(line)) => line,
            _ => break,
        };

        match ReplCommand::parse(&input) {
            ReplCommand::Play | ReplCommand::Resume => {
                let state = controller.get_playback_state().await.unwrap_or_default();
                let title = state.metadata.map(|m| m.title);
                let intent = PlaybackIntent {
                    action: PlaybackAction::Play,
                    target_ref_time: 0,
                    position_us: state.position_us,
                    track_title: title,
                };
                let envelope = Envelope {
                    sender,
                    payload: Message::Intent(intent),
                };
                if let Ok(bytes) = serialize(&envelope) {
                    let _ = socket.send_to(&bytes, leader_addr).await;
                }
                crate::session::runtime::print_event(
                    Some(&stdout),
                    "[Follower] Play request forwarded to room leader",
                );
            }
            ReplCommand::Pause => {
                let state = controller.get_playback_state().await.unwrap_or_default();
                let intent = PlaybackIntent {
                    action: PlaybackAction::Pause,
                    target_ref_time: 0,
                    position_us: state.position_us,
                    track_title: None,
                };
                let envelope = Envelope {
                    sender,
                    payload: Message::Intent(intent),
                };
                if let Ok(bytes) = serialize(&envelope) {
                    let _ = socket.send_to(&bytes, leader_addr).await;
                }
                crate::session::runtime::print_event(
                    Some(&stdout),
                    "[Follower] Pause request forwarded to room leader",
                );
            }
            ReplCommand::Seek(sec) => {
                let pos_us = (sec * 1_000_000.0) as i64;
                let intent = PlaybackIntent {
                    action: PlaybackAction::Seek {
                        target_position_us: pos_us,
                    },
                    target_ref_time: 0,
                    position_us: pos_us,
                    track_title: None,
                };
                let envelope = Envelope {
                    sender,
                    payload: Message::Intent(intent),
                };
                if let Ok(bytes) = serialize(&envelope) {
                    let _ = socket.send_to(&bytes, leader_addr).await;
                }
                crate::session::runtime::print_event(
                    Some(&stdout),
                    &format!("[Follower] Seek to {:.2}s forwarded to room leader", sec),
                );
            }
            ReplCommand::Next | ReplCommand::Skip => {
                let intent = PlaybackIntent {
                    action: PlaybackAction::NextTrack,
                    target_ref_time: 0,
                    position_us: 0,
                    track_title: None,
                };
                let envelope = Envelope {
                    sender,
                    payload: Message::Intent(intent),
                };
                if let Ok(bytes) = serialize(&envelope) {
                    let _ = socket.send_to(&bytes, leader_addr).await;
                }
                crate::session::runtime::print_event(
                    Some(&stdout),
                    "[Follower] Next track request forwarded to room leader",
                );
            }
            ReplCommand::Prev => {
                let intent = PlaybackIntent {
                    action: PlaybackAction::PreviousTrack,
                    target_ref_time: 0,
                    position_us: 0,
                    track_title: None,
                };
                let envelope = Envelope {
                    sender,
                    payload: Message::Intent(intent),
                };
                if let Ok(bytes) = serialize(&envelope) {
                    let _ = socket.send_to(&bytes, leader_addr).await;
                }
                crate::session::runtime::print_event(
                    Some(&stdout),
                    "[Follower] Previous track request forwarded to room leader",
                );
            }
            ReplCommand::Volume(target_vol) => {
                let vol = match target_vol {
                    Some(v) => v.min(100),
                    None => controller.get_volume().await.unwrap_or(70),
                };
                let envelope = Envelope {
                    sender,
                    payload: Message::SetVolume {
                        volume: vol,
                        actor: sender,
                    },
                };
                if let Ok(bytes) = serialize(&envelope) {
                    let _ = socket.send_to(&bytes, leader_addr).await;
                }
                crate::session::runtime::print_event(
                    Some(&stdout),
                    &format!("[Follower] Volume sync ({}%) requested to room leader", vol),
                );
            }
            ReplCommand::Status => {
                let _ = print_status(&controller, Some(&stdout)).await;
            }
            ReplCommand::Queue => {
                print_queue(&session, Some(&stdout));
            }
            ReplCommand::Help => {
                crate::session::runtime::print_event(
                    Some(&stdout),
                    "Available commands: play, pause, seek <sec>, next, prev, vol [0-100], status, exit",
                );
            }
            ReplCommand::Exit => {
                crate::session::runtime::print_event(Some(&stdout), "Exiting...");
                return Ok(());
            }
            ReplCommand::Chat(text) => {
                if text.is_empty() {
                    continue;
                }
                let envelope = Envelope {
                    sender,
                    payload: Message::Chat {
                        sender,
                        name: name.clone(),
                        text,
                    },
                };
                if let Ok(bytes) = serialize(&envelope) {
                    let _ = socket.send_to(&bytes, leader_addr).await;
                }
            }
        }
    }
    Ok(())
}
