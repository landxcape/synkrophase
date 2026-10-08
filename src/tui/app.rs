use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::controller::{MediaController, PlaybackState};
use crate::protocol::messages::{PeerInfo, Role};
use crate::session::SessionState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    Normal,
    Editing,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp: String,
    pub source: String,
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct DriftInfo {
    pub offset_us: i64,
    pub zone: u8,
    pub status: String,
}

impl Default for DriftInfo {
    fn default() -> Self {
        Self {
            offset_us: 0,
            zone: 1,
            status: "Locked (<50ms)".to_string(),
        }
    }
}

pub enum AppEvent {
    Tick,
    Key(crossterm::event::KeyEvent),
    Log { source: String, text: String },
    DriftUpdate(i64, u8, String),
    PlaybackUpdate(PlaybackState),
    PeerListUpdate(Vec<PeerInfo>),
}

pub struct TuiApp {
    pub room_code: String,
    pub device_name: String,
    pub self_id: Uuid,
    pub role: Role,
    pub is_leader: bool,

    pub playback: PlaybackState,
    pub drift: DriftInfo,
    pub peers: Vec<PeerInfo>,
    pub logs: Vec<LogEntry>,

    pub input_mode: InputMode,
    pub input_buffer: String,
    pub show_help: bool,
    pub should_quit: bool,

    pub session: Arc<SessionState>,
    pub controller: Arc<dyn MediaController>,
    pub event_tx: mpsc::UnboundedSender<AppEvent>,
}

impl TuiApp {
    pub fn new(
        room_code: String,
        device_name: String,
        self_id: Uuid,
        role: Role,
        session: Arc<SessionState>,
        controller: Arc<dyn MediaController>,
        event_tx: mpsc::UnboundedSender<AppEvent>,
    ) -> Self {
        let is_leader = matches!(role, Role::Leader);
        Self {
            room_code,
            device_name,
            self_id,
            role,
            is_leader,
            playback: PlaybackState::default(),
            drift: DriftInfo::default(),
            peers: Vec::new(),
            logs: vec![LogEntry {
                timestamp: current_time_str(),
                source: "System".to_string(),
                text: "Session started. Ready.".to_string(),
            }],
            input_mode: InputMode::Normal,
            input_buffer: String::new(),
            show_help: false,
            should_quit: false,
            session,
            controller,
            event_tx,
        }
    }

    pub fn add_log(&mut self, source: String, text: String) {
        if self.logs.len() > 200 {
            self.logs.remove(0);
        }
        self.logs.push(LogEntry {
            timestamp: current_time_str(),
            source,
            text,
        });
    }

    pub fn progress_ratio(&self) -> f64 {
        if let Some(meta) = &self.playback.metadata
            && let Some(dur_us) = meta.duration_us
            && dur_us > 0
        {
            let ratio = (self.playback.position_us as f64) / (dur_us as f64);
            return ratio.clamp(0.0, 1.0);
        }
        0.0
    }
}

pub fn current_time_str() -> String {
    let now = std::time::SystemTime::now();
    let duration = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs() % 86400;
    let hours = (secs / 3600) % 24; // UTC
    let mins = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", hours, mins, s)
}
