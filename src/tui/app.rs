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
    pub last_esc_press: Option<std::time::Instant>,
    pub invitation: Option<crate::session::invitation::RoomInvitation>,

    pub log_scroll_offset: usize,
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
            log_scroll_offset: 0,
            input_mode: InputMode::Normal,
            input_buffer: String::new(),
            show_help: false,
            should_quit: false,
            last_esc_press: None,
            invitation: None,
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
        if self.log_scroll_offset > 0 {
            // Keep viewing position stable when new messages arrive while scrolling history
            self.log_scroll_offset =
                (self.log_scroll_offset + 1).min(self.logs.len().saturating_sub(1));
        }
    }

    pub fn scroll_logs_up(&mut self, lines: usize) {
        let max_scroll = self.logs.len().saturating_sub(1);
        self.log_scroll_offset = (self.log_scroll_offset + lines).min(max_scroll);
    }

    pub fn scroll_logs_down(&mut self, lines: usize) {
        self.log_scroll_offset = self.log_scroll_offset.saturating_sub(lines);
    }

    pub fn reset_log_scroll(&mut self) {
        self.log_scroll_offset = 0;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::mock::MockMediaController;

    fn create_test_app() -> TuiApp {
        let (tx, _rx) = mpsc::unbounded_channel();
        let session = Arc::new(SessionState::new_leader(
            "TEST".to_string(),
            Uuid::new_v4(),
            "Device".to_string(),
        ));
        let controller = Arc::new(MockMediaController::new());
        TuiApp::new(
            "TEST".to_string(),
            "Device".to_string(),
            Uuid::new_v4(),
            Role::Leader,
            session,
            controller,
            tx,
        )
    }

    #[test]
    fn test_log_scrolling_and_auto_offset_preservation() {
        let mut app = create_test_app();
        assert_eq!(app.log_scroll_offset, 0);

        // Add 10 logs
        for i in 0..10 {
            app.add_log("System".to_string(), format!("msg {i}"));
        }
        // Offset remains 0 when at bottom
        assert_eq!(app.log_scroll_offset, 0);

        // Scroll up 3 lines
        app.scroll_logs_up(3);
        assert_eq!(app.log_scroll_offset, 3);

        // Scroll up by more than total logs, should clamp to max_scroll
        app.scroll_logs_up(100);
        assert_eq!(app.log_scroll_offset, app.logs.len() - 1);

        // Scroll down 5 lines
        app.scroll_logs_down(5);
        assert_eq!(app.log_scroll_offset, app.logs.len() - 1 - 5);

        // Reset scroll jumps to 0
        app.reset_log_scroll();
        assert_eq!(app.log_scroll_offset, 0);

        // If scrolled up by 2, adding a new log increments scroll offset by 1
        app.scroll_logs_up(2);
        assert_eq!(app.log_scroll_offset, 2);
        app.add_log("Peer".to_string(), "new message".to_string());
        assert_eq!(app.log_scroll_offset, 3);
    }
}
