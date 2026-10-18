use std::sync::Arc;
use tokio::time::{Duration, sleep};

use crate::controller::MediaController;
use crate::error::Result;
use crate::protocol::messages::{PlaybackAction, Role};
use crate::session::SessionState;
use crate::session::engine::{EngineCommand, SynkroEngine};

/// `FollowerZeroTouchMonitor` continuously checks the follower's local media player.
///
/// When the user has `Moderator` role or higher and interacts directly with their
/// physical player controls (e.g. keyboard media keys, headphone buttons, or player UI):
/// - Discontinuity in play/pause state is detected.
/// - The local action is propagated to the room via `SynkroEngine`.
///
/// For standard `Listener` roles, manual disruptions are ignored and reconciled
/// back to the leader's playback trajectory.
pub struct FollowerZeroTouchMonitor {
    engine: Arc<SynkroEngine>,
    session: Arc<SessionState>,
    controller: Arc<dyn MediaController>,
}

impl FollowerZeroTouchMonitor {
    pub fn new(engine: Arc<SynkroEngine>) -> Self {
        let session = engine.session();
        let controller = engine.controller();
        Self {
            engine,
            session,
            controller,
        }
    }

    pub async fn run_monitor_loop(&self) -> Result<()> {
        let initial_state = self.controller.get_playback_state().await.ok();
        let mut last_playing = initial_state.as_ref().map(|s| s.is_playing).unwrap_or(false);
        let mut last_sample_time = std::time::Instant::now();
        let mut last_sample_pos = initial_state.as_ref().map(|s| s.position_us).unwrap_or(0);
        let mut last_track = initial_state
            .as_ref()
            .and_then(|s| s.metadata.as_ref().map(|m| m.title.clone()));

        loop {
            sleep(Duration::from_millis(250)).await;

            if self.session.is_leader() {
                // Leaders are already handled by LeaderAnchorBroadcaster
                continue;
            }

            let role = self.session.role();
            if role < Role::Moderator {
                // Only Moderators and above have playback control authority
                continue;
            }

            let Ok(state) = self.controller.get_playback_state().await else {
                continue;
            };

            let now = std::time::Instant::now();

            // 1. Play/Pause detection
            if state.is_playing != last_playing {
                tracing::info!(
                    was_playing = last_playing,
                    now_playing = state.is_playing,
                    "Zero-touch local player play/pause state change detected by Moderator"
                );

                let action = if state.is_playing {
                    PlaybackAction::Play
                } else {
                    PlaybackAction::Pause
                };

                let _ = self
                    .engine
                    .send_command(EngineCommand::PlaybackAction(action));
                last_playing = state.is_playing;
                last_sample_time = now;
                last_sample_pos = state.position_us;
            }

            // 2. Timeline scrub detection (>1.5s jump from continuous playback)
            if state.is_playing && last_playing && last_sample_pos > 0 {
                let elapsed_us = now.duration_since(last_sample_time).as_micros() as i64;
                let expected_pos_us =
                    last_sample_pos + (elapsed_us as f64 * state.rate as f64) as i64;
                let diff_us = (state.position_us - expected_pos_us).abs();

                if diff_us > 1_500_000 {
                    tracing::info!(
                        jump_ms = diff_us / 1000,
                        "Zero-touch timeline scrub detected by Moderator"
                    );
                    let _ = self.engine.send_command(EngineCommand::PlaybackAction(
                        PlaybackAction::Seek {
                            target_position_us: state.position_us,
                        },
                    ));
                }
            }

            // 3. Track skip detection
            let current_track = state.metadata.as_ref().map(|m| m.title.clone());
            if current_track != last_track && last_track.is_some() {
                tracing::info!(
                    prev = ?last_track,
                    curr = ?current_track,
                    "Zero-touch track skip detected by Moderator"
                );
                let _ = self
                    .engine
                    .send_command(EngineCommand::PlaybackAction(PlaybackAction::NextTrack));
                last_track = current_track.clone();
            }

            last_sample_time = now;
            last_sample_pos = state.position_us;
            last_playing = state.is_playing;
            if last_track.is_none() && current_track.is_some() {
                last_track = current_track;
            }
        }
    }
}
