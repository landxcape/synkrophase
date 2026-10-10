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
        let mut last_playing = if let Ok(s) = self.controller.get_playback_state().await {
            s.is_playing
        } else {
            false
        };

        loop {
            sleep(Duration::from_millis(500)).await;

            if self.session.is_leader() {
                // Leaders are already handled by LeaderAnchorBroadcaster
                continue;
            }

            let role = self.session.role();
            if role < Role::Moderator {
                // Only Moderators and above have playback control authority
                continue;
            }

            if let Ok(state) = self.controller.get_playback_state().await
                && state.is_playing != last_playing
            {
                tracing::info!(
                    was_playing = last_playing,
                    now_playing = state.is_playing,
                    "Zero-touch local player state change detected by Moderator"
                );

                let action = if state.is_playing {
                    PlaybackAction::Play
                } else {
                    PlaybackAction::Pause
                };

                let _ = self.engine.send_command(EngineCommand::PlaybackAction(action));
                last_playing = state.is_playing;
            }
        }
    }
}
