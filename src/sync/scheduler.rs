use std::sync::Arc;

use crate::controller::MediaController;
use crate::error::Result;
use crate::protocol::messages::{PlaybackAction, PlaybackIntent};
use crate::session::SessionState;
use crate::sync::controller::ClockSource;
use crate::sync::tracer::TimelineTracer;

pub struct IntentScheduler {
    clock: Arc<dyn ClockSource>,
    controller: Arc<dyn MediaController>,
    tracer: TimelineTracer,
    session: Option<Arc<SessionState>>,
}

impl IntentScheduler {
    pub fn new(clock: Arc<dyn ClockSource>, controller: Arc<dyn MediaController>) -> Self {
        Self::with_session_opt(clock, controller, None)
    }

    pub fn with_session(
        clock: Arc<dyn ClockSource>,
        controller: Arc<dyn MediaController>,
        session: Arc<SessionState>,
    ) -> Self {
        Self::with_session_opt(clock, controller, Some(session))
    }

    pub fn with_session_opt(
        clock: Arc<dyn ClockSource>,
        controller: Arc<dyn MediaController>,
        session: Option<Arc<SessionState>>,
    ) -> Self {
        let tracer = TimelineTracer::new(Arc::clone(&clock));
        Self {
            clock,
            controller,
            tracer,
            session,
        }
    }

    pub fn set_session(&mut self, session: Arc<SessionState>) {
        self.session = Some(session);
    }

    pub async fn execute_intent(&self, intent: &PlaybackIntent) -> Result<()> {
        // 1. Follower Autonomous Self-Filtering: only if target track is explicitly specified
        // and intent action is Play
        if let Some(target_title) = &intent.track_title
            && matches!(intent.action, PlaybackAction::Play)
            && let Ok(local_state) = self.controller.get_playback_state().await
            && let Some(local_meta) = local_state.metadata
        {
            let target_lower = target_title.to_lowercase();
            let local_lower = local_meta.title.to_lowercase();
            if !local_lower.is_empty()
                && !target_lower.is_empty()
                && !local_lower.contains(&target_lower)
                && !target_lower.contains(&local_lower)
            {
                tracing::info!(
                    local_track = %local_meta.title,
                    target_track = %target_title,
                    "Skipping playback intent: active track mismatch"
                );
                return Ok(());
            }
        }

        // 2. Scheduled Trigger with Pre-Dispatch Actuation Compensation
        let now = self.clock.reference_now();
        let actuation_delay_us = self.controller.estimated_actuation_delay_us();
        let fire_ref_time = intent.target_ref_time.saturating_sub(actuation_delay_us);

        let overshoot_us = if fire_ref_time > now {
            // Arrived ahead of time: wait until deadline using timeline tracer
            self.tracer.wait_until_deadline(fire_ref_time).await;
            0
        } else if now <= intent.target_ref_time {
            // Fire immediately: between fire_ref_time and target_ref_time (partial actuation buffer)
            0
        } else {
            // Overdue arrival / mid-track join: target_ref_time was in the past
            (now - intent.target_ref_time) as i64
        };

        match intent.action {
            PlaybackAction::Play => {
                let target_position = if overshoot_us > 0 {
                    intent.position_us + overshoot_us
                } else {
                    intent.position_us
                };

                let local_state = self.controller.get_playback_state().await.ok();
                let local_pos = local_state.as_ref().map(|s| s.position_us).unwrap_or(0);
                let delta = (local_pos - target_position).abs();

                // Decouple seek from resume: only seek if position is specified and delta is outside deadband (300ms)
                if target_position > 0 && delta > 300_000 {
                    self.controller.seek_to(target_position).await?;
                }
                self.controller.play().await?;

                if let Some(session) = &self.session {
                    session.expect_playback_state(Some(true), Some(target_position));
                }
            }
            PlaybackAction::Pause => {
                self.controller.pause().await?;

                if let Some(session) = &self.session {
                    session.expect_playback_state(Some(false), None);
                }
            }
            PlaybackAction::Seek { target_position_us } => {
                let final_target = if overshoot_us > 0 {
                    target_position_us + overshoot_us
                } else {
                    target_position_us
                };
                self.controller.seek_to(final_target).await?;

                if let Some(session) = &self.session {
                    session.expect_playback_state(None, Some(final_target));
                }
            }
            PlaybackAction::NextTrack => {
                self.controller.next_track().await?;
            }
            PlaybackAction::PreviousTrack => {
                self.controller.previous_track().await?;
            }
        }

        Ok(())
    }

    /// Executes a clock-aligned track transition using TimelineTracer.
    /// Follower computes delta against target_ref_time and dispatch_ref_time to determine:
    /// - Ahead: pre-dispatches at (target_ref_time - actuation_delay) via TimelineTracer deadline wait.
    /// - Behind/Late: loads immediately and micro-seeks to overshoot offset.
    pub async fn execute_track_transition(
        &self,
        track: &crate::protocol::messages::TrackIdentity,
        target_ref_time: u64,
        _dispatch_ref_time: u64,
    ) -> Result<()> {
        let now = self.clock.reference_now();
        let actuation_delay_us = self.controller.estimated_actuation_delay_us();
        let fire_ref_time = target_ref_time.saturating_sub(actuation_delay_us);

        // Check if follower is already playing this exact track
        let already_playing = if let Ok(state) = self.controller.get_playback_state().await
            && let Some(meta) = state.metadata
        {
            let target_title = track.title.to_lowercase();
            let current_title = meta.title.to_lowercase();
            !target_title.is_empty()
                && !current_title.is_empty()
                && (current_title.contains(&target_title) || target_title.contains(&current_title))
        } else {
            false
        };

        if already_playing {
            // Already playing this song: only reconcile/seek timeline position
            if now > target_ref_time {
                let overshoot_us = (now - target_ref_time) as i64;
                self.controller.seek_to(overshoot_us).await?;
            } else {
                // If scheduled in the future, wait until deadline to align to track start
                if fire_ref_time > now {
                    self.tracer.wait_until_deadline(fire_ref_time).await;
                }
                self.controller.seek_to(0).await?;
            }
            return Ok(());
        }

        if fire_ref_time > now {
            // Ahead of deadline: wait until deadline using synced PTP clock
            self.tracer.wait_until_deadline(fire_ref_time).await;
            self.controller.load_track(track).await?;
        } else if now <= target_ref_time {
            // Right on time / within actuation window: fire immediately
            self.controller.load_track(track).await?;
        } else {
            // Late / overshoot: load immediately and compensate position
            let overshoot_us = (now - target_ref_time) as i64;
            self.controller.load_track(track).await?;
            if overshoot_us > 100_000 {
                // Seek to compensated track position if overshoot exceeds 100ms
                self.controller.seek_to(overshoot_us).await?;
            }
        }

        Ok(())
    }
}
