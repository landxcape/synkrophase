use std::sync::Arc;

use crate::controller::MediaController;
use crate::error::Result;
use crate::protocol::messages::{PlaybackAction, PlaybackIntent};
use crate::sync::controller::ClockSource;
use crate::sync::tracer::TimelineTracer;

pub struct IntentScheduler {
    clock: Arc<dyn ClockSource>,
    controller: Arc<dyn MediaController>,
    tracer: TimelineTracer,
}

impl IntentScheduler {
    pub fn new(clock: Arc<dyn ClockSource>, controller: Arc<dyn MediaController>) -> Self {
        let tracer = TimelineTracer::new(Arc::clone(&clock));
        Self {
            clock,
            controller,
            tracer,
        }
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

        if fire_ref_time > now {
            // Arrived ahead of time: wait until deadline using timeline tracer
            self.tracer.wait_until_deadline(fire_ref_time).await;

            match intent.action {
                PlaybackAction::Play => {
                    if intent.position_us > 0 {
                        self.controller.seek_to(intent.position_us).await?;
                    }
                    self.controller.play().await?;
                }
                PlaybackAction::Pause => {
                    self.controller.pause().await?;
                }
                PlaybackAction::Seek { target_position_us } => {
                    self.controller.seek_to(target_position_us).await?;
                }
                PlaybackAction::NextTrack => {
                    self.controller.next_track().await?;
                }
                PlaybackAction::PreviousTrack => {
                    self.controller.previous_track().await?;
                }
            }
        } else if now <= intent.target_ref_time {
            // Fire immediately: between fire_ref_time and target_ref_time (partial actuation buffer)
            match intent.action {
                PlaybackAction::Play => {
                    if intent.position_us > 0 {
                        self.controller.seek_to(intent.position_us).await?;
                    }
                    self.controller.play().await?;
                }
                PlaybackAction::Pause => {
                    self.controller.pause().await?;
                }
                PlaybackAction::Seek { target_position_us } => {
                    self.controller.seek_to(target_position_us).await?;
                }
                PlaybackAction::NextTrack => {
                    self.controller.next_track().await?;
                }
                PlaybackAction::PreviousTrack => {
                    self.controller.previous_track().await?;
                }
            }
        } else {
            // Overdue arrival / mid-track join: target_ref_time was in the past
            let overshoot_us = (now - intent.target_ref_time) as i64;

            match intent.action {
                PlaybackAction::Play => {
                    if intent.position_us > 0 {
                        let compensated_position_us = intent.position_us + overshoot_us;
                        self.controller.seek_to(compensated_position_us).await?;
                    }
                    self.controller.play().await?;
                }
                PlaybackAction::Pause => {
                    self.controller.pause().await?;
                }
                PlaybackAction::Seek { target_position_us } => {
                    let compensated_position_us = target_position_us + overshoot_us;
                    self.controller.seek_to(compensated_position_us).await?;
                }
                PlaybackAction::NextTrack => {
                    self.controller.next_track().await?;
                }
                PlaybackAction::PreviousTrack => {
                    self.controller.previous_track().await?;
                }
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
