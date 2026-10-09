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
}
