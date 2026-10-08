use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use crate::controller::MediaController;
use crate::error::Result;
use crate::protocol::messages::{PlaybackAction, PlaybackIntent};
use crate::sync::controller::ClockSource;

pub struct IntentScheduler {
    clock: Arc<dyn ClockSource>,
    controller: Arc<dyn MediaController>,
}

impl IntentScheduler {
    pub fn new(clock: Arc<dyn ClockSource>, controller: Arc<dyn MediaController>) -> Self {
        Self { clock, controller }
    }

    pub async fn execute_intent(&self, intent: &PlaybackIntent) -> Result<()> {
        // 1. Follower Autonomous Self-Filtering
        if let Some(target_title) = &intent.track_title {
            let local_state = self.controller.get_playback_state().await?;
            if let Some(local_meta) = local_state.metadata {
                let target_lower = target_title.to_lowercase();
                let local_lower = local_meta.title.to_lowercase();
                if !local_lower.is_empty()
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
        }

        // 2. Scheduled Trigger / Exact Value Skip
        let now = self.clock.reference_now();

        if intent.target_ref_time > now {
            // Arrived ahead of time: high-precision sleep until target time
            let wait_us = intent.target_ref_time - now;
            sleep(Duration::from_micros(wait_us)).await;

            match intent.action {
                PlaybackAction::Play => {
                    self.controller.seek_to(intent.position_us).await?;
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
            // Overdue arrival / mid-track join: compute exact value skip
            let overshoot_us = (now - intent.target_ref_time) as i64;

            match intent.action {
                PlaybackAction::Play => {
                    let compensated_position_us = intent.position_us + overshoot_us;
                    self.controller.seek_to(compensated_position_us).await?;
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
