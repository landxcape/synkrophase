use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};
use tokio::time::sleep;

use crate::controller::MediaController;
use crate::error::Result;
use crate::protocol::messages::SyncAnchor;
use crate::sync::controller::ClockSource;

#[derive(Debug, Clone, PartialEq)]
pub enum DriftAction {
    InSync { drift_us: i64 },
    FineTuneRate { rate: f32, drift_us: i64 },
    MicroSeek { target_position_us: i64, drift_us: i64 },
}

pub struct DriftEvaluator {
    clock: Arc<dyn ClockSource>,
    controller: Arc<dyn MediaController>,
    threshold_us: u64,
    notify_event: Arc<Notify>,
}

impl DriftEvaluator {
    pub fn new(
        clock: Arc<dyn ClockSource>,
        controller: Arc<dyn MediaController>,
        threshold_us: u64,
    ) -> Self {
        Self {
            clock,
            controller,
            threshold_us,
            notify_event: Arc::new(Notify::new()),
        }
    }

    pub fn notify_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.notify_event)
    }

    pub fn trigger_immediate(&self) {
        self.notify_event.notify_one();
    }

    pub async fn evaluate_and_reconcile(&self, anchor: &SyncAnchor) -> Result<DriftAction> {
        if !anchor.is_playing {
            return Ok(DriftAction::InSync { drift_us: 0 });
        }

        let now = self.clock.reference_now();
        let elapsed_us = (now.saturating_sub(anchor.reference_time) as f64 * anchor.playback_rate as f64) as i64;
        let expected_position_us = anchor.media_position_us + elapsed_us;

        let local_state = self.controller.get_playback_state().await?;
        let drift_us = local_state.position_us - expected_position_us;
        let drift_abs = drift_us.unsigned_abs();

        if drift_abs <= self.threshold_us {
            return Ok(DriftAction::InSync { drift_us });
        }

        // Two-stage correction:
        // Stage 1: Fine-tune rate if drift is moderate (50ms - 200ms)
        if drift_abs < 200_000 {
            let target_rate = if drift_us > 0 { 0.99 } else { 1.01 };
            if let Ok(()) = self.controller.set_rate(target_rate).await {
                // If player actually changed rate, return FineTuneRate
                let state_after = self.controller.get_playback_state().await?;
                if (state_after.rate - target_rate).abs() < f32::EPSILON {
                    return Ok(DriftAction::FineTuneRate { rate: target_rate, drift_us });
                }
            }
        }

        // Stage 2: Snap to target via micro-seek
        self.controller.seek_to(expected_position_us).await?;
        Ok(DriftAction::MicroSeek {
            target_position_us: expected_position_us,
            drift_us,
        })
    }

    pub async fn run_loop(
        &self,
        anchor_source: Arc<RwLock<Option<SyncAnchor>>>,
        interval_duration: Duration,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = sleep(interval_duration) => {
                    if let Some(anchor) = anchor_source.read().await.clone() {
                        let _ = self.evaluate_and_reconcile(&anchor).await;
                    }
                }
                _ = self.notify_event.notified() => {
                    if let Some(anchor) = anchor_source.read().await.clone() {
                        let _ = self.evaluate_and_reconcile(&anchor).await;
                    }
                }
            }
        }
    }
}
