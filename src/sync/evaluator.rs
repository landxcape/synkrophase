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
    InSync {
        drift_us: i64,
    },
    FineTuneRate {
        rate: f32,
        drift_us: i64,
    },
    MicroSeek {
        target_position_us: i64,
        drift_us: i64,
    },
}

pub struct DriftEvaluator {
    clock: Arc<dyn ClockSource>,
    controller: Arc<dyn MediaController>,
    threshold_us: u64,
    notify_event: Arc<Notify>,
    last_seek: std::sync::Mutex<Option<std::time::Instant>>,
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
            last_seek: std::sync::Mutex::new(None),
        }
    }

    pub fn notify_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.notify_event)
    }

    pub fn trigger_immediate(&self) {
        self.notify_event.notify_one();
    }

    pub async fn evaluate_drift(&self, anchor: &SyncAnchor) -> Result<(i64, u8, String)> {
        if !anchor.is_playing {
            return Ok((0, 1, "Paused".to_string()));
        }

        let t_start = self.clock.reference_now();
        let local_state = self.controller.get_playback_state().await?;
        let t_end = self.clock.reference_now();
        let query_midpoint = t_start + (t_end.saturating_sub(t_start)) / 2;

        let elapsed_us = (query_midpoint.saturating_sub(anchor.reference_time) as f64
            * anchor.playback_rate as f64) as i64;
        let expected_position_us = anchor.media_position_us + elapsed_us;

        let drift_us = local_state.position_us - expected_position_us;
        let drift_abs = drift_us.unsigned_abs();

        if drift_abs <= self.threshold_us {
            Ok((drift_us, 1, "Locked (<50ms)".to_string()))
        } else if drift_abs < 200_000 {
            Ok((
                drift_us,
                2,
                format!("Nudging ({:+0.1}ms)", (drift_us as f64) / 1000.0),
            ))
        } else {
            Ok((
                drift_us,
                3,
                format!("Drifting ({:+0.1}ms)", (drift_us as f64) / 1000.0),
            ))
        }
    }

    pub async fn evaluate_and_reconcile(&self, anchor: &SyncAnchor) -> Result<DriftAction> {
        if !anchor.is_playing {
            return Ok(DriftAction::InSync { drift_us: 0 });
        }

        let t_start = self.clock.reference_now();
        let local_state = self.controller.get_playback_state().await?;
        let t_end = self.clock.reference_now();
        // Midpoint of local query represents the true moment position was queried
        let query_midpoint = t_start + (t_end.saturating_sub(t_start)) / 2;

        let elapsed_us = (query_midpoint.saturating_sub(anchor.reference_time) as f64
            * anchor.playback_rate as f64) as i64;
        let expected_position_us = anchor.media_position_us + elapsed_us;

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
                    return Ok(DriftAction::FineTuneRate {
                        rate: target_rate,
                        drift_us,
                    });
                }
            }
        }

        // Stage 2: Micro-seek with cooldown to prevent seek thrashing & buffer jitter
        let now = std::time::Instant::now();
        let should_seek = {
            let mut last = self.last_seek.lock().unwrap();
            match *last {
                Some(prev) if now.duration_since(prev) < Duration::from_millis(2500) => false,
                _ => {
                    *last = Some(now);
                    true
                }
            }
        };

        if should_seek {
            self.controller.seek_to(expected_position_us).await?;
        }

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
