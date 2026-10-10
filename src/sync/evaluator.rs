use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
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
    last_seek: Mutex<Option<std::time::Instant>>,
    last_anchor_ref: Mutex<Option<u64>>,
    drift_history: Mutex<VecDeque<i64>>,
    last_zone: Mutex<u8>,
    consecutive_outliers: Mutex<u32>,
    buffering_until: Mutex<Option<std::time::Instant>>,
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
            last_seek: Mutex::new(None),
            last_anchor_ref: Mutex::new(None),
            drift_history: Mutex::new(VecDeque::with_capacity(5)),
            last_zone: Mutex::new(1),
            consecutive_outliers: Mutex::new(0),
            buffering_until: Mutex::new(None),
        }
    }

    pub fn enter_buffering(&self, duration: Duration) {
        let deadline = std::time::Instant::now() + duration;
        *self.buffering_until.lock().unwrap() = Some(deadline);
        *self.last_zone.lock().unwrap() = 1;
        self.trigger_immediate();
    }

    pub fn notify_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.notify_event)
    }

    pub fn trigger_immediate(&self) {
        self.notify_event.notify_one();
    }

    fn push_and_median_drift(&self, raw_drift: i64) -> i64 {
        let mut history = self.drift_history.lock().unwrap();
        if history.len() >= 5 {
            history.pop_front();
        }
        history.push_back(raw_drift);

        let mut sorted: Vec<i64> = history.iter().copied().collect();
        sorted.sort_unstable();
        sorted[sorted.len() / 2]
    }

    pub async fn evaluate_drift(&self, anchor: &SyncAnchor) -> Result<(i64, u8, String)> {
        if !anchor.is_playing {
            *self.last_zone.lock().unwrap() = 1;
            return Ok((0, 1, "Paused".to_string()));
        }

        let now = std::time::Instant::now();
        if let Some(buf_until) = *self.buffering_until.lock().unwrap() {
            if now < buf_until {
                return Ok((0, 1, "Buffering...".to_string()));
            } else {
                *self.buffering_until.lock().unwrap() = None;
            }
        }

        // Settling grace period check (1500ms post-seek)
        if let Some(prev_seek) = *self.last_seek.lock().unwrap()
            && now.duration_since(prev_seek) < Duration::from_millis(1500)
        {
            return Ok((0, 1, "Settling...".to_string()));
        }

        let t_start = self.clock.reference_now();
        let local_state = self.controller.get_playback_state().await?;
        let t_end = self.clock.reference_now();

        // Track Discontinuity Guard: check if follower is playing a different song
        if let (Some(anchor_title), Some(local_meta)) = (&anchor.track_title, &local_state.metadata)
        {
            let a_lower = anchor_title.to_lowercase();
            let l_lower = local_meta.title.to_lowercase();
            if !l_lower.is_empty()
                && !a_lower.is_empty()
                && !l_lower.contains(&a_lower)
                && !a_lower.contains(&l_lower)
            {
                return Ok((0, 1, format!("Different Track ({})", local_meta.title)));
            }
        }

        let query_midpoint = t_start + (t_end.saturating_sub(t_start)) / 2;

        let elapsed_us = (query_midpoint.saturating_sub(anchor.reference_time) as f64
            * anchor.playback_rate as f64) as i64;
        let expected_position_us = anchor.media_position_us + elapsed_us;

        let raw_drift_us = local_state.position_us - expected_position_us;
        let smoothed_drift_us = self.push_and_median_drift(raw_drift_us);
        let drift_abs = smoothed_drift_us.unsigned_abs();

        let mut current_zone = self.last_zone.lock().unwrap();
        // Hysteresis Banding:
        // Zone 1 (Locked): threshold_us (typically 50ms) with +25ms release margin
        let exit_zone1_threshold = self.threshold_us + 25_000;
        let zone = match *current_zone {
            1 => {
                if drift_abs <= exit_zone1_threshold {
                    1
                } else if drift_abs < 200_000 {
                    2
                } else {
                    3
                }
            }
            2 => {
                if drift_abs <= self.threshold_us {
                    1
                } else if drift_abs < 225_000 {
                    2
                } else {
                    3
                }
            }
            _ => {
                if drift_abs <= self.threshold_us {
                    1
                } else if drift_abs < 200_000 {
                    2
                } else {
                    3
                }
            }
        };
        *current_zone = zone;

        match zone {
            1 => Ok((smoothed_drift_us, 1, "Locked (<50ms)".to_string())),
            2 => Ok((
                smoothed_drift_us,
                2,
                format!("Nudging ({:+0.1}ms)", (smoothed_drift_us as f64) / 1000.0),
            )),
            _ => Ok((
                smoothed_drift_us,
                3,
                format!("Drifting ({:+0.1}ms)", (smoothed_drift_us as f64) / 1000.0),
            )),
        }
    }

    pub async fn evaluate_and_reconcile(&self, anchor: &SyncAnchor) -> Result<DriftAction> {
        if !anchor.is_playing {
            return Ok(DriftAction::InSync { drift_us: 0 });
        }

        // Settling grace period check (1500ms post-seek)
        let now = std::time::Instant::now();
        {
            let last_seek_guard = self.last_seek.lock().unwrap();
            let last_ref_guard = self.last_anchor_ref.lock().unwrap();
            let is_new_anchor = match *last_ref_guard {
                Some(prev_ref) => prev_ref != anchor.reference_time,
                None => true,
            };
            if !is_new_anchor
                && let Some(prev_seek) = *last_seek_guard
                && now.duration_since(prev_seek) < Duration::from_millis(1500)
            {
                return Ok(DriftAction::InSync { drift_us: 0 });
            }
        }

        let t_start = self.clock.reference_now();
        let local_state = self.controller.get_playback_state().await?;
        let t_end = self.clock.reference_now();

        // Track Discontinuity Guard: do not seek or reconcile if playing a different song
        if let (Some(anchor_title), Some(local_meta)) = (&anchor.track_title, &local_state.metadata)
        {
            let a_lower = anchor_title.to_lowercase();
            let l_lower = local_meta.title.to_lowercase();
            if !l_lower.is_empty()
                && !a_lower.is_empty()
                && !l_lower.contains(&a_lower)
                && !a_lower.contains(&l_lower)
            {
                return Ok(DriftAction::InSync { drift_us: 0 });
            }
        }

        // Midpoint of local query represents the true moment position was queried
        let query_midpoint = t_start + (t_end.saturating_sub(t_start)) / 2;

        let elapsed_us = (query_midpoint.saturating_sub(anchor.reference_time) as f64
            * anchor.playback_rate as f64) as i64;
        let expected_position_us = anchor.media_position_us + elapsed_us;

        let raw_drift_us = local_state.position_us - expected_position_us;
        let smoothed_drift_us = self.push_and_median_drift(raw_drift_us);
        let drift_abs = smoothed_drift_us.unsigned_abs();

        if drift_abs <= self.threshold_us {
            *self.consecutive_outliers.lock().unwrap() = 0;
            return Ok(DriftAction::InSync {
                drift_us: smoothed_drift_us,
            });
        }

        // Two-stage correction:
        // Stage 1: Fine-tune rate if drift is moderate (50ms - 200ms)
        if drift_abs < 200_000 {
            let target_rate = if smoothed_drift_us > 0 { 0.99 } else { 1.01 };
            if let Ok(()) = self.controller.set_rate(target_rate).await {
                // If player actually changed rate, return FineTuneRate
                let state_after = self.controller.get_playback_state().await?;
                if (state_after.rate - target_rate).abs() < f32::EPSILON {
                    *self.consecutive_outliers.lock().unwrap() = 0;
                    return Ok(DriftAction::FineTuneRate {
                        rate: target_rate,
                        drift_us: smoothed_drift_us,
                    });
                }
            }
        }

        // Stage 2: Micro-seek with cooldown & consecutive outlier confirmation
        let (should_seek, is_new_anchor) = {
            let mut last_ref = self.last_anchor_ref.lock().unwrap();
            let is_new = match *last_ref {
                Some(prev_ref) => prev_ref != anchor.reference_time,
                None => true,
            };

            let mut outliers = self.consecutive_outliers.lock().unwrap();
            *outliers = outliers.saturating_add(1);
            let confirmed = is_new || *outliers >= 2;

            let mut last = self.last_seek.lock().unwrap();
            if is_new {
                *last_ref = Some(anchor.reference_time);
                *last = Some(now);
                *outliers = 0;
                (true, true)
            } else if confirmed {
                match *last {
                    Some(prev) if now.duration_since(prev) < Duration::from_millis(1500) => {
                        (false, false)
                    }
                    _ => {
                        *last = Some(now);
                        *outliers = 0;
                        (true, false)
                    }
                }
            } else {
                (false, false)
            }
        };

        let actuation_delay_us = self.controller.estimated_actuation_delay_us() as i64;
        let target_seek_position_us = expected_position_us + actuation_delay_us;

        if should_seek {
            self.controller.seek_to(target_seek_position_us).await?;
            if is_new_anchor {
                self.drift_history.lock().unwrap().clear();
            }
        }

        Ok(DriftAction::MicroSeek {
            target_position_us: target_seek_position_us,
            drift_us: smoothed_drift_us,
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
