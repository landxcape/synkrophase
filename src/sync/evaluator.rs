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
    last_anchor: Mutex<Option<SyncAnchor>>,
    drift_history: Mutex<VecDeque<i64>>,
    last_zone: Mutex<u8>,
    consecutive_outliers: Mutex<u32>,
    buffering_until: Mutex<Option<std::time::Instant>>,
    current_adjusted_rate: Mutex<Option<f32>>,
    zone2_started_at: Mutex<Option<std::time::Instant>>,
    nudge_started_at: Mutex<Option<std::time::Instant>>,
    seek_priming_lead_us: Mutex<i64>,
    pending_seek_calibration: Mutex<bool>,
}

pub fn is_timeline_discontinuity(prev: &SyncAnchor, curr: &SyncAnchor) -> bool {
    if prev.track_title != curr.track_title {
        return true;
    }
    if prev.is_playing != curr.is_playing {
        return true;
    }
    if (prev.playback_rate - curr.playback_rate).abs() > 0.01 {
        return true;
    }
    if curr.reference_time < prev.reference_time {
        return true;
    }
    let dt_us = curr.reference_time.saturating_sub(prev.reference_time);
    let expected_pos_us = if prev.is_playing {
        prev.media_position_us + (dt_us as f64 * prev.playback_rate as f64) as i64
    } else {
        prev.media_position_us
    };
    (curr.media_position_us - expected_pos_us).abs() > 500_000
}

impl DriftEvaluator {
    pub fn new(
        clock: Arc<dyn ClockSource>,
        controller: Arc<dyn MediaController>,
        threshold_us: u64,
    ) -> Self {
        let default_lead = controller.default_seek_priming_lead_us() as i64;
        Self {
            clock,
            controller,
            threshold_us,
            notify_event: Arc::new(Notify::new()),
            last_seek: Mutex::new(None),
            last_anchor: Mutex::new(None),
            drift_history: Mutex::new(VecDeque::with_capacity(5)),
            last_zone: Mutex::new(1),
            consecutive_outliers: Mutex::new(0),
            buffering_until: Mutex::new(None),
            current_adjusted_rate: Mutex::new(None),
            zone2_started_at: Mutex::new(None),
            nudge_started_at: Mutex::new(None),
            seek_priming_lead_us: Mutex::new(default_lead),
            pending_seek_calibration: Mutex::new(false),
        }
    }

    pub fn enter_buffering(&self, duration: Duration) {
        let deadline = std::time::Instant::now() + duration;
        *self.buffering_until.lock().unwrap() = Some(deadline);
        *self.last_zone.lock().unwrap() = 1;
        *self.zone2_started_at.lock().unwrap() = None;
        *self.nudge_started_at.lock().unwrap() = None;
        *self.pending_seek_calibration.lock().unwrap() = false;
        *self.consecutive_outliers.lock().unwrap() = 0;
        self.drift_history.lock().unwrap().clear();
        self.trigger_immediate();
    }

    pub fn notify_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.notify_event)
    }

    pub fn record_seek(&self, anchor: Option<SyncAnchor>) {
        *self.last_seek.lock().unwrap() = Some(std::time::Instant::now());
        self.drift_history.lock().unwrap().clear();
        *self.last_zone.lock().unwrap() = 1;
        *self.zone2_started_at.lock().unwrap() = None;
        *self.nudge_started_at.lock().unwrap() = None;
        *self.pending_seek_calibration.lock().unwrap() = true;
        *self.consecutive_outliers.lock().unwrap() = 0;
        if let Some(a) = anchor {
            *self.last_anchor.lock().unwrap() = Some(a);
        }
    }

    #[doc(hidden)]
    pub fn set_zone2_started_at(&self, instant: Option<std::time::Instant>) {
        *self.zone2_started_at.lock().unwrap() = instant;
    }

    #[doc(hidden)]
    pub fn set_nudge_started_at(&self, instant: Option<std::time::Instant>) {
        *self.nudge_started_at.lock().unwrap() = instant;
    }

    #[doc(hidden)]
    pub fn set_last_seek(&self, instant: Option<std::time::Instant>) {
        *self.last_seek.lock().unwrap() = instant;
    }

    #[doc(hidden)]
    pub fn seek_priming_lead_us(&self) -> i64 {
        *self.seek_priming_lead_us.lock().unwrap()
    }

    #[doc(hidden)]
    pub fn set_seek_priming_lead_us(&self, lead_us: i64) {
        *self.seek_priming_lead_us.lock().unwrap() = lead_us;
    }

    #[doc(hidden)]
    pub fn set_pending_seek_calibration(&self, pending: bool) {
        *self.pending_seek_calibration.lock().unwrap() = pending;
    }

    pub fn trigger_immediate(&self) {
        self.notify_event.notify_one();
    }

    fn push_and_median(&self, raw_drift: i64) -> i64 {
        let mut history = self.drift_history.lock().unwrap();
        if history.len() >= 5 {
            history.pop_front();
        }
        history.push_back(raw_drift);

        let mut sorted: Vec<i64> = history.iter().copied().collect();
        sorted.sort_unstable();
        sorted[sorted.len() / 2]
    }

    /// Evaluates the target drift zone with hysteresis and adaptive network jitter tolerance.
    /// Zone 1: Locked (0 - 80ms deadband)
    /// Zone 2: Monitoring / Nudging (80ms - adaptive Zone 3 entry threshold)
    /// Zone 3: Micro-seeking / Re-aligning (exceeding adaptive threshold)
    pub fn compute_zone(&self, drift_abs: u64) -> u8 {
        let mut current_zone = self.last_zone.lock().unwrap();
        let exit_zone1_threshold = 80_000u64.max(self.threshold_us);

        // Base Zone 3 entry threshold is 200ms. If network has high RTT or jitter,
        // expand Zone 3 threshold up to 350ms so rate-nudging absorbs transient network jitter
        // without causing prolonged ~40s drift recovery periods.
        let base_zone3 = 200_000u64;
        let adaptive_zone3 = if let Some(stats) = self.clock.network_stats() {
            let network_padding = (stats.srtt_us / 2).saturating_add(stats.rttvar_us * 2);
            base_zone3.max(network_padding).min(350_000)
        } else {
            base_zone3
        };
        let adaptive_zone3_hysteresis = adaptive_zone3 + 25_000;

        let zone = match *current_zone {
            1 => {
                if drift_abs <= exit_zone1_threshold {
                    1
                } else if drift_abs < adaptive_zone3 {
                    2
                } else {
                    3
                }
            }
            2 => {
                if drift_abs <= exit_zone1_threshold {
                    1
                } else if drift_abs < adaptive_zone3_hysteresis {
                    2
                } else {
                    3
                }
            }
            _ => {
                if drift_abs <= exit_zone1_threshold {
                    1
                } else if drift_abs < adaptive_zone3 {
                    2
                } else {
                    3
                }
            }
        };
        *current_zone = zone;
        zone
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

        // Settling grace period check (2500ms post-seek)
        if let Some(prev_seek) = *self.last_seek.lock().unwrap()
            && now.duration_since(prev_seek) < Duration::from_millis(2500)
        {
            return Ok((0, 1, "Settling (2.5s)".to_string()));
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
        let smoothed_drift_us = self.push_and_median(raw_drift_us);
        let drift_abs = smoothed_drift_us.unsigned_abs();
        let zone = self.compute_zone(drift_abs);

        match zone {
            1 => Ok((smoothed_drift_us, 1, "Locked (<80ms)".to_string())),
            2 => {
                if self.controller.supports_rate_control() {
                    Ok((
                        smoothed_drift_us,
                        2,
                        format!("Nudging ({:+0.1}ms)", (smoothed_drift_us as f64) / 1000.0),
                    ))
                } else {
                    Ok((
                        smoothed_drift_us,
                        2,
                        format!(
                            "Monitoring ({:+0.1}ms)",
                            (smoothed_drift_us as f64) / 1000.0
                        ),
                    ))
                }
            }
            _ => Ok((
                smoothed_drift_us,
                3,
                format!(
                    "Re-aligning ({:+0.1}ms)",
                    (smoothed_drift_us as f64) / 1000.0
                ),
            )),
        }
    }

    pub async fn evaluate_and_reconcile(&self, anchor: &SyncAnchor) -> Result<DriftAction> {
        let t_start = self.clock.reference_now();
        let mut local_state = self.controller.get_playback_state().await?;
        let t_end = self.clock.reference_now();

        if !anchor.is_playing {
            // Leader is paused: ensure local follower player is paused
            if local_state.is_playing {
                tracing::info!("Leader is paused; pausing local follower playback");
                let _ = self.controller.pause().await;
            }

            // If paused position differs significantly (>200ms), align follower position
            let pos_diff = (local_state.position_us - anchor.media_position_us).abs();
            if pos_diff > 200_000 {
                tracing::info!(
                    diff_ms = pos_diff / 1000,
                    target_pos_ms = anchor.media_position_us / 1000,
                    "Aligning paused follower timeline position"
                );
                let _ = self.controller.seek_to(anchor.media_position_us).await;
            }

            *self.last_zone.lock().unwrap() = 1;
            *self.consecutive_outliers.lock().unwrap() = 0;
            *self.zone2_started_at.lock().unwrap() = None;
            *self.nudge_started_at.lock().unwrap() = None;
            return Ok(DriftAction::InSync { drift_us: 0 });
        }

        // Leader is playing: ensure local follower player is playing
        if !local_state.is_playing {
            tracing::info!("Leader is playing; resuming local follower playback");
            let _ = self.controller.play().await;
            local_state.is_playing = true;
        }

        // Buffering grace period check
        let now = std::time::Instant::now();
        if let Some(buf_until) = *self.buffering_until.lock().unwrap() {
            if now < buf_until {
                return Ok(DriftAction::InSync { drift_us: 0 });
            } else {
                *self.buffering_until.lock().unwrap() = None;
            }
        }

        let is_discontinuity = {
            let mut last_guard = self.last_anchor.lock().unwrap();
            match &*last_guard {
                Some(prev) => {
                    let disc = is_timeline_discontinuity(prev, anchor);
                    *last_guard = Some(anchor.clone());
                    disc
                }
                None => {
                    *last_guard = Some(anchor.clone());
                    true
                }
            }
        };

        // Settling grace period check (2500ms post-seek)
        if !is_discontinuity {
            let last_seek_guard = self.last_seek.lock().unwrap();
            if let Some(prev_seek) = *last_seek_guard
                && now.duration_since(prev_seek) < Duration::from_millis(2500)
            {
                return Ok(DriftAction::InSync { drift_us: 0 });
            }
        }

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

        if is_discontinuity {
            self.drift_history.lock().unwrap().clear();
            *self.pending_seek_calibration.lock().unwrap() = false;
        }

        let raw_drift_us = local_state.position_us - expected_position_us;
        let smoothed_drift_us = self.push_and_median(raw_drift_us);
        let drift_abs = smoothed_drift_us.unsigned_abs();
        let zone = self.compute_zone(drift_abs);

        // Auto-calibrate seek priming lead on the first stable sample after post-seek settling
        if !is_discontinuity && *self.pending_seek_calibration.lock().unwrap() {
            let mut lead_guard = self.seek_priming_lead_us.lock().unwrap();
            let prev_lead = *lead_guard;
            let delta = -smoothed_drift_us;
            let updated_lead = (prev_lead + delta).clamp(0, 350_000);
            *lead_guard = updated_lead;
            *self.pending_seek_calibration.lock().unwrap() = false;
            tracing::info!(
                residual_drift_ms = (smoothed_drift_us as f64) / 1000.0,
                prev_lead_ms = (prev_lead as f64) / 1000.0,
                new_lead_ms = (updated_lead as f64) / 1000.0,
                "Calibrated seek priming lead compensation"
            );
        }

        // Zone 1: Locked / InSync
        if zone == 1 {
            *self.consecutive_outliers.lock().unwrap() = 0;
            *self.zone2_started_at.lock().unwrap() = None;
            *self.nudge_started_at.lock().unwrap() = None;
            let needs_restore = self.current_adjusted_rate.lock().unwrap().is_some();
            if needs_restore {
                if let Ok(()) = self.controller.set_rate(1.0).await {
                    *self.current_adjusted_rate.lock().unwrap() = None;
                } else {
                    tracing::warn!(
                        "Failed to restore playback rate to 1.0; will retry on next evaluation"
                    );
                }
            }
            return Ok(DriftAction::InSync {
                drift_us: smoothed_drift_us,
            });
        }

        let mut is_persistent_zone2 = false;
        let mut is_nudge_timeout = false;

        // Zone 2: Monitoring or Time-Bounded Rate Nudging
        if zone == 2 {
            if self.controller.supports_rate_control() {
                let target_rate = if smoothed_drift_us > 0 { 0.99 } else { 1.01 };
                if let Ok(()) = self.controller.set_rate(target_rate).await {
                    let state_after = self.controller.get_playback_state().await?;
                    if (state_after.rate - target_rate).abs() < f32::EPSILON {
                        let (is_timeout, max_nudge_secs) = {
                            let mut nudge_start = self.nudge_started_at.lock().unwrap();
                            let started = *nudge_start.get_or_insert_with(|| now);
                            // Deadline: expected convergence time at 1% rate delta (10ms/s) plus 2s buffer
                            let max_nudge_secs = (drift_abs as f64 / 10_000.0) + 2.0;
                            let max_nudge_duration = Duration::from_secs_f64(max_nudge_secs);
                            if now.duration_since(started) > max_nudge_duration {
                                *nudge_start = None;
                                (true, max_nudge_secs)
                            } else {
                                (false, max_nudge_secs)
                            }
                        };

                        if is_timeout {
                            tracing::warn!(
                                "Rate nudging failed to converge within deadline ({max_nudge_secs:.1}s); escalating to seek"
                            );
                            let _ = self.controller.set_rate(1.0).await;
                            *self.current_adjusted_rate.lock().unwrap() = None;
                            is_nudge_timeout = true;
                        } else {
                            *self.consecutive_outliers.lock().unwrap() = 0;
                            *self.current_adjusted_rate.lock().unwrap() = Some(target_rate);
                            return Ok(DriftAction::FineTuneRate {
                                rate: target_rate,
                                drift_us: smoothed_drift_us,
                            });
                        }
                    } else {
                        tracing::warn!(
                            "Controller reported rate support but rate change to {target_rate} failed confirmation; falling through to persistence gate"
                        );
                    }
                }
            }

            // Fixed-rate players (Spotify, Apple Music, or unsupported/unconfirmed rate)
            if !is_nudge_timeout {
                *self.nudge_started_at.lock().unwrap() = None;
                if !is_discontinuity {
                    let mut z2_start = self.zone2_started_at.lock().unwrap();
                    let started = *z2_start.get_or_insert_with(|| now);
                    if now.duration_since(started) < Duration::from_millis(3000) {
                        return Ok(DriftAction::InSync {
                            drift_us: smoothed_drift_us,
                        });
                    }
                    // Persisted for >= 3.0s: escalate to seek!
                    *z2_start = None;
                    is_persistent_zone2 = true;
                }
            }
        } else {
            // Zone 3: clear Zone 2 persistence timers
            *self.zone2_started_at.lock().unwrap() = None;
            *self.nudge_started_at.lock().unwrap() = None;
        }

        // Zone 3 Micro-seek with cooldown & consecutive outlier confirmation
        let should_seek = {
            let mut outliers = self.consecutive_outliers.lock().unwrap();
            *outliers = outliers.saturating_add(1);
            let confirmed =
                is_discontinuity || is_persistent_zone2 || is_nudge_timeout || *outliers >= 2;

            let mut last = self.last_seek.lock().unwrap();
            if is_discontinuity {
                *last = Some(now);
                *outliers = 0;
                true
            } else if confirmed {
                match *last {
                    Some(prev) if now.duration_since(prev) < Duration::from_millis(2500) => false,
                    _ => {
                        *last = Some(now);
                        *outliers = 0;
                        true
                    }
                }
            } else {
                false
            }
        };

        let actuation_delay_us = self.controller.estimated_actuation_delay_us() as i64;
        let priming_lead_us = *self.seek_priming_lead_us.lock().unwrap();
        let target_seek_position_us = expected_position_us + actuation_delay_us + priming_lead_us;

        if should_seek {
            self.controller.seek_to(target_seek_position_us).await?;
            *self.pending_seek_calibration.lock().unwrap() = true;
            let needs_restore = self.current_adjusted_rate.lock().unwrap().is_some();
            if needs_restore {
                if let Ok(()) = self.controller.set_rate(1.0).await {
                    *self.current_adjusted_rate.lock().unwrap() = None;
                } else {
                    tracing::warn!(
                        "Failed to restore playback rate to 1.0 during seek; will retry on next evaluation"
                    );
                }
            }
            self.drift_history.lock().unwrap().clear();
            *self.last_zone.lock().unwrap() = 1;
            *self.zone2_started_at.lock().unwrap() = None;
            *self.nudge_started_at.lock().unwrap() = None;
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
