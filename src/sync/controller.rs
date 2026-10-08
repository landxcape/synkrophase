use std::sync::{Arc, RwLock};

use tokio::time::{Duration, sleep};

use crate::clock::sync::ClockSync;
use crate::config::SyncConfig;
use crate::error::Result;
use crate::protocol::messages::SyncAnchor;

pub trait ClockSource: Send + Sync {
    fn reference_now(&self) -> u64;
}

impl ClockSource for ClockSync {
    fn reference_now(&self) -> u64 {
        ClockSync::reference_now(self)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlaybackStatus {
    pub track_id: Option<String>,
    pub position_us: i64,
    pub rate: f32,
    pub is_playing: bool,
}

impl Default for PlaybackStatus {
    fn default() -> Self {
        Self {
            track_id: None,
            position_us: 0,
            rate: 1.0,
            is_playing: false,
        }
    }
}

pub trait PlaybackControl: Send + Sync {
    fn position(&self) -> i64;
    fn status(&self) -> PlaybackStatus;
    fn set_rate(&self, rate: f32) -> Result<()>;
    fn seek(&self, position_us: i64) -> Result<()>;
    fn pause(&self) -> Result<()> {
        Ok(())
    }
    fn resume(&self) -> Result<()> {
        Ok(())
    }
    fn stop(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct NoopPlaybackControl {
    status: std::sync::Mutex<PlaybackStatus>,
}

impl PlaybackControl for NoopPlaybackControl {
    fn position(&self) -> i64 {
        self.status.lock().unwrap().position_us
    }

    fn status(&self) -> PlaybackStatus {
        self.status.lock().unwrap().clone()
    }

    fn set_rate(&self, rate: f32) -> Result<()> {
        self.status.lock().unwrap().rate = rate;
        Ok(())
    }

    fn seek(&self, position_us: i64) -> Result<()> {
        self.status.lock().unwrap().position_us = position_us;
        Ok(())
    }
}

pub struct MediaControllerPlaybackAdapter {
    controller: Arc<dyn crate::controller::MediaController>,
    last_status: std::sync::Mutex<PlaybackStatus>,
}

impl MediaControllerPlaybackAdapter {
    pub fn new(controller: Arc<dyn crate::controller::MediaController>) -> Self {
        Self {
            controller,
            last_status: std::sync::Mutex::new(PlaybackStatus::default()),
        }
    }

    pub async fn poll_state(&self) -> Result<PlaybackStatus> {
        let state = self.controller.get_playback_state().await?;
        let status = PlaybackStatus {
            track_id: state.metadata.map(|m| m.title),
            position_us: state.position_us,
            rate: state.rate,
            is_playing: state.is_playing,
        };
        *self.last_status.lock().unwrap() = status.clone();
        Ok(status)
    }
}

impl PlaybackControl for MediaControllerPlaybackAdapter {
    fn position(&self) -> i64 {
        self.last_status.lock().unwrap().position_us
    }

    fn status(&self) -> PlaybackStatus {
        self.last_status.lock().unwrap().clone()
    }

    fn set_rate(&self, rate: f32) -> Result<()> {
        let controller = Arc::clone(&self.controller);
        tokio::spawn(async move {
            let _ = controller.set_rate(rate).await;
        });
        self.last_status.lock().unwrap().rate = rate;
        Ok(())
    }

    fn seek(&self, position_us: i64) -> Result<()> {
        let controller = Arc::clone(&self.controller);
        tokio::spawn(async move {
            let _ = controller.seek_to(position_us).await;
        });
        self.last_status.lock().unwrap().position_us = position_us;
        Ok(())
    }

    fn pause(&self) -> Result<()> {
        let controller = Arc::clone(&self.controller);
        tokio::spawn(async move {
            let _ = controller.pause().await;
        });
        self.last_status.lock().unwrap().is_playing = false;
        Ok(())
    }

    fn resume(&self) -> Result<()> {
        let controller = Arc::clone(&self.controller);
        tokio::spawn(async move {
            let _ = controller.play().await;
        });
        self.last_status.lock().unwrap().is_playing = true;
        Ok(())
    }
}

pub struct SyncController {
    clock: Arc<dyn ClockSource>,
    playback: Arc<dyn PlaybackControl>,
    config: SyncConfig,
    current_anchor: RwLock<Option<SyncAnchor>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SyncAction {
    InSync,
    AdjustRate { rate: f32 },
    RestoreRate,
    Seek { target_position_us: i64 },
}

impl SyncController {
    pub fn new(
        clock: Arc<dyn ClockSource>,
        playback: Arc<dyn PlaybackControl>,
        config: SyncConfig,
    ) -> Self {
        Self {
            clock,
            playback,
            config,
            current_anchor: RwLock::new(None),
        }
    }

    pub fn set_anchor(&self, anchor: SyncAnchor) {
        *self.current_anchor.write().unwrap() = Some(anchor);
    }

    pub fn evaluate(&self) -> SyncAction {
        let anchor = match self.current_anchor.read().unwrap().clone() {
            Some(anchor) => anchor,
            None => return SyncAction::InSync,
        };

        let now = self.clock.reference_now();
        let elapsed = now.saturating_sub(anchor.reference_time);
        let expected_us =
            anchor.media_position_us + (elapsed as f64 * anchor.playback_rate as f64) as i64;
        let actual_us = self.playback.position();
        let drift_us = actual_us - expected_us;
        let drift_abs = drift_us.unsigned_abs();
        let current_rate = self.playback.status().rate;

        if drift_abs < self.config.zone1_threshold_ms * 1_000 {
            if (current_rate - 1.0).abs() > f32::EPSILON {
                return SyncAction::RestoreRate;
            }
            return SyncAction::InSync;
        }

        if drift_abs < self.config.zone2_threshold_ms * 1_000 {
            let rate = if drift_us > 0 {
                self.config.rate_slow
            } else {
                self.config.rate_fast
            };
            return SyncAction::AdjustRate { rate };
        }

        SyncAction::Seek {
            target_position_us: expected_us,
        }
    }

    pub async fn run_sync_loop(&self) -> Result<()> {
        loop {
            let action = self.evaluate();
            match action {
                SyncAction::InSync => {}
                SyncAction::AdjustRate { rate } => {
                    self.playback.set_rate(rate)?;
                }
                SyncAction::RestoreRate => {
                    self.playback.set_rate(1.0)?;
                }
                SyncAction::Seek { target_position_us } => {
                    self.playback.seek(target_position_us)?;
                }
            }

            sleep(Duration::from_millis(self.config.sync_eval_interval_ms)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct MockClock {
        now: u64,
    }

    impl ClockSource for MockClock {
        fn reference_now(&self) -> u64 {
            self.now
        }
    }

    #[derive(Default)]
    struct MockPlayback {
        status: Mutex<PlaybackStatus>,
        set_rate_calls: Mutex<Vec<f32>>,
        seek_calls: Mutex<Vec<i64>>,
    }

    impl MockPlayback {
        fn with(position_us: i64, rate: f32) -> Self {
            Self {
                status: Mutex::new(PlaybackStatus {
                    track_id: None,
                    position_us,
                    rate,
                    is_playing: true,
                }),
                set_rate_calls: Mutex::new(vec![]),
                seek_calls: Mutex::new(vec![]),
            }
        }
    }

    impl PlaybackControl for MockPlayback {
        fn position(&self) -> i64 {
            self.status.lock().unwrap().position_us
        }

        fn status(&self) -> PlaybackStatus {
            self.status.lock().unwrap().clone()
        }

        fn set_rate(&self, rate: f32) -> Result<()> {
            self.status.lock().unwrap().rate = rate;
            self.set_rate_calls.lock().unwrap().push(rate);
            Ok(())
        }

        fn seek(&self, position_us: i64) -> Result<()> {
            self.status.lock().unwrap().position_us = position_us;
            self.seek_calls.lock().unwrap().push(position_us);
            Ok(())
        }
    }

    #[test]
    fn evaluate_returns_in_sync_in_zone1() {
        let clock = Arc::new(MockClock { now: 1_000_000 });
        let playback = Arc::new(MockPlayback::with(100_005, 1.0));
        let controller = SyncController::new(clock, playback, SyncConfig::default());
        controller.set_anchor(SyncAnchor {
            reference_time: 1_000_000,
            media_position_us: 100_000,
            playback_rate: 1.0,
            is_playing: true,
        });

        assert_eq!(controller.evaluate(), SyncAction::InSync);
    }

    #[test]
    fn evaluate_returns_restore_rate_when_drift_is_small_but_rate_is_modified() {
        let clock = Arc::new(MockClock { now: 1_000_000 });
        let playback = Arc::new(MockPlayback::with(100_005, 0.98));
        let controller = SyncController::new(clock, playback, SyncConfig::default());
        controller.set_anchor(SyncAnchor {
            reference_time: 1_000_000,
            media_position_us: 100_000,
            playback_rate: 1.0,
            is_playing: true,
        });

        assert_eq!(controller.evaluate(), SyncAction::RestoreRate);
    }

    #[test]
    fn evaluate_returns_adjust_rate_for_zone2() {
        let clock = Arc::new(MockClock { now: 1_000_000 });
        let playback = Arc::new(MockPlayback::with(200_000, 1.0));
        let controller = SyncController::new(clock, playback, SyncConfig::default());
        controller.set_anchor(SyncAnchor {
            reference_time: 1_000_000,
            media_position_us: 100_000,
            playback_rate: 1.0,
            is_playing: true,
        });

        assert_eq!(controller.evaluate(), SyncAction::AdjustRate { rate: 0.95 });
    }

    #[test]
    fn evaluate_returns_seek_for_zone3() {
        let clock = Arc::new(MockClock { now: 1_000_000 });
        let playback = Arc::new(MockPlayback::with(900_000, 1.0));
        let controller = SyncController::new(clock, playback, SyncConfig::default());
        controller.set_anchor(SyncAnchor {
            reference_time: 1_000_000,
            media_position_us: 100_000,
            playback_rate: 1.0,
            is_playing: true,
        });

        assert_eq!(
            controller.evaluate(),
            SyncAction::Seek {
                target_position_us: 100_000
            }
        );
    }
}
