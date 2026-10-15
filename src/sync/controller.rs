use std::sync::Arc;

use crate::clock::sync::ClockSync;
use crate::error::Result;

pub trait ClockSource: Send + Sync {
    fn reference_now(&self) -> u64;
    fn network_stats(&self) -> Option<crate::clock::NetworkStats> {
        None
    }
}

impl ClockSource for ClockSync {
    fn reference_now(&self) -> u64 {
        ClockSync::reference_now(self)
    }

    fn network_stats(&self) -> Option<crate::clock::NetworkStats> {
        Some(ClockSync::network_stats(self))
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
