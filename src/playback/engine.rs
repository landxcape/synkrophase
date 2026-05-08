use std::sync::RwLock;

use crate::clock::sync::local_now;
use crate::error::Result;

#[derive(Debug, Clone, PartialEq)]
pub struct PlaybackSnapshot {
    pub position_us: i64,
    pub reference_time: u64,
    pub rate: f32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlaybackStatus {
    pub track_id: Option<String>,
    pub stream_url: Option<String>,
    pub position_us: i64,
    pub rate: f32,
    pub is_playing: bool,
}

impl Default for PlaybackStatus {
    fn default() -> Self {
        Self {
            track_id: None,
            stream_url: None,
            position_us: 0,
            rate: 1.0,
            is_playing: false,
        }
    }
}

pub trait PlaybackBackend: Send + Sync {
    fn load_and_play(&self, stream_url: &str) -> Result<()>;
    fn set_rate(&self, rate: f32) -> Result<()>;
    fn seek(&self, position_us: i64) -> Result<()>;
    fn pause(&self) -> Result<()>;
    fn resume(&self) -> Result<()>;
    fn stop(&self) -> Result<()>;
}

pub struct PlaybackEngine {
    backend: Box<dyn PlaybackBackend>,
    status: RwLock<PlaybackStatus>,
}

impl PlaybackEngine {
    pub fn new(backend: Box<dyn PlaybackBackend>) -> Self {
        Self {
            backend,
            status: RwLock::new(PlaybackStatus::default()),
        }
    }

    pub fn new_rodio() -> Result<Self> {
        Ok(Self::new(Box::new(RodioPlaybackBackend::new()?)))
    }

    pub fn load_and_play(&self, track_id: &str, stream_url: &str) -> Result<()> {
        self.backend.load_and_play(stream_url)?;
        let mut status = self.status.write().unwrap();
        status.track_id = Some(track_id.to_string());
        status.stream_url = Some(stream_url.to_string());
        status.position_us = 0;
        status.rate = 1.0;
        status.is_playing = true;
        Ok(())
    }

    pub fn position(&self) -> i64 {
        self.status.read().unwrap().position_us
    }

    pub fn position_snapshot(&self) -> PlaybackSnapshot {
        let status = self.status.read().unwrap();
        PlaybackSnapshot {
            position_us: status.position_us,
            reference_time: local_now(),
            rate: status.rate,
        }
    }

    pub fn set_rate(&self, rate: f32) -> Result<()> {
        self.backend.set_rate(rate)?;
        self.status.write().unwrap().rate = rate;
        Ok(())
    }

    pub fn seek(&self, position_us: i64) -> Result<()> {
        self.backend.seek(position_us)?;
        self.status.write().unwrap().position_us = position_us;
        Ok(())
    }

    pub fn pause(&self) -> Result<()> {
        self.backend.pause()?;
        self.status.write().unwrap().is_playing = false;
        Ok(())
    }

    pub fn resume(&self) -> Result<()> {
        self.backend.resume()?;
        self.status.write().unwrap().is_playing = true;
        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        self.backend.stop()?;
        let mut status = self.status.write().unwrap();
        status.is_playing = false;
        status.position_us = 0;
        Ok(())
    }

    pub fn is_playing(&self) -> bool {
        self.status.read().unwrap().is_playing
    }

    pub fn status(&self) -> PlaybackStatus {
        self.status.read().unwrap().clone()
    }
}

pub struct RodioPlaybackBackend {
    inner: std::sync::Mutex<Option<RodioInner>>,
}

struct RodioInner {
    _sink_handle: rodio::MixerDeviceSink,
    player: rodio::Player,
}

impl RodioPlaybackBackend {
    pub fn new() -> Result<Self> {
        Ok(Self {
            inner: std::sync::Mutex::new(None),
        })
    }

    fn with_player<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&rodio::Player) -> Result<()>,
    {
        let guard = self.inner.lock().unwrap();
        let Some(inner) = guard.as_ref() else {
            return Err(crate::error::SynkroError::Playback(
                "no active playback stream".to_string(),
            ));
        };
        f(&inner.player)
    }
}

impl PlaybackBackend for RodioPlaybackBackend {
    fn load_and_play(&self, stream_url: &str) -> Result<()> {
        let response = reqwest::blocking::get(stream_url)
            .map_err(|err| crate::error::SynkroError::Playback(err.to_string()))?;
        if !response.status().is_success() {
            return Err(crate::error::SynkroError::Playback(format!(
                "audio stream fetch failed with status {}",
                response.status()
            )));
        }

        let bytes = response
            .bytes()
            .map_err(|err| crate::error::SynkroError::Playback(err.to_string()))?;
        let cursor = std::io::Cursor::new(bytes.to_vec());
        let decoder = rodio::Decoder::try_from(cursor)
            .map_err(|err| crate::error::SynkroError::Playback(err.to_string()))?;

        let sink_handle = rodio::DeviceSinkBuilder::open_default_sink()
            .map_err(|err| crate::error::SynkroError::Playback(err.to_string()))?;
        let player = rodio::Player::connect_new(sink_handle.mixer());
        player.append(decoder);
        player.play();

        *self.inner.lock().unwrap() = Some(RodioInner {
            _sink_handle: sink_handle,
            player,
        });
        Ok(())
    }

    fn set_rate(&self, rate: f32) -> Result<()> {
        self.with_player(|player| {
            player.set_speed(rate);
            Ok(())
        })
    }

    fn seek(&self, position_us: i64) -> Result<()> {
        self.with_player(|player| {
            player
                .try_seek(std::time::Duration::from_micros(position_us.max(0) as u64))
                .map_err(|err| crate::error::SynkroError::Playback(err.to_string()))
        })
    }

    fn pause(&self) -> Result<()> {
        self.with_player(|player| {
            player.pause();
            Ok(())
        })
    }

    fn resume(&self) -> Result<()> {
        self.with_player(|player| {
            player.play();
            Ok(())
        })
    }

    fn stop(&self) -> Result<()> {
        self.with_player(|player| {
            player.stop();
            Ok(())
        })
    }
}
