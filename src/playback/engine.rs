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
    fn position_us(&self) -> i64;
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

    pub fn new_rodio(device_index: Option<usize>) -> Result<Self> {
        Ok(Self::new(Box::new(RodioPlaybackBackend::new(device_index)?)))
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
        self.backend.position_us()
    }

    pub fn position_snapshot(&self) -> PlaybackSnapshot {
        let status = self.status.read().unwrap();
        PlaybackSnapshot {
            position_us: self.position(),
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

pub struct NoopPlaybackBackend;

impl PlaybackBackend for NoopPlaybackBackend {
    fn load_and_play(&self, _stream_url: &str) -> Result<()> { Ok(()) }
    fn position_us(&self) -> i64 { 0 }
    fn set_rate(&self, _rate: f32) -> Result<()> { Ok(()) }
    fn seek(&self, _position_us: i64) -> Result<()> { Ok(()) }
    fn pause(&self) -> Result<()> { Ok(()) }
    fn resume(&self) -> Result<()> { Ok(()) }
    fn stop(&self) -> Result<()> { Ok(()) }
}

pub struct RodioPlaybackBackend {
    inner: std::sync::Mutex<Option<RodioInner>>,
    device_index: Option<usize>,
}

struct RodioInner {
    _sink_handle: rodio::MixerDeviceSink,
    player: rodio::Player,
}

impl RodioPlaybackBackend {
    pub fn new(device_index: Option<usize>) -> Result<Self> {
        Ok(Self {
            inner: std::sync::Mutex::new(None),
            device_index,
        })
    }

    fn with_player<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&rodio::Player) -> T,
    {
        let guard = self.inner.lock().unwrap();
        let Some(inner) = guard.as_ref() else {
            return Err(crate::error::SynkroError::Playback("no active stream".into()));
        };
        Ok(f(&inner.player))
    }
}

impl PlaybackBackend for RodioPlaybackBackend {
    fn load_and_play(&self, stream_url: &str) -> Result<()> {
        let response = reqwest::blocking::get(stream_url).map_err(|e| crate::error::SynkroError::Playback(e.to_string()))?;
        let bytes = response.bytes().map_err(|e| crate::error::SynkroError::Playback(e.to_string()))?;
        let cursor = std::io::Cursor::new(bytes.to_vec());
        let decoder = rodio::Decoder::try_from(cursor).map_err(|e| crate::error::SynkroError::Playback(e.to_string()))?;

        let sink_handle = if let Some(idx) = self.device_index {
            use rodio::cpal::traits::HostTrait;
            let host = rodio::cpal::default_host();
            let mut devices = host.output_devices().map_err(|e| crate::error::SynkroError::Playback(e.to_string()))?;
            let device = devices.nth(idx).ok_or_else(|| crate::error::SynkroError::Playback("Device not found".into()))?;
            rodio::DeviceSinkBuilder::from_device(device).map_err(|e| crate::error::SynkroError::Playback(e.to_string()))?
                .open_stream().map_err(|e| crate::error::SynkroError::Playback(e.to_string()))?
        } else {
            rodio::DeviceSinkBuilder::open_default_sink().map_err(|e| crate::error::SynkroError::Playback(e.to_string()))?
        };

        let player = rodio::Player::connect_new(&sink_handle.mixer());
        player.append(decoder);
        player.play();

        *self.inner.lock().unwrap() = Some(RodioInner {
            _sink_handle: sink_handle,
            player,
        });
        Ok(())
    }

    fn position_us(&self) -> i64 {
        self.with_player(|p| p.get_pos().as_micros() as i64).unwrap_or(0)
    }

    fn set_rate(&self, rate: f32) -> Result<()> {
        let _ = self.with_player(|p| p.set_speed(rate));
        Ok(())
    }

    fn seek(&self, position_us: i64) -> Result<()> {
        let res = self.with_player(|p| {
            p.try_seek(std::time::Duration::from_micros(position_us.max(0) as u64))
                .map_err(|e| crate::error::SynkroError::Playback(e.to_string()))
        })?;
        res
    }

    fn pause(&self) -> Result<()> {
        let _ = self.with_player(|p| p.pause());
        Ok(())
    }

    fn resume(&self) -> Result<()> {
        let _ = self.with_player(|p| p.play());
        Ok(())
    }

    fn stop(&self) -> Result<()> {
        let _ = self.with_player(|p| p.stop());
        Ok(())
    }
}
