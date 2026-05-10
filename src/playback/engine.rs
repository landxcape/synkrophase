use std::sync::RwLock;
use rodio::cpal::traits::{DeviceTrait, HostTrait};

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
    device_index: Option<usize>,
}

struct RodioInner {
    _sink_handle: rodio::MixerDeviceSink,
    player: rodio::Player,
}

impl RodioPlaybackBackend {
    pub fn new(device_index: Option<usize>) -> Result<Self> {
        // Debug: List available devices on startup
        if let Ok(devices) = rodio::cpal::default_host().output_devices() {
            println!("[Audio] Detected output devices:");
            for (i, device) in devices.enumerate() {
                let name = device.name().unwrap_or_else(|_| "Unknown".into());
                println!("  {}: {}", i, name);
            }
        }

        Ok(Self {
            inner: std::sync::Mutex::new(None),
            device_index,
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
        println!("[Audio] Fetching stream: {}", stream_url);
        let response = reqwest::blocking::get(stream_url)
            .map_err(|err| {
                let e = crate::error::SynkroError::Playback(format!("Network error: {}", err));
                eprintln!("[Audio] Error: {}", e);
                e
            })?;
        
        if !response.status().is_success() {
            let e = crate::error::SynkroError::Playback(format!(
                "HTTP failure: {}",
                response.status()
            ));
            eprintln!("[Audio] Error: {}", e);
            return Err(e);
        }

        let bytes = response
            .bytes()
            .map_err(|err| crate::error::SynkroError::Playback(err.to_string()))?;
        
        println!("[Audio] Decoding buffer ({} bytes)...", bytes.len());
        let cursor = std::io::Cursor::new(bytes.to_vec());
        let decoder = rodio::Decoder::try_from(cursor)
            .map_err(|err| {
                let e = crate::error::SynkroError::Playback(format!("Decoder error: {}", err));
                eprintln!("[Audio] Error: {}", e);
                e
            })?;

        println!("[Audio] Opening output device...");
        let sink_handle = if let Some(idx) = self.device_index {
            let host = rodio::cpal::default_host();
            let mut devices = host.output_devices().map_err(|e| {
                crate::error::SynkroError::Playback(format!("Host error: {}", e))
            })?;
            let device = devices.nth(idx).ok_or_else(|| {
                crate::error::SynkroError::Playback(format!("Device index {} not found", idx))
            })?;
            rodio::DeviceSinkBuilder::from_device(device)
                .map_err(|e| crate::error::SynkroError::Playback(e.to_string()))?
                .build()
                .map_err(|e| crate::error::SynkroError::Playback(e.to_string()))
        } else {
            rodio::DeviceSinkBuilder::open_default_sink()
                .map_err(|e| crate::error::SynkroError::Playback(e.to_string()))
        }
        .map_err(|err| {
            eprintln!("[Audio] Error: {}", err);
            err
        })?;
        
        let player = rodio::Player::connect_new(sink_handle.mixer());
        player.append(decoder);
        player.play();
        println!("[Audio] Playback started.");

        *self.inner.lock().unwrap() = Some(RodioInner {
            _sink_handle: sink_handle,
            player,
        });
        Ok(())
    }

    fn position_us(&self) -> i64 {
        let guard = self.inner.lock().unwrap();
        guard
            .as_ref()
            .map(|inner| inner.player.get_pos().as_micros() as i64)
            .unwrap_or(0)
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
