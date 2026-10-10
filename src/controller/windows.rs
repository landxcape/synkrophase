use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::RwLock;

use windows::Media::Control::{
    GlobalSystemMediaTransportControlsSession,
    GlobalSystemMediaTransportControlsSessionManager,
    GlobalSystemMediaTransportControlsSessionPlaybackStatus,
};

use super::{MediaController, PlaybackState, TrackMetadata};
use crate::error::{Result, SynkroError};
use crate::protocol::messages::TrackIdentity;

const DEFAULT_ACTUATION_DELAY_US: u64 = 4_000; // ~4ms typical for Windows WinRT / GSMTC

#[derive(Debug, Clone)]
pub struct WindowsMediaController {
    last_app_id: Arc<RwLock<Option<String>>>,
    actuation_delay_us: Arc<AtomicU64>,
}

impl Default for WindowsMediaController {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowsMediaController {
    pub fn new() -> Self {
        Self {
            last_app_id: Arc::new(RwLock::new(None)),
            actuation_delay_us: Arc::new(AtomicU64::new(DEFAULT_ACTUATION_DELAY_US)),
        }
    }

    fn record_actuation_time(&self, elapsed_us: u64) {
        let prev = self.actuation_delay_us.load(Ordering::Relaxed);
        let updated = (prev * 8 + elapsed_us * 2) / 10;
        self.actuation_delay_us.store(updated, Ordering::Relaxed);
    }

    fn get_manager() -> Result<GlobalSystemMediaTransportControlsSessionManager> {
        let async_op = GlobalSystemMediaTransportControlsSessionManager::RequestAsync()
            .map_err(|e| SynkroError::MediaControl(format!("Failed to request GSMTC manager: {e}")))?;
        async_op
            .get()
            .map_err(|e| SynkroError::MediaControl(format!("Failed to get GSMTC manager: {e}")))
    }

    fn find_active_session(&self, manager: &GlobalSystemMediaTransportControlsSessionManager) -> Result<GlobalSystemMediaTransportControlsSession> {
        // Try GetCurrentSession first
        if let Ok(current) = manager.GetCurrentSession() {
            return Ok(current);
        }

        // Iterate all active sessions
        let sessions = manager.GetSessions()
            .map_err(|e| SynkroError::MediaControl(format!("Failed to get GSMTC sessions: {e}")))?;

        let mut playing_session = None;
        let mut spotify_session = None;
        let mut any_session = None;

        for session in sessions {
            if any_session.is_none() {
                any_session = Some(session.clone());
            }

            let app_id = session.SourceAppUserModelId()
                .map(|s| s.to_string())
                .unwrap_or_default();

            if app_id.to_lowercase().contains("spotify") {
                spotify_session = Some(session.clone());
            }

            if let Ok(info) = session.GetPlaybackInfo()
                && let Ok(status) = info.PlaybackStatus()
                && status == GlobalSystemMediaTransportControlsSessionPlaybackStatus::Playing
            {
                playing_session = Some(session);
                break;
            }
        }

        if let Some(s) = playing_session {
            return Ok(s);
        }
        if let Some(s) = spotify_session {
            return Ok(s);
        }
        if let Some(s) = any_session {
            return Ok(s);
        }

        Err(SynkroError::MediaControl("No active media sessions found on Windows GSMTC".into()))
    }
}

#[async_trait::async_trait]
impl MediaController for WindowsMediaController {
    async fn get_playback_state(&self) -> Result<PlaybackState> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let manager = Self::get_manager()?;
            let session = this.find_active_session(&manager)?;

            if let Ok(app_id) = session.SourceAppUserModelId() {
                let id_str = app_id.to_string();
                if let Ok(mut lock) = this.last_app_id.try_write() {
                    *lock = Some(id_str);
                }
            }

            // 1. Playback Status
            let is_playing = match session.GetPlaybackInfo() {
                Ok(info) => matches!(
                    info.PlaybackStatus(),
                    Ok(GlobalSystemMediaTransportControlsSessionPlaybackStatus::Playing)
                ),
                Err(_) => false,
            };

            // 2. Playback Rate
            let rate = match session.GetPlaybackInfo() {
                Ok(info) => match info.PlaybackRate() {
                    Ok(prop) => prop.Value().unwrap_or(1.0) as f32,
                    Err(_) => 1.0,
                },
                Err(_) => 1.0,
            };

            // 3. Position and Duration from TimelineProperties
            let (position_us, duration_us) = match session.GetTimelineProperties() {
                Ok(timeline) => {
                    // Position in WinRT is TimeSpan (100-nanosecond intervals)
                    let pos_100ns = timeline.Position().map(|ts| ts.Duration).unwrap_or(0);
                    let mut pos_us = pos_100ns / 10;

                    // Windows GSMTC position is a static snapshot that updates every ~1s.
                    // If the session is currently playing, project position forward using LastUpdatedTime:
                    if is_playing && let Ok(last_updated) = timeline.LastUpdatedTime() {
                        // Unix epoch (1970-01-01) is 116444736000000000 100ns units after Windows epoch (1601-01-01)
                        const WINDOWS_TICK_EPOCH_DIFF: i64 = 116_444_736_000_000_000;
                        if let Ok(now_system) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
                            let now_100ns = (now_system.as_micros() as i64 * 10) + WINDOWS_TICK_EPOCH_DIFF;
                            let elapsed_100ns = (now_100ns - last_updated.UniversalTime).max(0);
                            let elapsed_us = (elapsed_100ns / 10) as f64 * (rate as f64);
                            pos_us += elapsed_us as i64;
                        }
                    }

                    let end_100ns = timeline.EndTime().map(|ts| ts.Duration).unwrap_or(0);
                    let dur_us = if end_100ns > 0 { Some((end_100ns / 10) as u64) } else { None };

                    (pos_us, dur_us)
                }
                Err(_) => (0, None),
            };

            // 4. Metadata
            let metadata = match session.TryGetMediaPropertiesAsync() {
                Ok(async_op) => match async_op.get() {
                    Ok(props) => {
                        let title = props.Title().map(|s| s.to_string()).unwrap_or_default();
                        let artist = props.Artist().map(|s| s.to_string()).ok().filter(|s| !s.is_empty());
                        let album = props.AlbumTitle().map(|s| s.to_string()).ok().filter(|s| !s.is_empty());

                        if title.is_empty() && artist.is_none() {
                            None
                        } else {
                            Some(TrackMetadata {
                                title,
                                artist,
                                album,
                                duration_us,
                            })
                        }
                    }
                    Err(_) => None,
                },
                Err(_) => None,
            };

            Ok(PlaybackState {
                is_playing,
                position_us,
                rate,
                metadata,
            })
        })
        .await
        .map_err(|e| SynkroError::MediaControl(e.to_string()))?
    }

    async fn get_track_identity(&self) -> Result<Option<TrackIdentity>> {
        let state = self.get_playback_state().await?;
        let meta = match state.metadata {
            Some(m) => m,
            None => return Ok(None),
        };

        Ok(Some(TrackIdentity {
            title: meta.title,
            artist: meta.artist,
            album: meta.album,
            duration_us: meta.duration_us,
            spotify_uri: None,
            apple_music_id: None,
        }))
    }

    async fn load_track(&self, track: &TrackIdentity) -> Result<()> {
        if let Some(ref uri) = track.spotify_uri {
            let uri_clone = uri.clone();
            tokio::task::spawn_blocking(move || {
                let _ = std::process::Command::new("cmd")
                    .args(["/C", "start", &uri_clone])
                    .output();
            })
            .await
            .map_err(|e| SynkroError::MediaControl(e.to_string()))?;
        }
        Ok(())
    }

    async fn play(&self) -> Result<()> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let start = Instant::now();
            let manager = Self::get_manager()?;
            let session = this.find_active_session(&manager)?;
            let async_op = session.TryPlayAsync()
                .map_err(|e| SynkroError::MediaControl(format!("Failed to play: {e}")))?;
            let _ = async_op.get();
            this.record_actuation_time(start.elapsed().as_micros() as u64);
            Ok(())
        })
        .await
        .map_err(|e| SynkroError::MediaControl(e.to_string()))?
    }

    async fn pause(&self) -> Result<()> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let start = Instant::now();
            let manager = Self::get_manager()?;
            let session = this.find_active_session(&manager)?;
            let async_op = session.TryPauseAsync()
                .map_err(|e| SynkroError::MediaControl(format!("Failed to pause: {e}")))?;
            let _ = async_op.get();
            this.record_actuation_time(start.elapsed().as_micros() as u64);
            Ok(())
        })
        .await
        .map_err(|e| SynkroError::MediaControl(e.to_string()))?
    }

    async fn seek_to(&self, position_us: i64) -> Result<()> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let start = Instant::now();
            let manager = Self::get_manager()?;
            let session = this.find_active_session(&manager)?;

            // TryChangePlaybackPositionAsync takes requestedplaybackposition as i64 (in 100ns units)
            let pos_100ns = position_us * 10;

            let async_op = session.TryChangePlaybackPositionAsync(pos_100ns)
                .map_err(|e| SynkroError::MediaControl(format!("Failed to seek: {e}")))?;
            let _ = async_op.get();
            this.record_actuation_time(start.elapsed().as_micros() as u64);
            Ok(())
        })
        .await
        .map_err(|e| SynkroError::MediaControl(e.to_string()))?
    }

    async fn set_rate(&self, _rate: f32) -> Result<()> {
        // GSMTC does not expose direct playback rate setters for external sessions
        Ok(())
    }

    async fn next_track(&self) -> Result<()> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let start = Instant::now();
            let manager = Self::get_manager()?;
            let session = this.find_active_session(&manager)?;
            let async_op = session.TrySkipNextAsync()
                .map_err(|e| SynkroError::MediaControl(format!("Failed to skip next: {e}")))?;
            let _ = async_op.get();
            this.record_actuation_time(start.elapsed().as_micros() as u64);
            Ok(())
        })
        .await
        .map_err(|e| SynkroError::MediaControl(e.to_string()))?
    }

    async fn previous_track(&self) -> Result<()> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let start = Instant::now();
            let manager = Self::get_manager()?;
            let session = this.find_active_session(&manager)?;
            let async_op = session.TrySkipPreviousAsync()
                .map_err(|e| SynkroError::MediaControl(format!("Failed to skip previous: {e}")))?;
            let _ = async_op.get();
            this.record_actuation_time(start.elapsed().as_micros() as u64);
            Ok(())
        })
        .await
        .map_err(|e| SynkroError::MediaControl(e.to_string()))?
    }

    async fn get_volume(&self) -> Result<u8> {
        Ok(100)
    }

    async fn set_volume(&self, _volume: u8) -> Result<()> {
        Ok(())
    }

    fn estimated_actuation_delay_us(&self) -> u64 {
        self.actuation_delay_us.load(Ordering::Relaxed)
    }
}
