use std::process::Command;

use super::{MediaController, PlaybackState, TrackMetadata};
use crate::error::{Result, SynkroError};

#[derive(Default, Debug, Clone)]
pub struct MacOsMediaController;

impl MacOsMediaController {
    pub fn new() -> Self {
        Self
    }

    fn run_osascript(script: &str) -> std::result::Result<String, std::io::Error> {
        let output = Command::new("osascript")
            .arg("-e")
            .arg(script)
            .output()?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                String::from_utf8_lossy(&output.stderr).trim().to_string(),
            ))
        }
    }

    fn is_process_running(app_name: &str) -> bool {
        let script = format!(
            "tell application \"System Events\" to (name of processes) contains \"{}\"",
            app_name
        );
        Self::run_osascript(&script)
            .map(|res| res.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    fn query_spotify() -> Option<PlaybackState> {
        if !Self::is_process_running("Spotify") {
            return None;
        }

        let script = r#"
            tell application "Spotify"
                set pState to player state as string
                set pPos to player position
                set tName to name of current track
                set tArtist to artist of current track
                set tAlbum to album of current track
                set tDur to (duration of current track) / 1000
                return pState & "|||" & (pPos as string) & "|||" & tName & "|||" & tArtist & "|||" & tAlbum & "|||" & (tDur as string)
            end tell
        "#;

        let output = Self::run_osascript(script).ok()?;
        Self::parse_script_output(&output)
    }

    fn query_music() -> Option<PlaybackState> {
        if !Self::is_process_running("Music") {
            return None;
        }

        let script = r#"
            tell application "Music"
                set pState to player state as string
                set pPos to player position
                set tName to name of current track
                set tArtist to artist of current track
                set tAlbum to album of current track
                set tDur to duration of current track
                return pState & "|||" & (pPos as string) & "|||" & tName & "|||" & tArtist & "|||" & tAlbum & "|||" & (tDur as string)
            end tell
        "#;

        let output = Self::run_osascript(script).ok()?;
        Self::parse_script_output(&output)
    }

    fn parse_script_output(output: &str) -> Option<PlaybackState> {
        let parts: Vec<&str> = output.split("|||").collect();
        if parts.len() < 6 {
            return None;
        }

        let is_playing = parts[0].eq_ignore_ascii_case("playing");
        let position_sec: f64 = parts[1].parse().unwrap_or(0.0);
        let title = parts[2].to_string();
        let artist = if parts[3].is_empty() { None } else { Some(parts[3].to_string()) };
        let album = if parts[4].is_empty() { None } else { Some(parts[4].to_string()) };
        let duration_sec: f64 = parts[5].parse().unwrap_or(0.0);

        Some(PlaybackState {
            is_playing,
            position_us: (position_sec * 1_000_000.0) as i64,
            rate: 1.0,
            metadata: Some(TrackMetadata {
                title,
                artist,
                album,
                duration_us: if duration_sec > 0.0 { Some((duration_sec * 1_000_000.0) as u64) } else { None },
            }),
        })
    }
}

#[async_trait::async_trait]
impl MediaController for MacOsMediaController {
    async fn get_playback_state(&self) -> Result<PlaybackState> {
        tokio::task::spawn_blocking(|| {
            if let Some(state) = Self::query_spotify() {
                return Ok(state);
            }
            if let Some(state) = Self::query_music() {
                return Ok(state);
            }
            Ok(PlaybackState::default())
        })
        .await
        .map_err(|e| SynkroError::MediaControl(e.to_string()))?
    }

    async fn play(&self) -> Result<()> {
        tokio::task::spawn_blocking(|| {
            if Self::is_process_running("Spotify") {
                let _ = Self::run_osascript("tell application \"Spotify\" to play");
            } else if Self::is_process_running("Music") {
                let _ = Self::run_osascript("tell application \"Music\" to play");
            }
            Ok(())
        })
        .await
        .map_err(|e| SynkroError::MediaControl(e.to_string()))?
    }

    async fn pause(&self) -> Result<()> {
        tokio::task::spawn_blocking(|| {
            if Self::is_process_running("Spotify") {
                let _ = Self::run_osascript("tell application \"Spotify\" to pause");
            } else if Self::is_process_running("Music") {
                let _ = Self::run_osascript("tell application \"Music\" to pause");
            }
            Ok(())
        })
        .await
        .map_err(|e| SynkroError::MediaControl(e.to_string()))?
    }

    async fn seek_to(&self, position_us: i64) -> Result<()> {
        let seconds = (position_us as f64) / 1_000_000.0;
        tokio::task::spawn_blocking(move || {
            if Self::is_process_running("Spotify") {
                let script = format!("tell application \"Spotify\" to set player position to {}", seconds);
                let _ = Self::run_osascript(&script);
            } else if Self::is_process_running("Music") {
                let script = format!("tell application \"Music\" to set player position to {}", seconds);
                let _ = Self::run_osascript(&script);
            }
            Ok(())
        })
        .await
        .map_err(|e| SynkroError::MediaControl(e.to_string()))?
    }

    async fn set_rate(&self, _rate: f32) -> Result<()> {
        // macOS AppleScript players do not support continuous pitch/rate shifting.
        // Returning Ok(()) allows the caller to use micro-seek fallback if drift is detected.
        Ok(())
    }
}
