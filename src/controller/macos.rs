use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Instant;

use super::{MediaController, PlaybackState, TrackMetadata};
use crate::error::{Result, SynkroError};
use crate::protocol::messages::TrackIdentity;

const PLAYER_AUTO: u8 = 0;
const PLAYER_SPOTIFY: u8 = 1;
const PLAYER_MUSIC: u8 = 2;

// Default initial assumption for macOS osascript execution is ~150ms
const DEFAULT_OSASCRIPT_DELAY_US: u64 = 150_000;

#[derive(Debug, Clone)]
pub struct MacOsMediaController {
    last_player: Arc<AtomicU8>,
    actuation_delay_us: Arc<AtomicU64>,
}

impl Default for MacOsMediaController {
    fn default() -> Self {
        Self::new()
    }
}

impl MacOsMediaController {
    pub fn new() -> Self {
        Self {
            last_player: Arc::new(AtomicU8::new(PLAYER_AUTO)),
            actuation_delay_us: Arc::new(AtomicU64::new(DEFAULT_OSASCRIPT_DELAY_US)),
        }
    }

    fn record_actuation_time(&self, elapsed_us: u64) {
        // Exponential moving average: new_delay = (prev * 8 + elapsed * 2) / 10
        let prev = self.actuation_delay_us.load(Ordering::Relaxed);
        let updated = (prev * 8 + elapsed_us * 2) / 10;
        self.actuation_delay_us.store(updated, Ordering::Relaxed);
    }

    fn run_osascript(script: &str) -> std::result::Result<String, std::io::Error> {
        let output = Command::new("osascript").arg("-e").arg(script).output()?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&output.stderr).trim().to_string(),
            ))
        }
    }

    fn query_active_player(&self) -> Option<PlaybackState> {
        let last = self.last_player.load(Ordering::Acquire);
        let script = format!(
            r#"
            set sRunning to false
            set mRunning to false
            set sState to ""
            set mState to ""

            if application "Spotify" is running then
                set sRunning to true
                try
                    tell application "Spotify" to set sState to (player state as string)
                end try
            end if

            if application "Music" is running then
                set mRunning to true
                try
                    tell application "Music" to set mState to (player state as string)
                end try
            end if

            -- Rule 1: Actively playing player takes absolute precedence
            if sRunning and sState is "playing" then
                tell application "Spotify"
                    set pPos to player position
                    set tName to name of current track
                    set tArtist to artist of current track
                    set tAlbum to album of current track
                    set tDur to (duration of current track) / 1000
                    return "spotify|||" & sState & "|||" & (pPos as string) & "|||" & tName & "|||" & tArtist & "|||" & tAlbum & "|||" & (tDur as string)
                end tell
            else if mRunning and mState is "playing" then
                tell application "Music"
                    set pPos to player position
                    set tName to name of current track
                    set tArtist to artist of current track
                    set tAlbum to album of current track
                    set tDur to duration of current track
                    return "music|||" & mState & "|||" & (pPos as string) & "|||" & tName & "|||" & tArtist & "|||" & tAlbum & "|||" & (tDur as string)
                end tell
            -- Rule 2: Fall back to last active player if running
            else if {last} = 2 and mRunning and mState is not "" then
                tell application "Music"
                    set pPos to player position
                    set tName to name of current track
                    set tArtist to artist of current track
                    set tAlbum to album of current track
                    set tDur to duration of current track
                    return "music|||" & mState & "|||" & (pPos as string) & "|||" & tName & "|||" & tArtist & "|||" & tAlbum & "|||" & (tDur as string)
                end tell
            else if {last} = 1 and sRunning and sState is not "" then
                tell application "Spotify"
                    set pPos to player position
                    set tName to name of current track
                    set tArtist to artist of current track
                    set tAlbum to album of current track
                    set tDur to (duration of current track) / 1000
                    return "spotify|||" & sState & "|||" & (pPos as string) & "|||" & tName & "|||" & tArtist & "|||" & tAlbum & "|||" & (tDur as string)
                end tell
            -- Rule 3: Whichever player is running and responsive
            else if sRunning and sState is not "" then
                tell application "Spotify"
                    set pPos to player position
                    set tName to name of current track
                    set tArtist to artist of current track
                    set tAlbum to album of current track
                    set tDur to (duration of current track) / 1000
                    return "spotify|||" & sState & "|||" & (pPos as string) & "|||" & tName & "|||" & tArtist & "|||" & tAlbum & "|||" & (tDur as string)
                end tell
            else if mRunning and mState is not "" then
                tell application "Music"
                    set pPos to player position
                    set tName to name of current track
                    set tArtist to artist of current track
                    set tAlbum to album of current track
                    set tDur to duration of current track
                    return "music|||" & mState & "|||" & (pPos as string) & "|||" & tName & "|||" & tArtist & "|||" & tAlbum & "|||" & (tDur as string)
                end tell
            else
                return "none"
            end if
            "#
        );

        let output = Self::run_osascript(&script).ok()?;
        if output == "none" || output.is_empty() {
            return None;
        }

        let (player_id, state) = Self::parse_script_output(&output)?;
        self.last_player.store(player_id, Ordering::Release);
        Some(state)
    }

    fn parse_script_output(output: &str) -> Option<(u8, PlaybackState)> {
        let parts: Vec<&str> = output.split("|||").collect();
        if parts.len() < 7 {
            return None;
        }

        let player_id = match parts[0] {
            "spotify" => PLAYER_SPOTIFY,
            "music" => PLAYER_MUSIC,
            _ => PLAYER_AUTO,
        };

        let is_playing = parts[1].eq_ignore_ascii_case("playing");
        let position_sec: f64 = parts[2].parse().unwrap_or(0.0);
        let title = parts[3].to_string();
        let artist = if parts[4].is_empty() {
            None
        } else {
            Some(parts[4].to_string())
        };
        let album = if parts[5].is_empty() {
            None
        } else {
            Some(parts[5].to_string())
        };
        let duration_sec: f64 = parts[6].parse().unwrap_or(0.0);

        Some((
            player_id,
            PlaybackState {
                is_playing,
                position_us: (position_sec * 1_000_000.0) as i64,
                rate: 1.0,
                metadata: Some(TrackMetadata {
                    title,
                    artist,
                    album,
                    duration_us: if duration_sec > 0.0 {
                        Some((duration_sec * 1_000_000.0) as u64)
                    } else {
                        None
                    },
                }),
            },
        ))
    }

    fn action_script(&self, action_cmd: &str) -> String {
        let last = self.last_player.load(Ordering::Acquire);
        format!(
            r#"
            set sPlaying to false
            set mPlaying to false

            if application "Spotify" is running then
                try
                    tell application "Spotify" to set sPlaying to (player state is playing)
                end try
            end if

            if application "Music" is running then
                try
                    tell application "Music" to set mPlaying to (player state is playing)
                end try
            end if

            if sPlaying then
                tell application "Spotify" to {action_cmd}
            else if mPlaying then
                tell application "Music" to {action_cmd}
            else if {last} = 2 and application "Music" is running then
                tell application "Music" to {action_cmd}
            else if {last} = 1 and application "Spotify" is running then
                tell application "Spotify" to {action_cmd}
            else if application "Spotify" is running then
                tell application "Spotify" to {action_cmd}
            else if application "Music" is running then
                tell application "Music" to {action_cmd}
            end if
            "#
        )
    }
}

#[async_trait::async_trait]
impl MediaController for MacOsMediaController {
    async fn get_playback_state(&self) -> Result<PlaybackState> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(state) = this.query_active_player() {
                return Ok(state);
            }
            Ok(PlaybackState::default())
        })
        .await
        .map_err(|e| SynkroError::MediaControl(e.to_string()))?
    }

    async fn get_track_identity(&self) -> Result<Option<TrackIdentity>> {
        let last = self.last_player.load(Ordering::Acquire);
        tokio::task::spawn_blocking(move || {
            let script = format!(
                r#"
                if application "Spotify" is running and ({last} = 1 or {last} = 0) then
                    try
                        tell application "Spotify"
                            set sId to id of current track
                            set sName to name of current track
                            set sArtist to artist of current track
                            set sAlbum to album of current track
                            set sDur to (duration of current track) / 1000
                            return "spotify|||" & sId & "|||" & sName & "|||" & sArtist & "|||" & sAlbum & "|||" & (sDur as string)
                        end tell
                    end try
                end if

                if application "Music" is running then
                    try
                        tell application "Music"
                            set mName to name of current track
                            set mArtist to artist of current track
                            set mAlbum to album of current track
                            set mDur to duration of current track
                            return "music||||||" & mName & "|||" & mArtist & "|||" & mAlbum & "|||" & (mDur as string)
                        end tell
                    end try
                end if
                return "none"
                "#
            );

            let out = Self::run_osascript(&script).unwrap_or_default();
            if out == "none" || out.is_empty() {
                return Ok(None);
            }

            let parts: Vec<&str> = out.split("|||").collect();
            if parts.len() < 6 {
                return Ok(None);
            }

            let player = parts[0];
            let id = parts[1];
            let title = parts[2].to_string();
            let artist = if parts[3].is_empty() { None } else { Some(parts[3].to_string()) };
            let album = if parts[4].is_empty() { None } else { Some(parts[4].to_string()) };
            let duration_sec: f64 = parts[5].parse().unwrap_or(0.0);
            let duration_us = if duration_sec > 0.0 { Some((duration_sec * 1_000_000.0) as u64) } else { None };

            let (spotify_uri, apple_music_id) = if player == "spotify" {
                (if id.is_empty() { None } else { Some(id.to_string()) }, None)
            } else {
                (None, Some(title.clone()))
            };

            Ok(Some(TrackIdentity {
                title,
                artist,
                album,
                spotify_uri,
                apple_music_id,
                duration_us,
            }))
        })
        .await
        .map_err(|e| SynkroError::MediaControl(e.to_string()))?
    }

    async fn load_track(&self, track: &TrackIdentity) -> Result<()> {
        let track = track.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(uri) = &track.spotify_uri {
                let script = format!(
                    r#"
                    if application "Spotify" is running then
                        tell application "Spotify" to play track "{uri}"
                    end if
                    "#
                );
                let _ = Self::run_osascript(&script);
                return Ok(());
            }

            // Fallback: search and play in Apple Music or Spotify by title and artist
            let title_escaped = track.title.replace('"', "\\\"");
            let artist_query = track.artist.as_deref().unwrap_or("");
            let script = format!(
                r#"
                if application "Music" is running then
                    tell application "Music"
                        try
                            set matched to (every track whose name contains "{title_escaped}")
                            if (count of matched) > 0 then
                                play item 1 of matched
                                return "ok"
                            end if
                        end try
                    end tell
                end if
                if application "Spotify" is running then
                    tell application "Spotify"
                        -- Trigger search/play query via spotify URI
                        -- Format: spotify:search:<query>
                        set q to "{title_escaped} {artist_query}"
                        -- Attempt to play
                        play track "spotify:search:" & q
                    end tell
                end if
                "#
            );
            let _ = Self::run_osascript(&script);
            Ok(())
        })
        .await
        .map_err(|e| SynkroError::MediaControl(e.to_string()))?
    }

    async fn play(&self) -> Result<()> {
        let script = self.action_script("play");
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let start = Instant::now();
            let _ = Self::run_osascript(&script);
            this.record_actuation_time(start.elapsed().as_micros() as u64);
            Ok(())
        })
        .await
        .map_err(|e| SynkroError::MediaControl(e.to_string()))?
    }

    async fn pause(&self) -> Result<()> {
        let script = self.action_script("pause");
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let start = Instant::now();
            let _ = Self::run_osascript(&script);
            this.record_actuation_time(start.elapsed().as_micros() as u64);
            Ok(())
        })
        .await
        .map_err(|e| SynkroError::MediaControl(e.to_string()))?
    }

    async fn seek_to(&self, position_us: i64) -> Result<()> {
        let seconds = (position_us as f64) / 1_000_000.0;
        let script = self.action_script(&format!("set player position to {seconds}"));
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let start = Instant::now();
            let _ = Self::run_osascript(&script);
            this.record_actuation_time(start.elapsed().as_micros() as u64);
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

    async fn next_track(&self) -> Result<()> {
        let script = self.action_script("next track");
        tokio::task::spawn_blocking(move || {
            let _ = Self::run_osascript(&script);
            Ok(())
        })
        .await
        .map_err(|e| SynkroError::MediaControl(e.to_string()))?
    }

    async fn previous_track(&self) -> Result<()> {
        let script = self.action_script("previous track");
        tokio::task::spawn_blocking(move || {
            let _ = Self::run_osascript(&script);
            Ok(())
        })
        .await
        .map_err(|e| SynkroError::MediaControl(e.to_string()))?
    }

    async fn get_volume(&self) -> Result<u8> {
        tokio::task::spawn_blocking(|| {
            let out = Self::run_osascript("output volume of (get volume settings)")
                .map_err(|e| SynkroError::MediaControl(e.to_string()))?;
            out.trim()
                .parse::<u8>()
                .map_err(|e| SynkroError::MediaControl(e.to_string()))
        })
        .await
        .map_err(|e| SynkroError::MediaControl(e.to_string()))?
    }

    async fn set_volume(&self, volume: u8) -> Result<()> {
        let vol = volume.min(100);
        tokio::task::spawn_blocking(move || {
            let script = format!("set volume output volume {}", vol);
            let _ = Self::run_osascript(&script);
            Ok(())
        })
        .await
        .map_err(|e| SynkroError::MediaControl(e.to_string()))?
    }

    fn estimated_actuation_delay_us(&self) -> u64 {
        self.actuation_delay_us.load(Ordering::Relaxed)
    }
}
