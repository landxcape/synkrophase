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
            set sPlaying to false
            set mPlaying to false
            set sHasTrack to false
            set mHasTrack to false
            set sState to ""
            set mState to ""
            set frontApp to ""

            tell application "System Events"
                try
                    set frontApp to name of first application process whose frontmost is true
                end try
            end tell

            if application "Spotify" is running then
                set sRunning to true
                try
                    tell application "Spotify"
                        set sState to (player state as string)
                        if sState is "playing" then set sPlaying to true
                        if name of current track is not "" then set sHasTrack to true
                    end tell
                end try
            end if

            if application "Music" is running then
                set mRunning to true
                try
                    tell application "Music"
                        set mState to (player state as string)
                        if mState is "playing" then set mPlaying to true
                        if name of current track is not "" then set mHasTrack to true
                    end tell
                end try
            end if

            set pickApp to ""

            -- 1. Actively playing player takes precedence
            if mPlaying and not sPlaying then
                set pickApp to "music"
            else if sPlaying and not mPlaying then
                set pickApp to "spotify"
            else if mPlaying and sPlaying then
                if frontApp is "Music" then
                    set pickApp to "music"
                else if frontApp is "Spotify" then
                    set pickApp to "spotify"
                else if {last} = 2 then
                    set pickApp to "music"
                else
                    set pickApp to "spotify"
                end if

            -- 2. Respect last active player if running with loaded track
            else if {last} = 2 and mRunning and (mHasTrack or not sHasTrack) then
                set pickApp to "music"
            else if {last} = 1 and sRunning and (sHasTrack or not mHasTrack) then
                set pickApp to "spotify"

            -- 3. Loaded track tie-breaker
            else if mHasTrack and not sHasTrack then
                set pickApp to "music"
            else if sHasTrack and not mHasTrack then
                set pickApp to "spotify"

            -- 4. Frontmost focused application tie-breaker
            else if frontApp is "Music" and mRunning then
                set pickApp to "music"
            else if frontApp is "Spotify" and sRunning then
                set pickApp to "spotify"

            -- 5. Fallback to whichever is running
            else if mRunning and not sRunning then
                set pickApp to "music"
            else if sRunning and not mRunning then
                set pickApp to "spotify"
            else if {last} = 2 and mRunning then
                set pickApp to "music"
            else if mRunning then
                set pickApp to "music"
            else if sRunning then
                set pickApp to "spotify"
            end if

            if pickApp is "music" then
                tell application "Music"
                    try
                        set pPos to player position
                        set tName to name of current track
                        set tArtist to artist of current track
                        set tAlbum to album of current track
                        set tDur to duration of current track
                        return "music|||" & mState & "|||" & (pPos as string) & "|||" & tName & "|||" & tArtist & "|||" & tAlbum & "|||" & (tDur as string)
                    on error
                        return "music|||" & mState & "|||0.0|||||||||0.0"
                    end try
                end tell
            else if pickApp is "spotify" then
                tell application "Spotify"
                    try
                        set pPos to player position
                        set tName to name of current track
                        set tArtist to artist of current track
                        set tAlbum to album of current track
                        set tDur to (duration of current track) / 1000
                        return "spotify|||" & sState & "|||" & (pPos as string) & "|||" & tName & "|||" & tArtist & "|||" & tAlbum & "|||" & (tDur as string)
                    on error
                        return "spotify|||" & sState & "|||0.0|||||||||0.0"
                    end try
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
            set prevProc to ""
            tell application "System Events"
                try
                    set prevProc to name of first application process whose frontmost is true
                end try
            end tell

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

            set pickApp to ""
            if mPlaying and not sPlaying then
                set pickApp to "music"
            else if sPlaying and not mPlaying then
                set pickApp to "spotify"
            else if mPlaying and sPlaying then
                if prevProc is "Music" then
                    set pickApp to "music"
                else if prevProc is "Spotify" then
                    set pickApp to "spotify"
                else if {last} = 2 then
                    set pickApp to "music"
                else
                    set pickApp to "spotify"
                end if
            else if {last} = 2 and application "Music" is running then
                set pickApp to "music"
            else if {last} = 1 and application "Spotify" is running then
                set pickApp to "spotify"
            else if prevProc is "Music" and application "Music" is running then
                set pickApp to "music"
            else if prevProc is "Spotify" and application "Spotify" is running then
                set pickApp to "spotify"
            else if application "Music" is running and not application "Spotify" is running then
                set pickApp to "music"
            else if application "Spotify" is running and not application "Music" is running then
                set pickApp to "spotify"
            else if application "Music" is running then
                set pickApp to "music"
            else if application "Spotify" is running then
                set pickApp to "spotify"
            end if

            if pickApp is "music" then
                tell application "Music" to {action_cmd}
            else if pickApp is "spotify" then
                tell application "Spotify" to {action_cmd}
            end if

            if prevProc is not "" and prevProc is not "Spotify" and prevProc is not "Music" then
                delay 0.05
                try
                    tell application "System Events" to set frontmost of process prevProc to true
                end try
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
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let script = format!(
                r#"
                set sRunning to false
                set mRunning to false
                set sPlaying to false
                set mPlaying to false
                set frontApp to ""

                tell application "System Events"
                    try
                        set frontApp to name of first application process whose frontmost is true
                    end try
                end tell

                if application "Spotify" is running then
                    set sRunning to true
                    try
                        tell application "Spotify" to set sPlaying to (player state is playing)
                    end try
                end if

                if application "Music" is running then
                    set mRunning to true
                    try
                        tell application "Music" to set mPlaying to (player state is playing)
                    end try
                end if

                set pickApp to ""
                if mPlaying and not sPlaying then
                    set pickApp to "music"
                else if sPlaying and not mPlaying then
                    set pickApp to "spotify"
                else if {last} = 2 and mRunning then
                    set pickApp to "music"
                else if {last} = 1 and sRunning then
                    set pickApp to "spotify"
                else if frontApp is "Music" and mRunning then
                    set pickApp to "music"
                else if frontApp is "Spotify" and sRunning then
                    set pickApp to "spotify"
                else if mRunning and not sRunning then
                    set pickApp to "music"
                else if sRunning and not mRunning then
                    set pickApp to "spotify"
                else if mRunning then
                    set pickApp to "music"
                else if sRunning then
                    set pickApp to "spotify"
                end if

                if pickApp is "spotify" then
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
                else if pickApp is "music" then
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
            if player == "music" {
                this.last_player.store(PLAYER_MUSIC, Ordering::Release);
            } else if player == "spotify" {
                this.last_player.store(PLAYER_SPOTIFY, Ordering::Release);
            }

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
        let last = self.last_player.load(Ordering::Acquire);
        let track = track.clone();
        tokio::task::spawn_blocking(move || {
            let title_escaped = track.title.replace('"', "\\\"");
            let artist_query = track.artist.as_deref().unwrap_or("");
            let spotify_uri = track.spotify_uri.as_deref().unwrap_or("");

            let script = format!(
                r#"
                set prevProc to ""
                tell application "System Events"
                    try
                        set prevProc to name of first application process whose frontmost is true
                    end try
                end tell

                set sRunning to false
                set mRunning to false
                set sPlaying to false
                set mPlaying to false

                if application "Spotify" is running then
                    set sRunning to true
                    try
                        tell application "Spotify" to set sPlaying to (player state is playing)
                    end try
                end if

                if application "Music" is running then
                    set mRunning to true
                    try
                        tell application "Music" to set mPlaying to (player state is playing)
                    end try
                end if

                set targetApp to ""
                -- 1. Follow current actively playing player
                if mPlaying and not sPlaying then
                    set targetApp to "music"
                else if sPlaying and not mPlaying then
                    set targetApp to "spotify"
                -- 2. Follow last player preference
                else if {last} = 2 and mRunning then
                    set targetApp to "music"
                else if {last} = 1 and sRunning then
                    set targetApp to "spotify"
                -- 3. Follow frontmost application
                else if prevProc is "Music" and mRunning then
                    set targetApp to "music"
                else if prevProc is "Spotify" and sRunning then
                    set targetApp to "spotify"
                -- 4. Follow whichever is running
                else if mRunning and not sRunning then
                    set targetApp to "music"
                else if sRunning and not mRunning then
                    set targetApp to "spotify"
                else if mRunning then
                    set targetApp to "music"
                else if sRunning then
                    set targetApp to "spotify"
                end if

                if targetApp is "music" then
                    try
                        tell application "Music"
                            play (first track whose name is "{title_escaped}")
                        end tell
                    end try
                else if targetApp is "spotify" then
                    set sWasVis to true
                    tell application "System Events"
                        try
                            set sWasVis to visible of process "Spotify"
                        end try
                    end tell

                    try
                        tell application "Spotify"
                            if "{spotify_uri}" is not "" then
                                play track "{spotify_uri}"
                            else
                                play track "spotify:search:" & "{title_escaped} {artist_query}"
                            end if
                        end tell
                    end try

                    tell application "System Events"
                        if not sWasVis then
                            try
                                set visible of process "Spotify" to false
                            end try
                        end if
                    end tell
                end if

                if prevProc is not "" and prevProc is not "Spotify" and prevProc is not "Music" then
                    delay 0.05
                    try
                        tell application "System Events" to set frontmost of process prevProc to true
                    end try
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

    fn default_seek_priming_lead_us(&self) -> u64 {
        120_000
    }
}
