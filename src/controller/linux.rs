use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::RwLock;
use zbus::names::BusName;
use zbus::zvariant::{ObjectPath, Value};
use zbus::Connection;

use super::{MediaController, PlaybackState, TrackMetadata};
use crate::error::{Result, SynkroError};
use crate::protocol::messages::TrackIdentity;

const MPRIS_PREFIX: &str = "org.mpris.MediaPlayer2.";
const DEFAULT_ACTUATION_DELAY_US: u64 = 4_000; // ~4ms typical for Linux Unix socket D-Bus

#[derive(Debug, Clone)]
pub struct LinuxMediaController {
    last_player: Arc<RwLock<Option<String>>>,
    actuation_delay_us: Arc<AtomicU64>,
}

impl Default for LinuxMediaController {
    fn default() -> Self {
        Self::new()
    }
}

impl LinuxMediaController {
    pub fn new() -> Self {
        Self {
            last_player: Arc::new(RwLock::new(None)),
            actuation_delay_us: Arc::new(AtomicU64::new(DEFAULT_ACTUATION_DELAY_US)),
        }
    }

    fn record_actuation_time(&self, elapsed_us: u64) {
        let prev = self.actuation_delay_us.load(Ordering::Relaxed);
        let updated = (prev * 8 + elapsed_us * 2) / 10;
        self.actuation_delay_us.store(updated, Ordering::Relaxed);
    }

    async fn get_connection() -> Result<Connection> {
        Connection::session()
            .await
            .map_err(|e| SynkroError::MediaControl(format!("Failed to connect to D-Bus session: {e}")))
    }

    /// List all running MPRIS media players on the session bus.
    async fn list_mpris_players(conn: &Connection) -> Result<Vec<String>> {
        let reply: Vec<String> = conn
            .call_method(
                Some("org.freedesktop.DBus"),
                "/org/freedesktop/DBus",
                Some("org.freedesktop.DBus"),
                "ListNames",
                &(),
            )
            .await
            .map_err(|e| SynkroError::MediaControl(format!("Failed to list D-Bus names: {e}")))?
            .body()
            .deserialize()
            .map_err(|e| SynkroError::MediaControl(format!("Failed to parse D-Bus names: {e}")))?;

        let players: Vec<String> = reply
            .into_iter()
            .filter(|name| name.starts_with(MPRIS_PREFIX))
            .collect();

        Ok(players)
    }

    /// Resolve the most relevant media player currently running.
    /// Priority:
    /// 1. Any player currently reporting "Playing" (Spotify prioritized if multiple are playing)
    /// 2. Previously active player (if still running)
    /// 3. Spotify (if running)
    /// 4. Any running player
    async fn find_target_player(&self, conn: &Connection) -> Result<String> {
        let players = Self::list_mpris_players(conn).await?;
        if players.is_empty() {
            return Err(SynkroError::MediaControl("No MPRIS media players running".into()));
        }

        let mut playing_players = Vec::new();
        for player in &players {
            if let Ok(status) = Self::get_playback_status(conn, player).await
                && status == "Playing"
            {
                playing_players.push(player.clone());
            }
        }

        // Priority 1: Check playing players
        if !playing_players.is_empty() {
            if let Some(spotify) = playing_players.iter().find(|p| p.ends_with(".spotify")) {
                let chosen = spotify.clone();
                *self.last_player.write().await = Some(chosen.clone());
                return Ok(chosen);
            }
            let chosen = playing_players[0].clone();
            *self.last_player.write().await = Some(chosen.clone());
            return Ok(chosen);
        }

        // Priority 2: Last active player if still running
        {
            let last = self.last_player.read().await;
            if let Some(ref name) = *last
                && players.contains(name)
            {
                return Ok(name.clone());
            }
        }

        // Priority 3: Spotify if present
        if let Some(spotify) = players.iter().find(|p| p.ends_with(".spotify")) {
            let chosen = spotify.clone();
            *self.last_player.write().await = Some(chosen.clone());
            return Ok(chosen);
        }

        // Priority 4: First available player
        let chosen = players[0].clone();
        *self.last_player.write().await = Some(chosen.clone());
        Ok(chosen)
    }

    async fn get_playback_status(conn: &Connection, player_service: &str) -> Result<String> {
        let dest: BusName<'_> = player_service
            .try_into()
            .map_err(|e| SynkroError::MediaControl(format!("Invalid bus name {player_service}: {e}")))?;

        let reply = conn
            .call_method(
                Some(dest),
                "/org/mpris/MediaPlayer2",
                Some("org.freedesktop.DBus.Properties"),
                "Get",
                &("org.mpris.MediaPlayer2.Player", "PlaybackStatus"),
            )
            .await
            .map_err(|e| SynkroError::MediaControl(format!("Failed to get PlaybackStatus: {e}")))?;

        let value: zbus::zvariant::OwnedValue = reply
            .body()
            .deserialize()
            .map_err(|e| SynkroError::MediaControl(format!("Failed to parse PlaybackStatus variant: {e}")))?;

        let val: Value<'_> = value.into();
        match val {
            Value::Str(s) => Ok(s.to_string()),
            _ => Ok("Stopped".into()),
        }
    }

    async fn get_property_value(conn: &Connection, player_service: &str, property: &str) -> Result<Value<'static>> {
        let dest: BusName<'_> = player_service
            .try_into()
            .map_err(|e| SynkroError::MediaControl(format!("Invalid bus name {player_service}: {e}")))?;

        let reply = conn
            .call_method(
                Some(dest),
                "/org/mpris/MediaPlayer2",
                Some("org.freedesktop.DBus.Properties"),
                "Get",
                &("org.mpris.MediaPlayer2.Player", property),
            )
            .await
            .map_err(|e| SynkroError::MediaControl(format!("Failed to get {property}: {e}")))?;

        let value: zbus::zvariant::OwnedValue = reply
            .body()
            .deserialize()
            .map_err(|e| SynkroError::MediaControl(format!("Failed to parse {property}: {e}")))?;

        Ok(value.into())
    }

    async fn set_property_value(conn: &Connection, player_service: &str, property: &str, val: Value<'_>) -> Result<()> {
        let dest: BusName<'_> = player_service
            .try_into()
            .map_err(|e| SynkroError::MediaControl(format!("Invalid bus name {player_service}: {e}")))?;

        conn.call_method(
            Some(dest),
            "/org/mpris/MediaPlayer2",
            Some("org.freedesktop.DBus.Properties"),
            "Set",
            &("org.mpris.MediaPlayer2.Player", property, val),
        )
        .await
        .map_err(|e| SynkroError::MediaControl(format!("Failed to set {property}: {e}")))?;

        Ok(())
    }

    async fn call_player_method(conn: &Connection, player_service: &str, method: &str) -> Result<()> {
        let dest: BusName<'_> = player_service
            .try_into()
            .map_err(|e| SynkroError::MediaControl(format!("Invalid bus name {player_service}: {e}")))?;

        conn.call_method(
            Some(dest),
            "/org/mpris/MediaPlayer2",
            Some("org.mpris.MediaPlayer2.Player"),
            method,
            &(),
        )
        .await
        .map_err(|e| SynkroError::MediaControl(format!("Failed to call {method}: {e}")))?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl MediaController for LinuxMediaController {
    async fn get_playback_state(&self) -> Result<PlaybackState> {
        let conn = Self::get_connection().await?;
        let target = self.find_target_player(&conn).await?;

        // 1. PlaybackStatus
        let status = Self::get_playback_status(&conn, &target).await.unwrap_or_else(|_| "Stopped".into());
        let is_playing = status == "Playing";

        // 2. Position (in microseconds for MPRIS)
        let position_us = match Self::get_property_value(&conn, &target, "Position").await {
            Ok(Value::I64(pos)) => pos,
            Ok(Value::U64(pos)) => pos as i64,
            _ => 0,
        };

        // 3. Playback Rate
        let rate = match Self::get_property_value(&conn, &target, "Rate").await {
            Ok(Value::F64(r)) => r as f32,
            _ => 1.0,
        };

        // 4. Metadata dictionary
        let metadata = match Self::get_property_value(&conn, &target, "Metadata").await {
            Ok(Value::Dict(dict)) => {
                let mut meta_map: HashMap<String, Value<'static>> = HashMap::new();
                for (k, v) in dict.into_iter() {
                    if let Value::Str(key_str) = k {
                        meta_map.insert(key_str.to_string(), v);
                    }
                }

                let title = meta_map
                    .get("xesam:title")
                    .and_then(|v| match v {
                        Value::Str(s) => Some(s.to_string()),
                        _ => None,
                    })
                    .unwrap_or_default();

                let artist = meta_map.get("xesam:artist").and_then(|v| match v {
                    Value::Array(arr) => {
                        let artists: Vec<String> = arr
                            .iter()
                            .filter_map(|item| match item {
                                Value::Str(s) => Some(s.to_string()),
                                _ => None,
                            })
                            .collect();
                        if artists.is_empty() {
                            None
                        } else {
                            Some(artists.join(", "))
                        }
                    }
                    Value::Str(s) => Some(s.to_string()),
                    _ => None,
                });

                let album = meta_map.get("xesam:album").and_then(|v| match v {
                    Value::Str(s) => Some(s.to_string()),
                    _ => None,
                });

                let duration_us = meta_map.get("mpris:length").and_then(|v| match v {
                    Value::I64(len) if *len >= 0 => Some(*len as u64),
                    Value::U64(len) => Some(*len),
                    _ => None,
                });

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
            _ => None,
        };

        Ok(PlaybackState {
            is_playing,
            position_us,
            rate,
            metadata,
        })
    }

    async fn get_track_identity(&self) -> Result<Option<TrackIdentity>> {
        let state = self.get_playback_state().await?;
        let meta = match state.metadata {
            Some(m) => m,
            None => return Ok(None),
        };

        let conn = Self::get_connection().await?;
        let target = self.find_target_player(&conn).await?;

        // Extract spotify:track:xxx or url if available
        let mut spotify_uri = None;
        if let Ok(Value::Dict(dict)) = Self::get_property_value(&conn, &target, "Metadata").await {
            for (k, v) in dict.into_iter() {
                if let Value::Str(ref key_str) = k
                    && (key_str == "xesam:url" || key_str == "mpris:trackid")
                    && let Value::Str(val_str) = v
                    && (val_str.contains("spotify:track:") || val_str.starts_with("https://open.spotify.com/track/"))
                {
                    spotify_uri = Some(val_str.to_string());
                    break;
                }
            }
        }

        Ok(Some(TrackIdentity {
            title: meta.title,
            artist: meta.artist,
            album: meta.album,
            duration_us: meta.duration_us,
            spotify_uri,
            apple_music_id: None,
        }))
    }

    async fn load_track(&self, track: &TrackIdentity) -> Result<()> {
        let conn = Self::get_connection().await?;
        let target = self.find_target_player(&conn).await?;

        if let Some(ref uri) = track.spotify_uri {
            let start = Instant::now();
            let dest: BusName<'_> = target.as_str()
                .try_into()
                .map_err(|e| SynkroError::MediaControl(format!("Invalid bus name {target}: {e}")))?;

            let _ = conn
                .call_method(
                    Some(dest),
                    "/org/mpris/MediaPlayer2",
                    Some("org.mpris.MediaPlayer2.Player"),
                    "OpenUri",
                    &(uri.as_str()),
                )
                .await;
            self.record_actuation_time(start.elapsed().as_micros() as u64);
        }

        Ok(())
    }

    async fn play(&self) -> Result<()> {
        let start = Instant::now();
        let conn = Self::get_connection().await?;
        let target = self.find_target_player(&conn).await?;
        Self::call_player_method(&conn, &target, "Play").await?;
        self.record_actuation_time(start.elapsed().as_micros() as u64);
        Ok(())
    }

    async fn pause(&self) -> Result<()> {
        let start = Instant::now();
        let conn = Self::get_connection().await?;
        let target = self.find_target_player(&conn).await?;
        Self::call_player_method(&conn, &target, "Pause").await?;
        self.record_actuation_time(start.elapsed().as_micros() as u64);
        Ok(())
    }

    async fn seek_to(&self, position_us: i64) -> Result<()> {
        let start = Instant::now();
        let conn = Self::get_connection().await?;
        let target = self.find_target_player(&conn).await?;

        // In MPRIS specification, SetPosition takes (o: TrackId, x: Position)
        let mut track_id = "/org/mpris/MediaPlayer2/CurrentTrack".to_string();
        if let Ok(Value::Dict(dict)) = Self::get_property_value(&conn, &target, "Metadata").await {
            for (k, v) in dict.into_iter() {
                if let Value::Str(ref key_str) = k
                    && key_str == "mpris:trackid"
                {
                    if let Value::ObjectPath(p) = v {
                        track_id = p.to_string();
                        break;
                    } else if let Value::Str(s) = v {
                        track_id = s.to_string();
                        break;
                    }
                }
            }
        }

        let obj_path = match ObjectPath::try_from(track_id.as_str()) {
            Ok(p) => p,
            Err(_) => ObjectPath::try_from("/org/mpris/MediaPlayer2/CurrentTrack").unwrap(),
        };

        let dest: BusName<'_> = target.as_str()
            .try_into()
            .map_err(|e| SynkroError::MediaControl(format!("Invalid bus name {target}: {e}")))?;

        conn.call_method(
            Some(dest),
            "/org/mpris/MediaPlayer2",
            Some("org.mpris.MediaPlayer2.Player"),
            "SetPosition",
            &(obj_path, position_us),
        )
        .await
        .map_err(|e| SynkroError::MediaControl(format!("Failed to call SetPosition: {e}")))?;

        self.record_actuation_time(start.elapsed().as_micros() as u64);
        Ok(())
    }

    async fn set_rate(&self, rate: f32) -> Result<()> {
        let conn = Self::get_connection().await?;
        let target = self.find_target_player(&conn).await?;
        Self::set_property_value(&conn, &target, "Rate", Value::F64(rate as f64)).await
    }

    async fn next_track(&self) -> Result<()> {
        let start = Instant::now();
        let conn = Self::get_connection().await?;
        let target = self.find_target_player(&conn).await?;
        Self::call_player_method(&conn, &target, "Next").await?;
        self.record_actuation_time(start.elapsed().as_micros() as u64);
        Ok(())
    }

    async fn previous_track(&self) -> Result<()> {
        let start = Instant::now();
        let conn = Self::get_connection().await?;
        let target = self.find_target_player(&conn).await?;
        Self::call_player_method(&conn, &target, "Previous").await?;
        self.record_actuation_time(start.elapsed().as_micros() as u64);
        Ok(())
    }

    async fn get_volume(&self) -> Result<u8> {
        let conn = Self::get_connection().await?;
        let target = self.find_target_player(&conn).await?;
        let vol_value = Self::get_property_value(&conn, &target, "Volume").await?;
        match vol_value {
            Value::F64(v) => Ok((v.clamp(0.0, 1.0) * 100.0).round() as u8),
            _ => Ok(100),
        }
    }

    async fn set_volume(&self, volume: u8) -> Result<()> {
        let conn = Self::get_connection().await?;
        let target = self.find_target_player(&conn).await?;
        let vol_f64 = (volume.min(100) as f64) / 100.0;
        Self::set_property_value(&conn, &target, "Volume", Value::F64(vol_f64)).await
    }

    fn estimated_actuation_delay_us(&self) -> u64 {
        self.actuation_delay_us.load(Ordering::Relaxed)
    }
}
