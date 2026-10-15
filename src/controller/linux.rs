use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::RwLock;
use zbus::Connection;
use zbus::names::BusName;
use zbus::zvariant::{ObjectPath, Value};

use super::{MediaController, PlaybackState, TrackMetadata};
use crate::error::{Result, SynkroError};
use crate::protocol::messages::TrackIdentity;

const MPRIS_PREFIX: &str = "org.mpris.MediaPlayer2.";
const DEFAULT_ACTUATION_DELAY_US: u64 = 4_000; // ~4ms typical for Linux Unix socket D-Bus

#[derive(Debug, Clone)]
pub struct LinuxMediaController {
    last_player: Arc<RwLock<Option<String>>>,
    actuation_delay_us: Arc<AtomicU64>,
    supports_rate: Arc<AtomicBool>,
}

fn unwrap_value_ref<'a>(val: &'a Value<'a>) -> &'a Value<'a> {
    let mut cur = val;
    while let Value::Value(inner) = cur {
        cur = inner.as_ref();
    }
    cur
}

fn unwrap_value<'a>(val: Value<'a>) -> Value<'a> {
    let mut cur = val;
    while let Value::Value(inner) = cur {
        cur = *inner;
    }
    cur
}

fn extract_string(val: &Value<'_>) -> Option<String> {
    match unwrap_value_ref(val) {
        Value::Str(s) => Some(s.to_string()),
        _ => None,
    }
}

fn extract_string_array(val: &Value<'_>) -> Vec<String> {
    match unwrap_value_ref(val) {
        Value::Array(arr) => arr.iter().filter_map(extract_string).collect(),
        Value::Str(s) => vec![s.to_string()],
        _ => Vec::new(),
    }
}

fn extract_i64(val: &Value<'_>) -> Option<i64> {
    match unwrap_value_ref(val) {
        Value::I64(v) => Some(*v),
        Value::U64(v) => Some(*v as i64),
        Value::I32(v) => Some(*v as i64),
        Value::U32(v) => Some(*v as i64),
        _ => None,
    }
}

fn extract_u64(val: &Value<'_>) -> Option<u64> {
    match unwrap_value_ref(val) {
        Value::U64(v) => Some(*v),
        Value::I64(v) if *v >= 0 => Some(*v as u64),
        Value::U32(v) => Some(*v as u64),
        Value::I32(v) if *v >= 0 => Some(*v as u64),
        _ => None,
    }
}

fn extract_track_id(val: &Value<'_>) -> Option<String> {
    match unwrap_value_ref(val) {
        Value::ObjectPath(p) => Some(p.to_string()),
        Value::Str(s) => Some(s.to_string()),
        _ => None,
    }
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
            supports_rate: Arc::new(AtomicBool::new(false)),
        }
    }

    fn record_actuation_time(&self, elapsed_us: u64) {
        let prev = self.actuation_delay_us.load(Ordering::Relaxed);
        let updated = (prev * 8 + elapsed_us * 2) / 10;
        self.actuation_delay_us.store(updated, Ordering::Relaxed);
    }

    async fn get_connection() -> Result<Connection> {
        Connection::session().await.map_err(|e| {
            SynkroError::MediaControl(format!("Failed to connect to D-Bus session: {e}"))
        })
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
            return Err(SynkroError::MediaControl(
                "No MPRIS media players running".into(),
            ));
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
        let chosen = if !playing_players.is_empty() {
            if let Some(spotify) = playing_players.iter().find(|p| p.contains("spotify")) {
                spotify.clone()
            } else {
                playing_players[0].clone()
            }
        } else if let Some(ref name) = *self.last_player.read().await
            && players.contains(name)
        {
            name.clone()
        } else if let Some(spotify) = players.iter().find(|p| p.contains("spotify")) {
            spotify.clone()
        } else {
            players[0].clone()
        };

        *self.last_player.write().await = Some(chosen.clone());
        let rate_supported = Self::check_player_rate_support(conn, &chosen).await;
        self.supports_rate.store(rate_supported, Ordering::Relaxed);
        Ok(chosen)
    }

    /// Query the player's MPRIS MinimumRate and MaximumRate properties.
    /// A player supports Synkrophase micro-rate adjustments if it can accommodate [0.99, 1.01].
    async fn check_player_rate_support(conn: &Connection, player_service: &str) -> bool {
        let min_rate = match Self::get_property_value(conn, player_service, "MinimumRate").await {
            Ok(val) => match unwrap_value_ref(&val) {
                Value::F64(v) => *v,
                _ => return false,
            },
            _ => return false,
        };
        let max_rate = match Self::get_property_value(conn, player_service, "MaximumRate").await {
            Ok(val) => match unwrap_value_ref(&val) {
                Value::F64(v) => *v,
                _ => return false,
            },
            _ => return false,
        };
        (max_rate - min_rate) > 0.01 && min_rate <= 0.99 && max_rate >= 1.01
    }

    async fn get_playback_status(conn: &Connection, player_service: &str) -> Result<String> {
        let dest: BusName<'_> = player_service.try_into().map_err(|e| {
            SynkroError::MediaControl(format!("Invalid bus name {player_service}: {e}"))
        })?;

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

        let value: zbus::zvariant::OwnedValue = reply.body().deserialize().map_err(|e| {
            SynkroError::MediaControl(format!("Failed to parse PlaybackStatus variant: {e}"))
        })?;

        let val: Value<'_> = value.into();
        let status = extract_string(&val).unwrap_or_else(|| "Stopped".into());
        Ok(status)
    }

    async fn get_property_value(
        conn: &Connection,
        player_service: &str,
        property: &str,
    ) -> Result<Value<'static>> {
        let dest: BusName<'_> = player_service.try_into().map_err(|e| {
            SynkroError::MediaControl(format!("Invalid bus name {player_service}: {e}"))
        })?;

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

    async fn set_property_value(
        conn: &Connection,
        player_service: &str,
        property: &str,
        val: Value<'_>,
    ) -> Result<()> {
        let dest: BusName<'_> = player_service.try_into().map_err(|e| {
            SynkroError::MediaControl(format!("Invalid bus name {player_service}: {e}"))
        })?;

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

    async fn call_player_method(
        conn: &Connection,
        player_service: &str,
        method: &str,
    ) -> Result<()> {
        let dest: BusName<'_> = player_service.try_into().map_err(|e| {
            SynkroError::MediaControl(format!("Invalid bus name {player_service}: {e}"))
        })?;

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
        let status = Self::get_playback_status(&conn, &target)
            .await
            .unwrap_or_else(|_| "Stopped".into());
        let is_playing = status == "Playing";

        // 2. Position (in microseconds for MPRIS)
        let position_us = match Self::get_property_value(&conn, &target, "Position").await {
            Ok(val) => extract_i64(&val).unwrap_or(0),
            _ => 0,
        };

        // 3. Playback Rate
        let rate = match Self::get_property_value(&conn, &target, "Rate").await {
            Ok(val) => match unwrap_value_ref(&val) {
                Value::F64(r) => *r as f32,
                _ => 1.0,
            },
            _ => 1.0,
        };

        // 4. Metadata dictionary
        let metadata = match Self::get_property_value(&conn, &target, "Metadata").await {
            Ok(val) => {
                let unwrapped = unwrap_value(val);
                if let Value::Dict(dict) = unwrapped {
                    let mut meta_map: HashMap<String, Value<'static>> = HashMap::new();
                    for (k, v) in dict.into_iter() {
                        if let Some(key_str) = extract_string(&k) {
                            meta_map.insert(key_str, v);
                        }
                    }

                    let title = meta_map
                        .get("xesam:title")
                        .and_then(extract_string)
                        .unwrap_or_default();

                    let artist = meta_map
                        .get("xesam:artist")
                        .map(extract_string_array)
                        .and_then(|arr| {
                            if arr.is_empty() {
                                None
                            } else {
                                Some(arr.join(", "))
                            }
                        });

                    let album = meta_map.get("xesam:album").and_then(extract_string);

                    let duration_us = meta_map.get("mpris:length").and_then(extract_u64);

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
                } else {
                    None
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
        if let Ok(val) = Self::get_property_value(&conn, &target, "Metadata").await {
            let unwrapped = unwrap_value(val);
            if let Value::Dict(dict) = unwrapped {
                for (k, v) in dict.into_iter() {
                    if let Some(key_str) = extract_string(&k)
                        && (key_str == "xesam:url" || key_str == "mpris:trackid")
                        && let Some(val_str) = extract_string(&v).or_else(|| extract_track_id(&v))
                        && (val_str.contains("spotify:track:")
                            || val_str.starts_with("https://open.spotify.com/track/"))
                    {
                        spotify_uri = Some(val_str);
                        break;
                    }
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
            let dest: BusName<'_> = target.as_str().try_into().map_err(|e| {
                SynkroError::MediaControl(format!("Invalid bus name {target}: {e}"))
            })?;

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
        if let Ok(val) = Self::get_property_value(&conn, &target, "Metadata").await {
            let unwrapped = unwrap_value(val);
            if let Value::Dict(dict) = unwrapped {
                for (k, v) in dict.into_iter() {
                    if let Some(key_str) = extract_string(&k)
                        && key_str == "mpris:trackid"
                        && let Some(id_str) = extract_track_id(&v)
                    {
                        track_id = id_str;
                        break;
                    }
                }
            }
        }

        let obj_path = match ObjectPath::try_from(track_id.as_str()) {
            Ok(p) => p,
            Err(_) => ObjectPath::try_from("/org/mpris/MediaPlayer2/CurrentTrack").unwrap(),
        };

        let dest: BusName<'_> = target
            .as_str()
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
        match unwrap_value_ref(&vol_value) {
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

    fn supports_rate_control(&self) -> bool {
        self.supports_rate.load(Ordering::Relaxed)
    }

    fn default_seek_priming_lead_us(&self) -> u64 {
        120_000
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unwrap_value_nested_variants() {
        let leaf = Value::Str("test_string".into());
        let wrapped1 = Value::Value(Box::new(leaf));
        let wrapped2 = Value::Value(Box::new(wrapped1));
        let wrapped3 = Value::Value(Box::new(wrapped2));

        assert_eq!(extract_string(&wrapped3), Some("test_string".to_string()));
    }

    #[test]
    fn test_extract_string_array() {
        let item1 = Value::Value(Box::new(Value::Str("Artist 1".into())));
        let item2 = Value::Value(Box::new(Value::Str("Artist 2".into())));
        let arr = Value::Value(Box::new(Value::Array(vec![item1, item2].into())));

        let result = extract_string_array(&arr);
        assert_eq!(result, vec!["Artist 1".to_string(), "Artist 2".to_string()]);
    }

    #[test]
    fn test_extract_i64_and_u64() {
        let val_i64 = Value::Value(Box::new(Value::I64(42_000_000)));
        let val_u64 = Value::Value(Box::new(Value::U64(180_000_000)));

        assert_eq!(extract_i64(&val_i64), Some(42_000_000));
        assert_eq!(extract_u64(&val_u64), Some(180_000_000));
    }

    #[test]
    fn test_extract_track_id_object_path() {
        let path = ObjectPath::try_from("/org/mpris/MediaPlayer2/Track/42").unwrap();
        let val_path = Value::Value(Box::new(Value::ObjectPath(path)));

        assert_eq!(
            extract_track_id(&val_path),
            Some("/org/mpris/MediaPlayer2/Track/42".to_string())
        );
    }

    #[test]
    fn test_mpris_synthetic_metadata_parsing() {
        let title_val = Value::Value(Box::new(Value::Str("Never Gonna Give You Up".into())));
        let artist_item = Value::Value(Box::new(Value::Str("Rick Astley".into())));
        let artist_val = Value::Value(Box::new(Value::Array(vec![artist_item].into())));
        let album_val = Value::Value(Box::new(Value::Str("Whenever You Need Somebody".into())));
        let len_val = Value::Value(Box::new(Value::I64(213_000_000)));
        let trackid_path =
            ObjectPath::try_from("/com/spotify/track/4cOdK2wGLETKBW3PvgPWqT").unwrap();
        let trackid_val = Value::Value(Box::new(Value::ObjectPath(trackid_path)));

        let s_sig = zbus::zvariant::Signature::from_bytes(b"s").unwrap();
        let v_sig = zbus::zvariant::Signature::from_bytes(b"v").unwrap();
        let mut dict = zbus::zvariant::Dict::new(&s_sig, &v_sig);
        dict.append(Value::Str("xesam:title".into()), title_val)
            .unwrap();
        dict.append(Value::Str("xesam:artist".into()), artist_val)
            .unwrap();
        dict.append(Value::Str("xesam:album".into()), album_val)
            .unwrap();
        dict.append(Value::Str("mpris:length".into()), len_val)
            .unwrap();
        dict.append(Value::Str("mpris:trackid".into()), trackid_val)
            .unwrap();

        let meta_dict_value = Value::Value(Box::new(Value::Dict(dict)));
        let unwrapped = unwrap_value(meta_dict_value);

        if let Value::Dict(dict) = unwrapped {
            let mut meta_map: HashMap<String, Value<'static>> = HashMap::new();
            for (k, v) in dict.into_iter() {
                if let Some(key_str) = extract_string(&k) {
                    meta_map.insert(key_str, v);
                }
            }

            let title = meta_map
                .get("xesam:title")
                .and_then(extract_string)
                .unwrap_or_default();
            assert_eq!(title, "Never Gonna Give You Up");

            let artist = meta_map
                .get("xesam:artist")
                .map(extract_string_array)
                .and_then(|arr| {
                    if arr.is_empty() {
                        None
                    } else {
                        Some(arr.join(", "))
                    }
                });
            assert_eq!(artist, Some("Rick Astley".to_string()));

            let album = meta_map.get("xesam:album").and_then(extract_string);
            assert_eq!(album, Some("Whenever You Need Somebody".to_string()));

            let duration_us = meta_map.get("mpris:length").and_then(extract_u64);
            assert_eq!(duration_us, Some(213_000_000));

            let track_id = meta_map.get("mpris:trackid").and_then(extract_track_id);
            assert_eq!(
                track_id,
                Some("/com/spotify/track/4cOdK2wGLETKBW3PvgPWqT".to_string())
            );
        } else {
            panic!("Expected Value::Dict after unwrapping");
        }
    }
}
