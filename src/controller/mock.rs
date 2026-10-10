use std::sync::Arc;
use tokio::sync::RwLock;

use super::{MediaController, PlaybackState, TrackMetadata};
use crate::error::Result;

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default, Clone)]
pub struct MockMediaController {
    state: Arc<RwLock<PlaybackState>>,
    actuation_delay_us: Arc<AtomicU64>,
}

impl MockMediaController {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(PlaybackState::default())),
            actuation_delay_us: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn set_actuation_delay_us(&self, delay_us: u64) {
        self.actuation_delay_us.store(delay_us, Ordering::Relaxed);
    }

    pub async fn set_track(&self, metadata: Option<TrackMetadata>) {
        let mut state = self.state.write().await;
        state.metadata = metadata;
    }

    pub async fn set_position(&self, position_us: i64) {
        let mut state = self.state.write().await;
        state.position_us = position_us;
    }
}

#[async_trait::async_trait]
impl MediaController for MockMediaController {
    async fn get_playback_state(&self) -> Result<PlaybackState> {
        Ok(self.state.read().await.clone())
    }

    async fn get_track_identity(&self) -> Result<Option<crate::protocol::messages::TrackIdentity>> {
        let state = self.state.read().await;
        Ok(state
            .metadata
            .as_ref()
            .map(|m| crate::protocol::messages::TrackIdentity {
                title: m.title.clone(),
                artist: m.artist.clone(),
                album: m.album.clone(),
                spotify_uri: Some(format!("spotify:track:{}", m.title)),
                apple_music_id: None,
                duration_us: m.duration_us,
            }))
    }

    async fn load_track(&self, track: &crate::protocol::messages::TrackIdentity) -> Result<()> {
        let mut state = self.state.write().await;
        state.is_playing = true;
        state.position_us = 0;
        state.metadata = Some(TrackMetadata {
            title: track.title.clone(),
            artist: track.artist.clone(),
            album: track.album.clone(),
            duration_us: track.duration_us,
        });
        Ok(())
    }

    async fn play(&self) -> Result<()> {
        let mut state = self.state.write().await;
        state.is_playing = true;
        Ok(())
    }

    async fn pause(&self) -> Result<()> {
        let mut state = self.state.write().await;
        state.is_playing = false;
        Ok(())
    }

    async fn seek_to(&self, position_us: i64) -> Result<()> {
        let mut state = self.state.write().await;
        state.position_us = position_us;
        Ok(())
    }

    async fn set_rate(&self, rate: f32) -> Result<()> {
        let mut state = self.state.write().await;
        state.rate = rate;
        Ok(())
    }

    async fn next_track(&self) -> Result<()> {
        let mut state = self.state.write().await;
        state.position_us = 0;
        Ok(())
    }

    async fn previous_track(&self) -> Result<()> {
        let mut state = self.state.write().await;
        state.position_us = 0;
        Ok(())
    }

    async fn get_volume(&self) -> Result<u8> {
        Ok(50)
    }

    async fn set_volume(&self, _volume: u8) -> Result<()> {
        Ok(())
    }

    fn estimated_actuation_delay_us(&self) -> u64 {
        self.actuation_delay_us.load(Ordering::Relaxed)
    }
}
