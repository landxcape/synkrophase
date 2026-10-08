use std::sync::Arc;
use tokio::sync::RwLock;

use super::{MediaController, PlaybackState, TrackMetadata};
use crate::error::Result;

#[derive(Default, Clone)]
pub struct MockMediaController {
    state: Arc<RwLock<PlaybackState>>,
}

impl MockMediaController {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(PlaybackState::default())),
        }
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
}
