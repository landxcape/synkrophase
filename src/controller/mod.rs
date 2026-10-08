use crate::error::Result;

pub mod mock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackMetadata {
    pub title: String,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub duration_us: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlaybackState {
    pub is_playing: bool,
    pub position_us: i64,
    pub rate: f32,
    pub metadata: Option<TrackMetadata>,
}

impl Default for PlaybackState {
    fn default() -> Self {
        Self {
            is_playing: false,
            position_us: 0,
            rate: 1.0,
            metadata: None,
        }
    }
}

#[async_trait::async_trait]
pub trait MediaController: Send + Sync {
    /// Read the active player's state, position, and metadata.
    async fn get_playback_state(&self) -> Result<PlaybackState>;

    /// Resume or start playback.
    async fn play(&self) -> Result<()>;

    /// Pause playback.
    async fn pause(&self) -> Result<()>;

    /// Seek to a target media position in microseconds.
    async fn seek_to(&self, position_us: i64) -> Result<()>;

    /// Adjust playback rate (e.g. 1.01 or 0.99 for smooth drift correction).
    async fn set_rate(&self, rate: f32) -> Result<()>;
}
