use thiserror::Error;

#[derive(Error, Debug)]
pub enum SynkroError {
    #[error("clock sync failed: {0}")]
    ClockSync(String),

    #[error("network error: {0}")]
    Network(#[from] std::io::Error),

    #[error("operation requires leader role")]
    NotLeader,

    #[error("permission denied: {0}")]
    PermissionDenied(String),

    #[error("stale queue version: local={local}, received={received}")]
    StaleQueue { local: u64, received: u64 },

    #[error("media control error: {0}")]
    MediaControl(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("deserialization error: {0}")]
    Deserialization(String),

    #[error("configuration error: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, SynkroError>;
