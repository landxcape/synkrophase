use std::path::PathBuf;

use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct DeviceConfig {
    pub device_id: Uuid,
    pub name: String,
    pub data_dir: PathBuf,
}

#[derive(Clone, Debug)]
pub struct SyncConfig {
    pub clock_sample_count: usize,
    pub clock_refresh_secs: u64,
    pub anchor_broadcast_secs: u64,
    pub heartbeat_interval_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub zone1_threshold_ms: u64,
    pub zone2_threshold_ms: u64,
    pub rate_fast: f32,
    pub rate_slow: f32,
    pub sync_eval_interval_ms: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            clock_sample_count: 11,
            clock_refresh_secs: 30,
            anchor_broadcast_secs: 1,
            heartbeat_interval_ms: 1_000,
            heartbeat_timeout_ms: 3_000,
            zone1_threshold_ms: 5,
            zone2_threshold_ms: 200,
            rate_fast: 1.05,
            rate_slow: 0.95,
            sync_eval_interval_ms: 200,
        }
    }
}
