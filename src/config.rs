#[derive(Clone, Debug)]
pub struct SyncConfig {
    pub clock_sample_count: usize,
    pub clock_refresh_secs: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            clock_sample_count: 11,
            clock_refresh_secs: 30,
        }
    }
}
