use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;
use tokio::net::UdpSocket;
use std::sync::Arc;
use crate::config::SyncConfig;

lazy_static::lazy_static! {
    static ref EPOCH: Instant = Instant::now();
}

pub fn local_now() -> u64 {
    Instant::now().duration_since(*EPOCH).as_micros() as u64
}

pub struct ClockSync {
    offset_us: AtomicI64,
    socket: Arc<UdpSocket>,
    config: SyncConfig,
    self_id: uuid::Uuid,
}

impl ClockSync {
    pub fn new(socket: UdpSocket, config: SyncConfig, self_id: uuid::Uuid) -> Self {
        Self {
            offset_us: AtomicI64::new(0),
            socket: Arc::new(socket),
            config,
            self_id,
        }
    }

    pub fn offset(&self) -> i64 {
        self.offset_us.load(Ordering::Acquire)
    }

    pub fn reference_now(&self) -> u64 {
        let now = local_now() as i64;
        (now + self.offset()) as u64
    }
    
    pub fn set_offset(&self, offset: i64) {
        self.offset_us.store(offset, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_clock_sync_offset() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let config = SyncConfig::default();
        let id = uuid::Uuid::new_v4();
        
        let clock_sync = ClockSync::new(socket, config, id);
        
        assert_eq!(clock_sync.offset(), 0);
        clock_sync.set_offset(5000);
        assert_eq!(clock_sync.offset(), 5000);
        
        let local = local_now();
        let ref_now = clock_sync.reference_now();
        
        assert!(ref_now >= local + 5000);
    }
}
