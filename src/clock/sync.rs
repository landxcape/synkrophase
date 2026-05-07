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
