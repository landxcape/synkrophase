use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;
use tokio::net::UdpSocket;
use std::sync::Arc;
use crate::config::SyncConfig;
use std::net::SocketAddr;
use crate::protocol::messages::{Envelope, Message, serialize, deserialize};
use std::time::Duration;
use tokio::time::timeout;

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

    pub async fn measure_offset(&self, target_addr: SocketAddr) -> crate::error::Result<i64> {
        let mut offsets = Vec::with_capacity(self.config.clock_sample_count);
        let mut buf = [0u8; 1024];

        for _ in 0..self.config.clock_sample_count {
            let t1 = local_now();
            let req = Envelope {
                sender: self.self_id,
                payload: Message::ClockRequest { t1 },
            };
            let data = serialize(&req)?;
            
            self.socket.send_to(&data, target_addr).await?;

            if let Ok(Ok((len, addr))) = timeout(Duration::from_millis(500), self.socket.recv_from(&mut buf)).await {
                if addr == target_addr {
                    if let Ok(Envelope { payload: Message::ClockResponse { t1: rt1, t2, t3 }, .. }) = deserialize(&buf[..len]) {
                        if rt1 == t1 {
                            let t4 = local_now();
                            let offset = ((t2 as i64 - t1 as i64) + (t3 as i64 - t4 as i64)) / 2;
                            offsets.push(offset);
                        }
                    }
                }
            }
        }

        if offsets.is_empty() {
            return Err(crate::error::SynkroError::ClockSync("failed to get any valid clock responses".into()));
        }

        offsets.sort_unstable();
        let median = offsets[offsets.len() / 2];
        self.set_offset(median);
        
        Ok(median)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_measure_offset() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let dummy = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target_addr = dummy.local_addr().unwrap();
        let config = SyncConfig::default();
        let clock = ClockSync::new(socket, config, uuid::Uuid::new_v4());
        
        let err = clock.measure_offset(target_addr).await;
        assert!(err.is_err(), "should fail without a responding server");
    }
}
