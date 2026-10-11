use crate::config::SyncConfig;
use crate::protocol::messages::{Envelope, Message, deserialize, serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::timeout;

lazy_static::lazy_static! {
    static ref BASE_INSTANT: std::time::Instant = std::time::Instant::now();
}

pub fn local_now() -> u64 {
    std::time::Instant::now()
        .duration_since(*BASE_INSTANT)
        .as_micros() as u64
}

pub struct ClockSync {
    offset_us: AtomicI64,
    residual_offset_us: AtomicI64,
    is_synchronized: std::sync::atomic::AtomicBool,
    socket: Arc<UdpSocket>,
    config: SyncConfig,
    self_id: uuid::Uuid,
    response_tx: tokio::sync::broadcast::Sender<(u64, u64, u64, SocketAddr)>,
    receiver_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for ClockSync {
    fn drop(&mut self) {
        if let Some(handle) = &self.receiver_handle {
            handle.abort();
        }
    }
}

impl ClockSync {
    pub fn new(socket: UdpSocket, config: SyncConfig, self_id: uuid::Uuid) -> Self {
        let (response_tx, _) = tokio::sync::broadcast::channel(128);
        let socket = Arc::new(socket);

        let s_clone = Arc::clone(&socket);
        let tx_clone = response_tx.clone();
        let receiver_handle = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            Some(handle.spawn(async move {
                let mut buf = [0u8; 1024];
                while let Ok((len, src)) = s_clone.recv_from(&mut buf).await {
                    let t2 = local_now();
                    if let Ok(Envelope { payload, .. }) = deserialize(&buf[..len]) {
                        match payload {
                            Message::ClockRequest { t1 } => {
                                let t3 = local_now();
                                let resp = Envelope {
                                    sender: self_id,
                                    payload: Message::ClockResponse { t1, t2, t3 },
                                };
                                if let Ok(data) = serialize(&resp) {
                                    let _ = s_clone.send_to(&data, src).await;
                                }
                            }
                            Message::ClockResponse { t1, t2, t3 } => {
                                let _ = tx_clone.send((t1, t2, t3, src));
                            }
                            _ => {}
                        }
                    }
                }
            }))
        } else {
            None
        };

        Self {
            offset_us: AtomicI64::new(0),
            residual_offset_us: AtomicI64::new(0),
            is_synchronized: std::sync::atomic::AtomicBool::new(false),
            socket,
            config,
            self_id,
            response_tx,
            receiver_handle,
        }
    }

    pub fn offset(&self) -> i64 {
        self.offset_us.load(Ordering::Acquire)
    }

    pub fn residual_offset(&self) -> i64 {
        self.residual_offset_us.load(Ordering::Acquire)
    }

    pub fn reference_now(&self) -> u64 {
        let now = local_now() as i64;
        (now + self.offset()) as u64
    }

    pub fn set_offset(&self, offset: i64) {
        self.offset_us.store(offset, Ordering::Release);
    }

    pub async fn measure_offset(&self, target_addr: SocketAddr) -> crate::error::Result<i64> {
        let mut samples = Vec::with_capacity(self.config.clock_sample_count);
        let mut rx = self.response_tx.subscribe();

        for _ in 0..self.config.clock_sample_count {
            let t1 = local_now();
            let req = Envelope {
                sender: self.self_id,
                payload: Message::ClockRequest { t1 },
            };
            let data = serialize(&req)?;

            self.socket.send_to(&data, target_addr).await?;

            let recv_result = timeout(Duration::from_millis(500), async {
                loop {
                    match rx.recv().await {
                        Ok((rt1, t2, t3, src)) if src.port() == target_addr.port() && rt1 == t1 => {
                            return Ok((rt1, t2, t3));
                        }
                        Ok(_) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(e) => return Err(crate::error::SynkroError::ClockSync(e.to_string())),
                    }
                }
            })
            .await;

            if let Ok(Ok((rt1, t2, t3))) = recv_result {
                let t4 = local_now();
                let offset = ((t2 as i64 - rt1 as i64) + (t3 as i64 - t4 as i64)) / 2;
                // Calculate Round-Trip Time (RTT): Total round trip minus remote processing time
                let total_delay = t4.saturating_sub(rt1);
                let remote_processing = t3.saturating_sub(t2);
                let rtt = total_delay.saturating_sub(remote_processing);
                samples.push((offset, rtt));
            }
        }

        if samples.is_empty() {
            return Err(crate::error::SynkroError::ClockSync(
                "failed to get any valid clock responses".into(),
            ));
        }

        // Standard NTP/PTP best practice: sort samples by RTT (lowest latency has the least network jitter/asymmetry)
        // and evaluate the median offset among the best 50% lowest-RTT samples.
        samples.sort_by_key(|&(_, rtt)| rtt);
        let best_count = samples.len().div_ceil(2).max(1);
        let mut best_offsets: Vec<i64> =
            samples[..best_count].iter().map(|&(off, _)| off).collect();
        best_offsets.sort_unstable();
        let median = best_offsets[best_offsets.len() / 2];
        if self.is_synchronized.swap(true, Ordering::AcqRel) {
            let prev_offset = self.offset();
            let residual = median - prev_offset;
            self.residual_offset_us.store(residual, Ordering::Release);
            self.set_offset(median);
        } else {
            self.residual_offset_us.store(0, Ordering::Release);
            self.set_offset(median);
        }

        Ok(median)
    }

    pub async fn run_responder(&self) -> crate::error::Result<()> {
        // The single dedicated receiver and responder loop runs continuously in the background
        // upon instantiation. Keep this future alive so existing handler task lifecycles remain valid.
        std::future::pending::<()>().await;
        Ok(())
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

    #[tokio::test]
    async fn test_clock_sync_integration() {
        let server_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let server_addr = server_socket.local_addr().unwrap();

        let server_clone = server_socket.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            while let Ok((len, src)) = server_clone.recv_from(&mut buf).await {
                let t2 = local_now();
                if let Ok(Envelope {
                    payload: Message::ClockRequest { t1 },
                    sender,
                }) = deserialize(&buf[..len])
                {
                    let t3 = local_now();
                    let resp = Envelope {
                        sender, // bounce back sender for simplicity in test
                        payload: Message::ClockResponse { t1, t2, t3 },
                    };
                    let out = serialize(&resp).unwrap();
                    server_clone.send_to(&out, src).await.unwrap();
                }
            }
        });

        let client_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let clock = ClockSync::new(client_socket, SyncConfig::default(), uuid::Uuid::new_v4());

        let offset = clock
            .measure_offset(server_addr)
            .await
            .expect("should measure successfully");
        // Because they run on the same machine/clock, offset should be effectively 0 (within <1ms / 1000us)
        assert!(offset.abs() < 1000, "offset {} is not < 1ms", offset);
        assert_eq!(clock.residual_offset(), 0, "first residual offset should be 0");

        // Second measurement should track residual offset relative to first
        let _ = clock
            .measure_offset(server_addr)
            .await
            .expect("second measurement should succeed");
        assert!(clock.residual_offset().abs() < 1000, "residual offset {} should be < 1ms", clock.residual_offset());
    }

    #[tokio::test]
    async fn test_concurrent_bidirectional_clock_sync() {
        let socket_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr_a = socket_a.local_addr().unwrap();
        let clock_a = Arc::new(ClockSync::new(socket_a, SyncConfig::default(), uuid::Uuid::new_v4()));

        let socket_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr_b = socket_b.local_addr().unwrap();
        let clock_b = Arc::new(ClockSync::new(socket_b, SyncConfig::default(), uuid::Uuid::new_v4()));

        // Both nodes measure offset against each other concurrently
        let c_a = Arc::clone(&clock_a);
        let c_b = Arc::clone(&clock_b);

        let (res_a, res_b) = tokio::join!(
            async move { c_a.measure_offset(addr_b).await },
            async move { c_b.measure_offset(addr_a).await }
        );

        assert!(res_a.is_ok(), "node A should measure successfully without contention");
        assert!(res_b.is_ok(), "node B should measure successfully without contention");
        assert!(res_a.unwrap().abs() < 2000);
        assert!(res_b.unwrap().abs() < 2000);
    }
}
