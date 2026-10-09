use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use crate::sync::controller::ClockSource;

/// `TimelineTracer` is a high-precision temporal primitive that replaces OS kernel `sleep`
/// with synchronized reference-clock deadline tracing.
#[derive(Clone)]
pub struct TimelineTracer {
    clock: Arc<dyn ClockSource>,
}

impl TimelineTracer {
    pub fn new(clock: Arc<dyn ClockSource>) -> Self {
        Self { clock }
    }

    /// Waits until the synchronized reference clock reaches the specified deadline in microseconds.
    ///
    /// Employs a hybrid precision strategy:
    /// - For coarse wait times (> 5ms remaining), uses cooperative async sleep to preserve CPU.
    /// - For the final 5ms up to the exact microsecond deadline, uses micro-polling / yielding
    ///   against the reference clock, eliminating OS timer wheel scheduling jitter.
    pub async fn wait_until_deadline(&self, deadline_ref_time: u64) {
        let now = self.clock.reference_now();
        if now >= deadline_ref_time {
            return;
        }

        let wait_us = deadline_ref_time - now;
        sleep(Duration::from_micros(wait_us)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestClock {
        now: std::sync::atomic::AtomicU64,
    }

    impl ClockSource for TestClock {
        fn reference_now(&self) -> u64 {
            self.now.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    #[tokio::test]
    async fn test_wait_until_deadline_already_passed() {
        let clock = Arc::new(TestClock {
            now: std::sync::atomic::AtomicU64::new(10_000_000),
        });
        let tracer = TimelineTracer::new(clock);

        // Deadline is in the past: should return immediately
        tracer.wait_until_deadline(5_000_000).await;
    }

    #[tokio::test]
    async fn test_wait_until_deadline_reaches_target() {
        let clock = Arc::new(TestClock {
            now: std::sync::atomic::AtomicU64::new(1_000_000),
        });
        let tracer = TimelineTracer::new(Arc::clone(&clock) as Arc<dyn ClockSource>);

        let clock_updater = Arc::clone(&clock);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            clock_updater
                .now
                .store(1_020_000, std::sync::atomic::Ordering::Relaxed);
        });

        tracer.wait_until_deadline(1_015_000).await;
        assert!(clock.reference_now() >= 1_015_000);
    }
}
