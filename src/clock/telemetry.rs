use serde::{Deserialize, Serialize};

/// Real-time network health statistics and RFC-6298 adaptive jitter metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkStats {
    /// Latest measured round-trip time in microseconds.
    pub rtt_us: u64,
    /// Smoothed round-trip time in microseconds (RFC 6298 EWMA).
    pub srtt_us: u64,
    /// Round-trip time variance / jitter magnitude in microseconds (RFC 6298 EWMA).
    pub rttvar_us: u64,
    /// Adaptive Retransmission / Probe Timeout in microseconds clamped to [50ms, 1500ms].
    pub rto_us: u64,
    /// Total out-of-order/stale anchor packets dropped by monotonic filter.
    pub stale_anchors_dropped: u64,
    /// Total clock sync probe timeouts/losses experienced.
    pub clock_samples_lost: u64,
}

impl Default for NetworkStats {
    fn default() -> Self {
        Self {
            rtt_us: 0,
            srtt_us: 0,
            rttvar_us: 0,
            rto_us: 50_000, // Default to snappy 50ms floor
            stale_anchors_dropped: 0,
            clock_samples_lost: 0,
        }
    }
}

impl NetworkStats {
    pub const MIN_RTO_US: u64 = 50_000; // 50ms snappy LAN floor
    pub const MAX_RTO_US: u64 = 1_500_000; // 1,500ms congested network ceiling

    /// Updates SRTT, RTTVAR, and RTO according to RFC 6298 given a new RTT measurement.
    pub fn update_rtt(&mut self, sample_rtt_us: u64) {
        self.rtt_us = sample_rtt_us;
        if self.srtt_us == 0 {
            // First measurement initialization (RFC 6298 Sec 2.2)
            self.srtt_us = sample_rtt_us;
            self.rttvar_us = sample_rtt_us / 2;
        } else {
            // Subsequent measurements (RFC 6298 Sec 2.3)
            // RTTVAR <- (3/4) * RTTVAR + (1/4) * |SRTT - RTT|
            let diff = (self.srtt_us as i64 - sample_rtt_us as i64).unsigned_abs();
            self.rttvar_us = (3 * self.rttvar_us + diff) / 4;
            // SRTT <- (7/8) * SRTT + (1/8) * RTT
            self.srtt_us = (7 * self.srtt_us + sample_rtt_us) / 8;
        }

        // RTO <- SRTT + max(G, 4 * RTTVAR), clamped to [MIN_RTO_US, MAX_RTO_US]
        let calculated_rto = self.srtt_us + (4 * self.rttvar_us);
        self.rto_us = calculated_rto.clamp(Self::MIN_RTO_US, Self::MAX_RTO_US);
    }

    /// Records a timed-out clock probe and backs off RTO conservatively.
    pub fn record_sample_loss(&mut self) {
        self.clock_samples_lost = self.clock_samples_lost.saturating_add(1);
        // Exponential backoff up to ceiling
        self.rto_us = (self.rto_us.saturating_mul(2)).clamp(Self::MIN_RTO_US, Self::MAX_RTO_US);
    }

    /// Records an out-of-order/stale anchor drop.
    pub fn record_stale_anchor_drop(&mut self) {
        self.stale_anchors_dropped = self.stale_anchors_dropped.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rfc6298_initial_measurement() {
        let mut stats = NetworkStats::default();
        stats.update_rtt(10_000); // 10ms
        assert_eq!(stats.srtt_us, 10_000);
        assert_eq!(stats.rttvar_us, 5_000);
        // calculated RTO = 10_000 + 4 * 5_000 = 30_000; clamped to MIN_RTO_US = 50_000
        assert_eq!(stats.rto_us, NetworkStats::MIN_RTO_US);
    }

    #[test]
    fn test_rfc6298_high_jitter_expansion() {
        let mut stats = NetworkStats::default();
        stats.update_rtt(200_000); // 200ms initial
        // Spike to 800ms
        stats.update_rtt(800_000);
        assert!(stats.srtt_us > 200_000);
        assert!(stats.rttvar_us > 100_000);
        assert!(stats.rto_us > 800_000);
        assert!(stats.rto_us <= NetworkStats::MAX_RTO_US);
    }

    #[test]
    fn test_loss_backoff() {
        let mut stats = NetworkStats::default();
        assert_eq!(stats.rto_us, 50_000);
        stats.record_sample_loss();
        assert_eq!(stats.rto_us, 100_000);
        assert_eq!(stats.clock_samples_lost, 1);
    }
}
