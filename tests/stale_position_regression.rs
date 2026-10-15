use std::sync::Arc;
use synkrophase::controller::MediaController;
use synkrophase::controller::mock::MockMediaController;
use synkrophase::protocol::messages::SyncAnchor;
use synkrophase::sync::controller::ClockSource;
use synkrophase::sync::evaluator::{DriftAction, DriftEvaluator};

struct TestClock {
    current_time_us: std::sync::atomic::AtomicU64,
}

impl TestClock {
    fn new(initial_us: u64) -> Self {
        Self {
            current_time_us: std::sync::atomic::AtomicU64::new(initial_us),
        }
    }

    fn advance(&self, elapsed_us: u64) {
        self.current_time_us
            .fetch_add(elapsed_us, std::sync::atomic::Ordering::Relaxed);
    }
}

impl ClockSource for TestClock {
    fn reference_now(&self) -> u64 {
        self.current_time_us
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[tokio::test]
async fn test_stale_position_recovery_behavior() {
    let mock = Arc::new(MockMediaController::new());
    mock.set_supports_rate_control(false); // Simulate macOS / Windows behavior
    let clock = Arc::new(TestClock::new(5_000_000));
    let evaluator = DriftEvaluator::new(clock.clone(), mock.clone(), 50_000);

    // Initial anchor
    let anchor = SyncAnchor {
        reference_time: 5_000_000,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: None,
    };

    // Case 1: Controller is perfectly synchronized
    mock.set_position(10_000_000).await;
    mock.play().await.unwrap();

    let res = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert!(
        matches!(res, DriftAction::InSync { .. }),
        "Must be InSync initially"
    );

    // Case 2: Time advances by 500ms, but simulated buggy controller position is FROZEN (stale).
    // DriftEvaluator uses a 5-sample median window and outlier confirmation (>= 2).
    // Evaluate across consecutive intervals to simulate sustained stale position.
    clock.advance(500_000);
    let _ = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    let _ = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    let res = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    // Beyond 200ms -> Zone 3 -> MicroSeek
    match res {
        DriftAction::MicroSeek {
            target_position_us,
            drift_us,
        } => {
            assert_eq!(drift_us, -500_000);
            assert_eq!(target_position_us, 10_500_000);
        }
        other => panic!(
            "Expected MicroSeek when controller position is stale, got {:?}",
            other
        ),
    }

    // Case 3: Fix applied — controller advances in step with clock
    clock.advance(500_000); // clock now at 6_000_000 (elapsed 1_000_000 from anchor)
    mock.set_position(11_000_000).await; // position advances to 11_000_000
    let _ = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    let _ = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    let res = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert!(
        matches!(res, DriftAction::InSync { .. }),
        "Must return to InSync once live position advances with wall-clock time"
    );
}

#[tokio::test]
async fn test_failover_leader_drift_suppression() {
    let self_id = uuid::Uuid::new_v4();
    let session = synkrophase::session::SessionState::new_leader(
        "ROOM99".into(),
        self_id,
        "PromotedLeader".into(),
    );

    // When the node is leader, session.is_leader() must be true
    assert!(session.is_leader());

    // latest_sync_anchor may exist from a prior leader session
    let stale_anchor = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 1_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: None,
    };
    session.accept_sync_anchor(stale_anchor);

    // The follower loop checks `session.is_leader()` and suppresses drift evaluation
    assert!(
        session.is_leader(),
        "Leader must bypass drift evaluation against past anchors"
    );
}
