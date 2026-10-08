use std::sync::Arc;
use synkrophase::controller::MediaController;
use synkrophase::controller::mock::MockMediaController;
use synkrophase::protocol::messages::SyncAnchor;
use synkrophase::sync::controller::ClockSource;
use synkrophase::sync::evaluator::{DriftAction, DriftEvaluator};

struct TestClock {
    now: u64,
}

impl ClockSource for TestClock {
    fn reference_now(&self) -> u64 {
        self.now
    }
}

#[tokio::test]
async fn test_drift_evaluator_in_sync_within_threshold() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    // Local player is at 10_020_000 us (20ms ahead of expected 10_000_000)
    controller.seek_to(10_020_000).await.unwrap();
    controller.play().await.unwrap();

    let evaluator = DriftEvaluator::new(clock, controller, 50_000); // 50ms threshold
    let anchor = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: true,
    };

    let action = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    match action {
        DriftAction::InSync { drift_us } => {
            assert_eq!(drift_us, 20_000);
        }
        _ => panic!("Expected InSync action"),
    }
}

#[tokio::test]
async fn test_drift_evaluator_micro_seeks_beyond_threshold() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    // Local player is at 10_300_000 us (300ms ahead of expected 10_000_000)
    controller.seek_to(10_300_000).await.unwrap();
    controller.play().await.unwrap();

    let evaluator = DriftEvaluator::new(clock, controller.clone(), 50_000);
    let anchor = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: true,
    };

    let action = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    match action {
        DriftAction::MicroSeek {
            target_position_us,
            drift_us,
        } => {
            assert_eq!(drift_us, 300_000);
            assert_eq!(target_position_us, 10_000_000);
        }
        _ => panic!("Expected MicroSeek action"),
    }

    // Verify player was reconciled back to expected position
    let state = controller.get_playback_state().await.unwrap();
    assert_eq!(state.position_us, 10_000_000);
}

#[tokio::test]
async fn test_drift_evaluator_instant_trigger_wakes_loop() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    let evaluator = Arc::new(DriftEvaluator::new(clock, controller, 50_000));

    let notify = evaluator.notify_handle();
    let notified = notify.notified();

    evaluator.trigger_immediate();
    notified.await;
}
