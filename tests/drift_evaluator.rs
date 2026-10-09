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
        track_title: None,
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
        track_title: None,
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

#[tokio::test]
async fn test_drift_evaluator_allows_rapid_seeks_on_new_anchors() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    // Local player starts at position 0
    controller.seek_to(0).await.unwrap();
    controller.play().await.unwrap();

    let evaluator = DriftEvaluator::new(clock, controller.clone(), 50_000);

    // First anchor scrubs to 10s
    let anchor1 = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: None,
    };
    let action1 = evaluator.evaluate_and_reconcile(&anchor1).await.unwrap();
    assert!(matches!(
        action1,
        DriftAction::MicroSeek {
            target_position_us: 10_000_000,
            ..
        }
    ));
    assert_eq!(
        controller.get_playback_state().await.unwrap().position_us,
        10_000_000
    );

    // Rapid second anchor 200ms later (well within 1500ms cooldown) scrubs to 25s
    let anchor2 = SyncAnchor {
        reference_time: 1_200_000,
        media_position_us: 25_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: None,
    };
    let action2 = evaluator.evaluate_and_reconcile(&anchor2).await.unwrap();
    assert!(matches!(
        action2,
        DriftAction::MicroSeek {
            target_position_us: 25_000_000,
            ..
        }
    ));
    assert_eq!(
        controller.get_playback_state().await.unwrap().position_us,
        25_000_000
    );

    // Same anchor repeated immediately should be ignored by the post-seek settling grace window
    controller.seek_to(0).await.unwrap(); // Simulate temporary drift/glitch
    let action3 = evaluator.evaluate_and_reconcile(&anchor2).await.unwrap();
    assert!(matches!(action3, DriftAction::InSync { drift_us: 0 }));
    assert_eq!(
        controller.get_playback_state().await.unwrap().position_us,
        0
    );
}

#[tokio::test]
async fn test_drift_evaluator_hysteresis_and_settling_display() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    controller.play().await.unwrap();

    let evaluator = DriftEvaluator::new(clock, controller.clone(), 50_000); // 50ms threshold

    let anchor = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: None,
    };

    // 1. Initial lock at 40ms drift (Zone 1)
    controller.seek_to(10_040_000).await.unwrap();
    let (_, zone, text) = evaluator.evaluate_drift(&anchor).await.unwrap();
    assert_eq!(zone, 1);
    assert_eq!(text, "Locked (<50ms)");

    // 2. Drift rises to 65ms (within 50ms + 25ms hysteresis release margin)
    // Should remain in Zone 1 (Locked) due to hysteresis
    controller.seek_to(10_065_000).await.unwrap();
    let (_, zone, _) = evaluator.evaluate_drift(&anchor).await.unwrap();
    assert_eq!(zone, 1);

    // 3. Drift rises past 75ms (e.g. 100ms) -> transitions to Zone 2 (Nudging)
    controller.seek_to(10_100_000).await.unwrap();
    for _ in 0..3 {
        evaluator.evaluate_drift(&anchor).await.unwrap();
    }
    let (_, zone, text) = evaluator.evaluate_drift(&anchor).await.unwrap();
    assert_eq!(zone, 2);
    assert!(text.contains("Nudging"));
}
