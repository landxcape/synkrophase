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
    assert_eq!(text, "Locked (<80ms)");

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

#[tokio::test]
async fn test_drift_evaluator_unified_display_and_reconciliation_smoothing() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    controller.play().await.unwrap();
    controller.seek_to(10_000_000).await.unwrap();

    let evaluator = DriftEvaluator::new(clock, controller.clone(), 50_000);

    let anchor = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: None,
    };

    // Calling evaluate_drift for UI display seeds the unified history
    for _ in 0..10 {
        let (drift, zone, _) = evaluator.evaluate_drift(&anchor).await.unwrap();
        assert_eq!(drift, 0);
        assert_eq!(zone, 1);
    }

    // Now call evaluate_and_reconcile; it seamlessly uses the same consistent smoothed history
    let action = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert!(matches!(action, DriftAction::InSync { drift_us: 0 }));
}

#[tokio::test]
async fn test_reconcile_respects_hysteresis_zone1() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    controller.play().await.unwrap();

    let evaluator = DriftEvaluator::new(clock, controller.clone(), 50_000);

    let anchor = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: None,
    };

    // 1. Initial lock at 40ms drift (Zone 1)
    controller.seek_to(10_040_000).await.unwrap();
    let action = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert!(matches!(action, DriftAction::InSync { .. }));

    // 2. Drift temporarily rises to 65ms (within 50ms + 25ms = 75ms release margin)
    controller.seek_to(10_065_000).await.unwrap();
    let action2 = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    // Thanks to hysteresis, remains in Zone 1 (InSync) rather than flapping to rate adjust!
    assert!(matches!(action2, DriftAction::InSync { .. }));
}

#[tokio::test]
async fn test_continuous_anchors_respect_settling_grace_period() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    controller.seek_to(0).await.unwrap();
    controller.play().await.unwrap();

    let evaluator = DriftEvaluator::new(clock, controller.clone(), 50_000);

    // Initial anchor at t=1.0s, pos=10s triggers micro-seek
    let anchor1 = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: Some("Song".into()),
    };
    let action1 = evaluator.evaluate_and_reconcile(&anchor1).await.unwrap();
    assert!(matches!(
        action1,
        DriftAction::MicroSeek {
            target_position_us: 10_000_000,
            ..
        }
    ));

    // Follower seeks to 10s. Now simulate player lagging/buffering (e.g. at 5s)
    controller.seek_to(5_000_000).await.unwrap();

    // 1 second later, next routine periodic anchor arrives (continuous progression: ref=2.0s, pos=11s)
    let anchor2 = SyncAnchor {
        reference_time: 2_000_000,
        media_position_us: 11_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: Some("Song".into()),
    };

    // Because this is a continuous anchor (not a scrub/discontinuity) and within 1500ms post-seek cooldown,
    // evaluator MUST NOT seek again; it must report InSync { 0 } to allow the player to settle.
    let action2 = evaluator.evaluate_and_reconcile(&anchor2).await.unwrap();
    assert!(matches!(action2, DriftAction::InSync { drift_us: 0 }));
    // Position must not have been modified by a premature seek
    assert_eq!(
        controller.get_playback_state().await.unwrap().position_us,
        5_000_000
    );
}

#[tokio::test]
async fn test_playback_rate_restored_when_drift_settles() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    controller.seek_to(10_100_000).await.unwrap(); // 100ms ahead (Zone 2 fine-tune window)
    controller.play().await.unwrap();

    let evaluator = DriftEvaluator::new(clock, controller.clone(), 50_000);

    let anchor = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: None,
    };

    // First evaluation should fine-tune rate to 0.99
    let action = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert!(
        matches!(action, DriftAction::FineTuneRate { rate, .. } if (rate - 0.99).abs() < f32::EPSILON)
    );
    assert!((controller.get_playback_state().await.unwrap().rate - 0.99).abs() < f32::EPSILON);

    // Now player position converges to 10_020_000 us (within 50ms threshold)
    controller.seek_to(10_020_000).await.unwrap();

    // Allow 5-sample median window to reflect the new position
    let _ = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    let action2 = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert!(matches!(action2, DriftAction::InSync { .. }));
    assert!((controller.get_playback_state().await.unwrap().rate - 1.0).abs() < f32::EPSILON);
}

#[tokio::test]
async fn test_evaluator_skips_rate_nudge_when_unsupported() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    controller.set_supports_rate_control(false);
    controller.seek_to(10_100_000).await.unwrap(); // 100ms drift (Zone 2)
    controller.play().await.unwrap();

    let evaluator = DriftEvaluator::new(clock, controller.clone(), 50_000);
    let anchor = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: None,
    };

    // First evaluation: new anchor causes discontinuity -> immediate seek fallback
    let action1 = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert!(matches!(action1, DriftAction::MicroSeek { .. }));
    // Rate must NOT have been changed
    assert!((controller.get_playback_state().await.unwrap().rate - 1.0).abs() < f32::EPSILON);
}

#[tokio::test]
async fn test_evaluator_falls_through_to_seek_when_rate_unconfirmed() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    // Controller claims rate support is true, but player silently ignores rate changes
    controller.set_supports_rate_control(true);
    controller.set_ignore_rate_changes(true);
    controller.seek_to(10_100_000).await.unwrap(); // 100ms drift (Zone 2)
    controller.play().await.unwrap();

    let evaluator = DriftEvaluator::new(clock, controller.clone(), 50_000);
    let anchor = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: None,
    };

    // First evaluation: new anchor causes discontinuity -> immediate seek fallback when rate confirmation fails
    let action = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert!(matches!(action, DriftAction::MicroSeek { .. }));
    // Rate remains unchanged because the player ignores rate modifications
    assert!((controller.get_playback_state().await.unwrap().rate - 1.0).abs() < f32::EPSILON);
}

#[tokio::test]
async fn test_fixed_rate_player_zone2_persistence_gate() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    controller.set_supports_rate_control(false);
    controller.seek_to(10_000_000).await.unwrap();
    controller.play().await.unwrap();

    let evaluator = DriftEvaluator::new(clock, controller.clone(), 50_000);
    let anchor = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: Some("Song A".into()),
    };

    // 1. Initial lock establishes baseline anchor and position
    let action_init = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert!(matches!(action_init, DriftAction::InSync { .. }));

    // 2. Continuous playback drifts by +100ms (Zone 2 for fixed rate)
    controller.seek_to(10_100_000).await.unwrap();
    // Flush median buffer with this drift
    for _ in 0..5 {
        let _ = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    }

    // 3. Within 3 seconds, evaluator monitors and returns InSync without seeking
    let action_monitoring = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert!(
        matches!(action_monitoring, DriftAction::InSync { .. }),
        "Fixed-rate Zone 2 drift must not seek immediately during monitoring window"
    );

    let (_, zone, text) = evaluator.evaluate_drift(&anchor).await.unwrap();
    assert_eq!(zone, 2);
    assert!(text.contains("Monitoring"));

    // 4. Simulate persistence gate expiration (3.1s elapsed in Zone 2)
    evaluator.set_zone2_started_at(Some(
        std::time::Instant::now() - std::time::Duration::from_millis(3100),
    ));

    // 5. Next evaluation confirms persistent drift -> escalates to MicroSeek!
    let action_seek = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert!(
        matches!(action_seek, DriftAction::MicroSeek { .. }),
        "Persistent Zone 2 drift beyond 3.0s must escalate to MicroSeek"
    );
}

#[tokio::test]
async fn test_rate_nudge_hard_deadline_escalates_to_seek() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    controller.set_supports_rate_control(true);
    controller.seek_to(10_000_000).await.unwrap();
    controller.play().await.unwrap();

    let evaluator = DriftEvaluator::new(clock, controller.clone(), 50_000);
    let anchor = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: Some("Song Nudge".into()),
    };

    // Initial lock
    let _ = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();

    // Player drifts by +100ms
    controller.seek_to(10_100_000).await.unwrap();
    for _ in 0..5 {
        let _ = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    }

    // Rate-capable player starts rate nudging
    let action = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert!(
        matches!(action, DriftAction::FineTuneRate { rate, .. } if (rate - 0.99).abs() < f32::EPSILON)
    );

    // Simulate expired nudge deadline (13s elapsed, exceeding 12s deadline)
    evaluator.set_nudge_started_at(Some(
        std::time::Instant::now() - std::time::Duration::from_secs(13),
    ));

    // Evaluator must abort nudge, restore rate to 1.0, and escalate to MicroSeek
    let action_timeout = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert!(
        matches!(action_timeout, DriftAction::MicroSeek { .. }),
        "Expired nudge deadline must escalate to MicroSeek"
    );
    assert!(
        (controller.get_playback_state().await.unwrap().rate - 1.0).abs() < f32::EPSILON,
        "Playback rate must be restored to 1.0 on seek escalation"
    );
}

#[tokio::test]
async fn test_post_seek_settling_window_2500ms() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    controller.seek_to(10_300_000).await.unwrap(); // 300ms drift -> Zone 3 seek
    controller.play().await.unwrap();

    let evaluator = DriftEvaluator::new(clock, controller.clone(), 50_000);
    let anchor = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: Some("Song Settling".into()),
    };

    // 1. Initial seek executed
    let action1 = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert!(matches!(action1, DriftAction::MicroSeek { .. }));

    // 2. Simulate 2.0s post-seek (within 2.5s settling window)
    evaluator.set_last_seek(Some(
        std::time::Instant::now() - std::time::Duration::from_millis(2000),
    ));

    // evaluate_drift returns Settling (2.5s)
    let (_, zone, text) = evaluator.evaluate_drift(&anchor).await.unwrap();
    assert_eq!(zone, 1);
    assert_eq!(text, "Settling (2.5s)");

    // evaluate_and_reconcile suppresses further seeks
    controller.seek_to(10_300_000).await.unwrap(); // simulate post-seek glitch
    let action2 = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert!(matches!(action2, DriftAction::InSync { drift_us: 0 }));

    // 3. Simulate 2.6s post-seek (past 2.5s settling window)
    evaluator.set_last_seek(Some(
        std::time::Instant::now() - std::time::Duration::from_millis(2600),
    ));

    // After 2.6s, settling has completed
    let (_, _, text_after) = evaluator.evaluate_drift(&anchor).await.unwrap();
    assert_ne!(text_after, "Settling (2.5s)");
}

#[tokio::test]
async fn test_seek_priming_lead_initial_application() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    controller.set_seek_priming_lead_us(120_000); // 120ms baseline
    controller.set_actuation_delay_us(10_000); // 10ms IPC delay
    controller.seek_to(10_300_000).await.unwrap(); // 300ms drift -> Zone 3 seek
    controller.play().await.unwrap();

    let evaluator = DriftEvaluator::new(clock, controller.clone(), 50_000);
    assert_eq!(evaluator.seek_priming_lead_us(), 120_000);

    let anchor = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: Some("Priming Test".into()),
    };

    let action = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    match action {
        DriftAction::MicroSeek {
            target_position_us, ..
        } => {
            // expected = 10_000_000, actuation = 10_000, priming = 120_000 -> 10_130_000
            assert_eq!(target_position_us, 10_130_000);
        }
        other => panic!("Expected MicroSeek, got {:?}", other),
    }
}

#[tokio::test]
async fn test_closed_loop_seek_priming_auto_calibration() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    controller.set_seek_priming_lead_us(100_000); // starts at 100ms
    controller.play().await.unwrap();

    let evaluator = DriftEvaluator::new(clock, controller.clone(), 50_000);
    let anchor = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: Some("Auto Cal".into()),
    };

    // 1. Initial seek arms calibration
    let _ = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();

    // 2. Simulate 2.6s post-seek settling elapsing
    evaluator.set_last_seek(Some(
        std::time::Instant::now() - std::time::Duration::from_millis(2600),
    ));

    // 3. Player actually landed -50ms behind (10_000_000 - 50_000 = 9_950_000)
    controller.seek_to(9_950_000).await.unwrap();

    // 4. Next evaluation should detect -50ms residual drift and auto-calibrate:
    // new_lead = 100_000 + 50_000 = 150_000
    let _ = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert_eq!(evaluator.seek_priming_lead_us(), 150_000);

    // 5. Another seek incorporates the new 150ms lead
    controller.seek_to(10_300_000).await.unwrap(); // trigger seek again
    let action = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    match action {
        DriftAction::MicroSeek {
            target_position_us, ..
        } => {
            assert_eq!(target_position_us, 10_150_000);
        }
        other => panic!("Expected MicroSeek with calibrated lead, got {:?}", other),
    }
}

#[tokio::test]
async fn test_closed_loop_seek_priming_clamp_bounds() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    controller.set_seek_priming_lead_us(200_000);
    controller.play().await.unwrap();

    let evaluator = DriftEvaluator::new(clock, controller.clone(), 50_000);
    let anchor = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: Some("Clamp Test".into()),
    };

    // 0. Establish baseline anchor and lock
    let _ = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();

    // Case A: Huge undershoot (-500ms) clamps at upper bound 350ms
    evaluator.set_last_seek(Some(
        std::time::Instant::now() - std::time::Duration::from_millis(2600),
    ));
    evaluator.set_pending_seek_calibration(true);
    controller.seek_to(9_500_000).await.unwrap(); // -500ms drift
    let _ = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert_eq!(evaluator.seek_priming_lead_us(), 350_000);

    // Case B: Huge overshoot (+600ms) clamps at lower bound 0ms
    evaluator.set_last_seek(Some(
        std::time::Instant::now() - std::time::Duration::from_millis(2600),
    ));
    evaluator.set_pending_seek_calibration(true);
    controller.seek_to(10_600_000).await.unwrap(); // +600ms drift
    let _ = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert_eq!(evaluator.seek_priming_lead_us(), 0);
}

#[tokio::test]
async fn test_evaluator_pauses_local_player_when_anchor_is_paused() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    controller.play().await.unwrap();
    controller.seek_to(10_000_000).await.unwrap();
    assert!(controller.get_playback_state().await.unwrap().is_playing);

    let evaluator = DriftEvaluator::new(clock, controller.clone(), 50_000);
    let anchor = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: false,
        track_title: None,
    };

    let action = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert!(matches!(action, DriftAction::InSync { .. }));
    assert!(!controller.get_playback_state().await.unwrap().is_playing);
}

#[tokio::test]
async fn test_evaluator_resumes_local_player_when_anchor_is_playing() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    controller.pause().await.unwrap();
    controller.seek_to(10_000_000).await.unwrap();
    assert!(!controller.get_playback_state().await.unwrap().is_playing);

    let evaluator = DriftEvaluator::new(clock, controller.clone(), 50_000);
    let anchor = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: None,
    };

    let _ = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert!(controller.get_playback_state().await.unwrap().is_playing);
}

#[tokio::test]
async fn test_evaluator_aligns_paused_position_when_both_paused() {
    let clock = Arc::new(TestClock { now: 1_000_000 });
    let controller = Arc::new(MockMediaController::new());
    controller.pause().await.unwrap();
    controller.seek_to(10_000_000).await.unwrap();

    let evaluator = DriftEvaluator::new(clock, controller.clone(), 50_000);
    let anchor = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 15_000_000,
        playback_rate: 1.0,
        is_playing: false,
        track_title: None,
    };

    let _ = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert!(!controller.get_playback_state().await.unwrap().is_playing);
    assert_eq!(
        controller.get_playback_state().await.unwrap().position_us,
        15_000_000
    );
}

