use std::sync::Arc;
use synkrophase::controller::mock::MockMediaController;
use synkrophase::controller::{MediaController, TrackMetadata};
use synkrophase::protocol::messages::{PlaybackAction, PlaybackIntent};
use synkrophase::sync::controller::ClockSource;
use synkrophase::sync::scheduler::IntentScheduler;

struct TestClock {
    time: std::sync::atomic::AtomicU64,
}

impl TestClock {
    fn new(initial: u64) -> Self {
        Self {
            time: std::sync::atomic::AtomicU64::new(initial),
        }
    }
}

impl ClockSource for TestClock {
    fn reference_now(&self) -> u64 {
        self.time.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[tokio::test]
async fn test_intent_scheduler_normal_trigger() {
    let clock = Arc::new(TestClock::new(1_000_000));
    let controller = Arc::new(MockMediaController::new());
    controller
        .set_track(Some(TrackMetadata {
            title: "Song A".into(),
            artist: None,
            album: None,
            duration_us: Some(180_000_000),
        }))
        .await;

    let scheduler = IntentScheduler::new(clock, controller.clone());

    // Scheduled for 5ms into future
    let intent = PlaybackIntent {
        action: PlaybackAction::Play,
        target_ref_time: 1_005_000,
        position_us: 10_000_000,
        track_title: Some("Song A".into()),
    };

    scheduler.execute_intent(&intent).await.unwrap();

    let state = controller.get_playback_state().await.unwrap();
    assert!(state.is_playing);
    assert_eq!(state.position_us, 10_000_000);
}

#[tokio::test]
async fn test_intent_scheduler_value_skip_on_late_arrival() {
    let clock = Arc::new(TestClock::new(1_050_000)); // 50ms late!
    let controller = Arc::new(MockMediaController::new());
    controller
        .set_track(Some(TrackMetadata {
            title: "Song A".into(),
            artist: None,
            album: None,
            duration_us: Some(180_000_000),
        }))
        .await;

    let scheduler = IntentScheduler::new(clock, controller.clone());

    // Target was 1_000_000, but now is 1_050_000 (overshoot = 50_000 us)
    let intent = PlaybackIntent {
        action: PlaybackAction::Play,
        target_ref_time: 1_000_000,
        position_us: 10_000_000,
        track_title: Some("Song A".into()),
    };

    scheduler.execute_intent(&intent).await.unwrap();

    let state = controller.get_playback_state().await.unwrap();
    assert!(state.is_playing);
    // Value skip compensated position: 10_000_000 + 50_000 = 10_050_000
    assert_eq!(state.position_us, 10_050_000);
}

#[tokio::test]
async fn test_intent_scheduler_autonomous_filtering_skips_mismatched_track() {
    let clock = Arc::new(TestClock::new(1_000_000));
    let controller = Arc::new(MockMediaController::new());
    controller
        .set_track(Some(TrackMetadata {
            title: "Different Track".into(),
            artist: None,
            album: None,
            duration_us: Some(180_000_000),
        }))
        .await;

    let scheduler = IntentScheduler::new(clock, controller.clone());

    let intent = PlaybackIntent {
        action: PlaybackAction::Play,
        target_ref_time: 1_000_000,
        position_us: 10_000_000,
        track_title: Some("Song A".into()),
    };

    scheduler.execute_intent(&intent).await.unwrap();

    let state = controller.get_playback_state().await.unwrap();
    // Did NOT execute play or seek because track was mismatched!
    assert!(!state.is_playing);
    assert_eq!(state.position_us, 0);
}

#[tokio::test]
async fn test_intent_scheduler_predispatch_with_actuation_delay() {
    // Current clock is at 1_000_000 us
    let clock = Arc::new(TestClock::new(1_000_000));
    let controller = Arc::new(MockMediaController::new());
    // Simulate an actuation delay of 150_000 us (150ms)
    controller.set_actuation_delay_us(150_000);

    let scheduler = IntentScheduler::new(clock, controller.clone());

    // Target ref time is 1_150_000 us (+150ms).
    // fire_ref_time = 1_150_000 - 150_000 = 1_000_000 <= now.
    // Scheduler should fire immediately rather than sleeping for 150ms!
    let intent = PlaybackIntent {
        action: PlaybackAction::Play,
        target_ref_time: 1_150_000,
        position_us: 5_000_000,
        track_title: None,
    };

    scheduler.execute_intent(&intent).await.unwrap();

    let state = controller.get_playback_state().await.unwrap();
    assert!(state.is_playing);
    assert_eq!(state.position_us, 5_000_000);
}
