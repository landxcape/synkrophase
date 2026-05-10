use std::sync::{Arc, Mutex};
use std::time::Duration;

use synkrophase::config::SyncConfig;
use synkrophase::error::Result;
use synkrophase::playback::engine::PlaybackStatus;
use synkrophase::protocol::messages::{Message, QueueState, SyncAnchor};
use synkrophase::session::{FollowerSyncRuntime, SessionState};
use synkrophase::sync::controller::{ClockSource, PlaybackControl, SyncAction, SyncController};
use uuid::Uuid;

struct MockClock {
    now: u64,
}

impl ClockSource for MockClock {
    fn reference_now(&self) -> u64 {
        self.now
    }
}

#[derive(Default)]
struct MockPlayback {
    status: Mutex<PlaybackStatus>,
}

impl MockPlayback {
    fn with(position_us: i64, rate: f32) -> Self {
        Self {
            status: Mutex::new(PlaybackStatus {
                track_id: None,
                stream_url: None,
                position_us,
                rate,
                is_playing: true,
            }),
        }
    }
}

impl PlaybackControl for MockPlayback {
    fn position(&self) -> i64 {
        self.status.lock().unwrap().position_us
    }

    fn status(&self) -> PlaybackStatus {
        self.status.lock().unwrap().clone()
    }

    fn set_rate(&self, rate: f32) -> Result<()> {
        self.status.lock().unwrap().rate = rate;
        Ok(())
    }

    fn seek(&self, position_us: i64) -> Result<()> {
        self.status.lock().unwrap().position_us = position_us;
        Ok(())
    }
}

#[tokio::test]
async fn ingest_message_updates_session_and_controller_anchor() {
    let session = SessionState::from_join(
        "ROOM42".into(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        "127.0.0.1:8080".parse().unwrap(),
        vec![],
        QueueState::default(),
    );
    let clock = Arc::new(MockClock { now: 1_000_000 });
    let playback = Arc::new(MockPlayback::with(900_000, 1.0));
    let controller = Arc::new(SyncController::new(clock, playback, SyncConfig::default()));
    let runtime = FollowerSyncRuntime::new(Arc::clone(&controller));

    let anchor = SyncAnchor {
        reference_time: 1_000_000,
        media_position_us: 100_000,
        playback_rate: 1.0,
        is_playing: true,
    };
    runtime.ingest_message(&session, &Message::SyncAnchor(anchor.clone()));

    assert_eq!(session.latest_sync_anchor(), Some(anchor));
    assert_eq!(
        controller.evaluate(),
        SyncAction::Seek {
            target_position_us: 100_000
        }
    );
}

#[tokio::test]
async fn sync_loop_starts_once_and_can_stop() {
    let clock = Arc::new(MockClock { now: 1_000_000 });
    let playback = Arc::new(MockPlayback::with(100_000, 1.0));
    let mut config = SyncConfig::default();
    config.sync_eval_interval_ms = 5;
    let controller = Arc::new(SyncController::new(clock, playback, config));
    let runtime = FollowerSyncRuntime::new(controller);

    assert!(runtime.start_sync_loop());
    assert!(!runtime.start_sync_loop());
    assert!(runtime.sync_loop_running());

    tokio::time::sleep(Duration::from_millis(10)).await;
    runtime.stop_sync_loop();
    assert!(!runtime.sync_loop_running());
}
