use std::sync::{Arc, Mutex};
use std::time::Duration;

use synkrophase::playback::engine::{
    PlaybackBackend, PlaybackEngine, PlaybackSnapshot, PlaybackStatus,
};

#[derive(Debug, Clone, PartialEq)]
enum BackendCall {
    Load(String),
    Rate(f32),
    Seek(i64),
    Pause,
    Resume,
    Stop,
}

#[derive(Clone, Default)]
struct MockBackend {
    calls: Arc<Mutex<Vec<BackendCall>>>,
}

impl MockBackend {
    fn calls(&self) -> Vec<BackendCall> {
        self.calls.lock().unwrap().clone()
    }
}

impl PlaybackBackend for MockBackend {
    fn load_and_play(&self, stream_url: &str) -> synkrophase::error::Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(BackendCall::Load(stream_url.to_string()));
        Ok(())
    }

    fn set_rate(&self, rate: f32) -> synkrophase::error::Result<()> {
        self.calls.lock().unwrap().push(BackendCall::Rate(rate));
        Ok(())
    }

    fn seek(&self, position_us: i64) -> synkrophase::error::Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(BackendCall::Seek(position_us));
        Ok(())
    }

    fn pause(&self) -> synkrophase::error::Result<()> {
        self.calls.lock().unwrap().push(BackendCall::Pause);
        Ok(())
    }

    fn resume(&self) -> synkrophase::error::Result<()> {
        self.calls.lock().unwrap().push(BackendCall::Resume);
        Ok(())
    }

    fn stop(&self) -> synkrophase::error::Result<()> {
        self.calls.lock().unwrap().push(BackendCall::Stop);
        Ok(())
    }
}

#[test]
fn load_and_play_sets_playing_state_and_tracks_stream() {
    let backend = MockBackend::default();
    let engine = PlaybackEngine::new(Box::new(backend.clone()));

    engine
        .load_and_play("track-1", "https://cdn.example.com/audio")
        .unwrap();

    assert_eq!(
        backend.calls(),
        vec![BackendCall::Load("https://cdn.example.com/audio".into())]
    );
    assert!(engine.is_playing());
    assert_eq!(
        engine.status().track_id.as_deref(),
        Some("track-1")
    );
    assert_eq!(
        engine.status().stream_url.as_deref(),
        Some("https://cdn.example.com/audio")
    );
    assert_eq!(engine.status().rate, 1.0);
}

#[test]
fn set_rate_and_seek_update_status() {
    let backend = MockBackend::default();
    let engine = PlaybackEngine::new(Box::new(backend.clone()));
    engine
        .load_and_play("track-1", "https://cdn.example.com/audio")
        .unwrap();

    engine.set_rate(1.02).unwrap();
    engine.seek(420_000).unwrap();

    assert_eq!(
        backend.calls(),
        vec![
            BackendCall::Load("https://cdn.example.com/audio".into()),
            BackendCall::Rate(1.02),
            BackendCall::Seek(420_000),
        ]
    );

    let status = engine.status();
    assert_eq!(status.rate, 1.02);
    assert_eq!(status.position_us, 420_000);
}

#[test]
fn pause_resume_and_stop_toggle_playback_state() {
    let backend = MockBackend::default();
    let engine = PlaybackEngine::new(Box::new(backend.clone()));
    engine
        .load_and_play("track-1", "https://cdn.example.com/audio")
        .unwrap();

    engine.pause().unwrap();
    assert!(!engine.is_playing());

    engine.resume().unwrap();
    assert!(engine.is_playing());

    engine.stop().unwrap();
    assert!(!engine.is_playing());

    assert_eq!(
        backend.calls(),
        vec![
            BackendCall::Load("https://cdn.example.com/audio".into()),
            BackendCall::Pause,
            BackendCall::Resume,
            BackendCall::Stop,
        ]
    );
}

#[test]
fn position_snapshot_reflects_latest_status() {
    let backend = MockBackend::default();
    let engine = PlaybackEngine::new(Box::new(backend));
    engine
        .load_and_play("track-1", "https://cdn.example.com/audio")
        .unwrap();
    engine.seek(123_000).unwrap();
    engine.set_rate(0.98).unwrap();

    let snapshot = engine.position_snapshot();
    std::thread::sleep(Duration::from_micros(1));
    let later_snapshot = engine.position_snapshot();

    assert_eq!(
        snapshot,
        PlaybackSnapshot {
            position_us: 123_000,
            reference_time: snapshot.reference_time,
            rate: 0.98,
        }
    );
    assert!(later_snapshot.reference_time >= snapshot.reference_time);
}

#[test]
fn status_exposes_playback_state_for_sync_layer() {
    let backend = MockBackend::default();
    let engine = PlaybackEngine::new(Box::new(backend));

    assert_eq!(
        engine.status(),
        PlaybackStatus {
            track_id: None,
            stream_url: None,
            position_us: 0,
            rate: 1.0,
            is_playing: false,
        }
    );
}
