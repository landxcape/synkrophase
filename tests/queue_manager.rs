use synkrophase::error::SynkroError;
use synkrophase::protocol::messages::{QueueCommand, QueueState, Track};
use synkrophase::queue::state::QueueManager;
use uuid::Uuid;

fn track(id: &str) -> Track {
    Track {
        id: id.into(),
        youtube_url: format!("https://youtube.com/watch?v={id}"),
        title: format!("Track {id}"),
        requested_by: Uuid::nil(),
    }
}

#[test]
fn apply_add_sets_current_then_appends_upcoming() {
    let manager = QueueManager::new(QueueState::default());

    let state = manager.apply(QueueCommand::Add(track("one"))).unwrap();
    assert_eq!(state.version, 1);
    assert_eq!(state.current.as_ref().map(|t| t.id.as_str()), Some("one"));
    assert!(state.upcoming.is_empty());

    let state = manager.apply(QueueCommand::Add(track("two"))).unwrap();
    assert_eq!(state.version, 2);
    assert_eq!(state.current.as_ref().map(|t| t.id.as_str()), Some("one"));
    assert_eq!(state.upcoming.len(), 1);
    assert_eq!(state.upcoming[0].id, "two");
}

#[test]
fn apply_skip_promotes_next_track() {
    let manager = QueueManager::new(QueueState::default());
    manager.apply(QueueCommand::Add(track("one"))).unwrap();
    manager.apply(QueueCommand::Add(track("two"))).unwrap();

    let state = manager.apply(QueueCommand::Skip).unwrap();
    assert_eq!(state.version, 3);
    assert_eq!(state.current.as_ref().map(|t| t.id.as_str()), Some("two"));
    assert!(state.upcoming.is_empty());
}

#[test]
fn advance_returns_next_track_and_increments_version() {
    let manager = QueueManager::new(QueueState::default());
    manager.apply(QueueCommand::Add(track("one"))).unwrap();
    manager.apply(QueueCommand::Add(track("two"))).unwrap();

    let advanced = manager.advance();
    assert_eq!(advanced.as_ref().map(|t| t.id.as_str()), Some("two"));

    let snapshot = manager.snapshot();
    assert_eq!(snapshot.version, 3);
    assert_eq!(
        snapshot.current.as_ref().map(|t| t.id.as_str()),
        Some("two")
    );
    assert!(snapshot.upcoming.is_empty());
}

#[test]
fn accept_update_rejects_stale_versions() {
    let manager = QueueManager::new(QueueState::default());
    let current = manager.apply(QueueCommand::Add(track("one"))).unwrap();

    let err = manager.accept_update(current).unwrap_err();
    match err {
        SynkroError::StaleQueue { local, received } => {
            assert_eq!(local, 1);
            assert_eq!(received, 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn accept_update_replaces_local_state_when_newer() {
    let manager = QueueManager::new(QueueState::default());
    manager.apply(QueueCommand::Add(track("one"))).unwrap();

    let incoming = QueueState {
        version: 4,
        current: Some(track("remote")),
        upcoming: vec![track("next")],
    };

    manager.accept_update(incoming.clone()).unwrap();
    assert_eq!(manager.snapshot(), incoming);
}
