use synkrophase::controller::mock::MockMediaController;
use synkrophase::controller::{MediaController, TrackMetadata};

#[tokio::test]
async fn test_mock_media_controller_lifecycle() {
    let controller = MockMediaController::new();

    // Initial state: not playing, position 0
    let state = controller.get_playback_state().await.unwrap();
    assert!(!state.is_playing);
    assert_eq!(state.position_us, 0);
    assert_eq!(state.rate, 1.0);
    assert!(state.metadata.is_none());

    // Set track
    controller
        .set_track(Some(TrackMetadata {
            title: "Test Track".into(),
            artist: Some("Test Artist".into()),
            album: Some("Test Album".into()),
            duration_us: Some(180_000_000),
        }))
        .await;

    // Play
    controller.play().await.unwrap();
    let state = controller.get_playback_state().await.unwrap();
    assert!(state.is_playing);
    assert_eq!(state.metadata.as_ref().unwrap().title, "Test Track");

    // Seek
    controller.seek_to(42_000_000).await.unwrap();
    let state = controller.get_playback_state().await.unwrap();
    assert_eq!(state.position_us, 42_000_000);

    // Set rate
    controller.set_rate(1.02).await.unwrap();
    let state = controller.get_playback_state().await.unwrap();
    assert!((state.rate - 1.02).abs() < f32::EPSILON);

    // Pause
    controller.pause().await.unwrap();
    let state = controller.get_playback_state().await.unwrap();
    assert!(!state.is_playing);
}
