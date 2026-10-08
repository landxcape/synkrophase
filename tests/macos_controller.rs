#[cfg(target_os = "macos")]
use synkrophase::controller::MediaController;
#[cfg(target_os = "macos")]
use synkrophase::controller::macos::MacOsMediaController;

#[tokio::test]
#[cfg(target_os = "macos")]
async fn test_macos_media_controller_query() {
    let controller = MacOsMediaController::new();
    let state = controller.get_playback_state().await;
    assert!(
        state.is_ok(),
        "Failed to query macOS playback state: {:?}",
        state.err()
    );
    let state = state.unwrap();
    // Verify rate defaults to 1.0
    assert_eq!(state.rate, 1.0);
}
