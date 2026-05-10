use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use synkrophase::stream::resolver::MediaRouter;
use synkrophase::stream::server::MediaServer;
use uuid::Uuid;

fn temp_file(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("synkro-{name}-{}", Uuid::new_v4()))
}

#[tokio::test]
async fn resolve_returns_url_for_direct_link() {
    let server = Arc::new(MediaServer::new(0, None)); // Port 0 for random port
    let router = MediaRouter::new(server);

    let input = "https://example.com/audio.mp3";
    let resolved = router.resolve(input).await.unwrap();

    assert_eq!(resolved.url, input);
}

#[tokio::test]
async fn resolve_hosts_local_file_and_returns_server_url() {
    let server = Arc::new(MediaServer::new(0, None));
    let router = MediaRouter::new(server);

    let path = temp_file("test.mp3");
    fs::write(&path, "fake audio data").unwrap();

    let resolved = router.resolve(path.to_str().unwrap()).await.unwrap();

    // Should return a localhost/local IP URL from the server
    assert!(resolved.url.contains(":0/media") || resolved.url.contains("/media"));
    assert!(resolved.url.starts_with("http://"));

    fs::remove_file(path).unwrap();
}
