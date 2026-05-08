use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use synkrophase::protocol::messages::{Message, QueueCommand, StreamUrl, Track};
use synkrophase::session::SessionState;
use synkrophase::stream::resolver::StreamResolver;
use uuid::Uuid;

fn temp_file(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("synkro-{name}-{}", Uuid::new_v4()))
}

fn write_script(contents: &str) -> PathBuf {
    let path = temp_file("yt-dlp.sh");
    fs::write(&path, contents).unwrap();
    let mut perms = fs::metadata(&path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&path, perms).unwrap();
    path
}

fn track(id: &str) -> Track {
    Track {
        id: id.into(),
        youtube_url: format!("https://youtube.com/watch?v={id}"),
        title: format!("Track {id}"),
        requested_by: Uuid::nil(),
    }
}

#[tokio::test]
async fn leader_resolves_current_track_into_stream_url_message() {
    let leader_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let session = SessionState::new_leader("ROOM42".into(), leader_id);
    session
        .handle_queue_proposal(QueueCommand::Add(track("one")))
        .unwrap();

    let script = write_script(
        "#!/bin/sh\nprintf 'https://rr2---sn.googlevideo.com/videoplayback?expire=1712345678\\n'\n",
    );
    let resolver = StreamResolver::new(script);

    let stream = session.resolve_current_track(&resolver).await.unwrap();
    assert_eq!(
        stream,
        StreamUrl {
            track_id: "one".into(),
            url: "https://rr2---sn.googlevideo.com/videoplayback?expire=1712345678".into(),
            expires_at: 1_712_345_678,
        }
    );

    assert_eq!(session.stream_url_for("one"), Some(stream.clone()));
    assert_eq!(
        session.stream_message_for("one"),
        Some(Message::StreamUrl(stream))
    );
}

#[tokio::test]
async fn expiry_refresh_logic_triggers_re_resolution() {
    let leader_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let session = std::sync::Arc::new(SessionState::new_leader("ROOM42".into(), leader_id));
    session
        .handle_queue_proposal(QueueCommand::Add(track("refresh")))
        .unwrap();

    // 1. Resolve an "almost expired" URL (e.g., 1 minute from now)
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let expires_soon = now + 60;

    let script = write_script(&format!(
        "#!/bin/sh\nprintf 'https://cdn.example.com/stream?expire={expires_soon}\\n'\n"
    ));
    let resolver = StreamResolver::new(script);

    // Initial resolution
    let stream = session.resolve_current_track(&resolver).await.unwrap();
    assert_eq!(stream.expires_at, expires_soon);
    assert_eq!(session.stream_url_for("refresh").unwrap().expires_at, expires_soon);

    // 2. Setup a new resolver script that returns a FRESH URL (expiring in 2 hours)
    let expires_fresh = now + 7200;
    let fresh_script = write_script(&format!(
        "#!/bin/sh\nprintf 'https://cdn.example.com/stream?expire={expires_fresh}\\n'\n"
    ));
    let fresh_resolver = Some(StreamResolver::new(fresh_script));

    // 3. Run the logic from run_stream_distribution_loop manually (or a single iteration of it)
    // We can't easily run the actual loop in a test because it's in main.rs, 
    // so we'll simulate the logic here.
    
    let queue = session.queue_snapshot();
    let current = queue.current.unwrap();
    let current_stream = session.stream_url_for(&current.id);
    
    let needs_resolve = if let Some(stream) = current_stream {
        if stream.expires_at == 0 {
            false
        } else {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            stream.expires_at < now + 300
        }
    } else {
        true
    };

    assert!(needs_resolve, "Should need re-resolution because it expires in 60s (< 300s)");

    if needs_resolve {
        let stream = session.resolve_current_track(&fresh_resolver.unwrap()).await.unwrap();
        assert_eq!(stream.expires_at, expires_fresh);
    }

    assert_eq!(session.stream_url_for("refresh").unwrap().expires_at, expires_fresh);
}

#[test]
fn peer_accepts_stream_url_distribution() {
    let self_id = Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap();
    let leader_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let session = SessionState::from_join(
        "ROOM42".into(),
        self_id,
        leader_id,
        vec![],
        Default::default(),
    );

    let stream = StreamUrl {
        track_id: "remote".into(),
        url: "https://cdn.example.com/audio".into(),
        expires_at: 55,
    };

    session.accept_stream_url(stream.clone());
    assert_eq!(session.stream_url_for("remote"), Some(stream));
}
