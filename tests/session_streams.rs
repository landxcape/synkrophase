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
