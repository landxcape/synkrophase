use std::sync::Arc;

use synkrophase::protocol::messages::{Message, QueueCommand, StreamUrl, Track};
use synkrophase::session::SessionState;
use synkrophase::stream::resolver::MediaRouter;
use synkrophase::stream::server::MediaServer;
use uuid::Uuid;

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
    let session = SessionState::new_leader("ROOM42".into(), leader_id, "TestUser".to_string());
    session
        .handle_queue_proposal(QueueCommand::Add(track("one")))
        .unwrap();

    let server = Arc::new(MediaServer::new(0));
    let router = MediaRouter::new(server);

    let stream = session.resolve_current_track(&router).await.unwrap();
    assert_eq!(
        stream,
        StreamUrl {
            track_id: "one".into(),
            url: "https://youtube.com/watch?v=one".into(),
            expires_at: 0,
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
        "TestUser".to_string(),
        leader_id,
        "127.0.0.1:8080".parse().unwrap(),
        vec![],
        synkrophase::protocol::messages::QueueState::default(),
    );

    let stream = StreamUrl {
        track_id: "remote".into(),
        url: "https://cdn.example.com/audio".into(),
        expires_at: 0,
    };

    session.accept_stream_url(stream.clone());
    assert_eq!(session.stream_url_for("remote"), Some(stream));
}
