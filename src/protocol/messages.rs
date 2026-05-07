use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Envelope {
    pub sender: Uuid,
    pub payload: Message,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Message {
    ClockRequest {
        t1: u64,
    },
    ClockResponse {
        t1: u64,
        t2: u64,
        t3: u64,
    },
    Heartbeat {
        room_code: String,
        is_leader: bool,
    },
    JoinRequest {
        room_code: String,
    },
    JoinAccepted {
        peer_list: Vec<PeerInfo>,
        queue_state: QueueState,
    },
    PeerJoined(PeerInfo),
    PeerLeft(Uuid),
    LeaderElected(Uuid),
    TransferLeadership {
        to: Uuid,
    },
    Play,
    Pause,
    Resume,
    Skip,
    QueueProposal(QueueCommand),
    QueueUpdate(QueueState),
    StreamUrl(StreamUrl),
    SyncAnchor(SyncAnchor),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PeerInfo {
    pub device_id: Uuid,
    pub clock_offset_us: i64,
    pub last_seen: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct QueueState {
    pub version: u64,
    pub current: Option<Track>,
    pub upcoming: Vec<Track>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Track {
    pub id: String,
    pub youtube_url: String,
    pub title: String,
    pub requested_by: Uuid,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum QueueCommand {
    Add(Track),
    Skip,
    Remove { position: usize },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StreamUrl {
    pub track_id: String,
    pub url: String,
    pub expires_at: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SyncAnchor {
    pub reference_time: u64,
    pub media_position_us: i64,
    pub playback_rate: f32,
    pub is_playing: bool,
}

pub fn serialize(envelope: &Envelope) -> crate::error::Result<Vec<u8>> {
    postcard::to_allocvec(envelope)
        .map_err(|e| crate::error::SynkroError::Serialization(e.to_string()))
}

pub fn deserialize(bytes: &[u8]) -> crate::error::Result<Envelope> {
    postcard::from_bytes(bytes)
        .map_err(|e| crate::error::SynkroError::Deserialization(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialization_roundtrip() {
        let env = Envelope {
            sender: Uuid::new_v4(),
            payload: Message::ClockRequest { t1: 12345 },
        };
        let bytes = serialize(&env).unwrap();
        let decoded = deserialize(&bytes).unwrap();
        assert_eq!(env, decoded);
    }

    #[test]
    fn test_queue_update_roundtrip() {
        let track = Track {
            id: "track-1".into(),
            youtube_url: "https://youtube.com/watch?v=abc".into(),
            title: "Test Track".into(),
            requested_by: Uuid::nil(),
        };
        let env = Envelope {
            sender: Uuid::new_v4(),
            payload: Message::QueueUpdate(QueueState {
                version: 7,
                current: Some(track.clone()),
                upcoming: vec![track],
            }),
        };

        let bytes = serialize(&env).unwrap();
        let decoded = deserialize(&bytes).unwrap();
        assert_eq!(env, decoded);
    }

    #[test]
    fn test_join_accepted_roundtrip() {
        let env = Envelope {
            sender: Uuid::new_v4(),
            payload: Message::JoinAccepted {
                peer_list: vec![PeerInfo {
                    device_id: Uuid::new_v4(),
                    clock_offset_us: 42,
                    last_seen: 99,
                }],
                queue_state: QueueState::default(),
            },
        };

        let bytes = serialize(&env).unwrap();
        let decoded = deserialize(&bytes).unwrap();
        assert_eq!(env, decoded);
    }

    #[test]
    fn test_stream_url_roundtrip() {
        let env = Envelope {
            sender: Uuid::new_v4(),
            payload: Message::StreamUrl(StreamUrl {
                track_id: "track-1".into(),
                url: "https://cdn.example.com/audio".into(),
                expires_at: 55,
            }),
        };

        let bytes = serialize(&env).unwrap();
        let decoded = deserialize(&bytes).unwrap();
        assert_eq!(env, decoded);
    }

    #[test]
    fn test_sync_anchor_roundtrip() {
        let env = Envelope {
            sender: Uuid::new_v4(),
            payload: Message::SyncAnchor(SyncAnchor {
                reference_time: 1_000_000,
                media_position_us: 50_000,
                playback_rate: 1.02,
                is_playing: true,
            }),
        };

        let bytes = serialize(&env).unwrap();
        let decoded = deserialize(&bytes).unwrap();
        assert_eq!(env, decoded);
    }

    #[test]
    fn test_playback_controls_roundtrip() {
        let controls = vec![Message::Play, Message::Pause, Message::Resume, Message::Skip];
        for payload in controls {
            let env = Envelope {
                sender: Uuid::new_v4(),
                payload,
            };
            let bytes = serialize(&env).unwrap();
            let decoded = deserialize(&bytes).unwrap();
            assert_eq!(env, decoded);
        }
    }
}
