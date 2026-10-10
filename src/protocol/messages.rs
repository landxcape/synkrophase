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
        info: PeerInfo,
    },
    JoinRequest {
        room_code: String,
        name: String,
    },
    JoinAccepted {
        peer_list: Vec<PeerInfo>,
        queue_state: QueueState,
        assigned_role: Role,
        current_anchor: Option<SyncAnchor>,
        current_track: Option<TrackIdentity>,
    },
    PeerJoined(PeerInfo),
    PeerLeft(Uuid),
    LeaderElected(Uuid),
    TransferLeadership {
        to: Uuid,
    },
    AssignRole {
        target: Uuid,
        new_role: Role,
    },
    RoleAssigned {
        target: Uuid,
        new_role: Role,
        actor: Uuid,
    },
    TrackTransition {
        track: TrackIdentity,
        target_ref_time: u64,
        dispatch_ref_time: u64,
    },
    Play {
        actor: Uuid,
    },
    Pause {
        actor: Uuid,
    },
    Resume {
        actor: Uuid,
    },
    Skip,
    NextTrack {
        actor: Uuid,
    },
    PreviousTrack {
        actor: Uuid,
    },
    SetVolume {
        volume: u8,
        actor: Uuid,
    },
    QueueProposal(QueueCommand),
    QueueUpdate(QueueState),
    SyncAnchor(SyncAnchor),
    SystemLog(String),
    Chat {
        sender: Uuid,
        name: String,
        text: String,
    },
    ChatBroadcast {
        sender: Uuid,
        display_name: String,
        text: String,
    },
    Notification {
        text: String,
    },
    PeerListUpdate {
        names: Vec<String>,
    },
    Intent(PlaybackIntent),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TrackIdentity {
    pub title: String,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub spotify_uri: Option<String>,
    pub apple_music_id: Option<String>,
    pub duration_us: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    Listener = 0,
    Moderator = 1,
    Leader = 2,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PeerInfo {
    pub device_id: Uuid,
    pub name: String,
    pub clock_offset_us: i64,
    pub last_seen: u64,
    pub role: Role,
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SyncAnchor {
    pub reference_time: u64,
    pub media_position_us: i64,
    pub playback_rate: f32,
    pub is_playing: bool,
    pub track_title: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum PlaybackAction {
    Play,
    Pause,
    Seek { target_position_us: i64 },
    NextTrack,
    PreviousTrack,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PlaybackIntent {
    pub action: PlaybackAction,
    pub target_ref_time: u64,
    pub position_us: i64,
    pub track_title: Option<String>,
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
                    name: "Alice".into(),
                    clock_offset_us: 42,
                    last_seen: 99,
                    role: Role::Listener,
                }],
                queue_state: QueueState::default(),
                assigned_role: Role::Listener,
                current_anchor: None,
                current_track: None,
            },
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
                track_title: Some("Song Name".into()),
            }),
        };

        let bytes = serialize(&env).unwrap();
        let decoded = deserialize(&bytes).unwrap();
        assert_eq!(env, decoded);
    }

    #[test]
    fn test_playback_controls_roundtrip() {
        let controls = vec![
            Message::Play { actor: Uuid::nil() },
            Message::Pause { actor: Uuid::nil() },
            Message::Resume { actor: Uuid::nil() },
            Message::Skip,
        ];
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

    #[test]
    fn test_chat_broadcast_roundtrip() {
        let sender = Uuid::new_v4();
        let env = Envelope {
            sender,
            payload: Message::ChatBroadcast {
                sender,
                display_name: "Alice".into(),
                text: "Hello everyone!".into(),
            },
        };
        let bytes = serialize(&env).unwrap();
        let decoded = deserialize(&bytes).unwrap();
        assert_eq!(env, decoded);
    }

    #[test]
    fn test_playback_intent_roundtrip() {
        let intent = PlaybackIntent {
            action: PlaybackAction::Play,
            target_ref_time: 1_234_567_890,
            position_us: 42_000_000,
            track_title: Some("Seven Nation Army".into()),
        };
        let env = Envelope {
            sender: Uuid::new_v4(),
            payload: Message::Intent(intent),
        };
        let bytes = serialize(&env).unwrap();
        let decoded = deserialize(&bytes).unwrap();
        assert_eq!(env, decoded);
    }

    #[test]
    fn test_role_and_track_transition_roundtrip() {
        let target = Uuid::new_v4();
        let actor = Uuid::new_v4();

        let assign_env = Envelope {
            sender: actor,
            payload: Message::AssignRole {
                target,
                new_role: Role::Moderator,
            },
        };
        let bytes = serialize(&assign_env).unwrap();
        let decoded: Envelope = deserialize(&bytes).unwrap();
        assert_eq!(assign_env, decoded);

        let assigned_env = Envelope {
            sender: actor,
            payload: Message::RoleAssigned {
                target,
                new_role: Role::Moderator,
                actor,
            },
        };
        let bytes = serialize(&assigned_env).unwrap();
        let decoded: Envelope = deserialize(&bytes).unwrap();
        assert_eq!(assigned_env, decoded);

        let track_env = Envelope {
            sender: actor,
            payload: Message::TrackTransition {
                track: TrackIdentity {
                    title: "Bohemian Rhapsody".into(),
                    artist: Some("Queen".into()),
                    album: Some("A Night at the Opera".into()),
                    spotify_uri: Some("spotify:track:test1234".into()),
                    apple_music_id: None,
                    duration_us: Some(354_000_000),
                },
                target_ref_time: 1_234_567_890,
                dispatch_ref_time: 1_234_167_890,
            },
        };
        let bytes = serialize(&track_env).unwrap();
        let decoded: Envelope = deserialize(&bytes).unwrap();
        assert_eq!(track_env, decoded);
    }
}
