use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::protocol::messages::{PeerInfo, Role};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcRequest {
    pub id: Option<u64>,
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcResponse {
    pub id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<IpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcError {
    pub code: i32,
    pub message: String,
}

impl IpcResponse {
    pub fn ok(id: Option<u64>, result: serde_json::Value) -> Self {
        Self {
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn err(id: Option<u64>, code: i32, message: impl Into<String>) -> Self {
        Self {
            id,
            result: None,
            error: Some(IpcError {
                code,
                message: message.into(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcEvent {
    pub event: String,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonStatusSnapshot {
    pub room_code: String,
    pub role: Role,
    pub self_id: Uuid,
    pub device_name: String,
    pub leader_id: Option<Uuid>,
    pub leader_addr: Option<String>,
    pub is_playing: bool,
    pub position_sec: f64,
    pub track_title: Option<String>,
    pub track_artist: Option<String>,
    pub track_album: Option<String>,
    pub duration_sec: Option<f64>,
    pub clock_offset_us: i64,
    pub drift_offset_us: i64,
    pub drift_status: String,
    pub peers: Vec<PeerInfo>,
}
