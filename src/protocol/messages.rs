use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Envelope {
    pub sender: Uuid,
    pub payload: Message,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Message {
    ClockRequest { t1: u64 },
    ClockResponse { t1: u64, t2: u64, t3: u64 },
}

pub fn serialize(envelope: &Envelope) -> crate::error::Result<Vec<u8>> {
    postcard::to_allocvec(envelope)
        .map_err(|e| crate::error::SynkroError::Network(
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        ))
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
}
