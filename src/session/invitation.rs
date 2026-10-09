use std::net::SocketAddr;

/// Domain representation of a shareable room invitation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomInvitation {
    pub room_code: String,
    pub leader_addr: SocketAddr,
    pub session_port: u16,
    pub clock_port: u16,
}

impl RoomInvitation {
    pub fn new(
        room_code: impl Into<String>,
        leader_addr: SocketAddr,
        session_port: u16,
        clock_port: u16,
    ) -> Self {
        Self {
            room_code: room_code.into(),
            leader_addr,
            session_port,
            clock_port,
        }
    }

    /// Formats the canonical CLI command to join the room.
    pub fn cli_command(&self) -> String {
        format!(
            "synkro join {} --leader-addr {}",
            self.room_code, self.leader_addr
        )
    }

    /// Formats a shareable URI.
    pub fn uri(&self) -> String {
        format!(
            "synkro://{}/join?leader={}",
            self.room_code, self.leader_addr
        )
    }

    /// Copies the canonical CLI command into the system clipboard.
    /// Returns Ok(()) on success, or an informative Err message if unavailable.
    pub fn copy_to_clipboard(&self) -> Result<(), String> {
        copy_text_to_clipboard(&self.cli_command())
    }
}

/// Cross-platform clipboard helper with graceful degradation.
pub fn copy_text_to_clipboard(text: &str) -> Result<(), String> {
    match arboard::Clipboard::new() {
        Ok(mut clipboard) => clipboard
            .set_text(text)
            .map_err(|e| format!("Clipboard write failed: {e}")),
        Err(e) => Err(format!("Clipboard unavailable: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_room_invitation_formatting() {
        let addr = "192.168.1.50:5871".parse().unwrap();
        let inv = RoomInvitation::new("TESTROOM", addr, 5871, 5870);

        assert_eq!(
            inv.cli_command(),
            "synkro join TESTROOM --leader-addr 192.168.1.50:5871"
        );
        assert_eq!(inv.uri(), "synkro://TESTROOM/join?leader=192.168.1.50:5871");
    }
}
