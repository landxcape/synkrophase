use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::sleep;
use uuid::Uuid;

use crate::clock::sync::ClockSync;
use crate::config::SyncConfig;
use crate::error::{Result, SynkroError};
use crate::protocol::messages::{Envelope, serialize};
use crate::session::SessionState;
use crate::sync::controller::{ClockSource, PlaybackControl};

pub struct LeaderAnchorBroadcaster {
    session: Arc<SessionState>,
    clock: Arc<dyn ClockSource>,
    playback: Arc<dyn PlaybackControl>,
    controller: Option<Arc<dyn crate::controller::MediaController>>,
    config: SyncConfig,
}

impl LeaderAnchorBroadcaster {
    pub fn new(
        session: Arc<SessionState>,
        clock: Arc<ClockSync>,
        playback: Arc<dyn PlaybackControl>,
        config: SyncConfig,
    ) -> Self {
        Self {
            session,
            clock,
            playback,
            controller: None,
            config,
        }
    }

    pub fn with_controller(mut self, controller: Arc<dyn crate::controller::MediaController>) -> Self {
        self.controller = Some(controller);
        self
    }

    pub async fn build_anchor_envelope(&self, sender: Uuid) -> Result<Envelope> {
        let t_start = self.clock.reference_now();
        let status = if let Some(controller) = &self.controller {
            if let Ok(state) = controller.get_playback_state().await {
                crate::sync::controller::PlaybackStatus {
                    track_id: state.metadata.map(|m| m.title),
                    position_us: state.position_us,
                    rate: state.rate,
                    is_playing: state.is_playing,
                }
            } else {
                self.playback.status()
            }
        } else {
            self.playback.status()
        };
        let t_end = self.clock.reference_now();
        // Midpoint of the player query gives the most accurate timestamp for the retrieved position
        let sample_time = t_start + (t_end.saturating_sub(t_start)) / 2;

        let message = self
            .session
            .build_sync_anchor_message(sample_time, &status)?;
        Ok(Envelope {
            sender,
            payload: message,
        })
    }

    pub async fn broadcast_once(&self, socket: &UdpSocket, sender: Uuid) -> Result<usize> {
        if !self.session.is_leader() {
            return Err(SynkroError::NotLeader);
        }

        let envelope = self.build_anchor_envelope(sender).await?;
        let bytes = serialize(&envelope)?;
        let mut sent = 0usize;
        for addr in self.session.peer_socket_addrs() {
            socket.send_to(&bytes, addr).await?;
            sent += 1;
        }
        Ok(sent)
    }

    pub async fn run_broadcast_loop(&self, socket: Arc<UdpSocket>, sender: Uuid) -> Result<()> {
        loop {
            let _ = self.broadcast_once(&socket, sender).await;
            sleep(Duration::from_secs(self.config.anchor_broadcast_secs)).await;
        }
    }
}
