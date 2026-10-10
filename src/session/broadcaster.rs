use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::sleep;
use uuid::Uuid;

use crate::clock::sync::ClockSync;
use crate::config::SyncConfig;
use crate::error::{Result, SynkroError};
use crate::protocol::messages::{Envelope, Message, serialize};
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

    pub fn with_controller(
        mut self,
        controller: Arc<dyn crate::controller::MediaController>,
    ) -> Self {
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
        let mut last_sample_ref_time: u64 = self.clock.reference_now();
        let mut last_sample_pos_us: i64 = 0;
        let mut last_track: Option<String> = None;
        let mut last_broadcast_ref_time: u64 = 0;

        loop {
            // Poll cadence: check active player state every 250ms
            sleep(Duration::from_millis(250)).await;

            let now_ref = self.clock.reference_now();
            let mut should_broadcast = false;

            if let Some(controller) = &self.controller
                && let Ok(state) = controller.get_playback_state().await
            {
                let current_track = state.metadata.as_ref().map(|m| m.title.clone());

                // Condition 1: Track changed in Spotify / Music!
                if current_track != last_track && last_track.is_some() {
                    tracing::info!(
                        prev = ?last_track,
                        curr = ?current_track,
                        "Leader track changed in player, broadcasting immediate anchor and track identity"
                    );
                    should_broadcast = true;

                    // Extract and broadcast full TrackIdentity to all room followers
                    if let Ok(Some(track_ident)) = controller.get_track_identity().await {
                        let dispatch_ref_time = now_ref;
                        // Provide 400ms lead time for network transit and follower actuation
                        let target_ref_time = now_ref + 400_000;
                        let transition_env = Envelope {
                            sender,
                            payload: Message::TrackTransition {
                                track: track_ident,
                                target_ref_time,
                                dispatch_ref_time,
                            },
                        };
                        if let Ok(bytes) = serialize(&transition_env) {
                            for addr in self.session.peer_socket_addrs() {
                                let _ = socket.send_to(&bytes, addr).await;
                            }
                        }
                    }
                }

                // Condition 2: Timeline scrubbed (>1.5s position jump from continuous trajectory)
                if state.is_playing && last_sample_pos_us > 0 {
                    let elapsed_ref_us = now_ref.saturating_sub(last_sample_ref_time) as i64;
                    let expected_pos_us = last_sample_pos_us + elapsed_ref_us;
                    let diff_us = (state.position_us - expected_pos_us).abs();

                    if diff_us > 1_500_000 {
                        tracing::info!(
                            jump_ms = diff_us / 1000,
                            "Leader manual scrub detected in player, broadcasting immediate anchor"
                        );
                        should_broadcast = true;
                    }
                }

                last_sample_ref_time = now_ref;
                last_sample_pos_us = state.position_us;
                last_track = current_track;
            }

            // Condition 3: Periodic heartbeat anchor (every anchor_broadcast_secs, default 1s)
            let broadcast_interval_us = self.config.anchor_broadcast_secs * 1_000_000;
            if should_broadcast
                || now_ref.saturating_sub(last_broadcast_ref_time) >= broadcast_interval_us
            {
                let _ = self.broadcast_once(&socket, sender).await;
                last_broadcast_ref_time = now_ref;
            }
        }
    }
}
