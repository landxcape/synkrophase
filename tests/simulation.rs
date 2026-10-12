use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use uuid::Uuid;

use synkrophase::clock::sync::ClockSync;
use synkrophase::config::SyncConfig;
use synkrophase::controller::MediaController;
use synkrophase::controller::mock::MockMediaController;
use synkrophase::protocol::messages::{Envelope, Message, Role, SyncAnchor, TrackIdentity};
use synkrophase::session::SessionState;
use synkrophase::session::broadcaster::LeaderAnchorBroadcaster;
use synkrophase::session::runtime::SessionMessageRuntime;
use synkrophase::sync::controller::MediaControllerPlaybackAdapter;
use synkrophase::sync::evaluator::{DriftAction, DriftEvaluator};

/// A network condition profile for artificial latency, jitter, and packet loss.
#[derive(Clone, Copy, Debug)]
pub struct ChaosProfile {
    pub min_latency_ms: u64,
    pub max_latency_ms: u64,
    pub packet_loss_pct: u8,
}

impl ChaosProfile {
    pub const CLEAN: Self = Self {
        min_latency_ms: 0,
        max_latency_ms: 1,
        packet_loss_pct: 0,
    };

    pub const NOISY_WIFI: Self = Self {
        min_latency_ms: 5,
        max_latency_ms: 25,
        packet_loss_pct: 5,
    };

    pub const BUFFERBLOAT_SPIKE: Self = Self {
        min_latency_ms: 10,
        max_latency_ms: 120,
        packet_loss_pct: 10,
    };
}

/// Simple deterministic pseudo-random number generator (LCG) to eliminate external dependencies.
pub struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    pub fn next_u32(&mut self) -> u32 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.state >> 32) as u32
    }

    pub fn range(&mut self, min: u64, max: u64) -> u64 {
        if min >= max {
            return min;
        }
        let span = max - min + 1;
        min + (self.next_u32() as u64 % span)
    }

    pub fn should_drop(&mut self, loss_pct: u8) -> bool {
        if loss_pct == 0 {
            false
        } else {
            (self.next_u32() % 100) < loss_pct as u32
        }
    }
}

/// A bidirectional UDP chaos proxy that intercepts packets between two endpoints
/// and injects latency jitter and random packet loss.
pub struct NetworkChaosProxy {
    _socket: Arc<UdpSocket>,
    profile: ChaosProfile,
    forward_count: Arc<AtomicU64>,
    drop_count: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
}

impl NetworkChaosProxy {
    pub async fn bind(
        node_a_addr: SocketAddr,
        node_b_addr: SocketAddr,
        profile: ChaosProfile,
        seed: u64,
    ) -> (Self, SocketAddr) {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let proxy_addr = socket.local_addr().unwrap();
        let forward_count = Arc::new(AtomicU64::new(0));
        let drop_count = Arc::new(AtomicU64::new(0));
        let running = Arc::new(AtomicBool::new(true));

        let s_clone = Arc::clone(&socket);
        let f_count = Arc::clone(&forward_count);
        let d_count = Arc::clone(&drop_count);
        let is_running = Arc::clone(&running);
        let rng_mutex = Arc::new(Mutex::new(SimpleRng::new(seed)));

        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            while is_running.load(Ordering::Relaxed) {
                let (len, src) = match s_clone.recv_from(&mut buf).await {
                    Ok(res) => res,
                    Err(_) => break,
                };

                let target = if src == node_a_addr {
                    node_b_addr
                } else if src == node_b_addr {
                    node_a_addr
                } else {
                    // Unknown source, route to node B by default
                    node_b_addr
                };

                let (drop, delay_ms) = {
                    let mut rng = rng_mutex.lock().await;
                    let drop = rng.should_drop(profile.packet_loss_pct);
                    let delay = rng.range(profile.min_latency_ms, profile.max_latency_ms);
                    (drop, delay)
                };

                if drop {
                    d_count.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                f_count.fetch_add(1, Ordering::Relaxed);
                let packet_data = buf[..len].to_vec();
                let s_send = Arc::clone(&s_clone);

                tokio::spawn(async move {
                    if delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                    let _ = s_send.send_to(&packet_data, target).await;
                });
            }
        });

        (
            Self {
                _socket: socket,
                profile,
                forward_count,
                drop_count,
                running,
            },
            proxy_addr,
        )
    }

    pub fn forwarded(&self) -> u64 {
        self.forward_count.load(Ordering::Relaxed)
    }

    pub fn dropped(&self) -> u64 {
        self.drop_count.load(Ordering::Relaxed)
    }

    pub fn profile(&self) -> ChaosProfile {
        self.profile
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Scenario 1: Cold Join & Initial Lock Under Noisy Wi-Fi
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_simulation_noisy_wifi_cold_join() {
    let host_id = Uuid::new_v4();
    let follower_id = Uuid::new_v4();

    let host_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let follower_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());

    let host_addr = host_socket.local_addr().unwrap();
    let follower_addr = follower_socket.local_addr().unwrap();

    // Spawn chaos proxy between Host and Follower with noisy Wi-Fi profile (5-25ms jitter, 5% loss)
    let (proxy, proxy_addr) =
        NetworkChaosProxy::bind(host_addr, follower_addr, ChaosProfile::NOISY_WIFI, 42).await;

    // 1. Host Clock Sync
    let host_clock_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let host_clock_addr = host_clock_socket.local_addr().unwrap();
    let host_clock = Arc::new(ClockSync::new(
        host_clock_socket,
        SyncConfig::default(),
        host_id,
    ));

    // 2. Follower Clock Sync - uses custom config with 15 samples to withstand 5% loss
    let follower_clock_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let follower_clock_addr = follower_clock_socket.local_addr().unwrap();

    // Create a dedicated clock proxy for the PTP traffic as well
    let (_clock_proxy, clock_proxy_addr) = NetworkChaosProxy::bind(
        host_clock_addr,
        follower_clock_addr,
        ChaosProfile::NOISY_WIFI,
        1337,
    )
    .await;

    let follower_config = SyncConfig {
        clock_sample_count: 15,
        ..Default::default()
    };
    let follower_clock = Arc::new(ClockSync::new(
        follower_clock_socket,
        follower_config,
        follower_id,
    ));

    // Follower measures offset against Host through the chaos proxy
    let measured_offset = follower_clock.measure_offset(clock_proxy_addr).await;
    assert!(
        measured_offset.is_ok(),
        "Clock sync should succeed under noisy Wi-Fi with min-RTT filtering"
    );

    // 3. Host State Setup
    let host_controller = Arc::new(MockMediaController::new());
    host_controller
        .set_track(Some(synkrophase::controller::TrackMetadata {
            title: "Simulated Track".into(),
            artist: Some("Chaos Band".into()),
            album: None,
            duration_us: Some(180_000_000),
        }))
        .await;
    // Host is currently playing at 45.0 seconds
    host_controller.set_position(45_000_000).await;
    host_controller.play().await.unwrap();

    let host_session = Arc::new(SessionState::new_leader(
        "WIFI-TEST".into(),
        host_id,
        "HostNode".into(),
    ));

    // 4. Follower State Setup
    let follower_controller = Arc::new(MockMediaController::new());
    follower_controller
        .set_track(Some(synkrophase::controller::TrackMetadata {
            title: "Simulated Track".into(),
            artist: Some("Chaos Band".into()),
            album: None,
            duration_us: Some(180_000_000),
        }))
        .await;
    // Follower joins cold at position 0 (not started)
    follower_controller.set_position(0).await;

    let follower_session = Arc::new(SessionState::from_join(
        "WIFI-TEST".into(),
        follower_id,
        "FollowerNode".into(),
        host_id,
        proxy_addr,
        vec![],
        Default::default(),
    ));

    let follower_evaluator = Arc::new(DriftEvaluator::new(
        follower_clock.clone(),
        follower_controller.clone(),
        50_000, // 50ms Zone 1 threshold
    ));

    let follower_runtime = Arc::new(
        SessionMessageRuntime::new(follower_session.clone(), "FollowerNode".into())
            .with_clock(follower_clock.clone())
            .with_controller(follower_controller.clone())
            .with_drift_evaluator(follower_evaluator.clone()),
    );

    // 5. Host broadcaster generates anchor and sends via proxy
    let host_playback_adapter =
        Arc::new(MediaControllerPlaybackAdapter::new(host_controller.clone()));
    let broadcaster = LeaderAnchorBroadcaster::new(
        host_session.clone(),
        host_clock.clone(),
        host_playback_adapter,
        SyncConfig::default(),
    )
    .with_controller(host_controller.clone());

    let anchor_env = broadcaster.build_anchor_envelope(host_id).await.unwrap();
    let anchor_payload = match anchor_env.payload {
        Message::SyncAnchor(a) => a,
        _ => panic!("Expected SyncAnchor"),
    };

    // Cold join reconciliation: simulate JoinAccepted with the initial anchor
    let join_accepted = Envelope {
        sender: host_id,
        payload: Message::JoinAccepted {
            peer_list: vec![],
            queue_state: Default::default(),
            assigned_role: Role::Listener,
            current_anchor: Some(anchor_payload.clone()),
            current_track: Some(TrackIdentity {
                title: "Simulated Track".into(),
                artist: Some("Chaos Band".into()),
                album: None,
                spotify_uri: None,
                apple_music_id: None,
                duration_us: Some(180_000_000),
            }),
        },
    };

    follower_runtime
        .handle_incoming(&follower_socket, proxy_addr, join_accepted)
        .await
        .unwrap();

    // Allow background reconciliation spawn to execute seek & play
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Follower state should now be playing and positioned close to 45.0s
    let state = follower_controller.get_playback_state().await.unwrap();
    assert!(
        state.is_playing,
        "Follower should start playing upon cold join"
    );
    assert!(
        (state.position_us - 45_000_000).abs() < 500_000,
        "Follower position ({}) should jump to initial anchor position (~45s)",
        state.position_us
    );

    // Evaluate drift through DriftEvaluator: should be locked in Zone 1 (<50ms)
    let action = follower_evaluator
        .evaluate_and_reconcile(&anchor_payload)
        .await
        .unwrap();
    match action {
        DriftAction::InSync { drift_us } => {
            assert!(
                drift_us.unsigned_abs() <= 50_000,
                "Drift {}us exceeds Zone 1 threshold after cold join",
                drift_us
            );
        }
        other => panic!("Expected InSync after cold join, got {:?}", other),
    }

    proxy.stop();
}

// ---------------------------------------------------------------------------
// Scenario 2: Rapid Playlist Scrubbing & Settling Grace Period
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_simulation_rapid_scrub_grace_period() {
    let clock = Arc::new(ClockSync::new(
        UdpSocket::bind("127.0.0.1:0").await.unwrap(),
        SyncConfig::default(),
        Uuid::new_v4(),
    ));

    let follower_controller = Arc::new(MockMediaController::new());
    follower_controller
        .set_track(Some(synkrophase::controller::TrackMetadata {
            title: "Scrub Test Track".into(),
            artist: None,
            album: None,
            duration_us: Some(300_000_000),
        }))
        .await;
    follower_controller.set_position(10_000_000).await;
    follower_controller.play().await.unwrap();

    let evaluator = Arc::new(DriftEvaluator::new(
        clock.clone(),
        follower_controller.clone(),
        50_000,
    ));

    let now_ref = clock.reference_now();

    // 1. Initial anchor at 10.0s (follower is in sync)
    let anchor_1 = SyncAnchor {
        reference_time: now_ref,
        media_position_us: 10_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: Some("Scrub Test Track".into()),
    };
    let action_1 = evaluator.evaluate_and_reconcile(&anchor_1).await.unwrap();
    assert!(matches!(action_1, DriftAction::InSync { .. }));

    // 2. User scrubs forward rapidly: Scrub A -> 50.0s
    let ref_scrub_a = clock.reference_now();
    let anchor_scrub_a = SyncAnchor {
        reference_time: ref_scrub_a,
        media_position_us: 50_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: Some("Scrub Test Track".into()),
    };
    // Timeline discontinuity (>500ms jump) triggers immediate seek
    let action_a = evaluator
        .evaluate_and_reconcile(&anchor_scrub_a)
        .await
        .unwrap();
    match action_a {
        DriftAction::MicroSeek {
            target_position_us, ..
        } => {
            assert!(
                (target_position_us - 50_000_000).abs() < 50_000,
                "Target position should be approximately 50.0s, got {}",
                target_position_us
            );
        }
        other => panic!("Scrub A must trigger MicroSeek, got {:?}", other),
    }

    // Follower position jumped to ~50s
    let state_after_a = follower_controller.get_playback_state().await.unwrap();
    assert!(
        (state_after_a.position_us - 50_000_000).abs() < 50_000,
        "Follower position should jump to ~50s, got {}",
        state_after_a.position_us
    );

    // 3. User rapidly scrubs again: Scrub B -> 52.0s
    let ref_scrub_b = clock.reference_now();
    let anchor_scrub_b = SyncAnchor {
        reference_time: ref_scrub_b,
        media_position_us: 52_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: Some("Scrub Test Track".into()),
    };
    // This is also a discontinuity (>500ms jump from expected position after scrub A)
    let action_b = evaluator
        .evaluate_and_reconcile(&anchor_scrub_b)
        .await
        .unwrap();
    match action_b {
        DriftAction::MicroSeek {
            target_position_us, ..
        } => {
            assert!(
                (target_position_us - 52_000_000).abs() < 50_000,
                "Target position should be approximately 52.0s, got {}",
                target_position_us
            );
        }
        other => panic!("Scrub B must trigger MicroSeek, got {:?}", other),
    }

    // 4. Settling phase: continuous periodic anchors arrive during song playback
    let ref_playing = clock.reference_now();
    let anchor_playing = SyncAnchor {
        reference_time: ref_playing,
        media_position_us: 52_050_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: Some("Scrub Test Track".into()),
    };

    // Even if local playback has slight jitter or lag, settling grace period (1500ms)
    // must prevent repeated seeks or oscillations
    let (_, zone, status) = evaluator.evaluate_drift(&anchor_playing).await.unwrap();
    assert_eq!(zone, 1);
    assert_eq!(status, "Settling...");

    let action_settling = evaluator
        .evaluate_and_reconcile(&anchor_playing)
        .await
        .unwrap();
    assert!(
        matches!(action_settling, DriftAction::InSync { .. }),
        "Settling grace period must return InSync to avoid oscillation"
    );
}

// ---------------------------------------------------------------------------
// Scenario 3: Zone 2 Rate-Nudge Convergence & Clean Restoration
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_simulation_rate_nudge_recovery() {
    let clock = Arc::new(ClockSync::new(
        UdpSocket::bind("127.0.0.1:0").await.unwrap(),
        SyncConfig::default(),
        Uuid::new_v4(),
    ));

    let follower_controller = Arc::new(MockMediaController::new());
    follower_controller
        .set_track(Some(synkrophase::controller::TrackMetadata {
            title: "Nudge Test Track".into(),
            artist: None,
            album: None,
            duration_us: Some(180_000_000),
        }))
        .await;

    let now_ref = clock.reference_now();

    // Host is at 20.000s
    let anchor = SyncAnchor {
        reference_time: now_ref,
        media_position_us: 20_000_000,
        playback_rate: 1.0,
        is_playing: true,
        track_title: Some("Nudge Test Track".into()),
    };

    // Follower is at 20.120s (+120ms ahead -> Zone 2 Nudging: 50ms - 200ms)
    follower_controller.set_position(20_120_000).await;
    follower_controller.play().await.unwrap();

    let evaluator = Arc::new(DriftEvaluator::new(
        clock.clone(),
        follower_controller.clone(),
        50_000, // 50ms threshold
    ));

    // First evaluation: pushes to median history
    let _ = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    // Subsequent evaluations confirm Zone 2 (+120ms) -> rate should be slowed to 0.99x
    let action = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    match action {
        DriftAction::FineTuneRate { rate, drift_us } => {
            assert!(
                (rate - 0.99).abs() < f32::EPSILON,
                "Expected rate 0.99 for positive drift"
            );
            assert!(drift_us > 50_000);
        }
        other => panic!("Expected FineTuneRate in Zone 2, got {:?}", other),
    }

    let state = follower_controller.get_playback_state().await.unwrap();
    assert!(
        (state.rate - 0.99).abs() < f32::EPSILON,
        "Player rate should be updated to 0.99"
    );

    // Follower gradually catches up and re-enters Zone 1 (+30ms drift)
    follower_controller.set_position(20_030_000).await;
    // Push several samples to settle median filter
    for _ in 0..5 {
        let _ = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    }

    let restored_action = evaluator.evaluate_and_reconcile(&anchor).await.unwrap();
    assert!(
        matches!(restored_action, DriftAction::InSync { .. }),
        "Once drift is within Zone 1, action should be InSync"
    );

    // Verify rate was cleanly restored to 1.0x
    let final_state = follower_controller.get_playback_state().await.unwrap();
    assert!(
        (final_state.rate - 1.0).abs() < f32::EPSILON,
        "Player rate must be restored to 1.0x upon entering Zone 1"
    );
}
