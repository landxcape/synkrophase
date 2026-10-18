# Changelog

All notable changes to Synkrophase are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.8.0] - 2026-10-18

### Added
- **Interactive TUI Daemon Attachment (`synkro attach`)**: Added `synkro attach` and `synkro daemon attach` commands to attach the full interactive Ratatui dashboard to a running background daemon over Unix domain socket IPC (`synkro.sock`).
- **Unified TUI Abstraction (`TuiBackend`)**: Unified command dispatch across in-process foreground sessions (`LocalEngineBackend`) and attached daemon sessions (`IpcBackend`), sharing identical widgets, keybindings, progress tracking, and chat buffer.
- **IPC Real-Time Event Streaming**: Extended `IpcServer` with `broadcast_app_event` to stream `playback_update`, `peer_update`, `drift_update`, `network_update`, and room `log`/chat messages to connected subscribers. Periodic 200ms status polling activates only when subscribers are connected.
- **Clean Detach Lifecycle**: Pressing `q` or `Esc` in attached mode detaches cleanly, leaving the background daemon running undisturbed. An explicit `/shutdown` slash command allows remote daemon shutdown.
- **Role-Gated Bidirectional Native Player State Synchronization**: Native player controls (play, pause, scrub) initiated in Spotify or Apple Music by the `Leader` or `Moderator` propagate room-wide as scheduled `PlaybackIntent`s and authoritative `SyncAnchor`s. Read-only `Listener` nodes are rejected by leader RBAC.
- **Active Daemon Guardrails**: `synkro host` and `synkro join` detect running background daemons on startup and abort with an informative message pointing to `synkro attach`.

### Fixed
- **Windows MSVC Compilation**: Fixed cross-platform target builds by removing Unix-only gating on `DaemonPaths` in `IpcClient` and properly gating terminal I/O imports in the attached runner.
- **Play/Pause Anchor Reconciliation**: `DriftEvaluator` now pauses the local player when the incoming anchor is paused, resumes when playing, and aligns paused playback position discrepancies exceeding 200ms.
- **Joiner Initial Playback State**: Leader persists its authoritative `SyncAnchor` in `SessionState` so newly joined followers synchronize track and play/pause status immediately upon `JoinAccepted`.

## [0.7.4] - 2026-10-15

### Added
- **Closed-Loop Seek Priming Compensation**: Added `default_seek_priming_lead_us()` to `MediaController` trait with a 120ms baseline across macOS, Linux, and Windows controllers (0ms for Mock). Seeks pre-compensate for decoder flush and DAC ring buffer fill delay (`target_seek_position_us = expected_position_us + actuation_delay_us + priming_lead_us`).
- **Post-Settling Residual Auto-Calibration**: `DriftEvaluator` measures residual drift on the first steady-state sample following the 2,500ms post-seek settling window, adjusting the priming lead by residual error ($e_{\text{residual}}$) clamped within $0\text{--}350\text{ms}$.
- **Discontinuity Protection**: Manual scrubs and track transitions disarm pending seek calibration to prevent false lead adjustments.

## [0.7.3] - 2026-10-15

### Fixed
- **Zone 1 Deadband Threshold**: Widened the Zone 1 deadband from $50\text{ms}$ to $80\text{ms}$ to prevent acoustic micro-seek thrashing on players that lack rate-scaling capabilities.
- **Fixed-Rate Player Zone 2 Persistence Gate**: In Zone 2 ($80\text{ms}\text{--}\text{Adaptive Threshold}$), controllers without rate control must persist in Zone 2 continuously for $\ge 3.0\text{s}$ before escalating to seek, filtering out transient network or playback jitter.
- **Time-Bounded Rate Nudging**: Active rate nudging ($0.99\times / 1.01\times$) on rate-capable players is capped at a $3.5\text{s}$ hard deadline; if playback does not re-enter Zone 1 within this window, the engine restores the rate to $1.0\times$ and falls through to a corrective micro-seek.
- **Extended Post-Seek Settling Window**: Increased the post-seek grace settling window from $1,500\text{ms}$ to $2,500\text{ms}$ to accommodate slow player buffer priming before evaluating subsequent drift.

## [0.7.2] - 2026-10-15

### Fixed
- **Linux MPRIS Metadata & Track Recognition**: Implemented recursive D-Bus variant unwrapping in `LinuxMediaController` to properly unpack `a{sv}` metadata dictionaries. Fixes an issue where song titles and artists appeared unrecognized or empty on Linux.
- **Linux Drift Settling & Seek Execution**: In `seek_to()`, dynamically extract the player's active `mpris:trackid` from metadata rather than using a static fallback path. Fixes a bug where Spotify silently dropped `SetPosition` commands due to mismatched track identifiers, leaving Linux followers permanently drifting in Zone 3 (+500ms).
- **Linux Flatpak/Snap Player Prioritization**: Matched player service bus names using `.contains("spotify")` instead of `.ends_with(".spotify")`, ensuring instances running under Flatpak or Snap are prioritized correctly.

## [0.7.1] - 2026-10-15

### Fixed
- **Host Anchor Filtering**: Enforced strict anchor acceptance rules in `SessionMessageRuntime`. Leaders now unconditionally reject incoming `SyncAnchor` envelopes from peers, and followers strictly reject anchors from any node other than the active leader. Prevents desynchronization if demoted hosts or rogue peers broadcast sync anchors.

### Documentation
- **Clock Precision vs. Audible Sound Grounding**: Clarified the technical distinction between PTP-lite control-plane clock sync (<1ms precision) and physical acoustic output bounded by the ~50ms perceptual deadband (`Zone 1`), external player IPC latencies, and native OS audio buffer queues.
- **Partition Consistency Model**: Documented the eventual-convergence model via role hierarchy (`Leader > Moderator > Listener`) and lowest-UUID tie-breaks on network reconnection, explaining the intentional omission of distributed consensus (Raft/Paxos).

## [0.7.0] - 2026-10-15

### Fixed
- **Windows GSMTC Timeline Position Extrapolation**: Extrapolate live playback positions from `timeline.LastUpdatedTime()` in `windows.rs`. Resolves an issue where Windows followers remained permanently stuck in settling or drifting states due to WinRT returning a static position snapshot.
- **Failover Drift Evaluation Suppression**: Followers promoted to host immediately halt active drift tasks and emit `Leader (Ref)` to prevent self-evaluating against departed leaders.
- **Dynamic Leader Address Resolution in Clock Refresh**: The follower clock refresh loop now dynamically queries the active leader's network address and clears residual offsets upon host promotion, preventing RTO probe timeouts and exponential backoff after failover.
- **Spec-Driven Rate Capability Detection & Drift Fallback**: Added `supports_rate_control()` to `MediaController`. On Linux, the controller dynamically inspects MPRIS `MinimumRate` and `MaximumRate` D-Bus properties to detect fixed-rate players. If rate control is unsupported or unconfirmed, drift evaluation proceeds directly to the outlier-confirmed seek tier rather than stalling.

### Changed
- **Legacy Controller Pruning**: Removed orphaned `SyncController`, `FollowerSyncRuntime`, and uninvoked sync hooks in `SessionMessageRuntime`, standardizing all drift evaluation on `DriftEvaluator`.

## [0.6.9] - 2026-10-12

### Fixed
- **Timeline Synchronization Across Leader Transitions**: Fixed an issue where followers could stop synchronizing after a host departed or leadership changed, caused by stale packet filters rejecting valid timestamps from the new leader.
- **Drift Recovery Responsiveness**: Capped the adaptive seek threshold under noisy network conditions to prevent prolonged recovery delays during high jitter.

### Added
- **Failover Simulation Coverage**: Extended the chaos simulation suite to verify seamless re-synchronization across dynamic host transitions.

## [0.6.8] - 2026-10-12

### Added
- **RFC-6298 Adaptive Clock Timeout**: Dynamic retransmission and probe timeouts (`RTO`) based on smoothed round-trip time (`SRTT`) and variance (`RTTVAR`), scaling between a 50ms baseline floor on low-latency LANs and 1,500ms on congested networks.
- **Monotonic Packet Sequencing**: Reference-time watermark checking in `SessionState` to discard stale, out-of-order anchor packets delayed in transit.
- **Adaptive Drift Thresholding**: Dynamic Zone 3 micro-seek boundaries scaled with measured network variance to prevent false seek cuts during transient jitter bursts.
- **Live Network Telemetry**: Integrated real-time tracking of `RTT`, `Jitter`, `RTO`, and discarded stale packet counts into `NetworkStats`, displayed in the TUI health panel and `synkro status`.
- **Sustained Simulation Benchmark Suite**: Added 11 automated real-world chaos benchmarks in `tests/simulation.rs`, including 40-cycle sustained hardware crystal PPM drift and 30-anchor continuous noisy Wi-Fi streaming.

## [0.6.7] - 2026-10-11

### Fixed
- **Resilient Playback-Rate Restoration**: The evaluator now retains adjusted-rate state and retries resetting the player's rate to 1.0 if the media controller encounters a transient failure, ensuring the player is never permanently stranded at $0.99\times$ or $1.01\times$.
- **Clock UDP Receiver Error Resilience**: Converted the clock synchronization receiver task into an error-resilient loop that logs warnings and backs off on transient socket errors rather than terminating permanently.
- **Rate-Aware Scrub Detection**: Multiplied elapsed reference time by the active playback rate when predicting expected leader media position, eliminating false scrub detection alarms during non-$1.0\times$ playback.

## [0.6.6] - 2026-10-11

### Fixed
- **Clock Sync Socket Contention**: Replaced multiple concurrent `recv_from()` calls on the clock UDP socket with a single dedicated receiver loop in `ClockSync` that dispatches incoming requests and broadcasts responses to callers.
- **Clock Sync Exact Target Matching**: Fixed `measure_offset` packet filtering to compare exact `SocketAddr` (`src == target_addr`) rather than port-only matching, eliminating response cross-contamination across peers.
- **Clock Reference Underflow Guard**: Replaced raw casting in `reference_now()` with `now.saturating_add(self.offset()).max(0) as u64` to prevent negative offsets from overflowing to `u64::MAX`.
- **Post-Seek Settling Grace Period**: Introduced `is_timeline_discontinuity()` to distinguish continuous periodic anchors from true player disruptions (scrubs, track switches, state changes), ensuring routine 1-second anchors honor the 1500ms post-seek settling window without re-seeking.
- **Playback Rate Restoration**: Tracked active rate adjustments in `DriftEvaluator` and automatically restored the player rate to 1.0 upon returning to Zone 1 (`Locked`) or initiating a micro-seek.
- **Unified Drift Smoothing & Reconciler Hysteresis**: Consolidated separate display and reconciliation history buffers into a single shared median window. Applied full hysteresis banding (+25ms release margin for Zone 1) to `evaluate_and_reconcile()`, preventing rate flapping around the 50ms boundary.
- **Broadcaster Pause/Resume Trajectory Tracking**: Added `last_is_playing` tracking to immediately broadcast anchors on play/pause transitions and reset sample baselines on resume, eliminating false scrub detection alarms after pauses.
- **Instant Anchor Notification Waking**: Updated follower and background daemon drift loops to listen to `notify_event`, executing drift evaluations immediately upon anchor arrival over UDP.

### Added
- **CI Quality Gate**: Added automated `cargo test --all-targets`, `cargo clippy --all-targets -- -D warnings`, and `cargo fmt --check` pre-requisite verification to the GitHub Actions release workflow.

## [0.6.5] - 2026-10-11

### Added
- **TUI Activity & Chat Scrollback**: Added interactive history navigation using `↑`/`↓`, `k`/`j`, `PageUp`/`PageDown`, `Home`, and `End`, with a dynamic header badge (`Activity & Room Chat [↑ +N]`) and border highlight.
- **TUI Auto-Scroll Boundary Management**: Chronological message ordering with bounded auto-scroll that docks recent messages inside panel borders.
- **Documentation**: Expanded README with complete CLI command listings, port specifications (`5870/UDP`, `5871/UDP`), clipboard behavior, and OS permission requirements.

## [0.6.4] - 2026-10-10

### Added
- **Join Track Reconciliation**: When joining a room, followers receive the host's active track in `JoinAccepted` and automatically load the track, mirror play/pause state, and seek to the current playback position.
- **Windows GSMTC Adaptive Convergence**: Replaced fixed startup delays with an adaptive polling loop that repeatedly checks WinRT media state and calls `TryPlayAsync` until playback starts.

## [0.6.3] - 2026-10-10

### Changed
- **Decoupled Internal Timeline Offset from Displayed Clock Difference**: Preserved the internal monotonic timeline offset for playback calculations, while reporting the real-time residual clock difference in microseconds/milliseconds in the peer table.

## [0.6.2] - 2026-10-10

### Fixed
- **Monotonic Clock Alignment**: Replaced system wall-clock timestamps (`SystemTime`) in the clock synchronization engine with monotonic elapsed time (`Instant`), eliminating NTP and timezone skews between macOS and Windows machines.
- **Windows Auto-Play**: Dispatched `TryPlayAsync` following Spotify URI launch commands on Windows.

## [0.6.0] - 2026-10-10

### Added
- **Cross-Platform Controllers**:
  - Linux native MPRIS D-Bus media controller (`zbus`).
  - Windows native GSMTC media controller (`windows` WinRT crate).

## [0.5.0] - 2026-10-10

### Added
- **Background Daemon Mode**: Added `synkro daemon` command to run synchronization as a background service.
- **Local IPC Socket**: Unix domain socket at `~/.synkrophase/synkro.sock` accepting newline-delimited JSON commands.
- **Direct CLI Controls**: Commands like `synkro play`, `synkro pause`, `synkro next`, and `synkro volume` communicate directly with the local daemon.

## [0.4.2] - 2026-10-10

### Added
- **Automatic Track Mirroring**: Followers automatically load the host's active track when the host changes songs in Spotify or Apple Music.
- **Role Hierarchy**: Added permissions for Leader, Moderator, and Listener roles (`/role` and `/transfer`).

## [0.3.4] - 2026-10-09

### Added
- **Drift Management**: Three-zone drift decider (In-Sync `<50ms`, Rate Adjustment `50–200ms`, Micro-seek `>200ms`).
- **TimelineTracer**: Wait until reference deadlines using clock-aligned scheduling.

## [0.3.0] - 2026-10-08

### Added
- **Interactive Terminal UI**: Ratatui dashboard displaying playback progress, member list, clock offset, drift state, and room chat.
- **Track & Volume Controls**: Next/previous track navigation and room volume synchronization.

## [0.2.0] - 2026-10-08

### Added
- **External Media Controller Architecture**: Scrapped the initial in-app audio streaming engine (`rodio`, `cpal`, `tiny_http`) and pivoted to an external control plane synchronizer.
- macOS AppleScript controller bridge for Spotify and Apple Music.
- Intent scheduler with exact value-skip compensation.

## [0.1.0] - 2026-05-07

### Added
- Initial prototype exploring raw LAN audio streaming via internal HTTP server and local audio sink decoding.
- PTP-lite UDP clock synchronization engine.
- mDNS local network room discovery.
