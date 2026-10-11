# Changelog

All notable changes to Synkrophase are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
