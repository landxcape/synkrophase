# Changelog

All notable changes to Synkrophase are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
