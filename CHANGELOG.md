# Changelog

All notable changes to Synkrophase are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.4] - 2026-10-10

### Added
- **Join Track Reconciliation**: When joining a room, followers now receive the host's active track identity in `JoinAccepted` and automatically load the track, match the play/pause state, and seek to the current playback position.
- **Windows GSMTC Adaptive Convergence**: Replaced fixed startup delays with an adaptive polling loop that repeatedly checks WinRT media state and calls `TryPlayAsync` until playback starts.

## [0.6.3] - 2026-10-10

### Changed
- **Decoupled Internal Timeline Offset from Displayed Clock Difference**: The internal monotonic timeline offset is preserved for accurate playback calculations, while the peer table now displays the real-time residual clock difference (microseconds/milliseconds) rather than process startup deltas.

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
- **Interactive Terminal UI**: Ratatui dashboard displaying playback progress, member list, clock offset, drift state, and room chat.
- **Drift Decider**: Three-zone drift management (In-Sync `<50ms`, Rate Adjustment `50–200ms`, Micro-seek `>200ms`).

## [0.2.0] - 2026-10-08

### Added
- **External Media Controller Pivot**: Scrapped the raw audio streaming and in-app decoding engine (`rodio`, `cpal`, `tiny_http`).
- Re-architected Synkrophase as an external control plane synchronizer interfacing directly with desktop players (Spotify, Apple Music).
- macOS AppleScript controller bridge.
- Intent scheduler with exact value-skip compensation.

## [0.1.0] - 2026-10-08

### Added
- Initial prototype exploring raw LAN audio streaming via internal HTTP server and local audio sink decoding.
- PTP-lite UDP clock synchronization engine.
- mDNS local network room discovery.
