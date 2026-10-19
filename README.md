# Synkrophase

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Release](https://img.shields.io/github/v/release/landxcape/synkrophase)](https://github.com/landxcape/synkrophase/releases)

Synkrophase synchronizes media playback across multiple computers on the same local network without streaming audio over the wire.

Instead of transmitting heavy audio files, Synkrophase acts as a lightweight control-plane orchestrator: it coordinates each device's native media player directly, maintaining microsecond-accurate monotonic clock alignment and a targeted sub-80ms playback deadband via local UDP.

```text
╭ Synkrophase • Room: TESTROOM • Role: [LEADER] • Device: Host ────────────────╮
╰──────────────────────────────────────────────────────────────────────────────╯
╭ Now Playing ─────────────────────────────────────────────────────────────────╮
│ ▶ Numb   Linkin Park • Meteora  [Playing]                                    │
│ 01:05 [━━━━━━━━━━━━━━━━━━━━━━●───────────────────────────────] 03:07  (+12µs)│
╰──────────────────────────────────────────────────────────────────────────────╯
╭ Room Members ──────────────────────╮╭ Activity & Chat ───────────────────────╮
│ID       Peer            Role Offset││[21:14:00] Session started. Ready.      │
│e8a15b3a Host (You)    Leader    Ref││[21:14:03] Room: TESTROOM               │
│a3f290d1 Follower   Moderator  +12µs││[21:14:12] [Follower] Connected.        │
├─ Clock & Network Health ───────────┤│                                        │
│Sync: Locked (<80ms) | Offset: +12µs││                                        │
│RTT: 3.4ms | Jitter: 0.8ms | RTO: 50││                                        │
╰────────────────────────────────────╯╰────────────────────────────────────────╯
[Space] Play/Pause • [←/→] Seek ±5s • [↑/↓] Scroll • [n/p] Next/Prev • [v] Vol • [/] Chat • [q] Quit
```

---

## Key Highlights

- **Zero Bandwidth Overhead**: Synchronizes playback commands and reference timeline anchors rather than heavy audio streams. Audio decodes locally from each user's Spotify or Apple Music account.
- **Bidirectional Native Control**: Control playback using the terminal dashboard, CLI commands, or directly through the native player window (hitting spacebar, scrubbing the timeline, or skipping tracks in Spotify/Music propagates room-wide).
- **Sub-Millisecond Monotonic Clock**: Uses an internal PTP-lite UDP protocol with RFC-6298 smoothed RTT and jitter tracking, independent of system wall clocks or internet time servers.
- **3-Zone Adaptive Drift Correction**: Combines continuous rate-nudging on rate-capable players with closed-loop micro-seeks and hardware buffer priming compensation (<80ms acoustic deadband).
- **Zero-Touch Background Daemon**: Run headlessly in the background, query live status via structured JSON for status bars (Waybar, SketchyBar), and attach/detach interactively anytime via `synkro attach`.
- **Cross-Platform**: Native controller integrations for macOS (AppleScript), Linux (MPRIS D-Bus), and Windows (WinRT GSMTC).

---

## Supported Platforms and Players

| Operating System | Supported Players | Control Mechanism | Notes |
| :--- | :--- | :--- | :--- |
| **macOS** | Spotify, Apple Music | AppleScript | Requires terminal Automation permissions |
| **Linux** | Spotify | MPRIS D-Bus (`org.mpris.MediaPlayer2.spotify`) | Requires active D-Bus user session |
| **Windows** | Spotify | WinRT GSMTC | Requires native desktop Spotify application |

### Platform Capabilities Matrix

| Feature / Mode | macOS | Linux | Windows |
| :--- | :---: | :---: | :---: |
| **Media Player Control & Sync** | Supported | Supported | Supported |
| **PTP Clock Synchronization** | Supported | Supported | Supported |
| **Interactive TUI Dashboard** (`synkro host` / `join`) | Supported | Supported | Supported |
| **Headless Background Daemon** (`synkro daemon start`) | Supported | Supported | *Planned* (Unix-only socket) |
| **Interactive Daemon Attachment** (`synkro attach`) | Supported | Supported | *Planned* (Unix-only socket) |

> **Web Player Compatibility**: Synkrophase communicates with desktop media controllers. Web-browser tabs (such as `open.spotify.com`) do not expose OS-level controller APIs and cannot be synchronized. Always use the installed desktop client.

---

## Quick Start

### 1. Check Local Player
Ensure your media player is open, then verify Synkrophase can communicate with it:

```bash
synkro status
```

```text
=== Local Media Player Status ===
  Playing:  Yes
  Position: 65.40s
  Title:    Numb
  Artist:   Linkin Park
  Album:    Meteora
  Duration: 187.00s
```

### 2. Start a Room (Host)
Launch a session as the room leader:

```bash
synkro host MYROOM
```

*(If you omit `MYROOM`, Synkrophase automatically generates a random room code).*

On launch, Synkrophase starts the interactive dashboard and **automatically copies the join command (`synkro join MYROOM`) to your clipboard** to share with LAN peers. (Pass `--no-copy` to disable clipboard copying).

### 3. Join a Room (Follower)
On another computer on the same local network:

```bash
synkro join MYROOM
```

Upon joining, Synkrophase automatically:
1. Synchronizes its internal clock with the host via UDP PTP exchange.
2. Matches the host's current track and play/pause state.
3. Continuously aligns playback position within the sub-80ms perceptual deadband.

---

## Installation

### Homebrew (macOS & Linux)

```bash
brew tap landxcape/tap
brew install landxcape/tap/synkrophase
```

### Pre-built Binaries (macOS, Linux, Windows)

Download the latest release archive from [GitHub Releases](https://github.com/landxcape/synkrophase/releases/latest):

- **macOS (Apple Silicon)**: `synkrophase-macos-aarch64.tar.gz`
- **macOS (Intel)**: `synkrophase-macos-x86_64.tar.gz`
- **Linux (x86_64)**: `synkrophase-linux-x86_64.tar.gz`
- **Windows (x86_64)**: `synkrophase-windows-x86_64.zip` (extract and run `synkro.exe` via PowerShell or Command Prompt)

### Build from Source

Requires Rust 1.85+ (2024 edition):

```bash
git clone https://github.com/landxcape/synkrophase.git
cd synkrophase
cargo build --release
```

The compiled binary will be placed at `target/release/synkro`.

---

## Modes of Operation

### 1. Interactive Terminal Dashboard (TUI)

Running `synkro host` or `synkro join` launches a full terminal dashboard displaying live playback progress, peer offsets, network jitter, and real-time room chat.

#### Keyboard Shortcuts

| Shortcut | Description |
| :--- | :--- |
| `Space` | Toggle Play / Pause across the room |
| `←` / `→` | Seek backward / forward 5 seconds |
| `↑` / `↓` or `k` / `j` | Scroll activity log and chat history up / down |
| `PgUp` / `PgDn` | Scroll activity log by page (`Home` / `End` jumps to start / latest) |
| `n` / `p` | Skip to next / previous track in player |
| `v` | Sync local player volume to all room members |
| `c` | Copy room join invitation command to clipboard |
| `/` or `i` | Open chat and command input bar |
| `?` or `h` | Toggle keyboard help modal |
| `Esc` | Reset scroll to latest, close modal dialog, or double-tap to exit |
| `q` | Disconnect and exit session |

#### In-App Chat Slash Commands

Press `/` in the dashboard to open the command prompt:

| Command | Description |
| :--- | :--- |
| `/play` | Resume playback across all peers |
| `/pause` | Pause playback across all peers |
| `/next` | Skip to next track across all peers |
| `/prev` | Skip to previous track across all peers |
| `/seek <seconds>` | Seek to an absolute timeline position (e.g., `/seek 90`) |
| `/vol <0-100>` | Set volume across all peers (or `/vol` to mirror local volume) |
| `/transfer <peer>` | Transfer room leadership to a peer name or UUID prefix |
| `/role <peer> <role>` | Assign member role (`leader`, `moderator`, `listener`) |
| `/copy` or `/share` | Copy join command to clipboard |
| `/quit` | Leave the room and exit |
| `/shutdown` | Stop the background daemon and exit (in attached mode) |

---

### 2. Background Daemon Mode

Synkrophase can run as a detached, zero-touch background daemon without keeping a terminal window open:

```bash
# Start a room in the background as leader
synkro daemon start host MYROOM

# Or join an existing room in the background as follower
synkro daemon start join MYROOM

# Attach the interactive TUI dashboard to the active daemon
synkro attach

# Query live session status over Unix socket IPC
synkro daemon status

# Stream daemon log output in real-time
synkro daemon logs -f

# Terminate the background daemon
synkro daemon stop
```

> **Detaching**: When attached to a running daemon via `synkro attach`, pressing **`q`** or **`Esc`** cleanly detaches the TUI and returns to your shell, leaving the background daemon running undisturbed. To shut down the daemon directly from the attached TUI, type `/shutdown`.

> **Platform Support**: Background daemon mode and `synkro attach` communicate over Unix domain sockets and are currently supported on **macOS** and **Linux**. On Windows, run sessions in the interactive terminal dashboard (`synkro host` / `synkro join`). Windows Named Pipe daemon support is planned for a future milestone.

#### Status Bar Integration (JSON Output)
Pass `--json` to `synkro daemon status` to output structured JSON for Waybar, SketchyBar, polybar, or shell scripts:

```bash
synkro daemon status --json
```

```json
{
  "running": true,
  "room_code": "MYROOM",
  "role": "Leader",
  "is_playing": true,
  "position_us": 65400000,
  "clock_offset_us": 0,
  "peers": [
    {"device_id": "a3f290d1-...", "name": "Follower", "role": "Moderator", "clock_offset_us": 12}
  ]
}
```

---

### 3. Remote CLI Control

When a session is active (either foreground TUI or background daemon), playback and telemetry can be inspected or commanded from any other terminal window:

```bash
# Playback Controls
synkro play                    # Resume playback across room
synkro pause                   # Pause playback across room
synkro resume                  # Resume playback across room (alias for play)
synkro next                    # Skip to next track
synkro prev                    # Skip to previous track
synkro skip                    # Skip to next track (alias for next)
synkro seek 45                 # Seek to 45 seconds
synkro volume 80               # Set room volume to 80%
synkro volume                  # Mirror current host volume to room

# Room Telemetry & Collaboration
synkro status                  # Inspect local media player status
synkro sync                    # View live room sync offsets and drift
synkro debug                   # Output raw session JSON status
synkro share                   # Copy room join command to clipboard
synkro chat "Starting now!"    # Post a message to the room chat
synkro transfer <PEER>         # Hand off host to a peer name or UUID
synkro role <PEER> <ROLE>      # Set role ('moderator' or 'listener')
```

---

### 4. Diagnostics & Auto-Repair (`synkro doctor`)

Before hosting or joining a session, or if you encounter connection or automation issues, run `synkro doctor` to audit your local system environment:

```bash
# Run interactive system diagnostics
synkro doctor

# Automatically repair fixable issues (stale sockets, missing directories, launch player)
synkro doctor --fix

# Output machine-readable JSON for status bars or scripts
synkro doctor --json
```

The diagnostic suite audits:
- **Media Controller & Permissions**: Verifies player responsiveness (Spotify / Apple Music), measures round-trip latency, and checks macOS AppleScript automation permissions.
- **Network Interface & Monotonic Clock**: Detects active LAN IPv4 interfaces and tests OS high-resolution monotonic timer granularity ($< 50\text{ns}$).
- **Port Availability**: Confirms UDP ports `5870` (PTP Clock) and `5871` (Session messaging) are free, intelligently recognizing ports held by an active Synkrophase background daemon.
- **Multicast Discovery**: Checks mDNS multicast socket capabilities (`224.0.0.251:5353`).
- **Daemon & IPC Health**: Detects stale socket files and orphaned PID locks from abnormal daemon terminations.

---

## Roles and Permissions

Synkrophase enforces role-based access control (RBAC) across all interfaces (TUI, CLI, and native player interactions):

| Capability / Action | Leader (Host) | Moderator | Listener |
| :--- | :---: | :---: | :---: |
| Play / Pause (CLI, TUI, or Native Player) | Yes | Yes | No |
| Seek Timeline (CLI, TUI, or Native Player) | Yes | Yes | No |
| Skip Next / Previous (CLI, TUI, or Native Player) | Yes | Yes | No |
| Sync Room Volume | Yes | Yes | No |
| Send Chat Messages | Yes | Yes | Yes |
| View Progress and Peer Offsets | Yes | Yes | Yes |
| Assign Member Roles (`/role`) | Yes | No | No |
| Transfer Leadership (`/transfer`) | Yes | No | No |

- The room creator starts as **Leader**.
- The first peer to join is automatically granted **Moderator** status; subsequent peers join as **Listeners**.
- Leaders can promote or demote members at any time using `synkro role <peer> <moderator|listener>`.

---

## How Synchronization Works

### Control-Plane Clock Precision vs. Audible Output Alignment
It is important to distinguish between control-plane clock synchronization and physical acoustic playback:
- **Control-Plane Monotonic Reference Clock**: PTP-lite round-trip UDP probe exchanges establish a shared monotonic reference timeline across local peers with sub-millisecond precision.
- **Audible Output Alignment**: Because Synkrophase coordinates third-party media players (Spotify, Apple Music) rather than streaming raw PCM audio directly to DAC hardware, audible synchronization is bounded by the ~80ms perceptual deadband (`Zone 1`), external player IPC latencies, and native OS audio buffer queues. This design guarantees zero network bandwidth consumption and preserves local account playback quality.

### Monotonic Sequencing & Jitter Tracking
1. Follower nodes exchange lightweight PTP probe packets with the leader over UDP port 5870.
2. The engine tracks Smoothed Round-Trip Time (`SRTT`) and Round-Trip Variance (`RTTVAR`) using an RFC-6298 moving average.
3. Probe timeouts dynamically adapt between `50ms` and `1,500ms` based on observed latency and packet losses, suppressing false drops during transient Wi-Fi congestion.
4. Stale or out-of-order anchor packets delayed by network jitter are discarded via monotonic timeline watermarks.

### 3-Zone Adaptive Drift Management

While playing, followers continuously reconcile their local player timeline against incoming leader anchors:

| Zone | Drift Range | Action Taken | Status Display |
| :--- | :--- | :--- | :--- |
| **Zone 1** | `< 80ms` | No action required. Audio is within perceptual phase alignment. | `Locked (<80ms)` |
| **Zone 2** | `80ms – Adaptive Threshold` | Fine rate adjustment ($0.99\times$ or $1.01\times$) on players with rate control (capped at a 3.5s deadline). On fixed-rate players (macOS AppleScript, Windows GSMTC, or standard MPRIS), requires 3.0s continuous persistence before escalating to seek. | `Nudging (+85.0ms)` |
| **Zone 3** | `> Adaptive Threshold (200ms–350ms)` | Corrective micro-seek pre-compensated by actuation delay and buffer priming lead. | `Drifting (+240.0ms)` |

#### Closed-Loop Seek Priming Compensation
External player decoders require time to flush existing buffers and prime their DAC queues upon receiving a seek command. Synkrophase applies a 120ms baseline priming lead on seeks:

$$\text{target\_seek\_position} = \text{expected\_position} + \text{actuation\_delay} + \text{priming\_lead}$$

Following a seek, a 2,500ms post-seek settling window prevents repetitive seek thrashing. On the first steady-state measurement after settling, the engine auto-calibrates the residual landing error, dynamically tuning the buffer priming lead ($0\text{--}350\text{ms}$) for subsequent operations.

### Bidirectional Native Player Synchronization
Synkrophase actively observes the physical player process:
- **Physical Control Propagation**: When a user with `Leader` or `Moderator` privileges toggles playback, scrubs the track bar, or skips songs directly in Spotify or Apple Music, the local engine detects the state transition via single-flight active player checks.
- **Reverse Intent Synthesis**: The engine immediately packages the physical player action into a scheduled `PlaybackIntent` with clock-aligned execution and broadcasts updated authoritative `SyncAnchor`s across the room.
- **Role Isolation**: Actions initiated by read-only `Listener` nodes remain purely local and are rejected by the leader's RBAC gate, protecting the room against accidental desynchronization.

### Automatic Host Failover & Consistency Model
If the leader leaves or disconnects, remaining peers automatically elect a new Leader using a deterministic lowest-UUID tie-break:
- Timeline watermarks reset dynamically on failover so followers immediately synchronize against the new leader's clock.
- If a temporary network partition occurs, isolated peers continue local playback independently. Upon reconnection, conflicting leader claims are deterministically reconciled via role hierarchy (`Leader > Moderator > Listener`), converging the room back to a single reference timeline without stopping music.

---

## OS Permissions and Setup

### macOS Setup
macOS requires explicit permission for terminal applications to control Spotify or Apple Music via AppleScript:
1. Open **System Settings > Privacy & Security > Automation**.
2. Locate your terminal emulator (Terminal, iTerm2, Alacritty, Ghostty, Kitty, etc.).
3. Ensure the toggle for **Spotify** and/or **Music** is enabled.
4. If permissions were previously denied or became unresponsive, reset them via:
   ```bash
   tccutil reset AppleEvents
   ```

### Linux Setup
Synkrophase communicates with Spotify over the user's session D-Bus:
- Ensure `DBUS_SESSION_BUS_ADDRESS` is set in your environment (standard in modern desktop environments).
- Native Spotify packages (`.deb`, `.tar.gz`, Snap, or Flatpak with D-Bus access) work out of the box.

### Windows Setup
- Requires the native desktop Spotify app installed from the Spotify website or Microsoft Store.
- The Spotify application must be running before Synkrophase can discover and control it.

---

## Network and Port Configuration

Synkrophase uses UDP for low-latency communication on your local network:

| Port | Protocol | Purpose |
| :--- | :--- | :--- |
| `5870` | UDP | PTP clock synchronization exchanges |
| `5871` | UDP | Session messaging, heartbeats, playback intents, and chat |
| `5353` | UDP | mDNS room discovery (`_synkrophase._udp.local.`) |

> **Security Notice**: Synkrophase is designed strictly for trusted local networks (home and office LANs). All control packets and discovery messages are unencrypted UDP payloads. Do not expose Synkrophase ports to untrusted public networks or the open internet.

### Bypassing mDNS / Router AP Isolation
If room discovery does not find your host (common when Wi-Fi routers have **Client Isolation / AP Isolation** enabled or block multicast UDP packets):
1. Determine the host machine's LAN IP address (`ipconfig` on Windows, `ip a` or `ifconfig` on Linux/macOS).
2. Connect directly by supplying the `--leader-addr` flag:
   ```bash
   synkro join MYROOM --leader-addr 192.168.1.50:5871
   ```

---

## CLI Options Reference

### Global Options
Available on all commands:
- `--name <NAME>`: Custom display name for this device in the room table.
- `--ephemeral`: Use a temporary, randomized device ID (useful when testing multiple instances on a single machine).

### `synkro host`
```bash
synkro host [ROOM_CODE] [OPTIONS]
```
- `[ROOM_CODE]`: Optional room code. If omitted, a random 6-character code is generated.
- `--room-code <ROOM_CODE>`: Alternative flag to specify custom room code.
- `--no-copy`: Disable automatically copying the join invitation command to the system clipboard on launch.
- `--clock-port <PORT>`: UDP port for clock synchronization responder [default: `5870`].
- `--session-port <PORT>`: Local UDP port for session messaging [default: `5871`].

### `synkro join`
```bash
synkro join <ROOM_CODE> [OPTIONS]
```
- `<ROOM_CODE>`: Session room code to discover and join.
- `--leader-addr <IP:PORT>`: Direct IP and port of the host session, bypassing mDNS discovery.
- `--leader-id <UUID>`: Optional leader UUID for manual directed joins.
- `--leader-clock-port <PORT>`: Target leader clock synchronization port [default: `5870`].
- `--session-port <PORT>`: Local UDP port for session messaging [default: `5871`].

### `synkro daemon start`
```bash
synkro daemon start host [ROOM_CODE] [--clock-port <PORT>] [--session-port <PORT>]
synkro daemon start join <ROOM_CODE> [--leader-addr <IP:PORT>] [--session-port <PORT>]
```

---

## Troubleshooting

### "Different Track" Status on Follower
- **Symptom**: Follower display reports `Different Track (<title>)` when the host changes songs.
- **Cause**: The follower does not have access to that track (e.g., track is regionally restricted or requires an active subscription).
- **Resolution**: Verify that both users have access to the same track or playlist in Spotify or Apple Music.

### Room Not Discovered (mDNS Failure)
- **Symptom**: `synkro join MYROOM` hangs while searching for the host.
- **Resolution**:
  1. Confirm both machines are connected to the same Wi-Fi network and subnet.
  2. Verify your router does not enforce AP Isolation / Client Isolation.
  3. Bypass multicast discovery directly by supplying the host's LAN IP: `synkro join MYROOM --leader-addr <HOST_IP>:5871`.

### Playback Unresponsive on macOS
- **Symptom**: Commands succeed in terminal but Spotify/Apple Music does not react.
- **Resolution**: Verify permissions under **System Settings > Privacy & Security > Automation**, ensuring your terminal emulator has access to Spotify or Music. Run `tccutil reset AppleEvents` to clear corrupted permission states.

---

## Development and Testing

Synkrophase includes a local UDP network simulation proxy (`NetworkChaosProxy`) that injects artificial delay, jitter, packet drops, and simulated hardware clock drift on `localhost`:

```bash
# Run unit and integration test suite
cargo test

# Run UDP network chaos simulation suite
cargo test --test simulation
```

---

## License

Dual-licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.
