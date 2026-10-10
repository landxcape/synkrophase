# Synkrophase

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Release](https://img.shields.io/github/v/release/landxcape/synkrophase)](https://github.com/landxcape/synkrophase/releases)

Synkrophase synchronizes media playback across multiple computers on the same local network.

Instead of streaming audio over the network, Synkrophase controls each computer's native media player directly. When one person plays, pauses, seeks, or changes a track, all connected computers follow along in sync.

---

## Supported Platforms and Players

| Operating System | Supported Players | Control Mechanism | Notes |
| :--- | :--- | :--- | :--- |
| **macOS** | Spotify, Apple Music | AppleScript | Requires Automation permission |
| **Linux** | Spotify | MPRIS D-Bus (`org.mpris.MediaPlayer2.spotify`) | Requires active D-Bus session |
| **Windows** | Spotify | WinRT GSMTC | Requires native desktop Spotify app |

> **Note on Web Players**: Synkrophase communicates with desktop media controllers. Web-browser tabs (like open.spotify.com) do not expose OS-level controller APIs and cannot be controlled. Always use the installed desktop app.

---

## Installation

### Homebrew (macOS & Linux)

```bash
brew tap landxcape/tap
brew install landxcape/tap/synkrophase
```

### Pre-built Binaries (Windows, macOS, Linux)

Download pre-compiled binaries from [GitHub Releases](https://github.com/landxcape/synkrophase/releases/latest).

- **Windows**: Extract `synkrophase-windows-x86_64.zip` and run `synkro.exe` from PowerShell or Command Prompt.
- **macOS (Apple Silicon)**: `synkrophase-macos-aarch64.tar.gz`
- **macOS (Intel)**: `synkrophase-macos-x86_64.tar.gz`
- **Linux (x86_64)**: `synkrophase-linux-x86_64.tar.gz`

### Build from Source

Requires Rust 1.85+ (2024 edition):

```bash
git clone https://github.com/landxcape/synkrophase.git
cd synkrophase
cargo build --release
```

The compiled binary will be placed at `target/release/synkro`.

---

## OS Permissions and Setup

### macOS Setup
When Synkrophase runs for the first time, macOS will ask for permission to control Spotify or Music:
1. Open **System Settings > Privacy & Security > Automation**.
2. Find your terminal emulator (Terminal, iTerm2, Alacritty, Ghostty, etc.).
3. Ensure checkboxes for **Spotify** and/or **Music** are toggled **ON**.
4. If permissions were previously denied, commands will fail silently. Reset them using:
   ```bash
   tccutil reset AppleEvents
   ```

### Linux Setup
Synkrophase communicates over the user's session D-Bus.
- Ensure `DBUS_SESSION_BUS_ADDRESS` is set in your environment (standard in modern desktop environments).
- Native Spotify packages (`.deb`, `.tar.gz`, Snap, or Flatpak with D-Bus access) work out of the box.

### Windows Setup
- Requires the native Spotify desktop application installed from the Spotify website or Microsoft Store.
- The app must be running before Synkrophase can discover and control it.

---

## Network and Port Requirements

Synkrophase uses UDP for low-latency communication on your local network:

| Port | Protocol | Purpose |
| :--- | :--- | :--- |
| `5870` | UDP | PTP clock synchronization exchanges |
| `5871` | UDP | Session messaging, heartbeats, playback intents, and chat |
| `5353` | UDP | mDNS room discovery (`_synkrophase._udp.local.`) |

If room discovery does not find your host (e.g., when your Wi-Fi router has **Client Isolation** enabled or blocks multicast):
1. Find the host's local IP address (`ipconfig` on Windows, `ifconfig` or `ip a` on macOS/Linux).
2. Connect directly by passing the `--leader-addr` flag:
   ```bash
   synkro join MYROOM --leader-addr 192.168.1.50:5871
   ```

---

## Quick Start

### 1. Check Local Player
Ensure your media player is running, then check its status:

```bash
synkro status
```

Output:
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

```bash
synkro host MYROOM
```

This starts the room, opens the interactive dashboard, and **automatically copies the join command (`synkro join MYROOM`) to your clipboard** so you can paste and send it to your friends.

> **Tip**: You can copy the join command again at any time by pressing **`c`** inside the dashboard, typing **`/copy`** in chat, or running **`synkro share`** from another terminal. If you don't want the command copied automatically on launch, pass `--no-copy`.

### 3. Join a Room (Follower)

On another computer on the same network:

```bash
synkro join MYROOM
```

When you join:
- Synkrophase automatically synchronizes its reference clock with the host.
- It queries the host's current track and loads it in your local player.
- It matches the host's play/pause state and aligns playback to the exact position.

---

## How Synchronization Works

### Clock Synchronization
Devices do not rely on system wall clocks or time zones. When a follower joins:
1. It runs a lightweight PTP (Precision Time Protocol) round-trip exchange with the host over UDP port 5872.
2. It measures network latency and calculates the difference between its monotonic clock and the host's monotonic clock.
3. Every 10 seconds, it performs a background measurement to track oscillator drift and network jitter.
4. The **Offset** displayed in the room table shows this live residual clock difference (e.g. `+14µs`, `-0.2ms`).

### Drift Management
While playing, followers periodically evaluate their local playback position against the host's reference timeline. Playback is classified into three zones:

| Zone | Drift Range | Action Taken | Status Display |
| :--- | :--- | :--- | :--- |
| **Zone 1** | `< 50ms` | No action needed. Audio is in sync. | `Locked (<50ms)` |
| **Zone 2** | `50ms – 200ms` | Fine rate adjustment or gentle alignment. | `Nudging (+85.0ms)` |
| **Zone 3** | `> 200ms` | Corrective micro-seek directly to the target timestamp. | `Drifting (+240.0ms)` |

A 1500ms post-seek grace window prevents repeated seeking while players buffer.

### Automatic Host Failover
If the host leaves or disconnects, the remaining peers automatically elect a new Leader using a deterministic UUID tie-break, keeping playback in sync without terminating the room.

---

## Roles and Permissions

Synkrophase supports three member roles:

| Action / Capability | Leader (Host) | Moderator | Listener |
| :--- | :---: | :---: | :---: |
| Play / Pause | Yes | Yes | No |
| Seek Timeline | Yes | Yes | No |
| Skip Next / Previous | Yes | Yes | No |
| Sync Room Volume | Yes | Yes | No |
| Send Chat Messages | Yes | Yes | Yes |
| View Progress and Peer Offsets | Yes | Yes | Yes |
| Assign Member Roles (`/role`) | Yes | No | No |
| Transfer Host (`/transfer`) | Yes | No | No |

- When hosting, the host begins as **Leader**.
- The first peer to join is assigned **Moderator**; subsequent peers join as **Listeners**.
- The Leader can promote or demote any member using `/role <peer> <moderator|listener>`.

---

## Interactive Dashboard (TUI)

When hosting or joining a room interactively, Synkrophase launches a full terminal dashboard:

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
╰────────────────────────────────────╯╰────────────────────────────────────────╯
[Space] Play/Pause • [←/→] Seek ±5s • [n/p] Next/Prev • [v] Vol • [/] Chat • [q] Quit
```

### Keyboard Shortcuts

| Shortcut | Description |
| :--- | :--- |
| `Space` | Toggle Play / Pause across the room |
| `←` / `→` | Seek backward / forward 5 seconds |
| `n` | Next track in player |
| `p` | Previous track in player |
| `v` | Sync local volume to all room members |
| `c` | Copy room join command to clipboard |
| `/` or `i` | Open chat and command input bar |
| `?` or `h` | Toggle keyboard help modal |
| `q` | Disconnect and exit (requires double-tap `Esc` or `q`) |

### Chat Commands

Type `/` in the dashboard to access commands:

| Command | Description |
| :--- | :--- |
| `/play` | Resume playback across all peers |
| `/pause` | Pause playback across all peers |
| `/next` | Skip to the next track |
| `/prev` | Skip to the previous track |
| `/seek <seconds>` | Seek to an absolute timeline position (e.g. `/seek 90`) |
| `/vol <0-100>` | Set volume across all peers (e.g. `/vol 80`, or `/vol` to mirror local) |
| `/transfer <peer>` | Transfer host to a peer by name or 8-character ID prefix |
| `/role <peer> <role>` | Assign role (`leader`, `moderator`, `listener`) to a peer |
| `/copy` or `/share` | Copy join command to clipboard |
| `/quit` | Leave the room and exit |

---

## Background Daemon Mode

Synkrophase can run as a background service without an open terminal window:

```bash
# Start host daemon in the background
synkro daemon start host MYROOM

# Or join a room in the background
synkro daemon start join MYROOM

# Inspect live daemon status
synkro daemon status

# Stream daemon logs in real-time
synkro daemon logs -f

# Stop the daemon
synkro daemon stop
```

### Status Output for Scripts and Status Bars
Add `--json` to `synkro daemon status` to output structured JSON for integration with Waybar, SketchyBar, polybar, or custom shell scripts:

```bash
synkro daemon status --json
```

Output:
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

## Command-Line Interface (CLI) Reference

When a session is active (interactive room or background daemon), these commands can be executed from any terminal window:

```bash
# Playback Controls
synkro play                    # Resume playback across room
synkro pause                   # Pause playback across room
synkro resume                  # Resume playback across room (alias for play)
synkro next                    # Skip to next track
synkro prev                    # Skip to previous track
synkro skip                    # Skip to next track (alias for next)
synkro seek 45                 # Seek to 45 seconds
synkro volume 75               # Set room volume to 75%
synkro volume                  # Mirror current host volume to room

# Room Telemetry & Collaboration
synkro status                  # Inspect local media player status
synkro sync                    # View live room sync offsets and drift
synkro debug                   # Output raw session JSON status
synkro share                   # Copy room join command to clipboard
synkro chat "Starting now!"    # Post a message to the room chat
synkro transfer <PEER>         # Hand off host to a peer name or UUID
synkro role <PEER> <ROLE>      # Set role ('moderator' or 'listener')

# Global Options
synkro --name "Alice" host ROOM   # Set custom display name
synkro --ephemeral join ROOM      # Use temporary device ID (useful for multi-terminal testing)
```

---

## Configuration Options

These flags can be passed to `synkro host` or `synkro join`:

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--lead-time-ms` | `350` | Buffer time (in ms) allowed for network transmission before an intent executes |
| `--threshold-ms` | `50` | Acceptable playback drift margin (in ms) before seeking |
| `--heartbeat-ms` | `3000` | Cadence (in ms) of peer health pings |
| `--heartbeat-timeout-ms` | `10000` | Duration of inactivity before a peer is considered disconnected |
| `--no-copy` | `false` | Disable automatically copying the join command to clipboard on startup |
| `--leader-addr` | None | Direct IP:port address of the host, bypassing mDNS discovery |

---

## Troubleshooting

### "Different Track" Status on Follower
- The follower's player displays the error `Different Track (<title>)` when the host changes songs, but the follower hasn't switched.
- **Cause**: The follower does not have access to that song (e.g. track is regionally restricted or requires a Premium account).
- **Resolution**: Ensure both users have access to the same library or track URI in Spotify / Apple Music.

### Room Not Discovered (mDNS Failure)
- If `synkro join MYROOM` hangs searching for the room:
  1. Verify both machines are connected to the same Wi-Fi network or subnet.
  2. Check if your router has **AP Isolation / Client Isolation** enabled (which blocks broadcast packets between devices).
  3. Bypass discovery by providing the host's LAN IP directly:
     ```bash
     synkro join MYROOM --leader-addr 192.168.1.15:5871
     ```

### Commands Ignored on macOS
- If playback does not react when hitting `Space` or `/play`:
  1. Check **System Settings > Privacy & Security > Automation**.
  2. Confirm your terminal emulator is allowed to control Spotify or Music.

---

## License

Dual-licensed under either of:
- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))

at your option.
