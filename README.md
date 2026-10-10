# Synkrophase

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Release](https://img.shields.io/github/v/release/landxcape/synkrophase)](https://github.com/landxcape/synkrophase/releases)

Synkrophase synchronizes media playback across multiple computers on the same local network.

Instead of streaming audio over the network, Synkrophase controls each computer's native media player directly. When one person plays, pauses, seeks, or changes a track, all connected computers follow along in sync.

---

## Supported Platforms and Players

| Operating System | Supported Players | Control Mechanism |
| :--- | :--- | :--- |
| **macOS** | Spotify, Apple Music | AppleScript |
| **Linux** | Spotify | MPRIS D-Bus |
| **Windows** | Spotify | WinRT GSMTC |

---

## How It Works

1. **Host and Followers**: One computer hosts a room (`synkro host ROOM`), and others join (`synkro join ROOM`).
2. **Clock Sync**: Devices measure network latency and clock differences over UDP, keeping them aligned within milliseconds.
3. **Automatic Track Sync**: When the host changes a track, followers automatically load that track in their own player and seek to the correct position.
4. **Drift Correction**: If a follower's playback drifts out of alignment, Synkrophase nudges the position back into sync.

---

## Installation

### Homebrew (macOS & Linux)

```bash
brew tap landxcape/tap
brew install landxcape/tap/synkrophase
```

### Pre-built Binaries (Windows, macOS, Linux)

Download pre-compiled binaries from [GitHub Releases](https://github.com/landxcape/synkrophase/releases/latest).

On Windows, extract `synkrophase-windows-x86_64.zip` and run `synkro.exe` from PowerShell or Command Prompt.

### Build from Source

Requires Rust 1.85+ (2024 edition):

```bash
git clone https://github.com/landxcape/synkrophase.git
cd synkrophase/synkrophase
cargo build --release
```

The compiled binary will be at `target/release/synkro`.

---

## Quick Start

### 1. Check Player Status
Make sure your media player is running, then check its current status:

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

This starts the room and opens the interactive dashboard. The join command is automatically copied to your clipboard.

### 3. Join a Room (Follower)

On another machine on the same network:

```bash
synkro join MYROOM
```

If local network discovery (mDNS) is blocked by a firewall, specify the host's IP address directly:

```bash
synkro join MYROOM --leader-addr 192.168.1.100:5871
```

---

## Interactive Dashboard (TUI)

When hosting or joining, Synkrophase opens a terminal interface:

```text
╭ Synkrophase • Room: TESTROOM • Role: [LEADER] • Device: Host ────────────────╮
╰──────────────────────────────────────────────────────────────────────────────╯
╭ Now Playing ─────────────────────────────────────────────────────────────────╮
│ ▶ Numb   Linkin Park • Meteora  [Playing]                                    │
│ 01:05 [━━━━━━━━━━━━━━━━━━━━━━●───────────────────────────────] 03:07  (+12µs)│
╰──────────────────────────────────────────────────────────────────────────────╯
╭ Room Members ──────────────────────╮╭ Activity & Chat ───────────────────────╮
│ID       Peer            Role Offset││[21:14:03] Session started. Ready.      │
│e8a15b3a Host (You)    Leader    Ref││[21:14:03] Room: TESTROOM               │
│a3f290d1 Follower   Moderator  +12µs││[21:14:09] [Follower] Connected.        │
╰────────────────────────────────────╯╰────────────────────────────────────────╯
[Space] Play/Pause • [←/→] Seek ±5s • [n/p] Next/Prev • [v] Vol • [/] Chat • [q] Quit
```

### Keyboard Shortcuts

| Key | Action |
| :--- | :--- |
| `Space` | Play / Pause |
| `←` / `→` | Seek backward / forward 5 seconds |
| `n` | Next track |
| `p` | Previous track |
| `v` | Sync host volume to all peers |
| `c` | Copy join command to clipboard |
| `/` or `i` | Open chat and command prompt |
| `?` or `h` | Show help |
| `q` | Leave and exit |

### Chat Commands

Type `/` in the dashboard to access commands:
- `/play` / `/pause` — Control playback
- `/next` / `/prev` — Change tracks
- `/seek <seconds>` — Jump to position (e.g. `/seek 90`)
- `/vol <0-100>` — Set volume for all peers
- `/transfer <peer>` — Transfer room host to another member
- `/role <peer> <moderator|listener>` — Change permissions for a member
- `/quit` — Leave the room

---

## Background Daemon Mode

You can run Synkrophase in the background without keeping a terminal open:

```bash
# Start in the background
synkro daemon start host MYROOM
# or
synkro daemon start join MYROOM

# Check status
synkro daemon status

# View logs
synkro daemon logs -f

# Stop
synkro daemon stop
```

When the daemon is running, control playback from any terminal:

```bash
synkro play
synkro pause
synkro next
synkro prev
synkro seek 45
synkro volume 70
```

---

## Configuration Options

Pass these flags to `synkro host` or `synkro join`:

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--lead-time-ms` | `350` | Buffer time (in ms) allowed for network delivery before an action executes |
| `--threshold-ms` | `50` | Maximum acceptable playback drift (in ms) before seeking |
| `--heartbeat-ms` | `3000` | Interval between peer health pings |
| `--heartbeat-timeout-ms` | `10000` | Time before an unresponsive peer is considered disconnected |
| `--no-copy` | `false` | Do not copy join command to clipboard on start |

---

## License

Dual-licensed under either:
- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
