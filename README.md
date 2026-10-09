# Synkrophase

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Release](https://img.shields.io/github/v/release/landxcape/synkrophase)](https://github.com/landxcape/synkrophase/releases)

**Synkrophase** is a high-precision, low-latency playback controller synchronizer for local networks written in Rust.

Instead of streaming, decoding, or transmitting heavy audio/video media files over the network, Synkrophase operates exclusively as an external **control plane synchronizer**:
- **Primary Player Support**: Native **Spotify** and **Apple Music** integration with automatic active player detection and state prioritization (with cross-platform desktop controller architecture).
- Synchronizes LAN peer clocks using a high-precision **PTP-lite** UDP engine with sub-millisecond precision.
- Broadcasts lightweight binary **playback intents** stamped with synchronized reference clock trigger targets.
- Pre-dispatches OS commands using local **actuation latency compensation** (accounting for local OS IPC / AppleScript delays) with exact **value-skip compensation** for network jitter or mid-track joins.
- Features a full **interactive Ratatui terminal dashboard (TUI)** with an inline Unicode timeline slider, activity log, room chat, and a headless REPL mode fallback.
- Offers **native track navigation** (next/previous), **one-shot room volume synchronization**, and automatic **clipboard room sharing**.

---

## Architecture Overview

```text
┌─────────────────────────────────────────────────────────────┐
│ 1. Clock Synchronization Layer (PTP-lite UDP)               │
│    - Hardware-accurate monotonic clock timestamping         │
│    - Sub-millisecond offset calculation over UDP             │
└──────────────────────────────┬──────────────────────────────┘
                               │ Synchronized Reference Time
┌──────────────────────────────▼──────────────────────────────┐
│ 2. Session & Discovery Layer                                │
│    - mDNS LAN auto-discovery (_synkrophase._udp.local.)     │
│    - Dynamic room formation, peer heartbeats & timeouts      │
│    - Role succession and deterministic leader election       │
└──────────────────────────────┬──────────────────────────────┘
                               │ Postcard Binary Protocol
┌──────────────────────────────▼──────────────────────────────┐
│ 3. Intent Protocol & Fast-Lane Execution Plane              │
│    - PlaybackIntent { action, target_ref_time, position_us }│
│    - High-precision wait (Δ > 0)                            │
│    - Exact Value Skip (Δ ≤ 0): seek_to(position + |Δ|)      │
│    - Follower autonomous track title verification           │
└──────────────────────────────┬──────────────────────────────┘
                               │ Commands OS Controllers
┌──────────────────────────────▼──────────────────────────────┐
│ 4. Platform Media Controller Abstraction                    │
│    - macOS: AppleScript bridge (Spotify & Apple Music)      │
│    - Play, Pause, Micro-seek, Next, Prev, Volume            │
│    - Midpoint query timestamp compensation (~60-80ms)       │
└──────────────────────────────┬──────────────────────────────┘
                               │ Continuous Alignment
┌──────────────────────────────▼──────────────────────────────┐
│ 5. Dual-Trigger Drift Evaluator & Decider                   │
│    - Zone 1 (<50ms): InSync (zero disruption)               │
│    - Zone 2 (50–200ms): Fine rate adjustment (or seek)      │
│    - Zone 3 (>200ms): Precision micro-seek with cooldown    │
└─────────────────────────────────────────────────────────────┘
```

---

## Features

- **Zero Media Overhead**: Never transmits raw audio or video files. CPU, memory, and bandwidth footprints remain negligible.
- **Primary Player Focus: Spotify & Apple Music**: Synkrophase focuses primarily on **Spotify** and **Apple Music**. It automatically detects whichever player is actively playing, prioritizes it dynamically, and maintains state memory across pauses without manual configuration.
- **Actuation Latency Compensation & Pre-Dispatch**: Autonomously profiles local OS command overhead (~150ms for AppleScript IPC, ~15ms for D-Bus) using exponential moving averages and pre-dispatches actions early, ensuring audio commands take physical effect right at the synchronized network timestamp.
- **Interactive TUI Dashboard**: Full Ratatui terminal UI with live progress bar, peer status table with live clock offset telemetry, drift indicators, activity log, and integrated chat.
- **Sub-Millisecond Clock Sync**: Built-in PTP-lite UDP engine measures round-trip time and clock offset across LAN peers.
- **Exact Value-Skip Compensation**: If a command arrives late due to network delay or a peer connects mid-track, Synkrophase calculates temporal overshoot ($\Delta_{\text{late}} = \text{now} - T_{\text{target}}$) and seeks forward seamlessly.
- **Native Track Controls**: Seamless next track (`n`) and previous track (`p`) navigation across room peers.
- **One-Shot Room Volume Sync**: Broadcast host volume across all connected devices in one touch (`v` or `/vol`).
- **Autonomous Follower Filtering**: Followers verify their active player track against incoming intents; if playing another track, followers gracefully skip execution without interrupting peers.
- **Automatic Host Failover**: Deterministic leader succession ensures playback synchronization continues uninterrupted if the room host leaves.

---

## Installation

### Homebrew (macOS & Linux)

Install the pre-built binary via Homebrew:

```bash
brew tap landxcape/tap
brew install landxcape/tap/synkrophase
```

### Pre-built GitHub Releases

Download pre-compiled binaries for macOS (Apple Silicon / Intel), Linux, and Windows directly from [GitHub Releases](https://github.com/landxcape/synkrophase/releases/latest).

### Build from Source

Ensure you have a recent Rust toolchain installed (Rust 1.85+, 2024 edition):

```bash
git clone https://github.com/landxcape/synkrophase.git
cd synkrophase/synkrophase
cargo build --release
```

The compiled binary will be located at `target/release/synkro`.

---

## Quick Start & Usage

### 1. Check Local Player Status
Inspect the media player currently active on your system:

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

### 2. Host a Synchronization Room (Leader)
Start a new room. Synkrophase launches an interactive dashboard and advertises via mDNS:

```bash
synkro host MYROOM
```

Or run in headless REPL mode (useful for SSH sessions or scripts):

```bash
synkro host MYROOM --headless
```

### 3. Join an Existing Room (Follower)
Discover and connect to a room on your LAN:

```bash
# Auto-discover via mDNS:
synkro join MYROOM

# Or specify leader address directly:
synkro join MYROOM --leader-addr 192.168.1.100:5871
```

Followers lock their reference clocks to the leader, listen for playback intents, and adjust their local players automatically.

---

## Interactive Dashboard (TUI)

When hosting or joining a room, Synkrophase launches an interactive dashboard:

```text
╭ Synkrophase • Room: TESTROOM • Role:  [LEADER]  • Device: Host ─────────────────────────────────────────────────────╮
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭ Now Playing ────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ ▶ Numb   Linkin Park • Meteora  [Playing]                                                                           │
│ 01:05 [━━━━━━━━━━━━━━━━━━━━━━━━━━━━━●─────────────────────────────────────────────────] 03:07   (+12µs sync)        │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭ Room Members ──────────────────────────────────────╮╭ Activity & Room Chat ─────────────────────────────────────────╮
│ID       Peer            Role          Offset       ││[21:14:03] [System] Session started. Ready.                    │
│e8a15b3a Host (You)      Leader        Reference    ││[21:14:03] [System] Room: TESTROOM | Session: 5871             │
│a3f290d1 Follower        Moderator     +12µs        ││[21:14:09] [Follower] Hey everyone!                            │
│                                                    ││[21:14:12] [Host] Sync locked.                                 │
╰────────────────────────────────────────────────────╯╰───────────────────────────────────────────────────────────────╯
[?] Help • [Space] Play/Pause • [←/→] Seek ±5s • [n/p] Next/Prev • [v] VolSync • [c] Copy • [/] Chat/Cmd • [q] Quit
```

### Hotkeys (Normal Mode)
- **`[Space]`** — Toggle Play / Pause
- **`[← / →]`** — Micro-seek backward / forward ±5 seconds
- **`[n]`** — Next track (Spotify / Apple Music)
- **`[p]`** — Previous track (Spotify / Apple Music)
- **`[v]`** — One-shot volume sync (broadcasts host volume to room)
- **`[c]`** — Copy room invitation / join command to clipboard
- **`[/]`** or **`[i]`** — Open Chat & Command input prompt
- **`[?]`** or **`[h]`** — Open interactive Quick Reference help modal
- **`[q]`** — Disconnect and quit

### Slash Commands (Input Mode)
- **`/play`**, **`/pause`** — Control playback across all peers
- **`/next`**, **`/prev`** — Skip to next or previous track
- **`/seek <seconds>`** — Jump to absolute timeline position (e.g. `/seek 90`)
- **`/vol [0-100]`** — Sync room volume (e.g. `/vol 80`, or `/vol` to mirror self)
- **`/transfer <peer>`** — Transfer room leadership by peer name or 8-char short UUID (Leader only)
- **`/copy`**, **`/share`** — Copy room invitation / join command to clipboard
- **`/help`** — Toggle help popup
- **`/quit`** — Leave the room

---

## One-Shot CLI Commands

Synkrophase can also be invoked as a one-shot remote control CLI tool:

```bash
# Playback commands
synkro play MYROOM
synkro pause MYROOM
synkro resume MYROOM
synkro next MYROOM
synkro prev MYROOM
synkro seek MYROOM 120.5

# Volume control
synkro volume MYROOM --level 80

# Room administration & telemetry
synkro sync MYROOM                      # Inspect peer clock offsets
synkro chat MYROOM "Starting now!"      # Send a chat message
synkro transfer MYROOM <DEVICE_UUID>    # Hand off room leadership
```

---

## Configuration

Synkrophase comes configured for low latency out of the box, with options customizable via CLI flags:

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--lead-time-ms` | `350` | Scheduled future execution window for intent delivery & pre-dispatch |
| `--threshold-ms` | `50` | Maximum acceptable media drift before triggering micro-seeks |
| `--heartbeat-ms` | `3000` | Peer health ping cadence |
| `--heartbeat-timeout-ms`| `10000` | Timeout before declaring a peer dead and triggering succession |
| `--headless` | `false` | Run in headless REPL mode without terminal UI |
| `--no-copy` | `false` | Disable automatic copying of the join command to system clipboard on host start |

---

## Testing & Quality

Run the full automated test suite:

```bash
cargo test
```

Run static analysis with zero warnings:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

---

## License

Dual-licensed under either of:
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
