# Synkrophase

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

**Synkrophase** is a high-precision, low-latency playback controller synchronizer for local networks written in Rust.

Instead of streaming, decoding, or transmitting heavy audio/video media files over the network, Synkrophase operates exclusively as a **control plane synchronizer**:
- Interfaces with native external media controllers (macOS Music.app / Spotify, Linux MPRIS, Windows GSMTC).
- Synchronizes LAN peer clocks using a high-precision **PTP-lite** UDP engine.
- Broadcasts lightweight binary **playback intents** with absolute synchronized clock trigger targets.
- Executes commands with microsecond accuracy, using **exact value-skip compensation** for late packet arrivals or mid-track joins.
- Automatically handles **autonomous follower self-filtering** and **two-stage drift reconciliation**.

---

## Architecture Overview

```text
┌─────────────────────────────────────────────────────────────┐
│ 1. Clock Layer (PTP-lite)                                   │
│    - UDP request-response (t1, t2, t3, t4)                  │
│    - Sub-millisecond offset calculation                     │
└──────────────────────────────┬──────────────────────────────┘
                               │ Synchronized Reference Time
┌──────────────────────────────▼──────────────────────────────┐
│ 2. Session & Discovery Layer                                │
│    - mDNS LAN auto-discovery (_synkrophase._udp.local.)     │
│    - Dynamic room formation & peer registry                 │
│    - Role succession and leader election                    │
└──────────────────────────────┬──────────────────────────────┘
                               │ Postcard Binary Protocol
┌──────────────────────────────▼──────────────────────────────┐
│ 3. Intent Protocol & Fast-Lane Execution Plane              │
│    - PlaybackIntent { action, target_ref_time, position_us }│
│    - High-precision wait (Δ > 0)                            │
│    - Exact Value Skip (Δ ≤ 0): seek_to(position + |Δ|)      │
│    - Autonomous Follower Filtering (track title validation) │
└──────────────────────────────┬──────────────────────────────┘
                               │ Commands OS Controllers
┌──────────────────────────────▼──────────────────────────────┐
│ 4. Platform Media Controller Abstraction                    │
│    - macOS: AppleScript bridge / MediaRemote (Music/Spotify)│
│    - Linux: MPRIS D-Bus (Planned)                           │
│    - Windows: GSMTC (Planned)                               │
└──────────────────────────────┬──────────────────────────────┘
                               │ Background Alignment
┌──────────────────────────────▼──────────────────────────────┐
│ 5. Dual-Trigger Drift Evaluator & Decider                   │
│    - Sparse background heartbeat (3–5s)                     │
│    - Instant event triggers via notify_one()                │
│    - Zone 1 (<50ms): InSync (zero disruption)               │
│    - Zone 2 (50–200ms): Fine rate adjustment (or seek)      │
│    - Zone 3 (>200ms): Precision micro-seek                  │
└─────────────────────────────────────────────────────────────┘
```

---

## Features

- **Zero Media Overhead**: Never touches your audio or video files. CPU, memory, and bandwidth footprints remain negligible.
- **Player Agnostic**: Control native desktop applications like Spotify or Apple Music without needing special plugins or modified builds.
- **Sub-Millisecond Clock Sync**: Built-in PTP-lite UDP ping-pong measures round-trip time and peer clock skew down to microsecond resolutions.
- **Exact Value-Skip Compensation**: If network jitter causes an intent to arrive late, or if a follower joins mid-track, Synkrophase calculates the exact temporal overshoot ($\Delta_{\text{late}} = \text{now} - T_{\text{target}}$) and seeks forward seamlessly.
- **Follower Autonomous Filtering**: Leaders broadcast immediately without room filtering overhead ($O(1)$). Followers verify their active player track against the intent; if playing something else, followers gracefully skip execution without interrupting peers.
- **Decoupled Telemetry**: Fast-lane playback commands are strictly separated from slow-lane background statistics and drift checks.

---

## Installation

### Homebrew (macOS & Linux)

Install the pre-built binary via the official tap:

```bash
brew tap landxcape/tap
brew install landxcape/tap/synkrophase
```

### Build from Source

Ensure you have a recent Rust toolchain installed (2024 edition supported, Rust 1.85+):

```bash
git clone https://github.com/landxcape/synkrophase.git
cd synkrophase/synkrophase
cargo build --release
```

The compiled binary will be located at `target/release/synkro`.

---

## Usage Guide

### 1. Check Local Media Player Status
Query the active media player on your machine (e.g. Spotify, Music.app):

```bash
synkro status
```

Output:
```text
=== Local Media Player Status ===
  Playing:  Yes
  Position: 45.19s
  Title:    Seven Nation Army
  Artist:   The White Stripes
  Album:    Elephant
  Duration: 232.15s
```

### 2. Host a Synchronization Room (Leader)
Start a new room. Synkrophase advertises via mDNS on your local network:

```bash
synkro host MYROOM
```

Interactive commands available inside the host REPL:
- `play` — Broadcasts a scheduled play intent with dynamic lead time across all followers.
- `pause` — Broadcasts a synchronized pause intent.
- `seek <seconds>` — Broadcasts a synchronized seek intent to `<seconds>` (e.g. `seek 60.5`).
- `status` — Prints the live playback status of your local player.
- `queue` — Displays the collaborative room queue.
- `peers` — Lists connected LAN peers and measured clock offsets.

### 3. Join an Existing Room (Follower)
Discover and join an active room hosted on the local network:

```bash
# Auto-discover via mDNS:
synkro join MYROOM

# Or join directly by IP and port:
synkro join MYROOM --leader-addr 192.168.1.100:41234
```

Followers listen for leader intents, calculate precise target trigger times on the synchronized reference clock, and command their local media player simultaneously.

### 4. Check Group Sync Offset
Inspect measured clock drift and synchrony across all room members:

```bash
synkro sync MYROOM
```

---

## Configuration

Synkrophase defaults to sensible local LAN settings, but behavior can be customized via CLI flags or configuration files:
- **Lead Time**: Time allocated for packets to reach followers before scheduled firing (default: `150ms`).
- **Drift Threshold**: Permitted alignment tolerance before triggering micro-seeks (default: `50ms`).
- **Heartbeat Cadence**: Interval for peer health checks and background drift monitoring (default: `3s`–`5s`).

---

## Testing

Run the full automated test suite covering unit tests, clock synchronization, scheduler value-skip compensation, drift evaluator, and macOS controllers:

```bash
cargo test
```

Run static analysis and lints:

```bash
cargo clippy --all-targets --all-features
```

---

## License

Dual-licensed under either of:
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
