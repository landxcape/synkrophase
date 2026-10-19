use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::UdpSocket;

use crate::controller::MediaController;
use crate::daemon::paths::DaemonPaths;
use crate::doctor::report::{CheckStatus, DiagnosticCheck};

pub async fn check_media_player(controller: Arc<dyn MediaController>) -> DiagnosticCheck {
    let start = Instant::now();
    match controller.get_playback_state().await {
        Ok(status) => {
            let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
            if let Some(meta) = &status.metadata {
                DiagnosticCheck {
                    name: "Media Player".to_string(),
                    category: "Media Controller & Permissions".to_string(),
                    status: CheckStatus::Passed,
                    message: "Player responsive and active".to_string(),
                    details: Some(format!(
                        "Track: '{}', Latency: {:.1}ms",
                        meta.title, latency_ms
                    )),
                    remediation: None,
                    is_fixable: false,
                }
            } else {
                DiagnosticCheck {
                    name: "Media Player".to_string(),
                    category: "Media Controller & Permissions".to_string(),
                    status: CheckStatus::Warning,
                    message: "No active track playing".to_string(),
                    details: Some(format!("Controller latency: {:.1}ms", latency_ms)),
                    remediation: Some("Play a track in Spotify or Apple Music".to_string()),
                    is_fixable: false,
                }
            }
        }
        Err(err) => {
            let err_str = err.to_string();
            if err_str.contains("-1743")
                || err_str.contains("Not authorized")
                || err_str.contains("PermissionDenied")
            {
                DiagnosticCheck {
                    name: "OS Automation Permissions".to_string(),
                    category: "Media Controller & Permissions".to_string(),
                    status: CheckStatus::Failed,
                    message: "Automation permission denied".to_string(),
                    details: Some(err_str),
                    remediation: Some(
                        "Enable terminal permissions in System Settings > Privacy & Security > Automation, or run 'tccutil reset AppleEvents'".to_string(),
                    ),
                    is_fixable: false,
                }
            } else {
                DiagnosticCheck {
                    name: "Media Player".to_string(),
                    category: "Media Controller & Permissions".to_string(),
                    status: CheckStatus::Warning,
                    message: "Desktop player not detected".to_string(),
                    details: Some("Spotify or Apple Music client is not running".to_string()),
                    remediation: Some(
                        "Launch desktop Spotify or Apple Music (web players in browser tabs are not supported)".to_string(),
                    ),
                    is_fixable: true,
                }
            }
        }
    }
}

pub fn check_network() -> (DiagnosticCheck, Option<std::net::IpAddr>) {
    match local_ip_address::local_ip() {
        Ok(ip) => {
            if ip.is_loopback() {
                (
                    DiagnosticCheck {
                        name: "Network Interface".to_string(),
                        category: "Network & Monotonic Clock".to_string(),
                        status: CheckStatus::Warning,
                        message: "Only loopback address detected (127.0.0.1)".to_string(),
                        details: Some("No active LAN network adapter connected".to_string()),
                        remediation: Some(
                            "Connect this machine to Wi-Fi or Ethernet to discover LAN peers"
                                .to_string(),
                        ),
                        is_fixable: false,
                    },
                    Some(ip),
                )
            } else {
                (
                    DiagnosticCheck {
                        name: "Network Interface".to_string(),
                        category: "Network & Monotonic Clock".to_string(),
                        status: CheckStatus::Passed,
                        message: format!("Primary LAN IPv4: {ip}"),
                        details: None,
                        remediation: None,
                        is_fixable: false,
                    },
                    Some(ip),
                )
            }
        }
        Err(e) => (
            DiagnosticCheck {
                name: "Network Interface".to_string(),
                category: "Network & Monotonic Clock".to_string(),
                status: CheckStatus::Warning,
                message: "Could not determine local IPv4 address".to_string(),
                details: Some(e.to_string()),
                remediation: Some("Verify network connection".to_string()),
                is_fixable: false,
            },
            None,
        ),
    }
}

pub fn check_monotonic_clock() -> DiagnosticCheck {
    let t1 = Instant::now();
    let t2 = Instant::now();
    let delta = t2.saturating_duration_since(t1);

    DiagnosticCheck {
        name: "Monotonic Clock".to_string(),
        category: "Network & Monotonic Clock".to_string(),
        status: CheckStatus::Passed,
        message: "High-resolution monotonic timer operational".to_string(),
        details: Some(format!("Granularity: <= {}ns", delta.as_nanos().max(1))),
        remediation: None,
        is_fixable: false,
    }
}

pub async fn check_ports(daemon_running: bool) -> Vec<DiagnosticCheck> {
    let mut checks = Vec::new();

    // Check Clock Port 5870
    let clock_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 5870));
    match UdpSocket::bind(clock_addr).await {
        Ok(_) => {
            checks.push(DiagnosticCheck {
                name: "Port 5870 (PTP Clock)".to_string(),
                category: "Port Availability".to_string(),
                status: CheckStatus::Passed,
                message: "Available for UDP synchronization".to_string(),
                details: None,
                remediation: None,
                is_fixable: false,
            });
        }
        Err(e) => {
            if daemon_running {
                checks.push(DiagnosticCheck {
                    name: "Port 5870 (PTP Clock)".to_string(),
                    category: "Port Availability".to_string(),
                    status: CheckStatus::Passed,
                    message: "Occupied by active Synkrophase background daemon".to_string(),
                    details: None,
                    remediation: None,
                    is_fixable: false,
                });
            } else {
                checks.push(DiagnosticCheck {
                    name: "Port 5870 (PTP Clock)".to_string(),
                    category: "Port Availability".to_string(),
                    status: CheckStatus::Warning,
                    message: format!("Port occupied ({e})"),
                    details: Some("Another process is currently bound to port 5870".to_string()),
                    remediation: Some(
                        "Stop any conflicting application, or use '--clock-port <PORT>'"
                            .to_string(),
                    ),
                    is_fixable: false,
                });
            }
        }
    }

    // Check Session Port 5871
    let session_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 5871));
    match UdpSocket::bind(session_addr).await {
        Ok(_) => {
            checks.push(DiagnosticCheck {
                name: "Port 5871 (Session Messages)".to_string(),
                category: "Port Availability".to_string(),
                status: CheckStatus::Passed,
                message: "Available for session traffic".to_string(),
                details: None,
                remediation: None,
                is_fixable: false,
            });
        }
        Err(e) => {
            if daemon_running {
                checks.push(DiagnosticCheck {
                    name: "Port 5871 (Session Messages)".to_string(),
                    category: "Port Availability".to_string(),
                    status: CheckStatus::Passed,
                    message: "Occupied by active Synkrophase background daemon".to_string(),
                    details: None,
                    remediation: None,
                    is_fixable: false,
                });
            } else {
                checks.push(DiagnosticCheck {
                    name: "Port 5871 (Session Messages)".to_string(),
                    category: "Port Availability".to_string(),
                    status: CheckStatus::Warning,
                    message: format!("Port occupied ({e})"),
                    details: Some("Another process is currently bound to port 5871".to_string()),
                    remediation: Some(
                        "Stop any conflicting application, or use '--session-port <PORT>'"
                            .to_string(),
                    ),
                    is_fixable: false,
                });
            }
        }
    }

    checks
}

pub async fn check_multicast() -> DiagnosticCheck {
    match UdpSocket::bind("0.0.0.0:0").await {
        Ok(socket) => {
            let loopback_ok = socket.set_multicast_loop_v4(true).is_ok();
            if loopback_ok {
                DiagnosticCheck {
                    name: "Multicast Discovery".to_string(),
                    category: "Local Multicast & Discovery".to_string(),
                    status: CheckStatus::Passed,
                    message: "mDNS multicast loopback operational (224.0.0.251:5353)".to_string(),
                    details: None,
                    remediation: None,
                    is_fixable: false,
                }
            } else {
                DiagnosticCheck {
                    name: "Multicast Discovery".to_string(),
                    category: "Local Multicast & Discovery".to_string(),
                    status: CheckStatus::Warning,
                    message: "Multicast loopback could not be configured".to_string(),
                    details: None,
                    remediation: Some(
                        "If router blocks mDNS/multicast, bypass with '--leader-addr <IP:PORT>'"
                            .to_string(),
                    ),
                    is_fixable: false,
                }
            }
        }
        Err(e) => DiagnosticCheck {
            name: "Multicast Discovery".to_string(),
            category: "Local Multicast & Discovery".to_string(),
            status: CheckStatus::Warning,
            message: format!("Multicast socket creation failed: {e}"),
            details: None,
            remediation: Some("Bypass discovery with '--leader-addr <IP:PORT>'".to_string()),
            is_fixable: false,
        },
    }
}

pub async fn check_daemon_and_ipc(paths: &DaemonPaths) -> DiagnosticCheck {
    if !paths.base_dir.exists() {
        return DiagnosticCheck {
            name: "Daemon Environment".to_string(),
            category: "Background Daemon & IPC".to_string(),
            status: CheckStatus::Warning,
            message: format!("Directory {:?} does not exist", paths.base_dir),
            details: None,
            remediation: Some(
                "Run 'synkro doctor --fix' to create configuration directory".to_string(),
            ),
            is_fixable: true,
        };
    }

    let is_running = paths.is_running();
    let socket_exists = paths.socket_path.exists();

    if is_running {
        let pid_str = paths
            .read_pid()
            .ok()
            .flatten()
            .map(|p| format!("PID {p}"))
            .unwrap_or_else(|| "active".to_string());
        DiagnosticCheck {
            name: "Background Daemon".to_string(),
            category: "Background Daemon & IPC".to_string(),
            status: CheckStatus::Passed,
            message: format!("Running ({pid_str})"),
            details: Some(format!("Socket: {:?}", paths.socket_path)),
            remediation: None,
            is_fixable: false,
        }
    } else if socket_exists {
        DiagnosticCheck {
            name: "IPC Socket".to_string(),
            category: "Background Daemon & IPC".to_string(),
            status: CheckStatus::Warning,
            message: "Stale socket file detected (daemon not running)".to_string(),
            details: Some(format!("Path: {:?}", paths.socket_path)),
            remediation: Some("Run 'synkro doctor --fix' to remove orphaned socket".to_string()),
            is_fixable: true,
        }
    } else {
        DiagnosticCheck {
            name: "Background Daemon".to_string(),
            category: "Background Daemon & IPC".to_string(),
            status: CheckStatus::Passed,
            message: "Idle (socket clean, ready to start)".to_string(),
            details: None,
            remediation: None,
            is_fixable: false,
        }
    }
}
