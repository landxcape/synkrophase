pub mod checks;
pub mod repair;
pub mod report;

use self::report::DoctorReport;
use crate::cli::handlers::common::create_media_controller;
use crate::daemon::paths::DaemonPaths;
use crate::error::Result;

/// Runs all diagnostic checks and compiles a report without applying any modifications.
pub async fn run_diagnostics(paths: &DaemonPaths) -> DoctorReport {
    let mut report = DoctorReport::new(env!("CARGO_PKG_VERSION").to_string());

    // 1. Media Player Check
    let controller = create_media_controller();
    report.add_check(checks::check_media_player(controller).await);

    // 2. Network & Monotonic Clock Checks
    let (net_check, _) = checks::check_network();
    report.add_check(net_check);
    report.add_check(checks::check_monotonic_clock());

    // 3. Port Availability Checks
    let daemon_running = paths.is_running();
    for port_check in checks::check_ports(daemon_running).await {
        report.add_check(port_check);
    }

    // 4. Multicast Discovery Check
    report.add_check(checks::check_multicast().await);

    // 5. Background Daemon & IPC Check
    report.add_check(checks::check_daemon_and_ipc(paths).await);

    report
}

/// Executes the doctor diagnostic suite, optionally applying auto-repairs and printing output.
pub async fn run_doctor(fix: bool, json: bool) -> Result<()> {
    let paths = DaemonPaths::default_paths()?;
    let mut report = run_diagnostics(&paths).await;

    if fix && report.fixable_count > 0 {
        let applied = repair::apply_repairs(&report, &paths).await;
        let mut fresh_report = run_diagnostics(&paths).await;
        fresh_report.repairs_applied = applied;
        report = fresh_report;
    }

    if json {
        let json_str = report.to_json().map_err(|e| {
            crate::error::SynkroError::Serialization(format!(
                "Failed to serialize doctor report: {e}"
            ))
        })?;
        println!("{json_str}");
    } else {
        report.print_terminal();
    }

    Ok(())
}
