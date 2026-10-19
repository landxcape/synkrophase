use std::fs;
use std::sync::Arc;
use uuid::Uuid;

use synkrophase::controller::mock::MockMediaController;
use synkrophase::daemon::paths::DaemonPaths;
use synkrophase::doctor::checks::{
    check_daemon_and_ipc, check_media_player, check_monotonic_clock, check_multicast, check_ports,
};
use synkrophase::doctor::repair::apply_repairs;
use synkrophase::doctor::report::{CheckStatus, DiagnosticCheck, DoctorReport};

#[test]
fn test_doctor_report_json_serialization() {
    let mut report = DoctorReport::new("0.8.2".to_string());
    assert!(report.all_passed);
    assert_eq!(report.fixable_count, 0);

    report.add_check(DiagnosticCheck {
        name: "Test Check 1".to_string(),
        category: "Test Category".to_string(),
        status: CheckStatus::Passed,
        message: "Passed successfully".to_string(),
        details: Some("Extra details".to_string()),
        remediation: None,
        is_fixable: false,
    });
    assert!(report.all_passed);

    report.add_check(DiagnosticCheck {
        name: "Test Fixable Warning".to_string(),
        category: "Test Category".to_string(),
        status: CheckStatus::Warning,
        message: "Warning needs fix".to_string(),
        details: None,
        remediation: Some("Run fix".to_string()),
        is_fixable: true,
    });
    assert_eq!(report.fixable_count, 1);
    assert!(report.all_passed);

    report.add_check(DiagnosticCheck {
        name: "Test Failure".to_string(),
        category: "Test Category".to_string(),
        status: CheckStatus::Failed,
        message: "Failed check".to_string(),
        details: None,
        remediation: None,
        is_fixable: false,
    });
    assert!(!report.all_passed);

    // Serialize to JSON
    let json = report.to_json().expect("Serialization failed");
    assert!(json.contains("0.8.2"));
    assert!(json.contains("Test Check 1"));
    assert!(json.contains("Passed successfully"));
    assert!(json.contains("Test Fixable Warning"));

    // Deserialize back and assert equality
    let parsed: DoctorReport = serde_json::from_str(&json).expect("Deserialization failed");
    assert_eq!(parsed.version, "0.8.2");
    assert_eq!(parsed.checks.len(), 3);
    assert_eq!(parsed.fixable_count, 1);
    assert!(!parsed.all_passed);
}

#[tokio::test]
async fn test_doctor_repair_stale_socket() {
    let base_dir = std::env::temp_dir().join(format!("synkro-doc-sock-{}", Uuid::new_v4()));
    fs::create_dir_all(&base_dir).expect("create_dir_all failed");
    let socket_path = base_dir.join("synkro.sock");
    let pid_path = base_dir.join("daemon.pid");
    let log_path = base_dir.join("daemon.log");

    let paths = DaemonPaths {
        base_dir: base_dir.clone(),
        socket_path: socket_path.clone(),
        pid_path: pid_path.clone(),
        log_path,
    };

    // Create stale socket and pid file
    fs::write(&socket_path, "fake socket").expect("write socket failed");
    fs::write(&pid_path, "9999999").expect("write pid failed");

    // Check daemon & IPC
    let check = check_daemon_and_ipc(&paths).await;
    assert_eq!(check.name, "IPC Socket");
    assert_eq!(check.status, CheckStatus::Warning);
    assert!(check.is_fixable);

    let mut report = DoctorReport::new("0.8.2".to_string());
    report.add_check(check);
    assert_eq!(report.fixable_count, 1);

    // Apply repairs
    let repairs = apply_repairs(&report, &paths).await;
    assert_eq!(repairs.len(), 1);
    assert!(repairs[0].contains("Removed stale IPC socket"));

    // Verify files were unlinked
    assert!(!socket_path.exists());
    assert!(!pid_path.exists());

    // Re-check daemon & IPC
    let clean_check = check_daemon_and_ipc(&paths).await;
    assert_eq!(clean_check.name, "Background Daemon");
    assert_eq!(clean_check.status, CheckStatus::Passed);
    assert!(!clean_check.is_fixable);

    let _ = fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn test_doctor_repair_missing_base_dir() {
    let base_dir = std::env::temp_dir().join(format!("synkro-doc-dir-{}", Uuid::new_v4()));
    let paths = DaemonPaths {
        socket_path: base_dir.join("synkro.sock"),
        pid_path: base_dir.join("daemon.pid"),
        log_path: base_dir.join("daemon.log"),
        base_dir: base_dir.clone(),
    };

    assert!(!base_dir.exists());

    let check = check_daemon_and_ipc(&paths).await;
    assert_eq!(check.name, "Daemon Environment");
    assert_eq!(check.status, CheckStatus::Warning);
    assert!(check.is_fixable);

    let mut report = DoctorReport::new("0.8.2".to_string());
    report.add_check(check);

    let repairs = apply_repairs(&report, &paths).await;
    assert_eq!(repairs.len(), 1);
    assert!(repairs[0].contains("Created daemon configuration directory"));
    assert!(base_dir.exists());

    let _ = fs::remove_dir_all(&base_dir);
}

#[test]
fn test_doctor_monotonic_clock() {
    let check = check_monotonic_clock();
    assert_eq!(check.name, "Monotonic Clock");
    assert_eq!(check.status, CheckStatus::Passed);
    assert!(check.details.is_some());
}

#[tokio::test]
async fn test_doctor_ports_check() {
    let checks = check_ports(false).await;
    assert_eq!(checks.len(), 2);
    assert!(checks[0].name.contains("5870"));
    assert!(checks[1].name.contains("5871"));
}

#[tokio::test]
async fn test_doctor_multicast_check() {
    let check = check_multicast().await;
    assert_eq!(check.name, "Multicast Discovery");
    assert!(check.status == CheckStatus::Passed || check.status == CheckStatus::Warning);
}

#[tokio::test]
async fn test_doctor_mock_media_player() {
    let controller = Arc::new(MockMediaController::new());
    let check = check_media_player(controller).await;
    assert_eq!(check.name, "Media Player");
    assert!(check.status == CheckStatus::Passed || check.status == CheckStatus::Warning);
}
