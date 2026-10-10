use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::sleep;
use uuid::Uuid;

use super::common::{
    create_media_controller, resolve_join_target, run_heartbeat_loop, run_role_manager_loop,
    send_join_request,
};
use crate::clock::sync::ClockSync;
use crate::config::{DeviceConfig, SyncConfig};
use crate::error::Result;
use crate::protocol::messages::QueueState;
use crate::session::SessionState;
use crate::session::runtime::SessionMessageRuntime;
use crate::sync::evaluator::DriftEvaluator;
use crate::sync::scheduler::IntentScheduler;

pub async fn run_join(
    device: DeviceConfig,
    sync_config: SyncConfig,
    room_code: String,
    leader_addr: Option<SocketAddr>,
    leader_id: Option<Uuid>,
    leader_clock_port: u16,
    session_port: u16,
) -> Result<()> {
    let (resolved_leader_addr, resolved_leader_id) =
        resolve_join_target(&room_code, leader_addr, leader_id)?;

    let clock_socket =
        UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))).await?;
    let clock = Arc::new(ClockSync::new(
        clock_socket,
        sync_config.clone(),
        device.device_id,
    ));

    let session = Arc::new(SessionState::from_join(
        room_code.clone(),
        device.device_id,
        device.name.clone(),
        resolved_leader_id,
        resolved_leader_addr,
        vec![crate::protocol::messages::PeerInfo {
            device_id: resolved_leader_id,
            name: "Leader".into(),
            clock_offset_us: 0,
            last_seen: 0,
            role: crate::protocol::messages::Role::Leader,
        }],
        QueueState::default(),
    ));

    let leader_clock_addr = SocketAddr::new(resolved_leader_addr.ip(), leader_clock_port);
    match clock.measure_offset(leader_clock_addr).await {
        Ok(_offset) => {
            session.set_clock_offset(clock.residual_offset());
        }
        Err(err) => {
            eprintln!("clock offset measurement failed: {err}");
        }
    }

    let session_socket = Arc::new({
        let socket = UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::UNSPECIFIED,
            session_port,
        )))
        .await?;
        socket.set_broadcast(true)?;
        socket
    });

    let controller = create_media_controller();
    let scheduler = Arc::new(IntentScheduler::new(
        Arc::clone(&clock) as Arc<dyn crate::sync::controller::ClockSource>,
        Arc::clone(&controller),
    ));

    let drift_evaluator = Arc::new(DriftEvaluator::new(
        Arc::clone(&clock) as Arc<dyn crate::sync::controller::ClockSource>,
        Arc::clone(&controller),
        50_000,
    ));

    send_join_request(
        &session_socket,
        device.device_id,
        resolved_leader_addr,
        room_code.clone(),
        device.name.clone(),
    )
    .await?;

    let clock_task = {
        let clock = Arc::clone(&clock);
        tokio::spawn(async move { clock.run_responder().await })
    };

    // Active background drift check (every 1s)
    let drift_task = tokio::spawn({
        let evaluator = Arc::clone(&drift_evaluator);
        let session = Arc::clone(&session);
        async move {
            loop {
                sleep(Duration::from_millis(1000)).await;
                if let Some(anchor) = session.latest_sync_anchor() {
                    let _ = evaluator.evaluate_and_reconcile(&anchor).await;
                }
            }
        }
    });

    // Background periodic PTP offset refresh (every 10s)
    let clock_refresh_task = tokio::spawn({
        let clock = Arc::clone(&clock);
        let session = Arc::clone(&session);
        async move {
            loop {
                sleep(Duration::from_secs(10)).await;
                if let Ok(_offset) = clock.measure_offset(leader_clock_addr).await {
                    session.set_clock_offset(clock.residual_offset());
                }
            }
        }
    });

    let heartbeat_task = tokio::spawn({
        let session = Arc::clone(&session);
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        let cfg = sync_config.clone();
        let room = room_code.clone();
        let clock = Arc::clone(&clock);
        async move {
            run_heartbeat_loop(
                session,
                socket,
                sender,
                room,
                Some(resolved_leader_addr),
                cfg,
                Some(clock),
            )
            .await
        }
    });

    let role_manager_task = tokio::spawn({
        let session = Arc::clone(&session);
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        let cfg = sync_config.clone();
        async move { run_role_manager_loop(session, socket, sender, cfg).await }
    });

    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();

    // Feed drift updates to TUI
    let drift_tui_task = tokio::spawn({
        let evaluator = Arc::clone(&drift_evaluator);
        let session = Arc::clone(&session);
        let tx = event_tx.clone();
        async move {
            loop {
                sleep(Duration::from_millis(500)).await;
                if let Some(anchor) = session.latest_sync_anchor()
                    && let Ok((offset_us, zone, status)) =
                        evaluator.evaluate_drift(&anchor).await
                {
                    let _ = tx.send(crate::tui::AppEvent::DriftUpdate(offset_us, zone, status));
                }
            }
        }
    });

    let runtime = SessionMessageRuntime::new(Arc::clone(&session), device.name.clone())
        .with_event_tx(event_tx.clone())
        .with_controller(Arc::clone(&controller))
        .with_clock(Arc::clone(&clock))
        .with_scheduler(Arc::clone(&scheduler))
        .with_drift_evaluator(Arc::clone(&drift_evaluator));

    let receive_task = tokio::spawn({
        let socket = Arc::clone(&session_socket);
        async move { runtime.run_receive_loop(socket).await }
    });

    let abort_handles = vec![
        clock_task.abort_handle(),
        drift_task.abort_handle(),
        clock_refresh_task.abort_handle(),
        heartbeat_task.abort_handle(),
        role_manager_task.abort_handle(),
        drift_tui_task.abort_handle(),
        receive_task.abort_handle(),
    ];

    let (engine, mut command_rx) = crate::session::SynkroEngine::new(
        Arc::clone(&session),
        Arc::clone(&controller),
        Arc::clone(&session_socket),
        Some(resolved_leader_addr),
        Arc::clone(&clock),
        Arc::clone(&scheduler),
        event_tx.clone(),
        abort_handles,
    );
    let engine = Arc::new(engine);

    // Command dispatcher task
    let engine_cmd_worker = {
        let engine = Arc::clone(&engine);
        tokio::spawn(async move {
            while let Some(cmd) = command_rx.recv().await {
                let _ = engine.execute_command(cmd).await;
            }
        })
    };

    let _ = event_tx.send(crate::tui::AppEvent::Log {
        source: "System".into(),
        text: format!("Joining room {room_code} via {resolved_leader_addr}"),
    });

    let tui_res = crate::tui::run_tui(
        Arc::clone(&engine),
        room_code,
        device.name,
        None,
        event_rx,
        event_tx,
    )
    .await;

    engine_cmd_worker.abort();
    let _ = engine.shutdown().await;

    tui_res
}
