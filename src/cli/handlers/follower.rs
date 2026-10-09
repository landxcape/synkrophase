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
use crate::cli::repl::run_follower_repl;
use crate::clock::sync::ClockSync;
use crate::config::{DeviceConfig, SyncConfig};
use crate::error::Result;
use crate::protocol::messages::{Envelope, Message, QueueState, serialize};
use crate::session::SessionState;
use crate::session::runtime::SessionMessageRuntime;
use crate::sync::evaluator::DriftEvaluator;
use crate::sync::scheduler::IntentScheduler;

#[allow(clippy::too_many_arguments)]
pub async fn run_join(
    device: DeviceConfig,
    sync_config: SyncConfig,
    room_code: String,
    leader_addr: Option<SocketAddr>,
    leader_id: Option<Uuid>,
    leader_clock_port: u16,
    session_port: u16,
    headless: bool,
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
        Ok(offset) => {
            session.set_clock_offset(offset);
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

    // Sparse background drift check (every 4s)
    let drift_task = tokio::spawn({
        let evaluator = Arc::clone(&drift_evaluator);
        let session = Arc::clone(&session);
        async move {
            loop {
                sleep(Duration::from_secs(4)).await;
                if let Some(anchor) = session.latest_sync_anchor() {
                    let _ = evaluator.evaluate_and_reconcile(&anchor).await;
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

    if !headless {
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

        let runtime = SessionMessageRuntime::new(Arc::clone(&session), None, device.name.clone())
            .with_event_tx(event_tx.clone())
            .with_controller(Arc::clone(&controller))
            .with_clock(Arc::clone(&clock))
            .with_scheduler(Arc::clone(&scheduler))
            .with_drift_evaluator(Arc::clone(&drift_evaluator));

        let receive_task = tokio::spawn({
            let socket = Arc::clone(&session_socket);
            async move { runtime.run_receive_loop(socket).await }
        });

        let _ = event_tx.send(crate::tui::AppEvent::Log {
            source: "System".into(),
            text: format!("Joining room {room_code} via {resolved_leader_addr}"),
        });

        let tui_res = crate::tui::run_tui(
            room_code,
            device.name,
            device.device_id,
            crate::protocol::messages::Role::Listener,
            Arc::clone(&session),
            Arc::clone(&controller),
            Arc::clone(&session_socket),
            Some(resolved_leader_addr),
            None,
            Arc::clone(&clock),
            Arc::clone(&scheduler),
            event_rx,
            event_tx,
        )
        .await;

        // Graceful departure notification
        let leave_env = Envelope {
            sender: device.device_id,
            payload: Message::PeerLeft(device.device_id),
        };
        if let Ok(bytes) = serialize(&leave_env) {
            for addr in session.peer_socket_addrs() {
                let _ = session_socket.send_to(&bytes, addr).await;
            }
        }

        receive_task.abort();
        drift_tui_task.abort();
        clock_task.abort();
        drift_task.abort();
        heartbeat_task.abort();
        role_manager_task.abort();

        return tui_res;
    }

    // Headless REPL mode
    let (rl, stdout) = rustyline_async::Readline::new("synkro> ".to_string()).unwrap();

    let runtime = SessionMessageRuntime::new(
        Arc::clone(&session),
        Some(stdout.clone()),
        device.name.clone(),
    )
    .with_controller(Arc::clone(&controller))
    .with_clock(Arc::clone(&clock))
    .with_scheduler(Arc::clone(&scheduler))
    .with_drift_evaluator(Arc::clone(&drift_evaluator));

    crate::session::runtime::print_event(
        Some(&stdout),
        &format!(
            "Joining room {room_code} as follower {} via leader {resolved_leader_addr}",
            device.device_id
        ),
    );

    let receive_task = tokio::spawn({
        let socket = Arc::clone(&session_socket);
        async move { runtime.run_receive_loop(socket).await }
    });

    let repl_task = tokio::spawn({
        let session = Arc::clone(&session);
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        let name = device.name.clone();
        let leader_addr = resolved_leader_addr;
        let controller = Arc::clone(&controller);
        let clock = Arc::clone(&clock);
        let scheduler = Arc::clone(&scheduler);
        let stdout = stdout.clone();
        async move {
            run_follower_repl(
                rl,
                session,
                socket,
                sender,
                name,
                leader_addr,
                controller,
                clock,
                scheduler,
                stdout,
            )
            .await
        }
    });

    tokio::select! {
        res = receive_task => {
            if let Err(err) = res {
                eprintln!("receive task failed: {err}");
            }
        }
        res = clock_task => {
            if let Err(err) = res {
                eprintln!("clock responder failed: {err}");
            }
        }
        Err(err) = drift_task => {
            eprintln!("drift task failed: {err}");
        }
        res = heartbeat_task => {
            if let Err(err) = res {
                eprintln!("heartbeat loop failed: {err}");
            }
        }
        res = role_manager_task => {
            if let Err(err) = res {
                eprintln!("role manager loop failed: {err}");
            }
        }
        res = repl_task => {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => eprintln!("repl loop stopped: {err}"),
                Err(err) => eprintln!("repl task cancelled: {err}"),
            }
        }
        _ = tokio::signal::ctrl_c() => {
            println!("Leaving room {room_code}.");
        }
    }

    // Graceful departure notification
    let leave_env = Envelope {
        sender: device.device_id,
        payload: Message::PeerLeft(device.device_id),
    };
    if let Ok(bytes) = serialize(&leave_env) {
        for addr in session.peer_socket_addrs() {
            let _ = session_socket.send_to(&bytes, addr).await;
        }
    }

    Ok(())
}
