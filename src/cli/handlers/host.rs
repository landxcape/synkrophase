use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

use tokio::net::UdpSocket;

use super::common::{create_media_controller, run_heartbeat_loop, run_role_manager_loop};
use crate::cli::repl::run_host_repl;
use crate::clock::sync::ClockSync;
use crate::config::{DeviceConfig, SyncConfig};
use crate::error::Result;
use crate::protocol::messages::{Envelope, Message, serialize};
use crate::session::SessionState;
use crate::session::discovery::Discovery;
use crate::session::runtime::{LeaderAnchorBroadcaster, SessionMessageRuntime};
use crate::sync::controller::{NoopPlaybackControl, PlaybackControl};
use crate::sync::scheduler::IntentScheduler;

pub async fn run_host(
    device: DeviceConfig,
    sync_config: SyncConfig,
    room_code: String,
    clock_port: u16,
    session_port: u16,
    headless: bool,
    no_copy: bool,
) -> Result<()> {
    let clock_socket = UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::UNSPECIFIED,
        clock_port,
    )))
    .await?;
    let session_socket = Arc::new(
        UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::UNSPECIFIED,
            session_port,
        )))
        .await?,
    );
    session_socket.set_broadcast(true)?;

    let clock = Arc::new(ClockSync::new(
        clock_socket,
        sync_config.clone(),
        device.device_id,
    ));
    let session = Arc::new(SessionState::new_leader(
        room_code.clone(),
        device.device_id,
        device.name.clone(),
    ));

    let controller = create_media_controller();
    let scheduler = Arc::new(IntentScheduler::new(
        Arc::clone(&clock) as Arc<dyn crate::sync::controller::ClockSource>,
        Arc::clone(&controller),
    ));

    let playback: Arc<dyn PlaybackControl> = Arc::new(NoopPlaybackControl::default());

    let discovery = Discovery::new()?;
    discovery.register_session(&room_code, device.device_id, session_port)?;

    let local_ip =
        local_ip_address::local_ip().unwrap_or(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
    let leader_sock_addr = SocketAddr::new(local_ip, session_port);
    let invitation = crate::session::invitation::RoomInvitation::new(
        room_code.clone(),
        leader_sock_addr,
        session_port,
        clock_port,
    );

    let mut auto_copied = false;
    if !no_copy && invitation.copy_to_clipboard().is_ok() {
        auto_copied = true;
    }

    let clock_task = {
        let clock = Arc::clone(&clock);
        tokio::spawn(async move { clock.run_responder().await })
    };

    let broadcaster = LeaderAnchorBroadcaster::new(
        Arc::clone(&session),
        Arc::clone(&clock),
        Arc::clone(&playback),
        sync_config.clone(),
    )
    .with_controller(Arc::clone(&controller));

    let broadcast_task = tokio::spawn({
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        async move { broadcaster.run_broadcast_loop(socket, sender).await }
    });

    let heartbeat_task = tokio::spawn({
        let session = Arc::clone(&session);
        let socket = Arc::clone(&session_socket);
        let sender = device.device_id;
        let cfg = sync_config.clone();
        let room = room_code.clone();
        async move { run_heartbeat_loop(session, socket, sender, room, None, cfg, None).await }
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

        let runtime = SessionMessageRuntime::new(Arc::clone(&session), None, device.name.clone())
            .with_event_tx(event_tx.clone())
            .with_playback(Arc::clone(&playback))
            .with_scheduler(Arc::clone(&scheduler))
            .with_controller(Arc::clone(&controller))
            .with_clock(Arc::clone(&clock));

        let receive_task = tokio::spawn({
            let socket = Arc::clone(&session_socket);
            async move { runtime.run_receive_loop(socket).await }
        });

        let abort_handles = vec![
            clock_task.abort_handle(),
            broadcast_task.abort_handle(),
            heartbeat_task.abort_handle(),
            role_manager_task.abort_handle(),
            receive_task.abort_handle(),
        ];

        let (engine, mut command_rx) = crate::session::SynkroEngine::new(
            Arc::clone(&session),
            Arc::clone(&controller),
            Arc::clone(&session_socket),
            None,
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

        // Initial welcome log in TUI
        let _ = event_tx.send(crate::tui::AppEvent::Log {
            source: "System".into(),
            text: format!("Room: {room_code} | Session: {session_port} | Clock: {clock_port}"),
        });
        let _ = event_tx.send(crate::tui::AppEvent::Log {
            source: "System".into(),
            text: format!("Join: {}", invitation.cli_command()),
        });
        if auto_copied {
            let _ = event_tx.send(crate::tui::AppEvent::Log {
                source: "System".into(),
                text: "Join command copied to clipboard!".into(),
            });
        }

        let tui_res = crate::tui::run_tui(
            Arc::clone(&engine),
            room_code,
            device.name,
            Some(invitation),
            event_rx,
            event_tx,
        )
        .await;

        let _ = discovery.unregister();
        engine_cmd_worker.abort();
        let _ = engine.shutdown().await;

        return tui_res;
    }

    // Headless REPL mode
    let (rl, stdout) = rustyline_async::Readline::new("synkro> ".to_string()).unwrap();
    let runtime = SessionMessageRuntime::new(
        Arc::clone(&session),
        Some(stdout.clone()),
        device.name.clone(),
    )
    .with_playback(Arc::clone(&playback))
    .with_scheduler(Arc::clone(&scheduler))
    .with_controller(Arc::clone(&controller))
    .with_clock(Arc::clone(&clock));

    let copy_msg = if auto_copied {
        " (Copied to clipboard!)"
    } else {
        ""
    };

    crate::session::runtime::print_event(
        Some(&stdout),
        &format!(
            "Hosting room {room_code} as leader {} on {local_ip}\n  - Session Port: {session_port}\n  - Clock Port: {clock_port}\n\nJoin with: {}{copy_msg}",
            device.device_id,
            invitation.cli_command()
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
        let clock = Arc::clone(&clock);
        let controller = Arc::clone(&controller);
        let scheduler = Arc::clone(&scheduler);
        let stdout = stdout.clone();
        let invitation = Some(invitation);
        async move {
            run_host_repl(
                rl, session, socket, sender, name, clock, controller, scheduler, stdout, invitation,
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
        res = broadcast_task => {
            if let Err(err) = res {
                eprintln!("broadcast task failed: {err}");
            }
        }
        res = heartbeat_task => {
            if let Err(err) = res {
                eprintln!("heartbeat task failed: {err}");
            }
        }
        res = role_manager_task => {
            if let Err(err) = res {
                eprintln!("role manager failed: {err}");
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
            println!("Shutting down host.");
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

    let _ = discovery.unregister();
    Ok(())
}
