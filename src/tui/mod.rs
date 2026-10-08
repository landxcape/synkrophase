pub mod app;
pub mod event;
pub mod ui;

use std::io::stdout;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crossterm::{
    event::{Event, EventStream},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use futures::StreamExt;
use ratatui::{Terminal, backend::CrosstermBackend};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::clock::sync::ClockSync;
use crate::controller::MediaController;
use crate::error::Result;
use crate::protocol::messages::Role;
use crate::session::SessionState;
use crate::sync::scheduler::IntentScheduler;

pub use app::{AppEvent, TuiApp};

#[allow(clippy::too_many_arguments)]
pub async fn run_tui(
    room_code: String,
    device_name: String,
    self_id: Uuid,
    role: Role,
    session: Arc<SessionState>,
    controller: Arc<dyn MediaController>,
    socket: Arc<UdpSocket>,
    leader_addr: Option<SocketAddr>,
    clock: Arc<ClockSync>,
    scheduler: Arc<IntentScheduler>,
    mut event_rx: mpsc::UnboundedReceiver<AppEvent>,
    event_tx: mpsc::UnboundedSender<AppEvent>,
) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = TuiApp::new(
        room_code,
        device_name,
        self_id,
        role,
        session,
        Arc::clone(&controller),
        event_tx.clone(),
    );

    // Initial state query
    if let Ok(state) = controller.get_playback_state().await {
        app.playback = state;
    }

    // Terminal keyboard event reader task
    let mut reader = EventStream::new();
    let event_tx_keys = event_tx.clone();
    tokio::spawn(async move {
        while let Some(Ok(event)) = reader.next().await {
            if let Event::Key(key) = event {
                let _ = event_tx_keys.send(AppEvent::Key(key));
            }
        }
    });

    // Tick loop for periodic UI updates and player position polling
    let event_tx_tick = event_tx.clone();
    let controller_poll = Arc::clone(&controller);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if let Ok(state) = controller_poll.get_playback_state().await {
                let _ = event_tx_tick.send(AppEvent::PlaybackUpdate(state));
            }
            let _ = event_tx_tick.send(AppEvent::Tick);
        }
    });

    while !app.should_quit {
        // Update peer list from session registry
        app.peers = app.session.all_alive_peers().into_iter().map(|(_, e)| e.info).collect();

        terminal.draw(|f| ui::render(f, &app))?;

        if let Some(event) = event_rx.recv().await {
            match event {
                AppEvent::Tick => {}
                AppEvent::Key(key) => {
                    event::handle_key_event(
                        &mut app,
                        key,
                        &socket,
                        leader_addr,
                        &clock,
                        &scheduler,
                    )
                    .await?;
                }
                AppEvent::Log(source, msg) => {
                    app.add_log(source, msg);
                }
                AppEvent::PlaybackUpdate(state) => {
                    app.playback = state;
                }
                AppEvent::DriftUpdate(offset_us, zone, status) => {
                    app.drift.offset_us = offset_us;
                    app.drift.zone = zone;
                    app.drift.status = status;
                }
                AppEvent::PeerListUpdate(peers) => {
                    app.peers = peers;
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}
