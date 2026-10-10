pub mod app;
pub mod event;
pub mod ui;

use std::io::stdout;
use std::sync::Arc;
use std::time::Duration;

use crossterm::{
    event::{Event, EventStream},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use futures::StreamExt;
use ratatui::{Terminal, backend::CrosstermBackend};
use tokio::sync::mpsc;

use crate::error::Result;
use crate::session::engine::SynkroEngine;

pub use app::{AppEvent, TuiApp};

pub async fn run_tui(
    engine: Arc<SynkroEngine>,
    room_code: String,
    device_name: String,
    invitation: Option<crate::session::invitation::RoomInvitation>,
    mut event_rx: mpsc::UnboundedReceiver<AppEvent>,
    event_tx: mpsc::UnboundedSender<AppEvent>,
) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let session = engine.session();
    let controller = engine.controller();
    let self_id = session.self_id();
    let role = session.role();

    let mut app = TuiApp::new(
        room_code,
        device_name,
        self_id,
        role,
        session,
        Arc::clone(&controller),
        event_tx.clone(),
    );
    app.invitation = invitation;

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
        // Update peer list and role dynamically from session registry
        let mut members: Vec<_> = app
            .session
            .all_alive_peers()
            .into_iter()
            .map(|(_, e)| e.info)
            .collect();
        members.push(app.session.self_info());
        // Deduplicate in case self was somehow recorded in peers
        members.sort_by_key(|p| p.device_id);
        members.dedup_by_key(|p| p.device_id);
        app.peers = members;
        app.role = app.session.role();
        app.is_leader = app.session.is_leader();

        terminal.draw(|f| ui::render(f, &app))?;

        if let Some(event) = event_rx.recv().await {
            match event {
                AppEvent::Tick => {}
                AppEvent::Key(key) => {
                    event::handle_key_event(&mut app, key, &engine).await?;
                }
                AppEvent::Log { source, text } => {
                    app.add_log(source, text);
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
