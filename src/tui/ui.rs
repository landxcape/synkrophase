use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, List, ListItem, Paragraph, Row, Table},
};

use super::app::{InputMode, TuiApp};

pub fn render(frame: &mut Frame, app: &TuiApp) {
    let main_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header / Room Bar
            Constraint::Length(4), // Now Playing & Sleek Timeline
            Constraint::Min(8),    // Split: Peers & Clock (left) + Log & Activity (right)
            Constraint::Length(3), // Command & Input bar
        ])
        .split(frame.area());

    render_header(frame, app, main_layout[0]);
    render_playback(frame, app, main_layout[1]);
    render_middle_panel(frame, app, main_layout[2]);
    render_command_bar(frame, app, main_layout[3]);
}

fn render_header(frame: &mut Frame, app: &TuiApp, area: ratatui::layout::Rect) {
    let role_span = if app.is_leader {
        Span::styled(
            " [LEADER] ",
            Style::default()
                .fg(Color::Black)
                .bg(Color::Green)
                .add_modifier(Modifier::BOLD),
        )
    } else {
        Span::styled(
            " [FOLLOWER] ",
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
    };

    let header_line = Line::from(vec![
        Span::styled(
            " Synkrophase ",
            Style::default()
                .fg(Color::Magenta)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("• Room: "),
        Span::styled(
            &app.room_code,
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" • Role: "),
        role_span,
        Span::raw(" • Device: "),
        Span::styled(&app.device_name, Style::default().fg(Color::White)),
    ]);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::DarkGray));

    let paragraph = Paragraph::new(header_line)
        .block(block)
        .alignment(Alignment::Left);
    frame.render_widget(paragraph, area);
}

fn render_playback(frame: &mut Frame, app: &TuiApp, area: ratatui::layout::Rect) {
    let border_color = if app.playback.is_playing {
        Color::Green
    } else {
        Color::Yellow
    };

    let block = Block::default()
        .title(" Now Playing ")
        .title_style(
            Style::default()
                .fg(border_color)
                .add_modifier(Modifier::BOLD),
        )
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner_area = block.inner(area);
    frame.render_widget(block, area);

    if inner_area.height < 2 {
        return;
    }

    let sub_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(1)])
        .split(inner_area);

    // Row 1: Track Metadata & Status
    let (icon, state_text) = if app.playback.is_playing {
        ("▶ ", "Playing")
    } else {
        ("⏸ ", "Paused")
    };

    let title = app
        .playback
        .metadata
        .as_ref()
        .map(|m| m.title.as_str())
        .unwrap_or("No track active");

    let artist_album = app
        .playback
        .metadata
        .as_ref()
        .map(|m| {
            let mut s = String::new();
            if let Some(artist) = &m.artist {
                s.push_str(artist);
            }
            if let Some(album) = &m.album {
                if !s.is_empty() {
                    s.push_str(" • ");
                }
                s.push_str(album);
            }
            s
        })
        .unwrap_or_default();

    let meta_line = Line::from(vec![
        Span::styled(
            format!(" {icon}"),
            Style::default()
                .fg(border_color)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            title,
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        if !artist_album.is_empty() {
            Span::styled(
                format!("   {artist_album}"),
                Style::default().fg(Color::DarkGray),
            )
        } else {
            Span::raw("")
        },
        Span::styled(
            format!("  [{state_text}]"),
            Style::default().fg(border_color),
        ),
    ]);
    frame.render_widget(Paragraph::new(meta_line), sub_layout[0]);

    // Row 2: Sleek Unicode Timeline Slider
    let pos_sec = (app.playback.position_us as f64) / 1_000_000.0;
    let dur_sec = app
        .playback
        .metadata
        .as_ref()
        .and_then(|m| m.duration_us)
        .map(|d| (d as f64) / 1_000_000.0)
        .unwrap_or(0.0);

    let cur_str = format!(
        "{:02}:{:02}",
        (pos_sec / 60.0) as i64,
        (pos_sec % 60.0) as i64
    );
    let total_str = if dur_sec > 0.0 {
        format!(
            "{:02}:{:02}",
            (dur_sec / 60.0) as i64,
            (dur_sec % 60.0) as i64
        )
    } else {
        "--:--".to_string()
    };

    // Calculate slider width available
    let total_width = sub_layout[1].width as usize;

    let (sync_badge_str, sync_color) = if app.is_leader {
        ("   (Leader)".to_string(), Color::Green)
    } else {
        let color = match app.drift.zone {
            1 => Color::Green,
            2 => Color::Yellow,
            _ => Color::Red,
        };
        (format!("   ({:+}µs sync)", app.drift.offset_us), color)
    };

    // Overhead: " " (1) + cur_str (5) + " [" (2) + "] " (2) + total_str (5) + sync_badge_str (~16) = ~31
    let overhead = 18 + sync_badge_str.len();
    let bar_width = total_width.saturating_sub(overhead).max(10);
    let ratio = app.progress_ratio().clamp(0.0, 1.0);
    let filled_chars = ((ratio * bar_width as f64).round() as usize).min(bar_width);
    let unfilled_chars = bar_width.saturating_sub(filled_chars);

    let filled_part: String = if filled_chars > 0 {
        format!("{}●", "━".repeat(filled_chars.saturating_sub(1)))
    } else {
        "●".to_string()
    };
    let unfilled_part: String =
        "─".repeat(unfilled_chars.saturating_sub(if filled_chars == 0 { 1 } else { 0 }));

    let slider_color = if app.playback.is_playing {
        Color::Cyan
    } else {
        Color::Yellow
    };

    let timeline_line = Line::from(vec![
        Span::raw(" "),
        Span::styled(cur_str, Style::default().fg(Color::White)),
        Span::styled(" [", Style::default().fg(Color::DarkGray)),
        Span::styled(
            filled_part,
            Style::default()
                .fg(slider_color)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(unfilled_part, Style::default().fg(Color::Rgb(60, 60, 60))),
        Span::styled("] ", Style::default().fg(Color::DarkGray)),
        Span::styled(total_str, Style::default().fg(Color::DarkGray)),
        Span::styled(sync_badge_str, Style::default().fg(sync_color)),
    ]);
    frame.render_widget(Paragraph::new(timeline_line), sub_layout[1]);
}

fn render_middle_panel(frame: &mut Frame, app: &TuiApp, area: ratatui::layout::Rect) {
    let middle_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(area);

    // Left Panel: Peers & Drift
    let left_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(4), Constraint::Length(4)])
        .split(middle_layout[0]);

    // Peer Table
    let rows: Vec<Row> = app
        .peers
        .iter()
        .map(|p| {
            let is_self = p.device_id == app.self_id;
            let role_str = match p.role {
                crate::protocol::messages::Role::Leader => "Leader",
                crate::protocol::messages::Role::Moderator => "Moderator",
                crate::protocol::messages::Role::Listener => "Listener",
            };
            let offset_str = if is_self {
                "Local".to_string()
            } else {
                format!("{:+}µs", p.clock_offset_us)
            };
            let name_display = if is_self {
                format!("{} (You)", p.name)
            } else {
                p.name.clone()
            };

            Row::new(vec![
                Span::styled(
                    name_display,
                    if is_self {
                        Style::default().fg(Color::Cyan)
                    } else {
                        Style::default()
                    },
                ),
                Span::raw(role_str),
                Span::styled(offset_str, Style::default().fg(Color::Yellow)),
            ])
        })
        .collect();

    let peer_table = Table::new(
        rows,
        [
            Constraint::Percentage(50),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
        ],
    )
    .header(
        Row::new(vec!["Peer", "Role", "Offset"]).style(
            Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(
        Block::default()
            .title(" Room Members ")
            .title_style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::DarkGray)),
    );

    frame.render_widget(peer_table, left_layout[0]);

    // Drift Health
    let drift_color = match app.drift.zone {
        1 => Color::Green,
        2 => Color::Yellow,
        _ => Color::Red,
    };
    let drift_line = Line::from(vec![
        Span::raw("Sync: "),
        Span::styled(
            &app.drift.status,
            Style::default()
                .fg(drift_color)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" | Offset: "),
        Span::styled(
            format!("{:+0.2}ms", (app.drift.offset_us as f64) / 1000.0),
            Style::default().fg(Color::White),
        ),
    ]);
    let drift_block = Block::default()
        .title(" Clock Synchronization Health ")
        .title_style(Style::default().fg(drift_color))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::DarkGray));
    frame.render_widget(
        Paragraph::new(drift_line).block(drift_block),
        left_layout[1],
    );

    // Right Panel: Activity Log & Chat
    let log_items: Vec<ListItem> = app
        .logs
        .iter()
        .map(|entry| {
            let src_color = match entry.source.as_str() {
                "System" => Color::Magenta,
                "Leader" => Color::Green,
                _ => Color::Cyan,
            };
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("[{}] ", entry.timestamp),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(
                    format!("[{}] ", entry.source),
                    Style::default().fg(src_color).add_modifier(Modifier::BOLD),
                ),
                Span::raw(&entry.text),
            ]))
        })
        .collect();

    let logs_list = List::new(log_items).block(
        Block::default()
            .title(" Activity & Room Chat ")
            .title_style(
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            )
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::DarkGray)),
    );

    frame.render_widget(logs_list, middle_layout[1]);
}

fn render_command_bar(frame: &mut Frame, app: &TuiApp, area: ratatui::layout::Rect) {
    match app.input_mode {
        InputMode::Normal => {
            let help_line = Line::from(vec![
                Span::styled(
                    "[Space] ",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw("Play/Pause  • "),
                Span::styled(
                    "[←/→] ",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw("Seek ±5s  • "),
                Span::styled(
                    "[/] ",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw("Chat/Cmd  • "),
                Span::styled(
                    "[q] ",
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                ),
                Span::raw("Quit"),
            ]);

            let block = Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(Color::DarkGray));
            frame.render_widget(
                Paragraph::new(help_line)
                    .block(block)
                    .alignment(Alignment::Center),
                area,
            );
        }
        InputMode::Editing => {
            let input_line = Line::from(vec![
                Span::styled(
                    "Input: ",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(&app.input_buffer, Style::default().fg(Color::White)),
                Span::styled("█", Style::default().fg(Color::Yellow)),
            ]);

            let block = Block::default()
                .title(" Send Message or Command (Enter to submit, Esc to cancel) ")
                .title_style(Style::default().fg(Color::Cyan))
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(Color::Cyan));
            frame.render_widget(Paragraph::new(input_line).block(block), area);
        }
    }
}
