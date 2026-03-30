use crate::api::{self, ChatMessage, GenerateResponse};
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Margin},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState},
};
use std::io;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// Display types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum Role {
    User,
    Assistant,
}

#[derive(Clone, Debug)]
struct DisplayMessage {
    role: Role,
    content: String,
    config_json: Option<String>,
}

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

struct App {
    messages: Vec<DisplayMessage>,
    conversation: Vec<ChatMessage>,
    input: String,
    cursor_pos: usize,
    scroll_offset: u16,
    total_lines: u16,
    loading: bool,
    should_quit: bool,
}

impl App {
    fn new() -> Self {
        Self {
            messages: vec![DisplayMessage {
                role: Role::Assistant,
                content: "Welcome! Describe the workflow you want to create and I'll generate it for you.".into(),
                config_json: None,
            }],
            conversation: Vec::new(),
            input: String::new(),
            cursor_pos: 0,
            scroll_offset: 0,
            total_lines: 0,
            loading: false,
            should_quit: false,
        }
    }

    fn scroll_to_bottom(&mut self) {
        self.scroll_offset = self.total_lines;
    }

    fn handle_response(&mut self, response: GenerateResponse) {
        self.conversation = response.messages;

        match response.status.as_str() {
            "questions" => {
                self.messages.push(DisplayMessage {
                    role: Role::Assistant,
                    content: response.message.unwrap_or_default(),
                    config_json: None,
                });
            }
            "completed" => {
                let config_json = response
                    .config
                    .as_ref()
                    .and_then(|c| serde_json::to_string_pretty(c).ok());
                let name = response
                    .config
                    .as_ref()
                    .map(|c| c.name.clone())
                    .unwrap_or_default();
                self.messages.push(DisplayMessage {
                    role: Role::Assistant,
                    content: format!("Workflow generated: {name}"),
                    config_json,
                });
            }
            _ => {
                self.messages.push(DisplayMessage {
                    role: Role::Assistant,
                    content: response.message.unwrap_or("Unexpected response.".into()),
                    config_json: None,
                });
            }
        }

        self.loading = false;
        self.scroll_to_bottom();
    }

    fn handle_error(&mut self, err: String) {
        self.messages.push(DisplayMessage {
            role: Role::Assistant,
            content: format!("Error: {err}"),
            config_json: None,
        });
        self.loading = false;
        self.scroll_to_bottom();
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

fn render_plain_lines(text: &str, width: usize, style: Style) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    for text_line in text.lines() {
        let chars: Vec<char> = text_line.chars().collect();
        if chars.is_empty() {
            lines.push(Line::from(""));
        } else {
            for chunk in chars.chunks(width.max(1)) {
                let s: String = chunk.iter().collect();
                lines.push(Line::from(Span::styled(format!(" {s}"), style)));
            }
        }
    }
    lines
}

/// Convert a `ratatui_core::style::Style` into `ratatui::style::Style`.
fn convert_style(s: ratatui_core::style::Style) -> Style {
    let mut out = Style::default();
    if let Some(fg) = s.fg {
        out = out.fg(convert_color(fg));
    }
    if let Some(bg) = s.bg {
        out = out.bg(convert_color(bg));
    }
    out
}

fn convert_color(c: ratatui_core::style::Color) -> Color {
    match c {
        ratatui_core::style::Color::Reset => Color::Reset,
        ratatui_core::style::Color::Black => Color::Black,
        ratatui_core::style::Color::Red => Color::Red,
        ratatui_core::style::Color::Green => Color::Green,
        ratatui_core::style::Color::Yellow => Color::Yellow,
        ratatui_core::style::Color::Blue => Color::Blue,
        ratatui_core::style::Color::Magenta => Color::Magenta,
        ratatui_core::style::Color::Cyan => Color::Cyan,
        ratatui_core::style::Color::Gray => Color::Gray,
        ratatui_core::style::Color::DarkGray => Color::DarkGray,
        ratatui_core::style::Color::LightRed => Color::LightRed,
        ratatui_core::style::Color::LightGreen => Color::LightGreen,
        ratatui_core::style::Color::LightYellow => Color::LightYellow,
        ratatui_core::style::Color::LightBlue => Color::LightBlue,
        ratatui_core::style::Color::LightMagenta => Color::LightMagenta,
        ratatui_core::style::Color::LightCyan => Color::LightCyan,
        ratatui_core::style::Color::White => Color::White,
        ratatui_core::style::Color::Rgb(r, g, b) => Color::Rgb(r, g, b),
        ratatui_core::style::Color::Indexed(i) => Color::Indexed(i),
    }
}

fn render_markdown_lines(text: &str, width: u16) -> Vec<Line<'static>> {
    let md_text = tui_markdown::from_str(text);
    let mut lines = Vec::new();
    for line in md_text.lines {
        let line_width: usize = line.spans.iter().map(|s| s.content.len()).sum();
        if line_width <= width as usize {
            let mut spans = vec![Span::raw(" ")];
            spans.extend(line.spans.into_iter().map(|s| {
                Span::styled(s.content.into_owned(), convert_style(s.style))
            }));
            lines.push(Line::from(spans));
        } else {
            let flat: String = line.spans.iter().map(|s| s.content.as_ref()).collect();
            let chars: Vec<char> = flat.chars().collect();
            for chunk in chars.chunks(width as usize) {
                let s: String = chunk.iter().collect();
                lines.push(Line::from(format!(" {s}")));
            }
        }
    }
    lines
}

fn build_chat_lines(messages: &[DisplayMessage], width: u16) -> Vec<Line<'static>> {
    let mut lines: Vec<Line<'static>> = Vec::new();
    let content_width = (width as usize).saturating_sub(2);

    for msg in messages {
        // Role label with dimmed separator
        let (label, label_color) = match msg.role {
            Role::User => ("You", Color::Cyan),
            Role::Assistant => ("AI", Color::Green),
        };

        lines.push(Line::from(vec![
            Span::styled(
                format!("{label} "),
                Style::default()
                    .fg(label_color)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                "\u{2500}".repeat(content_width.saturating_sub(label.len() + 1).min(40)),
                Style::default().fg(Color::DarkGray),
            ),
        ]));

        // Content — markdown for assistant, plain for user
        match msg.role {
            Role::User => {
                lines.extend(render_plain_lines(
                    &msg.content,
                    content_width,
                    Style::default().fg(Color::White),
                ));
            }
            Role::Assistant => {
                lines.extend(render_markdown_lines(&msg.content, width.saturating_sub(2)));
            }
        }

        // Config JSON block
        if let Some(ref json) = msg.config_json {
            lines.push(Line::from(""));
            for json_line in json.lines() {
                let chars: Vec<char> = json_line.chars().collect();
                for chunk in chars.chunks(content_width.max(1)) {
                    let s: String = chunk.iter().collect();
                    lines.push(Line::from(Span::styled(
                        format!("  {s}"),
                        Style::default().fg(Color::Yellow),
                    )));
                }
            }
        }

        lines.push(Line::from(""));
    }

    lines
}

fn draw(frame: &mut Frame, app: &mut App) {
    let area = frame.area();

    // Horizontal padding
    let padded = Layout::horizontal([
        Constraint::Length(1),
        Constraint::Min(1),
        Constraint::Length(1),
    ])
    .split(area);
    let inner = padded[1];

    let chunks = Layout::vertical([
        Constraint::Length(2), // header
        Constraint::Min(1),   // chat
        Constraint::Length(1), // separator
        Constraint::Length(1), // input
        Constraint::Length(1), // help
    ])
    .split(inner);

    let header_area = chunks[0];
    let chat_area = chunks[1];
    let sep_area = chunks[2];
    let input_area = chunks[3];
    let help_area = chunks[4];

    // Header
    let header = Paragraph::new(Line::from(vec![
        Span::styled(
            " starlight ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            "workflow generator",
            Style::default().fg(Color::DarkGray),
        ),
    ]));
    frame.render_widget(header, header_area);

    // Chat area
    let mut chat_lines = build_chat_lines(&app.messages, chat_area.width);

    if app.loading {
        chat_lines.push(Line::from(Span::styled(
            " Thinking...",
            Style::default()
                .fg(Color::Magenta)
                .add_modifier(Modifier::ITALIC),
        )));
        chat_lines.push(Line::from(""));
    }

    app.total_lines = chat_lines.len() as u16;
    let visible_height = chat_area.height;

    // Auto-scroll to bottom
    let max_scroll = app.total_lines.saturating_sub(visible_height);
    if app.scroll_offset > max_scroll {
        app.scroll_offset = max_scroll;
    }

    let chat = Paragraph::new(Text::from(chat_lines)).scroll((app.scroll_offset, 0));
    frame.render_widget(chat, chat_area);

    // Scrollbar (thin, right edge)
    let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
        .thumb_style(Style::default().fg(Color::DarkGray))
        .track_style(Style::default().fg(Color::Rgb(40, 40, 40)));
    let mut scrollbar_state = ScrollbarState::new(app.total_lines as usize)
        .position(app.scroll_offset as usize)
        .viewport_content_length(visible_height as usize);
    frame.render_stateful_widget(
        scrollbar,
        chat_area.inner(Margin {
            vertical: 0,
            horizontal: 0,
        }),
        &mut scrollbar_state,
    );

    // Separator line
    let sep_line = "\u{2500}".repeat(sep_area.width as usize);
    let sep = Paragraph::new(Span::styled(
        sep_line,
        Style::default().fg(Color::Rgb(60, 60, 60)),
    ));
    frame.render_widget(sep, sep_area);

    // Input line with caret
    let (caret, caret_style) = if app.loading {
        ("\u{2026} ", Style::default().fg(Color::DarkGray))
    } else {
        ("\u{276F} ", Style::default().fg(Color::Cyan))
    };
    let input_text = if app.loading {
        "Waiting for response...".to_string()
    } else {
        app.input.clone()
    };
    let input_style = if app.loading {
        Style::default().fg(Color::DarkGray)
    } else {
        Style::default().fg(Color::White)
    };
    let input = Paragraph::new(Line::from(vec![
        Span::styled(caret, caret_style),
        Span::styled(input_text, input_style),
    ]));
    frame.render_widget(input, input_area);

    // Cursor position (after the caret "❯ ")
    if !app.loading {
        frame.set_cursor_position((
            input_area.x + 2 + app.cursor_pos as u16,
            input_area.y,
        ));
    }

    // Help bar
    let help = Paragraph::new(Line::from(vec![
        Span::styled("enter", Style::default().fg(Color::DarkGray)),
        Span::styled(" send ", Style::default().fg(Color::Rgb(80, 80, 80))),
        Span::styled("esc", Style::default().fg(Color::DarkGray)),
        Span::styled(" quit ", Style::default().fg(Color::Rgb(80, 80, 80))),
        Span::styled("ctrl+s", Style::default().fg(Color::DarkGray)),
        Span::styled(" save ", Style::default().fg(Color::Rgb(80, 80, 80))),
        Span::styled("\u{2191}/\u{2193}", Style::default().fg(Color::DarkGray)),
        Span::styled(" scroll", Style::default().fg(Color::Rgb(80, 80, 80))),
    ]));
    frame.render_widget(help, help_area);
}

// ---------------------------------------------------------------------------
// Config save
// ---------------------------------------------------------------------------

fn save_last_config(messages: &[DisplayMessage]) -> Option<String> {
    let last_config = messages.iter().rev().find_map(|m| m.config_json.as_ref())?;

    // Parse to get the id for the filename
    let config: eng::Config = serde_json::from_str(last_config).ok()?;
    let yaml = serde_yaml_bw::to_string(&config).ok()?;
    let filename = format!("{}.yaml", config.id);
    std::fs::write(&filename, &yaml).ok()?;
    Some(filename)
}

// ---------------------------------------------------------------------------
// Main loop
// ---------------------------------------------------------------------------

pub async fn run() -> anyhow::Result<()> {
    enable_raw_mode()?;
    crossterm::execute!(io::stdout(), EnterAlternateScreen)?;

    let result = run_inner().await;

    disable_raw_mode()?;
    crossterm::execute!(io::stdout(), LeaveAlternateScreen)?;

    result
}

async fn run_inner() -> anyhow::Result<()> {
    let mut terminal = ratatui::init();
    let mut app = App::new();
    app.scroll_to_bottom();

    // Channel for receiving async API responses
    let (tx, mut rx) = mpsc::channel::<Result<GenerateResponse, String>>(1);

    loop {
        terminal.draw(|frame| draw(frame, &mut app))?;

        // Check for async API response
        if let Ok(result) = rx.try_recv() {
            match result {
                Ok(response) => app.handle_response(response),
                Err(err) => app.handle_error(err),
            }
        }

        // Poll events with a short timeout so we can check the channel
        if event::poll(std::time::Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }

                match key.code {
                    KeyCode::Esc => {
                        app.should_quit = true;
                    }
                    KeyCode::Enter if !app.loading && !app.input.is_empty() => {
                        let user_text = app.input.clone();
                        app.input.clear();
                        app.cursor_pos = 0;

                        app.messages.push(DisplayMessage {
                            role: Role::User,
                            content: user_text.clone(),
                            config_json: None,
                        });
                        app.scroll_to_bottom();
                        app.loading = true;

                        // Send API request asynchronously
                        let tx = tx.clone();
                        let is_first = app.conversation.is_empty();
                        let mut conversation = app.conversation.clone();

                        if !is_first {
                            conversation.push(ChatMessage {
                                role: "user".into(),
                                content: user_text.clone(),
                            });
                        }

                        tokio::spawn(async move {
                            let result = if is_first {
                                api::generate_workflow(Some(&user_text), None).await
                            } else {
                                api::generate_workflow(None, Some(&conversation)).await
                            };
                            let _ = tx.send(result.map_err(|e| e.to_string())).await;
                        });
                    }
                    KeyCode::Char('s') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        if let Some(filename) = save_last_config(&app.messages) {
                            app.messages.push(DisplayMessage {
                                role: Role::Assistant,
                                content: format!("Config saved to {filename}"),
                                config_json: None,
                            });
                            app.scroll_to_bottom();
                        }
                    }
                    KeyCode::Up => {
                        app.scroll_offset = app.scroll_offset.saturating_sub(1);
                    }
                    KeyCode::Down => {
                        app.scroll_offset = app.scroll_offset.saturating_add(1);
                    }
                    KeyCode::Char(c) if !app.loading => {
                        app.input.insert(app.cursor_pos, c);
                        app.cursor_pos += 1;
                    }
                    KeyCode::Backspace if !app.loading && app.cursor_pos > 0 => {
                        app.cursor_pos -= 1;
                        app.input.remove(app.cursor_pos);
                    }
                    KeyCode::Left if app.cursor_pos > 0 => {
                        app.cursor_pos -= 1;
                    }
                    KeyCode::Right if app.cursor_pos < app.input.len() => {
                        app.cursor_pos += 1;
                    }
                    KeyCode::Home => {
                        app.cursor_pos = 0;
                    }
                    KeyCode::End => {
                        app.cursor_pos = app.input.len();
                    }
                    _ => {}
                }
            }
        }

        if app.should_quit {
            break;
        }
    }

    ratatui::restore();
    Ok(())
}
