use crate::api::{self, ChatMessage, GenerateResponse};
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use pulldown_cmark::{
    Alignment as MarkdownAlignment, Event as MarkdownEvent, Options as MarkdownOptions,
    Parser as MarkdownParser, Tag as MarkdownTag, TagEnd as MarkdownTagEnd,
};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Margin},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState},
};
use std::{io, ops::Range};
use tabled::{
    builder::Builder,
    settings::{Alignment as TableAlignment, Modify, Style as TableStyle, Width, object::Columns},
};
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

const SPINNER: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

struct App {
    messages: Vec<DisplayMessage>,
    conversation: Vec<ChatMessage>,
    input: String,
    cursor_pos: usize,
    scroll_offset: u16,
    total_lines: u16,
    loading: bool,
    spinner_frame: usize,
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
            spinner_frame: 0,
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
                let content = match response.validation.as_ref() {
                    Some(v) if v.valid && v.attempts > 1 => {
                        format!("Workflow generated, repaired, and validated: {name}")
                    }
                    Some(v) if v.valid => format!("Workflow generated and validated: {name}"),
                    _ => format!("Workflow generated: {name}"),
                };
                self.messages.push(DisplayMessage {
                    role: Role::Assistant,
                    content,
                    config_json,
                });
            }
            "validation_failed" => {
                let fallback = response
                    .validation
                    .as_ref()
                    .and_then(|v| v.errors.last())
                    .map(|e| format!("Workflow validation failed:\n\n```text\n{e}\n```"))
                    .unwrap_or_else(|| "Workflow validation failed.".into());
                self.messages.push(DisplayMessage {
                    role: Role::Assistant,
                    content: response.message.unwrap_or(fallback),
                    config_json: None,
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

#[derive(Debug)]
enum MarkdownSegment<'a> {
    Markdown(&'a str),
    Table(MarkdownTable),
}

#[derive(Debug)]
struct MarkdownTable {
    alignments: Vec<MarkdownAlignment>,
    rows: Vec<Vec<String>>,
}

#[derive(Debug)]
struct MarkdownTableState {
    source_range: Range<usize>,
    table: MarkdownTable,
    current_row: Option<Vec<String>>,
    current_cell: Option<String>,
}

impl MarkdownTableState {
    fn new(source_range: Range<usize>, alignments: Vec<MarkdownAlignment>) -> Self {
        Self {
            source_range,
            table: MarkdownTable {
                alignments,
                rows: Vec::new(),
            },
            current_row: None,
            current_cell: None,
        }
    }

    fn push_event(&mut self, event: MarkdownEvent<'_>) {
        match event {
            MarkdownEvent::Start(MarkdownTag::TableHead) => {
                self.current_row = Some(Vec::new());
            }
            MarkdownEvent::End(MarkdownTagEnd::TableHead) => {
                if let Some(row) = self.current_row.take() {
                    self.table.rows.push(row);
                }
            }
            MarkdownEvent::Start(MarkdownTag::TableRow) => {
                self.current_row = Some(Vec::new());
            }
            MarkdownEvent::End(MarkdownTagEnd::TableRow) => {
                if let Some(row) = self.current_row.take() {
                    self.table.rows.push(row);
                }
            }
            MarkdownEvent::Start(MarkdownTag::TableCell) => {
                self.current_cell = Some(String::new());
            }
            MarkdownEvent::End(MarkdownTagEnd::TableCell) => {
                if let (Some(row), Some(cell)) =
                    (self.current_row.as_mut(), self.current_cell.take())
                {
                    row.push(cell.trim().to_string());
                }
            }
            MarkdownEvent::Text(text)
            | MarkdownEvent::Code(text)
            | MarkdownEvent::InlineMath(text)
            | MarkdownEvent::DisplayMath(text)
            | MarkdownEvent::Html(text)
            | MarkdownEvent::InlineHtml(text)
            | MarkdownEvent::FootnoteReference(text) => {
                if let Some(cell) = self.current_cell.as_mut() {
                    cell.push_str(&text);
                }
            }
            MarkdownEvent::SoftBreak | MarkdownEvent::HardBreak => {
                if let Some(cell) = self.current_cell.as_mut() {
                    cell.push('\n');
                }
            }
            MarkdownEvent::Rule => {
                if let Some(cell) = self.current_cell.as_mut() {
                    cell.push_str("---");
                }
            }
            MarkdownEvent::TaskListMarker(checked) => {
                if let Some(cell) = self.current_cell.as_mut() {
                    cell.push_str(if checked { "[x] " } else { "[ ] " });
                }
            }
            _ => {}
        }
    }
}

fn split_markdown_segments(text: &str) -> Vec<MarkdownSegment<'_>> {
    let mut options = MarkdownOptions::empty();
    options.insert(MarkdownOptions::ENABLE_TABLES);

    let mut segments = Vec::new();
    let mut next_markdown_start = 0;
    let mut table_state: Option<MarkdownTableState> = None;

    for (event, range) in MarkdownParser::new_ext(text, options).into_offset_iter() {
        match event {
            MarkdownEvent::Start(MarkdownTag::Table(alignments)) => {
                if range.start > next_markdown_start {
                    segments.push(MarkdownSegment::Markdown(
                        &text[next_markdown_start..range.start],
                    ));
                }
                table_state = Some(MarkdownTableState::new(range, alignments));
            }
            MarkdownEvent::End(MarkdownTagEnd::Table) => {
                if let Some(state) = table_state.take() {
                    next_markdown_start = state.source_range.end;
                    if !state.table.rows.is_empty() {
                        segments.push(MarkdownSegment::Table(state.table));
                    }
                }
            }
            event => {
                if let Some(state) = table_state.as_mut() {
                    state.push_event(event);
                }
            }
        }
    }

    if next_markdown_start < text.len() {
        segments.push(MarkdownSegment::Markdown(&text[next_markdown_start..]));
    }

    segments
}

fn render_tui_markdown_lines(text: &str, width: u16) -> Vec<Line<'static>> {
    let md_text = tui_markdown::from_str(text);
    let mut lines = Vec::new();
    for line in md_text.lines {
        let line_width: usize = line.spans.iter().map(|s| s.content.len()).sum();
        if line_width <= width as usize {
            let mut spans = vec![Span::raw(" ")];
            spans.extend(
                line.spans
                    .into_iter()
                    .map(|s| Span::styled(s.content.into_owned(), convert_style(s.style))),
            );
            lines.push(Line::from(spans));
        } else {
            let flat: String = line.spans.iter().map(|s| s.content.as_ref()).collect();
            let chars: Vec<char> = flat.chars().collect();
            for chunk in chars.chunks(width.max(1) as usize) {
                let s: String = chunk.iter().collect();
                lines.push(Line::from(format!(" {s}")));
            }
        }
    }
    lines
}

fn render_markdown_table(table: MarkdownTable, width: u16) -> Vec<Line<'static>> {
    let mut builder = Builder::new();
    for row in table.rows {
        builder.push_record(row);
    }

    let mut table_view = builder.build();
    table_view
        .with(TableStyle::psql())
        .with(Width::wrap(width.saturating_sub(1).max(1) as usize).keep_words(true));

    for (column, alignment) in table.alignments.into_iter().enumerate() {
        match alignment {
            MarkdownAlignment::Left => {
                table_view.with(Modify::new(Columns::one(column)).with(TableAlignment::left()));
            }
            MarkdownAlignment::Center => {
                table_view.with(Modify::new(Columns::one(column)).with(TableAlignment::center()));
            }
            MarkdownAlignment::Right => {
                table_view.with(Modify::new(Columns::one(column)).with(TableAlignment::right()));
            }
            MarkdownAlignment::None => {}
        }
    }

    table_view
        .to_string()
        .lines()
        .map(|line| Line::from(format!(" {line}")))
        .collect()
}

fn render_markdown_lines(text: &str, width: u16) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    for segment in split_markdown_segments(text) {
        match segment {
            MarkdownSegment::Markdown(markdown) => {
                lines.extend(render_tui_markdown_lines(markdown, width));
            }
            MarkdownSegment::Table(table) => {
                lines.push(Line::from(""));
                lines.extend(render_markdown_table(table, width));
                lines.push(Line::from(""));
            }
        }
    }
    lines
}

fn build_chat_lines(messages: &[DisplayMessage], width: u16) -> Vec<Line<'static>> {
    let mut lines: Vec<Line<'static>> = Vec::new();
    // "❯ " or "⏺ " prefix is 2 chars; content indented by 2 on continuation lines
    let prefix_width: usize = 2;
    let content_width = (width as usize).saturating_sub(prefix_width);

    for msg in messages {
        let (icon, icon_color) = match msg.role {
            Role::User => ("\u{276F}", Color::Cyan),
            Role::Assistant => ("⏺", Color::Green),
        };
        let icon_style = Style::default().fg(icon_color).add_modifier(Modifier::BOLD);
        let indent = " ".repeat(prefix_width);

        // Render content lines — first line gets the icon prefix, rest get plain indent
        let content_lines: Vec<Line<'static>> = match msg.role {
            Role::User => render_plain_lines(
                &msg.content,
                content_width,
                Style::default().fg(Color::White),
            ),
            Role::Assistant => {
                render_markdown_lines(&msg.content, width.saturating_sub(prefix_width as u16))
            }
        };

        for (i, line) in content_lines.into_iter().enumerate() {
            if i == 0 {
                // Prepend icon to the first span of the first line
                let mut spans = vec![Span::styled(format!("{icon} "), icon_style)];
                spans.extend(line.spans);
                lines.push(Line::from(spans));
            } else {
                // Continuation lines: replace leading space with plain indent
                let mut spans = vec![Span::raw(indent.clone())];
                spans.extend(line.spans);
                lines.push(Line::from(spans));
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

/// Number of display rows the input text occupies given available width.
/// The caret "❯ " prefix takes 2 chars; continuation lines also indent by 2.
fn input_row_count(input: &str, available_width: u16) -> u16 {
    let text_width = (available_width as usize).saturating_sub(2).max(1);
    if input.is_empty() {
        return 1;
    }
    let chars = input.chars().count();
    ((chars + text_width - 1) / text_width) as u16
}

fn draw(frame: &mut Frame, app: &mut App) {
    let area = frame.area();

    // Horizontal padding
    let padded = Layout::horizontal([
        Constraint::Length(2),
        Constraint::Min(1),
        Constraint::Length(2),
    ])
    .split(area);
    let inner = padded[1];

    // Compute dynamic input height (min 1, grows with content)
    let input_rows = if app.loading {
        1
    } else {
        input_row_count(&app.input, inner.width).max(1)
    };

    let chunks = Layout::vertical([
        Constraint::Length(2),          // header
        Constraint::Min(1),             // chat
        Constraint::Length(1),          // separator
        Constraint::Length(input_rows), // input (dynamic)
        Constraint::Length(1),          // spacer
        Constraint::Length(1),          // help
    ])
    .split(inner);

    let header_area = chunks[0];
    let chat_area = chunks[1];
    let sep_area = chunks[2];
    let input_area = chunks[3];
    let help_area = chunks[5];

    // Header
    let header = Paragraph::new(Line::from(vec![
        Span::styled(
            "starlight ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("workflow generator", Style::default().fg(Color::DarkGray)),
    ]));
    frame.render_widget(header, header_area);

    // Chat area
    let mut chat_lines = build_chat_lines(&app.messages, chat_area.width);

    if app.loading {
        chat_lines.push(Line::from(vec![
            Span::styled(
                "⏺ ",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                "Thinking...",
                Style::default()
                    .fg(Color::DarkGray)
                    .add_modifier(Modifier::ITALIC),
            ),
        ]));
        chat_lines.push(Line::from(""));
    }

    app.total_lines = chat_lines.len() as u16;
    let visible_height = chat_area.height;

    let max_scroll = app.total_lines.saturating_sub(visible_height);
    if app.scroll_offset > max_scroll {
        app.scroll_offset = max_scroll;
    }

    let chat = Paragraph::new(Text::from(chat_lines)).scroll((app.scroll_offset, 0));
    frame.render_widget(chat, chat_area);

    // Scrollbar
    let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
        .thumb_style(Style::default().fg(Color::Rgb(50, 50, 50)))
        .begin_style(Style::default().fg(Color::Rgb(30, 30, 30)))
        .end_style(Style::default().fg(Color::Rgb(30, 30, 30)))
        .track_style(Style::default().fg(Color::Rgb(30, 30, 30)));
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
    frame.render_widget(
        Paragraph::new(Span::styled(
            sep_line,
            Style::default().fg(Color::Rgb(60, 60, 60)),
        )),
        sep_area,
    );

    // Input — multiline paragraph with ❯ prefix on first line, spaces on continuation
    let text_width = (input_area.width as usize).saturating_sub(2).max(1);
    if app.loading {
        let spinner = SPINNER[app.spinner_frame % SPINNER.len()];
        frame.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled(
                    format!("{spinner} "),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    "Waiting for response...",
                    Style::default().fg(Color::DarkGray),
                ),
            ])),
            input_area,
        );
    } else {
        let caret_style = Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD);
        let text_style = Style::default().fg(Color::White);
        let input_chars: Vec<char> = app.input.chars().collect();
        let mut input_lines: Vec<Line<'static>> = Vec::new();

        if input_chars.is_empty() {
            input_lines.push(Line::from(Span::styled("\u{276F} ", caret_style)));
        } else {
            for (i, chunk) in input_chars.chunks(text_width).enumerate() {
                let s: String = chunk.iter().collect();
                if i == 0 {
                    input_lines.push(Line::from(vec![
                        Span::styled("\u{276F} ", caret_style),
                        Span::styled(s, text_style),
                    ]));
                } else {
                    input_lines.push(Line::from(vec![
                        Span::raw("  "),
                        Span::styled(s, text_style),
                    ]));
                }
            }
        }

        frame.render_widget(Paragraph::new(Text::from(input_lines)), input_area);

        // Cursor: map char offset to row/col within the input area
        let cursor_char = app.cursor_pos.min(input_chars.len());
        let cursor_row = (cursor_char / text_width) as u16;
        let cursor_col = (cursor_char % text_width) as u16;
        frame.set_cursor_position((input_area.x + 2 + cursor_col, input_area.y + cursor_row));
    }

    // Help bar
    let help = Paragraph::new(Line::from(vec![
        Span::styled("enter", Style::default().fg(Color::DarkGray)),
        Span::styled(" send  ", Style::default().fg(Color::Rgb(60, 60, 60))),
        Span::styled("esc", Style::default().fg(Color::DarkGray)),
        Span::styled(" quit  ", Style::default().fg(Color::Rgb(60, 60, 60))),
        Span::styled("ctrl+s", Style::default().fg(Color::DarkGray)),
        Span::styled(" save  ", Style::default().fg(Color::Rgb(60, 60, 60))),
        Span::styled("\u{2191}\u{2193}", Style::default().fg(Color::DarkGray)),
        Span::styled(" scroll", Style::default().fg(Color::Rgb(60, 60, 60))),
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

        // Advance spinner on each tick while loading
        if app.loading {
            app.spinner_frame = app.spinner_frame.wrapping_add(1);
        }

        // Poll events with a short timeout so we can check the channel
        if event::poll(std::time::Duration::from_millis(80))? {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn line_text(line: &Line<'_>) -> String {
        line.spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect()
    }

    #[test]
    fn renders_markdown_tables_with_tabled() {
        let markdown = "| Name | Count |\n| :--- | ----: |\n| Alpha | 12 |\n";

        let rendered_lines = render_markdown_lines(markdown, 80)
            .iter()
            .map(line_text)
            .collect::<Vec<_>>();
        let rendered = rendered_lines.join("\n");

        assert_eq!(rendered_lines.first().map(String::as_str), Some(""));
        assert_eq!(rendered_lines.last().map(String::as_str), Some(""));
        assert!(rendered.contains("Name"));
        assert!(rendered.contains("Count"));
        assert!(rendered.contains("Alpha"));
        assert!(!rendered.contains(":---"));
        assert!(!rendered.contains("----:"));
    }

    #[test]
    fn keeps_markdown_before_and_after_tables() {
        let markdown = "Before\n\n| A | B |\n| - | - |\n| 1 | 2 |\n\nAfter";

        let rendered = render_markdown_lines(markdown, 80)
            .iter()
            .map(line_text)
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("Before"));
        assert!(rendered.contains("A"));
        assert!(rendered.contains("1"));
        assert!(rendered.contains("After"));
    }
}
