// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Result pane: tables, documents, the running view, and the getting-started guide.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Color;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::text::Span;
use ratatui::Frame;

use super::effects;
use super::theme;
use super::widgets;
use super::Ctx;
use super::HitTarget;
use super::ViewCache;
use crate::result_view::Grid;
use crate::result_view::LineKind;
use crate::result_view::ResultBody;
use crate::result_view::ResultTone;
use crate::result_view::ResultView;
use crate::result_view::COLUMN_GAP;
use crate::state::CommandExecutionState;
use crate::state::ExecutionPhase;
use crate::state::FocusArea;
use crate::state::Pane;
use crate::text::display_width;
use crate::text::slice_columns;
use crate::text::truncate;

/// Ticks the border keeps the outcome color after a command finished.
const FLASH_TICKS: u64 = 36;
/// Ticks over which the rows of a new result cascade in.
const REVEAL_TICKS: u64 = 14;
const REVEAL_ROWS_PER_TICK: usize = 4;
/// Longest line that is still syntax highlighted on every frame.
const HIGHLIGHT_LIMIT: usize = 4096;
const BAR_MAX_WIDTH: u16 = 44;

/// Returns the area that result rows are drawn in.
///
/// The frame's layout pass and the painter share this so the viewport that is
/// clamped is exactly the one that is shown.
pub(super) fn body_area(area: Rect, grid: bool) -> Rect {
    let inner = widgets::inset(area);
    let header = u16::from(grid).min(inner.height);
    Rect {
        x: inner.x.saturating_add(1),
        y: inner.y + header,
        width: inner.width.saturating_sub(2),
        height: inner.height - header,
    }
}

pub(super) fn paint(frame: &mut Frame, ctx: &Ctx<'_>, area: Rect, view: &mut ViewCache) {
    let state = ctx.state;
    let motion = state.motion();
    let focused = state.focus == FocusArea::Result;
    let running = state.execution.phase() == ExecutionPhase::Running;
    let result = state.result().filter(|_| !running);

    let mut border = ctx.pane_border(Pane::Result);
    if running {
        border = theme::mix(border, theme::CYAN, 0.55);
    }
    let outcome = [
        (ExecutionPhase::Succeeded, theme::SUCCESS),
        (ExecutionPhase::Failed, theme::DANGER),
        (ExecutionPhase::Cancelled, theme::WARNING),
    ]
    .into_iter()
    .find_map(|(phase, color)| ctx.phase_age(phase).map(|age| (color, ctx.tick - age)));
    if let Some((color, since)) = outcome {
        if motion.is_playing(since, FLASH_TICKS) {
            border = theme::mix(color, border, motion.transition(since, FLASH_TICKS));
        }
    }

    let mut title = vec![widgets::pane_title(result.map_or("Result", ResultView::title), focused)];
    if state.result_zoom {
        title.push(Span::styled("zoomed ", theme::faint()));
    }
    let mut block = widgets::pane(border).title_top(Line::from(title));
    if let Some(status) = status(ctx) {
        block = block.title_top(status.right_aligned());
    }
    let body = body_area(area, result.is_some_and(|result| result.grid().is_some()));
    if let Some(result) = result {
        if matches!(result.body(), ResultBody::Lines(_)) {
            let wrap = if result.wraps() { " wrap on " } else { " wrap off " };
            block = block.title_bottom(Line::from(Span::styled(wrap, theme::faint())));
        }
        if let Some(position) = position(result, body) {
            block = block.title_bottom(position.right_aligned());
        }
    }
    let inner = block.inner(area);
    frame.render_widget(block, area);
    view.register(area, HitTarget::Result);
    if inner.is_empty() {
        return;
    }

    let buffer = frame.buffer_mut();
    match result {
        _ if running => paint_running(buffer, ctx, inner),
        None => paint_guide(buffer, ctx, inner),
        Some(result) => {
            match result.body() {
                ResultBody::Grid(grid) => paint_grid(buffer, ctx, inner, body, result, grid, view),
                ResultBody::Lines(_) => paint_lines(buffer, ctx, body, result),
            }
            widgets::scrollbar(
                buffer,
                area.right() - 1,
                body.y,
                body.height,
                result.row_count(),
                usize::from(body.height),
                result.top(),
                if focused { theme::PRIMARY } else { theme::FAINT },
            );
        }
    }
}

/// Builds the outcome marker shown in the top border.
fn status(ctx: &Ctx<'_>) -> Option<Line<'static>> {
    let state = ctx.state;
    let duration = state.run_duration().map(widgets::format_duration);
    let (glyph, text, color) = match state.execution.phase() {
        ExecutionPhase::Idle | ExecutionPhase::Confirming => return None,
        ExecutionPhase::Running => (
            theme::spinner(ctx.tick),
            format!("running {}", duration.unwrap_or_default()),
            theme::CYAN,
        ),
        ExecutionPhase::Succeeded => (
            theme::GLYPH_OK,
            duration.unwrap_or_else(|| "done".to_string()),
            theme::SUCCESS,
        ),
        ExecutionPhase::Failed => (theme::GLYPH_FAIL, "failed".to_string(), theme::DANGER),
        ExecutionPhase::Cancelled => (theme::GLYPH_STOP, "cancelled".to_string(), theme::WARNING),
    };
    Some(Line::from(Span::styled(
        format!(" {glyph} {} ", text.trim_end()),
        theme::bold(color),
    )))
}

/// Builds the viewport position shown in the bottom border.
fn position(result: &ResultView, body: Rect) -> Option<Line<'static>> {
    let total = result.row_count();
    if total == 0 {
        return None;
    }
    let text = match result.body() {
        ResultBody::Grid(grid) => {
            let window = grid.column_window(result.column(), body.width);
            let (Some((first, _)), Some((last, _))) = (window.columns.first(), window.columns.last()) else {
                return None;
            };
            format!(
                " row {}/{total} · {}cols {}-{}/{}{} ",
                result.cursor() + 1,
                if window.hidden_left { "◂ " } else { "" },
                first + 1,
                last + 1,
                grid.headers().len(),
                if window.hidden_right { " ▸" } else { "" },
            )
        }
        ResultBody::Lines(_) => {
            let last = (result.top() + usize::from(body.height)).min(total);
            format!(" {}-{last}/{total} ", result.top() + 1)
        }
    };
    Some(Line::from(Span::styled(text, theme::faint())))
}

/// Returns how many rows of a just-arrived result are shown yet.
fn revealed_rows(ctx: &Ctx<'_>) -> usize {
    let motion = ctx.state.motion();
    [ExecutionPhase::Succeeded, ExecutionPhase::Failed]
        .into_iter()
        .find_map(|phase| ctx.phase_age(phase))
        .filter(|age| motion.is_playing(ctx.tick - age, REVEAL_TICKS))
        .map_or(usize::MAX, |age| (age as usize + 1) * REVEAL_ROWS_PER_TICK)
}

fn paint_grid(
    buffer: &mut Buffer,
    ctx: &Ctx<'_>,
    inner: Rect,
    body: Rect,
    result: &ResultView,
    grid: &Grid,
    view: &mut ViewCache,
) {
    let window = grid.column_window(result.column(), body.width);
    effects::fill(buffer, Rect { height: 1, ..inner }, theme::SURFACE);
    let mut x = body.x;
    for (column, width) in &window.columns {
        put_cell(
            buffer,
            x,
            inner.y,
            &grid.headers()[*column],
            *width,
            grid.is_numeric(*column),
            theme::bright(),
        );
        x += width + COLUMN_GAP;
    }
    if grid.row_count() == 0 {
        widgets::put(buffer, body.x, body.y, "No rows.", theme::faint(), body.width);
        return;
    }

    let focused = ctx.state.focus == FocusArea::Result;
    for offset in 0..usize::from(body.height).min(revealed_rows(ctx)) {
        let index = result.top() + offset;
        if index >= grid.row_count() {
            break;
        }
        let row = Rect {
            y: body.y + offset as u16,
            height: 1,
            ..inner
        };
        view.register(row, HitTarget::ResultRow(index));

        let selected = index == result.cursor();
        let background = match (selected, focused) {
            (true, true) => theme::SELECTION,
            (true, false) => theme::SURFACE_RAISED,
            (false, _) if ctx.hovered(row) => theme::SURFACE,
            (false, _) if index % 2 == 1 => theme::STRIPE,
            (false, _) => theme::BACKGROUND,
        };
        if background != theme::BACKGROUND {
            effects::fill(buffer, row, background);
        }
        let style = if selected {
            theme::colored(theme::BRIGHT)
        } else {
            theme::text()
        };
        let mut x = body.x;
        for (column, width) in &window.columns {
            put_cell(
                buffer,
                x,
                row.y,
                grid.cell(index, *column),
                *width,
                grid.is_numeric(*column),
                style,
            );
            x += width + COLUMN_GAP;
        }
    }
}

fn put_cell(buffer: &mut Buffer, x: u16, y: u16, text: &str, width: u16, align_right: bool, style: Style) {
    let shown = truncate(text, usize::from(width));
    let indent = if align_right {
        width.saturating_sub(display_width(&shown) as u16)
    } else {
        0
    };
    widgets::put(buffer, x + indent, y, &shown, style, width - indent);
}

fn paint_lines(buffer: &mut Buffer, ctx: &Ctx<'_>, body: Rect, result: &ResultView) {
    if body.is_empty() {
        return;
    }
    if result.row_count() == 0 {
        widgets::put(
            buffer,
            body.x,
            body.y,
            "The command returned no output.",
            theme::faint(),
            body.width,
        );
        return;
    }

    for offset in 0..usize::from(body.height).min(revealed_rows(ctx)) {
        let Some(row) = result.row(result.top() + offset) else {
            break;
        };
        let Some(document_line) = result.line(row.line) else {
            continue;
        };
        let Some(text) = document_line.text.get(row.start..row.end) else {
            continue;
        };
        let line = styled(document_line.kind, text, row.first, result.tone());
        let y = body.y + offset as u16;
        if result.column() == 0 {
            widgets::put_line(buffer, body.x, y, &line, body.width);
        } else {
            let panned = pan(&line, result.column(), usize::from(body.width));
            widgets::put_line(buffer, body.x, y, &panned, body.width);
        }
    }
}

fn styled(kind: LineKind, text: &str, first: bool, tone: ResultTone) -> Line<'_> {
    match kind {
        LineKind::Plain if tone == ResultTone::Failure => Line::from(Span::styled(
            text,
            theme::colored(theme::mix(theme::DANGER, theme::BRIGHT, 0.4)),
        )),
        LineKind::Plain => Line::from(Span::styled(text, theme::text())),
        LineKind::Heading => Line::from(Span::styled(text, theme::bright())),
        LineKind::Structured => structured_line(text),
        LineKind::Success => marked(text, first, theme::SUCCESS),
        LineKind::Failure => marked(text, first, theme::DANGER),
        LineKind::Summary { succeeded, failed } => Line::from(vec![
            count_chip(theme::GLYPH_OK, succeeded, "succeeded", theme::SUCCESS),
            Span::raw("  "),
            count_chip(theme::GLYPH_FAIL, failed, "failed", theme::DANGER),
        ]),
    }
}

/// Colors the leading mark of the first row of a summary entry.
fn marked(text: &str, first: bool, color: Color) -> Line<'_> {
    let mark_end = text.char_indices().nth(1).map(|(index, _)| index);
    match mark_end {
        Some(mark_end) if first => Line::from(vec![
            Span::styled(&text[..mark_end], theme::bold(color)),
            Span::styled(&text[mark_end..], theme::text()),
        ]),
        _ => Line::from(Span::styled(text, theme::text())),
    }
}

fn count_chip(glyph: &str, count: usize, noun: &str, color: Color) -> Span<'static> {
    let label = format!("{glyph} {count} {noun}");
    if count == 0 {
        Span::styled(format!(" {label} "), theme::faint())
    } else {
        widgets::chip(&label, theme::BACKGROUND, color)
    }
}

/// Splits one line of JSON or of a Rust debug representation into colored tokens.
///
/// Keys, strings, numbers, and literals each get a color, and the type names and
/// brackets that only give the value its shape recede. The scan is line-local and
/// forgiving: text in neither notation simply stays plain.
fn structured_line(text: &str) -> Line<'_> {
    if text.len() > HIGHLIGHT_LIMIT {
        return Line::from(Span::styled(text, theme::text()));
    }

    let key = theme::colored(theme::CYAN);
    let string = theme::colored(theme::mix(theme::SUCCESS, theme::BRIGHT, 0.35));
    let number = theme::colored(theme::mix(theme::WARNING, theme::BRIGHT, 0.2));
    let literal = theme::colored(theme::PRIMARY);
    let bytes = text.as_bytes();
    let mut spans = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        let start = index;
        let style = match bytes[index] {
            b'"' => {
                index += 1;
                while index < bytes.len() {
                    match bytes[index] {
                        b'\\' => index += 2,
                        b'"' => {
                            index += 1;
                            break;
                        }
                        _ => index += 1,
                    }
                }
                // An escape at the very end can step past the line.
                index = index.min(bytes.len());
                while !text.is_char_boundary(index) {
                    index += 1;
                }
                if text[index..].trim_start().starts_with(':') {
                    key
                } else {
                    string
                }
            }
            b'-' | b'0'..=b'9' => {
                while index < bytes.len() && matches!(bytes[index], b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9') {
                    index += 1;
                }
                number
            }
            b'{' | b'}' | b'[' | b']' | b'(' | b')' | b',' | b':' => {
                index += 1;
                theme::muted()
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                while index < bytes.len() && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_') {
                    index += 1;
                }
                let rest = text[index..].trim_start();
                if matches!(&text[start..index], "true" | "false" | "null" | "None") {
                    literal
                } else if rest.starts_with(':') && !rest.starts_with("::") {
                    key
                } else if rest.starts_with(['{', '(']) {
                    // A type or variant name that only opens the value it wraps.
                    theme::muted()
                } else {
                    theme::text()
                }
            }
            _ => {
                index += 1;
                while index < bytes.len() && !is_token_start(bytes[index]) {
                    index += 1;
                }
                while !text.is_char_boundary(index) {
                    index += 1;
                }
                theme::text()
            }
        };
        spans.push(Span::styled(&text[start..index], style));
    }
    Line::from(spans)
}

fn is_token_start(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'"' | b'-' | b'_' | b'{' | b'}' | b'[' | b']' | b'(' | b')' | b',' | b':'
        )
}

/// Returns the cells `start..start + width` of a styled line.
fn pan<'a>(line: &'a Line<'_>, start: usize, width: usize) -> Line<'a> {
    let end = start.saturating_add(width);
    let mut column = 0;
    let mut spans = Vec::new();
    for span in &line.spans {
        let span_start = column;
        column += span.width();
        if column <= start {
            continue;
        }
        if span_start >= end {
            break;
        }
        let visible_start = span_start.max(start);
        let visible = slice_columns(&span.content, visible_start - span_start, end - visible_start);
        spans.push(Span::styled(visible, span.style));
    }
    Line::from(spans)
}

fn paint_running(buffer: &mut Buffer, ctx: &Ctx<'_>, inner: Rect) {
    let state = ctx.state;
    let CommandExecutionState::Running { command_id, .. } = &state.execution else {
        return;
    };
    let title = state
        .commands()
        .iter()
        .find(|command| command.id == command_id)
        .map_or(command_id.as_str(), |command| command.title);
    let elapsed = state.run_duration().map(widgets::format_duration).unwrap_or_default();
    let progress = state
        .progress_message
        .as_deref()
        .unwrap_or("waiting for the first response");
    let width = usize::from(inner.width.saturating_sub(4));

    let lines = [
        Some(Line::from(vec![
            Span::styled(theme::spinner(ctx.tick), theme::bold(theme::CYAN)),
            Span::styled("  Running ", theme::muted()),
            Span::styled(truncate(title, width.saturating_sub(12)).into_owned(), theme::bright()),
        ])),
        Some(Line::from(Span::styled(
            truncate(&format!("{command_id} · {elapsed}"), width).into_owned(),
            theme::faint(),
        ))),
        Some(Line::default()),
        // The activity bar takes this row.
        None,
        Some(Line::from(Span::styled(
            truncate(progress, width).into_owned(),
            theme::muted(),
        ))),
        Some(Line::default()),
        Some(Line::from(vec![
            widgets::keycap("Esc"),
            Span::styled(" cancels", theme::faint()),
        ])),
    ];
    let top = inner.y + inner.height.saturating_sub(lines.len() as u16) / 2;
    for (offset, line) in lines.iter().enumerate() {
        let y = top + offset as u16;
        if y >= inner.bottom() {
            break;
        }
        match line {
            Some(line) => widgets::put_line_centered(buffer, inner, y, line),
            None => paint_bar(buffer, ctx, inner, y),
        }
    }
}

/// Draws an indeterminate progress bar with a highlight that keeps travelling.
fn paint_bar(buffer: &mut Buffer, ctx: &Ctx<'_>, inner: Rect, y: u16) {
    const BAND: f32 = 6.0;
    let width = BAR_MAX_WIDTH.min(inner.width.saturating_sub(6));
    let x = inner.x + (inner.width - width) / 2;
    let animated = ctx.state.motion().enabled();
    let age = ctx.phase_age(ExecutionPhase::Running).unwrap_or(0);
    let head = (age as f32 * 1.3) % (f32::from(width) + 2.0 * BAND) - BAND;
    for index in 0..width {
        let position = f32::from(index) / f32::from(width.saturating_sub(1).max(1));
        let rest = theme::mix(theme::BACKGROUND, theme::gradient(position), 0.3);
        let color = if animated {
            let strength = (1.0 - (f32::from(index) - head).abs() / BAND).max(0.0);
            theme::mix(
                rest,
                theme::mix(theme::gradient(position), theme::BRIGHT, 0.25),
                strength,
            )
        } else {
            theme::gradient(position)
        };
        widgets::put(buffer, x + index, y, theme::RULE_HEAVY, theme::colored(color), 1);
    }
}

/// Draws the idle state: three steps that track what the operator has already done.
fn paint_guide(buffer: &mut Buffer, ctx: &Ctx<'_>, inner: Rect) {
    const STEP_WIDTH: usize = 34;
    let state = ctx.state;
    let cancelled = state.execution.phase() == ExecutionPhase::Cancelled;
    let configured = !state.namesrv_addr.trim().is_empty();
    let current = match state.focus {
        FocusArea::Namesrv => 0,
        FocusArea::Search | FocusArea::CommandTree => 1,
        FocusArea::Args | FocusArea::Result => 2,
    };
    let pointer = theme::mix(
        theme::PRIMARY,
        theme::CYAN,
        crate::motion::wave(ctx.tick, 48) * state.motion().ambient(),
    );

    let heading = if cancelled {
        Line::from(vec![
            Span::styled(theme::GLYPH_STOP, theme::bold(theme::WARNING)),
            Span::styled(" Cancelled", theme::bright()),
            Span::styled("  a request the server already accepted is not undone", theme::muted()),
        ])
    } else {
        Line::from(vec![
            Span::styled(theme::GLYPH_BRAND, theme::bold(theme::gradient(0.5))),
            Span::styled(" Ready when you are", theme::bright()),
        ])
    };
    let steps = [
        ("Set the NameServer address", "n"),
        ("Pick a command", "/"),
        ("Fill in the parameters and run", "Enter"),
    ];
    let mut lines = vec![heading, Line::default()];
    for (index, (text, key)) in steps.into_iter().enumerate() {
        let (marker, marker_style) = if index == 0 && configured {
            (theme::GLYPH_OK, theme::bold(theme::SUCCESS))
        } else if index == current {
            (theme::GLYPH_POINTER, theme::bold(pointer))
        } else {
            (" ", theme::faint())
        };
        let text_style = if index == current {
            theme::text()
        } else {
            theme::muted()
        };
        lines.push(Line::from(vec![
            Span::styled(marker, marker_style),
            Span::styled(format!(" {} ", index + 1), theme::faint()),
            Span::styled(format!("{text:<STEP_WIDTH$}"), text_style),
            widgets::keycap(key),
        ]));
    }
    lines.push(Line::default());
    lines.push(Line::from(vec![
        widgets::keycap("F1"),
        Span::styled(" lists every shortcut", theme::faint()),
    ]));

    let block_width = lines.iter().map(Line::width).max().unwrap_or(0) as u16;
    let width = block_width.min(inner.width.saturating_sub(2));
    let x = inner.x + (inner.width - width) / 2;
    let top = inner.y + inner.height.saturating_sub(lines.len() as u16) / 2;
    for (offset, line) in lines.iter().enumerate() {
        let y = top + offset as u16;
        if y >= inner.bottom() {
            break;
        }
        widgets::put_line(buffer, x, y, line, width);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plain(line: &Line<'_>) -> String {
        line.spans.iter().map(|span| span.content.as_ref()).collect()
    }

    #[test]
    fn json_tokens_cover_the_whole_line_and_distinguish_keys_from_values() {
        let text = r#"  "name": "broker-a", "port": 10911, "master": true, "tag": null"#;
        let line = structured_line(text);

        assert_eq!(plain(&line), text);
        let style_of = |token: &str| {
            line.spans
                .iter()
                .find(|span| span.content == token)
                .unwrap_or_else(|| panic!("missing token {token}"))
                .style
        };
        assert_eq!(style_of("\"name\""), style_of("\"port\""));
        assert_ne!(style_of("\"name\""), style_of("\"broker-a\""));
        assert_ne!(style_of("10911"), style_of("\"broker-a\""));
        assert_eq!(style_of("true"), style_of("null"));
    }

    #[test]
    fn debug_tokens_distinguish_fields_values_and_the_names_that_wrap_them() {
        let text = r#"    BrokerEntry { broker_addr: "127.0.0.1:10911", queue_id: 4, perm: ReadWrite, slave: None, at: Some(7) },"#;
        let line = structured_line(text);

        assert_eq!(plain(&line), text);
        let style_of = |token: &str| {
            line.spans
                .iter()
                .find(|span| span.content == token)
                .unwrap_or_else(|| panic!("missing token {token}"))
                .style
        };
        assert_eq!(style_of("broker_addr"), style_of("queue_id"));
        assert_ne!(style_of("broker_addr"), style_of("\"127.0.0.1:10911\""));
        assert_ne!(style_of("4"), style_of("\"127.0.0.1:10911\""));
        assert_eq!(
            style_of("BrokerEntry"),
            style_of("Some"),
            "names that only wrap a value recede"
        );
        assert_eq!(style_of("BrokerEntry"), style_of("{"));
        assert_ne!(
            style_of("ReadWrite"),
            style_of("BrokerEntry"),
            "a variant that is the value stays readable"
        );
        assert_ne!(style_of("None"), style_of("ReadWrite"));
    }

    #[test]
    fn structured_highlighting_survives_text_in_neither_notation() {
        for text in [
            "",
            "plain words, no json",
            "\"unterminated",
            "\"escape at end\\",
            "\"escaped \\\"quote\\\" inside\": 1",
            "中文 \"键\": \"值\\主\" 末尾",
            "-",
            "{[]}:,",
            "path::to::Type(_private, __x)",
            "trailing_",
        ] {
            assert_eq!(plain(&structured_line(text)), text, "{text:?}");
        }
        let long = "x".repeat(HIGHLIGHT_LIMIT + 1);
        assert_eq!(structured_line(&long).spans.len(), 1);
    }

    #[test]
    fn pan_slices_across_span_boundaries() {
        let line = Line::from(vec![Span::raw("abc"), Span::raw("defg"), Span::raw("hi")]);

        assert_eq!(plain(&pan(&line, 0, 4)), "abcd");
        assert_eq!(plain(&pan(&line, 2, 4)), "cdef");
        assert_eq!(plain(&pan(&line, 7, 10)), "hi");
        assert_eq!(plain(&pan(&line, 20, 10)), "");
    }

    #[test]
    fn summary_marks_are_colored_only_on_the_first_row_of_an_entry() {
        let first = marked("✓ topic-a", true, theme::SUCCESS);
        assert_eq!(first.spans.len(), 2);
        assert_eq!(first.spans[0].content, "✓");

        let continued = marked("rest of entry", false, theme::SUCCESS);
        assert_eq!(continued.spans.len(), 1);
        assert_eq!(marked("", true, theme::SUCCESS).spans.len(), 1);
    }

    #[test]
    fn body_area_reserves_the_header_row_of_a_grid() {
        let area = Rect::new(10, 5, 40, 12);

        assert_eq!(body_area(area, false), Rect::new(12, 6, 36, 10));
        assert_eq!(body_area(area, true), Rect::new(12, 7, 36, 9));
        assert_eq!(body_area(Rect::new(0, 0, 2, 2), true).height, 0);
        assert!(body_area(Rect::new(0, 0, 0, 0), true).is_empty());
    }
}
