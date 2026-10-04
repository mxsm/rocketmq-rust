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

//! Layers above the panes: help, confirmation, row details, and notifications.

use ratatui::buffer::Buffer;
use ratatui::layout::Position;
use ratatui::layout::Rect;
use ratatui::style::Color;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::text::Span;
use ratatui::widgets::Clear;
use ratatui::Frame;

use super::effects;
use super::theme;
use super::widgets;
use super::Ctx;
use super::HitTarget;
use super::ViewCache;
use super::FOOTER_HEIGHT;
use super::MIN_HEIGHT;
use super::MIN_WIDTH;
use crate::commands::RiskLevel;
use crate::motion::progress;
use crate::state::CommandExecutionState;
use crate::state::InputTarget;
use crate::state::Overlay;
use crate::state::Toast;
use crate::state::ToastLevel;
use crate::text::char_width;
use crate::text::display_width;
use crate::text::truncate;
use crate::text::wrap;

/// Ticks an overlay takes to grow to full size.
const OPEN_TICKS: u64 = 6;
/// How far the panes behind an overlay are pushed toward the background.
const BACKDROP: f32 = 0.6;
const HELP_COLUMN_WIDTH: u16 = 46;
const HELP_KEY_WIDTH: usize = 16;
const CONFIRM_WIDTH: u16 = 68;
/// Parameters listed in the confirmation before the rest is summarized.
const CONFIRM_PARAMETERS: usize = 8;
const DETAIL_MAX_WIDTH: u16 = 104;
const TOAST_ENTER_TICKS: u64 = 8;
const TOAST_EXIT_TICKS: u64 = 12;

type HelpSection = (&'static str, &'static [(&'static str, &'static str)]);

const RISK_SECTION: &str = "Risk levels";

const HELP: [HelpSection; 7] = [
    (
        "Move",
        &[
            ("Tab  Shift+Tab", "next or previous pane"),
            ("↑ ↓  j k", "move in the focused pane"),
            ("PgUp  PgDn", "move a page"),
            ("Home End  g G", "first or last"),
            ("/  Ctrl+F", "search commands"),
            ("n  p  r", "NameServer, form, result"),
            ("Esc", "back, clear, or cancel"),
        ],
    ),
    (
        "Commands",
        &[
            ("Enter", "open a command, fold a group"),
            ("← →  h l", "fold or unfold a group"),
            ("-  +", "fold or unfold every group"),
        ],
    ),
    (
        "Parameters",
        &[
            ("↑ ↓", "previous or next field"),
            ("← →  Space", "switch a choice"),
            ("Ctrl+U  Ctrl+W", "clear field, delete word"),
            ("Ctrl+D", "restore the defaults"),
            ("Enter", "run the command"),
        ],
    ),
    (
        "Run",
        &[
            ("Ctrl+R  F5", "run from anywhere"),
            ("Esc  Ctrl+C", "cancel a running command"),
            ("Ctrl+L", "clear the result"),
        ],
    ),
    (
        "Result",
        &[
            ("Enter", "show every column of a row"),
            ("← →  h l", "scroll columns or pan"),
            ("w", "wrap long lines"),
            ("z", "zoom the result pane"),
        ],
    ),
    (
        "Application",
        &[
            ("F1  ?", "this help"),
            ("F2", "mouse capture on or off"),
            ("F3", "animations on or off"),
            ("q  Ctrl+Q", "quit"),
        ],
    ),
    (
        RISK_SECTION,
        &[
            ("○ safe", "runs immediately"),
            ("◆ mutating", "asks you to type confirm"),
            ("▲ dangerous", "asks you to type the target"),
        ],
    ),
];

pub(super) fn paint(frame: &mut Frame, ctx: &Ctx<'_>, area: Rect, view: &mut ViewCache) {
    let state = ctx.state;
    let overlay = state.overlay();
    view.overlay_rows = 0;
    view.overlay_max_scroll = 0;
    if overlay == Overlay::None {
        return;
    }

    let motion = state.motion();
    let tracked = motion.overlay();
    let opening = if tracked.current() == overlay {
        motion.transition(tracked.changed_at(), OPEN_TICKS)
    } else {
        1.0
    };
    // The key bar stays lit: it lists the keys of the overlay itself.
    let panes = Rect {
        height: area.height.saturating_sub(FOOTER_HEIGHT),
        ..area
    };
    effects::dim(frame.buffer_mut(), panes, BACKDROP * opening);
    view.register(area, HitTarget::Backdrop);
    match overlay {
        Overlay::None => {}
        Overlay::Help => paint_help(frame, ctx, area, opening, view),
        Overlay::Confirm => paint_confirm(frame, ctx, area, opening, view),
        Overlay::Detail => paint_detail(frame, ctx, area, opening, view),
    }
}

/// Returns a centered rectangle that grows to `width` by `height` as `opening` reaches one.
fn modal(area: Rect, width: u16, height: u16, opening: f32) -> Rect {
    let scale = 0.8 + 0.2 * opening.clamp(0.0, 1.0);
    let fit = |wanted: u16, available: u16, margin: u16| {
        let full = wanted.min(available.saturating_sub(margin)).max(1);
        ((f32::from(full) * scale).round() as u16).clamp(1, full)
    };
    let width = fit(width, area.width, 4).min(area.width);
    let height = fit(height, area.height, 2).min(area.height);
    Rect {
        x: area.x + (area.width - width) / 2,
        y: area.y + (area.height - height) / 2,
        width,
        height,
    }
}

/// Clears `rect`, frames it, and returns the padded content area.
fn open(
    frame: &mut Frame,
    rect: Rect,
    border: Color,
    title: Line<'static>,
    footer: Line<'static>,
    view: &mut ViewCache,
) -> Rect {
    frame.render_widget(Clear, rect);
    let block = widgets::pane(border)
        .title_top(title)
        .title_bottom(footer.right_aligned());
    let inner = block.inner(rect);
    frame.render_widget(block, rect);
    view.register(rect, HitTarget::Overlay);
    Rect {
        x: inner.x.saturating_add(1),
        width: inner.width.saturating_sub(2),
        ..inner
    }
}

fn close_hint() -> Line<'static> {
    Line::from(vec![
        Span::raw(" "),
        widgets::keycap("Esc"),
        Span::styled(" close ", theme::faint()),
    ])
}

fn help_column(sections: &[HelpSection]) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    for (title, keys) in sections {
        if !lines.is_empty() {
            lines.push(Line::default());
        }
        lines.push(Line::from(Span::styled(*title, theme::accent())));
        for (index, (key, action)) in keys.iter().enumerate() {
            let key_style = match (*title == RISK_SECTION, index) {
                (true, 0) => theme::bold(theme::risk_color(RiskLevel::Safe)),
                (true, 1) => theme::bold(theme::risk_color(RiskLevel::Mutating)),
                (true, _) => theme::bold(theme::risk_color(RiskLevel::Dangerous)),
                (false, _) => theme::colored(theme::BRIGHT),
            };
            lines.push(Line::from(vec![
                Span::styled(format!("{key:<HELP_KEY_WIDTH$}"), key_style),
                Span::styled(*action, theme::muted()),
            ]));
        }
    }
    lines
}

fn paint_help(frame: &mut Frame, ctx: &Ctx<'_>, area: Rect, opening: f32, view: &mut ViewCache) {
    let two_columns = area.width >= 2 * HELP_COLUMN_WIDTH + 10;
    let columns = if two_columns {
        vec![help_column(&HELP[..3]), help_column(&HELP[3..])]
    } else {
        vec![help_column(&HELP)]
    };
    let total = columns.iter().map(Vec::len).max().unwrap_or(0);

    let width = HELP_COLUMN_WIDTH * columns.len() as u16 + 4;
    let rect = modal(area, width, total as u16 + 2, opening);
    let content = open(
        frame,
        rect,
        theme::PRIMARY,
        Line::from(widgets::pane_title("Keyboard and mouse", true)),
        close_hint(),
        view,
    );

    let rows = usize::from(content.height);
    let max_scroll = total.saturating_sub(rows);
    let scroll = ctx.state.overlay_scroll.min(max_scroll);
    view.overlay_rows = rows;
    view.overlay_max_scroll = max_scroll;

    let buffer = frame.buffer_mut();
    let column_width = content.width / columns.len() as u16;
    for offset in 0..rows {
        let row = scroll + offset;
        let y = content.y + offset as u16;
        for (index, column) in columns.iter().enumerate() {
            if let Some(line) = column.get(row) {
                let x = content.x + column_width * index as u16;
                widgets::put_line(buffer, x, y, line, column_width.saturating_sub(1));
            }
        }
    }
    widgets::scrollbar(
        buffer,
        rect.right() - 1,
        content.y,
        content.height,
        total,
        rows,
        scroll,
        theme::PRIMARY,
    );
}

fn paint_confirm(frame: &mut Frame, ctx: &Ctx<'_>, area: Rect, opening: f32, view: &mut ViewCache) {
    let state = ctx.state;
    let motion = state.motion();
    let CommandExecutionState::Confirming {
        command_id, expected, ..
    } = &state.execution
    else {
        return;
    };
    let command = state
        .commands()
        .iter()
        .find(|command| command.id == command_id)
        .unwrap_or_else(|| state.selected_command());
    let color = theme::risk_color(command.risk_level);

    let parameters = command
        .args
        .iter()
        .filter_map(|arg| {
            let value = state.form.raw_value(arg.name)?.trim();
            (!value.is_empty()).then(|| {
                let shown = if arg.is_secret() {
                    theme::GLYPH_MASK.repeat(value.chars().count())
                } else {
                    value.to_string()
                };
                (arg.label, shown)
            })
        })
        .collect::<Vec<_>>();
    let listed = parameters.len().min(CONFIRM_PARAMETERS);
    let overflow = parameters.len() - listed;
    // Title, id, gap, parameters and their gap, prompt, input, verdict, gap, keys.
    let height = 8 + listed + usize::from(overflow > 0) + usize::from(listed > 0);

    let heading = if command.risk_level == RiskLevel::Dangerous {
        "Confirm dangerous command"
    } else {
        "Confirm command"
    };
    let rect = modal(area, CONFIRM_WIDTH, height as u16 + 2, opening);
    let content = open(
        frame,
        rect,
        color,
        Line::from(vec![
            Span::raw(" "),
            Span::styled(theme::GLYPH_ATTENTION, theme::bold(color)),
            Span::styled(format!(" {heading} "), theme::bright()),
        ]),
        Line::default(),
        view,
    );
    // A highlight circles the frame so the dialog cannot be mistaken for ordinary output.
    effects::comet(frame.buffer_mut(), rect, ctx.tick, theme::BRIGHT, motion.ambient());
    if content.is_empty() {
        return;
    }

    let buffer = frame.buffer_mut();
    let width = content.width;
    let bottom = content.bottom();
    let mut y = content.y;
    // Writes the next row and returns its position; rows below the dialog are skipped.
    let mut emit = |buffer: &mut Buffer, line: Line<'_>| {
        if y < bottom {
            widgets::put_line(buffer, content.x, y, &line, width);
        }
        y += 1;
        y - 1
    };

    let badge = Line::from(Span::styled(theme::risk_badge(command.risk_level), theme::bold(color)));
    let title_row = emit(buffer, Line::from(Span::styled(command.title, theme::bright())));
    if title_row < bottom {
        widgets::put_line_right(buffer, content.x, content.right(), title_row, &badge);
    }
    emit(buffer, Line::from(Span::styled(command.id, theme::faint())));
    emit(buffer, Line::default());

    let label_width = parameters
        .iter()
        .take(listed)
        .map(|(label, _)| display_width(label))
        .max()
        .unwrap_or(0);
    for (label, value) in parameters.iter().take(listed) {
        let value_width = usize::from(width).saturating_sub(label_width + 2);
        emit(
            buffer,
            Line::from(vec![
                Span::styled(format!("{label:<label_width$}  "), theme::muted()),
                Span::styled(truncate(value, value_width).into_owned(), theme::text()),
            ]),
        );
    }
    if overflow > 0 {
        emit(
            buffer,
            Line::from(Span::styled(format!("and {overflow} more"), theme::faint())),
        );
    }
    if listed > 0 {
        emit(buffer, Line::default());
    }

    emit(buffer, prompt_line(ctx, expected, color));

    let input_row = emit(buffer, Line::default());
    let typed = state.confirm_input.as_str();
    let matches = typed.trim() == expected;
    let mut cursor = None;
    if input_row < bottom {
        let field = Rect {
            x: content.x,
            y: input_row,
            width,
            height: 1,
        };
        let mut background = theme::SURFACE_RAISED;
        if let Some(rejected_at) = motion.invalid_at() {
            if motion.is_playing(rejected_at, 22) {
                let flash = theme::mix(theme::BACKGROUND, theme::DANGER, 0.5);
                background = theme::mix(flash, background, motion.transition(rejected_at, 22));
            }
        }
        effects::fill(buffer, field, background);
        cursor = Some(paint_confirm_input(
            buffer,
            field,
            typed,
            expected,
            state.input_cursor(InputTarget::Confirm),
        ));
    }

    let verdict = if matches {
        Line::from(Span::styled(
            format!("{} matches", theme::GLYPH_OK),
            theme::bold(theme::SUCCESS),
        ))
    } else if let Some(error) = &state.last_error {
        Line::from(Span::styled(
            truncate(&format!("{} {error}", theme::GLYPH_FAIL), usize::from(width)).into_owned(),
            theme::colored(theme::DANGER),
        ))
    } else {
        Line::from(Span::styled(
            format!("{}/{}", typed.chars().count(), expected.chars().count()),
            theme::faint(),
        ))
    };
    emit(buffer, verdict);
    emit(buffer, Line::default());
    emit(
        buffer,
        Line::from(vec![
            widgets::keycap("Enter"),
            Span::styled(" confirm   ", theme::muted()),
            widgets::keycap("Esc"),
            Span::styled(" cancel", theme::muted()),
        ]),
    );

    if let Some(cursor) = cursor {
        frame.set_cursor_position(cursor);
    }
}

/// Builds the instruction above the input, with the text to type emphasized.
fn prompt_line(ctx: &Ctx<'_>, expected: &str, color: Color) -> Line<'static> {
    let prompt = ctx
        .state
        .confirmation_prompt()
        .unwrap_or_else(|| format!("Type '{expected}' to execute"));
    let quoted = format!("'{expected}'");
    match prompt.split_once(&quoted) {
        Some((before, after)) => Line::from(vec![
            Span::styled(before.to_string(), theme::muted()),
            Span::styled(expected.to_string(), theme::bold(color)),
            Span::styled(after.to_string(), theme::muted()),
        ]),
        None => Line::from(Span::styled(prompt, theme::muted())),
    }
}

/// Draws the typed confirmation, green while it follows the expected text and red after
/// the first character that does not, and returns the cursor position.
fn paint_confirm_input(buffer: &mut Buffer, field: Rect, typed: &str, expected: &str, cursor: usize) -> Position {
    let text_x = field.x + 1;
    let text_width = usize::from(field.width.saturating_sub(2));
    let characters = typed.chars().collect::<Vec<_>>();
    let agreeing = typed
        .chars()
        .zip(expected.chars())
        .take_while(|(typed, expected)| typed == expected)
        .count();

    // Scroll by characters so the cursor stays inside the field.
    let first = cursor.saturating_sub(text_width.saturating_sub(1));
    let mut x = text_x;
    let mut cursor_x = text_x;
    let limit = text_x + text_width as u16;
    for (index, character) in characters.iter().enumerate().skip(first) {
        if index == cursor {
            cursor_x = x;
        }
        let cells = char_width(*character) as u16;
        if x + cells > limit {
            break;
        }
        let color = if index < agreeing {
            theme::SUCCESS
        } else {
            theme::DANGER
        };
        let mut encoded = [0_u8; 4];
        x = widgets::put(
            buffer,
            x,
            field.y,
            character.encode_utf8(&mut encoded),
            theme::bold(color),
            cells,
        );
    }
    if cursor >= characters.len() {
        cursor_x = x;
    }
    Position::new(cursor_x.min(limit.saturating_sub(1)), field.y)
}

fn paint_detail(frame: &mut Frame, ctx: &Ctx<'_>, area: Rect, opening: f32, view: &mut ViewCache) {
    let state = ctx.state;
    let Some(detail) = state.detail() else {
        return;
    };
    let width = DETAIL_MAX_WIDTH.min(area.width.saturating_sub(6));
    let text_width = usize::from(width.saturating_sub(4));

    // Each field is its name, its wrapped value, and a blank row.
    let mut rows: Vec<(Style, &str)> = Vec::new();
    for (name, value) in &detail.fields {
        rows.push((theme::bold(theme::CYAN), name.as_str()));
        if value.is_empty() {
            rows.push((theme::faint(), "(empty)"));
        } else {
            for line in value.lines() {
                wrap(line, text_width, |start, end| {
                    rows.push((theme::text(), &line[start..end]))
                });
            }
        }
        rows.push((theme::text(), ""));
    }
    rows.pop();

    let rect = modal(
        area,
        width,
        rows.len().min(usize::from(u16::MAX) - 2) as u16 + 2,
        opening,
    );
    let content = open(
        frame,
        rect,
        theme::PRIMARY,
        Line::from(widgets::pane_title(&detail.title, true)),
        close_hint(),
        view,
    );

    let visible = usize::from(content.height);
    let max_scroll = rows.len().saturating_sub(visible);
    let scroll = state.overlay_scroll.min(max_scroll);
    view.overlay_rows = visible;
    view.overlay_max_scroll = max_scroll;

    let buffer = frame.buffer_mut();
    for (offset, (style, text)) in rows.iter().skip(scroll).take(visible).enumerate() {
        widgets::put(
            buffer,
            content.x,
            content.y + offset as u16,
            text,
            *style,
            content.width,
        );
    }
    widgets::scrollbar(
        buffer,
        rect.right() - 1,
        content.y,
        content.height,
        rows.len(),
        visible,
        scroll,
        theme::PRIMARY,
    );
}

/// Draws the current notification sliding in from the right edge above the key bar.
pub(super) fn paint_toast(frame: &mut Frame, ctx: &Ctx<'_>, area: Rect) {
    let Some(toast) = ctx.state.toast() else {
        return;
    };
    if area.width < 16 || area.height < 5 {
        return;
    }
    let motion = ctx.state.motion();
    let (glyph, color) = match toast.level {
        ToastLevel::Info => (theme::GLYPH_DOT, theme::CYAN),
        ToastLevel::Success => (theme::GLYPH_OK, theme::SUCCESS),
        ToastLevel::Warning => (theme::GLYPH_ATTENTION, theme::WARNING),
        ToastLevel::Error => (theme::GLYPH_FAIL, theme::DANGER),
    };

    let text_width = (display_width(&toast.message) as u16).min(area.width - 10);
    let width = text_width + 6;
    let entered = motion.transition(toast.created_at, TOAST_ENTER_TICKS);
    let resting_x = area.right() - width - 1;
    let x = resting_x + ((1.0 - entered) * f32::from(width + 1)).round() as u16;
    let rect = Rect {
        x,
        y: area.bottom() - 4,
        width: width.min(area.right().saturating_sub(x)),
        height: 3,
    };
    if rect.width < 3 {
        return;
    }

    frame.render_widget(Clear, rect);
    frame.render_widget(widgets::pane(color), rect);
    let buffer = frame.buffer_mut();
    let inner = widgets::inset(rect);
    let message = Line::from(vec![
        Span::raw(" "),
        Span::styled(glyph, theme::bold(color)),
        Span::raw(" "),
        Span::styled(
            truncate(&toast.message, usize::from(text_width)).into_owned(),
            theme::text(),
        ),
    ]);
    widgets::put_line(buffer, inner.x, inner.y, &message, inner.width);

    if motion.enabled() {
        let leaving = progress(
            ctx.tick,
            toast.created_at + Toast::LIFETIME_TICKS - TOAST_EXIT_TICKS,
            TOAST_EXIT_TICKS,
        );
        effects::fade(buffer, rect, 1.0 - leaving);
    }
}

/// Explains why nothing is drawn when the terminal cannot fit the interface.
pub(super) fn paint_too_small(frame: &mut Frame, area: Rect) {
    let lines = [
        Line::from(Span::styled("Terminal too small", theme::bright())),
        Line::from(Span::styled(
            format!("needs {MIN_WIDTH}x{MIN_HEIGHT}, has {}x{}", area.width, area.height),
            theme::muted(),
        )),
    ];
    let buffer = frame.buffer_mut();
    let top = area.y + area.height.saturating_sub(lines.len() as u16) / 2;
    for (offset, line) in lines.iter().enumerate() {
        let y = top + offset as u16;
        if y < area.bottom() {
            widgets::put_line_centered(buffer, area, y, line);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modal_grows_to_its_full_size_and_stays_centered() {
        let area = Rect::new(0, 0, 100, 40);

        let closed = modal(area, 60, 20, 0.0);
        let opened = modal(area, 60, 20, 1.0);

        assert_eq!(opened, Rect::new(20, 10, 60, 20));
        assert!(closed.width < opened.width && closed.height < opened.height);
        assert_eq!(closed.x + closed.width / 2, 50);
    }

    #[test]
    fn modal_never_exceeds_the_screen() {
        let area = Rect::new(0, 0, 50, 12);

        let rect = modal(area, 200, 200, 1.0);
        assert_eq!((rect.width, rect.height), (46, 10));

        let tiny = modal(Rect::new(0, 0, 3, 1), 200, 200, 1.0);
        assert!(tiny.width <= 3 && tiny.height <= 1);
        assert!(tiny.width >= 1 && tiny.height >= 1);
    }

    #[test]
    fn every_help_section_lists_keys() {
        assert!(HELP.iter().all(|(title, keys)| !title.is_empty() && !keys.is_empty()));
        assert!(HELP
            .iter()
            .flat_map(|(_, keys)| keys.iter())
            .all(|(key, _)| display_width(key) < HELP_KEY_WIDTH));
        let widest = HELP
            .iter()
            .flat_map(|(_, keys)| keys.iter())
            .map(|(_, action)| HELP_KEY_WIDTH + display_width(action))
            .max()
            .unwrap();
        assert!(widest < usize::from(HELP_COLUMN_WIDTH));
    }

    #[test]
    fn confirmation_input_colors_follow_the_expected_text() {
        let mut buffer = Buffer::empty(Rect::new(0, 0, 20, 1));
        let field = Rect::new(0, 0, 20, 1);

        let cursor = paint_confirm_input(&mut buffer, field, "TopXc", "TopicA", 5);

        assert_eq!(cursor, Position::new(6, 0));
        assert_eq!(buffer[(1, 0)].symbol(), "T");
        assert_eq!(buffer[(3, 0)].fg, theme::SUCCESS);
        assert_eq!(buffer[(4, 0)].fg, theme::DANGER);
        assert_eq!(buffer[(5, 0)].fg, theme::DANGER, "everything after a mismatch is wrong");
    }

    #[test]
    fn confirmation_input_scrolls_and_keeps_the_cursor_inside_the_field() {
        let mut buffer = Buffer::empty(Rect::new(0, 0, 8, 1));
        let field = Rect::new(0, 0, 8, 1);
        let typed = "abcdefghijkl";

        let end = paint_confirm_input(&mut buffer, field, typed, typed, 12);
        assert_eq!(end, Position::new(6, 0));
        assert_eq!(buffer[(5, 0)].symbol(), "l");

        let start = paint_confirm_input(&mut buffer, field, typed, typed, 0);
        assert_eq!(start, Position::new(1, 0));
    }
}
