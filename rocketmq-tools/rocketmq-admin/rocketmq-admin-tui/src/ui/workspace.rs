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

//! Command pane: what the selected command does, its parameter form, and the run button.

use std::borrow::Cow;

use ratatui::buffer::Buffer;
use ratatui::layout::Position;
use ratatui::layout::Rect;
use ratatui::style::Color;
use ratatui::style::Modifier;
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
use crate::commands::ArgKind;
use crate::commands::ArgSpec;
use crate::commands::RiskLevel;
use crate::state::ExecutionPhase;
use crate::state::FocusArea;
use crate::state::InputTarget;
use crate::state::Pane;
use crate::text::display_width;
use crate::text::truncate;

const LABEL_MIN_WIDTH: usize = 8;
const LABEL_MAX_WIDTH: usize = 24;
/// Ticks the title takes to brighten after another command is selected.
const SWAP_TICKS: u64 = 7;
/// Ticks rejected fields keep flashing.
const INVALID_TICKS: u64 = 26;
/// Ticks the run button stays lit after it was triggered.
const PRESS_TICKS: u64 = 9;

pub(super) fn paint(frame: &mut Frame, ctx: &Ctx<'_>, area: Rect, view: &mut ViewCache) {
    let state = ctx.state;
    let motion = state.motion();
    let command = state.selected_command();
    let focused = state.focus == FocusArea::Args;

    // A freshly selected command brightens into place instead of snapping in.
    let swap = if motion.command().current() == state.selected_command_index() {
        motion.transition(motion.command().changed_at(), SWAP_TICKS)
    } else {
        1.0
    };
    let title_color = theme::mix(theme::FAINT, if focused { theme::BRIGHT } else { theme::TEXT }, swap);
    let title = Line::from(vec![
        Span::styled(format!(" {} ", command.category.as_str()), theme::muted()),
        Span::styled(theme::GLYPH_PATH, theme::faint()),
        Span::styled(format!(" {} ", command.title), theme::bold(title_color)),
    ]);
    let risk = Line::from(Span::styled(
        format!(" {} ", theme::risk_badge(command.risk_level)),
        theme::bold(theme::risk_color(command.risk_level)),
    ))
    .right_aligned();
    let mut block = widgets::pane(ctx.pane_border(Pane::Command))
        .title_top(title)
        .title_top(risk)
        .title_bottom(Line::from(Span::styled(
            format!(
                " {} · returns {} ",
                state.form.command_id(),
                command.result_view_kind.as_str()
            ),
            theme::faint(),
        )));
    if !command.args.is_empty() {
        let position = format!(" {}/{} ", state.form.focused_arg() + 1, command.args.len());
        block = block.title_bottom(Line::from(Span::styled(position, theme::faint())).right_aligned());
    }
    let inner = block.inner(area);
    frame.render_widget(block, area);
    view.register(area, HitTarget::Command);
    if inner.is_empty() {
        return;
    }

    let buffer = frame.buffer_mut();
    let has_description = inner.height >= 3;
    let has_action = inner.height >= 2;
    if has_description {
        let width = inner.width.saturating_sub(2);
        widgets::put(
            buffer,
            inner.x + 1,
            inner.y,
            &truncate(command.description, usize::from(width)),
            theme::colored(theme::mix(theme::FAINT, theme::MUTED, swap)),
            width,
        );
    }

    let fields = Rect {
        y: inner.y + u16::from(has_description),
        height: inner.height - u16::from(has_description) - u16::from(has_action),
        ..inner
    };
    let cursor = paint_fields(buffer, ctx, fields, view);
    widgets::scrollbar(
        buffer,
        area.right() - 1,
        fields.y,
        fields.height,
        command.args.len(),
        usize::from(fields.height),
        view.args_top,
        if focused { theme::PRIMARY } else { theme::FAINT },
    );
    if has_action {
        paint_action(
            buffer,
            ctx,
            Rect {
                y: inner.bottom() - 1,
                height: 1,
                ..inner
            },
            view,
        );
    }

    if let Some(cursor) = cursor {
        frame.set_cursor_position(cursor);
    }
}

fn paint_fields(buffer: &mut Buffer, ctx: &Ctx<'_>, fields: Rect, view: &mut ViewCache) -> Option<Position> {
    let state = ctx.state;
    let command = state.selected_command();
    let rows = usize::from(fields.height);
    if fields.is_empty() {
        return None;
    }
    if command.args.is_empty() {
        widgets::put(
            buffer,
            fields.x + 1,
            fields.y,
            "No parameters. This command runs as it is.",
            theme::faint(),
            fields.width.saturating_sub(2),
        );
        return None;
    }

    view.args_top = widgets::scroll_into_view(view.args_top, state.form.focused_arg(), command.args.len(), rows, 1);
    let label_width = command
        .args
        .iter()
        .map(|arg| display_width(arg.label))
        .max()
        .unwrap_or(0)
        .clamp(LABEL_MIN_WIDTH, LABEL_MAX_WIDTH) as u16;

    let mut cursor = None;
    for (index, arg) in command.args.iter().enumerate().skip(view.args_top).take(rows) {
        let row = Rect {
            y: fields.y + (index - view.args_top) as u16,
            height: 1,
            ..fields
        };
        view.register(row, HitTarget::ArgRow(index));
        if let Some(position) = paint_field(buffer, ctx, row, index, arg, label_width, view) {
            cursor = Some(position);
        }
    }
    cursor
}

/// Draws one parameter row and returns the text cursor when the row is being edited.
fn paint_field(
    buffer: &mut Buffer,
    ctx: &Ctx<'_>,
    row: Rect,
    index: usize,
    arg: &ArgSpec,
    label_width: u16,
    view: &mut ViewCache,
) -> Option<Position> {
    let state = ctx.state;
    let motion = state.motion();
    let current = index == state.form.focused_arg();
    let active = current && state.focus == FocusArea::Args;
    let error = state.form.validation_errors().get(arg.name);

    let mut background = if active {
        theme::SURFACE
    } else if ctx.hovered(row) {
        theme::STRIPE
    } else {
        theme::BACKGROUND
    };
    if let (Some(_), Some(rejected_at)) = (error, motion.invalid_at()) {
        if motion.is_playing(rejected_at, INVALID_TICKS) {
            let flash = theme::mix(theme::BACKGROUND, theme::DANGER, 0.45);
            background = theme::mix(flash, background, motion.transition(rejected_at, INVALID_TICKS));
        }
    }
    if background != theme::BACKGROUND {
        effects::fill(buffer, row, background);
    }

    let pointer_style = if active { theme::accent() } else { theme::faint() };
    if current {
        widgets::put(buffer, row.x + 1, row.y, theme::GLYPH_POINTER, pointer_style, 1);
    }
    let label_style = if error.is_some() {
        theme::colored(theme::DANGER)
    } else if active {
        theme::bright()
    } else {
        theme::muted()
    };
    let label_x = row.x + 3;
    widgets::put(buffer, label_x, row.y, arg.label, label_style, label_width);
    if arg.required {
        widgets::put(
            buffer,
            label_x + label_width + 1,
            row.y,
            "*",
            theme::colored(theme::DANGER),
            1,
        );
    }

    let value_x = label_x + label_width + 4;
    let mut value_width = row.right().saturating_sub(value_x + 1);
    if let Some(error) = error {
        // The reason sits at the end of the row; the value keeps at least half of it.
        let reason = format!("{} {error}", theme::GLYPH_FAIL);
        let reason_width = (display_width(&reason) as u16).min(value_width / 2);
        if reason_width >= 6 {
            widgets::put(
                buffer,
                value_x + value_width - reason_width,
                row.y,
                &truncate(&reason, usize::from(reason_width)),
                theme::colored(theme::DANGER),
                reason_width,
            );
            value_width -= reason_width + 1;
        }
    }
    if value_width == 0 {
        return None;
    }

    let value = state.form.raw_value(arg.name).unwrap_or_default();
    if let Some(choices) = arg.kind.choices() {
        paint_choices(buffer, row, index, choices, value, value_x, value_width, active, view);
        return None;
    }

    let shown: Cow<'_, str> = if arg.is_secret() {
        Cow::Owned(theme::GLYPH_MASK.repeat(value.chars().count()))
    } else {
        Cow::Borrowed(value)
    };
    if active {
        effects::fill(
            buffer,
            Rect {
                x: value_x - 1,
                width: value_width + 1,
                ..row
            },
            theme::SURFACE_RAISED,
        );
        let (visible, offset) = widgets::field_window(
            &shown,
            state.input_cursor(InputTarget::Arg(index)),
            usize::from(value_width),
        );
        if shown.is_empty() {
            widgets::put(buffer, value_x, row.y, &placeholder(arg), theme::faint(), value_width);
        } else {
            widgets::put(
                buffer,
                value_x,
                row.y,
                visible,
                theme::colored(theme::BRIGHT),
                value_width,
            );
        }
        return Some(Position::new(value_x + offset as u16, row.y));
    }

    if shown.is_empty() {
        widgets::put(buffer, value_x, row.y, &placeholder(arg), theme::faint(), value_width);
    } else {
        widgets::put(
            buffer,
            value_x,
            row.y,
            &truncate(&shown, usize::from(value_width)),
            theme::text(),
            value_width,
        );
    }
    None
}

/// Draws the values of a choice argument as a row of segments with the chosen one lit.
fn paint_choices(
    buffer: &mut Buffer,
    row: Rect,
    arg: usize,
    choices: &[&str],
    value: &str,
    x: u16,
    width: u16,
    active: bool,
    view: &mut ViewCache,
) {
    let widths = choices
        .iter()
        .map(|choice| display_width(choice) as u16 + 2)
        .collect::<Vec<_>>();
    let chosen = choices.iter().position(|choice| *choice == value);

    // Skip leading segments until the chosen one fits.
    let mut first = 0;
    if let Some(chosen) = chosen {
        while first < chosen {
            let lead = if first > 0 { 2 } else { 0 };
            let span = widths[first..=chosen].iter().sum::<u16>() + (chosen - first) as u16 + lead;
            if span <= width {
                break;
            }
            first += 1;
        }
    }

    let limit = x + width;
    let mut cursor = x;
    if first > 0 {
        cursor = widgets::put(buffer, cursor, row.y, "‹ ", theme::faint(), limit - cursor);
    }
    for (index, choice) in choices.iter().enumerate().skip(first) {
        let segment = widths[index];
        if cursor + segment > limit {
            widgets::put(buffer, cursor, row.y, "›", theme::faint(), limit.saturating_sub(cursor));
            break;
        }
        let style = if Some(index) == chosen {
            Style::new()
                .fg(theme::BRIGHT)
                .bg(if active {
                    theme::SELECTION
                } else {
                    theme::SURFACE_RAISED
                })
                .add_modifier(Modifier::BOLD)
        } else {
            theme::faint()
        };
        widgets::put(buffer, cursor, row.y, &format!(" {choice} "), style, segment);
        view.register(
            Rect {
                x: cursor,
                width: segment,
                ..row
            },
            HitTarget::ArgChoice { arg, choice: index },
        );
        cursor += segment + 1;
    }
}

fn placeholder(arg: &ArgSpec) -> Cow<'static, str> {
    if arg.is_secret() {
        return Cow::Borrowed("hidden while typing");
    }
    match &arg.kind {
        ArgKind::String { placeholder } | ArgKind::OptionalString { placeholder } => {
            Cow::Owned(format!("e.g. {placeholder}"))
        }
        ArgKind::Number { min: Some(min), .. } => Cow::Owned(format!("number, at least {min}")),
        ArgKind::KeyValueMap => Cow::Borrowed("key=value; key=value"),
        ArgKind::TimestampMillis => Cow::Borrowed("milliseconds since the epoch"),
        ArgKind::Number { .. } | ArgKind::Bool { .. } | ArgKind::Enum { .. } => Cow::Borrowed(arg.placeholder()),
    }
}

/// Draws the bottom row: guidance for the focused field on the left, the run button on the right.
fn paint_action(buffer: &mut Buffer, ctx: &Ctx<'_>, row: Rect, view: &mut ViewCache) {
    let state = ctx.state;
    let motion = state.motion();
    let command = state.selected_command();
    let phase = state.execution.phase();

    let (label, foreground, mut background): (String, Color, Color) = if phase == ExecutionPhase::Running {
        (
            format!(" {} Cancel ", theme::GLYPH_STOP),
            theme::BACKGROUND,
            theme::WARNING,
        )
    } else {
        match command.risk_level {
            RiskLevel::Safe => (
                format!(" {} Run ", theme::GLYPH_RUN),
                theme::BRIGHT,
                theme::PRIMARY_DEEP,
            ),
            // The ellipsis announces the confirmation step that follows.
            RiskLevel::Mutating => (
                format!(" {} Run… ", theme::GLYPH_RUN),
                theme::BACKGROUND,
                theme::WARNING,
            ),
            RiskLevel::Dangerous => (format!(" {} Run… ", theme::GLYPH_RUN), theme::BACKGROUND, theme::DANGER),
        }
    };
    let button_width = display_width(&label) as u16;
    let button = Rect {
        x: row.right().saturating_sub(button_width + 1).max(row.x),
        width: button_width.min(row.width),
        ..row
    };
    if ctx.hovered(button) {
        background = theme::mix(background, theme::BRIGHT, 0.2);
    }
    let pressed = [ExecutionPhase::Running, ExecutionPhase::Confirming]
        .into_iter()
        .find_map(|phase| ctx.phase_age(phase))
        .filter(|age| motion.is_playing(ctx.tick - age, PRESS_TICKS));
    if let Some(age) = pressed {
        background = theme::mix(
            theme::BRIGHT,
            background,
            motion.transition(ctx.tick - age, PRESS_TICKS),
        );
    }
    widgets::put(
        buffer,
        button.x,
        button.y,
        &label,
        Style::new().fg(foreground).bg(background).add_modifier(Modifier::BOLD),
        button.width,
    );
    view.register(button, HitTarget::RunButton);

    let hint_x = row.x + 1;
    let hint_width = button.x.saturating_sub(hint_x + 2);
    widgets::put_line(buffer, hint_x, row.y, &hint(ctx), hint_width);
}

/// Returns the guidance shown next to the run button.
fn hint(ctx: &Ctx<'_>) -> Line<'static> {
    let state = ctx.state;
    let command = state.selected_command();
    let errors = state.form.validation_errors();
    let focused_arg = state.form.current_arg(command);

    if let Some((arg, error)) = focused_arg.and_then(|arg| errors.get(arg.name).map(|error| (arg, error))) {
        return Line::from(Span::styled(
            format!("{} {}: {error}", theme::GLYPH_FAIL, arg.label),
            theme::colored(theme::DANGER),
        ));
    }
    if state.form.has_errors() {
        let noun = if errors.len() == 1 { "parameter" } else { "parameters" };
        return Line::from(Span::styled(
            format!("{} {} invalid {noun}", theme::GLYPH_FAIL, errors.len()),
            theme::colored(theme::DANGER),
        ));
    }
    if state.focus != FocusArea::Args {
        return Line::from(vec![
            widgets::keycap("Enter"),
            Span::styled(" opens the form   ", theme::faint()),
            widgets::keycap("Ctrl+R"),
            Span::styled(" runs", theme::faint()),
        ]);
    }
    match focused_arg {
        Some(arg) => Line::from(Span::styled(arg.help, theme::muted())),
        None => Line::from(vec![
            widgets::keycap("Enter"),
            Span::styled(" runs the command", theme::faint()),
        ]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::command_catalog;

    #[test]
    fn placeholders_describe_the_expected_input() {
        let catalog = command_catalog();
        let arg = |command: &str, name: &str| {
            catalog
                .iter()
                .find(|spec| spec.id == command)
                .and_then(|spec| spec.args.iter().find(|arg| arg.name == name))
                .cloned()
                .unwrap()
        };

        assert_eq!(placeholder(&arg("topic.route", "topic")), "e.g. TopicA");
        assert_eq!(
            placeholder(&arg("topic.update", "read_queue_nums")),
            "number, at least 1"
        );
        assert_eq!(
            placeholder(&arg("broker.config.update_apply", "entries")),
            "key=value; key=value"
        );
        assert_eq!(
            placeholder(&arg("offset.reset_by_time", "timestamp")),
            "milliseconds since the epoch"
        );
        assert_eq!(placeholder(&arg("auth.user.create", "password")), "hidden while typing");
    }
}
