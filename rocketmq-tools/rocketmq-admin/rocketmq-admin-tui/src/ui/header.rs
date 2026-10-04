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

//! Title bar: brand, NameServer field, execution status, and the activity rule.

use ratatui::buffer::Buffer;
use ratatui::layout::Position;
use ratatui::layout::Rect;
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
use crate::state::CommandExecutionState;
use crate::state::ExecutionPhase;
use crate::state::FocusArea;
use crate::state::InputTarget;
use crate::text::display_width;
use crate::text::truncate;

const BRAND: &str = "RocketMQ";
const NAMESRV_LABEL: &str = "NameServer";
const FIELD_MIN_WIDTH: u16 = 24;
const FIELD_MAX_WIDTH: u16 = 46;
/// Ticks between two highlights sweeping across the brand.
const SHIMMER_PERIOD: u64 = 160;
const SHIMMER_TICKS: u64 = 22;
/// Ticks the activity rule takes to settle after a command finished.
const FLASH_TICKS: u64 = 36;

pub(super) fn paint(frame: &mut Frame, ctx: &Ctx<'_>, area: Rect, view: &mut ViewCache) {
    if area.is_empty() {
        return;
    }
    let bar = Rect { height: 1, ..area };
    let buffer = frame.buffer_mut();

    let mut x = paint_brand(buffer, ctx, bar);
    x = x.saturating_add(3);
    let (field_end, cursor) = paint_namesrv(buffer, ctx, bar, x, view);

    let status = status_line(ctx);
    let left = field_end.saturating_add(2);
    let right = bar.right().saturating_sub(1);
    if left < right {
        widgets::put_line_right(buffer, left, right, bar.y, &status);
    }

    if area.height > 1 {
        paint_rule(
            buffer,
            ctx,
            Rect {
                y: area.y + 1,
                height: 1,
                ..area
            },
        );
    }
    if let Some(cursor) = cursor {
        frame.set_cursor_position(cursor);
    }
}

/// Draws the brand in the product gradient, with an occasional highlight sweeping across.
fn paint_brand(buffer: &mut Buffer, ctx: &Ctx<'_>, bar: Rect) -> u16 {
    let limit = bar.right();
    let mut x = bar.x.saturating_add(1);
    let pulse = crate::motion::wave(ctx.tick, 90) * ctx.state.motion().ambient();
    let mark = theme::mix(theme::PRIMARY, theme::CYAN, pulse);
    x = widgets::put(
        buffer,
        x,
        bar.y,
        theme::GLYPH_BRAND,
        theme::bold(mark),
        limit.saturating_sub(x),
    );
    x = x.saturating_add(1);

    let cycle = ctx.tick % SHIMMER_PERIOD;
    let shimmering = ctx.state.motion().ambient() > 0.0 && cycle < SHIMMER_TICKS;
    let letters = BRAND.chars().count();
    for (index, letter) in BRAND.chars().enumerate() {
        let mut color = theme::gradient(index as f32 / (letters - 1) as f32);
        if shimmering {
            let center = cycle as f32 / SHIMMER_TICKS as f32 * (letters as f32 + 4.0) - 2.0;
            let strength = (1.0 - (index as f32 - center).abs() / 2.0).max(0.0);
            color = theme::mix(color, theme::BRIGHT, strength);
        }
        let mut encoded = [0_u8; 4];
        x = widgets::put(
            buffer,
            x,
            bar.y,
            letter.encode_utf8(&mut encoded),
            theme::bold(color),
            limit.saturating_sub(x),
        );
    }
    widgets::put(buffer, x, bar.y, " Admin", theme::muted(), limit.saturating_sub(x))
}

/// Draws the NameServer field and returns its right edge and the text cursor, if it has focus.
fn paint_namesrv(
    buffer: &mut Buffer,
    ctx: &Ctx<'_>,
    bar: Rect,
    start: u16,
    view: &mut ViewCache,
) -> (u16, Option<Position>) {
    let state = ctx.state;
    let limit = bar.right().saturating_sub(1);
    let label_end = widgets::put(
        buffer,
        start,
        bar.y,
        NAMESRV_LABEL,
        theme::muted(),
        limit.saturating_sub(start),
    );
    let field_x = label_end.saturating_add(1);
    let value = state.namesrv_addr.as_str();
    let wanted = u16::try_from(display_width(value) + 5)
        .unwrap_or(u16::MAX)
        .clamp(FIELD_MIN_WIDTH, FIELD_MAX_WIDTH);
    let field = Rect {
        x: field_x,
        y: bar.y,
        width: wanted.min(limit.saturating_sub(field_x)),
        height: 1,
    };
    if field.width < 6 {
        return (label_end, None);
    }

    let focused = state.focus == FocusArea::Namesrv;
    let background = if focused || ctx.hovered(field) {
        theme::SURFACE_RAISED
    } else {
        theme::SURFACE
    };
    effects::fill(buffer, field, background);
    view.register(
        Rect {
            x: start,
            width: field.right() - start,
            ..field
        },
        HitTarget::Namesrv,
    );

    let configured = !value.trim().is_empty();
    let (dot, dot_color) = if configured {
        (theme::GLYPH_DOT, theme::CYAN)
    } else {
        (theme::GLYPH_RING, theme::WARNING)
    };
    widgets::put(buffer, field.x + 1, field.y, dot, theme::colored(dot_color), 1);

    let text_x = field.x + 3;
    let text_width = field.width - 4;
    let mut cursor = None;
    if focused {
        let (visible, offset) =
            widgets::field_window(value, state.input_cursor(InputTarget::Namesrv), usize::from(text_width));
        if value.is_empty() {
            widgets::put(
                buffer,
                text_x,
                field.y,
                "host:port;host:port",
                theme::faint(),
                text_width,
            );
        } else {
            widgets::put(
                buffer,
                text_x,
                field.y,
                visible,
                theme::colored(theme::BRIGHT),
                text_width,
            );
        }
        cursor = Some(Position::new(text_x + offset as u16, field.y));
    } else if configured {
        widgets::put(
            buffer,
            text_x,
            field.y,
            &truncate(value, usize::from(text_width)),
            theme::text(),
            text_width,
        );
    } else {
        widgets::put(
            buffer,
            text_x,
            field.y,
            "not set · press n",
            theme::colored(theme::WARNING),
            text_width,
        );
    }
    (field.right(), cursor)
}

/// Builds the right-aligned execution status.
fn status_line(ctx: &Ctx<'_>) -> Line<'static> {
    let state = ctx.state;
    let mut spans = Vec::new();
    if !state.mouse_capture() {
        spans.push(Span::styled("mouse off", theme::faint()));
        spans.push(Span::raw("   "));
    }
    if !state.motion().enabled() {
        spans.push(Span::styled("motion off", theme::faint()));
        spans.push(Span::raw("   "));
    }

    let (glyph, color, timed) = match &state.execution {
        CommandExecutionState::Idle => {
            spans.push(Span::styled("ready", theme::faint()));
            return Line::from(spans);
        }
        CommandExecutionState::Confirming { .. } => (theme::GLYPH_ATTENTION, theme::WARNING, false),
        CommandExecutionState::Running { .. } => (theme::spinner(ctx.tick), theme::CYAN, true),
        CommandExecutionState::Succeeded { .. } => (theme::GLYPH_OK, theme::SUCCESS, true),
        CommandExecutionState::Failed { .. } => (theme::GLYPH_FAIL, theme::DANGER, true),
        CommandExecutionState::Cancelled { .. } => (theme::GLYPH_STOP, theme::WARNING, false),
    };
    spans.push(Span::styled(glyph, theme::bold(color)));
    spans.push(Span::raw(" "));
    spans.push(Span::styled(state.execution.label(), theme::text()));
    if let Some(duration) = state.run_duration().filter(|_| timed) {
        spans.push(Span::styled(
            format!("  {}", widgets::format_duration(duration)),
            theme::muted(),
        ));
    }
    Line::from(spans)
}

/// Draws the rule under the title bar, which doubles as the activity indicator.
///
/// It is a quiet gradient at rest, carries a travelling highlight while a command
/// runs, and flashes in the outcome color when the command ends.
fn paint_rule(buffer: &mut Buffer, ctx: &Ctx<'_>, rule: Rect) {
    const BAND: f32 = 9.0;
    let motion = ctx.state.motion();
    let width = usize::from(rule.width);
    let running = ctx.phase_age(ExecutionPhase::Running);
    let flash = [
        (ExecutionPhase::Succeeded, theme::SUCCESS),
        (ExecutionPhase::Failed, theme::DANGER),
        (ExecutionPhase::Cancelled, theme::WARNING),
    ]
    .into_iter()
    .find_map(|(phase, color)| {
        let age = ctx.phase_age(phase)?;
        motion
            .is_playing(ctx.tick - age, FLASH_TICKS)
            .then(|| (color, 1.0 - motion.transition(ctx.tick - age, FLASH_TICKS)))
    });
    let revealed = motion.transition(0, 18) * width as f32;

    for index in 0..width {
        let position = index as f32 / width.saturating_sub(1).max(1) as f32;
        let mut color = theme::mix(theme::BACKGROUND, theme::gradient(position), 0.4);
        let mut symbol = theme::RULE_THIN;
        if let Some(age) = running {
            symbol = theme::RULE_HEAVY;
            if motion.enabled() {
                let cycle = width as f32 + 2.0 * BAND;
                let head = (age as f32 * 1.6) % cycle - BAND;
                let strength = (1.0 - (index as f32 - head).abs() / BAND).max(0.0);
                color = theme::mix(
                    color,
                    theme::mix(theme::gradient(position), theme::BRIGHT, 0.35),
                    strength,
                );
            } else {
                color = theme::gradient(position);
            }
        }
        if let Some((flash_color, strength)) = flash {
            symbol = theme::RULE_HEAVY;
            color = theme::mix(color, flash_color, strength);
        }
        if index as f32 > revealed {
            color = theme::BACKGROUND;
        }
        widgets::put(buffer, rule.x + index as u16, rule.y, symbol, Style::new().fg(color), 1);
    }
}
