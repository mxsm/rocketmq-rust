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

//! Frame post-processing: fades, backdrop dimming, and border highlights.
//!
//! Effects recolor cells that the widgets already drew; they never change a symbol.
//! Continuous effects are limited to a handful of cells per frame so an idle screen
//! stays cheap to redraw over a slow connection. Effects that touch whole regions
//! only run for the few frames of a transition.

use ratatui::buffer::Buffer;
use ratatui::layout::Position;
use ratatui::layout::Rect;
use ratatui::style::Color;

use super::theme;

/// Scales the visibility of `area`: `0.0` leaves only background, `1.0` is a no-op.
pub(super) fn fade(buffer: &mut Buffer, area: Rect, visibility: f32) {
    if visibility >= 1.0 {
        return;
    }
    recolor(buffer, area, |color| theme::mix(theme::BACKGROUND, color, visibility));
}

/// Pushes `area` toward the background by `amount`, as a backdrop behind an overlay.
pub(super) fn dim(buffer: &mut Buffer, area: Rect, amount: f32) {
    if amount <= 0.0 {
        return;
    }
    recolor(buffer, area, |color| theme::mix(color, theme::BACKGROUND, amount));
}

fn recolor(buffer: &mut Buffer, area: Rect, mut map: impl FnMut(Color) -> Color) {
    let area = area.intersection(buffer.area);
    for position in area.positions() {
        if let Some(cell) = buffer.cell_mut(position) {
            cell.fg = map(cell.fg);
            cell.bg = map(cell.bg);
        }
    }
}

/// Paints the background of one row segment, keeping the symbols and foreground.
pub(super) fn fill(buffer: &mut Buffer, area: Rect, background: Color) {
    let area = area.intersection(buffer.area);
    for position in area.positions() {
        if let Some(cell) = buffer.cell_mut(position) {
            cell.bg = background;
        }
    }
}

/// Draws a bright head with a fading tail that travels clockwise along a border.
///
/// Only the foreground of frame glyphs changes: corners keep their shape, and titles
/// written over the border are passed behind rather than recolored.
pub(super) fn comet(buffer: &mut Buffer, area: Rect, step: u64, head: Color, intensity: f32) {
    const TAIL: u64 = 9;
    for offset in (0..TAIL).rev() {
        let Some(position) = border_position(area, step.wrapping_sub(offset).wrapping_add(TAIL)) else {
            return;
        };
        let strength = (1.0 - offset as f32 / TAIL as f32) * intensity;
        if let Some(cell) = buffer.cell_mut(position).filter(|cell| is_frame_glyph(cell.symbol())) {
            cell.fg = theme::mix(cell.fg, head, strength);
        }
    }
}

/// Returns whether `symbol` is a box-drawing character.
fn is_frame_glyph(symbol: &str) -> bool {
    symbol
        .chars()
        .next()
        .is_some_and(|character| ('\u{2500}'..='\u{257f}').contains(&character))
}

/// Maps a step count to a cell on the border of `area`, walking clockwise from the
/// top-left corner.
pub(super) fn border_position(area: Rect, step: u64) -> Option<Position> {
    if area.width < 2 || area.height < 2 {
        return None;
    }

    let top = u64::from(area.width);
    let right = u64::from(area.height - 1);
    let bottom = u64::from(area.width - 1);
    let left = u64::from(area.height - 2);
    let mut position = step % (top + right + bottom + left);
    if position < top {
        return Some(Position::new(area.x + position as u16, area.y));
    }
    position -= top;
    if position < right {
        return Some(Position::new(area.x + area.width - 1, area.y + 1 + position as u16));
    }
    position -= right;
    if position < bottom {
        return Some(Position::new(
            area.x + area.width - 2 - position as u16,
            area.y + area.height - 1,
        ));
    }
    position -= bottom;
    Some(Position::new(area.x, area.y + area.height - 2 - position as u16))
}

/// Replaces every 24-bit color in the frame with its nearest xterm-256 entry.
pub(super) fn quantize(buffer: &mut Buffer) {
    for cell in &mut buffer.content {
        cell.fg = theme::to_indexed(cell.fg);
        cell.bg = theme::to_indexed(cell.bg);
    }
}

#[cfg(test)]
mod tests {
    use ratatui::style::Style;

    use super::*;

    fn filled(width: u16, height: u16, foreground: Color) -> Buffer {
        let mut buffer = Buffer::empty(Rect::new(0, 0, width, height));
        buffer.set_style(buffer.area, Style::new().fg(foreground).bg(theme::BACKGROUND));
        buffer
    }

    #[test]
    fn border_position_walks_clockwise_around_the_area() {
        let area = Rect::new(10, 20, 4, 3);

        assert_eq!(border_position(area, 0), Some(Position::new(10, 20)));
        assert_eq!(border_position(area, 3), Some(Position::new(13, 20)));
        assert_eq!(border_position(area, 4), Some(Position::new(13, 21)));
        assert_eq!(border_position(area, 6), Some(Position::new(12, 22)));
        assert_eq!(border_position(area, 9), Some(Position::new(10, 21)));
        assert_eq!(border_position(area, 10), Some(Position::new(10, 20)));
        assert_eq!(border_position(Rect::new(0, 0, 1, 3), 0), None);
    }

    #[test]
    fn every_border_step_stays_inside_the_area() {
        let area = Rect::new(3, 2, 9, 5);
        let perimeter = 2 * (9 + 5) - 4;

        let visited = (0..perimeter)
            .map(|step| border_position(area, step).unwrap())
            .map(|position| (position.x, position.y))
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(visited.len(), perimeter as usize);
        assert!(visited.iter().all(|(x, y)| {
            area.contains(Position::new(*x, *y))
                && (*x == area.left() || *x == area.right() - 1 || *y == area.top() || *y == area.bottom() - 1)
        }));
    }

    #[test]
    fn fade_and_dim_blend_toward_the_background() {
        let mut buffer = filled(4, 2, theme::BRIGHT);
        let area = buffer.area;

        fade(&mut buffer, area, 1.0);
        assert_eq!(buffer[(0, 0)].fg, theme::BRIGHT);

        fade(&mut buffer, Rect::new(0, 0, 2, 1), 0.0);
        assert_eq!(buffer[(0, 0)].fg, theme::BACKGROUND);
        assert_eq!(buffer[(2, 0)].fg, theme::BRIGHT);

        dim(&mut buffer, Rect::new(2, 0, 2, 2), 1.0);
        assert_eq!(buffer[(3, 1)].fg, theme::BACKGROUND);
        assert_eq!(buffer[(0, 1)].fg, theme::BRIGHT);
    }

    #[test]
    fn effects_ignore_areas_outside_the_buffer() {
        let mut buffer = filled(4, 2, theme::TEXT);
        let outside = Rect::new(2, 1, 40, 40);

        fade(&mut buffer, outside, 0.5);
        dim(&mut buffer, outside, 0.5);
        fill(&mut buffer, outside, theme::SURFACE);
        comet(&mut buffer, outside, 7, theme::PRIMARY, 1.0);

        assert_eq!(buffer[(3, 1)].bg, theme::SURFACE);
        assert_eq!(buffer[(0, 0)].fg, theme::TEXT);
    }

    #[test]
    fn comet_recolors_frame_glyphs_and_passes_behind_titles() {
        let mut buffer = filled(6, 4, theme::BORDER);
        let area = buffer.area;
        for step in 0..16 {
            let position = border_position(area, step).unwrap();
            buffer[(position.x, position.y)].set_symbol("─");
        }
        // The head leads the tail, so at step zero it sits one tail length along the border.
        let head = border_position(area, 9).unwrap();
        let title = border_position(area, 8).unwrap();
        buffer[(title.x, title.y)].set_symbol("T");

        comet(&mut buffer, area, 0, theme::PRIMARY, 1.0);

        assert_eq!(buffer[(head.x, head.y)].symbol(), "─");
        assert_eq!(buffer[(head.x, head.y)].fg, theme::PRIMARY);
        assert_eq!(buffer[(title.x, title.y)].fg, theme::BORDER, "titles keep their color");
        assert_eq!(buffer[(0, 0)].fg, theme::BORDER, "cells behind the tail are untouched");
        assert_eq!(buffer[(2, 2)].fg, theme::BORDER, "interior cells are untouched");
    }

    #[test]
    fn quantize_leaves_no_rgb_colors_behind() {
        let mut buffer = filled(3, 1, theme::PRIMARY);

        quantize(&mut buffer);

        assert!(buffer
            .content
            .iter()
            .all(|cell| matches!(cell.fg, Color::Indexed(_)) && matches!(cell.bg, Color::Indexed(_))));
    }
}
