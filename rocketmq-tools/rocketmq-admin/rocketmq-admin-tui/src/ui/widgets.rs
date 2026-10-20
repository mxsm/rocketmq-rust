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

//! Drawing helpers shared by the panes.

use std::time::Duration;

use ratatui::buffer::Buffer;
use ratatui::layout::Position;
use ratatui::layout::Rect;
use ratatui::style::Color;
use ratatui::style::Modifier;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::text::Span;
use ratatui::widgets::Block;
use ratatui::widgets::BorderType;

use super::theme;
use crate::text::byte_offset;
use crate::text::char_width;
use crate::text::display_width;
use crate::text::slice_columns;

/// Writes `text` at `(x, y)` in at most `max_width` cells and returns the next column.
///
/// Positions outside the frame are ignored, which keeps callers free of bounds checks
/// while a pane is animating or the terminal is very small.
pub(super) fn put(buffer: &mut Buffer, x: u16, y: u16, text: &str, style: Style, max_width: u16) -> u16 {
    if max_width == 0 || !buffer.area.contains(Position::new(x, y)) {
        return x;
    }
    buffer.set_stringn(x, y, text, usize::from(max_width), style).0
}

/// Writes a styled line at `(x, y)` in at most `max_width` cells and returns the next column.
pub(super) fn put_line(buffer: &mut Buffer, x: u16, y: u16, line: &Line<'_>, max_width: u16) -> u16 {
    if max_width == 0 || !buffer.area.contains(Position::new(x, y)) {
        return x;
    }
    buffer.set_line(x, y, line, max_width).0
}

/// Writes a styled line so that it ends just before column `right`, never left of `left`.
pub(super) fn put_line_right(buffer: &mut Buffer, left: u16, right: u16, y: u16, line: &Line<'_>) {
    let width = (line.width() as u16).min(right.saturating_sub(left));
    put_line(buffer, right.saturating_sub(width), y, line, width);
}

/// Writes a styled line centered within `area` on row `y`.
pub(super) fn put_line_centered(buffer: &mut Buffer, area: Rect, y: u16, line: &Line<'_>) {
    let width = (line.width() as u16).min(area.width);
    put_line(buffer, area.x + (area.width - width) / 2, y, line, width);
}

/// Returns the area inside a one-cell border.
pub(super) fn inset(area: Rect) -> Rect {
    Rect {
        x: area.x.saturating_add(1),
        y: area.y.saturating_add(1),
        width: area.width.saturating_sub(2),
        height: area.height.saturating_sub(2),
    }
}

/// Returns a rounded pane frame in `border` color.
pub(super) fn pane(border: Color) -> Block<'static> {
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(border))
        .style(Style::new().fg(theme::TEXT).bg(theme::BACKGROUND))
}

pub(super) fn pane_title(title: &str, focused: bool) -> Span<'static> {
    let style = if focused { theme::bright() } else { theme::muted() };
    Span::styled(format!(" {title} "), style)
}

/// Returns a key rendered as a raised cap.
pub(super) fn keycap(key: &str) -> Span<'static> {
    Span::styled(
        format!(" {key} "),
        Style::new()
            .fg(theme::BRIGHT)
            .bg(theme::SURFACE_RAISED)
            .add_modifier(Modifier::BOLD),
    )
}

/// Returns a filled label.
pub(super) fn chip(label: &str, foreground: Color, background: Color) -> Span<'static> {
    Span::styled(
        format!(" {label} "),
        Style::new().fg(foreground).bg(background).add_modifier(Modifier::BOLD),
    )
}

/// Draws a scroll thumb over the border column at `x`, spanning `height` rows from `y`.
pub(super) fn scrollbar(
    buffer: &mut Buffer,
    x: u16,
    y: u16,
    height: u16,
    total: usize,
    visible: usize,
    offset: usize,
    color: Color,
) {
    let track = usize::from(height);
    if track == 0 || visible == 0 || total <= visible {
        return;
    }
    let thumb = (visible * track).div_ceil(total).clamp(1, track);
    let travel = track - thumb;
    let last_offset = total - visible;
    let top = (offset.min(last_offset) * travel + last_offset / 2) / last_offset;
    for row in top..top + thumb {
        put(buffer, x, y + row as u16, theme::SCROLL_THUMB, theme::colored(color), 1);
    }
}

/// Returns the first visible index that keeps `cursor` on screen with `margin` rows of
/// context, moving the previous `top` as little as possible.
pub(super) fn scroll_into_view(top: usize, cursor: usize, total: usize, capacity: usize, margin: usize) -> usize {
    if capacity == 0 || total <= capacity {
        return 0;
    }
    let last_top = total - capacity;
    let margin = margin.min((capacity - 1) / 2);
    let cursor = cursor.min(total - 1);
    let top = top.min(last_top);
    let top = if cursor < top + margin {
        cursor.saturating_sub(margin)
    } else if cursor + margin >= top + capacity {
        cursor + margin + 1 - capacity
    } else {
        top
    };
    top.min(last_top)
}

/// Returns the slice of `text` to show in a field of `width` cells and the cell offset of
/// the cursor within it. The window scrolls just far enough to keep the cursor visible.
pub(super) fn field_window(text: &str, cursor: usize, width: usize) -> (&str, usize) {
    if width == 0 {
        return ("", 0);
    }
    let cursor_column = display_width(&text[..byte_offset(text, cursor)]);
    let wanted = cursor_column.saturating_sub(width - 1);
    // Start on a character boundary so a wide character is never split.
    let mut start = 0;
    for character in text.chars() {
        if start >= wanted {
            break;
        }
        start += char_width(character);
    }
    let start = start.min(cursor_column);
    (slice_columns(text, start, width), cursor_column - start)
}

/// Formats an elapsed duration for the status line.
pub(super) fn format_duration(duration: Duration) -> String {
    let millis = duration.as_millis();
    if millis < 1_000 {
        format!("{millis} ms")
    } else if millis < 60_000 {
        format!("{:.1} s", millis as f64 / 1_000.0)
    } else {
        let seconds = duration.as_secs();
        format!("{}m {:02}s", seconds / 60, seconds % 60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_ignores_positions_outside_the_frame() {
        let mut buffer = Buffer::empty(Rect::new(0, 0, 6, 2));

        assert_eq!(put(&mut buffer, 0, 9, "text", Style::new(), 4), 0);
        assert_eq!(put(&mut buffer, 9, 0, "text", Style::new(), 4), 9);
        assert_eq!(put(&mut buffer, 0, 0, "text", Style::new(), 0), 0);
        assert_eq!(put(&mut buffer, 4, 1, "text", Style::new(), 9), 6);
        assert_eq!(buffer[(5, 1)].symbol(), "e");
    }

    #[test]
    fn aligned_lines_stay_within_their_bounds() {
        let mut buffer = Buffer::empty(Rect::new(0, 0, 10, 1));
        let line = Line::from("abcdef");

        put_line_right(&mut buffer, 6, 10, 0, &line);
        assert_eq!(buffer[(6, 0)].symbol(), "a");
        assert_eq!(buffer[(5, 0)].symbol(), " ");

        let mut buffer = Buffer::empty(Rect::new(0, 0, 10, 1));
        put_line_centered(&mut buffer, Rect::new(0, 0, 10, 1), 0, &line);
        assert_eq!(buffer[(2, 0)].symbol(), "a");
        assert_eq!(buffer[(7, 0)].symbol(), "f");
    }

    #[test]
    fn scroll_into_view_moves_only_when_the_cursor_leaves_the_window() {
        // Everything fits.
        assert_eq!(scroll_into_view(3, 2, 5, 10, 2), 0);
        // The cursor is comfortably inside: the window stays put.
        assert_eq!(scroll_into_view(10, 15, 100, 10, 2), 10);
        // Moving down past the margin scrolls by the minimum amount.
        assert_eq!(scroll_into_view(10, 18, 100, 10, 2), 11);
        // Moving up past the margin does the same.
        assert_eq!(scroll_into_view(10, 11, 100, 10, 2), 9);
        // Both ends clamp.
        assert_eq!(scroll_into_view(10, 99, 100, 10, 2), 90);
        assert_eq!(scroll_into_view(10, 0, 100, 10, 2), 0);
        assert_eq!(scroll_into_view(0, 5, 100, 0, 2), 0);
    }

    #[test]
    fn scroll_into_view_keeps_cursor_visible_in_tiny_windows() {
        for capacity in 1..5 {
            for cursor in 0..30 {
                let top = scroll_into_view(12, cursor, 30, capacity, 2);
                assert!((top..top + capacity).contains(&cursor), "{capacity} {cursor} {top}");
            }
        }
    }

    #[test]
    fn field_window_scrolls_to_keep_the_cursor_visible() {
        assert_eq!(field_window("abcdef", 6, 10), ("abcdef", 6));
        assert_eq!(field_window("abcdefghij", 10, 4), ("hij", 3));
        assert_eq!(field_window("abcdefghij", 2, 4), ("abcd", 2));
        assert_eq!(field_window("abc", 99, 4), ("abc", 3));
        assert_eq!(field_window("abc", 1, 0), ("", 0));
        // Wide characters are never split at the left edge.
        assert_eq!(field_window("主题主题", 4, 4), ("题", 2));
        assert_eq!(field_window("a主题主", 4, 4), ("主", 2));
    }

    #[test]
    fn scrollbar_thumb_tracks_the_viewport() {
        let symbol = |buffer: &Buffer, y: u16| buffer[(0, y)].symbol().to_string();

        let mut buffer = Buffer::empty(Rect::new(0, 0, 1, 10));
        scrollbar(&mut buffer, 0, 0, 10, 100, 10, 0, theme::MUTED);
        assert_eq!(symbol(&buffer, 0), theme::SCROLL_THUMB);
        assert_eq!(symbol(&buffer, 1), " ");

        let mut buffer = Buffer::empty(Rect::new(0, 0, 1, 10));
        scrollbar(&mut buffer, 0, 0, 10, 100, 10, 90, theme::MUTED);
        assert_eq!(symbol(&buffer, 9), theme::SCROLL_THUMB);
        assert_eq!(symbol(&buffer, 8), " ");

        let mut buffer = Buffer::empty(Rect::new(0, 0, 1, 10));
        scrollbar(&mut buffer, 0, 0, 10, 5, 10, 0, theme::MUTED);
        assert!((0..10).all(|y| symbol(&buffer, y) == " "));
    }

    #[test]
    fn durations_use_the_most_readable_unit() {
        assert_eq!(format_duration(Duration::from_millis(42)), "42 ms");
        assert_eq!(format_duration(Duration::from_millis(1_240)), "1.2 s");
        assert_eq!(format_duration(Duration::from_secs(125)), "2m 05s");
    }
}
