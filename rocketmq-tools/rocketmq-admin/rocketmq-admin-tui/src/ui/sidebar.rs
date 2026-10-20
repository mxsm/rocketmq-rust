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

//! Command list: search field and the grouped command tree.

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
use crate::commands::CommandCategory;
use crate::commands::CommandSpec;
use crate::state::CommandTreeItem;
use crate::state::FocusArea;
use crate::state::InputTarget;
use crate::state::Pane;
use crate::text::display_width;
use crate::text::truncate;

/// Rows of context kept above and below the cursor while scrolling.
const SCROLL_MARGIN: usize = 2;
/// Ticks the row the cursor just left keeps glowing.
const AFTERGLOW_TICKS: u64 = 9;

pub(super) fn paint(frame: &mut Frame, ctx: &Ctx<'_>, area: Rect, view: &mut ViewCache) {
    let state = ctx.state;
    let focused = state.focus.pane() == Pane::Sidebar;
    let total = state.commands().len();
    let matching = ctx.category_sizes.values().sum::<usize>();
    let count = if state.search.trim().is_empty() {
        format!(" {total} ")
    } else {
        format!(" {matching} of {total} ")
    };
    let block = widgets::pane(ctx.pane_border(Pane::Sidebar))
        .title_top(Line::from(widgets::pane_title("Commands", focused)))
        .title_top(Line::from(Span::styled(count, theme::faint())).right_aligned());
    let inner = block.inner(area);
    frame.render_widget(block, area);
    view.register(area, HitTarget::Tree);
    if inner.is_empty() {
        return;
    }

    let search = Rect { height: 1, ..inner };
    let cursor = paint_search(frame.buffer_mut(), ctx, search, view);

    // A spare row separates the field from the list when there is room for it.
    let gap = u16::from(inner.height >= 10);
    let list = Rect {
        y: inner.y + 1 + gap,
        height: inner.height.saturating_sub(1 + gap),
        ..inner
    };
    paint_tree(frame.buffer_mut(), ctx, list, view);
    widgets::scrollbar(
        frame.buffer_mut(),
        area.right() - 1,
        list.y,
        list.height,
        ctx.tree.len(),
        usize::from(list.height),
        view.tree_top,
        if focused { theme::PRIMARY } else { theme::FAINT },
    );

    if let Some(cursor) = cursor {
        frame.set_cursor_position(cursor);
    }
}

fn paint_search(buffer: &mut Buffer, ctx: &Ctx<'_>, row: Rect, view: &mut ViewCache) -> Option<Position> {
    let state = ctx.state;
    let focused = state.focus == FocusArea::Search;
    let background = if focused || ctx.hovered(row) {
        theme::SURFACE_RAISED
    } else {
        theme::SURFACE
    };
    effects::fill(buffer, row, background);
    view.register(row, HitTarget::Search);

    let icon = if focused { theme::accent() } else { theme::faint() };
    widgets::put(buffer, row.x + 1, row.y, "/", icon, row.width.saturating_sub(1));

    let text_x = row.x + 3;
    let text_width = row.width.saturating_sub(4);
    if focused {
        let (visible, offset) = widgets::field_window(
            &state.search,
            state.input_cursor(InputTarget::Search),
            usize::from(text_width),
        );
        if state.search.is_empty() {
            widgets::put(buffer, text_x, row.y, "type to filter", theme::faint(), text_width);
        } else {
            widgets::put(
                buffer,
                text_x,
                row.y,
                visible,
                theme::colored(theme::BRIGHT),
                text_width,
            );
        }
        return Some(Position::new(text_x + offset as u16, row.y));
    }

    if state.search.is_empty() {
        widgets::put(buffer, text_x, row.y, "Search commands", theme::faint(), text_width);
    } else {
        widgets::put(
            buffer,
            text_x,
            row.y,
            &truncate(&state.search, usize::from(text_width)),
            theme::text(),
            text_width,
        );
    }
    None
}

fn paint_tree(buffer: &mut Buffer, ctx: &Ctx<'_>, list: Rect, view: &mut ViewCache) {
    let state = ctx.state;
    let rows = usize::from(list.height);
    view.tree_rows = rows;
    if list.is_empty() {
        return;
    }
    if ctx.tree.is_empty() {
        paint_no_match(buffer, ctx, list);
        return;
    }

    view.tree_top = widgets::scroll_into_view(view.tree_top, state.tree_cursor(), ctx.tree.len(), rows, SCROLL_MARGIN);
    for (position, item) in ctx.tree.iter().enumerate().skip(view.tree_top).take(rows) {
        let row = Rect {
            y: list.y + (position - view.tree_top) as u16,
            height: 1,
            ..list
        };
        view.register(row, HitTarget::TreeRow(position));

        let on_cursor = position == state.tree_cursor();
        let background = row_background(ctx, *item, on_cursor, row);
        if background != theme::BACKGROUND {
            effects::fill(buffer, row, background);
        }
        match *item {
            CommandTreeItem::Category(category) => paint_category(buffer, ctx, row, category, on_cursor),
            CommandTreeItem::Command(index) => paint_command(buffer, ctx, row, index, on_cursor),
        }
    }
}

fn row_background(ctx: &Ctx<'_>, item: CommandTreeItem, on_cursor: bool, row: Rect) -> Color {
    let state = ctx.state;
    if on_cursor {
        return match state.focus {
            FocusArea::CommandTree => theme::SELECTION,
            FocusArea::Search => theme::SURFACE_RAISED,
            FocusArea::Namesrv | FocusArea::Args | FocusArea::Result => theme::SURFACE,
        };
    }

    // The row the cursor just left fades out instead of snapping back.
    let motion = state.motion();
    let tracked = motion.tree_item();
    if state.focus == FocusArea::CommandTree
        && tracked.previous() == Some(item)
        && tracked.current() == state.focused_tree_item()
        && motion.is_playing(tracked.changed_at(), AFTERGLOW_TICKS)
    {
        return theme::mix(
            theme::SELECTION,
            theme::BACKGROUND,
            motion.transition(tracked.changed_at(), AFTERGLOW_TICKS),
        );
    }
    if ctx.hovered(row) {
        theme::SURFACE
    } else {
        theme::BACKGROUND
    }
}

fn paint_category(buffer: &mut Buffer, ctx: &Ctx<'_>, row: Rect, category: CommandCategory, on_cursor: bool) {
    let state = ctx.state;
    // A search shows every match, so groups are always open while it is active.
    let collapsed = state.search.trim().is_empty() && state.is_category_collapsed(category);
    let marker = if collapsed {
        theme::GLYPH_COLLAPSED
    } else {
        theme::GLYPH_EXPANDED
    };
    let name_style = if on_cursor {
        theme::bright()
    } else {
        theme::colored(theme::TEXT).add_modifier(Modifier::BOLD)
    };

    let count = ctx.category_sizes.get(&category).copied().unwrap_or(0).to_string();
    let count_width = display_width(&count) as u16;
    let count_x = row.right().saturating_sub(count_width + 1);
    widgets::put(buffer, row.x + 1, row.y, marker, theme::muted(), 1);
    widgets::put(
        buffer,
        row.x + 3,
        row.y,
        category.as_str(),
        name_style,
        count_x.saturating_sub(row.x + 4),
    );
    widgets::put(buffer, count_x, row.y, &count, theme::faint(), count_width);
}

fn paint_command(buffer: &mut Buffer, ctx: &Ctx<'_>, row: Rect, index: usize, on_cursor: bool) {
    let state = ctx.state;
    let command = &state.commands()[index];
    let selected = index == state.selected_command_index();
    if selected {
        widgets::put(buffer, row.x, row.y, theme::GLYPH_SELECTED, theme::accent(), 1);
    }
    widgets::put(
        buffer,
        row.x + 3,
        row.y,
        theme::risk_glyph(command.risk_level),
        theme::colored(theme::risk_color(command.risk_level)),
        1,
    );

    let base = if on_cursor {
        theme::bright()
    } else if selected {
        theme::accent()
    } else {
        theme::text()
    };
    let title_width = row.width.saturating_sub(6);
    widgets::put_line(
        buffer,
        row.x + 5,
        row.y,
        &highlighted_title(command, &state.search, usize::from(title_width), base),
        title_width,
    );
}

/// Returns the command title with the part that matches the search emphasized.
fn highlighted_title(command: &CommandSpec, search: &str, width: usize, base: Style) -> Line<'static> {
    let title = truncate(command.title, width).into_owned();
    let query = search.trim().to_ascii_lowercase();
    // ASCII lowercasing keeps byte offsets, so the match maps back onto the title.
    let matched = (!query.is_empty())
        .then(|| title.to_ascii_lowercase().find(&query))
        .flatten()
        .map(|start| (start, start + query.len()))
        .filter(|(start, end)| title.is_char_boundary(*start) && title.is_char_boundary(*end));
    let Some((start, end)) = matched else {
        return Line::from(Span::styled(title, base));
    };

    let emphasis = Style::new()
        .fg(theme::WARNING)
        .add_modifier(Modifier::BOLD | Modifier::UNDERLINED);
    Line::from(vec![
        Span::styled(title[..start].to_string(), base),
        Span::styled(title[start..end].to_string(), emphasis),
        Span::styled(title[end..].to_string(), base),
    ])
}

fn paint_no_match(buffer: &mut Buffer, ctx: &Ctx<'_>, list: Rect) {
    let width = list.width.saturating_sub(2);
    let query = format!(
        "“{}”",
        truncate(ctx.state.search.trim(), usize::from(width.saturating_sub(2)))
    );
    let lines = [
        Line::from(Span::styled("No command matches", theme::muted())),
        Line::from(Span::styled(query, theme::colored(theme::WARNING))),
        Line::default(),
        Line::from(vec![
            widgets::keycap("Esc"),
            Span::styled(" clears the search", theme::faint()),
        ]),
    ];
    for (offset, line) in lines.iter().enumerate().take(usize::from(list.height)) {
        widgets::put_line(buffer, list.x + 1, list.y + offset as u16, line, width);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::command_catalog;

    fn plain(line: &Line<'_>) -> String {
        line.spans.iter().map(|span| span.content.as_ref()).collect()
    }

    #[test]
    fn matching_part_of_a_title_is_emphasized() {
        let catalog = command_catalog();
        let command = catalog.iter().find(|command| command.id == "topic.route").unwrap();

        let line = highlighted_title(command, " ROUTE ", 40, theme::text());

        assert_eq!(plain(&line), "Topic Route");
        assert_eq!(line.spans.len(), 3);
        assert_eq!(line.spans[1].content, "Route");
        assert!(line.spans[1].style.add_modifier.contains(Modifier::UNDERLINED));
    }

    #[test]
    fn titles_without_a_match_keep_one_span() {
        let catalog = command_catalog();
        let command = catalog.iter().find(|command| command.id == "topic.route").unwrap();

        // The search matched the description or the id, not the title.
        assert_eq!(highlighted_title(command, "data", 40, theme::text()).spans.len(), 1);
        assert_eq!(highlighted_title(command, "", 40, theme::text()).spans.len(), 1);
        assert_eq!(
            plain(&highlighted_title(command, "route", 8, theme::text())),
            "Topic R…"
        );
    }
}
