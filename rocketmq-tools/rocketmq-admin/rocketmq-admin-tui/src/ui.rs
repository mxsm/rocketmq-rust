use std::ops::Range;

use ratatui::layout::Alignment;
use ratatui::layout::Constraint;
use ratatui::layout::Direction;
use ratatui::layout::Layout;
use ratatui::layout::Rect;
use ratatui::style::Color;
use ratatui::style::Modifier;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::text::Span;
use ratatui::widgets::Block;
use ratatui::widgets::Borders;
use ratatui::widgets::Cell;
use ratatui::widgets::Clear;
use ratatui::widgets::List;
use ratatui::widgets::ListItem;
use ratatui::widgets::Paragraph;
use ratatui::widgets::Row;
use ratatui::widgets::Table;
use ratatui::widgets::Wrap;
use ratatui::Frame;

use crate::commands::ArgKind;
use crate::commands::RiskLevel;
use crate::state::AppState;
use crate::state::CommandExecutionState;
use crate::state::CommandTreeItem;
use crate::state::FocusArea;
use crate::view_model::CommandResultViewModel;

pub(crate) fn render(frame: &mut Frame, state: &AppState) {
    let area = frame.area();
    let layout = workspace_layout(area);

    render_status_bar(frame, layout.status, state);
    render_command_panel(frame, layout.command_panel, state);
    render_workspace(frame, layout.workspace, state);
    render_key_bar(frame, layout.keys, state);

    if state.show_help {
        render_help(frame, centered_rect(68, 60, area));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkspaceLayout {
    status: Rect,
    command_panel: Rect,
    workspace: Rect,
    keys: Rect,
}

fn workspace_layout(area: Rect) -> WorkspaceLayout {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(12), Constraint::Length(3)])
        .split(area);
    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(31), Constraint::Percentage(69)])
        .split(vertical[1]);

    WorkspaceLayout {
        status: vertical[0],
        command_panel: body[0],
        workspace: body[1],
        keys: vertical[2],
    }
}

fn render_status_bar(frame: &mut Frame, area: Rect, state: &AppState) {
    let namesrv = if state.namesrv_addr.trim().is_empty() {
        "<not set>"
    } else {
        state.namesrv_addr.as_str()
    };
    let error = state
        .last_error
        .as_deref()
        .map(|value| truncate(value, 48))
        .unwrap_or_else(|| "-".to_string());
    let execution_id = state
        .execution
        .execution_id()
        .map(|id| id.to_string())
        .unwrap_or_else(|| "-".to_string());
    let (state_label, state_style) = execution_badge(state);

    let line = Line::from(vec![
        Span::styled("  ", Style::default().bg(Color::Red)),
        Span::raw(" "),
        Span::styled("  ", Style::default().bg(Color::Yellow)),
        Span::raw(" "),
        Span::styled("  ", Style::default().bg(Color::Green)),
        Span::raw("  "),
        Span::styled("RocketMQ Admin", title_style()),
        Span::styled("  Pro TUI", muted_value_style()),
        Span::raw("     "),
        Span::styled("NameServer ", label_style()),
        Span::styled(truncate(namesrv, 34), value_style()),
        Span::raw("   "),
        Span::styled("Focus ", label_style()),
        Span::styled(state.focus.label(), focus_text_style(state.animation_tick())),
        Span::raw("   "),
        Span::styled("State ", label_style()),
        Span::styled(state_label, state_style),
        Span::raw("   "),
        Span::styled("Exec ", label_style()),
        Span::styled(execution_id, value_style()),
        Span::raw("   "),
        Span::styled("Error ", danger_style()),
        Span::styled(error, muted_value_style()),
    ]);

    frame.render_widget(
        Paragraph::new(line)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" macOS Operations Bar ")
                    .border_style(Style::default().fg(border_color())),
            )
            .wrap(Wrap { trim: true }),
        area,
    );
    render_focus_halo(frame, area, state.focus == FocusArea::Namesrv, state.animation_tick());
}

fn render_command_panel(frame: &mut Frame, area: Rect, state: &AppState) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(6)])
        .split(area);
    render_search_summary(frame, sections[0], state);
    render_command_tree(frame, sections[1], state);
}

fn render_search_summary(frame: &mut Frame, area: Rect, state: &AppState) {
    let search = if state.search.trim().is_empty() {
        "<empty>"
    } else {
        state.search.as_str()
    };
    let visible = state.visible_command_indices().len();
    let selected = state
        .selected_visible_position()
        .map(|position| (position + 1).to_string())
        .unwrap_or_else(|| "-".to_string());
    let caret = if state.focus == FocusArea::Search && state.animation_tick() % 18 < 9 {
        "_"
    } else {
        " "
    };
    let line = Line::from(vec![
        Span::styled("Search ", label_style()),
        Span::styled("/", accent_style()),
        Span::raw(" "),
        Span::styled(
            truncate(search, 24),
            input_value_style(state.focus == FocusArea::Search),
        ),
        Span::styled(caret, focus_text_style(state.animation_tick())),
        Span::raw("   "),
        Span::styled("match ", label_style()),
        Span::styled(selected, value_style()),
        Span::raw("/"),
        Span::styled(visible.to_string(), value_style()),
    ]);

    frame.render_widget(
        Paragraph::new(line)
            .block(focused_block(
                "Command Search",
                state.focus == FocusArea::Search,
                state.animation_tick(),
            ))
            .wrap(Wrap { trim: true }),
        area,
    );
    render_focus_halo(frame, area, state.focus == FocusArea::Search, state.animation_tick());
}

fn render_workspace(frame: &mut Frame, area: Rect, state: &AppState) {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(6),
            Constraint::Percentage(34),
            Constraint::Percentage(66),
        ])
        .split(area);

    render_command_summary(frame, vertical[0], state);
    render_args(frame, vertical[1], state);
    render_result(frame, vertical[2], state);
}

fn render_command_tree(frame: &mut Frame, area: Rect, state: &AppState) {
    let visible = state.visible_tree_items();
    let row_capacity = area.height.saturating_sub(2) as usize;
    let viewport = tree_viewport_range(visible.len(), state.tree_cursor(), row_capacity);
    let title = if visible.is_empty() {
        "Sidebar Commands".to_string()
    } else {
        let cursor = state.tree_cursor().min(visible.len() - 1) + 1;
        title_with_scroll_hints(
            &format!("Sidebar Commands {cursor}/{}", visible.len()),
            viewport.start > 0,
            viewport.end < visible.len(),
            false,
            false,
        )
    };
    let mut items = Vec::new();
    for (row_index, item) in visible
        .iter()
        .copied()
        .enumerate()
        .skip(viewport.start)
        .take(viewport.end.saturating_sub(viewport.start))
    {
        let focused = state.focus == FocusArea::CommandTree && state.tree_cursor() == row_index;
        match item {
            CommandTreeItem::Category(category) => {
                let collapsed = state.is_category_collapsed(category);
                let marker = if collapsed { ">" } else { "v" };
                let style = if focused {
                    selected_style(state.animation_tick())
                } else {
                    Style::default().fg(sidebar_header_color()).add_modifier(Modifier::BOLD)
                };
                items.push(ListItem::new(Line::from(Span::styled(
                    format!("{marker} {}", category.as_str()),
                    style,
                ))));
            }
            CommandTreeItem::Command(index) => {
                let command = &state.commands()[index];
                let selected = index == state.selected_command_index();
                let marker = if selected {
                    animated_selector(state.animation_tick())
                } else {
                    " "
                };
                let style = if focused {
                    selected_style(state.animation_tick())
                } else if selected {
                    Style::default().fg(accent_color()).add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(text_color())
                };
                items.push(ListItem::new(Line::from(vec![
                    Span::styled(format!("  {marker} "), style),
                    Span::styled(risk_badge(command.risk_level), risk_style(command.risk_level)),
                    Span::styled(format!(" {}", command.title), style),
                ])));
            }
        }
    }

    if visible.is_empty() {
        items.push(ListItem::new(Line::from(vec![
            Span::styled(" No commands match ", muted_value_style()),
            Span::styled(truncate(&state.search, 24), input_value_style(false)),
            Span::styled(". Clear search or try another keyword.", muted_value_style()),
        ])));
    }

    let list = List::new(items).block(focused_block(
        &title,
        state.focus == FocusArea::CommandTree,
        state.animation_tick(),
    ));
    frame.render_widget(list, area);
    render_focus_halo(
        frame,
        area,
        state.focus == FocusArea::CommandTree,
        state.animation_tick(),
    );
}

fn tree_viewport_range(total_len: usize, cursor: usize, row_capacity: usize) -> Range<usize> {
    if total_len == 0 || row_capacity == 0 {
        return 0..0;
    }

    let cursor = cursor.min(total_len - 1);
    let row_capacity = row_capacity.min(total_len);
    let half_window = row_capacity / 2;
    let mut start = cursor.saturating_sub(half_window);
    if start + row_capacity > total_len {
        start = total_len - row_capacity;
    }

    start..start + row_capacity
}

fn render_command_summary(frame: &mut Frame, area: Rect, state: &AppState) {
    let command = state.selected_command();
    let arg_summary = if command.args.is_empty() {
        "none".to_string()
    } else {
        format!("{} args", command.args.len())
    };
    let progress = state.progress_message.as_deref().unwrap_or("ready");
    let (state_label, state_style) = execution_badge(state);
    let lines = vec![
        Line::from(vec![
            Span::styled("Command ", label_style()),
            Span::styled(command.title, title_style()),
            Span::raw("  "),
            Span::styled(risk_badge(command.risk_level), risk_style(command.risk_level)),
            Span::raw(" "),
            Span::styled(command.risk_level.as_str(), risk_style(command.risk_level)),
            Span::raw("  "),
            Span::styled(command.result_view_kind.as_str(), purple_style()),
        ]),
        Line::from(vec![
            Span::styled("id ", label_style()),
            Span::styled(state.form.command_id(), value_style()),
            Span::raw("  "),
            Span::styled("category ", label_style()),
            Span::styled(command.category.as_str(), value_style()),
            Span::raw("  "),
            Span::styled("args ", label_style()),
            Span::styled(arg_summary, value_style()),
        ]),
        Line::from(vec![
            Span::styled("state ", label_style()),
            Span::styled(state_label, state_style),
            Span::raw("  "),
            Span::styled("progress ", label_style()),
            Span::styled(progress_prefix(state), state_style),
            Span::styled(
                truncate(progress, area.width.saturating_sub(28) as usize),
                muted_value_style(),
            ),
        ]),
        Line::from(vec![
            Span::styled("about ", label_style()),
            Span::raw(truncate(command.description, area.width.saturating_sub(10) as usize)),
        ]),
    ];

    frame.render_widget(
        Paragraph::new(lines)
            .block(focused_block("Inspector", false, state.animation_tick()))
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn render_args(frame: &mut Frame, area: Rect, state: &AppState) {
    let command = state.selected_command();
    if command.args.is_empty() && !matches!(state.execution, CommandExecutionState::Confirming { .. }) {
        let line = Line::from(vec![
            Span::styled("Ready ", success_style()),
            Span::raw("No arguments required. Press "),
            Span::styled("Enter", label_style()),
            Span::raw(" to execute."),
        ]);
        frame.render_widget(
            Paragraph::new(line).block(focused_block(
                "Parameters",
                state.focus == FocusArea::Args,
                state.animation_tick(),
            )),
            area,
        );
        render_focus_halo(frame, area, state.focus == FocusArea::Args, state.animation_tick());
        return;
    }

    let mut rows = Vec::new();
    for (index, arg) in command.args.iter().enumerate() {
        let focused = state.focus == FocusArea::Args && state.form.focused_arg() == index;
        let value = state.form.raw_value(arg.name).unwrap_or_default();
        let error = state.form.validation_errors().get(arg.name);
        let style = arg_row_style(focused, error.is_some(), state.form.dirty(), state.animation_tick());
        let marker = if focused {
            animated_selector(state.animation_tick())
        } else {
            " "
        };
        let required = if arg.required { "yes" } else { " " };
        let help_or_error = error.map(|error| format!("error: {error}")).unwrap_or_else(|| {
            if focused {
                arg.help.to_string()
            } else {
                String::new()
            }
        });

        rows.push(
            Row::new([
                Cell::from(marker),
                Cell::from(required),
                Cell::from(arg.label),
                Cell::from(display_arg_value(value, &arg.kind, arg.placeholder())),
                Cell::from(help_or_error),
            ])
            .style(style),
        );
    }

    if let CommandExecutionState::Confirming { expected, .. } = &state.execution {
        let prompt = state
            .confirmation_prompt()
            .unwrap_or_else(|| "Confirmation required.".to_string());
        rows.push(
            Row::new([
                Cell::from(animated_selector(state.animation_tick())),
                Cell::from("yes"),
                Cell::from("Confirm"),
                Cell::from(state.confirm_input.clone()),
                Cell::from(format!("{prompt} Type: {expected}")),
            ])
            .style(warning_style()),
        );
    }

    let header = Row::new(["", "Req", "Name", "Value", "Help / Error"])
        .style(Style::default().fg(accent_color()).add_modifier(Modifier::BOLD));
    let widths = [
        Constraint::Length(1),
        Constraint::Length(3),
        Constraint::Length(18),
        Constraint::Percentage(38),
        Constraint::Percentage(62),
    ];
    let table = Table::new(rows, widths).header(header).block(focused_block(
        "Parameters",
        state.focus == FocusArea::Args,
        state.animation_tick(),
    ));

    frame.render_widget(table, area);
    render_focus_halo(frame, area, state.focus == FocusArea::Args, state.animation_tick());
}

fn render_result(frame: &mut Frame, area: Rect, state: &AppState) {
    let Some(result) = &state.result else {
        let body = if matches!(state.execution, CommandExecutionState::Running { .. }) {
            format!(
                "{} Running command...\n{}\n{}",
                spinner_frame(state.animation_tick()),
                progress_meter(state.animation_tick(), area.width.saturating_sub(6) as usize),
                state.progress_message.as_deref().unwrap_or("waiting for progress")
            )
        } else if let Some(error) = &state.last_error {
            format!("No result yet.\nLast error: {error}")
        } else {
            "No result yet.\nSelect a command, review parameters, then press Enter to execute.".to_string()
        };
        frame.render_widget(
            Paragraph::new(body)
                .block(focused_block(
                    "Result Console",
                    state.focus == FocusArea::Result,
                    state.animation_tick(),
                ))
                .wrap(Wrap { trim: false }),
            area,
        );
        render_focus_halo(frame, area, state.focus == FocusArea::Result, state.animation_tick());
        return;
    };

    match result {
        CommandResultViewModel::Table(table) if !table.headers.is_empty() => {
            let viewport = table.viewport(state.result_horizontal_scroll as usize, area.width.saturating_sub(2));
            let visible_rows = area.height.saturating_sub(3) as usize;
            let hidden_up = state.result_scroll > 0;
            let hidden_down = (state.result_scroll as usize).saturating_add(visible_rows) < viewport.rows.len();
            let title = title_with_scroll_hints(
                result.title(),
                hidden_up,
                hidden_down,
                viewport.hidden_left,
                viewport.hidden_right,
            );
            let widths = viewport
                .widths
                .iter()
                .copied()
                .map(Constraint::Length)
                .collect::<Vec<_>>();
            let header = Row::new(viewport.headers.iter().map(|header| {
                Cell::from(header.clone()).style(Style::default().fg(accent_color()).add_modifier(Modifier::BOLD))
            }));
            let rows = viewport
                .rows
                .iter()
                .skip(state.result_scroll as usize)
                .take(visible_rows)
                .map(|row| Row::new(row.iter().map(|cell| Cell::from(cell.clone()))));
            let widget = Table::new(rows, widths).header(header).block(focused_block(
                &title,
                state.focus == FocusArea::Result,
                state.animation_tick(),
            ));
            frame.render_widget(widget, area);
            render_focus_halo(frame, area, state.focus == FocusArea::Result, state.animation_tick());
        }
        _ => {
            let body = result.text_body();
            let total_lines = body.lines().count();
            let visible_lines = area.height.saturating_sub(2) as usize;
            let title = title_with_scroll_hints(
                result.title(),
                state.result_scroll > 0,
                (state.result_scroll as usize).saturating_add(visible_lines) < total_lines,
                state.result_horizontal_scroll > 0,
                false,
            );
            frame.render_widget(
                Paragraph::new(body)
                    .block(focused_block(
                        &title,
                        state.focus == FocusArea::Result,
                        state.animation_tick(),
                    ))
                    .scroll((state.result_scroll, state.result_horizontal_scroll))
                    .wrap(Wrap { trim: false }),
                area,
            );
            render_focus_halo(frame, area, state.focus == FocusArea::Result, state.animation_tick());
        }
    }
}

fn render_key_bar(frame: &mut Frame, area: Rect, state: &AppState) {
    let keys = key_hint_for_focus(state.focus);
    let status = if state.form.has_errors() {
        "form errors: yes"
    } else {
        "form errors: no"
    };
    let line = Line::from(vec![
        Span::styled("Active Keys ", label_style()),
        Span::styled(keys, muted_value_style()),
        Span::raw("  "),
        Span::styled("Status ", label_style()),
        Span::styled(
            status,
            if state.form.has_errors() {
                danger_style()
            } else {
                success_style()
            },
        ),
        Span::raw("  "),
        Span::styled("Global ", label_style()),
        Span::styled(
            "Tab focus | Ctrl+R rerun | Ctrl+L clear | ? help | q quit",
            muted_value_style(),
        ),
    ]);

    frame.render_widget(
        Paragraph::new(line)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(border_color())),
            )
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn render_help(frame: &mut Frame, area: Rect) {
    let lines = vec![
        Line::from(Span::styled("RocketMQ Admin TUI Help", title_style())),
        Line::raw(""),
        help_line(
            "Tab / Shift+Tab",
            "Move focus through NameServer, Search, Commands, Parameters, Result",
        ),
        help_line("n", "Edit NameServer address"),
        help_line("/ or s", "Focus command search when not editing parameters"),
        help_line("j/k or arrows", "Move command, parameter, or result viewport"),
        help_line(
            "Left / Right",
            "Collapse groups, cycle enum parameters, or scroll result columns",
        ),
        help_line("Space", "Toggle boolean parameter"),
        help_line("Enter", "Submit input, select command, execute, or confirm"),
        help_line("Ctrl+R", "Re-run selected command"),
        help_line("Ctrl+L", "Clear result"),
        help_line("Esc", "Close modal, cancel local wait, or quit"),
        help_line("q", "Close modal or quit"),
    ];
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Help ")
                    .border_style(Style::default().fg(accent_color())),
            )
            .alignment(Alignment::Left),
        area,
    );
}

fn display_arg_value(value: &str, kind: &ArgKind, placeholder: &str) -> String {
    if value.is_empty() {
        format!("<{placeholder}>")
    } else if matches!(kind, ArgKind::KeyValueMap) {
        truncate(&value.replace('\n', "; "), 52)
    } else {
        truncate(value, 52)
    }
}

fn truncate(value: &str, max_len: usize) -> String {
    if max_len <= 3 {
        return value.chars().take(max_len).collect();
    }
    if value.chars().count() <= max_len {
        value.to_string()
    } else {
        let mut truncated = value.chars().take(max_len.saturating_sub(3)).collect::<String>();
        truncated.push_str("...");
        truncated
    }
}

fn title_with_scroll_hints(
    title: &str,
    hidden_up: bool,
    hidden_down: bool,
    hidden_left: bool,
    hidden_right: bool,
) -> String {
    let vertical = match (hidden_up, hidden_down) {
        (true, true) => "^v ",
        (true, false) => "^ ",
        (false, true) => "v ",
        (false, false) => "",
    };
    let horizontal = match (hidden_left, hidden_right) {
        (true, true) => " < >",
        (true, false) => " <",
        (false, true) => " >",
        (false, false) => "",
    };
    format!("{vertical}{title}{horizontal}")
}

fn key_hint_for_focus(focus: FocusArea) -> &'static str {
    match focus {
        FocusArea::Namesrv => "type address | Enter save | Backspace delete | Esc quit",
        FocusArea::Search => "type filter | Enter commands | Backspace delete | Esc quit",
        FocusArea::CommandTree => {
            "j/k move | Enter select/toggle | Left collapse | Right expand | / search | n namesrv"
        }
        FocusArea::Args => "j/k field | type edit | Space bool | Left/Right enum | Enter execute",
        FocusArea::Result => "j/k scroll rows | Left/Right scroll columns | Enter reset scroll",
    }
}

fn focused_block(title: &str, focused: bool, tick: u64) -> Block<'_> {
    let style = if focused {
        Style::default().fg(focus_color(tick)).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(border_color())
    };
    Block::default()
        .borders(Borders::ALL)
        .title(format!(" {title} "))
        .border_style(style)
}

fn render_focus_halo(frame: &mut Frame, area: Rect, focused: bool, tick: u64) {
    if !focused {
        return;
    }

    let step = tick / 2;
    let tail_style = Style::default().fg(focus_color(tick)).add_modifier(Modifier::BOLD);
    let head_style = Style::default()
        .fg(Color::White)
        .bg(focus_color(tick))
        .add_modifier(Modifier::BOLD);

    for (offset, symbol, style) in [(4, ".", tail_style), (2, ".", tail_style), (0, "o", head_style)] {
        let Some((x, y)) = rotating_border_position(area, step.saturating_sub(offset)) else {
            continue;
        };
        frame.buffer_mut()[(x, y)].set_symbol(symbol).set_style(style);
    }
}

fn rotating_border_position(area: Rect, step: u64) -> Option<(u16, u16)> {
    if area.width < 2 || area.height < 2 {
        return None;
    }

    let top_len = u64::from(area.width);
    let right_len = u64::from(area.height - 1);
    let bottom_len = u64::from(area.width - 1);
    let left_len = u64::from(area.height.saturating_sub(2));
    let perimeter = top_len + right_len + bottom_len + left_len;
    if perimeter == 0 {
        return None;
    }

    let mut position = step % perimeter;
    if position < top_len {
        return Some((area.x + position as u16, area.y));
    }

    position -= top_len;
    if position < right_len {
        return Some((area.x + area.width - 1, area.y + 1 + position as u16));
    }

    position -= right_len;
    if position < bottom_len {
        return Some((area.x + area.width - 2 - position as u16, area.y + area.height - 1));
    }

    position -= bottom_len;
    if position < left_len {
        return Some((area.x, area.y + area.height - 2 - position as u16));
    }

    None
}

fn arg_row_style(focused: bool, has_error: bool, dirty: bool, tick: u64) -> Style {
    if focused {
        selected_style(tick)
    } else if has_error {
        danger_style()
    } else if dirty {
        Style::default().fg(text_color())
    } else {
        muted_value_style()
    }
}

fn selected_style(tick: u64) -> Style {
    Style::default()
        .fg(Color::Black)
        .bg(focus_color(tick))
        .add_modifier(Modifier::BOLD)
}

fn label_style() -> Style {
    Style::default().fg(label_color()).add_modifier(Modifier::BOLD)
}

fn title_style() -> Style {
    Style::default().fg(Color::White).add_modifier(Modifier::BOLD)
}

fn value_style() -> Style {
    Style::default().fg(text_color())
}

fn muted_value_style() -> Style {
    Style::default().fg(muted_text_color())
}

fn accent_style() -> Style {
    Style::default().fg(accent_color()).add_modifier(Modifier::BOLD)
}

fn purple_style() -> Style {
    Style::default()
        .fg(Color::Rgb(191, 171, 255))
        .add_modifier(Modifier::BOLD)
}

fn input_value_style(focused: bool) -> Style {
    if focused {
        Style::default().fg(Color::White).add_modifier(Modifier::BOLD)
    } else {
        value_style()
    }
}

fn focus_text_style(tick: u64) -> Style {
    Style::default().fg(focus_color(tick)).add_modifier(Modifier::BOLD)
}

fn success_style() -> Style {
    Style::default().fg(success_color()).add_modifier(Modifier::BOLD)
}

fn warning_style() -> Style {
    Style::default().fg(warning_color()).add_modifier(Modifier::BOLD)
}

fn danger_style() -> Style {
    Style::default().fg(danger_color()).add_modifier(Modifier::BOLD)
}

fn accent_color() -> Color {
    Color::Rgb(10, 132, 255)
}

fn focus_color(tick: u64) -> Color {
    if tick % 24 < 12 {
        accent_color()
    } else {
        Color::Rgb(100, 210, 255)
    }
}

fn label_color() -> Color {
    Color::Rgb(142, 142, 147)
}

fn sidebar_header_color() -> Color {
    Color::Rgb(174, 174, 178)
}

fn border_color() -> Color {
    Color::Rgb(72, 72, 74)
}

fn text_color() -> Color {
    Color::Rgb(242, 242, 247)
}

fn muted_text_color() -> Color {
    Color::Rgb(174, 174, 178)
}

fn success_color() -> Color {
    Color::Rgb(48, 209, 88)
}

fn warning_color() -> Color {
    Color::Rgb(255, 214, 10)
}

fn danger_color() -> Color {
    Color::Rgb(255, 69, 58)
}

fn spinner_frame(tick: u64) -> &'static str {
    match (tick / 3) % 4 {
        0 => "|",
        1 => "/",
        2 => "-",
        _ => "\\",
    }
}

fn animated_selector(tick: u64) -> &'static str {
    if tick % 18 < 9 {
        ">"
    } else {
        "-"
    }
}

fn progress_meter(tick: u64, width: usize) -> String {
    let width = width.clamp(8, 36);
    let mut bar = vec!['.'; width];
    let head = (tick as usize / 2) % width;
    for offset in 0..3 {
        bar[(head + offset) % width] = '=';
    }
    format!("[{}]", bar.into_iter().collect::<String>())
}

fn progress_prefix(state: &AppState) -> &'static str {
    match &state.execution {
        CommandExecutionState::Running { .. } => spinner_frame(state.animation_tick()),
        CommandExecutionState::Succeeded { .. } => "ok ",
        CommandExecutionState::Failed { .. } => "err ",
        CommandExecutionState::Cancelled { .. } => "stop ",
        CommandExecutionState::Confirming { .. } => "wait ",
        CommandExecutionState::Idle => "",
    }
}

fn execution_badge(state: &AppState) -> (String, Style) {
    let label = state.execution.label();
    match &state.execution {
        CommandExecutionState::Idle => ("ready".to_string(), muted_value_style()),
        CommandExecutionState::Confirming { .. } => (label, warning_style()),
        CommandExecutionState::Running { .. } => (
            format!("{} {label}", spinner_frame(state.animation_tick())),
            accent_style(),
        ),
        CommandExecutionState::Succeeded { .. } => (label, success_style()),
        CommandExecutionState::Failed { .. } => (label, danger_style()),
        CommandExecutionState::Cancelled { .. } => {
            (label, Style::default().fg(label_color()).add_modifier(Modifier::BOLD))
        }
    }
}

fn help_line(key: &'static str, description: &'static str) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{key:<18}"), label_style()),
        Span::styled(description, muted_value_style()),
    ])
}

fn risk_badge(risk: RiskLevel) -> &'static str {
    match risk {
        RiskLevel::Safe => "SAFE ",
        RiskLevel::Mutating => "MUT  ",
        RiskLevel::Dangerous => "DANGER",
    }
}

fn risk_style(risk: RiskLevel) -> Style {
    match risk {
        RiskLevel::Safe => success_style(),
        RiskLevel::Mutating => warning_style(),
        RiskLevel::Dangerous => danger_style(),
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

#[cfg(test)]
mod tests {
    use ratatui::layout::Rect;

    use super::key_hint_for_focus;
    use super::rotating_border_position;
    use super::title_with_scroll_hints;
    use super::tree_viewport_range;
    use super::truncate;
    use super::workspace_layout;
    use crate::state::FocusArea;

    #[test]
    fn command_tree_viewport_keeps_top_items_when_cursor_is_near_top() {
        assert_eq!(tree_viewport_range(100, 2, 10), 0..10);
    }

    #[test]
    fn command_tree_viewport_centers_middle_cursor_when_possible() {
        assert_eq!(tree_viewport_range(100, 50, 10), 45..55);
    }

    #[test]
    fn command_tree_viewport_keeps_bottom_items_when_cursor_is_near_bottom() {
        assert_eq!(tree_viewport_range(100, 99, 10), 90..100);
    }

    #[test]
    fn command_tree_viewport_handles_small_and_empty_lists() {
        assert_eq!(tree_viewport_range(4, 99, 10), 0..4);
        assert_eq!(tree_viewport_range(0, 0, 10), 0..0);
        assert_eq!(tree_viewport_range(10, 5, 0), 0..0);
    }

    #[test]
    fn command_tree_viewport_keeps_cursor_visible_in_small_terminal() {
        let viewport = tree_viewport_range(30, 17, 3);

        assert!(viewport.contains(&17));
        assert_eq!(viewport.len(), 3);
    }

    #[test]
    fn title_hints_show_vertical_and_horizontal_scroll_state() {
        assert_eq!(
            title_with_scroll_hints("Result", true, true, true, true),
            "^v Result < >"
        );
        assert_eq!(
            title_with_scroll_hints("Result", false, true, false, true),
            "v Result >"
        );
        assert_eq!(title_with_scroll_hints("Result", false, false, false, false), "Result");
    }

    #[test]
    fn focus_key_hints_are_contextual() {
        assert!(key_hint_for_focus(FocusArea::CommandTree).contains("Enter select"));
        assert!(key_hint_for_focus(FocusArea::Args).contains("Enter execute"));
        assert!(key_hint_for_focus(FocusArea::Result).contains("scroll"));
    }

    #[test]
    fn rotating_border_position_walks_around_panel_border() {
        let area = Rect::new(10, 20, 4, 3);

        assert_eq!(rotating_border_position(area, 0), Some((10, 20)));
        assert_eq!(rotating_border_position(area, 3), Some((13, 20)));
        assert_eq!(rotating_border_position(area, 4), Some((13, 21)));
        assert_eq!(rotating_border_position(area, 6), Some((12, 22)));
        assert_eq!(rotating_border_position(area, 9), Some((10, 21)));
        assert_eq!(rotating_border_position(area, 10), Some((10, 20)));
        assert_eq!(rotating_border_position(Rect::new(0, 0, 1, 3), 0), None);
    }

    #[test]
    fn truncate_preserves_short_text_and_shortens_long_text() {
        assert_eq!(truncate("short", 10), "short");
        assert_eq!(truncate("abcdefghijkl", 8), "abcde...");
    }

    #[test]
    fn workspace_layout_keeps_command_panel_and_workspace_nonzero() {
        let layout = workspace_layout(Rect::new(0, 0, 120, 40));

        assert_eq!(layout.status.height, 3);
        assert_eq!(layout.keys.height, 3);
        assert!(layout.command_panel.width > 0);
        assert!(layout.workspace.width > layout.command_panel.width);
    }
}
