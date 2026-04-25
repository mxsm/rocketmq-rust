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
        .constraints([Constraint::Percentage(34), Constraint::Percentage(66)])
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

    let line = Line::from(vec![
        Span::styled(" NameServer ", label_style()),
        Span::raw(truncate(namesrv, 36)),
        Span::raw("  "),
        Span::styled(" Focus ", label_style()),
        Span::raw(state.focus.label()),
        Span::raw("  "),
        Span::styled(" State ", label_style()),
        Span::raw(truncate(&state.execution.label(), 34)),
        Span::raw("  "),
        Span::styled(" Exec ", label_style()),
        Span::raw(execution_id),
        Span::raw("  "),
        Span::styled(" Error ", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
        Span::raw(error),
    ]);

    frame.render_widget(
        Paragraph::new(line)
            .block(Block::default().borders(Borders::ALL).title("RocketMQ Admin TUI"))
            .wrap(Wrap { trim: true }),
        area,
    );
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
    let line = Line::from(vec![
        Span::styled(" / ", label_style()),
        Span::raw(truncate(search, 28)),
        Span::raw("  "),
        Span::styled("selected ", label_style()),
        Span::raw(selected),
        Span::raw("/"),
        Span::raw(visible.to_string()),
    ]);

    frame.render_widget(
        Paragraph::new(line)
            .block(focused_block("Command Search", state.focus == FocusArea::Search))
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn render_workspace(frame: &mut Frame, area: Rect, state: &AppState) {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Percentage(38),
            Constraint::Percentage(62),
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
        "Commands".to_string()
    } else {
        let cursor = state.tree_cursor().min(visible.len() - 1) + 1;
        title_with_scroll_hints(
            &format!("Commands {cursor}/{}", visible.len()),
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
                    selected_style()
                } else {
                    Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)
                };
                items.push(ListItem::new(Line::from(Span::styled(
                    format!("{marker} {}", category.as_str()),
                    style,
                ))));
            }
            CommandTreeItem::Command(index) => {
                let command = &state.commands()[index];
                let selected = index == state.selected_command_index();
                let marker = if selected { ">" } else { " " };
                let style = if focused {
                    selected_style()
                } else if selected {
                    Style::default().fg(Color::Cyan)
                } else {
                    Style::default()
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
        items.push(ListItem::new("No commands match the search."));
    }

    let list = List::new(items).block(focused_block(&title, state.focus == FocusArea::CommandTree));
    frame.render_widget(list, area);
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
    let progress = state.progress_message.as_deref().unwrap_or("-");
    let lines = vec![
        Line::from(vec![
            Span::styled(command.title, Style::default().add_modifier(Modifier::BOLD)),
            Span::raw("  "),
            Span::styled(risk_badge(command.risk_level), risk_style(command.risk_level)),
            Span::raw(" "),
            Span::styled(command.risk_level.as_str(), risk_style(command.risk_level)),
            Span::raw("  "),
            Span::styled(command.result_view_kind.as_str(), Style::default().fg(Color::Magenta)),
        ]),
        Line::raw(truncate(command.description, area.width.saturating_sub(4) as usize)),
        Line::from(vec![
            Span::styled("id ", label_style()),
            Span::raw(state.form.command_id()),
            Span::raw("  "),
            Span::styled("category ", label_style()),
            Span::raw(command.category.as_str()),
            Span::raw("  "),
            Span::styled("args ", label_style()),
            Span::raw(arg_summary),
            Span::raw("  "),
            Span::styled("progress ", label_style()),
            Span::raw(truncate(progress, 32)),
        ]),
    ];

    frame.render_widget(
        Paragraph::new(lines)
            .block(focused_block("Command", false))
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn render_args(frame: &mut Frame, area: Rect, state: &AppState) {
    let command = state.selected_command();
    if command.args.is_empty() && !matches!(state.execution, CommandExecutionState::Confirming { .. }) {
        frame.render_widget(
            Paragraph::new("No arguments. Press Enter to execute.")
                .block(focused_block("Arguments", state.focus == FocusArea::Args)),
            area,
        );
        return;
    }

    let mut rows = Vec::new();
    for (index, arg) in command.args.iter().enumerate() {
        let focused = state.focus == FocusArea::Args && state.form.focused_arg() == index;
        let value = state.form.raw_value(arg.name).unwrap_or_default();
        let error = state.form.validation_errors().get(arg.name);
        let style = arg_row_style(focused, error.is_some(), state.form.dirty());
        let marker = if focused { ">" } else { " " };
        let required = if arg.required { "*" } else { " " };
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
                Cell::from(">"),
                Cell::from("!"),
                Cell::from("Confirm"),
                Cell::from(state.confirm_input.clone()),
                Cell::from(format!("{prompt} Type: {expected}")),
            ])
            .style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        );
    }

    let header = Row::new(["", "Req", "Name", "Value", "Help / Error"])
        .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD));
    let widths = [
        Constraint::Length(1),
        Constraint::Length(3),
        Constraint::Length(18),
        Constraint::Percentage(38),
        Constraint::Percentage(62),
    ];
    let table = Table::new(rows, widths)
        .header(header)
        .block(focused_block("Arguments", state.focus == FocusArea::Args));

    frame.render_widget(table, area);
}

fn render_result(frame: &mut Frame, area: Rect, state: &AppState) {
    let Some(result) = &state.result else {
        let body = if let Some(error) = &state.last_error {
            format!("No result.\nLast error: {error}")
        } else {
            "No result yet. Press Enter to execute the selected command.".to_string()
        };
        frame.render_widget(
            Paragraph::new(body)
                .block(focused_block("Result", state.focus == FocusArea::Result))
                .wrap(Wrap { trim: false }),
            area,
        );
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
                Cell::from(header.clone()).style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
            }));
            let rows = viewport
                .rows
                .iter()
                .skip(state.result_scroll as usize)
                .take(visible_rows)
                .map(|row| Row::new(row.iter().map(|cell| Cell::from(cell.clone()))));
            let widget = Table::new(rows, widths)
                .header(header)
                .block(focused_block(&title, state.focus == FocusArea::Result));
            frame.render_widget(widget, area);
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
                    .block(focused_block(&title, state.focus == FocusArea::Result))
                    .scroll((state.result_scroll, state.result_horizontal_scroll))
                    .wrap(Wrap { trim: false }),
                area,
            );
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
        Span::styled(" Keys ", label_style()),
        Span::raw(keys),
        Span::raw("  "),
        Span::styled(" Status ", label_style()),
        Span::raw(status),
        Span::raw("  "),
        Span::styled(" Global ", label_style()),
        Span::raw("Tab focus | Ctrl+R rerun | Ctrl+L clear | ? help | q quit"),
    ]);

    frame.render_widget(
        Paragraph::new(line)
            .block(Block::default().borders(Borders::ALL))
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn render_help(frame: &mut Frame, area: Rect) {
    let lines = vec![
        Line::from(Span::styled(
            "Keyboard Help",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::raw(""),
        Line::raw("Tab / Shift+Tab      Move focus"),
        Line::raw("n                    Edit NameServer address"),
        Line::raw("/ or s               Focus search when not editing args"),
        Line::raw("j/k or arrows        Move command, arg, or result viewport"),
        Line::raw("Left/Right           Collapse or expand command groups; cycle enum arg; scroll result"),
        Line::raw("Space                Toggle bool arg"),
        Line::raw("Enter                Submit input, toggle group, select command, execute, or confirm"),
        Line::raw("Ctrl+R               Re-run selected command"),
        Line::raw("Ctrl+L               Clear result"),
        Line::raw("Esc                  Close modal, cancel local wait, or quit"),
        Line::raw("q                    Close modal or quit"),
    ];
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL).title("Help"))
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

fn focused_block(title: &str, focused: bool) -> Block<'_> {
    let style = if focused {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::Gray)
    };
    Block::default().borders(Borders::ALL).title(title).border_style(style)
}

fn arg_row_style(focused: bool, has_error: bool, dirty: bool) -> Style {
    if focused {
        selected_style()
    } else if has_error {
        Style::default().fg(Color::Red)
    } else if dirty {
        Style::default().fg(Color::White)
    } else {
        Style::default()
    }
}

fn selected_style() -> Style {
    Style::default().fg(Color::Black).bg(Color::Yellow)
}

fn label_style() -> Style {
    Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)
}

fn risk_badge(risk: RiskLevel) -> &'static str {
    match risk {
        RiskLevel::Safe => "SAFE",
        RiskLevel::Mutating => "MUT",
        RiskLevel::Dangerous => "DANGER",
    }
}

fn risk_style(risk: RiskLevel) -> Style {
    match risk {
        RiskLevel::Safe => Style::default().fg(Color::Green),
        RiskLevel::Mutating => Style::default().fg(Color::Yellow),
        RiskLevel::Dangerous => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
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
