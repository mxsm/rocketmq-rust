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
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(5), Constraint::Min(12), Constraint::Length(3)])
        .split(area);

    render_header(frame, vertical[0], state);
    render_body(frame, vertical[1], state);
    render_footer(frame, vertical[2], state);

    if state.show_help {
        render_help(frame, centered_rect(68, 60, area));
    }
}

fn render_header(frame: &mut Frame, area: Rect, state: &AppState) {
    let block = focused_block(
        "RocketMQ Admin TUI",
        state.focus == FocusArea::Namesrv || state.focus == FocusArea::Search,
    );
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let selected = state.selected_command();
    let lines = vec![
        Line::from(vec![
            Span::styled("NameServer: ", Style::default().fg(Color::Cyan)),
            Span::raw(if state.namesrv_addr.trim().is_empty() {
                "<not set>"
            } else {
                state.namesrv_addr.as_str()
            }),
            Span::raw("    "),
            Span::styled("Focus: ", Style::default().fg(Color::Cyan)),
            Span::raw(state.focus.label()),
        ]),
        Line::from(vec![
            Span::styled("Command: ", Style::default().fg(Color::Cyan)),
            Span::raw(selected.category.as_str()),
            Span::raw(" / "),
            Span::raw(state.form.command_id()),
            Span::raw("    "),
            Span::styled("State: ", Style::default().fg(Color::Cyan)),
            Span::raw(state.execution.label()),
        ]),
        Line::from(vec![
            Span::styled("Search: ", Style::default().fg(Color::Cyan)),
            Span::raw(if state.search.is_empty() {
                "<empty>"
            } else {
                state.search.as_str()
            }),
        ]),
    ];
    frame.render_widget(Paragraph::new(lines), inner);
}

fn render_body(frame: &mut Frame, area: Rect, state: &AppState) {
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(30), Constraint::Percentage(70)])
        .split(area);
    render_command_tree(frame, horizontal[0], state);

    let right = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(28),
            Constraint::Percentage(32),
            Constraint::Percentage(40),
        ])
        .split(horizontal[1]);
    render_detail(frame, right[0], state);
    render_args(frame, right[1], state);
    render_result(frame, right[2], state);
}

fn render_command_tree(frame: &mut Frame, area: Rect, state: &AppState) {
    let mut items = Vec::new();
    for (row_index, item) in state.visible_tree_items().into_iter().enumerate() {
        let focused = state.focus == FocusArea::CommandTree && state.tree_cursor() == row_index;
        match item {
            CommandTreeItem::Category(category) => {
                let collapsed = state.is_category_collapsed(category);
                let marker = if collapsed { ">" } else { "v" };
                let style = if focused {
                    Style::default().fg(Color::Black).bg(Color::Cyan)
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
                let risk = match command.risk_level {
                    RiskLevel::Safe => " ",
                    RiskLevel::Mutating => "~",
                    RiskLevel::Dangerous => "!",
                };
                let marker = if selected { ">" } else { " " };
                let style = if focused {
                    Style::default().fg(Color::Black).bg(Color::Cyan)
                } else if selected {
                    Style::default().fg(Color::Cyan)
                } else {
                    Style::default()
                };
                items.push(ListItem::new(Line::from(Span::styled(
                    format!("  {marker} {risk} {}", command.title),
                    style,
                ))));
            }
        }
    }

    if items.is_empty() {
        items.push(ListItem::new("No commands match the search."));
    }

    let list = List::new(items).block(focused_block("Command Tree", state.focus == FocusArea::CommandTree));
    frame.render_widget(list, area);
}

fn render_detail(frame: &mut Frame, area: Rect, state: &AppState) {
    let command = state.selected_command();
    let lines = vec![
        Line::from(vec![
            Span::styled(command.title, Style::default().add_modifier(Modifier::BOLD)),
            Span::raw("  "),
            Span::styled(command.risk_level.as_str(), risk_style(command.risk_level)),
            Span::raw("  "),
            Span::raw(command.result_view_kind.as_str()),
        ]),
        Line::raw(command.description),
        Line::raw(""),
        Line::from(vec![
            Span::styled("Executor: ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{:?}", command.executor)),
        ]),
        Line::from(vec![
            Span::styled("Args: ", Style::default().fg(Color::Cyan)),
            Span::raw(if command.args.is_empty() {
                "none".to_string()
            } else {
                command.args.iter().map(|arg| arg.name).collect::<Vec<_>>().join(", ")
            }),
        ]),
    ];

    frame.render_widget(
        Paragraph::new(lines)
            .block(focused_block("Command Detail", false))
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn render_args(frame: &mut Frame, area: Rect, state: &AppState) {
    let command = state.selected_command();
    let mut lines = Vec::new();
    if command.args.is_empty() {
        lines.push(Line::raw("No arguments. Press Enter to execute."));
    } else {
        for (index, arg) in command.args.iter().enumerate() {
            let focused = state.focus == FocusArea::Args && state.form.focused_arg() == index;
            let value = state.form.raw_value(arg.name).unwrap_or_default();
            let error = state.form.validation_errors().get(arg.name);
            let required = if arg.required { "*" } else { " " };
            let marker = if focused { ">" } else { " " };
            let style = if focused {
                Style::default().fg(Color::Black).bg(Color::Yellow)
            } else if error.is_some() {
                Style::default().fg(Color::Red)
            } else if state.form.dirty() {
                Style::default().fg(Color::White)
            } else {
                Style::default()
            };
            lines.push(Line::from(Span::styled(
                format!(
                    "{marker} {required} {:18} {}",
                    arg.label,
                    display_arg_value(value, &arg.kind, arg.placeholder())
                ),
                style,
            )));
            if focused {
                lines.push(Line::from(Span::styled(
                    format!("    {}", arg.help),
                    Style::default().fg(Color::DarkGray),
                )));
            }
            if let Some(error) = error {
                lines.push(Line::from(Span::styled(
                    format!("    error: {error}"),
                    Style::default().fg(Color::Red),
                )));
            }
        }
    }

    if let CommandExecutionState::Confirming { expected, .. } = &state.execution {
        lines.push(Line::raw(""));
        if let Some(prompt) = state.confirmation_prompt() {
            lines.push(Line::from(Span::styled(prompt, Style::default().fg(Color::Yellow))));
        }
        lines.push(Line::from(Span::styled(
            format!("Confirmation required. Type: {expected}"),
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::from(vec![
            Span::styled("Input: ", Style::default().fg(Color::Cyan)),
            Span::raw(&state.confirm_input),
        ]));
    }

    frame.render_widget(
        Paragraph::new(lines)
            .block(focused_block("Command Args", state.focus == FocusArea::Args))
            .wrap(Wrap { trim: false }),
        area,
    );
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
                .block(focused_block(
                    "Execute Command Result",
                    state.focus == FocusArea::Result,
                ))
                .wrap(Wrap { trim: false }),
            area,
        );
        return;
    };

    match result {
        CommandResultViewModel::Table(table) if !table.headers.is_empty() => {
            let widths = table.headers.iter().map(|_| Constraint::Length(24)).collect::<Vec<_>>();
            let header = Row::new(table.headers.iter().map(|header| {
                Cell::from(header.clone()).style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
            }));
            let rows = table
                .rows
                .iter()
                .skip(state.result_scroll as usize)
                .map(|row| Row::new(row.iter().map(|cell| Cell::from(cell.clone()))));
            let widget = Table::new(rows, widths)
                .header(header)
                .block(focused_block(result.title(), state.focus == FocusArea::Result));
            frame.render_widget(widget, area);
        }
        _ => {
            frame.render_widget(
                Paragraph::new(result.text_body())
                    .block(focused_block(result.title(), state.focus == FocusArea::Result))
                    .scroll((state.result_scroll, state.result_horizontal_scroll))
                    .wrap(Wrap { trim: false }),
                area,
            );
        }
    }
}

fn render_footer(frame: &mut Frame, area: Rect, state: &AppState) {
    let error = state.last_error.as_deref().unwrap_or("");
    let visible_position = state
        .selected_visible_position()
        .map(|position| format!("{}", position + 1))
        .unwrap_or_else(|| "-".to_string());
    let error_state = if state.form.has_errors() { "yes" } else { "no" };
    let execution_id = state
        .execution
        .execution_id()
        .map(|id| id.to_string())
        .unwrap_or_else(|| "-".to_string());
    let lines = vec![
        Line::from(vec![
            Span::styled("Keys: ", Style::default().fg(Color::Cyan)),
            Span::raw(
                "Tab focus | n namesrv | / search | Enter run/toggle | Left/Right fold | Space bool | arrows/jk move \
                 | Ctrl+R rerun | Ctrl+L clear | ? help | q quit",
            ),
        ]),
        Line::from(vec![
            Span::styled("Status: ", Style::default().fg(Color::Cyan)),
            Span::raw(state.execution.label()),
            Span::raw("    "),
            Span::styled("Exec ID: ", Style::default().fg(Color::Cyan)),
            Span::raw(execution_id),
            Span::raw("    "),
            Span::styled("Selected: ", Style::default().fg(Color::Cyan)),
            Span::raw(visible_position),
            Span::raw("    "),
            Span::styled("Form errors: ", Style::default().fg(Color::Cyan)),
            Span::raw(error_state),
            Span::raw("    "),
            Span::styled("Progress: ", Style::default().fg(Color::Cyan)),
            Span::raw(state.progress_message.as_deref().unwrap_or("")),
            Span::raw("    "),
            Span::styled("Last error: ", Style::default().fg(Color::Cyan)),
            Span::raw(error),
        ]),
    ];
    frame.render_widget(
        Paragraph::new(lines).block(Block::default().borders(Borders::ALL)),
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
        Line::raw("Enter                Toggle group, select command, execute, or submit confirmation"),
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
        value.replace('\n', "; ")
    } else {
        truncate(value, 72)
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

fn focused_block(title: &str, focused: bool) -> Block<'_> {
    let style = if focused {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::Gray)
    };
    Block::default().borders(Borders::ALL).title(title).border_style(style)
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
