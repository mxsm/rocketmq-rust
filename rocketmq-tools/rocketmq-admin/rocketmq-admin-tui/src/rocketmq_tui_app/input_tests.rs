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

use ratatui::backend::TestBackend;
use ratatui::crossterm::event::Event;
use ratatui::crossterm::event::KeyCode;
use ratatui::crossterm::event::KeyEvent;
use ratatui::crossterm::event::KeyModifiers;
use ratatui::crossterm::event::MouseButton;
use ratatui::crossterm::event::MouseEvent;
use ratatui::crossterm::event::MouseEventKind;
use ratatui::Terminal;

use super::*;
use crate::admin_facade::test_client_runtime;
use crate::state::InputTarget;
use crate::state::Overlay;
use crate::view_model::TableViewModel;

fn app() -> RocketmqTuiApp {
    RocketmqTuiApp::new(test_client_runtime())
}

fn press(app: &mut RocketmqTuiApp, code: KeyCode) {
    app.handle_key_event(KeyEvent::new(code, KeyModifiers::NONE));
}

fn ctrl(app: &mut RocketmqTuiApp, character: char) {
    app.handle_key_event(KeyEvent::new(KeyCode::Char(character), KeyModifiers::CONTROL));
}

fn type_text(app: &mut RocketmqTuiApp, text: &str) {
    for character in text.chars() {
        press(app, KeyCode::Char(character));
    }
}

fn select(app: &mut RocketmqTuiApp, command_id: &str) {
    app.apply_action(Action::SearchChanged(command_id.to_string()));
    assert_eq!(app.state.selected_command().id, command_id);
}

/// Advances one frame and draws it, which is what gives input its hit regions and page sizes.
fn draw_sized(app: &mut RocketmqTuiApp, width: u16, height: u16) -> Terminal<TestBackend> {
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    app.state.advance_animation();
    terminal.draw(|frame| app.draw(frame)).unwrap();
    terminal
}

fn draw(app: &mut RocketmqTuiApp) -> Terminal<TestBackend> {
    draw_sized(app, 120, 36)
}

/// Returns the cell at which `needle` starts on the drawn screen.
fn locate(terminal: &Terminal<TestBackend>, needle: &str) -> (u16, u16) {
    let buffer = terminal.backend().buffer();
    (0..buffer.area.height)
        .find_map(|y| {
            let row = (0..buffer.area.width)
                .map(|x| buffer[(x, y)].symbol())
                .collect::<Vec<_>>();
            (0..row.len())
                .find(|x| row[*x..].concat().starts_with(needle))
                .map(|x| (x as u16, y))
        })
        .unwrap_or_else(|| panic!("{needle:?} is not on screen"))
}

fn mouse(app: &mut RocketmqTuiApp, kind: MouseEventKind, (column, row): (u16, u16), modifiers: KeyModifiers) {
    app.handle_mouse_event(MouseEvent {
        kind,
        column,
        row,
        modifiers,
    });
}

fn click(app: &mut RocketmqTuiApp, position: (u16, u16)) {
    mouse(
        app,
        MouseEventKind::Down(MouseButton::Left),
        position,
        KeyModifiers::NONE,
    );
}

fn show_table(app: &mut RocketmqTuiApp, rows: usize, columns: usize) {
    app.state.set_result(
        CommandResultViewModel::Table(TableViewModel {
            title: "Rows".to_string(),
            headers: (0..columns).map(|column| format!("column-{column}")).collect(),
            rows: (0..rows)
                .map(|row| (0..columns).map(|column| format!("cell-{row}-{column}")).collect())
                .collect(),
        }),
        ResultTone::Normal,
    );
    app.state.execution = CommandExecutionState::Succeeded {
        execution_id: 1,
        command_id: "topic.list".to_string(),
    };
    // Let the row cascade of a new result finish.
    for _ in 0..40 {
        app.state.advance_animation();
    }
}

fn toast(app: &RocketmqTuiApp) -> &str {
    app.state.toast().map_or("", |toast| toast.message.as_str())
}

#[test]
fn escape_steps_back_one_level_at_a_time() {
    let mut app = app();

    app.state.set_focus(FocusArea::Args);
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.state.focus, FocusArea::CommandTree);

    app.state.set_focus(FocusArea::Result);
    app.state.result_zoom = true;
    press(&mut app, KeyCode::Esc);
    assert!(!app.state.result_zoom);
    assert_eq!(app.state.focus, FocusArea::Result);
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.state.focus, FocusArea::CommandTree);

    app.apply_action(Action::FocusSearch);
    type_text(&mut app, "topic");
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.state.search, "");
    assert_eq!(app.state.focus, FocusArea::CommandTree);

    app.apply_action(Action::SearchChanged("topic".to_string()));
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.state.search, "");
    assert!(!app.should_quit());
}

#[test]
fn escape_at_the_top_level_quits_only_on_the_second_press() {
    let mut app = app();

    press(&mut app, KeyCode::Esc);
    assert!(!app.should_quit());
    assert_eq!(toast(&app), "Press Esc again to quit");

    press(&mut app, KeyCode::Esc);
    assert!(app.should_quit());
}

#[test]
fn the_quit_shortcut_disarms_after_a_pause() {
    let mut app = app();

    press(&mut app, KeyCode::Esc);
    for _ in 0..90 {
        app.state.advance_animation();
    }
    press(&mut app, KeyCode::Esc);

    assert!(!app.should_quit());
}

#[test]
fn escape_in_the_nameserver_field_restores_the_previous_address() {
    let facade = TuiAdminFacade::with_namesrv_addr(test_client_runtime(), "127.0.0.1:9876");
    let mut app = RocketmqTuiApp::with_admin_facade(facade);

    press(&mut app, KeyCode::Char('n'));
    assert_eq!(app.state.focus, FocusArea::Namesrv);
    type_text(&mut app, "0");
    assert_eq!(app.state.namesrv_addr, "127.0.0.1:98760");
    assert_eq!(app.admin_facade().namesrv_addr(), Some("127.0.0.1:98760"));

    press(&mut app, KeyCode::Esc);

    assert_eq!(app.state.namesrv_addr, "127.0.0.1:9876");
    assert_eq!(app.admin_facade().namesrv_addr(), Some("127.0.0.1:9876"));
    assert_eq!(app.state.focus, FocusArea::CommandTree);
    assert!(!app.should_quit());
}

#[test]
fn ctrl_c_cancels_a_running_command_and_quits_when_idle() {
    let mut app = app();
    app.state.execution = CommandExecutionState::Running {
        execution_id: 4,
        command_id: "topic.list".to_string(),
    };

    ctrl(&mut app, 'c');
    assert!(matches!(
        app.state.execution,
        CommandExecutionState::Cancelled { execution_id: 4, .. }
    ));
    assert!(!app.should_quit());
    assert!(toast(&app).starts_with("Cancelled locally"));

    ctrl(&mut app, 'c');
    assert!(app.should_quit());
}

#[test]
fn ctrl_q_quits_even_while_typing() {
    let mut app = app();
    app.state.set_focus(FocusArea::Args);

    ctrl(&mut app, 'q');

    assert!(app.should_quit());
}

#[test]
fn text_inputs_edit_at_the_cursor() {
    let mut app = app();
    app.apply_action(Action::FocusSearch);

    type_text(&mut app, "topc");
    press(&mut app, KeyCode::Left);
    type_text(&mut app, "i");
    assert_eq!(app.state.search, "topic");
    assert_eq!(app.state.input_cursor(InputTarget::Search), 4);

    press(&mut app, KeyCode::Home);
    press(&mut app, KeyCode::Delete);
    assert_eq!(app.state.search, "opic");
    press(&mut app, KeyCode::End);
    press(&mut app, KeyCode::Backspace);
    assert_eq!(app.state.search, "opi");

    press(&mut app, KeyCode::Left);
    ctrl(&mut app, 'u');
    assert_eq!(app.state.search, "i");
    ctrl(&mut app, 'k');
    assert_eq!(app.state.search, "");
}

#[test]
fn parameter_fields_edit_at_the_cursor() {
    let mut app = app();
    select(&mut app, "topic.route");
    app.state.set_focus(FocusArea::Args);

    type_text(&mut app, "Topic A");
    press(&mut app, KeyCode::Left);
    press(&mut app, KeyCode::Left);
    press(&mut app, KeyCode::Backspace);
    assert_eq!(app.state.form.raw_value("topic"), Some("Topi A"));
    assert_eq!(app.state.input_cursor(InputTarget::Arg(0)), 4);

    ctrl(&mut app, 'w');
    assert_eq!(app.state.form.raw_value("topic"), Some(" A"));
    assert!(app.state.form.dirty());
}

#[test]
fn arrow_keys_in_the_search_field_move_the_selection() {
    let mut app = app();
    app.apply_action(Action::FocusSearch);
    type_text(&mut app, "topic");
    let first = app.state.selected_command().id;

    press(&mut app, KeyCode::Down);

    assert_ne!(app.state.selected_command().id, first);
    assert_eq!(app.state.focus, FocusArea::Search);
    assert_eq!(app.state.search, "topic");
}

#[test]
fn choice_parameters_switch_with_arrows_and_space_and_take_no_text() {
    let mut app = app();
    select(&mut app, "topic.update");
    app.state.set_focus(FocusArea::Args);

    app.state.focus_arg(1);
    assert_eq!(app.state.active_input(), None);
    press(&mut app, KeyCode::Right);
    assert_eq!(app.state.form.raw_value("target_type"), Some("cluster"));
    press(&mut app, KeyCode::Left);
    assert_eq!(app.state.form.raw_value("target_type"), Some("broker"));
    type_text(&mut app, "xq?");
    assert_eq!(app.state.form.raw_value("target_type"), Some("broker"));
    assert!(!app.should_quit());

    app.state.focus_arg(6);
    press(&mut app, KeyCode::Char(' '));
    assert_eq!(app.state.form.raw_value("order"), Some("true"));
    press(&mut app, KeyCode::Left);
    assert_eq!(app.state.form.raw_value("order"), Some("false"));
}

#[test]
fn form_values_survive_a_visit_to_another_command() {
    let mut app = app();
    select(&mut app, "topic.route");
    app.state.set_focus(FocusArea::Args);
    type_text(&mut app, "TopicA");

    select(&mut app, "topic.status");
    assert_eq!(app.state.form.raw_value("topic"), Some(""));

    select(&mut app, "topic.route");
    assert_eq!(app.state.form.raw_value("topic"), Some("TopicA"));

    ctrl(&mut app, 'd');
    assert_eq!(app.state.form.raw_value("topic"), Some(""));
    assert_eq!(toast(&app), "Parameters restored to their defaults");

    select(&mut app, "topic.status");
    select(&mut app, "topic.route");
    assert_eq!(app.state.form.raw_value("topic"), Some(""));
}

#[test]
fn enter_on_a_command_opens_its_form_without_resetting_it() {
    let mut app = app();
    app.state.form.set_value("cluster_name", "DefaultCluster".to_string());

    press(&mut app, KeyCode::Enter);

    assert_eq!(app.state.focus, FocusArea::Args);
    assert_eq!(app.state.form.raw_value("cluster_name"), Some("DefaultCluster"));
}

#[test]
fn page_and_edge_keys_move_through_the_command_tree() {
    let mut app = app();
    draw(&mut app);
    let rows = app.view.tree_rows;
    assert!(rows > 5);
    let start = app.state.tree_cursor();

    press(&mut app, KeyCode::PageDown);
    assert_eq!(app.state.tree_cursor(), start + rows - 1);

    press(&mut app, KeyCode::End);
    assert_eq!(app.state.tree_cursor(), app.state.visible_tree_items().len() - 1);
    press(&mut app, KeyCode::Char('g'));
    assert_eq!(app.state.tree_cursor(), 0);
    press(&mut app, KeyCode::Char('G'));
    assert_eq!(app.state.tree_cursor(), app.state.visible_tree_items().len() - 1);
    press(&mut app, KeyCode::PageUp);
    press(&mut app, KeyCode::Home);
    assert_eq!(app.state.tree_cursor(), 0);
}

#[test]
fn single_letters_jump_between_panes_only_outside_text_inputs() {
    let mut app = app();

    press(&mut app, KeyCode::Char('p'));
    assert_eq!(app.state.focus, FocusArea::Args);
    type_text(&mut app, "rc");
    assert_eq!(app.state.focus, FocusArea::Args, "letters are text inside the form");
    assert_eq!(app.state.form.raw_value("cluster_name"), Some("rc"));

    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Char('r'));
    assert_eq!(app.state.focus, FocusArea::Result);
    press(&mut app, KeyCode::Char('c'));
    assert_eq!(app.state.focus, FocusArea::CommandTree);

    press(&mut app, KeyCode::Char('z'));
    assert!(app.state.result_zoom);
    assert_eq!(app.state.focus, FocusArea::Result);
    press(&mut app, KeyCode::Char('z'));
    assert!(!app.state.result_zoom);
}

#[test]
fn the_search_is_one_chord_away_from_a_text_field() {
    let mut app = app();
    select(&mut app, "topic.route");
    app.state.set_focus(FocusArea::Args);

    type_text(&mut app, "/a");
    assert_eq!(
        app.state.form.raw_value("topic"),
        Some("/a"),
        "a field types the shortcut keys"
    );
    assert_eq!(app.state.focus, FocusArea::Args);

    ctrl(&mut app, 'f');
    assert_eq!(app.state.focus, FocusArea::Search);
    assert_eq!(app.state.active_input(), Some(InputTarget::Search));
    assert_eq!(
        app.state.form.raw_value("topic"),
        Some("/a"),
        "the field keeps what was typed"
    );
}

#[test]
fn every_group_can_be_folded_and_unfolded_at_once() {
    let mut app = app();
    let everything = app.state.visible_tree_items().len();
    let selected = app.state.selected_command_index();

    press(&mut app, KeyCode::Char('-'));
    let folded = app.state.visible_tree_items();
    assert!(folded.iter().all(|item| matches!(item, CommandTreeItem::Category(_))));
    assert!(matches!(
        app.state.focused_tree_item(),
        Some(CommandTreeItem::Category(category)) if category == app.state.selected_command().category
    ));

    press(&mut app, KeyCode::Char('+'));
    assert_eq!(app.state.visible_tree_items().len(), everything);
    assert_eq!(app.state.focused_tree_item(), Some(CommandTreeItem::Command(selected)));
}

#[test]
fn clicks_focus_panes_select_commands_and_fold_groups() {
    let mut app = app();
    let screen = draw(&mut app);

    click(&mut app, locate(&screen, "Topic Route"));
    assert_eq!(app.state.selected_command().id, "topic.route");
    assert_eq!(app.state.focus, FocusArea::CommandTree);

    // The same row again opens the form.
    click(&mut app, locate(&screen, "Topic Route"));
    assert_eq!(app.state.focus, FocusArea::Args);

    click(&mut app, locate(&screen, "Search commands"));
    assert_eq!(app.state.focus, FocusArea::Search);
    click(&mut app, locate(&screen, "NameServer"));
    assert_eq!(app.state.focus, FocusArea::Namesrv);
    click(&mut app, locate(&screen, "Ready when you are"));
    assert_eq!(app.state.focus, FocusArea::Result);

    let auth = locate(&screen, "Auth");
    click(&mut app, auth);
    assert_eq!(app.state.focus, FocusArea::CommandTree);
    assert!(app.state.is_category_collapsed(crate::commands::CommandCategory::Auth));
    assert_eq!(
        app.state.selected_command().id,
        "topic.route",
        "folding selects nothing"
    );
}

#[test]
fn clicking_a_parameter_focuses_it_and_clicking_a_choice_picks_it() {
    let mut app = app();
    select(&mut app, "topic.update");
    let screen = draw(&mut app);

    click(&mut app, locate(&screen, "Write Queues"));
    assert_eq!(app.state.focus, FocusArea::Args);
    assert_eq!(app.state.form.focused_arg(), 4);

    click(&mut app, locate(&screen, " cluster "));
    assert_eq!(app.state.form.focused_arg(), 1);
    assert_eq!(app.state.form.raw_value("target_type"), Some("cluster"));
}

#[test]
fn the_run_button_asks_for_confirmation_before_a_mutating_command() {
    let mut app = app();
    select(&mut app, "auth.user.update");
    app.state.form.set_value("username", "alice".to_string());
    let screen = draw(&mut app);

    click(&mut app, locate(&screen, "Run"));

    assert!(matches!(
        &app.state.execution,
        CommandExecutionState::Confirming { command_id, expected, .. }
            if command_id == "auth.user.update" && expected == "confirm"
    ));
    assert!(app.running_task.is_none());
}

#[test]
fn the_wheel_scrolls_whatever_is_under_the_pointer() {
    let mut app = app();
    show_table(&mut app, 80, 12);
    let screen = draw(&mut app);
    let tree_row = locate(&screen, "Topic Route");
    let result_row = locate(&screen, "cell-3-0");

    let cursor = app.state.tree_cursor();
    mouse(&mut app, MouseEventKind::ScrollDown, tree_row, KeyModifiers::NONE);
    assert_eq!(app.state.tree_cursor(), cursor + 1);
    mouse(&mut app, MouseEventKind::ScrollUp, tree_row, KeyModifiers::NONE);
    assert_eq!(app.state.tree_cursor(), cursor);

    mouse(&mut app, MouseEventKind::ScrollDown, result_row, KeyModifiers::NONE);
    assert_eq!(app.state.result().unwrap().top(), 3);
    assert_eq!(app.state.result().unwrap().cursor(), 3, "the selection stays on screen");
    assert_eq!(app.state.tree_cursor(), cursor, "the tree did not move");

    mouse(&mut app, MouseEventKind::ScrollDown, result_row, KeyModifiers::SHIFT);
    assert_eq!(app.state.result().unwrap().column(), 1);
    mouse(&mut app, MouseEventKind::ScrollLeft, result_row, KeyModifiers::NONE);
    assert_eq!(app.state.result().unwrap().column(), 0);
}

#[test]
fn result_rows_open_their_details_from_keyboard_and_mouse() {
    let mut app = app();
    show_table(&mut app, 10, 3);
    app.state.set_focus(FocusArea::Result);

    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Enter);
    assert_eq!(app.state.overlay(), Overlay::Detail);
    let detail = app.state.detail().unwrap();
    assert_eq!(detail.fields[0], ("column-0".to_string(), "cell-1-0".to_string()));

    // Keys go to the overlay while it is open.
    press(&mut app, KeyCode::Char('j'));
    assert_eq!(app.state.result().unwrap().cursor(), 1);
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.state.overlay(), Overlay::None);
    assert_eq!(app.state.focus, FocusArea::Result);

    let screen = draw(&mut app);
    let row = locate(&screen, "cell-4-0");
    click(&mut app, row);
    assert_eq!(app.state.result().unwrap().cursor(), 4);
    assert_eq!(app.state.overlay(), Overlay::None);
    click(&mut app, row);
    assert_eq!(app.state.overlay(), Overlay::Detail);
}

#[test]
fn documents_scroll_by_page_and_toggle_wrapping() {
    let mut app = app();
    app.state.set_result(
        CommandResultViewModel::Text {
            title: "Topics".to_string(),
            body: (0..200).map(|line| format!("topic-{line}\n")).collect(),
        },
        ResultTone::Normal,
    );
    app.state.set_focus(FocusArea::Result);
    draw(&mut app);
    let rows = app.view.result_rows;

    press(&mut app, KeyCode::PageDown);
    assert_eq!(app.state.result().unwrap().top(), rows - 1);
    press(&mut app, KeyCode::Char(' '));
    assert_eq!(app.state.result().unwrap().top(), 2 * (rows - 1));
    press(&mut app, KeyCode::Char('G'));
    draw(&mut app);
    assert_eq!(app.state.result().unwrap().top(), 200 - rows);

    assert!(app.state.result().unwrap().wraps());
    press(&mut app, KeyCode::Char('w'));
    assert!(!app.state.result().unwrap().wraps());
    press(&mut app, KeyCode::Enter);
    assert_eq!(app.state.overlay(), Overlay::None, "a document has no row details");
}

#[test]
fn an_overlay_keeps_clicks_and_keys_away_from_the_panes() {
    let mut app = app();
    press(&mut app, KeyCode::Char('?'));
    assert_eq!(app.state.overlay(), Overlay::Help);
    for _ in 0..10 {
        app.state.advance_animation();
    }
    let screen = draw(&mut app);
    let selected = app.state.selected_command().id;

    // Inside the dialog nothing happens; outside it closes without reaching the pane below.
    click(&mut app, locate(&screen, "Keyboard and mouse"));
    assert_eq!(app.state.overlay(), Overlay::Help);
    click(&mut app, (2, 8));
    assert_eq!(app.state.overlay(), Overlay::None);
    assert_eq!(app.state.selected_command().id, selected);

    press(&mut app, KeyCode::F(1));
    press(&mut app, KeyCode::Char('q'));
    assert_eq!(app.state.overlay(), Overlay::None);
    assert!(!app.should_quit(), "q closes the help instead of quitting");
}

#[test]
fn a_confirmation_ignores_the_mouse() {
    let mut app = app();
    select(&mut app, "auth.user.update");
    app.state.form.set_value("username", "alice".to_string());
    ctrl(&mut app, 'r');
    let screen = draw(&mut app);

    click(&mut app, (2, 8));
    click(&mut app, locate(&screen, "Confirm command"));
    mouse(&mut app, MouseEventKind::ScrollDown, (2, 8), KeyModifiers::NONE);

    assert_eq!(app.state.overlay(), Overlay::Confirm);
    assert_eq!(app.state.selected_command().id, "auth.user.update");
}

#[test]
fn the_help_scrolls_within_its_content_on_a_short_terminal() {
    let mut app = app();
    press(&mut app, KeyCode::F(1));
    draw_sized(&mut app, 80, 14);
    let max = app.view.overlay_max_scroll;
    assert!(max > 0);

    for _ in 0..200 {
        press(&mut app, KeyCode::Down);
    }
    assert_eq!(app.state.overlay_scroll, max);
    press(&mut app, KeyCode::Home);
    assert_eq!(app.state.overlay_scroll, 0);
    press(&mut app, KeyCode::PageDown);
    assert_eq!(app.state.overlay_scroll, (app.view.overlay_rows - 1).min(max));
    mouse(&mut app, MouseEventKind::ScrollUp, (40, 7), KeyModifiers::NONE);
    assert!(app.state.overlay_scroll < (app.view.overlay_rows - 1).min(max));
}

#[test]
fn pasted_text_is_inserted_as_one_line_and_never_runs_a_command() {
    let mut app = app();
    select(&mut app, "topic.route");

    app.handle_event(&Event::Paste("ignored\n".to_string()));
    assert_eq!(
        app.state.form.raw_value("topic"),
        Some(""),
        "no input has focus in the tree"
    );

    app.state.set_focus(FocusArea::Args);
    app.handle_event(&Event::Paste("Topic\nA\n".to_string()));
    assert_eq!(app.state.form.raw_value("topic"), Some("Topic A"));
    assert_eq!(app.state.execution, CommandExecutionState::Idle);
    assert!(app.running_task.is_none());

    select(&mut app, "broker.config.update_apply");
    app.state.set_focus(FocusArea::Args);
    let entries = app
        .state
        .selected_command()
        .args
        .iter()
        .position(|arg| arg.name == "entries")
        .unwrap();
    app.state.focus_arg(entries);
    app.handle_event(&Event::Paste("a=1\r\nb=2\n".to_string()));
    assert_eq!(app.state.form.raw_value("entries"), Some("a=1; b=2"));
}

#[test]
fn mouse_capture_and_motion_can_be_switched_off() {
    let mut app = app();
    assert!(app.state.mouse_capture());
    assert!(app.state.motion().enabled());

    press(&mut app, KeyCode::F(2));
    assert!(!app.state.mouse_capture());
    assert_eq!(app.pending_mouse_capture, Some(false));
    assert!(toast(&app).contains("selects text"));

    press(&mut app, KeyCode::F(2));
    assert_eq!(app.pending_mouse_capture, Some(true));

    press(&mut app, KeyCode::F(3));
    assert!(!app.state.motion().enabled());
    assert_eq!(toast(&app), "Animations off");
}

#[test]
fn a_rejected_run_moves_to_the_first_invalid_parameter() {
    let mut app = app();
    select(&mut app, "topic.update");
    app.state.form.set_value("topic", "TopicA".to_string());

    press(&mut app, KeyCode::F(5));

    assert_eq!(app.state.focus, FocusArea::Args);
    assert_eq!(
        app.state.form.focused_arg(),
        2,
        "target is the first field without a value"
    );
    assert_eq!(toast(&app), "1 parameter needs attention");
    assert!(app.state.motion().invalid_at().is_some());
    assert_eq!(app.state.execution, CommandExecutionState::Idle);
}

#[test]
fn help_opens_from_a_text_field_with_f1_and_from_a_list_with_a_question_mark() {
    let mut app = app();
    app.apply_action(Action::FocusSearch);

    press(&mut app, KeyCode::Char('?'));
    assert_eq!(app.state.search, "?");
    assert_eq!(app.state.overlay(), Overlay::None);

    press(&mut app, KeyCode::F(1));
    assert_eq!(app.state.overlay(), Overlay::Help);
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.state.overlay(), Overlay::None);
    assert_eq!(app.state.focus, FocusArea::Search);
}

#[test]
fn the_confirmation_input_edits_at_the_cursor_and_clears_the_mismatch_report() {
    let mut app = app();
    select(&mut app, "auth.user.update");
    app.state.form.set_value("username", "alice".to_string());
    ctrl(&mut app, 'r');

    type_text(&mut app, "cnfirm");
    press(&mut app, KeyCode::Enter);
    assert!(app.state.last_error.is_some());
    assert!(app.state.motion().invalid_at().is_some());

    press(&mut app, KeyCode::Home);
    press(&mut app, KeyCode::Right);
    type_text(&mut app, "o");
    assert_eq!(app.state.confirm_input, "confirm");
    assert!(app.state.last_error.is_none());
    assert_eq!(app.state.input_cursor(InputTarget::Confirm), 2);
}

#[test]
fn cancelling_a_confirmation_reports_that_nothing_was_sent() {
    let mut app = app();
    select(&mut app, "auth.user.update");
    app.state.form.set_value("username", "alice".to_string());
    ctrl(&mut app, 'r');

    ctrl(&mut app, 'c');

    assert!(matches!(app.state.execution, CommandExecutionState::Cancelled { .. }));
    assert_eq!(toast(&app), "Confirmation cancelled. Nothing was sent.");
    assert!(app.state.run_duration().is_none());
    assert!(!app.should_quit());
}

#[test]
fn key_releases_and_unrelated_events_are_ignored() {
    let mut app = app();
    app.apply_action(Action::FocusSearch);
    let mut release = KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE);
    release.kind = ratatui::crossterm::event::KeyEventKind::Release;

    app.handle_event(&Event::Key(release));
    app.handle_event(&Event::Resize(80, 24));
    app.handle_event(&Event::FocusLost);

    assert_eq!(app.state.search, "");
}
