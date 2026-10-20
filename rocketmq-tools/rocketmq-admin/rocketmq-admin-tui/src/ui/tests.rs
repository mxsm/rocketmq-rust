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
use ratatui::buffer::Buffer;
use ratatui::Terminal;

use super::*;
use crate::result_view::ResultTone;
use crate::state::CommandExecutionState;
use crate::state::ToastLevel;
use crate::view_model::CommandResultViewModel;
use crate::view_model::OperationSummaryViewModel;
use crate::view_model::TableViewModel;

fn draw(state: &mut AppState, view: &mut ViewCache, width: u16, height: u16) -> Terminal<TestBackend> {
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal.draw(|frame| render(frame, state, view)).unwrap();
    terminal
}

/// Advances the clock until every transition that started so far has finished.
fn settle(state: &mut AppState) {
    for _ in 0..90 {
        state.advance_animation();
        state.touch();
    }
}

fn settled() -> AppState {
    let mut state = AppState::new(Some("127.0.0.1:9876"));
    settle(&mut state);
    state
}

fn symbols(buffer: &Buffer, y: u16) -> Vec<&str> {
    (0..buffer.area.width).map(|x| buffer[(x, y)].symbol()).collect()
}

fn screen(buffer: &Buffer) -> String {
    (0..buffer.area.height)
        .map(|y| symbols(buffer, y).concat())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Returns the cell at which `needle` starts.
fn find(buffer: &Buffer, needle: &str) -> Option<(u16, u16)> {
    (0..buffer.area.height).find_map(|y| {
        let row = symbols(buffer, y);
        (0..row.len())
            .find(|x| row[*x..].concat().starts_with(needle))
            .map(|x| (x as u16, y))
    })
}

fn table(rows: usize, columns: usize) -> CommandResultViewModel {
    CommandResultViewModel::Table(TableViewModel {
        title: "Consumer Progress".to_string(),
        headers: (0..columns).map(|column| format!("column-{column}")).collect(),
        rows: (0..rows)
            .map(|row| (0..columns).map(|column| format!("value-{row}-{column}")).collect())
            .collect(),
    })
}

fn document(lines: usize) -> CommandResultViewModel {
    CommandResultViewModel::Text {
        title: "Topics".to_string(),
        body: (0..lines)
            .map(|line| format!("line {line} with enough words to wrap in a narrow pane"))
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

fn select(state: &mut AppState, command_id: &str) {
    state.set_search(command_id.to_string());
    assert_eq!(state.selected_command().id, command_id);
}

fn confirming(state: &mut AppState, command_id: &str, expected: &str) {
    state.execution = CommandExecutionState::Confirming {
        execution_id: 7,
        command_id: command_id.to_string(),
        expected: expected.to_string(),
    };
}

fn succeeded(state: &mut AppState, result: CommandResultViewModel) {
    state.set_result(result, ResultTone::Normal);
    state.execution = CommandExecutionState::Succeeded {
        execution_id: 7,
        command_id: "topic.list".to_string(),
    };
}

#[test]
fn layout_splits_wide_terminals_and_stacks_narrow_ones() {
    let mut state = AppState::new(None);

    let wide = frames(Rect::new(0, 0, 120, 40), &state);
    let (sidebar, command, result) = (wide.sidebar.unwrap(), wide.command.unwrap(), wide.result.unwrap());
    assert_eq!(wide.header, Rect::new(0, 0, 120, 2));
    assert_eq!(wide.footer, Rect::new(0, 39, 120, 1));
    assert_eq!(sidebar, Rect::new(0, 2, 36, 37));
    assert_eq!((command.x, command.width), (36, 84));
    assert_eq!(command.bottom(), result.y);
    assert_eq!(command.height + result.height, 37);

    // A narrow terminal shows the list or the workspace, whichever has focus.
    let narrow = frames(Rect::new(0, 0, 80, 24), &state);
    assert_eq!(narrow.sidebar, Some(Rect::new(0, 2, 80, 21)));
    assert_eq!((narrow.command, narrow.result), (None, None));

    state.focus = FocusArea::Args;
    let narrow = frames(Rect::new(0, 0, 80, 24), &state);
    assert_eq!(narrow.sidebar, None);
    assert_eq!(narrow.command.unwrap().width, 80);
    assert!(narrow.result.is_some());

    state.result_zoom = true;
    let zoomed = frames(Rect::new(0, 0, 120, 40), &state);
    assert_eq!((zoomed.sidebar, zoomed.command), (None, None));
    assert_eq!(zoomed.result, Some(Rect::new(0, 2, 120, 37)));
}

#[test]
fn sidebar_width_stays_within_its_bounds() {
    let state = AppState::new(None);

    assert_eq!(
        frames(Rect::new(0, 0, 96, 30), &state).sidebar.unwrap().width,
        SIDEBAR_MIN_WIDTH
    );
    assert_eq!(
        frames(Rect::new(0, 0, 300, 30), &state).sidebar.unwrap().width,
        SIDEBAR_MAX_WIDTH
    );
    assert!(frames(Rect::new(0, 0, u16::MAX, 30), &state).sidebar.is_some());
}

#[test]
fn command_pane_height_follows_the_form_and_leaves_room_for_results() {
    assert_eq!(command_height(37, 0), 5);
    assert_eq!(command_height(37, 1), 5);
    assert_eq!(command_height(37, 9), 13);
    assert_eq!(command_height(37, 40), 29);
    assert_eq!(command_height(10, 9), 5);
    assert_eq!(command_height(3, 9), 3);
    assert_eq!(command_height(0, 2), 0);
}

#[test]
fn the_main_screen_shows_every_pane() {
    let mut state = settled();
    let terminal = draw(&mut state, &mut ViewCache::default(), 120, 36);
    let screen = screen(terminal.backend().buffer());

    for expected in [
        "RocketMQ Admin",
        "NameServer",
        "127.0.0.1:9876",
        "Commands",
        "Search commands",
        "List Topics",
        "SAFE",
        "topic.list",
        "Result",
        "Ready when you are",
        "COMMANDS",
        "F1",
    ] {
        assert!(screen.contains(expected), "missing {expected:?} in\n{screen}");
    }
}

#[test]
fn a_terminal_that_is_too_small_explains_itself() {
    let mut state = settled();
    let mut view = ViewCache::default();
    let terminal = draw(&mut state, &mut view, 40, 8);
    let screen = screen(terminal.backend().buffer());

    assert!(screen.contains("Terminal too small"), "{screen}");
    assert!(screen.contains("needs 48x12, has 40x8"), "{screen}");
    assert_eq!(view.target_at(0, 0), None);
}

#[test]
fn rendering_never_panics_at_any_size_in_any_state() {
    type Setup = fn(&mut AppState);
    let setups: [Setup; 20] = [
        |_| {},
        |state| state.focus = FocusArea::Namesrv,
        |state| {
            state.focus = FocusArea::Search;
            state.set_search("no such command anywhere".to_string());
        },
        |state| {
            state.focus = FocusArea::Search;
            state.set_search("topic".to_string());
        },
        |state| {
            select(state, "topic.update");
            state.focus = FocusArea::Args;
            state.validate_selected_form();
            state.flag_invalid();
        },
        |state| {
            select(state, "consumer.update_subscription_group");
            state.focus = FocusArea::Args;
            state.focus_arg(usize::MAX);
        },
        |state| state.focus = FocusArea::Result,
        |state| state.show_help = true,
        |state| {
            select(state, "auth.user.delete");
            state.form.set_value("username", "admin-user".to_string());
            confirming(state, "auth.user.delete", "admin-user");
            state.confirm_input = "admin-x".to_string();
            state.last_error = Some("confirmation must match 'admin-user'".to_string());
        },
        |state| {
            state.execution = CommandExecutionState::Running {
                execution_id: 3,
                command_id: "topic.list".to_string(),
            };
            state.mark_run_started();
            state.progress_message = Some("pulled 12 messages".to_string());
        },
        |state| {
            succeeded(state, table(120, 14));
            state.focus = FocusArea::Result;
            if let Some(result) = state.result_mut() {
                result.move_rows(57);
                result.move_columns(3);
            }
        },
        |state| {
            succeeded(state, table(0, 3));
        },
        |state| {
            succeeded(state, table(4, 3));
            state.focus = FocusArea::Result;
            state.open_detail();
        },
        |state| {
            succeeded(state, document(200));
            state.focus = FocusArea::Result;
            state.result_zoom = true;
            if let Some(result) = state.result_mut() {
                result.toggle_wrap();
                result.move_columns(4);
                result.move_to_end();
            }
        },
        |state| {
            succeeded(
                state,
                CommandResultViewModel::Json {
                    title: "Config".to_string(),
                    body: "{\n  \"名称\": \"值\",\n  \"n\": 1\n}".to_string(),
                },
            );
        },
        |state| {
            succeeded(
                state,
                CommandResultViewModel::OperationSummary(OperationSummaryViewModel {
                    title: "Update".to_string(),
                    success_count: 2,
                    failure_count: 1,
                    targets: vec!["broker-a".to_string(), "broker-b".to_string()],
                    errors: vec!["broker-c: connection refused".to_string()],
                }),
            );
        },
        |state| {
            state.set_result(
                CommandResultViewModel::error("Command Failed", "connect to 127.0.0.1:9876 failed"),
                ResultTone::Failure,
            );
            state.execution = CommandExecutionState::Failed {
                execution_id: 7,
                command_id: "topic.list".to_string(),
            };
        },
        |state| {
            state.execution = CommandExecutionState::Cancelled {
                execution_id: 7,
                command_id: "topic.list".to_string(),
            };
            state.notify(
                ToastLevel::Warning,
                "Cancelled locally. A request the server accepted is not undone.",
            );
        },
        |state| {
            state.collapse_all_categories();
            state.set_mouse_capture(false);
            state.toggle_motion();
            state.notify(ToastLevel::Info, "Animations off");
        },
        |state| {
            state.namesrv_addr = "主题服务器.example.internal:9876;10.0.0.2:9876;10.0.0.3:9876".to_string();
            state.focus = FocusArea::Namesrv;
            state.set_input_cursor(crate::state::InputTarget::Namesrv, 3);
        },
    ];
    let widths = [0, 1, 2, 7, 30, 47, 48, 49, 64, 80, 95, 96, 97, 120, 160, 240];
    let heights = [0, 1, 2, 6, 11, 12, 13, 16, 24, 40, 70];

    for setup in setups {
        let mut state = AppState::new(Some("127.0.0.1:9876"));
        let mut view = ViewCache::new(ColorDepth::TrueColor);
        setup(&mut state);
        // Cover the first frame, the middle of the transitions, and the settled screen.
        for ticks in [1, 4, 80] {
            for _ in 0..ticks {
                state.advance_animation();
            }
            view.set_hover(Some(Position::new(40, 9)));
            for width in widths {
                for height in heights {
                    draw(&mut state, &mut view, width, height);
                }
            }
        }
    }
}

#[test]
fn clickable_regions_are_registered_where_they_are_drawn() {
    let mut state = settled();
    select(&mut state, "topic.update");
    state.set_search(String::new());
    let mut view = ViewCache::default();
    let terminal = draw(&mut state, &mut view, 120, 36);
    let buffer = terminal.backend().buffer();
    let at = |needle: &str| {
        let (x, y) = find(buffer, needle).unwrap_or_else(|| panic!("{needle:?} is not on screen"));
        view.target_at(x, y)
    };

    assert_eq!(at("NameServer"), Some(HitTarget::Namesrv));
    assert_eq!(at("127.0.0.1:9876"), Some(HitTarget::Namesrv));
    assert_eq!(at("Search commands"), Some(HitTarget::Search));
    assert_eq!(at("List Topics"), Some(HitTarget::TreeRow(1)));
    assert_eq!(at("Target Type"), Some(HitTarget::ArgRow(1)));
    assert_eq!(at(" cluster "), Some(HitTarget::ArgChoice { arg: 1, choice: 1 }));
    assert_eq!(at("Run"), Some(HitTarget::RunButton));
    assert_eq!(at("Ready when you are"), Some(HitTarget::Result));
    assert_eq!(view.target_at(0, 0), None, "the brand is not interactive");
}

#[test]
fn an_overlay_covers_every_pane_target() {
    let mut state = settled();
    state.show_help = true;
    settle(&mut state);
    let mut view = ViewCache::default();
    let terminal = draw(&mut state, &mut view, 120, 36);
    let buffer = terminal.backend().buffer();
    let screen = screen(buffer);

    assert!(screen.contains("Keyboard and mouse"), "{screen}");
    assert!(screen.contains("Ctrl+R"), "{screen}");
    assert!(screen.contains("Risk levels"), "{screen}");
    assert_eq!(view.target_at(60, 18), Some(HitTarget::Overlay));
    assert_eq!(view.target_at(1, 4), Some(HitTarget::Backdrop));
    assert_eq!(view.overlay_max_scroll, 0, "the help fits a 36-row terminal");

    // A short terminal scrolls the help instead of cutting it off.
    draw(&mut state, &mut view, 80, 14);
    assert!(view.overlay_max_scroll > 0);
    assert!(view.overlay_rows > 0);
}

#[test]
fn the_backdrop_behind_an_overlay_is_dimmed() {
    let mut state = settled();
    let plain = draw(&mut state, &mut ViewCache::default(), 120, 36);
    let brand = plain.backend().buffer()[(1, 0)].fg;

    state.show_help = true;
    settle(&mut state);
    let covered = draw(&mut state, &mut ViewCache::default(), 120, 36);
    let dimmed = covered.backend().buffer()[(1, 0)].fg;

    assert_ne!(brand, dimmed);
    assert_eq!(dimmed, theme::mix(brand, theme::BACKGROUND, 0.6));
}

#[test]
fn the_text_cursor_follows_the_focused_input() {
    let mut state = settled();
    let mut view = ViewCache::default();
    let terminal = draw(&mut state, &mut view, 120, 36);
    assert!(
        !terminal.backend().cursor_visible(),
        "no input has focus in the command list"
    );

    state.focus = FocusArea::Search;
    state.set_search("topic".to_string());
    let mut terminal_with_search = draw(&mut state, &mut view, 120, 36);
    let (x, y) = find(terminal_with_search.backend().buffer(), "topic").unwrap();
    terminal_with_search
        .backend_mut()
        .assert_cursor_position(Position::new(x + 5, y));

    // The cursor moves inside the text when it is not at the end.
    state.set_input_cursor(crate::state::InputTarget::Search, 2);
    let mut moved = draw(&mut state, &mut view, 120, 36);
    moved.backend_mut().assert_cursor_position(Position::new(x + 2, y));

    state.focus = FocusArea::Args;
    select(&mut state, "topic.route");
    state.form.set_value("topic", "TopicA".to_string());
    let mut with_field = draw(&mut state, &mut view, 120, 36);
    let (x, y) = find(with_field.backend().buffer(), "TopicA").unwrap();
    with_field.backend_mut().assert_cursor_position(Position::new(x + 6, y));
}

#[test]
fn secrets_are_masked_in_the_form_and_in_the_confirmation() {
    let mut state = settled();
    select(&mut state, "auth.user.create");
    state.form.set_value("username", "alice".to_string());
    state.form.set_value("password", "hunter2-secret".to_string());
    state.focus = FocusArea::Args;
    state.focus_arg(3);

    let form = draw(&mut state, &mut ViewCache::default(), 120, 36);
    let form = screen(form.backend().buffer());
    assert!(form.contains("alice"), "{form}");
    assert!(!form.contains("hunter2"), "{form}");
    assert!(form.contains(&theme::GLYPH_MASK.repeat(14)), "{form}");

    confirming(&mut state, "auth.user.create", "confirm");
    settle(&mut state);
    let dialog = draw(&mut state, &mut ViewCache::default(), 120, 36);
    let dialog = screen(dialog.backend().buffer());
    assert!(dialog.contains("Confirm command"), "{dialog}");
    assert!(dialog.contains("MUTATING"), "{dialog}");
    assert!(!dialog.contains("hunter2"), "{dialog}");
}

#[test]
fn a_dangerous_confirmation_shows_what_will_run_and_what_to_type() {
    let mut state = settled();
    select(&mut state, "auth.user.delete");
    state.form.set_value("username", "admin-user".to_string());
    confirming(&mut state, "auth.user.delete", "admin-user");
    state.confirm_input = "admin".to_string();
    settle(&mut state);

    let mut view = ViewCache::default();
    let mut terminal = draw(&mut state, &mut view, 120, 36);
    let screen = screen(terminal.backend().buffer());

    for expected in [
        "Confirm dangerous command",
        "DANGEROUS",
        "auth.user.delete",
        "Username",
        "to execute dangerous command",
        "5/10",
    ] {
        assert!(screen.contains(expected), "missing {expected:?} in\n{screen}");
    }
    let (x, y) = find(terminal.backend().buffer(), "admin ").unwrap();
    terminal.backend_mut().assert_cursor_position(Position::new(x + 5, y));
    assert_eq!(terminal.backend().buffer()[(x, y)].fg, theme::SUCCESS);
}

#[test]
fn a_table_result_shows_its_header_rows_and_position() {
    let mut state = settled();
    succeeded(&mut state, table(40, 4));
    state.focus = FocusArea::Result;
    settle(&mut state);
    let mut view = ViewCache::default();

    let terminal = draw(&mut state, &mut view, 120, 36);
    let screen = screen(terminal.backend().buffer());
    assert!(screen.contains("Consumer Progress"), "{screen}");
    assert!(screen.contains("column-0"), "{screen}");
    assert!(screen.contains("value-0-3"), "{screen}");
    assert!(screen.contains("row 1/40"), "{screen}");
    assert!(view.result_rows > 0);

    let (x, y) = find(terminal.backend().buffer(), "value-2-0").unwrap();
    assert_eq!(view.target_at(x, y), Some(HitTarget::ResultRow(2)));
    let selected = find(terminal.backend().buffer(), "value-0-0").unwrap();
    assert_eq!(terminal.backend().buffer()[selected].bg, theme::SELECTION);
    assert_ne!(terminal.backend().buffer()[(x, y)].bg, theme::SELECTION);
}

#[test]
fn the_result_viewport_is_clamped_to_the_pane_while_rendering() {
    let mut state = settled();
    succeeded(&mut state, document(100));
    if let Some(result) = state.result_mut() {
        result.toggle_wrap();
        result.move_to_end();
    }
    settle(&mut state);
    let mut view = ViewCache::default();

    let terminal = draw(&mut state, &mut view, 120, 36);
    let result = state.result().unwrap();

    assert_eq!(result.row_count(), 100);
    assert_eq!(result.top(), 100 - view.result_rows);
    let screen = screen(terminal.backend().buffer());
    assert!(screen.contains("line 99 "), "{screen}");
    assert!(screen.contains("wrap off"), "{screen}");
}

#[test]
fn list_viewports_follow_the_cursor() {
    let mut state = settled();
    state.set_tree_cursor(usize::MAX);
    let mut view = ViewCache::default();

    let terminal = draw(&mut state, &mut view, 120, 20);

    assert!(view.tree_top > 0);
    assert_eq!(view.tree_top + view.tree_rows, state.visible_tree_items().len());
    let title = state.selected_command().title;
    assert!(screen(terminal.backend().buffer()).contains(title));

    // Moving one row up inside the window does not scroll it.
    let top = view.tree_top;
    state.move_tree_cursor(-3);
    draw(&mut state, &mut view, 120, 20);
    assert_eq!(view.tree_top, top);
}

#[test]
fn indexed_terminals_receive_no_rgb_colors() {
    let mut state = settled();
    succeeded(&mut state, table(5, 3));
    state.notify(ToastLevel::Success, "done");
    let mut view = ViewCache::new(ColorDepth::Indexed);

    let terminal = draw(&mut state, &mut view, 120, 36);

    assert!(terminal
        .backend()
        .buffer()
        .content
        .iter()
        .all(|cell| !matches!(cell.fg, Color::Rgb(..)) && !matches!(cell.bg, Color::Rgb(..))));
}

#[test]
fn the_intro_reveals_panes_in_order_and_then_leaves_them_alone() {
    let mut state = AppState::new(None);
    state.advance_animation();
    let first = draw(&mut state, &mut ViewCache::default(), 120, 36);
    // The key bar is the last stage, so its mode chip is still invisible.
    assert_eq!(first.backend().buffer()[(1, 35)].bg, theme::BACKGROUND);
    assert_eq!(first.backend().buffer()[(1, 35)].symbol(), "C");

    settle(&mut state);
    let settled = draw(&mut state, &mut ViewCache::default(), 120, 36);
    assert_eq!(settled.backend().buffer()[(1, 35)].bg, theme::PRIMARY_DEEP);
}

#[test]
fn switching_motion_off_draws_the_settled_frame_immediately() {
    let mut state = AppState::new(None);
    state.toggle_motion();
    state.advance_animation();

    let terminal = draw(&mut state, &mut ViewCache::default(), 120, 36);

    assert_eq!(terminal.backend().buffer()[(1, 35)].bg, theme::PRIMARY_DEEP);
    assert!(screen(terminal.backend().buffer()).contains("motion off"));
}

#[test]
fn focus_lights_the_border_of_the_focused_pane_only() {
    let mut state = settled();
    state.advance_animation();
    // Idle long enough that the travelling highlight has stopped.
    for _ in 0..(30 * 30) {
        state.advance_animation();
    }
    let terminal = draw(&mut state, &mut ViewCache::default(), 120, 36);
    let buffer = terminal.backend().buffer();

    // The sidebar starts at column 0 and the command pane at column 36, both on row 2.
    assert_eq!(buffer[(0, 2)].symbol(), "╭");
    assert_eq!(buffer[(0, 2)].fg, theme::PRIMARY);
    assert_eq!(buffer[(36, 2)].symbol(), "╭");
    assert_eq!(buffer[(36, 2)].fg, theme::BORDER);
}

#[test]
fn a_toast_is_drawn_above_the_key_bar() {
    let mut state = settled();
    state.notify(ToastLevel::Success, "NameServer set to 127.0.0.1:9876");
    for _ in 0..20 {
        state.advance_animation();
    }

    let terminal = draw(&mut state, &mut ViewCache::default(), 120, 36);
    let (_, y) = find(terminal.backend().buffer(), "NameServer set to").unwrap();

    assert_eq!(y, 33);
}
