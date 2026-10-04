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

//! Keyboard, mouse, and paste handling.
//!
//! Input is resolved in layers: an open overlay first, then the keys that work
//! everywhere, then text editing for the focused input, then navigation.

use crossterm::event::Event;
use crossterm::event::KeyCode;
use crossterm::event::KeyEvent;
use crossterm::event::KeyEventKind;
use crossterm::event::KeyModifiers;
use crossterm::event::MouseButton;
use crossterm::event::MouseEvent;
use crossterm::event::MouseEventKind;
use ratatui::layout::Position;

use super::RocketmqTuiApp;
use crate::action::Action;
use crate::commands::ArgKind;
use crate::event::is_ctrl;
use crate::event::key_char;
use crate::state::CommandExecutionState;
use crate::state::CommandTreeItem;
use crate::state::FocusArea;
use crate::state::InputTarget;
use crate::state::Overlay;
use crate::state::ToastLevel;
use crate::text_input;
use crate::text_input::TextEdit;
use crate::ui::HitTarget;

/// Rows scrolled per wheel notch.
const WHEEL_ROWS: isize = 3;

impl RocketmqTuiApp {
    pub(super) fn handle_event(&mut self, event: &Event) {
        match event {
            Event::Key(key) if key.kind == KeyEventKind::Press => self.handle_key_event(*key),
            Event::Mouse(mouse) => self.handle_mouse_event(*mouse),
            Event::Paste(text) => self.handle_paste(text),
            _ => {}
        }
    }

    pub(super) fn handle_key_event(&mut self, key: KeyEvent) {
        self.state.touch();
        // The pointer highlight belongs to the mouse; typing hides it.
        self.view.set_hover(None);

        match self.state.overlay() {
            Overlay::Help => return self.handle_help_key(key),
            Overlay::Confirm => return self.handle_confirmation_key(key),
            Overlay::Detail => return self.handle_detail_key(key),
            Overlay::None => {}
        }
        if self.handle_global_key(key) {
            return;
        }
        if self.state.active_input().is_some() && self.handle_text_key(key) {
            return;
        }
        self.handle_navigation_key(key);
    }

    /// Handles the keys that mean the same thing in every pane.
    fn handle_global_key(&mut self, key: KeyEvent) -> bool {
        if is_ctrl(&key, 'c') {
            self.cancel_or_quit();
        } else if is_ctrl(&key, 'q') {
            self.apply_action(Action::Quit);
        } else if is_ctrl(&key, 'l') {
            self.apply_action(Action::ResultCleared);
        } else if is_ctrl(&key, 'r') {
            self.apply_action(Action::ExecuteRequested);
        } else if is_ctrl(&key, 'f') {
            // A text field types `/` instead of acting on it, so the search needs a chord.
            self.apply_action(Action::FocusSearch);
        } else {
            match key.code {
                KeyCode::F(1) => self.apply_action(Action::HelpToggled),
                KeyCode::F(2) => self.toggle_mouse_capture(),
                KeyCode::F(3) => self.toggle_motion(),
                KeyCode::F(5) => self.apply_action(Action::ExecuteRequested),
                KeyCode::Esc => self.handle_escape(),
                KeyCode::Tab => self.apply_action(Action::FocusNext),
                KeyCode::BackTab => self.apply_action(Action::FocusPrevious),
                KeyCode::Enter => self.handle_enter(),
                _ => return false,
            }
        }
        true
    }

    /// Applies an editing key to the active input, returning whether the key was one.
    fn handle_text_key(&mut self, key: KeyEvent) -> bool {
        let control = key.modifiers.contains(KeyModifiers::CONTROL);
        let edit = match key.code {
            KeyCode::Backspace if control => TextEdit::DeleteWord,
            KeyCode::Backspace => TextEdit::Backspace,
            KeyCode::Delete => TextEdit::Delete,
            KeyCode::Left if control => TextEdit::WordLeft,
            KeyCode::Right if control => TextEdit::WordRight,
            KeyCode::Left => TextEdit::Left,
            KeyCode::Right => TextEdit::Right,
            KeyCode::Home => TextEdit::Home,
            KeyCode::End => TextEdit::End,
            KeyCode::Char('u') if control => TextEdit::ClearBefore,
            KeyCode::Char('k') if control => TextEdit::ClearAfter,
            KeyCode::Char('w') if control => TextEdit::DeleteWord,
            KeyCode::Char('a') if control => TextEdit::Home,
            KeyCode::Char('e') if control => TextEdit::End,
            _ => match key_char(&key) {
                Some(character) => TextEdit::Insert(character),
                None => return false,
            },
        };
        self.edit_input(edit);
        true
    }

    fn handle_navigation_key(&mut self, key: KeyEvent) {
        let control = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Up => self.move_rows(-1),
            KeyCode::Down => self.move_rows(1),
            KeyCode::Left => self.move_columns(-1),
            KeyCode::Right => self.move_columns(1),
            KeyCode::PageUp => self.move_pages(-1),
            KeyCode::PageDown => self.move_pages(1),
            KeyCode::Home => self.move_to_edge(false),
            KeyCode::End => self.move_to_edge(true),
            KeyCode::Char('d') if control && self.state.focus == FocusArea::Args => self.restore_form_defaults(),
            KeyCode::Char(' ') => self.handle_space(),
            KeyCode::Char(character)
                if !control && matches!(self.state.focus, FocusArea::CommandTree | FocusArea::Result) =>
            {
                self.handle_shortcut(character);
            }
            _ => {}
        }
    }

    /// Handles single-letter shortcuts, which only exist where no text is typed.
    fn handle_shortcut(&mut self, character: char) {
        let in_tree = self.state.focus == FocusArea::CommandTree;
        match character {
            '?' => self.apply_action(Action::HelpToggled),
            'q' => self.apply_action(Action::Quit),
            'n' => self.apply_action(Action::FocusNamesrv),
            '/' => self.apply_action(Action::FocusSearch),
            's' if in_tree => self.apply_action(Action::FocusSearch),
            'c' => self.state.set_focus(FocusArea::CommandTree),
            'p' => self.state.set_focus(FocusArea::Args),
            'r' => self.state.set_focus(FocusArea::Result),
            'j' => self.move_rows(1),
            'k' => self.move_rows(-1),
            'h' => self.move_columns(-1),
            'l' => self.move_columns(1),
            'g' => self.move_to_edge(false),
            'G' => self.move_to_edge(true),
            'z' => self.toggle_zoom(),
            'w' if !in_tree => {
                if let Some(result) = self.state.result_mut() {
                    result.toggle_wrap();
                }
            }
            '-' if in_tree => self.state.collapse_all_categories(),
            '+' | '=' if in_tree => self.state.expand_all_categories(),
            _ => {}
        }
    }

    fn handle_help_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc | KeyCode::Enter | KeyCode::F(1) | KeyCode::Char('q' | '?') => {
                self.apply_action(Action::HelpToggled);
            }
            _ if is_ctrl(&key, 'c') => self.apply_action(Action::HelpToggled),
            _ => self.scroll_overlay_with(key),
        }
    }

    fn handle_detail_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc | KeyCode::Enter | KeyCode::Char('q') => self.state.close_detail(),
            _ if is_ctrl(&key, 'c') => self.state.close_detail(),
            _ => self.scroll_overlay_with(key),
        }
    }

    fn scroll_overlay_with(&mut self, key: KeyEvent) {
        let page = self.view.overlay_rows.saturating_sub(1).max(1) as isize;
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => self.scroll_overlay(-1),
            KeyCode::Down | KeyCode::Char('j') => self.scroll_overlay(1),
            KeyCode::PageUp => self.scroll_overlay(-page),
            KeyCode::PageDown | KeyCode::Char(' ') => self.scroll_overlay(page),
            KeyCode::Home | KeyCode::Char('g') => self.state.overlay_scroll = 0,
            KeyCode::End | KeyCode::Char('G') => self.state.overlay_scroll = self.view.overlay_max_scroll,
            _ => {}
        }
    }

    fn scroll_overlay(&mut self, delta: isize) {
        let scroll = self.state.overlay_scroll.min(self.view.overlay_max_scroll);
        self.state.overlay_scroll = scroll.saturating_add_signed(delta).min(self.view.overlay_max_scroll);
    }

    fn handle_confirmation_key(&mut self, key: KeyEvent) {
        let CommandExecutionState::Confirming {
            execution_id,
            command_id,
            expected,
        } = self.state.execution.clone()
        else {
            return;
        };
        if key.code == KeyCode::Esc || is_ctrl(&key, 'c') {
            self.apply_action(Action::CancelExecution {
                execution_id,
                command_id,
            });
        } else if key.code == KeyCode::Enter {
            if self.state.confirm_input.trim() == expected {
                self.start_execution(execution_id, command_id);
            } else {
                self.state.last_error = Some(format!("confirmation must match '{expected}'"));
                self.state.flag_invalid();
            }
        } else {
            self.handle_text_key(key);
        }
    }

    /// Steps back one level: a running command, a zoomed result, an input, then the
    /// application itself, which takes a second press so one stray Esc never quits.
    fn handle_escape(&mut self) {
        if let CommandExecutionState::Running {
            execution_id,
            command_id,
        } = self.state.execution.clone()
        {
            self.apply_action(Action::CancelExecution {
                execution_id,
                command_id,
            });
            return;
        }
        if self.state.result_zoom {
            self.state.result_zoom = false;
            return;
        }

        match self.state.focus {
            FocusArea::Namesrv => {
                if let Some(previous) = self.state.take_namesrv_before_edit() {
                    self.apply_action(Action::NamesrvChanged(previous));
                }
                self.state.set_focus(FocusArea::CommandTree);
            }
            FocusArea::Search => {
                if !self.state.search.is_empty() {
                    self.apply_action(Action::SearchChanged(String::new()));
                }
                self.state.set_focus(FocusArea::CommandTree);
            }
            FocusArea::Args | FocusArea::Result => self.state.set_focus(FocusArea::CommandTree),
            FocusArea::CommandTree if !self.state.search.is_empty() => {
                self.apply_action(Action::SearchChanged(String::new()));
            }
            FocusArea::CommandTree => {
                if self.state.arm_quit() {
                    self.apply_action(Action::Quit);
                } else {
                    self.state.notify(ToastLevel::Info, "Press Esc again to quit");
                }
            }
        }
    }

    fn cancel_or_quit(&mut self) {
        match self.state.execution.clone() {
            CommandExecutionState::Running {
                execution_id,
                command_id,
            } => self.apply_action(Action::CancelExecution {
                execution_id,
                command_id,
            }),
            _ => self.apply_action(Action::Quit),
        }
    }

    fn handle_enter(&mut self) {
        match self.state.focus {
            FocusArea::CommandTree => self.open_focused_tree_item(),
            FocusArea::Namesrv => self.submit_namesrv_input(),
            FocusArea::Search => self.submit_search_input(),
            FocusArea::Args => self.apply_action(Action::ExecuteRequested),
            FocusArea::Result => {
                self.state.open_detail();
            }
        }
    }

    fn open_focused_tree_item(&mut self) {
        match self.state.focused_tree_item() {
            Some(CommandTreeItem::Category(_)) => self.state.toggle_focused_tree_category(),
            Some(CommandTreeItem::Command(_)) => self.state.set_focus(FocusArea::Args),
            None => {}
        }
    }

    fn handle_space(&mut self) {
        match self.state.focus {
            FocusArea::CommandTree => self.open_focused_tree_item(),
            FocusArea::Args => self.cycle_choice(false),
            FocusArea::Result => self.move_pages(1),
            FocusArea::Namesrv | FocusArea::Search => {}
        }
    }

    fn submit_namesrv_input(&mut self) {
        let namesrv_addr = self.state.namesrv_addr.trim().to_string();
        self.apply_action(Action::NamesrvChanged(namesrv_addr.clone()));
        self.state.last_error = None;
        if namesrv_addr.is_empty() {
            self.state.notify(ToastLevel::Warning, "NameServer address cleared");
        } else {
            self.state
                .notify(ToastLevel::Success, format!("NameServer set to {namesrv_addr}"));
        }
        self.state.set_focus(FocusArea::CommandTree);
    }

    fn submit_search_input(&mut self) {
        self.state.last_error = None;
        self.state.set_focus(FocusArea::CommandTree);
    }

    fn move_rows(&mut self, delta: isize) {
        match self.state.focus {
            FocusArea::CommandTree | FocusArea::Search => {
                self.state.move_tree_cursor(delta);
                self.emit_selected_command_action();
            }
            FocusArea::Args => self.state.move_arg_focus(delta),
            FocusArea::Result => {
                if let Some(result) = self.state.result_mut() {
                    result.move_rows(delta);
                }
            }
            FocusArea::Namesrv => {}
        }
    }

    fn move_columns(&mut self, delta: isize) {
        match self.state.focus {
            FocusArea::CommandTree if delta.is_negative() => self.state.collapse_focused_tree_category(),
            FocusArea::CommandTree => self.state.expand_focused_tree_category(),
            FocusArea::Args => self.cycle_choice(delta.is_negative()),
            FocusArea::Result => {
                if let Some(result) = self.state.result_mut() {
                    result.move_columns(delta);
                }
            }
            FocusArea::Namesrv | FocusArea::Search => {}
        }
    }

    fn move_pages(&mut self, pages: isize) {
        match self.state.focus {
            FocusArea::CommandTree | FocusArea::Search => {
                let page = self.view.tree_rows.saturating_sub(1).max(1) as isize;
                self.state.move_tree_cursor(pages.saturating_mul(page));
                self.emit_selected_command_action();
            }
            FocusArea::Args => self.move_to_edge(pages.is_positive()),
            FocusArea::Result => {
                let page = self.view.result_rows.saturating_sub(1).max(1) as isize;
                if let Some(result) = self.state.result_mut() {
                    result.move_rows(pages.saturating_mul(page));
                }
            }
            FocusArea::Namesrv => {}
        }
    }

    fn move_to_edge(&mut self, end: bool) {
        match self.state.focus {
            FocusArea::CommandTree | FocusArea::Search => {
                self.state.set_tree_cursor(if end { usize::MAX } else { 0 });
                self.emit_selected_command_action();
            }
            FocusArea::Args => self.state.focus_arg(if end { usize::MAX } else { 0 }),
            FocusArea::Result => {
                if let Some(result) = self.state.result_mut() {
                    if end {
                        result.move_to_end();
                    } else {
                        result.move_to_start();
                    }
                }
            }
            FocusArea::Namesrv => {}
        }
    }

    /// Switches the focused choice parameter: a boolean flips, an enumeration steps
    /// through its values in the given direction.
    fn cycle_choice(&mut self, reverse: bool) {
        let command = self.state.selected_command().clone();
        let Some(arg) = self.state.form.current_arg(&command) else {
            return;
        };
        match arg.kind {
            ArgKind::Bool { .. } => self.state.form.toggle_bool_current(&command),
            ArgKind::Enum { .. } => self.state.form.cycle_enum_current(&command, reverse),
            ArgKind::String { .. }
            | ArgKind::OptionalString { .. }
            | ArgKind::Number { .. }
            | ArgKind::KeyValueMap
            | ArgKind::TimestampMillis => {}
        }
    }

    fn restore_form_defaults(&mut self) {
        self.state.reset_form_for_selected_command();
        self.state
            .notify(ToastLevel::Info, "Parameters restored to their defaults");
    }

    fn toggle_zoom(&mut self) {
        let zoom = !self.state.result_zoom;
        self.state.set_focus(FocusArea::Result);
        self.state.result_zoom = zoom;
    }

    fn toggle_mouse_capture(&mut self) {
        let enabled = !self.state.mouse_capture();
        self.state.set_mouse_capture(enabled);
        self.pending_mouse_capture = Some(enabled);
        self.view.set_hover(None);
        self.state.notify(
            ToastLevel::Info,
            if enabled {
                "Mouse capture on"
            } else {
                "Mouse capture off: the terminal selects text"
            },
        );
    }

    fn toggle_motion(&mut self) {
        let enabled = self.state.toggle_motion();
        self.state.notify(
            ToastLevel::Info,
            if enabled { "Animations on" } else { "Animations off" },
        );
    }

    /// Applies `edit` to the active input and stores the result through its action.
    fn edit_input(&mut self, edit: TextEdit) {
        let Some(target) = self.state.active_input() else {
            return;
        };
        let mut value = self.state.input_value(target).to_string();
        let mut cursor = self.state.input_cursor(target);
        if text_input::apply(&mut value, &mut cursor, edit) {
            self.commit_input(target, value);
        }
        // Committing a search can reselect a command, which resets cursors; this one stays.
        self.state.set_input_cursor(target, cursor);
    }

    fn commit_input(&mut self, target: InputTarget, value: String) {
        match target {
            InputTarget::Namesrv => self.apply_action(Action::NamesrvChanged(value)),
            InputTarget::Search => self.apply_action(Action::SearchChanged(value)),
            InputTarget::Confirm => {
                self.state.confirm_input = value;
                // The mismatch report refers to the text that was just changed.
                self.state.last_error = None;
            }
            InputTarget::Arg(index) => {
                if let Some(name) = self.state.selected_command().args.get(index).map(|arg| arg.name) {
                    self.apply_action(Action::ArgChanged {
                        name: name.to_string(),
                        value,
                    });
                }
            }
        }
    }

    /// Inserts pasted text into the active input as a single line.
    fn handle_paste(&mut self, pasted: &str) {
        self.state.touch();
        let Some(target) = self.state.active_input() else {
            return;
        };
        // A key=value map takes several entries on one line; elsewhere lines join with a space.
        let separator = match target {
            InputTarget::Arg(index)
                if self
                    .state
                    .selected_command()
                    .args
                    .get(index)
                    .is_some_and(|arg| arg.kind == ArgKind::KeyValueMap) =>
            {
                "; "
            }
            _ => " ",
        };
        self.edit_input(TextEdit::InsertText(text_input::single_line(pasted, separator)));
    }

    pub(super) fn handle_mouse_event(&mut self, mouse: MouseEvent) {
        self.state.touch();
        self.view.set_hover(Some(Position::new(mouse.column, mouse.row)));
        let target = self.view.target_at(mouse.column, mouse.row);
        match mouse.kind {
            MouseEventKind::Down(MouseButton::Left) => self.handle_click(target),
            MouseEventKind::ScrollUp => self.handle_wheel(target, -1, mouse.modifiers),
            MouseEventKind::ScrollDown => self.handle_wheel(target, 1, mouse.modifiers),
            MouseEventKind::ScrollLeft => self.handle_wheel(target, -1, KeyModifiers::SHIFT),
            MouseEventKind::ScrollRight => self.handle_wheel(target, 1, KeyModifiers::SHIFT),
            MouseEventKind::Down(_) | MouseEventKind::Up(_) | MouseEventKind::Drag(_) | MouseEventKind::Moved => {}
        }
    }

    fn handle_click(&mut self, target: Option<HitTarget>) {
        let Some(target) = target else {
            return;
        };
        match self.state.overlay() {
            Overlay::None => {}
            Overlay::Help => {
                if target == HitTarget::Backdrop {
                    self.apply_action(Action::HelpToggled);
                }
                return;
            }
            Overlay::Detail => {
                if target == HitTarget::Backdrop {
                    self.state.close_detail();
                }
                return;
            }
            // A confirmation is only ever answered from the keyboard.
            Overlay::Confirm => return,
        }

        match target {
            HitTarget::Namesrv => self.apply_action(Action::FocusNamesrv),
            HitTarget::Search => self.apply_action(Action::FocusSearch),
            HitTarget::Tree => self.state.set_focus(FocusArea::CommandTree),
            HitTarget::TreeRow(position) => self.click_tree_row(position),
            HitTarget::Command => self.state.set_focus(FocusArea::Args),
            HitTarget::ArgRow(index) => {
                self.state.set_focus(FocusArea::Args);
                self.state.focus_arg(index);
            }
            HitTarget::ArgChoice { arg, choice } => self.click_choice(arg, choice),
            HitTarget::RunButton => self.click_run(),
            HitTarget::Result => self.state.set_focus(FocusArea::Result),
            HitTarget::ResultRow(row) => self.click_result_row(row),
            HitTarget::Overlay | HitTarget::Backdrop => {}
        }
    }

    /// Selects the clicked row; a group folds, and a second click on a command opens its form.
    fn click_tree_row(&mut self, position: usize) {
        let repeated = self.state.focus == FocusArea::CommandTree && self.state.tree_cursor() == position;
        self.state.set_focus(FocusArea::CommandTree);
        self.state.set_tree_cursor(position);
        match self.state.focused_tree_item() {
            Some(CommandTreeItem::Category(_)) => self.state.toggle_focused_tree_category(),
            Some(CommandTreeItem::Command(_)) if repeated => self.state.set_focus(FocusArea::Args),
            Some(CommandTreeItem::Command(_)) => self.emit_selected_command_action(),
            None => {}
        }
    }

    fn click_choice(&mut self, arg: usize, choice: usize) {
        self.state.set_focus(FocusArea::Args);
        self.state.focus_arg(arg);
        let chosen = self
            .state
            .selected_command()
            .args
            .get(arg)
            .and_then(|spec| Some((spec.name, *spec.kind.choices()?.get(choice)?)));
        if let Some((name, value)) = chosen {
            self.apply_action(Action::ArgChanged {
                name: name.to_string(),
                value: value.to_string(),
            });
        }
    }

    fn click_run(&mut self) {
        match self.state.execution.clone() {
            CommandExecutionState::Running {
                execution_id,
                command_id,
            } => self.apply_action(Action::CancelExecution {
                execution_id,
                command_id,
            }),
            _ => self.apply_action(Action::ExecuteRequested),
        }
    }

    /// Selects the clicked row; a second click on the selected row opens its details.
    fn click_result_row(&mut self, row: usize) {
        let was_focused = self.state.focus == FocusArea::Result;
        self.state.set_focus(FocusArea::Result);
        let repeated = self.state.result_mut().is_some_and(|result| result.select_row(row));
        if repeated && was_focused {
            self.state.open_detail();
        }
    }

    fn handle_wheel(&mut self, target: Option<HitTarget>, direction: isize, modifiers: KeyModifiers) {
        match self.state.overlay() {
            Overlay::None => {}
            Overlay::Help | Overlay::Detail => return self.scroll_overlay(direction * WHEEL_ROWS),
            Overlay::Confirm => return,
        }
        match target {
            Some(HitTarget::Search | HitTarget::Tree | HitTarget::TreeRow(_)) => {
                self.state.move_tree_cursor(direction);
                self.emit_selected_command_action();
            }
            Some(HitTarget::Command | HitTarget::ArgRow(_) | HitTarget::ArgChoice { .. } | HitTarget::RunButton) => {
                self.state.move_arg_focus(direction);
            }
            Some(HitTarget::Result | HitTarget::ResultRow(_)) => {
                let rows = self.view.result_rows;
                if let Some(result) = self.state.result_mut() {
                    if modifiers.contains(KeyModifiers::SHIFT) {
                        result.move_columns(direction);
                    } else {
                        result.scroll_viewport(direction * WHEEL_ROWS, rows);
                    }
                }
            }
            Some(HitTarget::Namesrv | HitTarget::Overlay | HitTarget::Backdrop) | None => {}
        }
    }
}
