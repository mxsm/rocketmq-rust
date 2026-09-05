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

use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use crossterm::event::Event;
use crossterm::event::EventStream;
use crossterm::event::KeyCode;
use crossterm::event::KeyEvent;
use crossterm::event::KeyEventKind;
use ratatui::DefaultTerminal;
use ratatui::Frame;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tokio_stream::StreamExt;

use crate::action::Action;
use crate::admin_facade::TuiAdminFacade;
use crate::commands::execute_command_with_progress;
use crate::event::is_ctrl;
use crate::event::key_char;
use crate::state::AppState;
use crate::state::CommandExecutionState;
use crate::state::CommandTreeItem;
use crate::state::FocusArea;

pub struct RocketmqTuiApp {
    admin_facade: TuiAdminFacade,
    should_quit: bool,
    state: AppState,
    action_tx: mpsc::Sender<QueuedAction>,
    action_rx: mpsc::Receiver<QueuedAction>,
    action_queue_diagnostics: Arc<ActionQueueDiagnostics>,
    running_task: Option<RunningCommandTask>,
}

const ACTION_QUEUE_CAPACITY: usize = 128;

#[derive(Default)]
struct ActionQueueDiagnostics {
    accepted: AtomicU64,
    rejected: AtomicU64,
    coalesced: AtomicU64,
    queue: Mutex<ActionQueueState>,
}

#[derive(Default)]
struct ActionQueueState {
    next_id: u64,
    entries: VecDeque<ActionQueueEntry>,
}

struct ActionQueueEntry {
    id: u64,
    bytes: usize,
    enqueued_at: Instant,
}

struct QueuedAction {
    id: u64,
    action: Action,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActionQueueSnapshot {
    pub capacity: usize,
    pub queued: usize,
    pub queued_bytes: usize,
    pub oldest_age_millis: Option<u64>,
    pub accepted: u64,
    pub rejected: u64,
    pub coalesced: u64,
}

struct RunningCommandTask {
    execution_id: u64,
    abort_handle: AbortHandle,
}

impl RocketmqTuiApp {
    pub fn new(client_runtime: std::sync::Arc<rocketmq_admin_core::client_adapter::ClientRuntime>) -> Self {
        Self::with_admin_facade(TuiAdminFacade::new(client_runtime))
    }

    pub fn with_admin_facade(admin_facade: TuiAdminFacade) -> Self {
        let (action_tx, action_rx) = mpsc::channel(ACTION_QUEUE_CAPACITY);
        let state = AppState::new(admin_facade.namesrv_addr());
        Self {
            admin_facade,
            should_quit: false,
            state,
            action_tx,
            action_rx,
            action_queue_diagnostics: Arc::new(ActionQueueDiagnostics::default()),
            running_task: None,
        }
    }

    #[allow(dead_code)]
    pub fn admin_facade(&self) -> &TuiAdminFacade {
        &self.admin_facade
    }

    pub fn should_quit(&self) -> bool {
        self.should_quit
    }

    pub fn quit(&mut self) {
        self.abort_running_task();
        self.should_quit = true;
    }

    pub fn action_queue_snapshot(&self) -> ActionQueueSnapshot {
        let (queued, queued_bytes, oldest_age_millis) = self.action_queue_diagnostics.snapshot_queue();
        ActionQueueSnapshot {
            capacity: self.action_tx.max_capacity(),
            queued,
            queued_bytes,
            oldest_age_millis,
            accepted: self.action_queue_diagnostics.accepted.load(Ordering::Relaxed),
            rejected: self.action_queue_diagnostics.rejected.load(Ordering::Relaxed),
            coalesced: self.action_queue_diagnostics.coalesced.load(Ordering::Relaxed),
        }
    }
}

impl ActionQueueDiagnostics {
    fn enqueue(&self, action: Action) -> QueuedAction {
        let mut queue = self.queue.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        queue.next_id = queue.next_id.wrapping_add(1).max(1);
        let id = queue.next_id;
        queue.entries.push_back(ActionQueueEntry {
            id,
            bytes: action.retained_bytes(),
            enqueued_at: Instant::now(),
        });
        QueuedAction { id, action }
    }

    fn dequeue(&self, id: u64) {
        let mut queue = self.queue.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(index) = queue.entries.iter().position(|entry| entry.id == id) {
            queue.entries.remove(index);
        }
    }

    fn snapshot_queue(&self) -> (usize, usize, Option<u64>) {
        let queue = self.queue.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        (
            queue.entries.len(),
            queue
                .entries
                .iter()
                .fold(0_usize, |total, entry| total.saturating_add(entry.bytes)),
            queue.entries.front().map(|entry| {
                u64::try_from(now.saturating_duration_since(entry.enqueued_at).as_millis()).unwrap_or(u64::MAX)
            }),
        )
    }
}

fn try_send_progress(
    sender: &mpsc::Sender<QueuedAction>,
    diagnostics: &ActionQueueDiagnostics,
    execution_id: u64,
    message: String,
) {
    match sender.try_reserve() {
        Ok(permit) => {
            permit.send(diagnostics.enqueue(Action::ProgressUpdated { execution_id, message }));
            diagnostics.accepted.fetch_add(1, Ordering::Relaxed);
        }
        Err(mpsc::error::TrySendError::Full(())) => {
            diagnostics.coalesced.fetch_add(1, Ordering::Relaxed);
        }
        Err(mpsc::error::TrySendError::Closed(())) => {
            diagnostics.rejected.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn send_required_action(
    sender: &mpsc::Sender<QueuedAction>,
    diagnostics: &ActionQueueDiagnostics,
    action: Action,
) {
    match sender.reserve().await {
        Ok(permit) => {
            permit.send(diagnostics.enqueue(action));
            diagnostics.accepted.fetch_add(1, Ordering::Relaxed);
        }
        Err(_) => {
            diagnostics.rejected.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl RocketmqTuiApp {
    const FRAMES_PER_SECOND: f32 = 30.0;

    pub async fn run(mut self, mut terminal: DefaultTerminal) -> anyhow::Result<()> {
        let period = Duration::from_secs_f32(1.0 / Self::FRAMES_PER_SECOND);
        let mut interval = tokio::time::interval(period);
        let mut events = EventStream::new();
        while !self.should_quit() {
            tokio::select! {
                _ = interval.tick() => {
                    self.state.advance_animation();
                    terminal.draw(|frame| self.draw(frame))?;
                },
                Some(Ok(event)) = events.next() => self.handle_event(&event),
                Some(queued) = self.action_rx.recv() => {
                    self.action_queue_diagnostics.dequeue(queued.id);
                    self.apply_action(queued.action);
                },
            }
        }
        let queue = self.action_queue_snapshot();
        tracing::debug!(
            capacity = queue.capacity,
            queued = queue.queued,
            queued_bytes = queue.queued_bytes,
            oldest_age_millis = queue.oldest_age_millis,
            accepted = queue.accepted,
            rejected = queue.rejected,
            coalesced = queue.coalesced,
            "RocketMQ admin TUI action queue stopped"
        );
        Ok(())
    }

    fn handle_event(&mut self, event: &Event) {
        if let Event::Key(key) = event {
            if key.kind == KeyEventKind::Press {
                self.handle_key_event(*key);
            }
        }
    }

    fn handle_key_event(&mut self, key: KeyEvent) {
        if self.state.show_help {
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc | KeyCode::Char('?') => {
                    self.apply_action(Action::HelpToggled);
                }
                _ => {}
            }
            return;
        }

        if matches!(self.state.execution, CommandExecutionState::Confirming { .. }) {
            self.handle_confirmation_key(key);
            return;
        }

        if is_ctrl(&key, 'l') {
            self.apply_action(Action::ResultCleared);
            return;
        }
        if is_ctrl(&key, 'r') {
            self.apply_action(Action::ExecuteRequested);
            return;
        }

        match key.code {
            KeyCode::Char('?') => self.apply_action(Action::HelpToggled),
            KeyCode::Char('q') => self.apply_action(Action::Quit),
            KeyCode::Esc => self.handle_escape(),
            KeyCode::Tab => self.apply_action(Action::FocusNext),
            KeyCode::BackTab => self.apply_action(Action::FocusPrevious),
            KeyCode::Char('n') if self.state.focus != FocusArea::Args => self.apply_action(Action::FocusNamesrv),
            KeyCode::Char('/') if self.state.focus != FocusArea::Args => self.apply_action(Action::FocusSearch),
            KeyCode::Char('s') if self.state.focus == FocusArea::CommandTree => self.apply_action(Action::FocusSearch),
            KeyCode::Enter => self.handle_enter(),
            KeyCode::Down | KeyCode::Char('j') => self.move_down(),
            KeyCode::Up | KeyCode::Char('k') => self.move_up(),
            KeyCode::Left => self.move_left(),
            KeyCode::Right => self.move_right(),
            KeyCode::Backspace => self.handle_backspace(),
            KeyCode::Char(' ') => self.handle_space(),
            _ => {
                if let Some(value) = key_char(&key) {
                    self.handle_char(value);
                }
            }
        }
    }

    fn handle_confirmation_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                if let CommandExecutionState::Confirming {
                    execution_id,
                    command_id,
                    ..
                } = self.state.execution.clone()
                {
                    self.apply_action(Action::CancelExecution {
                        execution_id,
                        command_id,
                    });
                }
            }
            KeyCode::Enter => {
                if let CommandExecutionState::Confirming {
                    execution_id,
                    command_id,
                    expected,
                } = self.state.execution.clone()
                {
                    if self.state.confirm_input.trim() == expected {
                        self.start_execution(execution_id, command_id);
                    } else {
                        self.state.last_error = Some(format!("confirmation must match '{expected}'"));
                    }
                }
            }
            KeyCode::Backspace => {
                self.state.confirm_input.pop();
            }
            _ => {
                if let Some(value) = key_char(&key) {
                    self.state.confirm_input.push(value);
                }
            }
        }
    }

    fn handle_escape(&mut self) {
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
            FocusArea::CommandTree => match self.state.focused_tree_item() {
                Some(CommandTreeItem::Category(_)) => self.state.toggle_focused_tree_category(),
                Some(CommandTreeItem::Command(_)) => {
                    self.state.reset_form_for_selected_command();
                    self.state.focus = FocusArea::Args;
                }
                None => {}
            },
            FocusArea::Namesrv => self.submit_namesrv_input(),
            FocusArea::Search => self.submit_search_input(),
            FocusArea::Args => self.apply_action(Action::ExecuteRequested),
            FocusArea::Result => {
                self.state.result_scroll = 0;
                self.state.result_horizontal_scroll = 0;
            }
        }
    }

    fn submit_namesrv_input(&mut self) {
        let namesrv_addr = self.state.namesrv_addr.trim().to_string();
        self.apply_action(Action::NamesrvChanged(namesrv_addr.clone()));
        self.state.last_error = None;
        self.state.progress_message = Some(if namesrv_addr.is_empty() {
            "NameServer address cleared".to_string()
        } else {
            format!("NameServer address set to {namesrv_addr}")
        });
        self.state.focus = FocusArea::CommandTree;
    }

    fn submit_search_input(&mut self) {
        self.state.last_error = None;
        self.state.focus = FocusArea::CommandTree;
    }

    fn move_down(&mut self) {
        match self.state.focus {
            FocusArea::CommandTree => {
                self.state.select_next_tree_item();
                self.emit_selected_command_action();
            }
            FocusArea::Args => {
                let command = self.state.selected_command().clone();
                self.state.form.focus_next_arg(&command);
            }
            FocusArea::Result => self.state.result_scroll = self.state.result_scroll.saturating_add(1),
            FocusArea::Namesrv | FocusArea::Search => {}
        }
    }

    fn move_up(&mut self) {
        match self.state.focus {
            FocusArea::CommandTree => {
                self.state.select_previous_tree_item();
                self.emit_selected_command_action();
            }
            FocusArea::Args => self.state.form.focus_previous_arg(),
            FocusArea::Result => self.state.result_scroll = self.state.result_scroll.saturating_sub(1),
            FocusArea::Namesrv | FocusArea::Search => {}
        }
    }

    fn move_left(&mut self) {
        match self.state.focus {
            FocusArea::CommandTree => self.state.collapse_focused_tree_category(),
            FocusArea::Args => {
                let command = self.state.selected_command().clone();
                self.state.form.cycle_enum_current(&command, true);
            }
            FocusArea::Result => {
                self.state.result_horizontal_scroll = self.state.result_horizontal_scroll.saturating_sub(1);
            }
            _ => {}
        }
    }

    fn move_right(&mut self) {
        match self.state.focus {
            FocusArea::CommandTree => self.state.expand_focused_tree_category(),
            FocusArea::Args => {
                let command = self.state.selected_command().clone();
                self.state.form.cycle_enum_current(&command, false);
            }
            FocusArea::Result => {
                self.state.result_horizontal_scroll = self.state.result_horizontal_scroll.saturating_add(1);
            }
            _ => {}
        }
    }

    fn handle_backspace(&mut self) {
        match self.state.focus {
            FocusArea::Namesrv => {
                self.state.namesrv_addr.pop();
                self.apply_action(Action::NamesrvChanged(self.state.namesrv_addr.clone()));
            }
            FocusArea::Search => {
                let mut search = self.state.search.clone();
                search.pop();
                self.apply_action(Action::SearchChanged(search));
            }
            FocusArea::Args => {
                let command = self.state.selected_command().clone();
                self.state.form.backspace_current(&command);
                self.emit_current_arg_changed(&command);
            }
            FocusArea::CommandTree | FocusArea::Result => {}
        }
    }

    fn handle_space(&mut self) {
        if self.state.focus == FocusArea::Args {
            let command = self.state.selected_command().clone();
            self.state.form.toggle_bool_current(&command);
            self.emit_current_arg_changed(&command);
        }
    }

    fn handle_char(&mut self, value: char) {
        match self.state.focus {
            FocusArea::Namesrv => {
                self.state.namesrv_addr.push(value);
                self.apply_action(Action::NamesrvChanged(self.state.namesrv_addr.clone()));
            }
            FocusArea::Search => {
                let mut search = self.state.search.clone();
                search.push(value);
                self.apply_action(Action::SearchChanged(search));
            }
            FocusArea::Args => {
                let command = self.state.selected_command().clone();
                self.state.form.append_to_current(&command, value);
                self.emit_current_arg_changed(&command);
            }
            FocusArea::CommandTree | FocusArea::Result => {}
        }
    }

    fn apply_action(&mut self, action: Action) {
        match action {
            Action::Quit => self.quit(),
            Action::FocusNext => self.focus_next(),
            Action::FocusPrevious => self.focus_previous(),
            Action::FocusSearch => self.state.focus = FocusArea::Search,
            Action::FocusNamesrv => self.state.focus = FocusArea::Namesrv,
            Action::SearchChanged(search) => self.state.set_search(search),
            Action::NamesrvChanged(namesrv_addr) => {
                self.admin_facade.set_namesrv_addr(Some(namesrv_addr.clone()));
                self.state.namesrv_addr = namesrv_addr;
            }
            Action::ExecuteRequested => self.prepare_execution(),
            Action::ConfirmRequested {
                execution_id,
                command_id,
                expected,
            } => {
                self.state.confirm_input.clear();
                self.state.execution = CommandExecutionState::Confirming {
                    execution_id,
                    command_id,
                    expected,
                };
            }
            Action::CommandStarted {
                execution_id,
                command_id,
            } => {
                self.state.last_error = None;
                self.state.progress_message = Some(format!("started {command_id}"));
                self.state.result = None;
                self.state.execution = CommandExecutionState::Running {
                    execution_id,
                    command_id,
                };
            }
            Action::CommandSucceeded {
                execution_id,
                command_id,
                result,
            } => {
                if self.is_current_running_execution(execution_id) {
                    self.clear_running_task(execution_id);
                    self.state.result = Some(result);
                    self.state.progress_message = Some(format!("finished {command_id}"));
                    self.state.result_scroll = 0;
                    self.state.result_horizontal_scroll = 0;
                    self.state.execution = CommandExecutionState::Succeeded {
                        execution_id,
                        command_id,
                    };
                }
            }
            Action::CommandFailed {
                execution_id,
                command_id,
                error,
            } => {
                if self.is_current_running_execution(execution_id) {
                    self.clear_running_task(execution_id);
                    self.state.last_error = Some(error.clone());
                    self.state.progress_message = Some(format!("failed {command_id}"));
                    self.state.result = Some(crate::view_model::CommandResultViewModel::error(
                        "Command Failed",
                        error,
                    ));
                    self.state.execution = CommandExecutionState::Failed {
                        execution_id,
                        command_id,
                    };
                }
            }
            Action::CancelExecution {
                execution_id,
                command_id,
            } => {
                if self.state.execution.execution_id() == Some(execution_id) {
                    self.abort_running_task_if_matches(execution_id);
                    self.state.execution = CommandExecutionState::Cancelled {
                        execution_id,
                        command_id,
                    };
                    self.state.progress_message = Some("cancelled locally; late result will be ignored".to_string());
                    self.state.confirm_input.clear();
                }
            }
            Action::HelpToggled => self.state.show_help = !self.state.show_help,
            Action::ResultCleared => {
                self.state.result = None;
                self.state.last_error = None;
                self.state.progress_message = None;
                self.state.result_scroll = 0;
                self.state.result_horizontal_scroll = 0;
            }
            Action::CommandSelected(command_id) => {
                if let Some(position) = self
                    .state
                    .visible_command_indices()
                    .iter()
                    .position(|index| self.state.commands()[*index].id == command_id)
                {
                    self.state.select_visible_command_at(position);
                }
            }
            Action::ArgChanged { name, value } => self.state.form.set_value(&name, value),
            Action::ProgressUpdated { execution_id, message } => {
                if self.is_current_running_execution(execution_id) {
                    self.state.progress_message = Some(message);
                }
            }
        }
    }

    fn focus_next(&mut self) {
        self.state.focus = match self.state.focus {
            FocusArea::Namesrv => FocusArea::Search,
            FocusArea::Search => FocusArea::CommandTree,
            FocusArea::CommandTree => FocusArea::Args,
            FocusArea::Args => FocusArea::Result,
            FocusArea::Result => FocusArea::Namesrv,
        };
    }

    fn focus_previous(&mut self) {
        self.state.focus = match self.state.focus {
            FocusArea::Namesrv => FocusArea::Result,
            FocusArea::Search => FocusArea::Namesrv,
            FocusArea::CommandTree => FocusArea::Search,
            FocusArea::Args => FocusArea::CommandTree,
            FocusArea::Result => FocusArea::Args,
        };
    }

    fn prepare_execution(&mut self) {
        if !self.state.validate_selected_form() {
            self.state.last_error = Some("fix argument validation errors before executing".to_string());
            self.state.focus = FocusArea::Args;
            return;
        }

        let execution_id = self.state.next_execution_id();
        let command = self.state.selected_command().clone();
        if let Some(expected) = command.expected_confirmation(&self.state.form) {
            self.apply_action(Action::ConfirmRequested {
                execution_id,
                command_id: command.id.to_string(),
                expected,
            });
        } else {
            self.start_execution(execution_id, command.id.to_string());
        }
    }

    fn start_execution(&mut self, execution_id: u64, command_id: String) {
        self.abort_running_task();
        self.apply_action(Action::CommandStarted {
            execution_id,
            command_id: command_id.clone(),
        });
        let command = self.state.selected_command().clone();
        let form = self.state.form.clone();
        let facade = self.admin_facade.clone();
        let tx = self.action_tx.clone();
        let diagnostics = Arc::clone(&self.action_queue_diagnostics);
        let progress_tx = tx.clone();
        let progress_diagnostics = Arc::clone(&diagnostics);
        let command_id_for_task = command_id.clone();
        let command_task = tokio::task::spawn_local(async move {
            let result = execute_command_with_progress(&facade, &command, &form, move |message| {
                try_send_progress(&progress_tx, &progress_diagnostics, execution_id, message);
            })
            .await;
            let action = match result {
                Ok(result) => Action::CommandSucceeded {
                    execution_id,
                    command_id: command_id_for_task,
                    result,
                },
                Err(error) => Action::CommandFailed {
                    execution_id,
                    command_id: command_id_for_task,
                    error: error.to_string(),
                },
            };
            send_required_action(&tx, &diagnostics, action).await;
        });
        let abort_handle = command_task.abort_handle();
        drop(command_task);
        self.running_task = Some(RunningCommandTask {
            execution_id,
            abort_handle,
        });
    }

    fn is_current_running_execution(&self, execution_id: u64) -> bool {
        matches!(
            self.state.execution,
            CommandExecutionState::Running {
                execution_id: current,
                ..
            } if current == execution_id
        )
    }

    fn abort_running_task(&mut self) {
        if let Some(task) = self.running_task.take() {
            task.abort_handle.abort();
        }
    }

    fn abort_running_task_if_matches(&mut self, execution_id: u64) {
        if self
            .running_task
            .as_ref()
            .is_some_and(|task| task.execution_id == execution_id)
        {
            self.abort_running_task();
        }
    }

    fn clear_running_task(&mut self, execution_id: u64) {
        if self
            .running_task
            .as_ref()
            .is_some_and(|task| task.execution_id == execution_id)
        {
            self.running_task = None;
        }
    }

    fn emit_current_arg_changed(&mut self, command: &crate::commands::CommandSpec) {
        if let Some(arg) = self.state.form.current_arg(command) {
            let value = self.state.form.raw_value(arg.name).unwrap_or_default().to_string();
            self.apply_action(Action::ArgChanged {
                name: arg.name.to_string(),
                value,
            });
        }
    }

    fn emit_selected_command_action(&mut self) {
        if matches!(self.state.focused_tree_item(), Some(CommandTreeItem::Command(_))) {
            self.apply_action(Action::CommandSelected(self.state.selected_command().id.to_string()));
        }
    }

    fn draw(&self, frame: &mut Frame) {
        crate::ui::render(frame, &self.state);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::Context;
    use std::task::Poll;

    use ratatui::crossterm::event::KeyModifiers;

    use super::*;
    use crate::admin_facade::test_client_runtime;
    use crate::admin_facade::TuiAdminFacade;

    #[test]
    fn app_can_be_constructed_with_admin_facade() {
        let facade = TuiAdminFacade::with_namesrv_addr(test_client_runtime(), "127.0.0.1:9876");
        let app = RocketmqTuiApp::with_admin_facade(facade);

        assert_eq!(app.admin_facade().namesrv_addr(), Some("127.0.0.1:9876"));
        assert_eq!(app.action_queue_snapshot().capacity, ACTION_QUEUE_CAPACITY);
    }

    #[test]
    fn progress_bursts_are_bounded_and_coalesced() {
        let app = RocketmqTuiApp::new(test_client_runtime());
        for index in 0..ACTION_QUEUE_CAPACITY * 4 {
            try_send_progress(
                &app.action_tx,
                &app.action_queue_diagnostics,
                1,
                format!("progress-{index}"),
            );
        }

        let snapshot = app.action_queue_snapshot();
        assert_eq!(snapshot.capacity, ACTION_QUEUE_CAPACITY);
        assert_eq!(snapshot.queued, ACTION_QUEUE_CAPACITY);
        assert!(snapshot.queued_bytes >= ACTION_QUEUE_CAPACITY * std::mem::size_of::<Action>());
        assert!(snapshot.oldest_age_millis.is_some());
        assert_eq!(snapshot.accepted, ACTION_QUEUE_CAPACITY as u64);
        assert_eq!(snapshot.rejected, 0);
        assert_eq!(snapshot.coalesced, (ACTION_QUEUE_CAPACITY * 3) as u64);
    }

    #[test]
    fn namesrv_action_updates_facade_and_state() {
        let mut app = RocketmqTuiApp::new(test_client_runtime());

        app.apply_action(Action::NamesrvChanged(" 127.0.0.1:9876 ".to_string()));

        assert_eq!(app.admin_facade().namesrv_addr(), Some("127.0.0.1:9876"));
        assert_eq!(app.state.namesrv_addr, " 127.0.0.1:9876 ");
    }

    #[test]
    fn enter_in_namesrv_input_commits_address_without_executing_command() {
        let mut app = RocketmqTuiApp::new(test_client_runtime());

        app.apply_action(Action::FocusNamesrv);
        app.apply_action(Action::NamesrvChanged(" 127.0.0.1:9876 ".to_string()));
        app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        assert_eq!(app.admin_facade().namesrv_addr(), Some("127.0.0.1:9876"));
        assert_eq!(app.state.namesrv_addr, "127.0.0.1:9876");
        assert_eq!(app.state.focus, FocusArea::CommandTree);
        assert_eq!(app.state.execution, CommandExecutionState::Idle);
        assert!(app.running_task.is_none());
        assert!(app.state.last_error.is_none());
    }

    #[test]
    fn enter_in_search_input_returns_to_command_tree_without_executing_command() {
        let mut app = RocketmqTuiApp::new(test_client_runtime());

        app.apply_action(Action::FocusSearch);
        app.apply_action(Action::SearchChanged("topic.cluster".to_string()));
        app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        assert_eq!(app.state.focus, FocusArea::CommandTree);
        assert_eq!(app.state.execution, CommandExecutionState::Idle);
        assert!(app.running_task.is_none());
        assert!(app.state.last_error.is_none());
    }

    #[test]
    fn execution_requires_valid_args() {
        let mut app = RocketmqTuiApp::new(test_client_runtime());
        app.apply_action(Action::SearchChanged("topic.cluster".to_string()));
        app.state.select_next_tree_item();
        app.apply_action(Action::ExecuteRequested);

        assert!(app.state.last_error.is_some());
        assert_eq!(app.state.focus, FocusArea::Args);
    }

    #[test]
    fn starting_command_execution_builds_background_task_without_stack_overflow() {
        let local = tokio::task::LocalSet::new();

        local.block_on(&tokio::runtime::Builder::new_current_thread().build().unwrap(), async {
            let mut app = RocketmqTuiApp::new(test_client_runtime());
            app.apply_action(Action::SearchChanged("message.decode_id".to_string()));
            app.apply_action(Action::CommandSelected("message.decode_id".to_string()));
            app.state.reset_form_for_selected_command();
            app.state
                .form
                .set_value("message_ids", "7F0000010007D8260BF075769D36C348".to_string());

            app.apply_action(Action::ExecuteRequested);

            assert!(matches!(app.state.execution, CommandExecutionState::Running { .. }));
            assert!(app.running_task.is_some());
            app.abort_running_task();
        });
    }

    #[test]
    fn cancel_execution_aborts_tracked_local_task() {
        let local = tokio::task::LocalSet::new();

        local.block_on(&tokio::runtime::Builder::new_current_thread().build().unwrap(), async {
            let aborted = Rc::new(Cell::new(false));
            let command_task = tokio::task::spawn_local(AbortProbe {
                aborted: aborted.clone(),
            });
            let abort_handle = command_task.abort_handle();
            drop(command_task);

            let mut app = RocketmqTuiApp::new(test_client_runtime());
            app.running_task = Some(RunningCommandTask {
                execution_id: 7,
                abort_handle,
            });
            app.state.execution = CommandExecutionState::Running {
                execution_id: 7,
                command_id: "message.consume".to_string(),
            };

            app.apply_action(Action::CancelExecution {
                execution_id: 7,
                command_id: "message.consume".to_string(),
            });

            let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
            while !aborted.get() {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "abort probe task was not dropped"
                );
                tokio::task::yield_now().await;
            }

            assert!(app.running_task.is_none());
        });
    }

    struct AbortProbe {
        aborted: Rc<Cell<bool>>,
    }

    impl Future for AbortProbe {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for AbortProbe {
        fn drop(&mut self) {
            self.aborted.set(true);
        }
    }
}
