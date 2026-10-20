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
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use crossterm::event::EventStream;
use ratatui::DefaultTerminal;
use ratatui::Frame;
use rocketmq_admin_core::client_adapter::ClientRuntime;
use rocketmq_admin_core::client_adapter::ClientRuntimeConfig;
use rocketmq_error::Result as CanonicalResult;
use rocketmq_runtime::ScopeId;
use rocketmq_runtime::TaskKind;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinError;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;

use crate::action::Action;
use crate::admin_facade::TuiAdminFacade;
use crate::commands::execute_command_with_progress;
use crate::motion::FRAMES_PER_SECOND;
use crate::result_view::ResultTone;
use crate::state::AppState;
use crate::state::CommandExecutionState;
use crate::state::CommandTreeItem;
use crate::state::FocusArea;
use crate::state::FramePace;
use crate::state::ToastLevel;
use crate::ui::ColorDepth;
use crate::ui::ViewCache;
use crate::view_model::CommandResultViewModel;

pub struct RocketmqTuiApp {
    admin_facade: TuiAdminFacade,
    should_quit: bool,
    state: AppState,
    action_tx: mpsc::Sender<QueuedAction>,
    action_rx: mpsc::Receiver<QueuedAction>,
    action_queue_diagnostics: Arc<ActionQueueDiagnostics>,
    running_task: Option<RunningCommandTask>,
    command_tasks: JoinSet<Option<Action>>,
    view: ViewCache,
    /// Mouse capture setting that still has to reach the terminal.
    pending_mouse_capture: Option<bool>,
    /// Whether input or a command changed something since the last frame.
    redraw: bool,
}

const ACTION_QUEUE_CAPACITY: usize = 128;
/// Ticks between two frames while only ambient effects are playing.
const AMBIENT_FRAME_TICKS: u64 = 2;

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

/// The command that is executing.
///
/// Dropping it stops the command at its next suspension point. The task that ran the
/// command still drains the client scope the command used.
struct RunningCommandTask {
    execution_id: u64,
    /// Nothing is ever sent: the command's task observes this sender being dropped.
    _stop_on_drop: oneshot::Sender<()>,
}

/// Marks a command whose future panicked while it was polled.
struct CommandPanicked;

/// Polls a command and reports a panic as a value.
///
/// A command shares its task with the cleanup of its client scope, and that task
/// belongs to the application's client scope. A panic that left the command would skip
/// the cleanup and poison that owner against every later command.
struct ContainPanic<F> {
    command: Pin<Box<F>>,
}

impl<F> ContainPanic<F> {
    fn new(command: F) -> Self {
        Self {
            command: Box::pin(command),
        }
    }
}

impl<F: Future> Future for ContainPanic<F> {
    type Output = Result<F::Output, CommandPanicked>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let command = self.command.as_mut();
        match std::panic::catch_unwind(AssertUnwindSafe(|| command.poll(context))) {
            Ok(poll) => poll.map(Ok),
            // The command is finished: it is dropped without being polled again.
            Err(_) => Poll::Ready(Err(CommandPanicked)),
        }
    }
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
            command_tasks: JoinSet::new(),
            view: ViewCache::new(ColorDepth::detect()),
            pending_mouse_capture: None,
            redraw: true,
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

impl RocketmqTuiApp {
    pub async fn run(mut self, mut terminal: DefaultTerminal) -> anyhow::Result<()> {
        let result = self.run_events(&mut terminal).await;
        self.shutdown_commands().await;
        result
    }

    async fn run_events(&mut self, terminal: &mut DefaultTerminal) -> anyhow::Result<()> {
        let period = Duration::from_secs(1) / FRAMES_PER_SECOND as u32;
        let mut interval = tokio::time::interval(period);
        let mut events = EventStream::new();
        while !self.should_quit() {
            tokio::select! {
                _ = interval.tick() => {
                    self.redraw |= crate::terminal::recover_if_disturbed(terminal, self.state.mouse_capture())?;
                    self.state.advance_animation();
                    if self.take_frame_due() {
                        crate::terminal::draw(terminal, |frame| self.draw(frame))?;
                    }
                },
                Some(Ok(event)) = events.next() => {
                    self.handle_event(&event);
                    self.redraw = true;
                    if let Some(enabled) = self.pending_mouse_capture.take() {
                        crate::terminal::set_mouse_capture(terminal.backend_mut(), enabled)?;
                    }
                },
                Some(queued) = self.action_rx.recv() => {
                    self.action_queue_diagnostics.dequeue(queued.id);
                    self.apply_action(queued.action);
                    self.redraw = true;
                },
                Some(completion) = self.command_tasks.join_next(), if !self.command_tasks.is_empty() => {
                    self.complete_command_task(completion);
                    self.redraw = true;
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

    /// Returns whether the current tick has to draw, and consumes a pending request.
    ///
    /// Transitions get every tick and ambient effects every other one. A screen that
    /// shows nothing time-dependent is left alone until something happens, so it
    /// writes nothing to the terminal.
    fn take_frame_due(&mut self) -> bool {
        let requested = std::mem::take(&mut self.redraw);
        requested
            || match self.state.frame_pace() {
                FramePace::Full => true,
                FramePace::Ambient => self.state.animation_tick().is_multiple_of(AMBIENT_FRAME_TICKS),
                FramePace::Still => false,
            }
    }

    fn apply_action(&mut self, action: Action) {
        match action {
            Action::Quit => self.quit(),
            Action::FocusNext => self.focus_next(),
            Action::FocusPrevious => self.focus_previous(),
            Action::FocusSearch => self.state.set_focus(FocusArea::Search),
            Action::FocusNamesrv => self.state.set_focus(FocusArea::Namesrv),
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
                self.state.last_error = None;
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
                self.state.clear_result();
                self.state.mark_run_started();
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
                    self.state.set_result(result, ResultTone::Normal);
                    self.state.progress_message = Some(format!("finished {command_id}"));
                    self.state.mark_run_finished();
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
                    self.state.set_result(
                        CommandResultViewModel::error("Command Failed", error),
                        ResultTone::Failure,
                    );
                    self.state.mark_run_finished();
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
                    let was_running = self.is_current_running_execution(execution_id);
                    self.abort_running_task_if_matches(execution_id);
                    self.state.execution = CommandExecutionState::Cancelled {
                        execution_id,
                        command_id,
                    };
                    self.state.confirm_input.clear();
                    if was_running {
                        self.state.mark_run_finished();
                        self.state.progress_message =
                            Some("cancelled locally; late result will be ignored".to_string());
                        self.state.notify(
                            ToastLevel::Warning,
                            "Cancelled locally. A request the server accepted is not undone.",
                        );
                    } else {
                        self.state.clear_run_timing();
                        self.state
                            .notify(ToastLevel::Info, "Confirmation cancelled. Nothing was sent.");
                    }
                }
            }
            Action::HelpToggled => {
                self.state.show_help = !self.state.show_help;
                self.state.overlay_scroll = 0;
            }
            Action::ResultCleared => {
                self.state.clear_result();
                self.state.last_error = None;
                self.state.progress_message = None;
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
        self.state.set_focus(match self.state.focus {
            FocusArea::Namesrv => FocusArea::Search,
            FocusArea::Search => FocusArea::CommandTree,
            FocusArea::CommandTree => FocusArea::Args,
            FocusArea::Args => FocusArea::Result,
            FocusArea::Result => FocusArea::Namesrv,
        });
    }

    fn focus_previous(&mut self) {
        self.state.set_focus(match self.state.focus {
            FocusArea::Namesrv => FocusArea::Result,
            FocusArea::Search => FocusArea::Namesrv,
            FocusArea::CommandTree => FocusArea::Search,
            FocusArea::Args => FocusArea::CommandTree,
            FocusArea::Result => FocusArea::Args,
        });
    }

    fn prepare_execution(&mut self) {
        if !self.state.validate_selected_form() {
            let command = self.state.selected_command().clone();
            let invalid = self.state.form.validation_errors().len();
            self.state.last_error = Some("fix argument validation errors before executing".to_string());
            self.state.set_focus(FocusArea::Args);
            // Land on the first field that needs attention.
            if let Some(index) = self.state.form.first_invalid_arg(&command) {
                self.state.focus_arg(index);
            }
            self.state.flag_invalid();
            self.state.notify(
                ToastLevel::Error,
                if invalid == 1 {
                    "1 parameter needs attention".to_string()
                } else {
                    format!("{invalid} parameters need attention")
                },
            );
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
        let facade = match self.command_facade(execution_id) {
            Ok(facade) => facade,
            Err(error) => {
                self.apply_action(Action::CommandFailed {
                    execution_id,
                    command_id,
                    error: error.to_string(),
                });
                return;
            }
        };
        let client_runtime = facade.client_runtime();
        let command = self.state.selected_command().clone();
        let form = self.state.form.clone();
        let progress_tx = self.action_tx.clone();
        let progress_diagnostics = Arc::clone(&self.action_queue_diagnostics);
        let operation = async move {
            execute_command_with_progress(&facade, &command, &form, move |message| {
                try_send_progress(&progress_tx, &progress_diagnostics, execution_id, message);
            })
            .await
        };
        self.spawn_command_task(execution_id, command_id, client_runtime, operation);
    }

    fn command_facade(&self, execution_id: u64) -> CanonicalResult<TuiAdminFacade> {
        let parent = self.admin_facade.client_runtime();
        let scope = ScopeId::try_new(format!("command-{execution_id}"))
            .map_err(|_| crate::errors::invariant_violated("invalid command runtime scope"))?;
        let context = parent
            .service_context()
            .try_component(scope)
            .map_err(|error| rocketmq_error::Error::caused_by(error.descriptor(), error))?;
        let client_runtime = ClientRuntime::try_new(
            context,
            ClientRuntimeConfig::default(),
            parent.telemetry_handle().clone(),
        )
        .map_err(|error| error.into_error())?;
        Ok(self.admin_facade.with_client_runtime(client_runtime))
    }

    /// Runs `operation` on the runtime and hands its outcome to the interface thread.
    ///
    /// The interface thread only draws and reads input. A command awaits the complete
    /// admin, client, and transport call graph, so a runtime worker polls it: the
    /// runtime configuration sizes the stack that call graph runs on, and the interface
    /// keeps responding while the command runs.
    fn spawn_command_task<F>(
        &mut self,
        execution_id: u64,
        command_id: String,
        client_runtime: Arc<ClientRuntime>,
        operation: F,
    ) where
        F: Future<Output = CanonicalResult<CommandResultViewModel>> + Send + 'static,
    {
        let (stop_on_drop, mut stopped) = oneshot::channel::<()>();
        let (completion_tx, completion_rx) = oneshot::channel();
        let reported_command_id = command_id.clone();
        let task = async move {
            let outcome = tokio::select! {
                biased;
                _ = &mut stopped => None,
                outcome = ContainPanic::new(operation) => Some(outcome),
            };
            // The command is destroyed by now, whether it finished or was stopped. Its
            // isolated client pool is drained before anything is reported. Shutdown
            // never touches a subsequent command's connections or the application's
            // client runtime.
            let report = client_runtime.shutdown().await;
            if !report.is_healthy() {
                tracing::warn!(execution_id, report = %report.to_json(), "admin command runtime shutdown is unhealthy");
            }
            let result = match outcome {
                // The cancellation is already on screen.
                None => {
                    let _ = completion_tx.send(None);
                    return;
                }
                Some(Err(CommandPanicked)) => Err(crate::errors::invariant_violated("admin command task failed")),
                Some(Ok(result)) if report.is_healthy() => result,
                Some(Ok(_)) => Err(crate::errors::invariant_violated(
                    "admin command runtime shutdown is unhealthy",
                )),
            };
            let _ = completion_tx.send(Some(match result {
                Ok(result) => Action::CommandSucceeded {
                    execution_id,
                    command_id,
                    result,
                },
                Err(error) => Action::CommandFailed {
                    execution_id,
                    command_id,
                    error: error.to_string(),
                },
            }));
        };

        // The application's client scope owns the task, so quitting waits for the
        // cleanup of every command that was started.
        let owner = self.admin_facade.client_runtime();
        let spawned = owner
            .service_context()
            .task_group()
            .spawn("admin-command", TaskKind::Worker, task);
        if let Err(error) = spawned {
            // The rejected task was dropped with the command and its unused client scope.
            let error = rocketmq_error::Error::caused_by(error.descriptor(), error);
            self.apply_action(Action::CommandFailed {
                execution_id,
                command_id: reported_command_id,
                error: error.to_string(),
            });
            return;
        }
        self.command_tasks.spawn_local(async move {
            match completion_rx.await {
                Ok(action) => action,
                // The task ended without reporting, so it was torn down after the command.
                Err(_) => Some(Action::CommandFailed {
                    execution_id,
                    command_id: reported_command_id,
                    error: crate::errors::invariant_violated("admin command task failed").to_string(),
                }),
            }
        });
        self.running_task = Some(RunningCommandTask {
            execution_id,
            _stop_on_drop: stop_on_drop,
        });
    }

    fn complete_command_task(&mut self, completion: Result<Option<Action>, JoinError>) {
        match completion {
            Ok(Some(action)) => self.apply_action(action),
            Ok(None) => {}
            Err(_) => tracing::error!("admin command cleanup task failed"),
        }
    }

    async fn shutdown_commands(&mut self) {
        self.abort_running_task();
        while let Some(completion) = self.command_tasks.join_next().await {
            self.complete_command_task(completion);
        }
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
        // Dropping the handle is what stops the command.
        self.running_task = None;
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

    fn emit_selected_command_action(&mut self) {
        if matches!(self.state.focused_tree_item(), Some(CommandTreeItem::Command(_))) {
            self.apply_action(Action::CommandSelected(self.state.selected_command().id.to_string()));
        }
    }

    fn draw(&mut self, frame: &mut Frame) {
        crate::ui::render(frame, &mut self.state, &mut self.view);
    }
}

mod input;

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use ratatui::crossterm::event::KeyCode;
    use ratatui::crossterm::event::KeyEvent;
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
    fn shortcut_characters_are_typed_in_text_inputs() {
        let mut namesrv_app = RocketmqTuiApp::new(test_client_runtime());
        namesrv_app.state.focus = FocusArea::Namesrv;
        let mut search_app = RocketmqTuiApp::new(test_client_runtime());
        search_app.state.focus = FocusArea::Search;
        let mut args_app = RocketmqTuiApp::new(test_client_runtime());
        args_app.state.focus = FocusArea::Args;
        let name = args_app.state.selected_command().args[0].name;
        let before = args_app.state.form.raw_value(name).unwrap_or_default().to_string();

        for value in "q?jk/n".chars() {
            namesrv_app.handle_key_event(KeyEvent::new(KeyCode::Char(value), KeyModifiers::NONE));
            search_app.handle_key_event(KeyEvent::new(KeyCode::Char(value), KeyModifiers::NONE));
            args_app.handle_key_event(KeyEvent::new(KeyCode::Char(value), KeyModifiers::NONE));
        }
        assert_eq!(namesrv_app.state.namesrv_addr, "q?jk/n");
        assert_eq!(search_app.state.search, "q?jk/n");
        assert_eq!(
            args_app.state.form.raw_value(name),
            Some(format!("{before}q?jk/n").as_str())
        );
    }

    #[test]
    fn execution_requires_valid_args() {
        let mut app = RocketmqTuiApp::new(test_client_runtime());
        app.apply_action(Action::SearchChanged("topic.cluster".to_string()));
        app.state.move_tree_cursor(1);
        app.apply_action(Action::ExecuteRequested);

        assert!(app.state.last_error.is_some());
        assert_eq!(app.state.focus, FocusArea::Args);
    }

    #[test]
    fn starting_command_execution_builds_background_task_without_stack_overflow() {
        let local = tokio::task::LocalSet::new();

        local.block_on(
            &tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
            async {
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
                app.shutdown_commands().await;
            },
        );
    }

    #[test]
    fn cancel_execution_stops_the_running_command() {
        let local = tokio::task::LocalSet::new();

        local.block_on(
            &tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
            async {
                let aborted = Arc::new(AtomicBool::new(false));
                let mut app = RocketmqTuiApp::new(test_client_runtime());
                let client_runtime = app.command_facade(7).unwrap().client_runtime();
                app.spawn_command_task(
                    7,
                    "message.consume".to_string(),
                    client_runtime.clone(),
                    AbortProbe {
                        aborted: aborted.clone(),
                    },
                );
                app.state.execution = CommandExecutionState::Running {
                    execution_id: 7,
                    command_id: "message.consume".to_string(),
                };

                app.apply_action(Action::CancelExecution {
                    execution_id: 7,
                    command_id: "message.consume".to_string(),
                });

                app.shutdown_commands().await;
                assert!(aborted.load(Ordering::Acquire));
                assert!(client_runtime.is_shutdown());
                assert!(app.command_tasks.is_empty());
                assert!(app.running_task.is_none());
            },
        );
    }

    fn select_dangerous_auth_user_delete(app: &mut RocketmqTuiApp) {
        app.apply_action(Action::SearchChanged("auth.user.delete".to_string()));
        app.apply_action(Action::CommandSelected("auth.user.delete".to_string()));
        app.state.reset_form_for_selected_command();
        app.state.form.set_value("username", "admin-user".to_string());
    }

    fn type_text(app: &mut RocketmqTuiApp, text: &str) {
        for character in text.chars() {
            app.handle_key_event(KeyEvent::new(KeyCode::Char(character), KeyModifiers::NONE));
        }
    }

    fn execute_request() -> KeyEvent {
        KeyEvent::new(KeyCode::Char('r'), KeyModifiers::CONTROL)
    }

    #[test]
    fn execute_request_on_dangerous_command_enters_confirmation_with_expected_text() {
        let mut app = RocketmqTuiApp::new(test_client_runtime());
        select_dangerous_auth_user_delete(&mut app);

        app.handle_key_event(execute_request());

        assert!(matches!(
            &app.state.execution,
            CommandExecutionState::Confirming { command_id, expected, .. }
                if command_id == "auth.user.delete" && expected == "admin-user"
        ));
        assert!(app.state.confirm_input.is_empty());
        assert!(app.running_task.is_none());
        assert!(app.state.last_error.is_none());
    }

    #[test]
    fn execute_request_on_safe_command_skips_confirmation_and_runs_directly() {
        let local = tokio::task::LocalSet::new();

        local.block_on(
            &tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
            async {
                let mut app = RocketmqTuiApp::new(test_client_runtime());
                app.apply_action(Action::SearchChanged("message.decode_id".to_string()));
                app.apply_action(Action::CommandSelected("message.decode_id".to_string()));
                app.state.reset_form_for_selected_command();
                app.state
                    .form
                    .set_value("message_ids", "7F0000010007D8260BF075769D36C348".to_string());

                app.handle_key_event(execute_request());

                assert!(matches!(app.state.execution, CommandExecutionState::Running { .. }));
                assert!(app.running_task.is_some());
                app.shutdown_commands().await;
            },
        );
    }

    #[test]
    fn exact_confirmation_text_starts_execution() {
        let local = tokio::task::LocalSet::new();

        local.block_on(
            &tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
            async {
                let mut app = RocketmqTuiApp::new(test_client_runtime());
                select_dangerous_auth_user_delete(&mut app);
                app.handle_key_event(execute_request());

                type_text(&mut app, "admin-user");
                app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

                assert!(matches!(app.state.execution, CommandExecutionState::Running { .. }));
                assert!(app.running_task.is_some());
                assert!(app.state.last_error.is_none());
                app.shutdown_commands().await;
            },
        );
    }

    #[test]
    fn frames_are_drawn_only_when_they_are_due() {
        let mut app = RocketmqTuiApp::new(test_client_runtime());
        assert!(app.take_frame_due(), "the first frame is always drawn");

        app.state.toggle_motion();
        for _ in 0..3 {
            app.state.advance_animation();
        }
        assert!(!app.take_frame_due(), "a still screen writes nothing to the terminal");

        // What an input event, a queued action, or a finished command leaves behind.
        app.redraw = true;
        assert!(app.take_frame_due());
        assert!(!app.take_frame_due(), "a request is served once");

        app.state.execution = CommandExecutionState::Running {
            execution_id: 1,
            command_id: "topic.list".to_string(),
        };
        app.state.advance_animation();
        assert!(app.take_frame_due(), "a change is drawn on the tick that observes it");
        let drawn = (0..10)
            .filter(|_| {
                app.state.advance_animation();
                app.take_frame_due()
            })
            .count();
        assert_eq!(drawn, 5, "a running command is redrawn on every other tick");
    }

    #[test]
    fn confirmation_match_trims_surrounding_whitespace() {
        let local = tokio::task::LocalSet::new();

        local.block_on(
            &tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
            async {
                let mut app = RocketmqTuiApp::new(test_client_runtime());
                select_dangerous_auth_user_delete(&mut app);
                app.handle_key_event(execute_request());

                type_text(&mut app, "  admin-user  ");
                app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

                assert!(matches!(app.state.execution, CommandExecutionState::Running { .. }));
                assert!(app.running_task.is_some());
                app.shutdown_commands().await;
            },
        );
    }

    #[test]
    fn mismatched_confirmation_keeps_confirming_and_reports_expected() {
        let mut app = RocketmqTuiApp::new(test_client_runtime());
        select_dangerous_auth_user_delete(&mut app);
        app.handle_key_event(execute_request());

        type_text(&mut app, "wrong-text");
        app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        assert!(matches!(app.state.execution, CommandExecutionState::Confirming { .. }));
        assert!(app
            .state
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("admin-user")));
        assert!(app.running_task.is_none());
    }

    #[test]
    fn correct_confirmation_after_mismatch_still_starts_execution() {
        let local = tokio::task::LocalSet::new();

        local.block_on(
            &tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
            async {
                let mut app = RocketmqTuiApp::new(test_client_runtime());
                select_dangerous_auth_user_delete(&mut app);
                app.handle_key_event(execute_request());
                type_text(&mut app, "wrong");
                app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
                assert!(matches!(app.state.execution, CommandExecutionState::Confirming { .. }));

                for _ in 0.."wrong".len() {
                    app.handle_key_event(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
                }
                assert!(app.state.confirm_input.is_empty());
                type_text(&mut app, "admin-user");
                app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

                assert!(matches!(app.state.execution, CommandExecutionState::Running { .. }));
                assert!(app.running_task.is_some());
                app.shutdown_commands().await;
            },
        );
    }

    #[test]
    fn backspace_edits_confirmation_input_and_tolerates_empty_input() {
        let mut app = RocketmqTuiApp::new(test_client_runtime());
        select_dangerous_auth_user_delete(&mut app);
        app.handle_key_event(execute_request());

        type_text(&mut app, "ab");
        app.handle_key_event(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
        assert_eq!(app.state.confirm_input, "a");

        for _ in 0..4 {
            app.handle_key_event(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
        }
        assert_eq!(app.state.confirm_input, "");
        assert!(matches!(app.state.execution, CommandExecutionState::Confirming { .. }));
    }

    #[test]
    fn escape_cancels_confirmation_and_clears_input() {
        let mut app = RocketmqTuiApp::new(test_client_runtime());
        select_dangerous_auth_user_delete(&mut app);
        app.handle_key_event(execute_request());
        type_text(&mut app, "admin");

        app.handle_key_event(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));

        assert!(matches!(
            &app.state.execution,
            CommandExecutionState::Cancelled { command_id, .. } if command_id == "auth.user.delete"
        ));
        assert!(app.state.confirm_input.is_empty());
        assert!(app.running_task.is_none());
    }

    #[test]
    fn character_keys_append_to_confirmation_input_instead_of_triggering_shortcuts() {
        let mut app = RocketmqTuiApp::new(test_client_runtime());
        select_dangerous_auth_user_delete(&mut app);
        app.handle_key_event(execute_request());

        type_text(&mut app, "qjk?");

        assert_eq!(app.state.confirm_input, "qjk?");
        assert!(!app.should_quit());
        assert!(!app.state.show_help);
        assert!(matches!(app.state.execution, CommandExecutionState::Confirming { .. }));
    }

    struct AbortProbe {
        aborted: Arc<AtomicBool>,
    }

    impl Future for AbortProbe {
        type Output = CanonicalResult<CommandResultViewModel>;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for AbortProbe {
        fn drop(&mut self) {
            self.aborted.store(true, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod command_lifecycle_tests;
#[cfg(test)]
mod input_tests;
