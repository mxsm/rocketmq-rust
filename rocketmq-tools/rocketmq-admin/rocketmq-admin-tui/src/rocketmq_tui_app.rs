use std::time::Duration;

use ratatui::crossterm::event::Event;
use ratatui::crossterm::event::EventStream;
use ratatui::crossterm::event::KeyCode;
use ratatui::crossterm::event::KeyEvent;
use ratatui::crossterm::event::KeyEventKind;
use ratatui::DefaultTerminal;
use ratatui::Frame;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
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
    action_tx: mpsc::UnboundedSender<Action>,
    action_rx: mpsc::UnboundedReceiver<Action>,
    running_task: Option<RunningCommandTask>,
}

struct RunningCommandTask {
    execution_id: u64,
    handle: JoinHandle<()>,
}

impl Default for RocketmqTuiApp {
    fn default() -> Self {
        Self::new()
    }
}

impl RocketmqTuiApp {
    pub fn new() -> Self {
        Self::with_admin_facade(TuiAdminFacade::default())
    }

    pub fn with_admin_facade(admin_facade: TuiAdminFacade) -> Self {
        let (action_tx, action_rx) = mpsc::unbounded_channel();
        let state = AppState::new(admin_facade.namesrv_addr());
        Self {
            admin_facade,
            should_quit: false,
            state,
            action_tx,
            action_rx,
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
}

impl RocketmqTuiApp {
    const FRAMES_PER_SECOND: f32 = 30.0;

    pub async fn run(mut self, mut terminal: DefaultTerminal) -> anyhow::Result<()> {
        let period = Duration::from_secs_f32(1.0 / Self::FRAMES_PER_SECOND);
        let mut interval = tokio::time::interval(period);
        let mut events = EventStream::new();
        while !self.should_quit() {
            tokio::select! {
                _ = interval.tick() => { terminal.draw(|frame| self.draw(frame))?; },
                Some(Ok(event)) = events.next() => self.handle_event(&event),
                Some(action) = self.action_rx.recv() => self.apply_action(action),
            }
        }
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
            FocusArea::Args | FocusArea::Search | FocusArea::Namesrv => self.apply_action(Action::ExecuteRequested),
            FocusArea::Result => {
                self.state.result_scroll = 0;
                self.state.result_horizontal_scroll = 0;
            }
        }
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
                    self.state.progress_message =
                        Some("cancelled locally; running task aborted and late result will be ignored".to_string());
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
        let _ = self.action_tx.send(Action::ProgressUpdated {
            execution_id,
            message: format!("started {command_id}"),
        });

        let command = self.state.selected_command().clone();
        let form = self.state.form.clone();
        let facade = self.admin_facade.clone();
        let tx = self.action_tx.clone();
        let progress_tx = tx.clone();
        let handle = tokio::task::spawn_local(async move {
            let action = match execute_command_with_progress(&facade, &command, &form, |message| {
                let _ = progress_tx.send(Action::ProgressUpdated { execution_id, message });
            })
            .await
            {
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
            };
            let _ = tx.send(action);
        });
        self.running_task = Some(RunningCommandTask { execution_id, handle });
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
            task.handle.abort();
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

    use super::*;
    use crate::admin_facade::TuiAdminFacade;

    #[test]
    fn app_can_be_constructed_with_admin_facade() {
        let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");
        let app = RocketmqTuiApp::with_admin_facade(facade);

        assert_eq!(app.admin_facade().namesrv_addr(), Some("127.0.0.1:9876"));
    }

    #[test]
    fn namesrv_action_updates_facade_and_state() {
        let mut app = RocketmqTuiApp::new();

        app.apply_action(Action::NamesrvChanged(" 127.0.0.1:9876 ".to_string()));

        assert_eq!(app.admin_facade().namesrv_addr(), Some("127.0.0.1:9876"));
        assert_eq!(app.state.namesrv_addr, " 127.0.0.1:9876 ");
    }

    #[test]
    fn execution_requires_valid_args() {
        let mut app = RocketmqTuiApp::new();
        app.apply_action(Action::SearchChanged("topic.cluster".to_string()));
        app.state.select_next_tree_item();
        app.apply_action(Action::ExecuteRequested);

        assert!(app.state.last_error.is_some());
        assert_eq!(app.state.focus, FocusArea::Args);
    }

    #[test]
    fn cancel_execution_aborts_tracked_local_task() {
        let local = tokio::task::LocalSet::new();

        local.block_on(&tokio::runtime::Builder::new_current_thread().build().unwrap(), async {
            let aborted = Rc::new(Cell::new(false));
            let handle = tokio::task::spawn_local(AbortProbe {
                aborted: aborted.clone(),
            });

            let mut app = RocketmqTuiApp::new();
            app.running_task = Some(RunningCommandTask {
                execution_id: 7,
                handle,
            });
            app.state.execution = CommandExecutionState::Running {
                execution_id: 7,
                command_id: "message.consume".to_string(),
            };

            app.apply_action(Action::CancelExecution {
                execution_id: 7,
                command_id: "message.consume".to_string(),
            });
            tokio::task::yield_now().await;

            assert!(aborted.get());
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
