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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;
use std::time::Instant;

use crate::commands::command_catalog;
use crate::commands::ArgKind;
use crate::commands::ArgSpec;
use crate::commands::CommandCategory;
use crate::commands::CommandSpec;
use crate::commands::RiskLevel;
use crate::motion::ease_out_cubic;
use crate::motion::progress;
use crate::motion::Tracked;
use crate::motion::FRAMES_PER_SECOND;
use crate::result_view::ResultTone;
use crate::result_view::ResultView;
use crate::view_model::CommandResultViewModel;
use rocketmq_error::Result as CanonicalResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FocusArea {
    Namesrv,
    Search,
    CommandTree,
    Args,
    Result,
}

impl FocusArea {
    /// Returns the label rendered for this focus area.
    pub fn label(self) -> &'static str {
        match self {
            Self::Namesrv => "NameServer",
            Self::Search => "Search",
            Self::CommandTree => "Commands",
            Self::Args => "Parameters",
            Self::Result => "Result",
        }
    }

    /// Returns the pane that contains this focus area.
    pub fn pane(self) -> Pane {
        match self {
            Self::Namesrv => Pane::Header,
            Self::Search | Self::CommandTree => Pane::Sidebar,
            Self::Args => Pane::Command,
            Self::Result => Pane::Result,
        }
    }
}

/// Screen region that owns one or more focus areas.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pane {
    Header,
    Sidebar,
    Command,
    Result,
}

/// Modal layer drawn above the panes. At most one is open at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Overlay {
    None,
    Help,
    Confirm,
    Detail,
}

/// Text input that receives typed characters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputTarget {
    Namesrv,
    Search,
    /// Argument of the selected command, by position.
    Arg(usize),
    Confirm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToastLevel {
    Info,
    Success,
    Warning,
    Error,
}

/// Transient notification shown above the key bar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Toast {
    pub level: ToastLevel,
    pub message: String,
    /// Animation tick at which the toast was raised.
    pub created_at: u64,
}

impl Toast {
    pub const LIFETIME_TICKS: u64 = 4 * FRAMES_PER_SECOND;
}

/// Every column of one result row, shown in the detail overlay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowDetail {
    pub title: String,
    pub fields: Vec<(String, String)>,
}

/// How soon the screen needs another frame when no input arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FramePace {
    /// Nothing on screen depends on time: the next frame can wait for an event.
    Still,
    /// Only slow, decorative effects or a running command are on screen.
    Ambient,
    /// A transition is playing, or something changed that has not been drawn yet.
    Full,
}

/// Execution state without its payload, for change tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionPhase {
    Idle,
    Confirming,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandExecutionState {
    Idle,
    Confirming {
        execution_id: u64,
        command_id: String,
        expected: String,
    },
    Running {
        execution_id: u64,
        command_id: String,
    },
    Succeeded {
        execution_id: u64,
        command_id: String,
    },
    Failed {
        execution_id: u64,
        command_id: String,
    },
    Cancelled {
        execution_id: u64,
        command_id: String,
    },
}

impl CommandExecutionState {
    /// Returns the status label rendered for the current execution state.
    pub fn label(&self) -> String {
        match self {
            Self::Idle => "idle".to_string(),
            Self::Confirming { command_id, .. } => format!("confirming {command_id}"),
            Self::Running { command_id, .. } => format!("running {command_id}"),
            Self::Succeeded { command_id, .. } => format!("succeeded {command_id}"),
            Self::Failed { command_id, .. } => format!("failed {command_id}"),
            Self::Cancelled { command_id, .. } => format!("cancelled {command_id}"),
        }
    }

    /// Returns the state without its payload.
    pub fn phase(&self) -> ExecutionPhase {
        match self {
            Self::Idle => ExecutionPhase::Idle,
            Self::Confirming { .. } => ExecutionPhase::Confirming,
            Self::Running { .. } => ExecutionPhase::Running,
            Self::Succeeded { .. } => ExecutionPhase::Succeeded,
            Self::Failed { .. } => ExecutionPhase::Failed,
            Self::Cancelled { .. } => ExecutionPhase::Cancelled,
        }
    }

    /// Returns the execution identifier for a non-idle state.
    pub fn execution_id(&self) -> Option<u64> {
        match self {
            Self::Idle => None,
            Self::Confirming { execution_id, .. }
            | Self::Running { execution_id, .. }
            | Self::Succeeded { execution_id, .. }
            | Self::Failed { execution_id, .. }
            | Self::Cancelled { execution_id, .. } => Some(*execution_id),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandTreeItem {
    Category(CommandCategory),
    Command(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandFormState {
    command_id: String,
    values: BTreeMap<String, String>,
    focused_arg: usize,
    validation_errors: BTreeMap<String, String>,
    dirty: bool,
}

impl CommandFormState {
    /// Creates form state initialized from a command specification.
    pub fn for_command(command: &CommandSpec) -> Self {
        let values = command
            .args
            .iter()
            .map(|arg| (arg.name.to_string(), arg.default_value()))
            .collect();
        Self {
            command_id: command.id.to_string(),
            values,
            focused_arg: 0,
            validation_errors: BTreeMap::new(),
            dirty: false,
        }
    }

    /// Returns the command identifier this form represents.
    pub fn command_id(&self) -> &str {
        &self.command_id
    }

    /// Returns the index of the focused argument.
    pub fn focused_arg(&self) -> usize {
        self.focused_arg
    }

    /// Returns validation errors keyed by argument name.
    pub fn validation_errors(&self) -> &BTreeMap<String, String> {
        &self.validation_errors
    }

    /// Returns whether validation has recorded any errors.
    pub fn has_errors(&self) -> bool {
        !self.validation_errors.is_empty()
    }

    /// Returns whether any argument value has been edited.
    pub fn dirty(&self) -> bool {
        self.dirty
    }

    /// Returns the unnormalized value for an argument.
    pub fn raw_value(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }

    /// Replaces a known argument value and clears its validation error.
    pub fn set_value(&mut self, name: &str, value: String) {
        if let Some(slot) = self.values.get_mut(name) {
            *slot = value;
            self.dirty = true;
            self.validation_errors.remove(name);
        }
    }

    /// Returns the specification for the focused argument.
    pub fn current_arg<'a>(&self, command: &'a CommandSpec) -> Option<&'a ArgSpec> {
        command.args.get(self.focused_arg)
    }

    /// Moves focus to the argument at `index`, clamped to the last argument.
    pub fn focus_arg(&mut self, command: &CommandSpec, index: usize) {
        self.focused_arg = index.min(command.args.len().saturating_sub(1));
    }

    /// Returns the position of the first argument that failed validation.
    pub fn first_invalid_arg(&self, command: &CommandSpec) -> Option<usize> {
        command
            .args
            .iter()
            .position(|arg| self.validation_errors.contains_key(arg.name))
    }

    /// Toggles the focused argument when it is a Boolean.
    pub fn toggle_bool_current(&mut self, command: &CommandSpec) {
        let Some(arg) = self.current_arg(command) else {
            return;
        };
        if !matches!(arg.kind, ArgKind::Bool { .. }) {
            return;
        }
        let current = self.bool_value(arg.name).unwrap_or(false);
        self.set_value(arg.name, (!current).to_string());
    }

    /// Selects the next or previous value for a focused enum argument.
    pub fn cycle_enum_current(&mut self, command: &CommandSpec, reverse: bool) {
        let Some(arg) = self.current_arg(command) else {
            return;
        };
        let ArgKind::Enum { values, default } = &arg.kind else {
            return;
        };
        if values.is_empty() {
            self.set_value(arg.name, (*default).to_string());
            return;
        }
        let current = self.raw_value(arg.name).unwrap_or(default);
        let index = values.iter().position(|value| *value == current).unwrap_or(0);
        let next = if reverse {
            index.checked_sub(1).unwrap_or(values.len() - 1)
        } else {
            (index + 1) % values.len()
        };
        self.set_value(arg.name, values[next].to_string());
    }

    /// Validates all form values against the command specification.
    pub fn validate_for(&mut self, command: &CommandSpec) -> bool {
        self.validation_errors.clear();

        for arg in &command.args {
            let value = self.raw_value(arg.name).unwrap_or_default().trim();
            if arg.required && value.is_empty() {
                self.validation_errors
                    .insert(arg.name.to_string(), "required".to_string());
                continue;
            }

            match &arg.kind {
                ArgKind::Number { min, .. } if !value.is_empty() => match value.parse::<i64>() {
                    Ok(parsed) if min.is_none_or(|min| parsed >= min) => {}
                    Ok(_) => {
                        self.validation_errors
                            .insert(arg.name.to_string(), format!("must be >= {}", min.unwrap_or(i64::MIN)));
                    }
                    Err(error) => {
                        self.validation_errors
                            .insert(arg.name.to_string(), format!("invalid number: {error}"));
                    }
                },
                ArgKind::Bool { .. } if !value.is_empty() && !matches!(value, "true" | "false") => {
                    self.validation_errors
                        .insert(arg.name.to_string(), "must be true or false".to_string());
                }
                ArgKind::Enum { values, .. } if !value.is_empty() && !values.contains(&value) => {
                    self.validation_errors
                        .insert(arg.name.to_string(), format!("must be one of: {}", values.join(", ")));
                }
                ArgKind::KeyValueMap if !value.is_empty() => {
                    if let Err(error) = parse_key_value_map(value) {
                        self.validation_errors.insert(arg.name.to_string(), error);
                    }
                }
                ArgKind::TimestampMillis if !value.is_empty() => {
                    if let Err(error) = value.parse::<u64>() {
                        self.validation_errors
                            .insert(arg.name.to_string(), format!("invalid timestamp: {error}"));
                    }
                }
                _ => {}
            }
        }

        self.validation_errors.is_empty()
    }

    /// Reads a required argument and trims surrounding whitespace.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error for a missing or blank value.
    pub fn required_string(&self, name: &str) -> CanonicalResult<String> {
        self.raw_value(name)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| crate::errors::argument_invalid(format!("{name} is required")))
    }

    /// Reads an optional argument and removes blank values.
    pub fn optional_string(&self, name: &str) -> Option<String> {
        self.raw_value(name)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    }

    /// Reads a required enum argument as its catalog spelling.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error for a missing or blank value.
    pub fn enum_string(&self, name: &str) -> CanonicalResult<String> {
        self.required_string(name)
    }

    /// Parses a required Boolean argument.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error unless the value is `true` or `false`.
    pub fn bool_value(&self, name: &str) -> CanonicalResult<bool> {
        let value = self.required_string(name)?;
        value
            .parse::<bool>()
            .map_err(|error| crate::errors::argument_invalid(format!("{name} must be true or false: {error}")))
    }

    /// Parses a required signed 64-bit integer argument.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error for a missing or invalid `i64`.
    pub fn number_i64(&self, name: &str) -> CanonicalResult<i64> {
        let value = self.required_string(name)?;
        value
            .parse::<i64>()
            .map_err(|error| crate::errors::argument_invalid(format!("{name} must be a signed integer: {error}")))
    }

    /// Parses an optional signed 64-bit integer argument.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error for an invalid nonblank `i64`.
    pub fn optional_i64(&self, name: &str) -> CanonicalResult<Option<i64>> {
        self.optional_string(name)
            .map(|value| {
                value.parse::<i64>().map_err(|error| {
                    crate::errors::argument_invalid(format!("{name} must be a signed integer: {error}"))
                })
            })
            .transpose()
    }

    /// Parses a required signed 32-bit integer argument.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error for a missing, invalid, or out-of-range value.
    pub fn number_i32(&self, name: &str) -> CanonicalResult<i32> {
        let value = self.number_i64(name)?;
        i32::try_from(value)
            .map_err(|error| crate::errors::argument_invalid(format!("{name} is out of range for i32: {error}")))
    }

    /// Parses an optional signed 32-bit integer argument.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error for an invalid or out-of-range nonblank value.
    pub fn optional_i32(&self, name: &str) -> CanonicalResult<Option<i32>> {
        self.optional_i64(name)?
            .map(|value| {
                i32::try_from(value).map_err(|error| {
                    crate::errors::argument_invalid(format!("{name} is out of range for i32: {error}"))
                })
            })
            .transpose()
    }

    /// Parses a required unsigned 64-bit integer argument.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error for a missing or invalid `u64`.
    pub fn number_u64(&self, name: &str) -> CanonicalResult<u64> {
        let value = self.required_string(name)?;
        value
            .parse::<u64>()
            .map_err(|error| crate::errors::argument_invalid(format!("{name} must be an unsigned integer: {error}")))
    }

    /// Parses a required unsigned 32-bit integer argument.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error for a missing, invalid, or out-of-range value.
    pub fn number_u32(&self, name: &str) -> CanonicalResult<u32> {
        let value = self.number_u64(name)?;
        u32::try_from(value)
            .map_err(|error| crate::errors::argument_invalid(format!("{name} is out of range for u32: {error}")))
    }

    /// Parses an optional unsigned 32-bit integer argument.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error for an invalid or out-of-range nonblank value.
    pub fn optional_u32(&self, name: &str) -> CanonicalResult<Option<u32>> {
        self.optional_string(name)
            .map(|value| {
                let parsed = value.parse::<u64>().map_err(|error| {
                    crate::errors::argument_invalid(format!("{name} must be an unsigned integer: {error}"))
                })?;
                u32::try_from(parsed).map_err(|error| {
                    crate::errors::argument_invalid(format!("{name} is out of range for u32: {error}"))
                })
            })
            .transpose()
    }

    /// Parses a required millisecond timestamp as an unsigned integer.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error for a missing or invalid `u64`.
    pub fn timestamp_millis(&self, name: &str) -> CanonicalResult<u64> {
        self.number_u64(name)
    }

    /// Parses a required semicolon- or newline-delimited `key=value` map.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error for a missing or malformed `key=value` entry.
    pub fn key_value_map(&self, name: &str) -> CanonicalResult<BTreeMap<String, String>> {
        let value = self.required_string(name)?;
        parse_key_value_map(&value)
            .map_err(crate::errors::argument_invalid)
            .map(|entries| entries.into_iter().collect())
    }
}

/// Animation clock plus the change log the renderer derives transitions from.
///
/// [`AppState::advance_animation`] observes the state once per frame, so no code
/// that mutates the state has to know that a transition exists.
#[derive(Debug)]
pub struct Motion {
    tick: u64,
    enabled: bool,
    last_activity: u64,
    /// Tick of the most recent change that starts a transition.
    last_change: u64,
    pane: Tracked<Pane>,
    tree_item: Tracked<Option<CommandTreeItem>>,
    command: Tracked<usize>,
    phase: Tracked<(ExecutionPhase, Option<u64>)>,
    overlay: Tracked<Overlay>,
    invalid_at: Option<u64>,
}

impl Motion {
    /// Ambient effects stop after this long without input so an idle screen is static.
    const IDLE_AFTER_TICKS: u64 = 20 * FRAMES_PER_SECOND;
    /// Length of the fade that ends ambient effects.
    const IDLE_FADE_TICKS: u64 = FRAMES_PER_SECOND;
    /// Longest transition a change may start. Once it has passed, only ambient
    /// effects are left, and those do not need every frame.
    pub const TRANSITION_TICKS: u64 = 40;

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Returns whether a transition started by the latest change may still be playing.
    fn in_transition(&self) -> bool {
        self.enabled && self.tick.saturating_sub(self.last_change) < Self::TRANSITION_TICKS
    }

    /// Returns the strength, in `0.0..=1.0`, of continuously running decorative effects.
    ///
    /// The strength fades to zero once the application has been idle for a while.
    pub fn ambient(&self) -> f32 {
        if !self.enabled {
            return 0.0;
        }
        let idle = self.tick.saturating_sub(self.last_activity);
        1.0 - progress(
            idle,
            Self::IDLE_AFTER_TICKS - Self::IDLE_FADE_TICKS,
            Self::IDLE_FADE_TICKS,
        )
    }

    /// Returns the eased progress of a transition, already complete when motion is off.
    pub fn transition(&self, started: u64, duration: u64) -> f32 {
        debug_assert!(
            duration <= Self::TRANSITION_TICKS,
            "the frame pace assumes shorter transitions"
        );
        if self.enabled {
            ease_out_cubic(progress(self.tick, started, duration))
        } else {
            1.0
        }
    }

    /// Returns whether a transition of `duration` ticks that began at `started` is still playing.
    pub fn is_playing(&self, started: u64, duration: u64) -> bool {
        debug_assert!(
            duration <= Self::TRANSITION_TICKS,
            "the frame pace assumes shorter transitions"
        );
        self.enabled && self.tick.saturating_sub(started) < duration
    }

    pub fn pane(&self) -> Tracked<Pane> {
        self.pane
    }

    pub fn tree_item(&self) -> Tracked<Option<CommandTreeItem>> {
        self.tree_item
    }

    pub fn command(&self) -> Tracked<usize> {
        self.command
    }

    pub fn phase(&self) -> Tracked<(ExecutionPhase, Option<u64>)> {
        self.phase
    }

    pub fn overlay(&self) -> Tracked<Overlay> {
        self.overlay
    }

    /// Returns the tick of the most recent rejected submission.
    pub fn invalid_at(&self) -> Option<u64> {
        self.invalid_at
    }
}

#[derive(Debug)]
pub struct AppState {
    commands: Vec<CommandSpec>,
    selected_command_index: usize,
    tree_cursor: usize,
    collapsed_categories: BTreeSet<CommandCategory>,
    pub focus: FocusArea,
    pub namesrv_addr: String,
    pub search: String,
    pub form: CommandFormState,
    pub execution: CommandExecutionState,
    pub last_error: Option<String>,
    pub progress_message: Option<String>,
    pub show_help: bool,
    pub confirm_input: String,
    /// Scroll offset of the open overlay.
    pub overlay_scroll: usize,
    /// Whether the result pane covers the whole workspace.
    pub result_zoom: bool,
    result: Option<ResultView>,
    detail: Option<RowDetail>,
    toast: Option<Toast>,
    /// Edited forms of commands that are not selected, restored on return.
    form_memory: BTreeMap<&'static str, CommandFormState>,
    /// Cursor of the input it was last moved in; other inputs keep theirs at the end.
    cursor: Option<(InputTarget, usize)>,
    /// NameServer address to restore when the edit is abandoned.
    namesrv_before_edit: Option<String>,
    mouse_capture: bool,
    quit_armed_at: Option<u64>,
    run_started_at: Option<Instant>,
    last_run: Option<Duration>,
    motion: Motion,
    next_execution_id: u64,
}

impl AppState {
    /// A second Esc within this window quits the application.
    const QUIT_ARM_TICKS: u64 = 2 * FRAMES_PER_SECOND;

    /// Creates application state from the static command catalog.
    ///
    /// # Panics
    ///
    /// Panics if the static command catalog is empty.
    pub fn new(namesrv_addr: Option<&str>) -> Self {
        let commands = command_catalog();
        let form = CommandFormState::for_command(&commands[0]);
        let focus = FocusArea::CommandTree;
        let mut state = Self {
            commands,
            selected_command_index: 0,
            tree_cursor: 0,
            collapsed_categories: BTreeSet::new(),
            focus,
            namesrv_addr: namesrv_addr.unwrap_or_default().to_string(),
            search: String::new(),
            form,
            execution: CommandExecutionState::Idle,
            last_error: None,
            progress_message: None,
            show_help: false,
            confirm_input: String::new(),
            overlay_scroll: 0,
            result_zoom: false,
            result: None,
            detail: None,
            toast: None,
            form_memory: BTreeMap::new(),
            cursor: None,
            namesrv_before_edit: None,
            mouse_capture: true,
            quit_armed_at: None,
            run_started_at: None,
            last_run: None,
            motion: Motion {
                tick: 0,
                enabled: true,
                last_activity: 0,
                last_change: 0,
                pane: Tracked::new(focus.pane()),
                tree_item: Tracked::new(None),
                command: Tracked::new(0),
                phase: Tracked::new((ExecutionPhase::Idle, None)),
                overlay: Tracked::new(Overlay::None),
                invalid_at: None,
            },
            next_execution_id: 1,
        };
        state.align_tree_cursor_to_selected_command();
        state.motion.tree_item = Tracked::new(state.focused_tree_item());
        state
    }

    /// Returns the complete command catalog.
    pub fn commands(&self) -> &[CommandSpec] {
        &self.commands
    }

    /// Returns the currently selected command.
    pub fn selected_command(&self) -> &CommandSpec {
        &self.commands[self.selected_command_index]
    }

    /// Returns the catalog index of the selected command.
    pub fn selected_command_index(&self) -> usize {
        self.selected_command_index
    }

    /// Returns the current animation tick.
    pub fn animation_tick(&self) -> u64 {
        self.motion.tick
    }

    /// Returns the animation clock and change log.
    pub fn motion(&self) -> &Motion {
        &self.motion
    }

    /// Advances the animation tick and records what changed since the last frame.
    pub fn advance_animation(&mut self) {
        let tick = self.motion.tick.wrapping_add(1);
        let tree_item = self.focused_tree_item();
        let overlay = self.overlay();
        let motion = &mut self.motion;
        motion.tick = tick;
        let mut changed = motion.pane.observe(self.focus.pane(), tick);
        changed |= motion.tree_item.observe(tree_item, tick);
        changed |= motion.command.observe(self.selected_command_index, tick);
        changed |= motion
            .phase
            .observe((self.execution.phase(), self.execution.execution_id()), tick);
        changed |= motion.overlay.observe(overlay, tick);
        if matches!(self.execution, CommandExecutionState::Running { .. }) {
            // A running command is activity even when the operator is only watching.
            motion.last_activity = tick;
        }

        if self
            .toast
            .as_ref()
            .is_some_and(|toast| tick.saturating_sub(toast.created_at) >= Toast::LIFETIME_TICKS)
        {
            self.toast = None;
            changed = true;
        }
        if self
            .quit_armed_at
            .is_some_and(|armed_at| tick.saturating_sub(armed_at) >= Self::QUIT_ARM_TICKS)
        {
            self.quit_armed_at = None;
        }
        if changed {
            self.motion.last_change = tick;
        }
    }

    /// Returns how soon the screen needs another frame when no input arrives.
    ///
    /// An idle screen is not redrawn at all, so it costs neither terminal output nor
    /// rendering time.
    pub fn frame_pace(&self) -> FramePace {
        let motion = &self.motion;
        if motion.last_change == motion.tick || motion.in_transition() {
            FramePace::Full
        } else if matches!(self.execution, CommandExecutionState::Running { .. })
            || self.toast.is_some()
            || motion.ambient() > 0.0
        {
            FramePace::Ambient
        } else {
            FramePace::Still
        }
    }

    /// Records user input so ambient effects keep playing.
    pub fn touch(&mut self) {
        self.motion.last_activity = self.motion.tick;
    }

    /// Switches animations on or off and returns the new setting.
    pub fn toggle_motion(&mut self) -> bool {
        self.motion.enabled = !self.motion.enabled;
        self.motion.enabled
    }

    /// Marks the current submission as rejected so the offending fields flash.
    pub fn flag_invalid(&mut self) {
        self.motion.invalid_at = Some(self.motion.tick);
        self.motion.last_change = self.motion.tick;
    }

    /// Returns whether the terminal reports mouse events to the application.
    pub fn mouse_capture(&self) -> bool {
        self.mouse_capture
    }

    pub fn set_mouse_capture(&mut self, enabled: bool) {
        self.mouse_capture = enabled;
    }

    /// Arms the quit shortcut, returning `true` when it was already armed.
    pub fn arm_quit(&mut self) -> bool {
        if self.quit_armed_at.is_some() {
            return true;
        }
        self.quit_armed_at = Some(self.motion.tick);
        false
    }

    /// Raises a notification, replacing the current one.
    pub fn notify(&mut self, level: ToastLevel, message: impl Into<String>) {
        self.toast = Some(Toast {
            level,
            message: message.into(),
            created_at: self.motion.tick,
        });
        self.motion.last_change = self.motion.tick;
    }

    pub fn toast(&self) -> Option<&Toast> {
        self.toast.as_ref()
    }

    /// Returns the modal layer that currently owns keyboard and mouse input.
    pub fn overlay(&self) -> Overlay {
        if self.show_help {
            Overlay::Help
        } else if matches!(self.execution, CommandExecutionState::Confirming { .. }) {
            Overlay::Confirm
        } else if self.detail.is_some() {
            Overlay::Detail
        } else {
            Overlay::None
        }
    }

    /// Moves focus, discarding per-focus transient state.
    pub fn set_focus(&mut self, focus: FocusArea) {
        if self.focus == focus {
            return;
        }
        self.namesrv_before_edit = (focus == FocusArea::Namesrv).then(|| self.namesrv_addr.clone());
        if focus != FocusArea::Result {
            self.result_zoom = false;
        }
        self.cursor = None;
        self.focus = focus;
    }

    /// Takes the NameServer address that was in effect when its input gained focus.
    pub fn take_namesrv_before_edit(&mut self) -> Option<String> {
        self.namesrv_before_edit.take()
    }

    /// Returns the input that typed characters go to, if any.
    pub fn active_input(&self) -> Option<InputTarget> {
        match self.overlay() {
            Overlay::Confirm => return Some(InputTarget::Confirm),
            Overlay::Help | Overlay::Detail => return None,
            Overlay::None => {}
        }
        match self.focus {
            FocusArea::Namesrv => Some(InputTarget::Namesrv),
            FocusArea::Search => Some(InputTarget::Search),
            FocusArea::Args => {
                let arg = self.form.current_arg(self.selected_command())?;
                arg.kind
                    .choices()
                    .is_none()
                    .then_some(InputTarget::Arg(self.form.focused_arg()))
            }
            FocusArea::CommandTree | FocusArea::Result => None,
        }
    }

    /// Returns the text held by `target`.
    pub fn input_value(&self, target: InputTarget) -> &str {
        match target {
            InputTarget::Namesrv => &self.namesrv_addr,
            InputTarget::Search => &self.search,
            InputTarget::Confirm => &self.confirm_input,
            InputTarget::Arg(index) => self
                .selected_command()
                .args
                .get(index)
                .and_then(|arg| self.form.raw_value(arg.name))
                .unwrap_or_default(),
        }
    }

    /// Returns the cursor of `target` as a character index within its text.
    pub fn input_cursor(&self, target: InputTarget) -> usize {
        let length = self.input_value(target).chars().count();
        match self.cursor {
            Some((owner, position)) if owner == target => position.min(length),
            _ => length,
        }
    }

    pub fn set_input_cursor(&mut self, target: InputTarget, position: usize) {
        self.cursor = Some((target, position));
    }

    /// Moves parameter focus by `delta` fields.
    pub fn move_arg_focus(&mut self, delta: isize) {
        let focused = self.form.focused_arg();
        let target = if delta.is_negative() {
            focused.saturating_sub(delta.unsigned_abs())
        } else {
            focused.saturating_add(delta.unsigned_abs())
        };
        self.focus_arg(target);
    }

    /// Moves parameter focus to the field at `index`, clamped to the last field.
    pub fn focus_arg(&mut self, index: usize) {
        let command = &self.commands[self.selected_command_index];
        self.form.focus_arg(command, index);
        self.cursor = None;
    }

    pub fn result(&self) -> Option<&ResultView> {
        self.result.as_ref()
    }

    pub fn result_mut(&mut self) -> Option<&mut ResultView> {
        self.result.as_mut()
    }

    /// Replaces the displayed result with a freshly prepared one.
    pub fn set_result(&mut self, result: CommandResultViewModel, tone: ResultTone) {
        self.detail = None;
        self.result = Some(ResultView::new(result, tone));
    }

    pub fn clear_result(&mut self) {
        self.detail = None;
        self.result = None;
    }

    pub fn detail(&self) -> Option<&RowDetail> {
        self.detail.as_ref()
    }

    /// Opens the detail overlay for the selected result row, if the result is tabular.
    pub fn open_detail(&mut self) -> bool {
        let Some(result) = &self.result else {
            return false;
        };
        let Some(fields) = result.selected_fields() else {
            return false;
        };
        self.detail = Some(RowDetail {
            title: format!("{} · row {}", result.title(), result.cursor() + 1),
            fields,
        });
        self.overlay_scroll = 0;
        true
    }

    pub fn close_detail(&mut self) {
        self.detail = None;
        self.overlay_scroll = 0;
    }

    /// Starts timing a command execution.
    pub fn mark_run_started(&mut self) {
        self.run_started_at = Some(Instant::now());
        self.last_run = None;
    }

    /// Stops timing the running command and keeps its duration.
    pub fn mark_run_finished(&mut self) {
        if let Some(started_at) = self.run_started_at.take() {
            self.last_run = Some(started_at.elapsed());
        }
    }

    /// Forgets the timing of the last run, for an execution that never started.
    pub fn clear_run_timing(&mut self) {
        self.run_started_at = None;
        self.last_run = None;
    }

    /// Returns how long the running command has taken, or how long the last one took.
    pub fn run_duration(&self) -> Option<Duration> {
        self.run_started_at
            .map(|started_at| started_at.elapsed())
            .or(self.last_run)
    }

    /// Returns catalog indices for commands visible under the current filter.
    pub fn visible_command_indices(&self) -> Vec<usize> {
        self.visible_tree_items()
            .into_iter()
            .filter_map(|item| match item {
                CommandTreeItem::Command(index) => Some(index),
                CommandTreeItem::Category(_) => None,
            })
            .collect()
    }

    /// Builds the visible category and command tree for the current filter.
    pub fn visible_tree_items(&self) -> Vec<CommandTreeItem> {
        let search_active = !self.search.trim().is_empty();
        let mut items = Vec::new();
        let mut last_category = None;

        for (index, command) in self
            .commands
            .iter()
            .enumerate()
            .filter(|(_, command)| command.matches_query(&self.search))
        {
            if last_category != Some(command.category) {
                items.push(CommandTreeItem::Category(command.category));
                last_category = Some(command.category);
            }

            if search_active || !self.collapsed_categories.contains(&command.category) {
                items.push(CommandTreeItem::Command(index));
            }
        }

        items
    }

    /// Returns the cursor position in the visible command tree.
    pub fn tree_cursor(&self) -> usize {
        self.tree_cursor
    }

    /// Returns the visible tree item under the cursor.
    pub fn focused_tree_item(&self) -> Option<CommandTreeItem> {
        self.visible_tree_items().get(self.tree_cursor).copied()
    }

    /// Returns whether a command category is collapsed.
    pub fn is_category_collapsed(&self, category: CommandCategory) -> bool {
        self.collapsed_categories.contains(&category)
    }

    /// Moves the tree cursor by `delta` visible items, stopping at either end.
    pub fn move_tree_cursor(&mut self, delta: isize) {
        let target = if delta.is_negative() {
            self.tree_cursor.saturating_sub(delta.unsigned_abs())
        } else {
            self.tree_cursor.saturating_add(delta.unsigned_abs())
        };
        self.set_tree_cursor(target);
    }

    /// Moves the tree cursor to a visible position and selects a command found there.
    pub fn set_tree_cursor(&mut self, position: usize) {
        let visible = self.visible_tree_items();
        if visible.is_empty() {
            self.tree_cursor = 0;
            return;
        }

        let position = position.min(visible.len() - 1);
        self.tree_cursor = position;
        if let CommandTreeItem::Command(index) = visible[position] {
            self.select_command_index(index);
        }
    }

    /// Toggles the collapsed state of the focused category.
    pub fn toggle_focused_tree_category(&mut self) {
        let Some(category) = self.focused_tree_category() else {
            return;
        };
        if !self.collapsed_categories.insert(category) {
            self.collapsed_categories.remove(&category);
        }
        self.move_tree_cursor_to_category(category);
    }

    /// Collapses the category containing the focused tree item.
    pub fn collapse_focused_tree_category(&mut self) {
        if let Some(category) = self.focused_tree_category() {
            self.collapsed_categories.insert(category);
            self.move_tree_cursor_to_category(category);
        }
    }

    /// Expands the category containing the focused tree item.
    pub fn expand_focused_tree_category(&mut self) {
        if let Some(category) = self.focused_tree_category() {
            self.collapsed_categories.remove(&category);
            self.move_tree_cursor_to_category(category);
        }
    }

    /// Collapses every category and leaves the cursor on the focused one.
    pub fn collapse_all_categories(&mut self) {
        let focused = self.focused_tree_category();
        self.collapsed_categories = self.commands.iter().map(|command| command.category).collect();
        match focused {
            Some(category) => self.move_tree_cursor_to_category(category),
            None => self.ensure_tree_cursor_valid(),
        }
    }

    /// Expands every category and returns the cursor to the selected command.
    pub fn expand_all_categories(&mut self) {
        self.collapsed_categories.clear();
        self.ensure_tree_cursor_valid();
        self.align_tree_cursor_to_selected_command();
    }

    /// Returns the category containing the focused tree item.
    pub fn focused_tree_category(&self) -> Option<CommandCategory> {
        match self.focused_tree_item()? {
            CommandTreeItem::Category(category) => Some(category),
            CommandTreeItem::Command(index) => Some(self.commands[index].category),
        }
    }

    /// Allocates the next local execution identifier.
    pub fn next_execution_id(&mut self) -> u64 {
        let id = self.next_execution_id;
        self.next_execution_id += 1;
        id
    }

    /// Replaces the command filter and realigns the visible selection.
    pub fn set_search(&mut self, search: String) {
        self.search = search;
        self.ensure_tree_cursor_valid();
        self.ensure_selected_visible();
    }

    /// Selects the command at a visible command position when it exists.
    pub fn select_visible_command_at(&mut self, visible_position: usize) {
        if let Some(index) = self.visible_command_indices().get(visible_position).copied() {
            self.select_command_index(index);
        }
    }

    /// Restores the selected command's form to its catalog defaults.
    pub fn reset_form_for_selected_command(&mut self) {
        let command = &self.commands[self.selected_command_index];
        self.form_memory.remove(command.id);
        self.form = CommandFormState::for_command(command);
        self.cursor = None;
    }

    /// Validates the selected command's current form.
    pub fn validate_selected_form(&mut self) -> bool {
        let command = self.selected_command().clone();
        self.form.validate_for(&command)
    }

    /// Returns the confirmation prompt required by the selected command.
    pub fn confirmation_prompt(&self) -> Option<String> {
        let command = self.selected_command();
        command.expected_confirmation(&self.form).map(|expected| {
            if command.risk_level == RiskLevel::Dangerous {
                format!("Type '{expected}' to execute dangerous command {}", command.id)
            } else {
                format!("Type '{expected}' to execute {}", command.id)
            }
        })
    }

    fn select_command_index(&mut self, index: usize) {
        if self.selected_command_index != index {
            let previous_id = self.commands[self.selected_command_index].id;
            let restored = self
                .form_memory
                .remove(self.commands[index].id)
                .unwrap_or_else(|| CommandFormState::for_command(&self.commands[index]));
            let edited = std::mem::replace(&mut self.form, restored);
            if edited.dirty() {
                self.form_memory.insert(previous_id, edited);
            }
            self.selected_command_index = index;
            self.cursor = None;
        }
        self.align_tree_cursor_to_selected_command();
    }

    fn ensure_selected_visible(&mut self) {
        let visible = self.visible_command_indices();
        if visible.is_empty() {
            return;
        }
        if visible.contains(&self.selected_command_index) {
            self.align_tree_cursor_to_selected_command();
        } else {
            self.select_command_index(visible[0]);
        }
    }

    fn ensure_tree_cursor_valid(&mut self) {
        let visible_len = self.visible_tree_items().len();
        if visible_len == 0 {
            self.tree_cursor = 0;
        } else {
            self.tree_cursor = self.tree_cursor.min(visible_len - 1);
        }
    }

    fn move_tree_cursor_to_category(&mut self, category: CommandCategory) {
        match self
            .visible_tree_items()
            .into_iter()
            .position(|item| item == CommandTreeItem::Category(category))
        {
            Some(position) => self.tree_cursor = position,
            None => self.ensure_tree_cursor_valid(),
        }
    }

    fn align_tree_cursor_to_selected_command(&mut self) {
        if let Some(position) = self
            .visible_tree_items()
            .into_iter()
            .position(|item| item == CommandTreeItem::Command(self.selected_command_index))
        {
            self.tree_cursor = position;
        }
    }
}

fn parse_key_value_map(value: &str) -> Result<BTreeMap<String, String>, String> {
    let mut entries = BTreeMap::new();
    for (index, raw_line) in value
        .lines()
        .flat_map(|line| line.split(';'))
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .enumerate()
    {
        let Some((key, value)) = raw_line.split_once('=') else {
            return Err(format!("entry {} must be key=value", index + 1));
        };
        let key = key.trim();
        let value = value.trim();
        if key.is_empty() {
            return Err(format!("entry {} has empty key", index + 1));
        }
        if value.is_empty() {
            return Err(format!("entry {} has empty value", index + 1));
        }
        entries.insert(key.to_string(), value.to_string());
    }
    if entries.is_empty() {
        return Err("at least one key=value entry is required".to_string());
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::AppState;
    use super::CommandExecutionState;
    use super::CommandFormState;
    use super::CommandTreeItem;
    use super::ExecutionPhase;
    use super::FocusArea;
    use super::FramePace;
    use super::InputTarget;
    use super::Motion;
    use super::Overlay;
    use super::Pane;
    use super::Toast;
    use super::ToastLevel;
    use crate::commands::command_catalog;
    use crate::motion::FRAMES_PER_SECOND;
    use crate::result_view::ResultTone;
    use crate::view_model::CommandResultViewModel;

    #[test]
    fn form_validates_required_number_enum_map_and_timestamp_fields() {
        let catalog = command_catalog();
        let command = catalog
            .iter()
            .find(|command| command.id == "broker.config.update_apply")
            .unwrap();
        let mut form = CommandFormState::for_command(command);

        assert!(!form.validate_for(command));
        assert!(form.validation_errors().contains_key("entries"));

        form.set_value("broker_addr", "127.0.0.1:10911".to_string());
        form.set_value("entries", "flushDiskType=ASYNC_FLUSH".to_string());
        assert!(form.validate_for(command));

        form.set_value("entries", "broken".to_string());
        assert!(!form.validate_for(command));
    }

    #[test]
    fn timestamp_validation_rejects_non_number() {
        let catalog = command_catalog();
        let command = catalog
            .iter()
            .find(|command| command.id == "offset.reset_by_time")
            .unwrap();
        let mut form = CommandFormState::for_command(command);

        form.set_value("group", "GroupA".to_string());
        form.set_value("topic", "TopicA".to_string());
        form.set_value("timestamp", "abc".to_string());

        assert!(!form.validate_for(command));
        assert!(form.validation_errors().contains_key("timestamp"));
    }

    #[test]
    fn app_state_filters_commands_by_search() {
        let mut state = AppState::new(Some("127.0.0.1:9876"));
        state.set_search("producer".to_string());

        let visible = state.visible_command_indices();
        assert!(!visible.is_empty());
        assert!(visible
            .iter()
            .all(|index| state.commands()[*index].matches_query("producer")));
    }

    #[test]
    fn command_tree_collapses_current_category() {
        let mut state = AppState::new(None);
        let category = state.selected_command().category;

        state.collapse_focused_tree_category();

        assert!(state.is_category_collapsed(category));
        assert!(state
            .visible_tree_items()
            .contains(&CommandTreeItem::Category(category)));
        assert!(!state
            .visible_command_indices()
            .iter()
            .any(|index| state.commands()[*index].category == category));
    }

    #[test]
    fn command_tree_search_ignores_collapsed_categories() {
        let mut state = AppState::new(None);
        let command_id = state.selected_command().id.to_string();
        let category = state.selected_command().category;

        state.collapse_focused_tree_category();
        state.set_search(command_id);

        assert!(state.is_category_collapsed(category));
        assert!(matches!(
            state.focused_tree_item(),
            Some(CommandTreeItem::Command(index)) if state.commands()[index].category == category
        ));
    }

    #[test]
    fn command_tree_cursor_updates_selected_command() {
        let mut state = AppState::new(None);
        let original = state.selected_command().id;

        state.move_tree_cursor(1);

        assert_ne!(state.selected_command().id, original);
        assert!(matches!(
            state.focused_tree_item(),
            Some(CommandTreeItem::Command(index)) if index == state.selected_command_index()
        ));
    }

    #[test]
    fn animation_tick_advances_without_changing_command_state() {
        let mut state = AppState::new(None);
        let selected = state.selected_command_index();

        state.advance_animation();
        state.advance_animation();

        assert_eq!(state.animation_tick(), 2);
        assert_eq!(state.selected_command_index(), selected);
    }

    #[test]
    fn confirmation_prompt_marks_dangerous_commands_and_omits_the_word_for_mutating() {
        let mut state = AppState::new(None);

        state.set_search("auth.user.delete".to_string());
        assert_eq!(state.selected_command().id, "auth.user.delete");
        state.form.set_value("username", "admin-user".to_string());
        let prompt = state.confirmation_prompt().unwrap();
        assert!(prompt.contains("dangerous"));
        assert!(prompt.contains("'admin-user'"));

        state.set_search("auth.user.update".to_string());
        assert_eq!(state.selected_command().id, "auth.user.update");
        let prompt = state.confirmation_prompt().unwrap();
        assert!(!prompt.contains("dangerous"));
        assert!(prompt.contains("'confirm'"));
    }

    #[test]
    fn confirmation_prompt_is_none_without_confirmation_requirement() {
        let mut state = AppState::new(None);

        state.set_search("message.decode_id".to_string());
        assert_eq!(state.selected_command().id, "message.decode_id");

        assert!(state.confirmation_prompt().is_none());
    }

    #[test]
    fn motion_records_when_observed_values_change() {
        let mut state = AppState::new(None);
        for _ in 0..5 {
            state.advance_animation();
        }
        assert_eq!(state.motion().pane().changed_at(), 0);

        state.focus = FocusArea::Args;
        state.execution = CommandExecutionState::Running {
            execution_id: 3,
            command_id: "topic.list".to_string(),
        };
        state.advance_animation();

        let motion = state.motion();
        assert_eq!(motion.pane().current(), Pane::Command);
        assert_eq!(motion.pane().previous(), Pane::Sidebar);
        assert_eq!(motion.pane().changed_at(), 6);
        assert_eq!(motion.phase().current(), (ExecutionPhase::Running, Some(3)));
        assert_eq!(motion.phase().changed_at(), 6);
        assert_eq!(motion.command().changed_at(), 0, "the selection did not change");
        assert!(motion.is_playing(6, 4));
        assert!(motion.transition(6, 4) < 1.0);
    }

    #[test]
    fn moving_within_a_pane_is_not_a_pane_change() {
        let mut state = AppState::new(None);
        state.advance_animation();

        state.focus = FocusArea::Search;
        state.advance_animation();

        assert_eq!(state.motion().pane().changed_at(), 0);
    }

    #[test]
    fn switching_motion_off_completes_every_transition() {
        let mut state = AppState::new(None);
        state.advance_animation();
        assert!(state.motion().transition(1, 10) < 1.0);
        assert!(state.motion().ambient() > 0.0);

        assert!(!state.toggle_motion());

        assert_eq!(state.motion().transition(1, 10), 1.0);
        assert!(!state.motion().is_playing(1, 10));
        assert_eq!(state.motion().ambient(), 0.0);
    }

    #[test]
    fn ambient_effects_fade_out_when_idle_and_stay_on_while_a_command_runs() {
        let mut state = AppState::new(None);
        for _ in 0..(21 * FRAMES_PER_SECOND) {
            state.advance_animation();
        }
        assert_eq!(state.motion().ambient(), 0.0);

        state.touch();
        assert_eq!(state.motion().ambient(), 1.0);

        state.execution = CommandExecutionState::Running {
            execution_id: 1,
            command_id: "consumer.start_monitoring".to_string(),
        };
        for _ in 0..(40 * FRAMES_PER_SECOND) {
            state.advance_animation();
        }
        assert_eq!(state.motion().ambient(), 1.0);
    }

    #[test]
    fn a_toast_expires_and_a_newer_one_replaces_it() {
        let mut state = AppState::new(None);
        state.notify(ToastLevel::Info, "first");
        state.notify(ToastLevel::Error, "second");
        assert_eq!(state.toast().unwrap().message, "second");
        assert_eq!(state.toast().unwrap().level, ToastLevel::Error);

        for _ in 0..Toast::LIFETIME_TICKS - 1 {
            state.advance_animation();
        }
        assert!(state.toast().is_some());
        state.advance_animation();
        assert!(state.toast().is_none());
    }

    #[test]
    fn overlays_take_input_in_a_fixed_order() {
        let mut state = AppState::new(None);
        assert_eq!(state.overlay(), Overlay::None);
        assert_eq!(state.active_input(), None);

        state.execution = CommandExecutionState::Confirming {
            execution_id: 1,
            command_id: "topic.delete".to_string(),
            expected: "TopicA".to_string(),
        };
        assert_eq!(state.overlay(), Overlay::Confirm);
        assert_eq!(state.active_input(), Some(InputTarget::Confirm));

        state.show_help = true;
        assert_eq!(state.overlay(), Overlay::Help);
        assert_eq!(state.active_input(), None);
    }

    #[test]
    fn each_input_keeps_its_own_cursor() {
        let mut state = AppState::new(Some("127.0.0.1:9876"));
        state.search = "topic".to_string();

        assert_eq!(
            state.input_cursor(InputTarget::Search),
            5,
            "an untouched input ends with its cursor"
        );
        state.set_input_cursor(InputTarget::Search, 2);
        assert_eq!(state.input_cursor(InputTarget::Search), 2);
        assert_eq!(state.input_cursor(InputTarget::Namesrv), 14);

        state.search = "t".to_string();
        assert_eq!(
            state.input_cursor(InputTarget::Search),
            1,
            "the cursor never passes the text"
        );

        state.set_focus(FocusArea::Search);
        assert_eq!(state.input_cursor(InputTarget::Search), 1);
        state.set_input_cursor(InputTarget::Search, 0);
        state.set_focus(FocusArea::CommandTree);
        assert_eq!(
            state.input_cursor(InputTarget::Search),
            1,
            "leaving an input resets its cursor"
        );
    }

    #[test]
    fn only_typed_parameters_are_text_inputs() {
        let mut state = AppState::new(None);
        state.set_search("topic.update".to_string());
        state.focus = FocusArea::Args;

        assert_eq!(state.active_input(), Some(InputTarget::Arg(0)));
        state.focus_arg(1);
        assert_eq!(state.active_input(), None, "an enumeration is chosen, not typed");
        state.move_arg_focus(1);
        assert_eq!(state.active_input(), Some(InputTarget::Arg(2)));
        state.move_arg_focus(99);
        assert_eq!(state.form.focused_arg(), 8);
        state.move_arg_focus(-99);
        assert_eq!(state.form.focused_arg(), 0);

        state.form.set_value("topic", "TopicA".to_string());
        assert_eq!(state.input_value(InputTarget::Arg(0)), "TopicA");
        assert_eq!(state.input_value(InputTarget::Arg(99)), "");
    }

    #[test]
    fn focusing_the_nameserver_field_remembers_the_address_to_restore() {
        let mut state = AppState::new(Some("127.0.0.1:9876"));

        state.set_focus(FocusArea::Namesrv);
        state.namesrv_addr.push('0');
        assert_eq!(state.take_namesrv_before_edit().as_deref(), Some("127.0.0.1:9876"));
        assert_eq!(state.take_namesrv_before_edit(), None);

        state.set_focus(FocusArea::CommandTree);
        state.set_focus(FocusArea::Namesrv);
        state.set_focus(FocusArea::Search);
        assert_eq!(
            state.take_namesrv_before_edit(),
            None,
            "leaving the field keeps the edit"
        );
    }

    #[test]
    fn leaving_the_result_pane_ends_the_zoom() {
        let mut state = AppState::new(None);
        state.set_focus(FocusArea::Result);
        state.result_zoom = true;

        state.set_focus(FocusArea::Result);
        assert!(state.result_zoom);
        state.set_focus(FocusArea::Args);
        assert!(!state.result_zoom);
    }

    #[test]
    fn edited_forms_are_remembered_per_command() {
        let mut state = AppState::new(None);
        state.set_search("topic.route".to_string());
        state.form.set_value("topic", "TopicA".to_string());

        state.set_search("topic.status".to_string());
        assert_eq!(state.selected_command().id, "topic.status");
        assert_eq!(state.form.raw_value("topic"), Some(""));
        assert!(!state.form.dirty());

        state.set_search("topic.route".to_string());
        assert_eq!(state.form.raw_value("topic"), Some("TopicA"));
        assert_eq!(state.form.command_id(), "topic.route");

        state.reset_form_for_selected_command();
        assert_eq!(state.form.raw_value("topic"), Some(""));
    }

    #[test]
    fn folding_a_group_leaves_the_cursor_on_its_heading() {
        let mut state = AppState::new(None);
        state.move_tree_cursor(3);
        let category = state.selected_command().category;

        state.collapse_focused_tree_category();
        assert_eq!(state.focused_tree_item(), Some(CommandTreeItem::Category(category)));

        state.toggle_focused_tree_category();
        assert!(!state.is_category_collapsed(category));
        assert_eq!(state.focused_tree_item(), Some(CommandTreeItem::Category(category)));

        state.set_tree_cursor(usize::MAX);
        assert_eq!(state.tree_cursor(), state.visible_tree_items().len() - 1);
        assert_eq!(
            state.focused_tree_item(),
            Some(CommandTreeItem::Command(state.selected_command_index()))
        );
    }

    #[test]
    fn row_details_exist_only_for_tabular_results() {
        let mut state = AppState::new(None);
        assert!(!state.open_detail());

        state.set_result(
            CommandResultViewModel::Text {
                title: "Topics".to_string(),
                body: "TopicA".to_string(),
            },
            ResultTone::Normal,
        );
        assert!(!state.open_detail());

        state.set_result(
            CommandResultViewModel::KeyValue(crate::view_model::KeyValueViewModel {
                title: "Config".to_string(),
                rows: vec![("brokerName".to_string(), "broker-a".to_string())],
            }),
            ResultTone::Normal,
        );
        state.overlay_scroll = 9;
        assert!(state.open_detail());
        assert_eq!(state.overlay(), Overlay::Detail);
        assert_eq!(state.overlay_scroll, 0);
        let detail = state.detail().unwrap();
        assert_eq!(detail.title, "Config · row 1");
        assert_eq!(
            detail.fields,
            vec![
                ("Key".to_string(), "brokerName".to_string()),
                ("Value".to_string(), "broker-a".to_string()),
            ]
        );

        state.clear_result();
        assert_eq!(state.overlay(), Overlay::None, "details do not outlive their result");
    }

    #[test]
    fn the_quit_shortcut_arms_once_and_expires() {
        let mut state = AppState::new(None);

        assert!(!state.arm_quit());
        assert!(state.arm_quit());
        for _ in 0..AppState::QUIT_ARM_TICKS {
            state.advance_animation();
        }
        assert!(!state.arm_quit());
    }

    #[test]
    fn the_frame_pace_drops_as_the_screen_settles() {
        let mut state = AppState::new(None);
        state.advance_animation();
        assert_eq!(state.frame_pace(), FramePace::Full, "the intro is a transition");

        for _ in 0..Motion::TRANSITION_TICKS {
            state.advance_animation();
        }
        assert_eq!(state.frame_pace(), FramePace::Ambient);

        for _ in 0..(21 * FRAMES_PER_SECOND) {
            state.advance_animation();
        }
        assert_eq!(state.frame_pace(), FramePace::Still, "an idle screen is not redrawn");

        state.focus = FocusArea::Args;
        state.advance_animation();
        assert_eq!(state.frame_pace(), FramePace::Full);
    }

    #[test]
    fn a_running_command_and_a_toast_keep_frames_coming_without_motion() {
        let mut state = AppState::new(None);
        state.toggle_motion();
        for _ in 0..3 {
            state.advance_animation();
        }
        assert_eq!(state.frame_pace(), FramePace::Still);

        state.execution = CommandExecutionState::Running {
            execution_id: 1,
            command_id: "topic.list".to_string(),
        };
        state.advance_animation();
        assert_eq!(state.frame_pace(), FramePace::Full, "a change is drawn at once");
        state.advance_animation();
        assert_eq!(
            state.frame_pace(),
            FramePace::Ambient,
            "the elapsed time keeps counting"
        );

        state.execution = CommandExecutionState::Idle;
        state.advance_animation();
        state.advance_animation();
        assert_eq!(state.frame_pace(), FramePace::Still);

        state.notify(ToastLevel::Info, "saved");
        assert_eq!(state.frame_pace(), FramePace::Full);
        state.advance_animation();
        assert_eq!(state.frame_pace(), FramePace::Ambient);
        for _ in 0..Toast::LIFETIME_TICKS - 2 {
            state.advance_animation();
        }
        assert!(state.toast().is_some());
        state.advance_animation();
        assert!(state.toast().is_none());
        assert_eq!(
            state.frame_pace(),
            FramePace::Full,
            "the frame that removes the toast is drawn"
        );
        state.advance_animation();
        assert_eq!(state.frame_pace(), FramePace::Still);
    }
}
