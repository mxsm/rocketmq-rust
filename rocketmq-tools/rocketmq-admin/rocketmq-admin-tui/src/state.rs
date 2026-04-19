use std::collections::BTreeMap;

use crate::commands::command_catalog;
use crate::commands::ArgKind;
use crate::commands::ArgSpec;
use crate::commands::CommandSpec;
use crate::commands::RiskLevel;
use crate::view_model::CommandResultViewModel;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FocusArea {
    Namesrv,
    Search,
    CommandTree,
    Args,
    Result,
}

impl FocusArea {
    pub fn label(self) -> &'static str {
        match self {
            Self::Namesrv => "NameServer",
            Self::Search => "Search",
            Self::CommandTree => "Commands",
            Self::Args => "Args",
            Self::Result => "Result",
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandFormState {
    command_id: String,
    values: BTreeMap<String, String>,
    focused_arg: usize,
    validation_errors: BTreeMap<String, String>,
    dirty: bool,
}

impl CommandFormState {
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

    pub fn command_id(&self) -> &str {
        &self.command_id
    }

    pub fn focused_arg(&self) -> usize {
        self.focused_arg
    }

    pub fn validation_errors(&self) -> &BTreeMap<String, String> {
        &self.validation_errors
    }

    pub fn has_errors(&self) -> bool {
        !self.validation_errors.is_empty()
    }

    pub fn dirty(&self) -> bool {
        self.dirty
    }

    pub fn raw_value(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }

    pub fn set_value(&mut self, name: &str, value: String) {
        if let Some(slot) = self.values.get_mut(name) {
            *slot = value;
            self.dirty = true;
            self.validation_errors.remove(name);
        }
    }

    pub fn current_arg<'a>(&self, command: &'a CommandSpec) -> Option<&'a ArgSpec> {
        command.args.get(self.focused_arg)
    }

    pub fn focus_next_arg(&mut self, command: &CommandSpec) {
        if !command.args.is_empty() {
            self.focused_arg = (self.focused_arg + 1).min(command.args.len() - 1);
        }
    }

    pub fn focus_previous_arg(&mut self) {
        self.focused_arg = self.focused_arg.saturating_sub(1);
    }

    pub fn append_to_current(&mut self, command: &CommandSpec, value: char) {
        let Some(arg) = self.current_arg(command) else {
            return;
        };
        if matches!(arg.kind, ArgKind::Bool { .. } | ArgKind::Enum { .. }) {
            return;
        }
        self.values.entry(arg.name.to_string()).or_default().push(value);
        self.dirty = true;
        self.validation_errors.remove(arg.name);
    }

    pub fn backspace_current(&mut self, command: &CommandSpec) {
        let Some(arg) = self.current_arg(command) else {
            return;
        };
        if let Some(value) = self.values.get_mut(arg.name) {
            value.pop();
            self.dirty = true;
            self.validation_errors.remove(arg.name);
        }
    }

    pub fn toggle_bool_current(&mut self, command: &CommandSpec) {
        let Some(arg) = self.current_arg(command) else {
            return;
        };
        if !matches!(arg.kind, ArgKind::Bool { .. }) {
            self.append_to_current(command, ' ');
            return;
        }
        let current = self.bool_value(arg.name).unwrap_or(false);
        self.set_value(arg.name, (!current).to_string());
    }

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

    pub fn required_string(&self, name: &str) -> anyhow::Result<String> {
        self.raw_value(name)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow::anyhow!("{name} is required"))
    }

    pub fn optional_string(&self, name: &str) -> Option<String> {
        self.raw_value(name)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    }

    pub fn enum_string(&self, name: &str) -> anyhow::Result<String> {
        self.required_string(name)
    }

    pub fn bool_value(&self, name: &str) -> anyhow::Result<bool> {
        let value = self.required_string(name)?;
        value
            .parse::<bool>()
            .map_err(|error| anyhow::anyhow!("{name} must be true or false: {error}"))
    }

    pub fn number_i64(&self, name: &str) -> anyhow::Result<i64> {
        let value = self.required_string(name)?;
        value
            .parse::<i64>()
            .map_err(|error| anyhow::anyhow!("{name} must be a signed integer: {error}"))
    }

    pub fn optional_i64(&self, name: &str) -> anyhow::Result<Option<i64>> {
        self.optional_string(name)
            .map(|value| {
                value
                    .parse::<i64>()
                    .map_err(|error| anyhow::anyhow!("{name} must be a signed integer: {error}"))
            })
            .transpose()
    }

    pub fn number_i32(&self, name: &str) -> anyhow::Result<i32> {
        let value = self.number_i64(name)?;
        i32::try_from(value).map_err(|error| anyhow::anyhow!("{name} is out of range for i32: {error}"))
    }

    pub fn optional_i32(&self, name: &str) -> anyhow::Result<Option<i32>> {
        self.optional_i64(name)?
            .map(|value| {
                i32::try_from(value).map_err(|error| anyhow::anyhow!("{name} is out of range for i32: {error}"))
            })
            .transpose()
    }

    pub fn number_u64(&self, name: &str) -> anyhow::Result<u64> {
        let value = self.required_string(name)?;
        value
            .parse::<u64>()
            .map_err(|error| anyhow::anyhow!("{name} must be an unsigned integer: {error}"))
    }

    pub fn number_u32(&self, name: &str) -> anyhow::Result<u32> {
        let value = self.number_u64(name)?;
        u32::try_from(value).map_err(|error| anyhow::anyhow!("{name} is out of range for u32: {error}"))
    }

    pub fn optional_u32(&self, name: &str) -> anyhow::Result<Option<u32>> {
        self.optional_string(name)
            .map(|value| {
                let parsed = value
                    .parse::<u64>()
                    .map_err(|error| anyhow::anyhow!("{name} must be an unsigned integer: {error}"))?;
                u32::try_from(parsed).map_err(|error| anyhow::anyhow!("{name} is out of range for u32: {error}"))
            })
            .transpose()
    }

    pub fn timestamp_millis(&self, name: &str) -> anyhow::Result<u64> {
        self.number_u64(name)
    }

    pub fn key_value_map(&self, name: &str) -> anyhow::Result<BTreeMap<String, String>> {
        let value = self.required_string(name)?;
        parse_key_value_map(&value)
            .map_err(|error| anyhow::anyhow!("{error}"))
            .map(|entries| entries.into_iter().collect())
    }
}

#[derive(Debug)]
pub struct AppState {
    commands: Vec<CommandSpec>,
    selected_command_index: usize,
    pub focus: FocusArea,
    pub namesrv_addr: String,
    pub search: String,
    pub form: CommandFormState,
    pub execution: CommandExecutionState,
    pub result: Option<CommandResultViewModel>,
    pub last_error: Option<String>,
    pub progress_message: Option<String>,
    pub show_help: bool,
    pub confirm_input: String,
    pub result_scroll: u16,
    pub result_horizontal_scroll: u16,
    next_execution_id: u64,
}

impl AppState {
    pub fn new(namesrv_addr: Option<&str>) -> Self {
        let commands = command_catalog();
        let form = CommandFormState::for_command(&commands[0]);
        Self {
            commands,
            selected_command_index: 0,
            focus: FocusArea::CommandTree,
            namesrv_addr: namesrv_addr.unwrap_or_default().to_string(),
            search: String::new(),
            form,
            execution: CommandExecutionState::Idle,
            result: None,
            last_error: None,
            progress_message: None,
            show_help: false,
            confirm_input: String::new(),
            result_scroll: 0,
            result_horizontal_scroll: 0,
            next_execution_id: 1,
        }
    }

    pub fn commands(&self) -> &[CommandSpec] {
        &self.commands
    }

    pub fn selected_command(&self) -> &CommandSpec {
        &self.commands[self.selected_command_index]
    }

    pub fn selected_command_index(&self) -> usize {
        self.selected_command_index
    }

    pub fn visible_command_indices(&self) -> Vec<usize> {
        self.commands
            .iter()
            .enumerate()
            .filter_map(|(index, command)| command.matches_query(&self.search).then_some(index))
            .collect()
    }

    pub fn next_execution_id(&mut self) -> u64 {
        let id = self.next_execution_id;
        self.next_execution_id += 1;
        id
    }

    pub fn set_search(&mut self, search: String) {
        self.search = search;
        self.ensure_selected_visible();
    }

    pub fn select_next_command(&mut self) {
        self.move_selection(1);
    }

    pub fn select_previous_command(&mut self) {
        self.move_selection(-1);
    }

    pub fn select_visible_command_at(&mut self, visible_position: usize) {
        if let Some(index) = self.visible_command_indices().get(visible_position).copied() {
            self.select_command_index(index);
        }
    }

    pub fn selected_visible_position(&self) -> Option<usize> {
        self.visible_command_indices()
            .into_iter()
            .position(|index| index == self.selected_command_index)
    }

    pub fn reset_form_for_selected_command(&mut self) {
        self.form = CommandFormState::for_command(self.selected_command());
    }

    pub fn validate_selected_form(&mut self) -> bool {
        let command = self.selected_command().clone();
        self.form.validate_for(&command)
    }

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

    fn move_selection(&mut self, delta: isize) {
        let visible = self.visible_command_indices();
        if visible.is_empty() {
            return;
        }
        let current = visible
            .iter()
            .position(|index| *index == self.selected_command_index)
            .unwrap_or(0);
        let next = if delta.is_negative() {
            current.saturating_sub(delta.unsigned_abs())
        } else {
            (current + delta as usize).min(visible.len() - 1)
        };
        self.select_command_index(visible[next]);
    }

    fn select_command_index(&mut self, index: usize) {
        if self.selected_command_index != index {
            self.selected_command_index = index;
            self.reset_form_for_selected_command();
            self.result_scroll = 0;
            self.result_horizontal_scroll = 0;
        }
    }

    fn ensure_selected_visible(&mut self) {
        let visible = self.visible_command_indices();
        if visible.is_empty() {
            return;
        }
        if !visible.contains(&self.selected_command_index) {
            self.select_command_index(visible[0]);
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
    use super::CommandFormState;
    use crate::commands::command_catalog;

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
}
