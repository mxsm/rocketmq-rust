// Copyright 2023 The RocketMQ Rust Authors
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

use crate::view_model::CommandResultViewModel;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Quit,
    FocusNext,
    FocusPrevious,
    FocusSearch,
    FocusNamesrv,
    SearchChanged(String),
    NamesrvChanged(String),
    CommandSelected(String),
    ArgChanged {
        name: String,
        value: String,
    },
    ExecuteRequested,
    ConfirmRequested {
        execution_id: u64,
        command_id: String,
        expected: String,
    },
    CommandStarted {
        execution_id: u64,
        command_id: String,
    },
    CommandSucceeded {
        execution_id: u64,
        command_id: String,
        result: CommandResultViewModel,
    },
    CommandFailed {
        execution_id: u64,
        command_id: String,
        error: String,
    },
    ProgressUpdated {
        execution_id: u64,
        message: String,
    },
    CancelExecution {
        execution_id: u64,
        command_id: String,
    },
    HelpToggled,
    ResultCleared,
}

impl Action {
    pub(crate) fn retained_bytes(&self) -> usize {
        let dynamic = match self {
            Self::SearchChanged(value) | Self::NamesrvChanged(value) | Self::CommandSelected(value) => value.len(),
            Self::ArgChanged { name, value } => name.len().saturating_add(value.len()),
            Self::ConfirmRequested {
                command_id, expected, ..
            } => command_id.len().saturating_add(expected.len()),
            Self::CommandStarted { command_id, .. } | Self::CancelExecution { command_id, .. } => command_id.len(),
            Self::CommandSucceeded { command_id, result, .. } => {
                command_id.len().saturating_add(result.retained_bytes())
            }
            Self::CommandFailed { command_id, error, .. } => command_id.len().saturating_add(error.len()),
            Self::ProgressUpdated { message, .. } => message.len(),
            Self::Quit
            | Self::FocusNext
            | Self::FocusPrevious
            | Self::FocusSearch
            | Self::FocusNamesrv
            | Self::ExecuteRequested
            | Self::HelpToggled
            | Self::ResultCleared => 0,
        };
        std::mem::size_of::<Self>().saturating_add(dynamic)
    }
}
