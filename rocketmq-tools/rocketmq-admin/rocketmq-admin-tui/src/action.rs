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
