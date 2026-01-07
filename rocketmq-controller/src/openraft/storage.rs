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

//! Storage layer for OpenRaft
//!
//! This module provides the storage implementation for OpenRaft,
//! including log storage and state machine.

use super::log_store::LogStore;
use super::state_machine::StateMachine;

/// Combined store containing both log store and state machine
///
/// In OpenRaft 0.10, the log store and state machine are separate components
/// that are passed to Raft::new() independently.
#[derive(Clone)]
pub struct Store {
    pub log_store: LogStore,
    pub state_machine: StateMachine,
}

impl Store {
    /// Create a new store
    pub fn new() -> Self {
        Self {
            log_store: LogStore::new(),
            state_machine: StateMachine::new(),
        }
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}
