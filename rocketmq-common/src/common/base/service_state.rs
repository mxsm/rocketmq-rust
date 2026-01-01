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

use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceState {
    /// Service just created, not started
    CreateJust,
    /// Service running
    Running,
    /// Service shutdown
    ShutdownAlready,
    /// Service start failure
    StartFailed,
}

impl Display for ServiceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServiceState::CreateJust => write!(f, "CreateJust"),
            ServiceState::Running => write!(f, "Running"),
            ServiceState::ShutdownAlready => write!(f, "ShutdownAlready"),
            ServiceState::StartFailed => write!(f, "StartFailed"),
        }
    }
}
