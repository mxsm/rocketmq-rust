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

use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum LocalStoreState {
    Created = 0,
    Initialized = 1,
    Started = 2,
    Shutdown = 3,
    RecoveringConsumeQueue = 4,
    RecoveringCommitLog = 5,
    RecoveringTopicQueueTable = 6,
}

impl LocalStoreState {
    pub const fn is_recovering(self) -> bool {
        matches!(
            self,
            Self::RecoveringConsumeQueue | Self::RecoveringCommitLog | Self::RecoveringTopicQueueTable
        )
    }

    const fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Initialized,
            2 => Self::Started,
            3 => Self::Shutdown,
            4 => Self::RecoveringConsumeQueue,
            5 => Self::RecoveringCommitLog,
            6 => Self::RecoveringTopicQueueTable,
            _ => Self::Created,
        }
    }
}

pub struct LocalStoreLifecycle {
    state: AtomicU8,
}

impl Default for LocalStoreLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalStoreLifecycle {
    pub const fn new() -> Self {
        Self {
            state: AtomicU8::new(LocalStoreState::Created as u8),
        }
    }

    pub fn state(&self) -> LocalStoreState {
        LocalStoreState::from_u8(self.state.load(Ordering::Acquire))
    }

    pub fn transition_to(&self, state: LocalStoreState) {
        self.state.store(state as u8, Ordering::Release);
    }

    pub fn is_available_for_io(&self, shutdown_requested: bool) -> bool {
        let state = self.state();
        !shutdown_requested && state != LocalStoreState::Shutdown && !state.is_recovering()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_tracks_recovery_and_io_availability() {
        let lifecycle = LocalStoreLifecycle::new();
        assert!(lifecycle.is_available_for_io(false));

        lifecycle.transition_to(LocalStoreState::RecoveringCommitLog);
        assert!(lifecycle.state().is_recovering());
        assert!(!lifecycle.is_available_for_io(false));

        lifecycle.transition_to(LocalStoreState::Started);
        assert!(lifecycle.is_available_for_io(false));
        assert!(!lifecycle.is_available_for_io(true));
    }
}
