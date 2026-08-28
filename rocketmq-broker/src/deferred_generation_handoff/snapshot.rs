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

use super::DeferredGeneration;
use super::DeferredGenerationTarget;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeferredGenerationTargetSnapshot {
    pub(crate) target: DeferredGenerationTarget,
    pub(crate) generation: DeferredGeneration,
    pub(crate) legacy_waiters: usize,
    pub(crate) candidates: usize,
    pub(crate) active_wakes: usize,
    pub(crate) wake_gates: usize,
    pub(crate) continuations: usize,
    pub(crate) replay_tokens: usize,
    pub(crate) abandoned_replays: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeferredGenerationHandoffSnapshot {
    pub(crate) default_generation: DeferredGeneration,
    pub(crate) sealed: bool,
    pub(crate) legacy_acceptance_sealed: bool,
    pub(crate) v2_aggregate_published: bool,
    pub(crate) tracked_targets: usize,
    /// Exact legacy V1 waiter occupancy, kept under the lifecycle-report name.
    pub(crate) occupancy: usize,
    pub(crate) candidates: usize,
    pub(crate) active_wakes: usize,
    pub(crate) wake_gates: usize,
    pub(crate) continuations: usize,
    pub(crate) replay_tokens: usize,
    pub(crate) abandoned_replays: usize,
    pub(crate) targets: Vec<DeferredGenerationTargetSnapshot>,
}

impl DeferredGenerationHandoffSnapshot {
    #[must_use]
    pub(crate) const fn is_zero(&self) -> bool {
        self.tracked_targets == 0
            && self.occupancy == 0
            && self.candidates == 0
            && self.active_wakes == 0
            && self.wake_gates == 0
            && self.continuations == 0
            && self.replay_tokens == 0
            && self.abandoned_replays == 0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeferredGenerationHandoffZeroReport {
    pub(crate) snapshot: DeferredGenerationHandoffSnapshot,
}

impl DeferredGenerationHandoffZeroReport {
    #[must_use]
    pub(crate) const fn is_zero(&self) -> bool {
        self.snapshot.is_zero()
    }
}

impl From<DeferredGenerationHandoffSnapshot> for DeferredGenerationHandoffZeroReport {
    fn from(snapshot: DeferredGenerationHandoffSnapshot) -> Self {
        Self { snapshot }
    }
}
