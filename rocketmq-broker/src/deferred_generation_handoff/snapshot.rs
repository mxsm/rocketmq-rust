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

use super::DeferredGenerationTarget;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeferredGenerationTargetSnapshot {
    pub(crate) target: DeferredGenerationTarget,
    pub(crate) candidates: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeferredGenerationHandoffSnapshot {
    pub(crate) sealed: bool,
    pub(crate) tracked_targets: usize,
    pub(crate) candidates: usize,
    pub(crate) targets: Vec<DeferredGenerationTargetSnapshot>,
}

impl DeferredGenerationHandoffSnapshot {
    #[must_use]
    pub(crate) const fn is_zero(&self) -> bool {
        self.tracked_targets == 0 && self.candidates == 0
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
