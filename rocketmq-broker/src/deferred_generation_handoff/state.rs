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

use std::collections::HashMap;

use super::DeferredGenerationHandoffSnapshot;
use super::DeferredGenerationRouteError;
use super::DeferredGenerationTarget;
use super::DeferredGenerationTargetSnapshot;

#[derive(Debug, Default)]
pub(super) struct DeferredGenerationHandoffState {
    pub(super) shutdown_sealed: bool,
    targets: HashMap<DeferredGenerationTarget, DeferredGenerationTargetState>,
}

impl DeferredGenerationHandoffState {
    pub(super) fn acquire_route(
        &mut self,
        target: &DeferredGenerationTarget,
    ) -> Result<(), DeferredGenerationRouteError> {
        if self.shutdown_sealed {
            return Err(DeferredGenerationRouteError::ShutdownSealed);
        }
        self.targets.entry(target.clone()).or_default().candidates += 1;
        Ok(())
    }

    pub(super) fn release_candidate(&mut self, target: &DeferredGenerationTarget) {
        if let Some(target_state) = self.targets.get_mut(target) {
            target_state.candidates = target_state.candidates.saturating_sub(1);
        }
        self.remove_if_quiescent(target);
    }

    fn remove_if_quiescent(&mut self, target: &DeferredGenerationTarget) {
        if self.targets.get(target).is_some_and(|state| state.candidates == 0) {
            self.targets.remove(target);
        }
    }

    pub(super) fn prune_quiescent_targets(&mut self) {
        self.targets.retain(|_, state| state.candidates != 0);
    }

    pub(super) fn snapshot(&self) -> DeferredGenerationHandoffSnapshot {
        let mut targets = self
            .targets
            .iter()
            .map(|(target, state)| DeferredGenerationTargetSnapshot {
                target: target.clone(),
                candidates: state.candidates,
            })
            .collect::<Vec<_>>();
        targets.sort_by_key(|snapshot| snapshot.target.stable_name());
        DeferredGenerationHandoffSnapshot {
            sealed: self.shutdown_sealed,
            tracked_targets: targets.len(),
            candidates: targets.iter().map(|snapshot| snapshot.candidates).sum(),
            targets,
        }
    }
}

#[derive(Debug, Default)]
struct DeferredGenerationTargetState {
    candidates: usize,
}
