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

use super::DeferredGeneration;
use super::DeferredGenerationHandoffSnapshot;
use super::DeferredGenerationRouteError;
use super::DeferredGenerationTarget;
use super::DeferredGenerationTargetSnapshot;

#[derive(Debug)]
pub(super) struct DeferredGenerationHandoffState {
    pub(super) default_generation: DeferredGeneration,
    pub(super) legacy_acceptance_sealed: bool,
    pub(super) shutdown_sealed: bool,
    pub(super) v2_aggregate_published: bool,
    pub(super) cutover_transaction_active: bool,
    pub(super) targets: HashMap<DeferredGenerationTarget, DeferredGenerationTargetState>,
}

impl Default for DeferredGenerationHandoffState {
    fn default() -> Self {
        Self {
            default_generation: DeferredGeneration::Legacy,
            legacy_acceptance_sealed: false,
            shutdown_sealed: false,
            v2_aggregate_published: false,
            cutover_transaction_active: false,
            targets: HashMap::new(),
        }
    }
}

impl DeferredGenerationHandoffState {
    pub(super) fn generation_for(&self, target: &DeferredGenerationTarget) -> DeferredGeneration {
        self.targets
            .get(target)
            .map_or(self.default_generation, |target_state| target_state.generation)
    }

    pub(super) fn acquire_route(
        &mut self,
        target: &DeferredGenerationTarget,
    ) -> Result<DeferredGeneration, DeferredGenerationRouteError> {
        if self.shutdown_sealed {
            return Err(DeferredGenerationRouteError::ShutdownSealed);
        }
        let generation = self.generation_for(target);
        self.targets
            .entry(target.clone())
            .or_insert_with(|| DeferredGenerationTargetState::new(generation))
            .candidates += 1;
        Ok(generation)
    }

    pub(super) fn check_legacy_enrollment(
        &self,
        target: &DeferredGenerationTarget,
    ) -> Result<(), LegacyEnrollmentCheckError> {
        if self.shutdown_sealed {
            return Err(LegacyEnrollmentCheckError::ShutdownSealed);
        }
        if self.legacy_acceptance_sealed {
            return Err(LegacyEnrollmentCheckError::LegacyAcceptanceSealed);
        }
        if self.generation_for(target) != DeferredGeneration::Legacy {
            return Err(LegacyEnrollmentCheckError::TargetAlreadyNew);
        }
        Ok(())
    }

    pub(super) fn record_legacy_wait(&mut self, target: &DeferredGenerationTarget) {
        let generation = self.generation_for(target);
        self.targets
            .entry(target.clone())
            .or_insert_with(|| DeferredGenerationTargetState::new(generation))
            .legacy_waiters += 1;
    }

    pub(super) fn begin_legacy_wake(
        &mut self,
        target: &DeferredGenerationTarget,
    ) -> Result<(), LegacyWakeBeginFailure> {
        let Some(target_state) = self.targets.get_mut(target) else {
            return Err(LegacyWakeBeginFailure::NotReady);
        };
        if target_state.generation != DeferredGeneration::Legacy
            || target_state.legacy_waiters == 0
            || target_state.candidates == 0
        {
            return Err(LegacyWakeBeginFailure::NotReady);
        }
        if target.is_pop_lite() && target_state.pop_lite_wake_active {
            return Err(LegacyWakeBeginFailure::PopLiteSingleFlight);
        }
        target_state.legacy_waiters -= 1;
        target_state.candidates -= 1;
        target_state.active_wakes += 1;
        target_state.wake_gates += 1;
        target_state.pop_lite_wake_active = target.is_pop_lite();
        Ok(())
    }

    pub(super) fn wake_into_continuation(&mut self, target: &DeferredGenerationTarget) {
        let Some(target_state) = self.targets.get_mut(target) else {
            return;
        };
        if target_state.wake_gates == 0 {
            return;
        }
        target_state.wake_gates -= 1;
        target_state.continuations += 1;
    }

    pub(super) fn release_candidate(&mut self, target: &DeferredGenerationTarget) {
        if let Some(target_state) = self.targets.get_mut(target) {
            if target_state.candidates > 0 {
                target_state.candidates -= 1;
            }
        }
        self.remove_if_quiescent_and_default(target);
    }

    pub(super) fn release_legacy_wait(&mut self, target: &DeferredGenerationTarget) {
        if let Some(target_state) = self.targets.get_mut(target) {
            if target_state.legacy_waiters > 0 {
                target_state.legacy_waiters -= 1;
            }
        }
        self.remove_if_quiescent_and_default(target);
    }

    pub(super) fn release_wake_gate(&mut self, target: &DeferredGenerationTarget) {
        if let Some(target_state) = self.targets.get_mut(target) {
            if target_state.wake_gates > 0 && target_state.active_wakes > 0 {
                target_state.wake_gates -= 1;
                target_state.active_wakes -= 1;
                if target.is_pop_lite() {
                    target_state.pop_lite_wake_active = false;
                }
            }
        }
        self.remove_if_quiescent_and_default(target);
    }

    pub(super) fn release_continuation(&mut self, target: &DeferredGenerationTarget) {
        if let Some(target_state) = self.targets.get_mut(target) {
            if target_state.continuations > 0 && target_state.active_wakes > 0 {
                target_state.continuations -= 1;
                target_state.active_wakes -= 1;
                if target.is_pop_lite() {
                    target_state.pop_lite_wake_active = false;
                }
            }
        }
        self.remove_if_quiescent_and_default(target);
    }

    pub(super) fn complete_replay_token(&mut self, target: &DeferredGenerationTarget) {
        if let Some(target_state) = self.targets.get_mut(target) {
            if target_state.replay_tokens > 0 {
                target_state.replay_tokens -= 1;
            }
        }
        self.remove_if_quiescent_and_default(target);
    }

    pub(super) fn abandon_replay_token(&mut self, target: &DeferredGenerationTarget) {
        if let Some(target_state) = self.targets.get_mut(target) {
            if target_state.replay_tokens > 0 {
                target_state.replay_tokens -= 1;
                target_state.abandoned_replays += 1;
            }
        }
    }

    pub(super) fn remove_if_quiescent_and_default(&mut self, target: &DeferredGenerationTarget) {
        let remove = self.targets.get(target).is_some_and(|target_state| {
            target_state.is_quiescent() && (target_state.generation == self.default_generation || self.shutdown_sealed)
        });
        if remove {
            self.targets.remove(target);
        }
    }

    pub(super) fn prune_quiescent_targets(&mut self) {
        let default_generation = self.default_generation;
        let shutdown_sealed = self.shutdown_sealed;
        self.targets.retain(|_, target_state| {
            !target_state.is_quiescent() || (target_state.generation != default_generation && !shutdown_sealed)
        });
    }

    pub(super) fn target_snapshot(&self, target: &DeferredGenerationTarget) -> DeferredGenerationTargetSnapshot {
        let target_state = self
            .targets
            .get(target)
            .expect("target state is protected by the handoff write gate");
        DeferredGenerationTargetSnapshot::from_state(target.clone(), target_state)
    }

    pub(super) fn snapshot(&self) -> DeferredGenerationHandoffSnapshot {
        let mut targets = self
            .targets
            .iter()
            .map(|(target, state)| DeferredGenerationTargetSnapshot::from_state(target.clone(), state))
            .collect::<Vec<_>>();
        targets.sort_by_key(|snapshot| snapshot.target.stable_name());
        DeferredGenerationHandoffSnapshot {
            default_generation: self.default_generation,
            sealed: self.shutdown_sealed,
            legacy_acceptance_sealed: self.legacy_acceptance_sealed,
            v2_aggregate_published: self.v2_aggregate_published,
            tracked_targets: targets.len(),
            occupancy: targets.iter().map(|snapshot| snapshot.legacy_waiters).sum(),
            candidates: targets.iter().map(|snapshot| snapshot.candidates).sum(),
            active_wakes: targets.iter().map(|snapshot| snapshot.active_wakes).sum(),
            wake_gates: targets.iter().map(|snapshot| snapshot.wake_gates).sum(),
            continuations: targets.iter().map(|snapshot| snapshot.continuations).sum(),
            replay_tokens: targets.iter().map(|snapshot| snapshot.replay_tokens).sum(),
            abandoned_replays: targets.iter().map(|snapshot| snapshot.abandoned_replays).sum(),
            targets,
        }
    }
}

#[derive(Debug)]
pub(super) struct DeferredGenerationTargetState {
    pub(super) generation: DeferredGeneration,
    pub(super) legacy_waiters: usize,
    pub(super) candidates: usize,
    pub(super) active_wakes: usize,
    pub(super) wake_gates: usize,
    pub(super) continuations: usize,
    pub(super) replay_tokens: usize,
    pub(super) abandoned_replays: usize,
    pop_lite_wake_active: bool,
}

impl DeferredGenerationTargetState {
    pub(super) const fn new(generation: DeferredGeneration) -> Self {
        Self {
            generation,
            legacy_waiters: 0,
            candidates: 0,
            active_wakes: 0,
            wake_gates: 0,
            continuations: 0,
            replay_tokens: 0,
            abandoned_replays: 0,
            pop_lite_wake_active: false,
        }
    }

    pub(super) const fn is_drained(&self) -> bool {
        self.legacy_waiters == 0
            && self.candidates == 0
            && self.active_wakes == 0
            && self.wake_gates == 0
            && self.continuations == 0
    }

    const fn is_quiescent(&self) -> bool {
        self.is_drained() && self.replay_tokens == 0 && self.abandoned_replays == 0
    }
}

impl DeferredGenerationTargetSnapshot {
    fn from_state(target: DeferredGenerationTarget, state: &DeferredGenerationTargetState) -> Self {
        Self {
            target,
            generation: state.generation,
            legacy_waiters: state.legacy_waiters,
            candidates: state.candidates,
            active_wakes: state.active_wakes,
            wake_gates: state.wake_gates,
            continuations: state.continuations,
            replay_tokens: state.replay_tokens,
            abandoned_replays: state.abandoned_replays,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LegacyEnrollmentCheckError {
    ShutdownSealed,
    LegacyAcceptanceSealed,
    TargetAlreadyNew,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LegacyWakeBeginFailure {
    NotReady,
    PopLiteSingleFlight,
}
