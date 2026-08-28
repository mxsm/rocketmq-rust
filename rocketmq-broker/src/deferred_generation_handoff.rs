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

//! Broker-private accounting and cutover control for deferred generations.

use std::sync::Arc;

use cheetah_string::CheetahString;
use parking_lot::Mutex;

mod cutover_replay;
mod lease;
mod snapshot;
mod state;
mod target;
#[cfg(test)]
mod tests;

pub(crate) use cutover_replay::*;
pub(crate) use lease::*;
pub(crate) use snapshot::*;
use state::DeferredGenerationHandoffState;
pub(crate) use target::*;

/// The one Broker-owned coordinator. Its write gate serializes enrollment,
/// route acquisition, explicit target transitions, and the MIG-05 transaction.
#[derive(Debug)]
pub(crate) struct DeferredGenerationHandoff {
    write_gate: Arc<Mutex<DeferredGenerationHandoffState>>,
}

impl Default for DeferredGenerationHandoff {
    fn default() -> Self {
        Self::new()
    }
}

impl DeferredGenerationHandoff {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            write_gate: Arc::new(Mutex::new(DeferredGenerationHandoffState::default())),
        }
    }

    #[must_use]
    pub(crate) fn default_generation(&self) -> DeferredGeneration {
        self.write_gate.lock().default_generation
    }

    #[must_use]
    pub(crate) fn generation_for(&self, target: &DeferredGenerationTarget) -> DeferredGeneration {
        self.write_gate.lock().generation_for(target)
    }

    pub(crate) fn seal(&self) -> DeferredGenerationSeal {
        let mut state = self.write_gate.lock();
        if state.shutdown_sealed {
            DeferredGenerationSeal::AlreadySealed
        } else {
            state.shutdown_sealed = true;
            state.legacy_acceptance_sealed = true;
            state.prune_quiescent_targets();
            DeferredGenerationSeal::Sealed
        }
    }

    #[must_use]
    pub(crate) fn is_sealed(&self) -> bool {
        self.write_gate.lock().shutdown_sealed
    }

    pub(crate) fn acquire_route(
        &self,
        target: DeferredGenerationTarget,
    ) -> Result<RoutePermit, DeferredGenerationRouteError> {
        let generation = self.write_gate.lock().acquire_route(&target)?;
        Ok(RoutePermit::committed(Arc::clone(&self.write_gate), target, generation))
    }

    /// Compatibility spelling retained while producer code moves to routes.
    pub(crate) fn acquire_candidate(
        &self,
        target: DeferredGenerationTarget,
    ) -> Result<RoutePermit, DeferredGenerationRouteError> {
        self.acquire_route(target)
    }

    pub(crate) fn acquire_pop_candidate(
        &self,
        topic: CheetahString,
        consumer_group: CheetahString,
        queue_id: i32,
    ) -> Result<RoutePermit, DeferredGenerationRouteError> {
        self.acquire_route(DeferredGenerationTarget::pop(topic, consumer_group, queue_id))
    }

    pub(crate) fn acquire_notification_candidate(
        &self,
        topic: CheetahString,
        consumer_group: CheetahString,
        queue_id: i32,
    ) -> Result<RoutePermit, DeferredGenerationRouteError> {
        self.acquire_route(DeferredGenerationTarget::notification(topic, consumer_group, queue_id))
    }

    pub(crate) fn acquire_pull_candidate(
        &self,
        topic: CheetahString,
        queue_id: i32,
    ) -> Result<RoutePermit, DeferredGenerationRouteError> {
        self.acquire_route(DeferredGenerationTarget::pull(topic, queue_id))
    }

    pub(crate) fn acquire_pop_lite_candidate(
        &self,
        client_id: CheetahString,
    ) -> Result<RoutePermit, DeferredGenerationRouteError> {
        self.acquire_route(DeferredGenerationTarget::pop_lite(client_id))
    }

    #[must_use]
    pub(crate) fn arrival_adapter(&self) -> DeferredGenerationArrivalAdapter<'_> {
        DeferredGenerationArrivalAdapter { handoff: self }
    }

    pub(crate) fn cutover_transaction(&self) -> Result<DeferredGenerationCutover<'_>, DeferredGenerationCutoverError> {
        let mut state = self.write_gate.lock();
        if state.shutdown_sealed {
            return Err(DeferredGenerationCutoverError::ShutdownSealed);
        }
        if state.default_generation == DeferredGeneration::New {
            return Err(DeferredGenerationCutoverError::AlreadyPublishedNew);
        }
        if state.cutover_transaction_active {
            return Err(DeferredGenerationCutoverError::TransactionActive);
        }
        let stage = if state.v2_aggregate_published {
            DeferredGenerationCutoverStage::V2AggregatePublished
        } else if state.legacy_acceptance_sealed {
            DeferredGenerationCutoverStage::LegacyAcceptanceSealed
        } else {
            DeferredGenerationCutoverStage::Open
        };
        state.cutover_transaction_active = true;
        Ok(DeferredGenerationCutover { state, stage })
    }

    /// Runs one legacy-table operation with the coordinator write gate held.
    ///
    /// Callers may acquire a legacy table or queue only from `operation`.
    /// The global lock order is therefore coordinator gate -> legacy table;
    /// no legacy-table operation may call back into this coordinator.
    fn with_legacy_table_transaction<R>(
        &self,
        operation: impl FnOnce(&mut DeferredGenerationHandoffState, &Arc<Mutex<DeferredGenerationHandoffState>>) -> R,
    ) -> R {
        let write_gate = Arc::clone(&self.write_gate);
        let mut state = write_gate.lock();
        operation(&mut state, &write_gate)
    }

    /// Changes one drained target from Legacy to New and authorizes its replay.
    /// The injected legacy-table probe runs under the write gate and must not
    /// call back into this coordinator.
    pub(crate) fn try_transition_target_to_new<F>(
        &self,
        target: DeferredGenerationTarget,
        legacy_table_probe: F,
    ) -> Result<ReplayToken, DeferredGenerationTargetTransitionError>
    where
        F: FnOnce(&DeferredGenerationTarget) -> bool,
    {
        {
            let state = self.write_gate.lock();
            if state.shutdown_sealed {
                return Err(DeferredGenerationTargetTransitionError::ShutdownSealed);
            }
            if state.default_generation != DeferredGeneration::New || !state.v2_aggregate_published {
                return Err(DeferredGenerationTargetTransitionError::CutoverNotPublished);
            }
            let Some(target_state) = state.targets.get(&target) else {
                return Err(DeferredGenerationTargetTransitionError::TargetAbsent);
            };
            if target_state.generation == DeferredGeneration::New {
                return Err(DeferredGenerationTargetTransitionError::TargetAlreadyNew);
            }
            if !target_state.is_drained() {
                return Err(DeferredGenerationTargetTransitionError::Draining(
                    state.target_snapshot(&target),
                ));
            }
        }

        // Legacy acceptance is sealed before aggregate publication, so table
        // occupancy can only drain. Probe outside the coordinator gate to
        // avoid running a caller-controlled table lock while holding it.
        if legacy_table_probe(&target) {
            return Err(DeferredGenerationTargetTransitionError::LegacyTableOccupied);
        }

        let mut state = self.write_gate.lock();
        if state.shutdown_sealed {
            return Err(DeferredGenerationTargetTransitionError::ShutdownSealed);
        }
        if state.default_generation != DeferredGeneration::New || !state.v2_aggregate_published {
            return Err(DeferredGenerationTargetTransitionError::CutoverNotPublished);
        }
        let Some(target_state) = state.targets.get(&target) else {
            return Err(DeferredGenerationTargetTransitionError::TargetAbsent);
        };
        if target_state.generation == DeferredGeneration::New {
            return Err(DeferredGenerationTargetTransitionError::TargetAlreadyNew);
        }
        if !target_state.is_drained() {
            return Err(DeferredGenerationTargetTransitionError::Draining(
                state.target_snapshot(&target),
            ));
        }
        let Some(target_state) = state.targets.get_mut(&target) else {
            return Err(DeferredGenerationTargetTransitionError::TargetAbsent);
        };
        target_state.generation = DeferredGeneration::New;
        target_state.replay_tokens += 1;
        Ok(ReplayToken {
            write_gate: Arc::clone(&self.write_gate),
            target,
            armed: true,
        })
    }

    /// Retries one explicitly observed, abandoned replay without discarding it.
    pub(crate) fn retry_abandoned_replay(
        &self,
        target: DeferredGenerationTarget,
    ) -> Result<ReplayToken, DeferredGenerationReplayRetryError> {
        let mut state = self.write_gate.lock();
        if state.shutdown_sealed {
            return Err(DeferredGenerationReplayRetryError::ShutdownSealed);
        }
        let Some(target_state) = state.targets.get_mut(&target) else {
            return Err(DeferredGenerationReplayRetryError::TargetAbsent);
        };
        if target_state.generation != DeferredGeneration::New {
            return Err(DeferredGenerationReplayRetryError::TargetNotNew);
        }
        if target_state.replay_tokens != 0 || target_state.abandoned_replays == 0 {
            return Err(DeferredGenerationReplayRetryError::NoRetryableReplay);
        }
        target_state.abandoned_replays -= 1;
        target_state.replay_tokens += 1;
        Ok(ReplayToken {
            write_gate: Arc::clone(&self.write_gate),
            target,
            armed: true,
        })
    }

    #[must_use]
    pub(crate) fn snapshot(&self) -> DeferredGenerationHandoffSnapshot {
        self.write_gate.lock().snapshot()
    }

    #[must_use]
    pub(crate) fn zero_report(&self) -> DeferredGenerationHandoffZeroReport {
        DeferredGenerationHandoffZeroReport::from(self.snapshot())
    }
}
