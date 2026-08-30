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

//! Broker-private lifecycle accounting for canonical deferred routes.

use std::sync::Arc;

use cheetah_string::CheetahString;
use parking_lot::Mutex;

mod lease;
mod snapshot;
mod state;
mod target;
#[cfg(test)]
#[path = "../tests/unit/deferred_generation_handoff.rs"]
mod tests;

pub(crate) use lease::*;
pub(crate) use snapshot::*;
use state::DeferredGenerationHandoffState;
pub(crate) use target::*;

/// Serializes route admission and shutdown for Broker-owned deferred work.
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

    pub(crate) fn seal(&self) -> DeferredGenerationSeal {
        let mut state = self.write_gate.lock();
        if state.shutdown_sealed {
            return DeferredGenerationSeal::AlreadySealed;
        }
        state.shutdown_sealed = true;
        state.prune_quiescent_targets();
        DeferredGenerationSeal::Sealed
    }

    #[must_use]
    pub(crate) fn is_sealed(&self) -> bool {
        self.write_gate.lock().shutdown_sealed
    }

    pub(crate) fn acquire_route(
        &self,
        target: DeferredGenerationTarget,
    ) -> Result<RoutePermit, DeferredGenerationRouteError> {
        self.write_gate.lock().acquire_route(&target)?;
        Ok(RoutePermit::new(Arc::clone(&self.write_gate), target))
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

    #[must_use]
    pub(crate) fn snapshot(&self) -> DeferredGenerationHandoffSnapshot {
        self.write_gate.lock().snapshot()
    }

    #[must_use]
    pub(crate) fn zero_report(&self) -> DeferredGenerationHandoffZeroReport {
        DeferredGenerationHandoffZeroReport::from(self.snapshot())
    }
}
