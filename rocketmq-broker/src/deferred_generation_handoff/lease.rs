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

use std::sync::Arc;

use parking_lot::Mutex;

use super::state::DeferredGenerationHandoffState;
use super::DeferredGenerationHandoff;
use super::DeferredGenerationTarget;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeferredGenerationSeal {
    Sealed,
    AlreadySealed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeferredGenerationRouteError {
    ShutdownSealed,
}

pub(crate) struct DeferredGenerationArrivalAdapter<'a> {
    pub(super) handoff: &'a DeferredGenerationHandoff,
}

impl DeferredGenerationArrivalAdapter<'_> {
    pub(crate) fn acquire_route(
        &self,
        target: DeferredGenerationTarget,
    ) -> Result<RoutePermit, DeferredGenerationRouteError> {
        self.handoff.acquire_route(target)
    }

    /// Executes the fixed Pull → Pop → Notification → Lite producer order.
    pub(crate) fn route_arrival<P, O, N, L>(&self, pull: P, pop: O, notification: N, pop_lite: L)
    where
        P: FnOnce(&DeferredGenerationHandoff),
        O: FnOnce(&DeferredGenerationHandoff),
        N: FnOnce(&DeferredGenerationHandoff),
        L: FnOnce(&DeferredGenerationHandoff),
    {
        pull(self.handoff);
        pop(self.handoff);
        notification(self.handoff);
        pop_lite(self.handoff);
    }
}

/// Affine accounting for one accepted canonical deferred route.
#[derive(Debug)]
#[must_use]
pub(crate) struct RoutePermit {
    write_gate: Arc<Mutex<DeferredGenerationHandoffState>>,
    target: DeferredGenerationTarget,
    armed: bool,
}

impl RoutePermit {
    pub(super) const fn new(
        write_gate: Arc<Mutex<DeferredGenerationHandoffState>>,
        target: DeferredGenerationTarget,
    ) -> Self {
        Self {
            write_gate,
            target,
            armed: true,
        }
    }
}

impl Drop for RoutePermit {
    fn drop(&mut self) {
        if self.armed {
            self.write_gate.lock().release_candidate(&self.target);
            self.armed = false;
        }
    }
}
