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

use std::panic::AssertUnwindSafe;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard};

use super::state::DeferredGenerationHandoffState;
use super::{DeferredGenerationTarget, DeferredGenerationTargetSnapshot};

/// Issued after an explicit Legacy → New transition.
#[derive(Debug)]
#[must_use]
pub(crate) struct ReplayToken {
    pub(super) write_gate: Arc<Mutex<DeferredGenerationHandoffState>>,
    pub(super) target: DeferredGenerationTarget,
    pub(super) armed: bool,
}

impl ReplayToken {
    #[must_use]
    pub(crate) fn target(&self) -> &DeferredGenerationTarget {
        &self.target
    }

    /// Marks the replay as accepted by its canonical new-generation owner.
    pub(crate) fn complete_after_replay_accepted(mut self) {
        if self.armed {
            self.write_gate.lock().complete_replay_token(&self.target);
            self.armed = false;
        }
    }
}

impl Drop for ReplayToken {
    fn drop(&mut self) {
        if self.armed {
            self.write_gate.lock().abandon_replay_token(&self.target);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeferredGenerationCutoverError {
    ShutdownSealed,
    AlreadyPublishedNew,
    TransactionActive,
    InvalidStage,
}

#[derive(Debug)]
pub(crate) enum DeferredGenerationV2PublishError<E> {
    Cutover(DeferredGenerationCutoverError),
    Publish(E),
}

/// One affine, atomic publication attempt for the prepared V2 aggregate.
///
/// The publisher runs while the handoff coordinator gate is held. Production
/// publishers must therefore be nonblocking and must not re-enter the handoff
/// coordinator or acquire a legacy waiter table. Publication itself must be
/// atomic: returning `Err` or unwinding is permitted only before the aggregate
/// becomes externally visible, while `Ok(())` means the aggregate was
/// published exactly once. These constraints let the coordinator commit its
/// matching stage before any shutdown or producer route can observe the
/// publication.
#[must_use]
pub(crate) struct DeferredGenerationV2Publisher<F> {
    publish: F,
}

impl<F> DeferredGenerationV2Publisher<F> {
    pub(crate) fn nonblocking_atomic(publish: F) -> Self {
        Self { publish }
    }

    #[cfg(test)]
    pub(crate) fn blocking_for_serialization_test(publish: F) -> Self {
        Self { publish }
    }

    fn publish<E>(self) -> Result<(), E>
    where
        F: FnOnce() -> Result<(), E>,
    {
        (self.publish)()
    }
}

/// MIG-05 holds this transaction while it performs the real aggregate publish.
pub(crate) struct DeferredGenerationCutover<'a> {
    pub(super) state: MutexGuard<'a, DeferredGenerationHandoffState>,
    pub(super) stage: DeferredGenerationCutoverStage,
    pub(super) transition_scan_ready: &'a AtomicBool,
}

impl DeferredGenerationCutover<'_> {
    pub(crate) fn seal_legacy_acceptance(&mut self) -> Result<(), DeferredGenerationCutoverError> {
        match self.stage {
            DeferredGenerationCutoverStage::Open => {
                self.state.legacy_acceptance_sealed = true;
                self.stage = DeferredGenerationCutoverStage::LegacyAcceptanceSealed;
                Ok(())
            }
            DeferredGenerationCutoverStage::LegacyAcceptanceSealed
            | DeferredGenerationCutoverStage::V2AggregatePublished => Ok(()),
            DeferredGenerationCutoverStage::DefaultNewPublished => Err(DeferredGenerationCutoverError::InvalidStage),
        }
    }

    pub(crate) fn publish_v2_aggregate<E, F>(
        &mut self,
        publisher: DeferredGenerationV2Publisher<F>,
    ) -> Result<(), DeferredGenerationV2PublishError<E>>
    where
        F: FnOnce() -> Result<(), E>,
    {
        match self.stage {
            DeferredGenerationCutoverStage::LegacyAcceptanceSealed => {
                let published = std::panic::catch_unwind(AssertUnwindSafe(|| publisher.publish()));
                match published {
                    Ok(Ok(())) => {
                        if !self.state.cutover_transaction_active
                            || self.stage != DeferredGenerationCutoverStage::LegacyAcceptanceSealed
                            || !self.state.legacy_acceptance_sealed
                        {
                            return Err(DeferredGenerationV2PublishError::Cutover(
                                DeferredGenerationCutoverError::InvalidStage,
                            ));
                        }
                        self.state.v2_aggregate_published = true;
                        self.stage = DeferredGenerationCutoverStage::V2AggregatePublished;
                        Ok(())
                    }
                    Ok(Err(error)) => Err(DeferredGenerationV2PublishError::Publish(error)),
                    Err(payload) => std::panic::resume_unwind(payload),
                }
            }
            DeferredGenerationCutoverStage::V2AggregatePublished => Ok(()),
            DeferredGenerationCutoverStage::Open | DeferredGenerationCutoverStage::DefaultNewPublished => Err(
                DeferredGenerationV2PublishError::Cutover(DeferredGenerationCutoverError::InvalidStage),
            ),
        }
    }

    pub(crate) fn publish_default_new(&mut self) -> Result<(), DeferredGenerationCutoverError> {
        if self.stage != DeferredGenerationCutoverStage::V2AggregatePublished {
            return Err(DeferredGenerationCutoverError::InvalidStage);
        }
        self.state.publish_default_new();
        self.stage = DeferredGenerationCutoverStage::DefaultNewPublished;
        self.transition_scan_ready.store(true, Ordering::Release);
        Ok(())
    }
}

impl Drop for DeferredGenerationCutover<'_> {
    fn drop(&mut self) {
        self.state.cutover_transaction_active = false;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DeferredGenerationCutoverStage {
    Open,
    LegacyAcceptanceSealed,
    V2AggregatePublished,
    DefaultNewPublished,
}

#[derive(Debug)]
pub(crate) enum DeferredGenerationTargetTransitionError {
    ShutdownSealed,
    CutoverNotPublished,
    TargetAbsent,
    TargetAlreadyNew,
    Draining(DeferredGenerationTargetSnapshot),
    LegacyTableOccupied,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeferredGenerationReplayRetryError {
    ShutdownSealed,
    TargetAbsent,
    TargetNotNew,
    NoRetryableReplay,
}
