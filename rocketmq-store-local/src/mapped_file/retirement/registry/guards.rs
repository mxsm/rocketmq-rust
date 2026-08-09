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

use super::*;
use crate::mapped_file::retirement::codec;

/// Two-phase reservation that never retains a registry mutex guard.
pub(crate) struct PreparedRetirementIntent<'a, O> {
    pub(super) registry: &'a RetirementRegistry<O>,
    pub(super) binding: RetirementIntentBinding,
    pub(super) active: bool,
}

impl<'a, O> PreparedRetirementIntent<'a, O> {
    pub(crate) const fn binding(&self) -> &RetirementIntentBinding {
        &self.binding
    }

    /// Arms the no-lock ledger-I/O interval. Dropping the returned guard requires replay.
    pub(crate) fn begin_append(mut self) -> RetirementIntentAppend<'a, O> {
        let binding = self.binding.clone();
        self.active = false;
        RetirementIntentAppend {
            registry: self.registry,
            binding,
            armed: true,
        }
    }

    /// Releases a reservation only before any durable evidence is presented.
    pub(crate) fn rollback(mut self) {
        self.registry.rollback_prepared(&self.binding);
        self.active = false;
    }
}

/// No-lock interval in which a writer may have made the intent durable or ambiguous.
pub(crate) struct RetirementIntentAppend<'a, O> {
    registry: &'a RetirementRegistry<O>,
    binding: RetirementIntentBinding,
    armed: bool,
}

impl<O> RetirementIntentAppend<'_, O> {
    pub(crate) const fn binding(&self) -> &RetirementIntentBinding {
        &self.binding
    }

    pub(crate) fn intent_record(&self) -> codec::LedgerRecord {
        self.binding.to_record()
    }

    /// Commits exact writer evidence; every rejection leaves the registry recovery-fenced.
    pub(crate) fn commit(
        mut self,
        evidence: DurableIntentEvidence,
    ) -> Result<DurableRetirementToken<O>, RegistryError> {
        let result = self.registry.commit_prepared(&self.binding, evidence);
        self.armed = false;
        result
    }
}

impl<O> Drop for RetirementIntentAppend<'_, O> {
    fn drop(&mut self) {
        if self.armed {
            self.registry.fence_inflight_intent();
        }
    }
}

impl<O> Drop for PreparedRetirementIntent<'_, O> {
    fn drop(&mut self) {
        if self.active {
            self.registry.rollback_prepared(&self.binding);
        }
    }
}

/// Lock-free interval capability for one exact ArcSwap compare-and-swap attempt.
pub(crate) struct PreparedQueueHandoff<'a, O> {
    pub(super) registry: &'a RetirementRegistry<O>,
    pub(super) token: Option<DurableRetirementToken<O>>,
    pub(super) armed: bool,
}

impl<O> PreparedQueueHandoff<'_, O> {
    pub(crate) fn binding(&self) -> Option<&RetirementIntentBinding> {
        self.token.as_ref().map(|token| &token.binding)
    }

    pub(crate) fn owner(&self) -> Option<&Arc<O>> {
        self.token.as_ref().map(|token| &token.owner)
    }

    pub(crate) fn queue_identity(&self) -> Option<&QueueIdentity> {
        self.token.as_ref().map(|token| &token.queue_identity)
    }

    /// Reports an ArcSwap conflict and returns the original, still-unconsumed token.
    pub(crate) fn rollback(mut self) -> Result<DurableRetirementToken<O>, RegistryError> {
        let Some(token) = self.token.take() else {
            self.armed = false;
            self.registry.fence_abandoned_handoff();
            return Err(RegistryError::NeedsRecovery);
        };
        self.armed = false;
        self.registry.rollback_handoff(&token)?;
        Ok(token)
    }

    /// Finalizes token consumption only after the caller completed the exact queue handoff.
    pub(super) fn commit(mut self) -> Result<RetirementHandoffCapability<O>, RegistryError> {
        let Some(token) = self.token.take() else {
            self.armed = false;
            self.registry.fence_abandoned_handoff();
            return Err(RegistryError::NeedsRecovery);
        };
        self.armed = false;
        self.registry.commit_handoff(token)
    }
}

impl<O> Drop for PreparedQueueHandoff<'_, O> {
    fn drop(&mut self) {
        if self.armed {
            self.registry.fence_abandoned_handoff();
        }
    }
}

impl<O> fmt::Debug for PreparedQueueHandoff<'_, O> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedQueueHandoff")
            .field("binding", &self.binding())
            .finish_non_exhaustive()
    }
}
