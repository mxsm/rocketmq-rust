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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use rocketmq_sre_contracts::{ClusterId, ExecutionRequest, TenantId, canonical_sha256};

use crate::ExecutorRequestFailure;

const MAX_ACTIVE_INTENTS: usize = 256;

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct IntentKey {
    tenant: TenantId,
    cluster: ClusterId,
    // Retain a fixed-size digest rather than request bodies or idempotency keys.
    selector: String,
}

impl IntentKey {
    fn from_request(request: &ExecutionRequest) -> Result<Self, ExecutorRequestFailure> {
        Ok(Self {
            tenant: request.tenant_id,
            cluster: request.cluster_id,
            selector: canonical_sha256(&request.idempotency_key).map_err(|_| ExecutorRequestFailure::InvalidRequest)?,
        })
    }
}

/// Only protects operations inside this Executor instance. PostgreSQL leases,
/// authority checks and Agent fencing continue to arbitrate other instances.
#[derive(Clone, Default)]
pub(super) struct ActiveIntents(Arc<Mutex<State>>);

#[derive(Default)]
struct State {
    intents: BTreeMap<IntentKey, String>,
    taking_over: BTreeSet<ClusterId>,
}

impl ActiveIntents {
    pub(super) fn enter(&self, request: &ExecutionRequest) -> Result<IntentGuard, ExecutorRequestFailure> {
        let binding = canonical_sha256(request).map_err(|_| ExecutorRequestFailure::InvalidRequest)?;
        self.enter_key(IntentKey::from_request(request)?, binding)
    }

    fn enter_key(&self, key: IntentKey, binding: String) -> Result<IntentGuard, ExecutorRequestFailure> {
        let mut state = self.0.lock().map_err(|_| ExecutorRequestFailure::ReconcileBlocked)?;
        if let Some(original) = state.intents.get(&key) {
            return Err(if original == &binding {
                ExecutorRequestFailure::ReconcileBlocked
            } else {
                ExecutorRequestFailure::PreconditionChanged
            });
        }
        if state.intents.len() >= MAX_ACTIVE_INTENTS || state.taking_over.contains(&key.cluster) {
            return Err(ExecutorRequestFailure::ReconcileBlocked);
        }
        state.intents.insert(key.clone(), binding);
        Ok(IntentGuard {
            owner: self.clone(),
            key,
        })
    }

    pub(super) fn begin_takeover(&self, request: &ExecutionRequest) -> Result<TakeoverGuard, ExecutorRequestFailure> {
        self.takeover_key(IntentKey::from_request(request)?)
    }

    fn takeover_key(&self, key: IntentKey) -> Result<TakeoverGuard, ExecutorRequestFailure> {
        let mut state = self.0.lock().map_err(|_| ExecutorRequestFailure::ReconcileBlocked)?;
        if !state.intents.contains_key(&key)
            || state
                .intents
                .keys()
                .any(|other| other.cluster == key.cluster && *other != key)
            || !state.taking_over.insert(key.cluster)
        {
            return Err(ExecutorRequestFailure::ReconcileBlocked);
        }
        Ok(TakeoverGuard {
            owner: self.clone(),
            cluster: key.cluster,
        })
    }
}

// Guards live in the owned execution/recovery future, never the HTTP waiter.
// No synchronous lock guard crosses an await.
pub(super) struct IntentGuard {
    owner: ActiveIntents,
    key: IntentKey,
}

impl Drop for IntentGuard {
    fn drop(&mut self) {
        let mut state = self.owner.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.intents.remove(&self.key);
    }
}

pub(super) struct TakeoverGuard {
    owner: ActiveIntents,
    cluster: ClusterId,
}

impl Drop for TakeoverGuard {
    fn drop(&mut self) {
        let mut state = self.owner.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.taking_over.remove(&self.cluster);
    }
}

#[cfg(test)]
mod tests;
