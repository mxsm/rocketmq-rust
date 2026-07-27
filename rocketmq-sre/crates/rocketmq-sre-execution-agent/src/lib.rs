// Copyright 2023 The RocketMQ Rust Authors
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

//! Persisted fencing and effect ledger for the dispatch-disabled Agent.

mod effect_store;
mod error;

pub use effect_store::AgentEffectRecord;
pub use effect_store::AgentEffectStore;
pub use effect_store::EffectCreation;
pub use error::AgentStoreError;

/// Static agent state exposed to readiness and capability APIs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionAgentState {
    Disabled,
}

/// Returns the only Phase 00 execution agent state.
#[must_use]
pub const fn state() -> ExecutionAgentState {
    ExecutionAgentState::Disabled
}
