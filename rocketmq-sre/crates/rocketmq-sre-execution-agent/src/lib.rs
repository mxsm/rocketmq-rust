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

//! Fenced, typed mutation boundary for supervised RocketMQ SRE execution.

mod api;
mod authority_client;
mod config;
mod credential_owner;
mod dispatch_barrier;
mod drivers;
mod effect_store;
mod error;
mod fence;
mod registry;
mod service;

pub use api::build_router;
pub use api::run;
pub use authority_client::AuthorityFuture;
pub use authority_client::HttpLeaseAuthorityClient;
pub use authority_client::LeaseAuthorityClient;
pub use config::DEFAULT_EXECUTION_AGENT_PORT;
pub use config::ExecutionAgentConfig;
pub use credential_owner::MutationCredentialOwner;
pub use dispatch_barrier::DispatchBarrier;
pub use drivers::AdminCoreDriver;
pub use drivers::AgentActionHandler;
pub use drivers::ConfigDriver;
pub use drivers::ConfigWriteClient;
pub use drivers::DriverDispatchOutcome;
pub use drivers::DriverFuture;
pub use drivers::KubernetesDriver;
pub use drivers::LoggerLevelTtlWrite;
pub use effect_store::AgentEffectRecord;
pub use effect_store::AgentEffectStore;
pub use effect_store::EffectCreation;
pub use error::AgentStoreError;
pub use error::ExecutionAgentError;
pub use fence::FenceAckSigner;
pub use registry::AgentDriverRegistry;
pub use registry::DriverFamily;
pub use rocketmq_sre_contracts::AdvanceFenceRequest;
pub use rocketmq_sre_contracts::AdvanceFenceResponse;
pub use rocketmq_sre_contracts::AgentDispatchRequest;
pub use rocketmq_sre_contracts::AgentDispatchResponse;
pub use rocketmq_sre_contracts::AgentReadRequest;
pub use rocketmq_sre_contracts::AgentReadResult;
pub use rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION;
pub use rocketmq_sre_contracts::ExecutionAgentCapabilities;
pub use rocketmq_sre_contracts::ReconcileEffectRequest;
pub use rocketmq_sre_contracts::ReconcileEffectResponse;
pub use rocketmq_sre_contracts::ReconcileEffectState;
pub use service::ExecutionAgent;
pub use service::ExecutionAgentMetricsSnapshot;

/// Static agent state exposed to readiness and capability APIs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionAgentState {
    FencedTypedOnly,
}

/// Returns the supervised execution boundary state.
#[must_use]
pub const fn state() -> ExecutionAgentState {
    ExecutionAgentState::FencedTypedOnly
}
