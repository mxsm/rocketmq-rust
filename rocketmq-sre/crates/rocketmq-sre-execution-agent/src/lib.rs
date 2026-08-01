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

#![recursion_limit = "256"]

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
pub use drivers::AgentActionHandler;
pub use drivers::BrokerConfigPatch;
pub use drivers::BrokerConfigPatchApplyOutcome;
pub use drivers::BrokerConfigPatchClient;
pub use drivers::BrokerConfigPatchHandler;
pub use drivers::BrokerConfigPatchParameters;
pub use drivers::BrokerConfigPatchRestore;
pub use drivers::BrokerConfigPatchState;
pub use drivers::BrokerConfigPatchWrite;
pub use drivers::ConfigWriteClient;
pub use drivers::CredentialOverlapRestore;
pub use drivers::CredentialOverlapWrite;
pub use drivers::CredentialRotationClient;
pub use drivers::CredentialRotationHandler;
pub use drivers::CredentialRotationParameters;
pub use drivers::CredentialRotationState;
pub use drivers::DriverDispatchOutcome;
pub use drivers::DriverFuture;
pub use drivers::LoggerLevelControlClient;
pub use drivers::LoggerLevelState;
pub use drivers::LoggerLevelTtlHandler;
pub use drivers::LoggerLevelTtlParameters;
pub use drivers::LoggerLevelTtlRestore;
pub use drivers::LoggerLevelTtlWrite;
pub use drivers::ProxyImageCanaryClient;
pub use drivers::ProxyImageCanaryHandler;
pub use drivers::ProxyImageCanaryParameters;
pub use drivers::ProxyImageCanaryRestore;
pub use drivers::ProxyImageCanaryState;
pub use drivers::ProxyImageCanaryWrite;
pub use drivers::ProxyRestartClient;
pub use drivers::ProxyRestartOneHandler;
pub use drivers::ProxyRestartOneParameters;
pub use drivers::ProxyRestartOneWrite;
pub use drivers::ProxyRestartRestore;
pub use drivers::ProxyRestartRestoreOutcome;
pub use drivers::ProxyRestartState;
pub use drivers::ProxyScaleClient;
pub use drivers::ProxyScaleOutOneHandler;
pub use drivers::ProxyScaleOutOneParameters;
pub use drivers::ProxyScaleOutOneWrite;
pub use drivers::ProxyScaleRestore;
pub use drivers::ProxyScaleState;
pub use drivers::SubscriptionGroupPatch;
pub use drivers::SubscriptionGroupPatchApplyOutcome;
pub use drivers::SubscriptionGroupPatchClient;
pub use drivers::SubscriptionGroupPatchHandler;
pub use drivers::SubscriptionGroupPatchParameters;
pub use drivers::SubscriptionGroupPatchRestore;
pub use drivers::SubscriptionGroupPatchState;
pub use drivers::SubscriptionGroupPatchWrite;
pub use drivers::TelemetryCollectorRestartClient;
pub use drivers::TelemetryCollectorRestartOneHandler;
pub use drivers::TelemetryCollectorRestartOneParameters;
pub use drivers::TelemetryCollectorRestartOneWrite;
pub use drivers::TelemetryCollectorRestartState;
pub use drivers::TopicConfigPatch;
pub use drivers::TopicConfigPatchApplyOutcome;
pub use drivers::TopicConfigPatchClient;
pub use drivers::TopicConfigPatchHandler;
pub use drivers::TopicConfigPatchParameters;
pub use drivers::TopicConfigPatchRestore;
pub use drivers::TopicConfigPatchState;
pub use drivers::TopicConfigPatchWrite;
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
