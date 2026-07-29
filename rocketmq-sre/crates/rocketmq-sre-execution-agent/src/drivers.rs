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

mod admin_core;
mod broker_config_patch;
mod config;
mod credential_rotation;
mod kubernetes;
mod logger_level_ttl;
mod production_broker_config;
mod production_proxy_restart;
mod production_proxy_scale;
mod production_telemetry_collector_restart;
mod proxy_image_canary;
mod proxy_restart_one;
mod proxy_scale_out_one;
mod subscription_group_patch;
mod telemetry_collector_restart_one;
#[cfg(test)]
mod test_support;
mod topic_config_patch;

use std::future::Future;
use std::pin::Pin;

use rocketmq_sre_contracts::AgentStepRequest;

pub use admin_core::AdminCoreDriver;
pub use admin_core::BrokerConfigPatch;
pub use admin_core::BrokerConfigPatchApplyOutcome;
pub use admin_core::BrokerConfigPatchClient;
pub use admin_core::BrokerConfigPatchRestore;
pub use admin_core::BrokerConfigPatchState;
pub use admin_core::BrokerConfigPatchWrite;
pub use admin_core::SubscriptionGroupPatch;
pub use admin_core::SubscriptionGroupPatchApplyOutcome;
pub use admin_core::SubscriptionGroupPatchClient;
pub use admin_core::SubscriptionGroupPatchRestore;
pub use admin_core::SubscriptionGroupPatchState;
pub use admin_core::SubscriptionGroupPatchWrite;
pub use admin_core::TopicConfigPatch;
pub use admin_core::TopicConfigPatchApplyOutcome;
pub use admin_core::TopicConfigPatchClient;
pub use admin_core::TopicConfigPatchRestore;
pub use admin_core::TopicConfigPatchState;
pub use admin_core::TopicConfigPatchWrite;
pub use broker_config_patch::BrokerConfigPatchHandler;
pub use broker_config_patch::BrokerConfigPatchParameters;
pub use config::ConfigDriver;
pub use config::ConfigWriteClient;
pub use config::CredentialOverlapRestore;
pub use config::CredentialOverlapWrite;
pub use config::CredentialRotationClient;
pub use config::CredentialRotationState;
pub use config::LoggerLevelControlClient;
pub use config::LoggerLevelState;
pub use config::LoggerLevelTtlRestore;
pub use config::LoggerLevelTtlWrite;
pub use credential_rotation::CredentialRotationHandler;
pub use credential_rotation::CredentialRotationParameters;
pub use kubernetes::KubernetesDriver;
pub use kubernetes::ProxyImageCanaryClient;
pub use kubernetes::ProxyImageCanaryRestore;
pub use kubernetes::ProxyImageCanaryState;
pub use kubernetes::ProxyImageCanaryWrite;
pub use kubernetes::ProxyRestartClient;
pub use kubernetes::ProxyRestartOneWrite;
pub use kubernetes::ProxyRestartRestore;
pub use kubernetes::ProxyRestartRestoreOutcome;
pub use kubernetes::ProxyRestartState;
pub use kubernetes::ProxyScaleClient;
pub use kubernetes::ProxyScaleOutOneWrite;
pub use kubernetes::ProxyScaleRestore;
pub use kubernetes::ProxyScaleState;
pub use kubernetes::TelemetryCollectorRestartClient;
pub use kubernetes::TelemetryCollectorRestartOneWrite;
pub use kubernetes::TelemetryCollectorRestartState;
pub use logger_level_ttl::LoggerLevelTtlHandler;
pub use logger_level_ttl::LoggerLevelTtlParameters;
pub(crate) use production_broker_config::ProductionBrokerConfigPatchClient;
pub(crate) use production_proxy_restart::ProductionProxyRestartClient;
pub(crate) use production_proxy_scale::ProductionProxyScaleClient;
pub(crate) use production_telemetry_collector_restart::ProductionTelemetryCollectorRestartClient;
pub use proxy_image_canary::ProxyImageCanaryHandler;
pub use proxy_image_canary::ProxyImageCanaryParameters;
pub use proxy_restart_one::ProxyRestartOneHandler;
pub use proxy_restart_one::ProxyRestartOneParameters;
pub use proxy_scale_out_one::ProxyScaleOutOneHandler;
pub use proxy_scale_out_one::ProxyScaleOutOneParameters;
pub use subscription_group_patch::SubscriptionGroupPatchHandler;
pub use subscription_group_patch::SubscriptionGroupPatchParameters;
pub use telemetry_collector_restart_one::TelemetryCollectorRestartOneHandler;
pub use telemetry_collector_restart_one::TelemetryCollectorRestartOneParameters;
pub use topic_config_patch::TopicConfigPatchHandler;
pub use topic_config_patch::TopicConfigPatchParameters;

use crate::ExecutionAgentError;
use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::AgentReadResult;
use rocketmq_sre_contracts::ReconcileEffectResponse;

/// Sanitized successful driver result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DriverDispatchOutcome {
    pub operation_id: String,
    pub outcome_code: String,
    pub sanitized_summary: String,
}

/// Common closed behavior implemented by one of the three typed driver families.
pub trait AgentActionHandler: Send + Sync {
    fn read_state<'a>(&'a self, request: &'a AgentReadRequest) -> DriverFuture<'a, AgentReadResult>;

    fn dispatch<'a>(
        &'a self,
        request: &'a AgentStepRequest,
        operation_id: &'a str,
    ) -> DriverFuture<'a, DriverDispatchOutcome>;

    fn reconcile<'a>(
        &'a self,
        request: &'a AgentReadRequest,
        operation_id: Option<&str>,
    ) -> DriverFuture<'a, ReconcileEffectResponse>;

    fn compensate<'a>(
        &'a self,
        request: &'a AgentStepRequest,
        operation_id: &'a str,
    ) -> DriverFuture<'a, DriverDispatchOutcome>;
}

pub type DriverFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ExecutionAgentError>> + Send + 'a>>;
