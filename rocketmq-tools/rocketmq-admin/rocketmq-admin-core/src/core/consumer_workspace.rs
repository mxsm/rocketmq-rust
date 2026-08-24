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

//! Truthful read-only Consumer and Producer workspace contracts.

use serde::{Deserialize, Serialize};

use crate::core::{
    consumer::{
        DashboardConsumerConfigAttribute, DashboardConsumerConnection, DashboardConsumerProgress,
        DashboardConsumerRunningInfo, DashboardConsumerRunningInfoRequest, SubscriptionGroupConfigCasState,
    },
    dashboard::DashboardProducerConnections,
    AdminFuture,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceObservationState {
    Complete,
    Partial,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceUnknownReason {
    Unsupported,
    Unavailable,
    InvalidResponse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceFailureStage {
    Inventory,
    Clients,
    Progress,
    Configuration,
    Diagnostics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceFailureCode {
    NotFound,
    Unavailable,
    Unsupported,
    InvalidData,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceTargetFailure {
    pub target: String,
    pub stage: WorkspaceFailureStage,
    pub code: WorkspaceFailureCode,
    pub retryable: bool,
}

impl std::fmt::Debug for WorkspaceTargetFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkspaceTargetFailure")
            .field("stage", &self.stage)
            .field("code", &self.code)
            .field("retryable", &self.retryable)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "observation", rename_all = "snake_case")]
pub enum WorkspaceObservation<T> {
    Complete {
        value: T,
    },
    Partial {
        value: T,
        successful_target_count: usize,
        failures: Vec<WorkspaceTargetFailure>,
    },
    Unknown {
        reason: WorkspaceUnknownReason,
    },
}

impl<T> WorkspaceObservation<T> {
    #[must_use]
    pub const fn state(&self) -> WorkspaceObservationState {
        match self {
            Self::Complete { .. } => WorkspaceObservationState::Complete,
            Self::Partial { .. } => WorkspaceObservationState::Partial,
            Self::Unknown { .. } => WorkspaceObservationState::Unknown,
        }
    }

    #[must_use]
    pub const fn value(&self) -> Option<&T> {
        match self {
            Self::Complete { value } | Self::Partial { value, .. } => Some(value),
            Self::Unknown { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerInventoryRequest {
    pub skip_system_groups: bool,
    /// A selected Proxy endpoint. `None` means NameServer Direct.
    pub forwarded_address: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerWorkspaceTarget {
    pub cluster_name: String,
    pub broker_name: String,
    pub broker_address: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerInventoryItem {
    pub group: String,
    pub category: String,
    pub client_count: WorkspaceObservation<usize>,
    pub diff_total: WorkspaceObservation<i64>,
    pub consume_type: WorkspaceObservation<String>,
    pub message_model: WorkspaceObservation<String>,
    pub targets: Vec<ConsumerWorkspaceTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerInventoryResult {
    pub items: Vec<ConsumerInventoryItem>,
    pub targets: Vec<ConsumerWorkspaceTarget>,
    pub observation: WorkspaceObservationState,
    pub failures: Vec<WorkspaceTargetFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerResourceRequest {
    pub group: String,
    /// A selected Proxy endpoint for the only operations that support forwarding.
    pub forwarded_address: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerClientsResult {
    pub observation: WorkspaceObservation<DashboardConsumerConnection>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerProgressResult {
    pub observation: WorkspaceObservation<DashboardConsumerProgress>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerConfigTarget {
    pub target: ConsumerWorkspaceTarget,
    pub observation: WorkspaceObservation<SubscriptionGroupConfigCasState>,
    /// Safe display-only configuration entries. Sensitive attributes are omitted.
    pub entries: Vec<DashboardConsumerConfigAttribute>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerConfigurationResult {
    pub group: String,
    pub targets: Vec<ConsumerConfigTarget>,
    pub observation: WorkspaceObservationState,
    pub failures: Vec<WorkspaceTargetFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerExactTargetsRequest {
    pub group: String,
    pub targets: Vec<ConsumerWorkspaceTarget>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerConfigPresence {
    Present,
    Absent,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerConfigPresenceTarget {
    pub target: ConsumerWorkspaceTarget,
    pub presence: ConsumerConfigPresence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerConfigPresenceResult {
    pub targets: Vec<ConsumerConfigPresenceTarget>,
    pub failures: Vec<WorkspaceTargetFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerConnectionTarget {
    pub target: ConsumerWorkspaceTarget,
    pub observation: WorkspaceObservation<DashboardConsumerConnection>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerConnectionsAtTargetsResult {
    pub targets: Vec<ConsumerConnectionTarget>,
    pub failures: Vec<WorkspaceTargetFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerInventoryItem {
    pub group: String,
    pub client_count: WorkspaceObservation<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerInventoryResult {
    pub items: Vec<ProducerInventoryItem>,
    pub observation: WorkspaceObservationState,
    pub failures: Vec<WorkspaceTargetFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerConnectionsRequest {
    pub topic: String,
    pub group: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerConnectionsResult {
    pub observation: WorkspaceObservation<DashboardProducerConnections>,
}

/// Query-only workspace used by Dashboard frontends. Direct-only resources do
/// not accept an address and therefore cannot silently route through Proxy.
pub trait ConsumerWorkspaceAdmin: Send + Sync {
    fn consumer_inventory<'a>(
        &'a self,
        request: &'a ConsumerInventoryRequest,
    ) -> AdminFuture<'a, ConsumerInventoryResult>;

    fn consumer_clients<'a>(&'a self, request: &'a ConsumerResourceRequest) -> AdminFuture<'a, ConsumerClientsResult>;

    fn consumer_progress<'a>(&'a self, request: &'a ConsumerResourceRequest)
        -> AdminFuture<'a, ConsumerProgressResult>;

    fn consumer_configuration<'a>(&'a self, group: &'a str) -> AdminFuture<'a, ConsumerConfigurationResult>;

    fn consumer_config_presence<'a>(
        &'a self,
        request: &'a ConsumerExactTargetsRequest,
    ) -> AdminFuture<'a, ConsumerConfigPresenceResult>;

    fn consumer_connections_at_targets<'a>(
        &'a self,
        request: &'a ConsumerExactTargetsRequest,
    ) -> AdminFuture<'a, ConsumerConnectionsAtTargetsResult>;

    fn consumer_diagnostic<'a>(
        &'a self,
        request: &'a DashboardConsumerRunningInfoRequest,
    ) -> AdminFuture<'a, DashboardConsumerRunningInfo>;

    fn producer_inventory(&self) -> AdminFuture<'_, ProducerInventoryResult>;

    fn producer_connections<'a>(
        &'a self,
        request: &'a ProducerConnectionsRequest,
    ) -> AdminFuture<'a, ProducerConnectionsResult>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_observation_has_no_zero_fallback() {
        let observation = WorkspaceObservation::<i64>::Unknown {
            reason: WorkspaceUnknownReason::Unavailable,
        };
        assert_eq!(observation.state(), WorkspaceObservationState::Unknown);
        assert_eq!(observation.value(), None);
    }

    #[test]
    fn partial_observation_keeps_safe_per_target_evidence() {
        let observation = WorkspaceObservation::Partial {
            value: -7_i64,
            successful_target_count: 1,
            failures: vec![WorkspaceTargetFailure {
                target: "broker-b".into(),
                stage: WorkspaceFailureStage::Progress,
                code: WorkspaceFailureCode::Unavailable,
                retryable: true,
            }],
        };
        assert_eq!(observation.value(), Some(&-7));
        assert_eq!(observation.state(), WorkspaceObservationState::Partial);
    }
}
