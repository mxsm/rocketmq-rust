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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::ClusterId;
use crate::ComplianceFindingId;
use crate::EvidenceId;
use crate::FleetId;
use crate::FleetInspectionRunId;
use crate::QuotaPolicyId;
use crate::RegionId;
use crate::SreTimestamp;
use crate::TenantId;

/// Stable schema family for enterprise Fleet contracts.
pub const FLEET_SCHEMA_VERSION: &str = "rocketmq-sre.fleet.v1";

/// Deployment environment used for Fleet filtering and policy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FleetEnvironment {
    Development,
    Test,
    Staging,
    Production,
    Other,
}

/// Persisted lifecycle for one registered RocketMQ cluster.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterRegistrationState {
    Pending,
    Onboarding,
    Active,
    ReadOnlyDegraded,
    Offboarding,
    Retired,
}

impl ClusterRegistrationState {
    /// Returns whether a requested lifecycle transition is valid.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Pending, Self::Onboarding)
                | (
                    Self::Onboarding,
                    Self::Active | Self::ReadOnlyDegraded | Self::Offboarding
                )
                | (Self::Active, Self::ReadOnlyDegraded | Self::Offboarding)
                | (Self::ReadOnlyDegraded, Self::Active | Self::Offboarding)
                | (Self::Offboarding, Self::Retired)
        )
    }
}

/// Data-residency treatment for a Fleet artifact.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DataResidencyClass {
    RegionLocal,
    AggregatedMetadata,
    ExportAllowed,
}

/// Enterprise grouping of one or more RocketMQ tenants.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Fleet {
    pub id: FleetId,
    pub name: String,
    pub owner: String,
    pub created_at: SreTimestamp,
    pub updated_at: SreTimestamp,
}

/// Tenant metadata scoped to exactly one Fleet.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FleetTenant {
    pub id: TenantId,
    pub fleet_id: FleetId,
    pub name: String,
    pub owner: String,
    pub active: bool,
    pub created_at: SreTimestamp,
    pub updated_at: SreTimestamp,
}

/// Region and data-residency boundary within one Fleet.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FleetRegion {
    pub id: RegionId,
    pub fleet_id: FleetId,
    pub key: String,
    pub display_name: String,
    pub owner: String,
    pub residency_tags: BTreeSet<String>,
    pub active: bool,
    pub created_at: SreTimestamp,
    pub updated_at: SreTimestamp,
}

/// Fleet placement and lifecycle metadata for one RocketMQ cluster.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterRegistration {
    pub cluster_id: ClusterId,
    pub fleet_id: FleetId,
    pub tenant_id: TenantId,
    pub region_id: RegionId,
    pub external_cluster_key: String,
    pub environment: FleetEnvironment,
    pub owner: String,
    pub state: ClusterRegistrationState,
    pub residency_tags: BTreeSet<String>,
    pub lifecycle_revision: u64,
    pub created_at: SreTimestamp,
    pub updated_at: SreTimestamp,
}

/// Region-local runtime process types participating in a capability handshake.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RegionalEndpointKind {
    Connector,
    Executor,
    ExecutionAgent,
    Mcp,
}

/// Health state for a regional process registration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RegionalEndpointHealth {
    Healthy,
    Degraded,
    Disconnected,
    Incompatible,
}

/// Capability registration emitted by a region-local runtime.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RegionalEndpoint {
    pub id: String,
    pub fleet_id: FleetId,
    pub tenant_id: TenantId,
    pub region_id: RegionId,
    pub cluster_id: Option<ClusterId>,
    pub kind: RegionalEndpointKind,
    pub component_version: String,
    pub protocol_version: String,
    pub schema_digest: String,
    pub capabilities: BTreeSet<String>,
    pub residency_tags: BTreeSet<String>,
    pub capacity: u32,
    pub health: RegionalEndpointHealth,
    pub last_heartbeat_at: SreTimestamp,
}

/// Hard limits for bounded Fleet work.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaLimits {
    pub queries_per_minute: u32,
    pub model_tokens_per_hour: u64,
    pub concurrent_workflows: u32,
    pub concurrent_inspections: u32,
    pub evidence_bytes_per_hour: u64,
    pub notifications_per_hour: u32,
    pub automatic_actions_per_hour: u32,
}

/// Versioned quota policy at tenant, region, or cluster scope.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaPolicy {
    pub id: QuotaPolicyId,
    pub fleet_id: FleetId,
    pub tenant_id: TenantId,
    pub region_id: Option<RegionId>,
    pub cluster_id: Option<ClusterId>,
    pub version: u64,
    pub limits: QuotaLimits,
    pub owner: String,
    pub active: bool,
    pub created_at: SreTimestamp,
}

/// Current bounded usage compared with a quota policy.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaUsage {
    pub policy_id: QuotaPolicyId,
    pub queries: u64,
    pub model_tokens: u64,
    pub active_workflows: u32,
    pub active_inspections: u32,
    pub evidence_bytes: u64,
    pub notifications: u64,
    pub automatic_actions: u64,
    pub observed_at: SreTimestamp,
}

/// Aggregated, non-sensitive Fleet asset index entry.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FleetAssetIndex {
    pub cluster_id: ClusterId,
    pub fleet_id: FleetId,
    pub tenant_id: TenantId,
    pub region_id: RegionId,
    pub environment: FleetEnvironment,
    pub owner: String,
    pub component: String,
    pub component_version: String,
    pub image_digest: Option<String>,
    pub feature_digest: Option<String>,
    pub configuration_digest: Option<String>,
    pub health: String,
    pub attributes: BTreeMap<String, String>,
    pub observed_at: SreTimestamp,
}

/// Severity of one Fleet compliance difference.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ComplianceSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

/// State of a compliance finding without an automatic mutation path.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ComplianceFindingState {
    Open,
    Acknowledged,
    Resolved,
    AcceptedException,
}

/// Immutable comparison between an expected template and live cluster state.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ComplianceFinding {
    pub id: ComplianceFindingId,
    pub fleet_id: FleetId,
    pub tenant_id: TenantId,
    pub region_id: RegionId,
    pub cluster_id: ClusterId,
    pub category: String,
    pub expected_digest: String,
    pub live_digest: String,
    pub evidence_ids: Vec<EvidenceId>,
    pub severity: ComplianceSeverity,
    pub owner: String,
    pub recommendation: String,
    pub state: ComplianceFindingState,
    pub observed_at: SreTimestamp,
}

/// Bounded, sharded Fleet inspection state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FleetInspectionState {
    Pending,
    Running,
    Completed,
    PartiallyCompleted,
    Failed,
    Cancelled,
}

/// One bounded multi-cluster inspection with explicit budgets.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FleetInspectionRun {
    pub id: FleetInspectionRunId,
    pub fleet_id: FleetId,
    pub tenant_id: TenantId,
    pub region_ids: BTreeSet<RegionId>,
    pub cluster_ids: Vec<ClusterId>,
    pub pack_ids: Vec<String>,
    pub max_concurrency: u32,
    pub timeout_seconds: u32,
    pub model_token_budget: u64,
    pub evidence_byte_budget: u64,
    pub state: FleetInspectionState,
    pub completed_clusters: u32,
    pub failed_clusters: u32,
    pub created_at: SreTimestamp,
    pub completed_at: Option<SreTimestamp>,
}

/// Stable dimensions used for showback and optional chargeback.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CostAllocationKey {
    pub fleet_id: FleetId,
    pub tenant_id: TenantId,
    pub region_id: RegionId,
    pub cluster_id: Option<ClusterId>,
    pub environment: FleetEnvironment,
    pub owner: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_lifecycle_is_forward_only_with_explicit_degradation_recovery() {
        assert!(ClusterRegistrationState::Pending.can_transition_to(ClusterRegistrationState::Onboarding));
        assert!(ClusterRegistrationState::Onboarding.can_transition_to(ClusterRegistrationState::Active));
        assert!(ClusterRegistrationState::Active.can_transition_to(ClusterRegistrationState::ReadOnlyDegraded));
        assert!(ClusterRegistrationState::ReadOnlyDegraded.can_transition_to(ClusterRegistrationState::Active));
        assert!(ClusterRegistrationState::Active.can_transition_to(ClusterRegistrationState::Offboarding));
        assert!(ClusterRegistrationState::Offboarding.can_transition_to(ClusterRegistrationState::Retired));

        assert!(!ClusterRegistrationState::Retired.can_transition_to(ClusterRegistrationState::Active));
        assert!(!ClusterRegistrationState::Offboarding.can_transition_to(ClusterRegistrationState::Active));
        assert!(!ClusterRegistrationState::Pending.can_transition_to(ClusterRegistrationState::Retired));
    }
}
