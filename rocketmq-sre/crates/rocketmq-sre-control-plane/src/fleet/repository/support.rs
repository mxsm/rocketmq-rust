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

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ClusterRegistration;
use rocketmq_sre_contracts::ClusterRegistrationState;
use rocketmq_sre_contracts::ComplianceFinding;
use rocketmq_sre_contracts::ComplianceFindingId;
use rocketmq_sre_contracts::ComplianceFindingState;
use rocketmq_sre_contracts::ComplianceSeverity;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::Fleet;
use rocketmq_sre_contracts::FleetAssetIndex;
use rocketmq_sre_contracts::FleetEnvironment;
use rocketmq_sre_contracts::FleetId;
use rocketmq_sre_contracts::FleetInspectionRun;
use rocketmq_sre_contracts::FleetInspectionRunId;
use rocketmq_sre_contracts::FleetInspectionState;
use rocketmq_sre_contracts::FleetRegion;
use rocketmq_sre_contracts::FleetTenant;
use rocketmq_sre_contracts::QuotaLimits;
use rocketmq_sre_contracts::QuotaPolicy;
use rocketmq_sre_contracts::QuotaPolicyId;
use rocketmq_sre_contracts::QuotaUsage;
use rocketmq_sre_contracts::RegionId;
use rocketmq_sre_contracts::RegionalEndpoint;
use rocketmq_sre_contracts::RegionalEndpointHealth;
use rocketmq_sre_contracts::RegionalEndpointKind;
use rocketmq_sre_contracts::TenantId;
use serde_json::Value;
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::ControlPlaneError;

pub(super) fn fleet_from_row(row: &PgRow) -> Result<Fleet, ControlPlaneError> {
    Ok(Fleet {
        id: FleetId::from_uuid(row.try_get("id")?),
        name: row.try_get("name")?,
        owner: row.try_get("owner_name")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub(super) fn tenant_from_row(row: &PgRow) -> Result<FleetTenant, ControlPlaneError> {
    Ok(FleetTenant {
        id: TenantId::from_uuid(row.try_get("id")?),
        fleet_id: FleetId::from_uuid(row.try_get("fleet_id")?),
        name: row.try_get("name")?,
        owner: row.try_get("owner_name")?,
        active: row.try_get("active")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub(super) fn region_from_row(row: &PgRow) -> Result<FleetRegion, ControlPlaneError> {
    Ok(FleetRegion {
        id: RegionId::from_uuid(row.try_get("id")?),
        fleet_id: FleetId::from_uuid(row.try_get("fleet_id")?),
        key: row.try_get("region_key")?,
        display_name: row.try_get("display_name")?,
        owner: row.try_get("owner_name")?,
        residency_tags: string_set(row.try_get("residency_tags")?)?,
        active: row.try_get("active")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub(super) fn registration_from_row(row: &PgRow) -> Result<ClusterRegistration, ControlPlaneError> {
    Ok(ClusterRegistration {
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        fleet_id: FleetId::from_uuid(row.try_get("fleet_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        region_id: RegionId::from_uuid(row.try_get("region_id")?),
        external_cluster_key: row.try_get("external_cluster_key")?,
        environment: environment(row.try_get("environment")?)?,
        owner: row.try_get("owner_name")?,
        state: registration_state(row.try_get("lifecycle_state")?)?,
        residency_tags: string_set(row.try_get("residency_tags")?)?,
        lifecycle_revision: u64_value(row.try_get("lifecycle_revision")?, "lifecycle revision")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub(super) fn quota_policy_from_row(row: &PgRow) -> Result<QuotaPolicy, ControlPlaneError> {
    Ok(QuotaPolicy {
        id: QuotaPolicyId::from_uuid(row.try_get("id")?),
        fleet_id: FleetId::from_uuid(row.try_get("fleet_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        region_id: row.try_get::<Option<Uuid>, _>("region_id")?.map(RegionId::from_uuid),
        cluster_id: row.try_get::<Option<Uuid>, _>("cluster_id")?.map(ClusterId::from_uuid),
        version: u64_value(row.try_get("policy_version")?, "policy version")?,
        limits: QuotaLimits {
            queries_per_minute: u32_value(row.try_get("queries_per_minute")?, "queries per minute")?,
            model_tokens_per_hour: u64_value(row.try_get("model_tokens_per_hour")?, "model tokens per hour")?,
            concurrent_workflows: u32_value(row.try_get("concurrent_workflows")?, "concurrent workflows")?,
            concurrent_inspections: u32_value(row.try_get("concurrent_inspections")?, "concurrent inspections")?,
            evidence_bytes_per_hour: u64_value(row.try_get("evidence_bytes_per_hour")?, "evidence bytes per hour")?,
            notifications_per_hour: u32_value(row.try_get("notifications_per_hour")?, "notifications per hour")?,
            automatic_actions_per_hour: u32_value(
                row.try_get("automatic_actions_per_hour")?,
                "automatic actions per hour",
            )?,
        },
        owner: row.try_get("owner_name")?,
        active: row.try_get("active")?,
        created_at: row.try_get("created_at")?,
    })
}

pub(super) fn quota_usage_from_row(policy_id: QuotaPolicyId, row: &PgRow) -> Result<QuotaUsage, ControlPlaneError> {
    Ok(QuotaUsage {
        policy_id,
        queries: u64_value(row.try_get("queries")?, "query usage")?,
        model_tokens: u64_value(row.try_get("model_tokens")?, "model token usage")?,
        active_workflows: u32_value(row.try_get("active_workflows")?, "active workflows")?,
        active_inspections: u32_value(row.try_get("active_inspections")?, "active inspections")?,
        evidence_bytes: u64_value(row.try_get("evidence_bytes")?, "evidence bytes")?,
        notifications: u64_value(row.try_get("notifications")?, "notifications")?,
        automatic_actions: u64_value(row.try_get("automatic_actions")?, "automatic actions")?,
        observed_at: row.try_get("observed_at")?,
    })
}

pub(super) fn endpoint_from_row(row: &PgRow) -> Result<RegionalEndpoint, ControlPlaneError> {
    Ok(RegionalEndpoint {
        id: row.try_get("id")?,
        fleet_id: FleetId::from_uuid(row.try_get("fleet_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        region_id: RegionId::from_uuid(row.try_get("region_id")?),
        cluster_id: row.try_get::<Option<Uuid>, _>("cluster_id")?.map(ClusterId::from_uuid),
        kind: endpoint_kind(row.try_get("endpoint_kind")?)?,
        component_version: row.try_get("component_version")?,
        protocol_version: row.try_get("protocol_version")?,
        schema_digest: row.try_get("schema_digest")?,
        capabilities: string_set(row.try_get("capabilities")?)?,
        residency_tags: string_set(row.try_get("residency_tags")?)?,
        capacity: u32_value(row.try_get("capacity")?, "endpoint capacity")?,
        health: endpoint_health(row.try_get("health")?)?,
        last_heartbeat_at: row.try_get("last_heartbeat_at")?,
    })
}

pub(super) fn asset_from_row(row: &PgRow) -> Result<FleetAssetIndex, ControlPlaneError> {
    Ok(FleetAssetIndex {
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        fleet_id: FleetId::from_uuid(row.try_get("fleet_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        region_id: RegionId::from_uuid(row.try_get("region_id")?),
        environment: environment(row.try_get("environment")?)?,
        owner: row.try_get("owner_name")?,
        component: row.try_get("component")?,
        component_version: row.try_get("component_version")?,
        image_digest: row.try_get("image_digest")?,
        feature_digest: row.try_get("feature_digest")?,
        configuration_digest: row.try_get("configuration_digest")?,
        health: row.try_get("health")?,
        attributes: string_map(row.try_get("attributes")?)?,
        observed_at: row.try_get("observed_at")?,
    })
}

pub(super) fn finding_from_row(row: &PgRow) -> Result<ComplianceFinding, ControlPlaneError> {
    Ok(ComplianceFinding {
        id: ComplianceFindingId::from_uuid(row.try_get("id")?),
        fleet_id: FleetId::from_uuid(row.try_get("fleet_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        region_id: RegionId::from_uuid(row.try_get("region_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        category: row.try_get("category")?,
        expected_digest: row.try_get("expected_digest")?,
        live_digest: row.try_get("live_digest")?,
        evidence_ids: row
            .try_get::<Vec<Uuid>, _>("evidence_ids")?
            .into_iter()
            .map(EvidenceId::from_uuid)
            .collect(),
        severity: compliance_severity(row.try_get("severity")?)?,
        owner: row.try_get("owner_name")?,
        recommendation: row.try_get("recommendation")?,
        state: finding_state(row.try_get("finding_state")?)?,
        observed_at: row.try_get("observed_at")?,
    })
}

pub(super) fn inspection_from_row(row: &PgRow) -> Result<FleetInspectionRun, ControlPlaneError> {
    Ok(FleetInspectionRun {
        id: FleetInspectionRunId::from_uuid(row.try_get("id")?),
        fleet_id: FleetId::from_uuid(row.try_get("fleet_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        region_ids: row
            .try_get::<Vec<Uuid>, _>("region_ids")?
            .into_iter()
            .map(RegionId::from_uuid)
            .collect(),
        cluster_ids: row
            .try_get::<Vec<Uuid>, _>("cluster_ids")?
            .into_iter()
            .map(ClusterId::from_uuid)
            .collect(),
        pack_ids: row.try_get("pack_ids")?,
        max_concurrency: u32_value(row.try_get("max_concurrency")?, "inspection concurrency")?,
        timeout_seconds: u32_value(row.try_get("timeout_seconds")?, "inspection timeout")?,
        model_token_budget: u64_value(row.try_get("model_token_budget")?, "inspection token budget")?,
        evidence_byte_budget: u64_value(row.try_get("evidence_byte_budget")?, "inspection evidence budget")?,
        state: inspection_state(row.try_get("inspection_state")?)?,
        completed_clusters: u32_value(row.try_get("completed_clusters")?, "completed clusters")?,
        failed_clusters: u32_value(row.try_get("failed_clusters")?, "failed clusters")?,
        created_at: row.try_get("created_at")?,
        completed_at: row.try_get("completed_at")?,
    })
}

pub(super) const fn environment_name(value: FleetEnvironment) -> &'static str {
    match value {
        FleetEnvironment::Development => "development",
        FleetEnvironment::Test => "test",
        FleetEnvironment::Staging => "staging",
        FleetEnvironment::Production => "production",
        FleetEnvironment::Other => "other",
    }
}

pub(super) const fn endpoint_kind_name(value: RegionalEndpointKind) -> &'static str {
    match value {
        RegionalEndpointKind::Connector => "connector",
        RegionalEndpointKind::Executor => "executor",
        RegionalEndpointKind::ExecutionAgent => "execution_agent",
        RegionalEndpointKind::Mcp => "mcp",
    }
}

pub(super) const fn endpoint_health_name(value: RegionalEndpointHealth) -> &'static str {
    match value {
        RegionalEndpointHealth::Healthy => "healthy",
        RegionalEndpointHealth::Degraded => "degraded",
        RegionalEndpointHealth::Disconnected => "disconnected",
        RegionalEndpointHealth::Incompatible => "incompatible",
    }
}

pub(super) const fn compliance_severity_name(value: ComplianceSeverity) -> &'static str {
    match value {
        ComplianceSeverity::Info => "info",
        ComplianceSeverity::Warning => "warning",
        ComplianceSeverity::Error => "error",
        ComplianceSeverity::Critical => "critical",
    }
}

pub(super) const fn finding_state_name(value: ComplianceFindingState) -> &'static str {
    match value {
        ComplianceFindingState::Open => "open",
        ComplianceFindingState::Acknowledged => "acknowledged",
        ComplianceFindingState::Resolved => "resolved",
        ComplianceFindingState::AcceptedException => "accepted_exception",
    }
}

fn environment(value: String) -> Result<FleetEnvironment, ControlPlaneError> {
    match value.as_str() {
        "development" => Ok(FleetEnvironment::Development),
        "test" => Ok(FleetEnvironment::Test),
        "staging" => Ok(FleetEnvironment::Staging),
        "production" => Ok(FleetEnvironment::Production),
        "other" => Ok(FleetEnvironment::Other),
        _ => Err(stored_value_error("Fleet environment")),
    }
}

fn registration_state(value: String) -> Result<ClusterRegistrationState, ControlPlaneError> {
    match value.as_str() {
        "pending" => Ok(ClusterRegistrationState::Pending),
        "onboarding" => Ok(ClusterRegistrationState::Onboarding),
        "active" => Ok(ClusterRegistrationState::Active),
        "read_only_degraded" => Ok(ClusterRegistrationState::ReadOnlyDegraded),
        "offboarding" => Ok(ClusterRegistrationState::Offboarding),
        "retired" => Ok(ClusterRegistrationState::Retired),
        _ => Err(stored_value_error("cluster registration state")),
    }
}

fn endpoint_kind(value: String) -> Result<RegionalEndpointKind, ControlPlaneError> {
    match value.as_str() {
        "connector" => Ok(RegionalEndpointKind::Connector),
        "executor" => Ok(RegionalEndpointKind::Executor),
        "execution_agent" => Ok(RegionalEndpointKind::ExecutionAgent),
        "mcp" => Ok(RegionalEndpointKind::Mcp),
        _ => Err(stored_value_error("regional endpoint kind")),
    }
}

fn endpoint_health(value: String) -> Result<RegionalEndpointHealth, ControlPlaneError> {
    match value.as_str() {
        "healthy" => Ok(RegionalEndpointHealth::Healthy),
        "degraded" => Ok(RegionalEndpointHealth::Degraded),
        "disconnected" => Ok(RegionalEndpointHealth::Disconnected),
        "incompatible" => Ok(RegionalEndpointHealth::Incompatible),
        _ => Err(stored_value_error("regional endpoint health")),
    }
}

fn compliance_severity(value: String) -> Result<ComplianceSeverity, ControlPlaneError> {
    match value.as_str() {
        "info" => Ok(ComplianceSeverity::Info),
        "warning" => Ok(ComplianceSeverity::Warning),
        "error" => Ok(ComplianceSeverity::Error),
        "critical" => Ok(ComplianceSeverity::Critical),
        _ => Err(stored_value_error("compliance severity")),
    }
}

fn finding_state(value: String) -> Result<ComplianceFindingState, ControlPlaneError> {
    match value.as_str() {
        "open" => Ok(ComplianceFindingState::Open),
        "acknowledged" => Ok(ComplianceFindingState::Acknowledged),
        "resolved" => Ok(ComplianceFindingState::Resolved),
        "accepted_exception" => Ok(ComplianceFindingState::AcceptedException),
        _ => Err(stored_value_error("compliance finding state")),
    }
}

fn inspection_state(value: String) -> Result<FleetInspectionState, ControlPlaneError> {
    match value.as_str() {
        "pending" => Ok(FleetInspectionState::Pending),
        "running" => Ok(FleetInspectionState::Running),
        "completed" => Ok(FleetInspectionState::Completed),
        "partially_completed" => Ok(FleetInspectionState::PartiallyCompleted),
        "failed" => Ok(FleetInspectionState::Failed),
        "cancelled" => Ok(FleetInspectionState::Cancelled),
        _ => Err(stored_value_error("Fleet inspection state")),
    }
}

fn string_set(value: Value) -> Result<BTreeSet<String>, ControlPlaneError> {
    serde_json::from_value(value).map_err(|_| stored_value_error("Fleet string set"))
}

fn string_map(value: Value) -> Result<BTreeMap<String, String>, ControlPlaneError> {
    serde_json::from_value(value).map_err(|_| stored_value_error("Fleet attribute map"))
}

fn u64_value(value: i64, field: &str) -> Result<u64, ControlPlaneError> {
    u64::try_from(value).map_err(|_| stored_value_error(field))
}

fn u32_value(value: i32, field: &str) -> Result<u32, ControlPlaneError> {
    u32::try_from(value).map_err(|_| stored_value_error(field))
}

fn stored_value_error(field: &str) -> ControlPlaneError {
    ControlPlaneError::configuration(format!("{field} contains an invalid persisted value"))
}
