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

use std::collections::BTreeSet;

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::FinOpsAllocationMode;
use rocketmq_sre_contracts::FinOpsAllocationPolicy;
use rocketmq_sre_contracts::FinOpsAllocationPolicyId;
use rocketmq_sre_contracts::FinOpsBudget;
use rocketmq_sre_contracts::FinOpsBudgetDecision;
use rocketmq_sre_contracts::FinOpsBudgetId;
use rocketmq_sre_contracts::FinOpsBudgetPeriod;
use rocketmq_sre_contracts::FinOpsBudgetScopeKind;
use rocketmq_sre_contracts::FinOpsCostEntry;
use rocketmq_sre_contracts::FinOpsCostEntryId;
use rocketmq_sre_contracts::FinOpsCostSource;
use rocketmq_sre_contracts::FinOpsDecisionId;
use rocketmq_sre_contracts::FinOpsDegradation;
use rocketmq_sre_contracts::FinOpsWorkClass;
use rocketmq_sre_contracts::FinOpsWorkloadKind;
use rocketmq_sre_contracts::FleetId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::RegionId;
use rocketmq_sre_contracts::TenantId;
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::ControlPlaneError;

pub(super) fn cost_entry_from_row(row: &PgRow) -> Result<FinOpsCostEntry, ControlPlaneError> {
    Ok(FinOpsCostEntry {
        id: FinOpsCostEntryId::from_uuid(row.try_get("id")?),
        idempotency_key: row.try_get("idempotency_key")?,
        fleet_id: FleetId::from_uuid(row.try_get("fleet_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        region_id: RegionId::from_uuid(row.try_get("region_id")?),
        cluster_id: row.try_get::<Option<Uuid>, _>("cluster_id")?.map(ClusterId::from_uuid),
        source: cost_source(row.try_get("source_kind")?)?,
        workload_kind: workload_kind(row.try_get("workload_kind")?)?,
        provider_profile: row.try_get("provider_profile")?,
        model_family: row.try_get("model_family")?,
        incident_id: row
            .try_get::<Option<Uuid>, _>("incident_id")?
            .map(IncidentId::from_uuid),
        pack_id: row.try_get("pack_id")?,
        workflow_id: row.try_get("workflow_id")?,
        request_count: unsigned(row.try_get("request_count")?, "request count")?,
        input_tokens: unsigned(row.try_get("input_tokens")?, "input tokens")?,
        output_tokens: unsigned(row.try_get("output_tokens")?, "output tokens")?,
        latency_millis: unsigned(row.try_get("latency_millis")?, "latency")?,
        error_count: unsigned(row.try_get("error_count")?, "error count")?,
        quantity_millis: unsigned(row.try_get("quantity_millis")?, "quantity")?,
        cost_micros: unsigned(row.try_get("cost_micros")?, "cost")?,
        occurred_at: row.try_get("occurred_at")?,
        recorded_at: row.try_get("recorded_at")?,
    })
}

pub(super) fn budget_from_row(row: &PgRow) -> Result<FinOpsBudget, ControlPlaneError> {
    Ok(FinOpsBudget {
        id: FinOpsBudgetId::from_uuid(row.try_get("id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        scope_kind: budget_scope(row.try_get("scope_kind")?)?,
        scope_key: row.try_get("scope_key")?,
        version: unsigned(row.try_get("budget_version")?, "budget version")?,
        period: budget_period(row.try_get("period_kind")?)?,
        soft_limit_micros: unsigned(row.try_get("soft_limit_micros")?, "soft limit")?,
        hard_limit_micros: unsigned(row.try_get("hard_limit_micros")?, "hard limit")?,
        owner: row.try_get("owner_name")?,
        active: row.try_get("active")?,
        created_at: row.try_get("created_at")?,
    })
}

pub(super) fn decision_from_row(row: &PgRow) -> Result<FinOpsBudgetDecision, ControlPlaneError> {
    let protected = row.try_get::<Vec<String>, _>("protected_controls")?;
    Ok(FinOpsBudgetDecision {
        id: FinOpsDecisionId::from_uuid(row.try_get("id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: row.try_get::<Option<Uuid>, _>("cluster_id")?.map(ClusterId::from_uuid),
        budget_id: FinOpsBudgetId::from_uuid(row.try_get("budget_id")?),
        work_class: work_class(row.try_get("work_class")?)?,
        requested_cost_micros: unsigned(row.try_get("requested_cost_micros")?, "requested cost")?,
        observed_cost_micros: unsigned(row.try_get("observed_cost_micros")?, "observed cost")?,
        projected_cost_micros: unsigned(row.try_get("projected_cost_micros")?, "projected cost")?,
        soft_limit_micros: unsigned(row.try_get("soft_limit_micros")?, "soft limit")?,
        hard_limit_micros: unsigned(row.try_get("hard_limit_micros")?, "hard limit")?,
        allowed: row.try_get("allowed")?,
        degradation: degradation(row.try_get("degradation")?)?,
        reason_code: row.try_get("reason_code")?,
        protected_controls: protected
            .into_iter()
            .map(work_class)
            .collect::<Result<BTreeSet<_>, _>>()?,
        evaluated_at: row.try_get("evaluated_at")?,
    })
}

pub(super) fn allocation_from_row(row: &PgRow) -> Result<FinOpsAllocationPolicy, ControlPlaneError> {
    let allocation_keys: BTreeSet<String> = serde_json::from_value(row.try_get("allocation_keys")?).map_err(|_| {
        ControlPlaneError::validation(
            "invalid_persisted_finops_state",
            "allocation keys do not match the FinOps contract",
        )
    })?;
    Ok(FinOpsAllocationPolicy {
        id: FinOpsAllocationPolicyId::from_uuid(row.try_get("id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        version: unsigned(row.try_get("policy_version")?, "allocation policy version")?,
        mode: allocation_mode(row.try_get("allocation_mode")?)?,
        allocation_keys,
        organization_confirmed: row.try_get("organization_confirmed")?,
        owner: row.try_get("owner_name")?,
        active: row.try_get("active")?,
        created_at: row.try_get("created_at")?,
    })
}

pub(super) const fn cost_source_name(value: FinOpsCostSource) -> &'static str {
    match value {
        FinOpsCostSource::ModelInvocation => "model_invocation",
        FinOpsCostSource::ControlPlane => "control_plane",
        FinOpsCostSource::Connector => "connector",
        FinOpsCostSource::ExecutionAgent => "execution_agent",
        FinOpsCostSource::Observability => "observability",
        FinOpsCostSource::ObjectStorage => "object_storage",
        FinOpsCostSource::SyntheticProbe => "synthetic_probe",
    }
}

pub(super) const fn workload_name(value: FinOpsWorkloadKind) -> &'static str {
    match value {
        FinOpsWorkloadKind::Incident => "incident",
        FinOpsWorkloadKind::DiagnosticPack => "diagnostic_pack",
        FinOpsWorkloadKind::Workflow => "workflow",
        FinOpsWorkloadKind::Inspection => "inspection",
        FinOpsWorkloadKind::Verification => "verification",
        FinOpsWorkloadKind::Rollback => "rollback",
        FinOpsWorkloadKind::Audit => "audit",
        FinOpsWorkloadKind::System => "system",
    }
}

pub(super) const fn budget_scope_name(value: FinOpsBudgetScopeKind) -> &'static str {
    match value {
        FinOpsBudgetScopeKind::Tenant => "tenant",
        FinOpsBudgetScopeKind::Provider => "provider",
        FinOpsBudgetScopeKind::Model => "model",
        FinOpsBudgetScopeKind::Region => "region",
        FinOpsBudgetScopeKind::Cluster => "cluster",
        FinOpsBudgetScopeKind::Incident => "incident",
        FinOpsBudgetScopeKind::DiagnosticPack => "diagnostic_pack",
        FinOpsBudgetScopeKind::Workflow => "workflow",
    }
}

pub(super) const fn budget_period_name(value: FinOpsBudgetPeriod) -> &'static str {
    match value {
        FinOpsBudgetPeriod::Hourly => "hourly",
        FinOpsBudgetPeriod::Daily => "daily",
        FinOpsBudgetPeriod::Monthly => "monthly",
    }
}

pub(super) const fn work_class_name(value: FinOpsWorkClass) -> &'static str {
    match value {
        FinOpsWorkClass::SafetyCheck => "safety_check",
        FinOpsWorkClass::Audit => "audit",
        FinOpsWorkClass::Verification => "verification",
        FinOpsWorkClass::Rollback => "rollback",
        FinOpsWorkClass::ActiveIncident => "active_incident",
        FinOpsWorkClass::Interactive => "interactive",
        FinOpsWorkClass::Background => "background",
    }
}

pub(super) const fn degradation_name(value: FinOpsDegradation) -> &'static str {
    match value {
        FinOpsDegradation::None => "none",
        FinOpsDegradation::PreferLowerCostModel => "prefer_lower_cost_model",
        FinOpsDegradation::ReduceSampling => "reduce_sampling",
        FinOpsDegradation::DeferLowPriority => "defer_low_priority",
        FinOpsDegradation::DenyLowPriority => "deny_low_priority",
    }
}

pub(super) const fn allocation_mode_name(value: FinOpsAllocationMode) -> &'static str {
    match value {
        FinOpsAllocationMode::Showback => "showback",
        FinOpsAllocationMode::Chargeback => "chargeback",
    }
}

fn cost_source(value: String) -> Result<FinOpsCostSource, ControlPlaneError> {
    match value.as_str() {
        "model_invocation" => Ok(FinOpsCostSource::ModelInvocation),
        "control_plane" => Ok(FinOpsCostSource::ControlPlane),
        "connector" => Ok(FinOpsCostSource::Connector),
        "execution_agent" => Ok(FinOpsCostSource::ExecutionAgent),
        "observability" => Ok(FinOpsCostSource::Observability),
        "object_storage" => Ok(FinOpsCostSource::ObjectStorage),
        "synthetic_probe" => Ok(FinOpsCostSource::SyntheticProbe),
        _ => Err(invalid_persisted("cost source")),
    }
}

fn workload_kind(value: String) -> Result<FinOpsWorkloadKind, ControlPlaneError> {
    match value.as_str() {
        "incident" => Ok(FinOpsWorkloadKind::Incident),
        "diagnostic_pack" => Ok(FinOpsWorkloadKind::DiagnosticPack),
        "workflow" => Ok(FinOpsWorkloadKind::Workflow),
        "inspection" => Ok(FinOpsWorkloadKind::Inspection),
        "verification" => Ok(FinOpsWorkloadKind::Verification),
        "rollback" => Ok(FinOpsWorkloadKind::Rollback),
        "audit" => Ok(FinOpsWorkloadKind::Audit),
        "system" => Ok(FinOpsWorkloadKind::System),
        _ => Err(invalid_persisted("workload kind")),
    }
}

fn budget_scope(value: String) -> Result<FinOpsBudgetScopeKind, ControlPlaneError> {
    match value.as_str() {
        "tenant" => Ok(FinOpsBudgetScopeKind::Tenant),
        "provider" => Ok(FinOpsBudgetScopeKind::Provider),
        "model" => Ok(FinOpsBudgetScopeKind::Model),
        "region" => Ok(FinOpsBudgetScopeKind::Region),
        "cluster" => Ok(FinOpsBudgetScopeKind::Cluster),
        "incident" => Ok(FinOpsBudgetScopeKind::Incident),
        "diagnostic_pack" => Ok(FinOpsBudgetScopeKind::DiagnosticPack),
        "workflow" => Ok(FinOpsBudgetScopeKind::Workflow),
        _ => Err(invalid_persisted("budget scope")),
    }
}

fn budget_period(value: String) -> Result<FinOpsBudgetPeriod, ControlPlaneError> {
    match value.as_str() {
        "hourly" => Ok(FinOpsBudgetPeriod::Hourly),
        "daily" => Ok(FinOpsBudgetPeriod::Daily),
        "monthly" => Ok(FinOpsBudgetPeriod::Monthly),
        _ => Err(invalid_persisted("budget period")),
    }
}

fn work_class(value: String) -> Result<FinOpsWorkClass, ControlPlaneError> {
    match value.as_str() {
        "safety_check" => Ok(FinOpsWorkClass::SafetyCheck),
        "audit" => Ok(FinOpsWorkClass::Audit),
        "verification" => Ok(FinOpsWorkClass::Verification),
        "rollback" => Ok(FinOpsWorkClass::Rollback),
        "active_incident" => Ok(FinOpsWorkClass::ActiveIncident),
        "interactive" => Ok(FinOpsWorkClass::Interactive),
        "background" => Ok(FinOpsWorkClass::Background),
        _ => Err(invalid_persisted("work class")),
    }
}

fn degradation(value: String) -> Result<FinOpsDegradation, ControlPlaneError> {
    match value.as_str() {
        "none" => Ok(FinOpsDegradation::None),
        "prefer_lower_cost_model" => Ok(FinOpsDegradation::PreferLowerCostModel),
        "reduce_sampling" => Ok(FinOpsDegradation::ReduceSampling),
        "defer_low_priority" => Ok(FinOpsDegradation::DeferLowPriority),
        "deny_low_priority" => Ok(FinOpsDegradation::DenyLowPriority),
        _ => Err(invalid_persisted("degradation")),
    }
}

fn allocation_mode(value: String) -> Result<FinOpsAllocationMode, ControlPlaneError> {
    match value.as_str() {
        "showback" => Ok(FinOpsAllocationMode::Showback),
        "chargeback" => Ok(FinOpsAllocationMode::Chargeback),
        _ => Err(invalid_persisted("allocation mode")),
    }
}

fn unsigned(value: i64, field: &str) -> Result<u64, ControlPlaneError> {
    u64::try_from(value).map_err(|_| invalid_persisted(field))
}

fn invalid_persisted(field: &str) -> ControlPlaneError {
    ControlPlaneError::validation(
        "invalid_persisted_finops_state",
        format!("persisted FinOps {field} is invalid"),
    )
}
