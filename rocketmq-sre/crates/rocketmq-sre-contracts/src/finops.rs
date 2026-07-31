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

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::ClusterId;
use crate::FinOpsAllocationPolicyId;
use crate::FinOpsBudgetId;
use crate::FinOpsCostEntryId;
use crate::FinOpsDecisionId;
use crate::FleetId;
use crate::IncidentId;
use crate::RegionId;
use crate::TenantId;

pub const FINOPS_SCHEMA_VERSION: &str = "rocketmq-sre.finops.v1";

/// Metered source represented in the shared cost ledger.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinOpsCostSource {
    ModelInvocation,
    ControlPlane,
    Connector,
    ExecutionAgent,
    Observability,
    ObjectStorage,
    SyntheticProbe,
}

/// Stable workload dimension used for cost and outcome attribution.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinOpsWorkloadKind {
    Incident,
    DiagnosticPack,
    Workflow,
    Inspection,
    Verification,
    Rollback,
    Audit,
    System,
}

/// Append-only usage and cost record. Monetary values are micro-US dollars.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinOpsCostEntry {
    pub id: FinOpsCostEntryId,
    pub idempotency_key: String,
    pub fleet_id: FleetId,
    pub tenant_id: TenantId,
    pub region_id: RegionId,
    pub cluster_id: Option<ClusterId>,
    pub source: FinOpsCostSource,
    pub workload_kind: FinOpsWorkloadKind,
    pub provider_profile: Option<String>,
    pub model_family: Option<String>,
    pub incident_id: Option<IncidentId>,
    pub pack_id: Option<String>,
    pub workflow_id: Option<String>,
    pub request_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub latency_millis: u64,
    pub error_count: u64,
    pub quantity_millis: u64,
    pub cost_micros: u64,
    pub occurred_at: DateTime<Utc>,
    pub recorded_at: DateTime<Utc>,
}

/// Scope supported by a soft/hard cost budget.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinOpsBudgetScopeKind {
    Tenant,
    Provider,
    Model,
    Region,
    Cluster,
    Incident,
    DiagnosticPack,
    Workflow,
}

/// Calendar window used for usage aggregation and projection.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinOpsBudgetPeriod {
    Hourly,
    Daily,
    Monthly,
}

/// Versioned budget with an advisory soft limit and an enforceable hard limit.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinOpsBudget {
    pub id: FinOpsBudgetId,
    pub tenant_id: TenantId,
    pub scope_kind: FinOpsBudgetScopeKind,
    pub scope_key: String,
    pub version: u64,
    pub period: FinOpsBudgetPeriod,
    pub soft_limit_micros: u64,
    pub hard_limit_micros: u64,
    pub owner: String,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

/// Work classification used to preserve safety and recovery under cost pressure.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinOpsWorkClass {
    SafetyCheck,
    Audit,
    Verification,
    Rollback,
    ActiveIncident,
    Interactive,
    Background,
}

impl FinOpsWorkClass {
    #[must_use]
    pub const fn is_cost_protected(self) -> bool {
        matches!(
            self,
            Self::SafetyCheck | Self::Audit | Self::Verification | Self::Rollback
        )
    }
}

/// Allowed degradation when a budget is under pressure.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinOpsDegradation {
    None,
    PreferLowerCostModel,
    ReduceSampling,
    DeferLowPriority,
    DenyLowPriority,
}

/// Immutable outcome of one budget evaluation.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinOpsBudgetDecision {
    pub id: FinOpsDecisionId,
    pub tenant_id: TenantId,
    pub cluster_id: Option<ClusterId>,
    pub budget_id: FinOpsBudgetId,
    pub work_class: FinOpsWorkClass,
    pub requested_cost_micros: u64,
    pub observed_cost_micros: u64,
    pub projected_cost_micros: u64,
    pub soft_limit_micros: u64,
    pub hard_limit_micros: u64,
    pub allowed: bool,
    pub degradation: FinOpsDegradation,
    pub reason_code: String,
    pub protected_controls: BTreeSet<FinOpsWorkClass>,
    pub evaluated_at: DateTime<Utc>,
}

impl FinOpsBudgetDecision {
    /// Returns the controls that cost policy is never allowed to weaken.
    #[must_use]
    pub fn required_protected_controls() -> BTreeSet<FinOpsWorkClass> {
        BTreeSet::from([
            FinOpsWorkClass::SafetyCheck,
            FinOpsWorkClass::Audit,
            FinOpsWorkClass::Verification,
            FinOpsWorkClass::Rollback,
        ])
    }

    /// Verifies the non-negotiable safety boundary.
    ///
    /// # Errors
    ///
    /// Returns an error when a protected path was denied/degraded or the
    /// decision omitted a required protected control.
    pub fn validate_safety_boundary(&self) -> Result<(), &'static str> {
        if !Self::required_protected_controls().is_subset(&self.protected_controls) {
            return Err("FinOps decision omitted a protected safety control");
        }
        if self.work_class.is_cost_protected() && (!self.allowed || self.degradation != FinOpsDegradation::None) {
            return Err("FinOps decision weakened a protected safety path");
        }
        Ok(())
    }
}

/// Period projection for one budget.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinOpsForecast {
    pub budget_id: FinOpsBudgetId,
    pub period_start: DateTime<Utc>,
    pub period_end: DateTime<Utc>,
    pub observed_cost_micros: u64,
    pub projected_cost_micros: u64,
    pub hard_limit_micros: u64,
    pub sample_count: u64,
    pub coverage_basis_points: u32,
    pub projected_over_budget: bool,
    pub generated_at: DateTime<Utc>,
}

/// Simple bounded deviation from a preceding comparable window.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinOpsAnomaly {
    pub scope_kind: FinOpsBudgetScopeKind,
    pub scope_key: String,
    pub current_cost_micros: u64,
    pub baseline_cost_micros: u64,
    pub change_basis_points: Option<u32>,
    pub reason_code: String,
}

/// Allocation mode is showback unless an organization explicitly confirms
/// its chargeback keys.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinOpsAllocationMode {
    Showback,
    Chargeback,
}

/// Versioned cost-allocation policy.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinOpsAllocationPolicy {
    pub id: FinOpsAllocationPolicyId,
    pub tenant_id: TenantId,
    pub version: u64,
    pub mode: FinOpsAllocationMode,
    pub allocation_keys: BTreeSet<String>,
    pub organization_confirmed: bool,
    pub owner: String,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

/// Cost, quality, SLO and saved-time values for one showback dimension.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinOpsShowbackRow {
    pub dimensions: BTreeMap<String, String>,
    pub request_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub error_count: u64,
    pub average_latency_millis: Option<u64>,
    pub cost_micros: u64,
    pub successful_outcomes: u64,
    pub slo_compliant_outcomes: u64,
    pub estimated_minutes_saved: u64,
}

/// Bounded report that exposes coverage rather than fabricating missing cost.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinOpsReport {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub allocation_mode: FinOpsAllocationMode,
    pub chargeback_enabled: bool,
    pub rows: Vec<FinOpsShowbackRow>,
    pub total_cost_micros: u64,
    pub ledger_entries: u64,
    pub entries_missing_cost: u64,
    pub cost_coverage_basis_points: Option<u32>,
    pub forecasts: Vec<FinOpsForecast>,
    pub anomalies: Vec<FinOpsAnomaly>,
    pub warnings: Vec<String>,
    pub generated_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protected_work_cannot_be_degraded_by_cost_policy() {
        let mut decision = FinOpsBudgetDecision {
            id: FinOpsDecisionId::new(),
            tenant_id: TenantId::new(),
            cluster_id: None,
            budget_id: FinOpsBudgetId::new(),
            work_class: FinOpsWorkClass::Rollback,
            requested_cost_micros: 10,
            observed_cost_micros: 100,
            projected_cost_micros: 110,
            soft_limit_micros: 50,
            hard_limit_micros: 75,
            allowed: true,
            degradation: FinOpsDegradation::None,
            reason_code: "protected_capacity".to_owned(),
            protected_controls: FinOpsBudgetDecision::required_protected_controls(),
            evaluated_at: Utc::now(),
        };
        assert!(decision.validate_safety_boundary().is_ok());

        decision.allowed = false;
        decision.degradation = FinOpsDegradation::DenyLowPriority;
        assert!(decision.validate_safety_boundary().is_err());
    }

    #[test]
    fn background_work_may_degrade_without_removing_protected_controls() {
        let decision = FinOpsBudgetDecision {
            id: FinOpsDecisionId::new(),
            tenant_id: TenantId::new(),
            cluster_id: None,
            budget_id: FinOpsBudgetId::new(),
            work_class: FinOpsWorkClass::Background,
            requested_cost_micros: 10,
            observed_cost_micros: 100,
            projected_cost_micros: 110,
            soft_limit_micros: 50,
            hard_limit_micros: 75,
            allowed: false,
            degradation: FinOpsDegradation::DenyLowPriority,
            reason_code: "hard_budget_exceeded".to_owned(),
            protected_controls: FinOpsBudgetDecision::required_protected_controls(),
            evaluated_at: Utc::now(),
        };
        assert!(decision.validate_safety_boundary().is_ok());
    }
}
