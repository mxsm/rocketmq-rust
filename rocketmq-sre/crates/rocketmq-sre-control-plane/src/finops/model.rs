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

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::FinOpsAllocationMode;
use rocketmq_sre_contracts::FinOpsAllocationPolicy;
use rocketmq_sre_contracts::FinOpsBudget;
use rocketmq_sre_contracts::FinOpsBudgetDecision;
use rocketmq_sre_contracts::FinOpsBudgetId;
use rocketmq_sre_contracts::FinOpsBudgetPeriod;
use rocketmq_sre_contracts::FinOpsBudgetScopeKind;
use rocketmq_sre_contracts::FinOpsCostEntry;
use rocketmq_sre_contracts::FinOpsCostSource;
use rocketmq_sre_contracts::FinOpsWorkClass;
use rocketmq_sre_contracts::FinOpsWorkloadKind;
use rocketmq_sre_contracts::FleetId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::RegionId;
use serde::Deserialize;
use serde::Serialize;

pub(crate) const FINOPS_API_SCHEMA_VERSION: &str = "rocketmq-sre.finops-api.v1";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecordFinOpsCostRequest {
    pub(crate) idempotency_key: String,
    pub(crate) fleet_id: FleetId,
    pub(crate) region_id: RegionId,
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) source: FinOpsCostSource,
    pub(crate) workload_kind: FinOpsWorkloadKind,
    pub(crate) provider_profile: Option<String>,
    pub(crate) model_family: Option<String>,
    pub(crate) incident_id: Option<IncidentId>,
    pub(crate) pack_id: Option<String>,
    pub(crate) workflow_id: Option<String>,
    #[serde(default)]
    pub(crate) request_count: u64,
    #[serde(default)]
    pub(crate) input_tokens: u64,
    #[serde(default)]
    pub(crate) output_tokens: u64,
    #[serde(default)]
    pub(crate) latency_millis: u64,
    #[serde(default)]
    pub(crate) error_count: u64,
    #[serde(default)]
    pub(crate) quantity_millis: u64,
    #[serde(default)]
    pub(crate) cost_micros: u64,
    pub(crate) occurred_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FinOpsLedgerQuery {
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) source: Option<FinOpsCostSource>,
    pub(crate) from: Option<DateTime<Utc>>,
    pub(crate) to: Option<DateTime<Utc>>,
    #[serde(default = "default_limit")]
    pub(crate) limit: u16,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct FinOpsLedgerPage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<FinOpsCostEntry>,
    pub(crate) truncated: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateFinOpsBudgetRequest {
    pub(crate) scope_kind: FinOpsBudgetScopeKind,
    pub(crate) scope_key: String,
    pub(crate) period: FinOpsBudgetPeriod,
    pub(crate) soft_limit_micros: u64,
    pub(crate) hard_limit_micros: u64,
    pub(crate) owner: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FinOpsBudgetQuery {
    pub(crate) scope_kind: Option<FinOpsBudgetScopeKind>,
    pub(crate) active: Option<bool>,
    #[serde(default = "default_limit")]
    pub(crate) limit: u16,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct FinOpsBudgetPage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<FinOpsBudget>,
    pub(crate) truncated: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EvaluateFinOpsBudgetRequest {
    pub(crate) budget_id: FinOpsBudgetId,
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) work_class: FinOpsWorkClass,
    pub(crate) requested_cost_micros: u64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct FinOpsBudgetDecisionView {
    pub(crate) schema_version: &'static str,
    pub(crate) decision: FinOpsBudgetDecision,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateFinOpsAllocationPolicyRequest {
    pub(crate) mode: FinOpsAllocationMode,
    pub(crate) allocation_keys: BTreeSet<String>,
    pub(crate) organization_confirmed: bool,
    pub(crate) owner: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct FinOpsAllocationPolicyView {
    pub(crate) schema_version: &'static str,
    pub(crate) policy: FinOpsAllocationPolicy,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FinOpsReportQuery {
    pub(crate) from: DateTime<Utc>,
    pub(crate) to: DateTime<Utc>,
    pub(crate) cluster_id: Option<ClusterId>,
    #[serde(default = "default_report_limit")]
    pub(crate) limit: u16,
}

pub(crate) const fn bounded_limit(limit: u16) -> i64 {
    i64::from(limit.clamp(1, 200))
}

pub(crate) const fn bounded_report_limit(limit: u16) -> i64 {
    i64::from(limit.clamp(1, 500))
}

const fn default_limit() -> u16 {
    100
}

const fn default_report_limit() -> u16 {
    200
}
