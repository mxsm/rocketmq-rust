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

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::AutonomyOutcome;
use rocketmq_sre_contracts::AutonomyOutcomeClass;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::TenantId;
use serde::Deserialize;
use serde::Serialize;

pub(super) const OPERATIONS_SCHEMA_VERSION: &str = "rocketmq-sre.autonomy-operations.v1";

/// Bounded query over the append-only autonomy outcome dataset.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutonomyOutcomeListQuery {
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) action: Option<ExecutionAction>,
    pub(crate) class: Option<AutonomyOutcomeClass>,
    pub(crate) from: Option<DateTime<Utc>>,
    pub(crate) until: Option<DateTime<Utc>>,
    #[serde(default = "default_outcome_limit")]
    pub(crate) limit: u16,
}

/// Tenant- and cluster-scoped outcome page.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct AutonomyOutcomePage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<AutonomyOutcome>,
    pub(crate) truncated: bool,
    pub(crate) observed_at: DateTime<Utc>,
}

/// Supported persisted operating-report periods.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AutonomyReportPeriod {
    #[default]
    Weekly,
    Monthly,
}

impl AutonomyReportPeriod {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Weekly => "weekly",
            Self::Monthly => "monthly",
        }
    }
}

/// Query one normalized week or month. `anchor` may point anywhere inside the
/// requested period; the server computes the exact UTC boundary.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutonomyOperationalReportQuery {
    #[serde(default)]
    pub(crate) period: AutonomyReportPeriod,
    pub(crate) anchor: Option<DateTime<Utc>>,
    pub(crate) cluster_id: Option<ClusterId>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct AutonomyReportWindow {
    pub(crate) period: AutonomyReportPeriod,
    pub(crate) start: DateTime<Utc>,
    pub(crate) end: DateTime<Utc>,
    pub(crate) complete: bool,
}

/// Deterministic candidate and terminal outcome counts.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct AutonomyOutcomeMetrics {
    pub(crate) candidates: u64,
    pub(crate) eligible: u64,
    pub(crate) denied: u64,
    pub(crate) successes: u64,
    pub(crate) execution_failures: u64,
    pub(crate) rollbacks: u64,
    pub(crate) unknown_effects: u64,
    pub(crate) human_handoffs: u64,
}

/// Latency metrics are nullable when the underlying lifecycle timestamp is
/// absent; missing samples are reported separately.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct AutonomyDurationMetrics {
    pub(crate) mean_time_to_acknowledge_seconds: Option<f64>,
    pub(crate) mean_time_to_resolve_seconds: Option<f64>,
    pub(crate) average_diagnosis_seconds: Option<f64>,
    pub(crate) average_execution_seconds: Option<f64>,
    pub(crate) average_recovery_seconds: Option<f64>,
    pub(crate) acknowledged_incidents: u64,
    pub(crate) resolved_incidents: u64,
    pub(crate) diagnosed_incidents: u64,
    pub(crate) completed_executions: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct AutonomyQualityMetrics {
    pub(crate) raw_alert_occurrences: u64,
    pub(crate) correlated_alerts: u64,
    pub(crate) noise_reduction_basis_points: Option<u32>,
    pub(crate) routed_incidents: u64,
    pub(crate) owner_routing_hit_basis_points: Option<u32>,
    pub(crate) terminal_incidents: u64,
    pub(crate) recurrent_incidents: u64,
    pub(crate) recurrence_basis_points: Option<u32>,
    pub(crate) overdue_action_items: u64,
    pub(crate) post_close_recurrences: u64,
    pub(crate) health_score_delta: Option<f64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct AutonomyFeedbackMetrics {
    pub(crate) total: u64,
    pub(crate) adopted: u64,
    pub(crate) modified: u64,
    pub(crate) rejected: u64,
    pub(crate) adoption_basis_points: Option<u32>,
    pub(crate) modification_basis_points: Option<u32>,
    pub(crate) rejection_basis_points: Option<u32>,
}

/// Conservative saved-time estimate with the assumptions exposed to users.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct AutomationSavingsMetrics {
    pub(crate) successful_no_side_effect_runs: u64,
    pub(crate) successful_preventive_runs: u64,
    pub(crate) estimated_minutes_saved: u64,
    pub(crate) estimate_method: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct ModelUsageMetrics {
    pub(crate) calls: u64,
    pub(crate) input_tokens: u64,
    pub(crate) output_tokens: u64,
    pub(crate) cost_micros: u64,
    pub(crate) calls_missing_tokens: u64,
    pub(crate) calls_missing_cost: u64,
    pub(crate) failed_calls: u64,
    pub(crate) fallback_calls: u64,
    pub(crate) usage_coverage_basis_points: Option<u32>,
    pub(crate) cost_coverage_basis_points: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ActionOutcomeBreakdown {
    pub(crate) cluster_id: ClusterId,
    pub(crate) action_id: String,
    pub(crate) action_version: String,
    pub(crate) outcomes: AutonomyOutcomeMetrics,
    pub(crate) average_execution_seconds: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ModelCostBreakdown {
    pub(crate) provider_family: String,
    pub(crate) model_family: String,
    pub(crate) model_revision: String,
    pub(crate) actual_profile_id: uuid::Uuid,
    pub(crate) usage: ModelUsageMetrics,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct IncidentModelCost {
    pub(crate) incident_id: IncidentId,
    pub(crate) usage: ModelUsageMetrics,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct VersionEffectComparison {
    pub(crate) dimension: String,
    pub(crate) version: String,
    pub(crate) samples: u64,
    pub(crate) successes: u64,
    pub(crate) success_basis_points: Option<u32>,
    pub(crate) cost_micros: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct CostBudgetAlert {
    pub(crate) scope_kind: String,
    pub(crate) scope_id: String,
    pub(crate) observed_cost_micros: u64,
    pub(crate) budget_micros: u64,
    pub(crate) reason_code: String,
    pub(crate) recommended_degradation: String,
    pub(crate) automatic_provider_mutation: bool,
}

/// Optimization output is a review candidate only. It cannot publish a
/// policy, prompt, pack, action descriptor, or autonomy transition.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct OptimizationCandidate {
    pub(crate) id: String,
    pub(crate) category: String,
    pub(crate) scope: String,
    pub(crate) reason_code: String,
    pub(crate) evidence_summary: String,
    pub(crate) review_status: String,
    pub(crate) requires_human_review: bool,
    pub(crate) publication_allowed: bool,
}

/// Immutable report snapshot persisted only for completed UTC periods.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct AutonomyOperationalReport {
    pub(crate) schema_version: String,
    pub(crate) tenant_id: TenantId,
    pub(crate) cluster_ids: Vec<ClusterId>,
    pub(crate) window: AutonomyReportWindow,
    pub(crate) outcomes: AutonomyOutcomeMetrics,
    pub(crate) durations: AutonomyDurationMetrics,
    pub(crate) quality: AutonomyQualityMetrics,
    pub(crate) feedback: AutonomyFeedbackMetrics,
    pub(crate) savings: AutomationSavingsMetrics,
    pub(crate) model_usage: ModelUsageMetrics,
    pub(crate) action_breakdown: Vec<ActionOutcomeBreakdown>,
    pub(crate) model_breakdown: Vec<ModelCostBreakdown>,
    pub(crate) incident_costs: Vec<IncidentModelCost>,
    pub(crate) version_effects: Vec<VersionEffectComparison>,
    pub(crate) budget_alerts: Vec<CostBudgetAlert>,
    pub(crate) optimization_candidates: Vec<OptimizationCandidate>,
    pub(crate) warnings: Vec<String>,
    pub(crate) generated_at: DateTime<Utc>,
}

pub(super) const fn bounded_outcome_limit(limit: u16) -> u16 {
    if limit == 0 {
        1
    } else if limit > 200 {
        200
    } else {
        limit
    }
}

const fn default_outcome_limit() -> u16 {
    100
}

#[cfg(test)]
mod tests {
    use super::bounded_outcome_limit;

    #[test]
    fn outcome_pages_are_always_bounded() {
        assert_eq!(bounded_outcome_limit(0), 1);
        assert_eq!(bounded_outcome_limit(42), 42);
        assert_eq!(bounded_outcome_limit(u16::MAX), 200);
    }
}
