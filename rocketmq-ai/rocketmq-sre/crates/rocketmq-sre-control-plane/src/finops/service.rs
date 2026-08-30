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
use chrono::Datelike;
use chrono::Duration;
use chrono::TimeZone;
use chrono::Timelike;
use chrono::Utc;
use rocketmq_sre_contracts::FINOPS_SCHEMA_VERSION;
use rocketmq_sre_contracts::FinOpsAllocationMode;
use rocketmq_sre_contracts::FinOpsAllocationPolicy;
use rocketmq_sre_contracts::FinOpsAllocationPolicyId;
use rocketmq_sre_contracts::FinOpsAnomaly;
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
use rocketmq_sre_contracts::FinOpsForecast;
use rocketmq_sre_contracts::FinOpsReport;
use rocketmq_sre_contracts::FinOpsWorkClass;
use rocketmq_sre_contracts::TenantId;

use super::model::CreateFinOpsAllocationPolicyRequest;
use super::model::CreateFinOpsBudgetRequest;
use super::model::EvaluateFinOpsBudgetRequest;
use super::model::FINOPS_API_SCHEMA_VERSION;
use super::model::FinOpsAllocationPolicyView;
use super::model::FinOpsBudgetDecisionView;
use super::model::FinOpsBudgetPage;
use super::model::FinOpsBudgetQuery;
use super::model::FinOpsLedgerPage;
use super::model::FinOpsLedgerQuery;
use super::model::FinOpsReportQuery;
use super::model::RecordFinOpsCostRequest;
use super::repository::FinOpsRepository;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

const MAX_REPORT_WINDOW_DAYS: i64 = 366;
const MAX_CLOCK_SKEW_MINUTES: i64 = 5;

#[derive(Clone)]
pub(crate) struct FinOpsService {
    repository: FinOpsRepository,
}

impl FinOpsService {
    pub(crate) fn new(repository: PostgresRepository) -> Self {
        Self {
            repository: FinOpsRepository::new(repository.pool),
        }
    }

    pub(crate) async fn record_cost(
        &self,
        auth: &AuthContext,
        request: &RecordFinOpsCostRequest,
    ) -> Result<FinOpsCostEntry, ControlPlaneError> {
        require_finops_writer(auth)?;
        require_cluster(auth, request.cluster_id)?;
        validate_cost_request(request)?;
        let now = Utc::now();
        let entry = FinOpsCostEntry {
            id: FinOpsCostEntryId::new(),
            idempotency_key: request.idempotency_key.trim().to_owned(),
            fleet_id: request.fleet_id,
            tenant_id: auth.tenant_id,
            region_id: request.region_id,
            cluster_id: request.cluster_id,
            source: request.source,
            workload_kind: request.workload_kind,
            provider_profile: request.provider_profile.clone(),
            model_family: request.model_family.clone(),
            incident_id: request.incident_id,
            pack_id: request.pack_id.clone(),
            workflow_id: request.workflow_id.clone(),
            request_count: request.request_count,
            input_tokens: request.input_tokens,
            output_tokens: request.output_tokens,
            latency_millis: request.latency_millis,
            error_count: request.error_count,
            quantity_millis: request.quantity_millis,
            cost_micros: request.cost_micros,
            occurred_at: request.occurred_at,
            recorded_at: now,
        };
        if !self.repository.scope_exists(&entry).await? {
            return Err(ControlPlaneError::forbidden(
                "finops_scope_mismatch",
                "FinOps Fleet, tenant, region, and cluster dimensions are inconsistent",
            ));
        }
        self.repository.record_cost(&entry).await
    }

    pub(crate) async fn ledger(
        &self,
        auth: &AuthContext,
        query: &FinOpsLedgerQuery,
    ) -> Result<FinOpsLedgerPage, ControlPlaneError> {
        require_finops_read(auth)?;
        require_cluster(auth, query.cluster_id)?;
        validate_window(query.from, query.to)?;
        let (items, truncated) = self.repository.list_costs(auth.tenant_id, query).await?;
        Ok(FinOpsLedgerPage {
            schema_version: FINOPS_API_SCHEMA_VERSION,
            items,
            truncated,
        })
    }

    pub(crate) async fn create_budget(
        &self,
        auth: &AuthContext,
        request: &CreateFinOpsBudgetRequest,
    ) -> Result<FinOpsBudget, ControlPlaneError> {
        require_finops_writer(auth)?;
        validate_budget_request(auth, request)?;
        let scope_key = request.scope_key.trim().to_owned();
        let version = self
            .repository
            .next_budget_version(auth.tenant_id, request.scope_kind, &scope_key)
            .await?;
        self.repository
            .create_budget(&FinOpsBudget {
                id: FinOpsBudgetId::new(),
                tenant_id: auth.tenant_id,
                scope_kind: request.scope_kind,
                scope_key,
                version,
                period: request.period,
                soft_limit_micros: request.soft_limit_micros,
                hard_limit_micros: request.hard_limit_micros,
                owner: request.owner.trim().to_owned(),
                active: true,
                created_at: Utc::now(),
            })
            .await
    }

    pub(crate) async fn budgets(
        &self,
        auth: &AuthContext,
        query: &FinOpsBudgetQuery,
    ) -> Result<FinOpsBudgetPage, ControlPlaneError> {
        require_finops_read(auth)?;
        let (items, truncated) = self.repository.budgets(auth.tenant_id, query).await?;
        Ok(FinOpsBudgetPage {
            schema_version: FINOPS_API_SCHEMA_VERSION,
            items,
            truncated,
        })
    }

    pub(crate) async fn evaluate_budget(
        &self,
        auth: &AuthContext,
        request: &EvaluateFinOpsBudgetRequest,
    ) -> Result<FinOpsBudgetDecisionView, ControlPlaneError> {
        require_finops_read(auth)?;
        require_cluster(auth, request.cluster_id)?;
        let budget = self.repository.budget(auth.tenant_id, request.budget_id).await?;
        if !budget.active {
            return Err(ControlPlaneError::conflict_code(
                "finops_budget_inactive",
                "inactive FinOps budgets cannot authorize new work",
            ));
        }
        validate_budget_cluster_scope(auth, &budget, request.cluster_id)?;
        let now = Utc::now();
        let (from, to) = period_window(budget.period, now)?;
        let (observed, _) = self.repository.budget_cost(&budget, from, to).await?;
        let projected = observed.saturating_add(request.requested_cost_micros);
        let (allowed, degradation, reason_code) = budget_outcome(&budget, request.work_class, projected);
        let decision = FinOpsBudgetDecision {
            id: FinOpsDecisionId::new(),
            tenant_id: auth.tenant_id,
            cluster_id: request.cluster_id,
            budget_id: budget.id,
            work_class: request.work_class,
            requested_cost_micros: request.requested_cost_micros,
            observed_cost_micros: observed,
            projected_cost_micros: projected,
            soft_limit_micros: budget.soft_limit_micros,
            hard_limit_micros: budget.hard_limit_micros,
            allowed,
            degradation,
            reason_code: reason_code.to_owned(),
            protected_controls: FinOpsBudgetDecision::required_protected_controls(),
            evaluated_at: now,
        };
        decision
            .validate_safety_boundary()
            .map_err(|detail| ControlPlaneError::configuration(format!("FinOps safety invariant failed: {detail}")))?;
        Ok(FinOpsBudgetDecisionView {
            schema_version: FINOPS_API_SCHEMA_VERSION,
            decision: self.repository.record_decision(&decision).await?,
        })
    }

    pub(crate) async fn create_allocation_policy(
        &self,
        auth: &AuthContext,
        request: &CreateFinOpsAllocationPolicyRequest,
    ) -> Result<FinOpsAllocationPolicyView, ControlPlaneError> {
        require_finops_writer(auth)?;
        validate_allocation_request(auth, request)?;
        let version = self.repository.next_allocation_version(auth.tenant_id).await?;
        let policy = self
            .repository
            .create_allocation_policy(&FinOpsAllocationPolicy {
                id: FinOpsAllocationPolicyId::new(),
                tenant_id: auth.tenant_id,
                version,
                mode: request.mode,
                allocation_keys: request.allocation_keys.clone(),
                organization_confirmed: request.organization_confirmed,
                owner: request.owner.trim().to_owned(),
                active: true,
                created_at: Utc::now(),
            })
            .await?;
        Ok(FinOpsAllocationPolicyView {
            schema_version: FINOPS_API_SCHEMA_VERSION,
            policy,
        })
    }

    pub(crate) async fn allocation_policy(
        &self,
        auth: &AuthContext,
    ) -> Result<FinOpsAllocationPolicyView, ControlPlaneError> {
        require_finops_read(auth)?;
        let policy = self
            .repository
            .allocation_policy(auth.tenant_id)
            .await?
            .unwrap_or_else(|| default_showback_policy(auth.tenant_id));
        Ok(FinOpsAllocationPolicyView {
            schema_version: FINOPS_API_SCHEMA_VERSION,
            policy,
        })
    }

    pub(crate) async fn report(
        &self,
        auth: &AuthContext,
        query: &FinOpsReportQuery,
    ) -> Result<FinOpsReport, ControlPlaneError> {
        require_finops_read(auth)?;
        require_cluster(auth, query.cluster_id)?;
        validate_report_window(query)?;
        let data = self.repository.report_data(auth.tenant_id, query).await?;
        let allocation = self
            .repository
            .allocation_policy(auth.tenant_id)
            .await?
            .unwrap_or_else(|| default_showback_policy(auth.tenant_id));
        let budgets = self
            .repository
            .budgets(
                auth.tenant_id,
                &FinOpsBudgetQuery {
                    scope_kind: None,
                    active: Some(true),
                    limit: 200,
                },
            )
            .await?
            .0;
        let now = Utc::now();
        let mut forecasts = Vec::with_capacity(budgets.len());
        for budget in budgets {
            forecasts.push(self.forecast(&budget, now).await?);
        }
        let duration = query.to - query.from;
        let baseline_from = query.from - duration;
        let baseline_cost = self
            .repository
            .window_cost(auth.tenant_id, query.cluster_id, baseline_from, query.from)
            .await?;
        let exact_total = self
            .repository
            .window_cost(auth.tenant_id, query.cluster_id, query.from, query.to)
            .await?;
        let anomalies = cost_anomalies(auth.tenant_id, exact_total, baseline_cost);
        let mut warnings = Vec::new();
        if data.entries_missing_cost > 0 {
            warnings.push(format!(
                "cost_coverage_partial:{} model invocation(s) did not expose cost",
                data.entries_missing_cost
            ));
        }
        if data.truncated {
            warnings.push("showback_rows_truncated".to_owned());
        }
        warnings.push(
            "slo_outcome_attribution_not_available:cost and successful outcomes are shown without fabricated SLO \
             attribution"
                .to_owned(),
        );
        if data.successful_outcomes == 0 && data.estimated_minutes_saved == 0 {
            warnings.push("outcome_value_samples_missing".to_owned());
        }
        Ok(FinOpsReport {
            schema_version: FINOPS_SCHEMA_VERSION.to_owned(),
            tenant_id: auth.tenant_id,
            from: query.from,
            to: query.to,
            allocation_mode: allocation.mode,
            chargeback_enabled: allocation.mode == FinOpsAllocationMode::Chargeback
                && allocation.organization_confirmed,
            rows: data.rows,
            total_cost_micros: exact_total,
            ledger_entries: data.entries,
            entries_missing_cost: data.entries_missing_cost,
            cost_coverage_basis_points: coverage_basis_points(
                data.entries.saturating_sub(data.entries_missing_cost),
                data.entries,
            ),
            forecasts,
            anomalies,
            warnings,
            generated_at: now,
        })
    }

    async fn forecast(&self, budget: &FinOpsBudget, now: DateTime<Utc>) -> Result<FinOpsForecast, ControlPlaneError> {
        let (from, to) = period_window(budget.period, now)?;
        let (observed, samples) = self.repository.budget_cost(budget, from, to).await?;
        let elapsed_millis = (now - from).num_milliseconds().max(1) as u128;
        let period_millis = (to - from).num_milliseconds().max(1) as u128;
        let projected =
            (u128::from(observed).saturating_mul(period_millis) / elapsed_millis).min(u128::from(u64::MAX)) as u64;
        Ok(FinOpsForecast {
            budget_id: budget.id,
            period_start: from,
            period_end: to,
            observed_cost_micros: observed,
            projected_cost_micros: projected,
            hard_limit_micros: budget.hard_limit_micros,
            sample_count: samples,
            coverage_basis_points: if samples == 0 { 0 } else { 10_000 },
            projected_over_budget: projected > budget.hard_limit_micros,
            generated_at: now,
        })
    }
}

fn budget_outcome(
    budget: &FinOpsBudget,
    work_class: FinOpsWorkClass,
    projected: u64,
) -> (bool, FinOpsDegradation, &'static str) {
    if work_class.is_cost_protected() {
        return (true, FinOpsDegradation::None, "cost_protected_capacity");
    }
    if projected <= budget.soft_limit_micros {
        return (true, FinOpsDegradation::None, "within_budget");
    }
    if projected <= budget.hard_limit_micros {
        return match work_class {
            FinOpsWorkClass::Background => (true, FinOpsDegradation::ReduceSampling, "soft_budget_pressure"),
            _ => (true, FinOpsDegradation::PreferLowerCostModel, "soft_budget_pressure"),
        };
    }
    match work_class {
        FinOpsWorkClass::ActiveIncident | FinOpsWorkClass::Interactive => (
            true,
            FinOpsDegradation::PreferLowerCostModel,
            "hard_budget_interactive_degradation",
        ),
        FinOpsWorkClass::Background => (false, FinOpsDegradation::DenyLowPriority, "hard_budget_exceeded"),
        FinOpsWorkClass::SafetyCheck
        | FinOpsWorkClass::Audit
        | FinOpsWorkClass::Verification
        | FinOpsWorkClass::Rollback => (true, FinOpsDegradation::None, "cost_protected_capacity"),
    }
}

fn period_window(
    period: FinOpsBudgetPeriod,
    now: DateTime<Utc>,
) -> Result<(DateTime<Utc>, DateTime<Utc>), ControlPlaneError> {
    let start = match period {
        FinOpsBudgetPeriod::Hourly => now
            .with_minute(0)
            .and_then(|value| value.with_second(0))
            .and_then(|value| value.with_nanosecond(0)),
        FinOpsBudgetPeriod::Daily => Utc
            .with_ymd_and_hms(now.year(), now.month(), now.day(), 0, 0, 0)
            .single(),
        FinOpsBudgetPeriod::Monthly => Utc.with_ymd_and_hms(now.year(), now.month(), 1, 0, 0, 0).single(),
    }
    .ok_or_else(|| ControlPlaneError::configuration("FinOps period boundary is invalid"))?;
    let end = match period {
        FinOpsBudgetPeriod::Hourly => start + Duration::hours(1),
        FinOpsBudgetPeriod::Daily => start + Duration::days(1),
        FinOpsBudgetPeriod::Monthly => {
            let (year, month) = if start.month() == 12 {
                (start.year() + 1, 1)
            } else {
                (start.year(), start.month() + 1)
            };
            Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0)
                .single()
                .ok_or_else(|| ControlPlaneError::configuration("FinOps month boundary is invalid"))?
        }
    };
    Ok((start, end))
}

fn validate_cost_request(request: &RecordFinOpsCostRequest) -> Result<(), ControlPlaneError> {
    validate_text("FinOps idempotency key", &request.idempotency_key, 256)?;
    if request.source == FinOpsCostSource::ModelInvocation {
        return Err(ControlPlaneError::validation(
            "finops_model_cost_is_derived",
            "model invocation cost is derived from the canonical model invocation ledger",
        ));
    }
    if request.provider_profile.is_some() || request.model_family.is_some() {
        return Err(ControlPlaneError::validation(
            "invalid_finops_cost",
            "infrastructure cost entries cannot declare model provider dimensions",
        ));
    }
    if request.error_count > request.request_count && request.request_count != 0 {
        return Err(ControlPlaneError::validation(
            "invalid_finops_cost",
            "FinOps error count cannot exceed request count",
        ));
    }
    if request.occurred_at > Utc::now() + Duration::minutes(MAX_CLOCK_SKEW_MINUTES) {
        return Err(ControlPlaneError::validation(
            "invalid_finops_cost",
            "FinOps occurrence time is too far in the future",
        ));
    }
    for value in [&request.pack_id, &request.workflow_id].into_iter().flatten() {
        validate_text("FinOps workload dimension", value, 256)?;
    }
    Ok(())
}

fn validate_budget_request(auth: &AuthContext, request: &CreateFinOpsBudgetRequest) -> Result<(), ControlPlaneError> {
    validate_text("FinOps scope key", &request.scope_key, 256)?;
    validate_text("FinOps budget owner", &request.owner, 256)?;
    if request.owner.trim() != auth.subject {
        return Err(ControlPlaneError::forbidden(
            "finops_owner_mismatch",
            "the authenticated operator must own a newly created FinOps budget",
        ));
    }
    if request.hard_limit_micros == 0 || request.soft_limit_micros > request.hard_limit_micros {
        return Err(ControlPlaneError::validation(
            "invalid_finops_budget",
            "FinOps budget requires 0 <= soft limit <= non-zero hard limit",
        ));
    }
    if request.scope_kind == FinOpsBudgetScopeKind::Tenant && request.scope_key.trim() != auth.tenant_id.to_string() {
        return Err(ControlPlaneError::forbidden(
            "tenant_mismatch",
            "tenant budget scope must match the authenticated tenant",
        ));
    }
    if request.scope_kind == FinOpsBudgetScopeKind::Cluster {
        let cluster_id = request.scope_key.trim().parse().map_err(|_| {
            ControlPlaneError::validation("invalid_finops_budget", "cluster budget scope must be a UUID")
        })?;
        require_cluster(auth, Some(cluster_id))?;
    }
    Ok(())
}

fn validate_budget_cluster_scope(
    auth: &AuthContext,
    budget: &FinOpsBudget,
    requested_cluster: Option<rocketmq_sre_contracts::ClusterId>,
) -> Result<(), ControlPlaneError> {
    if budget.scope_kind != FinOpsBudgetScopeKind::Cluster {
        return Ok(());
    }
    let budget_cluster = budget.scope_key.parse().map_err(|_| {
        ControlPlaneError::validation(
            "invalid_persisted_finops_state",
            "persisted cluster budget scope is not a UUID",
        )
    })?;
    require_cluster(auth, Some(budget_cluster))?;
    if requested_cluster.is_some_and(|cluster_id| cluster_id != budget_cluster) {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "FinOps budget does not apply to the requested cluster",
        ));
    }
    Ok(())
}

fn validate_allocation_request(
    auth: &AuthContext,
    request: &CreateFinOpsAllocationPolicyRequest,
) -> Result<(), ControlPlaneError> {
    validate_text("FinOps allocation owner", &request.owner, 256)?;
    if request.owner.trim() != auth.subject {
        return Err(ControlPlaneError::forbidden(
            "finops_owner_mismatch",
            "the authenticated operator must own the allocation policy",
        ));
    }
    let supported = BTreeSet::from([
        "tenant",
        "region",
        "cluster",
        "environment",
        "owner",
        "provider",
        "model",
        "incident",
        "diagnostic_pack",
        "workflow",
    ]);
    if request.allocation_keys.len() > 10
        || !request
            .allocation_keys
            .iter()
            .all(|key| supported.contains(key.as_str()))
    {
        return Err(ControlPlaneError::validation(
            "invalid_finops_allocation",
            "FinOps allocation keys contain unsupported or excessive dimensions",
        ));
    }
    if request.mode == FinOpsAllocationMode::Chargeback
        && (!request.organization_confirmed || request.allocation_keys.is_empty())
    {
        return Err(ControlPlaneError::validation(
            "chargeback_confirmation_required",
            "chargeback requires organization-confirmed allocation keys",
        ));
    }
    Ok(())
}

fn validate_report_window(query: &FinOpsReportQuery) -> Result<(), ControlPlaneError> {
    validate_window(Some(query.from), Some(query.to))?;
    if query.to - query.from > Duration::days(MAX_REPORT_WINDOW_DAYS) {
        return Err(ControlPlaneError::validation(
            "invalid_finops_window",
            "FinOps report window exceeds 366 days",
        ));
    }
    Ok(())
}

fn validate_window(from: Option<DateTime<Utc>>, to: Option<DateTime<Utc>>) -> Result<(), ControlPlaneError> {
    if from.zip(to).is_some_and(|(from, to)| from >= to) {
        Err(ControlPlaneError::validation(
            "invalid_finops_window",
            "FinOps window end must be after its start",
        ))
    } else {
        Ok(())
    }
}

fn default_showback_policy(tenant_id: TenantId) -> FinOpsAllocationPolicy {
    FinOpsAllocationPolicy {
        id: FinOpsAllocationPolicyId::from_uuid(uuid::Uuid::nil()),
        tenant_id,
        version: 0,
        mode: FinOpsAllocationMode::Showback,
        allocation_keys: BTreeSet::from(["tenant".to_owned()]),
        organization_confirmed: false,
        owner: "system-default".to_owned(),
        active: true,
        created_at: DateTime::<Utc>::UNIX_EPOCH,
    }
}

fn cost_anomalies(tenant_id: TenantId, current: u64, baseline: u64) -> Vec<FinOpsAnomaly> {
    let anomalous = if baseline == 0 {
        current > 0
    } else {
        current > baseline.saturating_add(baseline / 2)
    };
    if !anomalous {
        return Vec::new();
    }
    vec![FinOpsAnomaly {
        scope_kind: FinOpsBudgetScopeKind::Tenant,
        scope_key: tenant_id.to_string(),
        current_cost_micros: current,
        baseline_cost_micros: baseline,
        change_basis_points: (baseline > 0).then(|| {
            (u128::from(current.saturating_sub(baseline)) * 10_000 / u128::from(baseline)).min(u128::from(u32::MAX))
                as u32
        }),
        reason_code: "cost_increase_above_50_percent".to_owned(),
    }]
}

fn coverage_basis_points(covered: u64, total: u64) -> Option<u32> {
    (total > 0).then(|| (u128::from(covered) * 10_000 / u128::from(total)).min(u128::from(10_000_u32)) as u32)
}

fn require_finops_read(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if auth.roles.iter().any(|role| {
        matches!(
            role.as_str(),
            "diagnose" | "rocketmq:diagnose" | "operator" | "approver" | "model-governance" | "finops"
        )
    }) {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "FinOps reads require diagnose, operator, or finops access",
        ))
    }
}

fn require_finops_writer(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if auth.roles.contains("operator") || auth.roles.contains("finops") {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "FinOps changes require an operator or finops role",
        ))
    }
}

fn require_cluster(
    auth: &AuthContext,
    cluster_id: Option<rocketmq_sre_contracts::ClusterId>,
) -> Result<(), ControlPlaneError> {
    if cluster_id.is_none_or(|cluster_id| auth.clusters.contains(&cluster_id)) {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "the authenticated identity cannot access this FinOps cluster",
        ))
    }
}

fn validate_text(name: &str, value: &str, max: usize) -> Result<(), ControlPlaneError> {
    let value = value.trim();
    if value.is_empty() || value.len() > max {
        return Err(ControlPlaneError::validation(
            "invalid_finops_request",
            format!("{name} must contain between 1 and {max} bytes"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget() -> FinOpsBudget {
        FinOpsBudget {
            id: FinOpsBudgetId::new(),
            tenant_id: TenantId::new(),
            scope_kind: FinOpsBudgetScopeKind::Tenant,
            scope_key: "tenant".to_owned(),
            version: 1,
            period: FinOpsBudgetPeriod::Daily,
            soft_limit_micros: 100,
            hard_limit_micros: 200,
            owner: "finops".to_owned(),
            active: true,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn cost_pressure_never_degrades_protected_work() {
        for work_class in [
            FinOpsWorkClass::SafetyCheck,
            FinOpsWorkClass::Audit,
            FinOpsWorkClass::Verification,
            FinOpsWorkClass::Rollback,
        ] {
            assert_eq!(
                budget_outcome(&budget(), work_class, u64::MAX),
                (true, FinOpsDegradation::None, "cost_protected_capacity")
            );
        }
    }

    #[test]
    fn background_work_is_softly_then_hardly_degraded() {
        assert_eq!(
            budget_outcome(&budget(), FinOpsWorkClass::Background, 150),
            (true, FinOpsDegradation::ReduceSampling, "soft_budget_pressure")
        );
        assert_eq!(
            budget_outcome(&budget(), FinOpsWorkClass::Background, 250),
            (false, FinOpsDegradation::DenyLowPriority, "hard_budget_exceeded")
        );
    }
}
