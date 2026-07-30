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

use std::env;

use chrono::DateTime;
use chrono::Datelike;
use chrono::Duration;
use chrono::NaiveDate;
use chrono::TimeZone;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;

use super::operations::AutonomyOperationalReport;
use super::operations::AutonomyOperationalReportQuery;
use super::operations::AutonomyOutcomeListQuery;
use super::operations::AutonomyOutcomePage;
use super::operations::AutonomyReportPeriod;
use super::operations::AutonomyReportWindow;
use super::operations::CostBudgetAlert;
use super::operations::OPERATIONS_SCHEMA_VERSION;
use super::operations::OptimizationCandidate;
use super::operations::OperationsAnalyticsQuery;
use super::operations::OperationsAnalyticsReport;
use super::operations::bounded_outcome_limit;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

const DEFAULT_WEEKLY_TENANT_COST_BUDGET_MICROS: u64 = 50_000_000;
const DEFAULT_WEEKLY_PROVIDER_COST_BUDGET_MICROS: u64 = 20_000_000;
const DEFAULT_WEEKLY_INCIDENT_COST_BUDGET_MICROS: u64 = 5_000_000;

#[derive(Clone, Copy)]
struct CostBudgets {
    weekly_tenant_micros: u64,
    weekly_provider_micros: u64,
    weekly_incident_micros: u64,
}

#[derive(Clone)]
pub(crate) struct AutonomyOperationsService {
    repository: PostgresRepository,
    budgets: CostBudgets,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct OperationalReportRunSummary {
    pub(crate) tenants: u64,
    pub(crate) attempted: u64,
    pub(crate) inserted: u64,
    pub(crate) failures: u64,
}

impl AutonomyOperationsService {
    pub(crate) fn new(repository: PostgresRepository) -> Result<Self, ControlPlaneError> {
        Ok(Self {
            repository,
            budgets: CostBudgets {
                weekly_tenant_micros: budget_from_env(
                    "ROCKETMQ_SRE_AUTONOMY_WEEKLY_TENANT_COST_BUDGET_MICROS",
                    DEFAULT_WEEKLY_TENANT_COST_BUDGET_MICROS,
                )?,
                weekly_provider_micros: budget_from_env(
                    "ROCKETMQ_SRE_AUTONOMY_WEEKLY_PROVIDER_COST_BUDGET_MICROS",
                    DEFAULT_WEEKLY_PROVIDER_COST_BUDGET_MICROS,
                )?,
                weekly_incident_micros: budget_from_env(
                    "ROCKETMQ_SRE_AUTONOMY_WEEKLY_INCIDENT_COST_BUDGET_MICROS",
                    DEFAULT_WEEKLY_INCIDENT_COST_BUDGET_MICROS,
                )?,
            },
        })
    }

    pub(crate) async fn outcomes(
        &self,
        auth: &AuthContext,
        query: &AutonomyOutcomeListQuery,
    ) -> Result<AutonomyOutcomePage, ControlPlaneError> {
        require_report_reader(auth)?;
        if let Some(cluster_id) = query.cluster_id {
            require_cluster(auth, cluster_id)?;
        }
        if query.from.zip(query.until).is_some_and(|(from, until)| until <= from) {
            return Err(ControlPlaneError::validation(
                "invalid_outcome_window",
                "outcome query end must be later than its start",
            ));
        }
        let limit = bounded_outcome_limit(query.limit);
        let clusters = authorized_clusters(auth, query.cluster_id)?;
        let mut items = self
            .repository
            .autonomy_outcomes(auth.tenant_id, &clusters, query, i64::from(limit).saturating_add(1))
            .await?;
        let truncated = items.len() > usize::from(limit);
        items.truncate(usize::from(limit));
        Ok(AutonomyOutcomePage {
            schema_version: OPERATIONS_SCHEMA_VERSION,
            items,
            truncated,
            observed_at: Utc::now(),
        })
    }

    pub(crate) async fn report(
        &self,
        auth: &AuthContext,
        query: &AutonomyOperationalReportQuery,
    ) -> Result<AutonomyOperationalReport, ControlPlaneError> {
        require_report_reader(auth)?;
        if let Some(cluster_id) = query.cluster_id {
            require_cluster(auth, cluster_id)?;
        }
        let now = Utc::now();
        let window = report_window(query.period, query.anchor.unwrap_or(now), now)?;
        let clusters = authorized_clusters(auth, query.cluster_id)?;
        let report = self
            .repository
            .build_autonomy_operational_report(auth.tenant_id, &clusters, window)
            .await?;
        Ok(self.decorate(report))
    }

    pub(crate) async fn analytics(
        &self,
        auth: &AuthContext,
        query: &OperationsAnalyticsQuery,
    ) -> Result<OperationsAnalyticsReport, ControlPlaneError> {
        require_report_reader(auth)?;
        query.validate()?;
        if let Some(cluster_id) = query.cluster_id {
            require_cluster(auth, cluster_id)?;
        }
        let now = Utc::now();
        let window = report_window(query.period, query.anchor.unwrap_or(now), now)?;
        let clusters = authorized_clusters(auth, query.cluster_id)?;
        self.repository
            .operations_analytics(auth.tenant_id, &clusters, query, window)
            .await
    }

    /// Materializes the previous completed week and month. Repeated scans are
    /// idempotent because report snapshots have a unique period identity.
    pub(crate) async fn run_due_reports(&self) -> OperationalReportRunSummary {
        let now = Utc::now();
        let periods = match previous_completed_windows(now) {
            Ok(periods) => periods,
            Err(error) => {
                tracing::warn!(error = %error, "autonomy report period calculation failed");
                return OperationalReportRunSummary {
                    failures: 1,
                    ..OperationalReportRunSummary::default()
                };
            }
        };
        let scopes = match self.repository.report_tenant_scopes().await {
            Ok(scopes) => scopes,
            Err(error) => {
                tracing::warn!(error = %error, "autonomy report scope scan failed");
                return OperationalReportRunSummary {
                    failures: 1,
                    ..OperationalReportRunSummary::default()
                };
            }
        };
        let mut summary = OperationalReportRunSummary {
            tenants: u64::try_from(scopes.len()).unwrap_or(u64::MAX),
            ..OperationalReportRunSummary::default()
        };
        for (tenant_id, clusters) in scopes {
            for window in &periods {
                summary.attempted = summary.attempted.saturating_add(1);
                let result = async {
                    let report = self
                        .repository
                        .build_autonomy_operational_report(tenant_id, &clusters, window.clone())
                        .await?;
                    self.repository
                        .persist_autonomy_operational_report(&self.decorate(report))
                        .await
                }
                .await;
                match result {
                    Ok(true) => summary.inserted = summary.inserted.saturating_add(1),
                    Ok(false) => {}
                    Err(error) => {
                        summary.failures = summary.failures.saturating_add(1);
                        tracing::warn!(
                            tenant_id = %tenant_id,
                            period = window.period.as_str(),
                            error = %error,
                            "autonomy operating report generation failed"
                        );
                    }
                }
            }
        }
        summary
    }

    fn decorate(&self, mut report: AutonomyOperationalReport) -> AutonomyOperationalReport {
        report.budget_alerts = self.budget_alerts(&report);
        report.optimization_candidates = optimization_candidates(&report);
        if !report.budget_alerts.is_empty() {
            report.warnings.push(format!(
                "cost_budget_exceeded:{} scoped budget alert(s) require operator review",
                report.budget_alerts.len()
            ));
        }
        report
    }

    fn budget_alerts(&self, report: &AutonomyOperationalReport) -> Vec<CostBudgetAlert> {
        let multiplier = match report.window.period {
            AutonomyReportPeriod::Weekly => 1,
            AutonomyReportPeriod::Monthly => 4,
        };
        let tenant_budget = self.budgets.weekly_tenant_micros.saturating_mul(multiplier);
        let provider_budget = self.budgets.weekly_provider_micros.saturating_mul(multiplier);
        let incident_budget = self.budgets.weekly_incident_micros.saturating_mul(multiplier);
        let mut alerts = Vec::new();
        push_budget_alert(
            &mut alerts,
            "tenant",
            report.tenant_id.to_string(),
            report.model_usage.cost_micros,
            tenant_budget,
        );
        for model in &report.model_breakdown {
            push_budget_alert(
                &mut alerts,
                "provider_profile",
                model.actual_profile_id.to_string(),
                model.usage.cost_micros,
                provider_budget,
            );
        }
        for incident in &report.incident_costs {
            push_budget_alert(
                &mut alerts,
                "incident",
                incident.incident_id.to_string(),
                incident.usage.cost_micros,
                incident_budget,
            );
        }
        alerts.truncate(200);
        alerts
    }
}

fn optimization_candidates(report: &AutonomyOperationalReport) -> Vec<OptimizationCandidate> {
    let mut candidates = Vec::new();
    for action in &report.action_breakdown {
        if action.outcomes.rollbacks > 0 || action.outcomes.unknown_effects > 0 {
            candidates.push(candidate(
                "action",
                format!("{}:{}@{}", action.cluster_id, action.action_id, action.action_version),
                "action_outcome_regression",
                format!(
                    "{} rollback(s), {} unknown effect(s)",
                    action.outcomes.rollbacks, action.outcomes.unknown_effects
                ),
            ));
        }
    }
    if report.model_usage.calls_missing_tokens > 0 || report.model_usage.calls_missing_cost > 0 {
        candidates.push(candidate(
            "model",
            report.tenant_id.to_string(),
            "model_usage_coverage_gap",
            format!(
                "{} token-gap call(s), {} cost-gap call(s)",
                report.model_usage.calls_missing_tokens, report.model_usage.calls_missing_cost
            ),
        ));
    }
    if report.feedback.rejected > report.feedback.adopted {
        candidates.push(candidate(
            "prompt_pack",
            report.tenant_id.to_string(),
            "operator_rejection_above_adoption",
            format!(
                "{} rejected versus {} adopted recommendation/plan feedback records",
                report.feedback.rejected, report.feedback.adopted
            ),
        ));
    }
    if report.quality.recurrent_incidents > 0 || report.quality.post_close_recurrences > 0 {
        candidates.push(candidate(
            "diagnostic_pack",
            report.tenant_id.to_string(),
            "root_cause_recurrence_detected",
            format!(
                "{} recurrent incident(s), {} recurrence(s) after close",
                report.quality.recurrent_incidents, report.quality.post_close_recurrences
            ),
        ));
    }
    if report.quality.overdue_action_items > 0 {
        candidates.push(candidate(
            "operations",
            report.tenant_id.to_string(),
            "overdue_action_items",
            format!(
                "{} overdue postmortem action item(s)",
                report.quality.overdue_action_items
            ),
        ));
    }
    candidates.truncate(200);
    candidates
}

fn candidate(
    category: &'static str,
    scope: String,
    reason_code: &'static str,
    evidence_summary: String,
) -> OptimizationCandidate {
    OptimizationCandidate {
        id: format!("{category}:{reason_code}:{scope}"),
        category: category.to_owned(),
        scope,
        reason_code: reason_code.to_owned(),
        evidence_summary,
        review_status: "candidate".to_owned(),
        requires_human_review: true,
        publication_allowed: false,
    }
}

fn push_budget_alert(
    alerts: &mut Vec<CostBudgetAlert>,
    scope_kind: &'static str,
    scope_id: String,
    observed: u64,
    budget: u64,
) {
    if observed <= budget {
        return;
    }
    alerts.push(CostBudgetAlert {
        scope_kind: scope_kind.to_owned(),
        scope_id,
        observed_cost_micros: observed,
        budget_micros: budget,
        reason_code: "model_cost_budget_exceeded".to_owned(),
        recommended_degradation: "prefer a lower-cost healthy profile or rules-only diagnosis".to_owned(),
        automatic_provider_mutation: false,
    });
}

fn authorized_clusters(auth: &AuthContext, requested: Option<ClusterId>) -> Result<Vec<ClusterId>, ControlPlaneError> {
    if let Some(cluster_id) = requested {
        require_cluster(auth, cluster_id)?;
        return Ok(vec![cluster_id]);
    }
    if auth.clusters.is_empty() {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "autonomy report requires at least one authorized cluster",
        ));
    }
    Ok(auth.clusters.iter().copied().collect())
}

fn report_window(
    period: AutonomyReportPeriod,
    anchor: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<AutonomyReportWindow, ControlPlaneError> {
    let start_date = match period {
        AutonomyReportPeriod::Weekly => {
            anchor.date_naive() - Duration::days(i64::from(anchor.weekday().num_days_from_monday()))
        }
        AutonomyReportPeriod::Monthly => NaiveDate::from_ymd_opt(anchor.year(), anchor.month(), 1)
            .ok_or_else(|| ControlPlaneError::validation("invalid_report_period", "month boundary is invalid"))?,
    };
    let start = Utc.from_utc_datetime(
        &start_date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| ControlPlaneError::validation("invalid_report_period", "period boundary is invalid"))?,
    );
    if start > now {
        return Err(ControlPlaneError::validation(
            "future_report_period",
            "autonomy reports cannot be generated for a future period",
        ));
    }
    let end = match period {
        AutonomyReportPeriod::Weekly => start + Duration::days(7),
        AutonomyReportPeriod::Monthly => {
            let (year, month) = if start.month() == 12 {
                (start.year() + 1, 1)
            } else {
                (start.year(), start.month() + 1)
            };
            let next = NaiveDate::from_ymd_opt(year, month, 1)
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .ok_or_else(|| {
                    ControlPlaneError::validation("invalid_report_period", "next month boundary is invalid")
                })?;
            Utc.from_utc_datetime(&next)
        }
    };
    Ok(AutonomyReportWindow {
        period,
        start,
        end,
        complete: end <= now,
    })
}

fn previous_completed_windows(now: DateTime<Utc>) -> Result<[AutonomyReportWindow; 2], ControlPlaneError> {
    let current_week = report_window(AutonomyReportPeriod::Weekly, now, now)?;
    let previous_week_anchor = current_week.start - Duration::seconds(1);
    let previous_week = report_window(AutonomyReportPeriod::Weekly, previous_week_anchor, now)?;
    let current_month = report_window(AutonomyReportPeriod::Monthly, now, now)?;
    let previous_month_anchor = current_month.start - Duration::seconds(1);
    let previous_month = report_window(AutonomyReportPeriod::Monthly, previous_month_anchor, now)?;
    Ok([previous_week, previous_month])
}

fn budget_from_env(name: &'static str, default: u64) -> Result<u64, ControlPlaneError> {
    let Some(value) = env::var_os(name) else {
        return Ok(default);
    };
    let value = value
        .into_string()
        .map_err(|_| ControlPlaneError::configuration(format!("{name} must be valid UTF-8")))?;
    let budget = value
        .parse::<u64>()
        .map_err(|_| ControlPlaneError::configuration(format!("{name} must be a positive integer")))?;
    if budget == 0 {
        return Err(ControlPlaneError::configuration(format!(
            "{name} must be greater than zero"
        )));
    }
    Ok(budget)
}

fn require_report_reader(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if auth.roles.contains("operator") || auth.roles.contains("diagnose") || auth.roles.contains("rocketmq:diagnose") {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "autonomy_report_authority_required",
            "autonomy outcomes and reports require diagnose or operator authority",
        ))
    }
}

fn require_cluster(auth: &AuthContext, cluster_id: ClusterId) -> Result<(), ControlPlaneError> {
    if auth.clusters.contains(&cluster_id) {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "autonomy report cluster is outside the authenticated scope",
        ))
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn report_periods_use_normalized_utc_boundaries() {
        let now = Utc
            .with_ymd_and_hms(2026, 7, 29, 12, 0, 0)
            .single()
            .expect("valid timestamp");
        let weekly = report_window(AutonomyReportPeriod::Weekly, now, now).expect("weekly window");
        assert_eq!(
            weekly.start,
            Utc.with_ymd_and_hms(2026, 7, 27, 0, 0, 0)
                .single()
                .expect("valid timestamp")
        );
        assert!(!weekly.complete);

        let monthly = previous_completed_windows(now).expect("previous windows")[1].clone();
        assert_eq!(
            monthly.start,
            Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
                .single()
                .expect("valid timestamp")
        );
        assert_eq!(
            monthly.end,
            Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0)
                .single()
                .expect("valid timestamp")
        );
        assert!(monthly.complete);
    }

    #[test]
    fn optimization_candidates_never_publish_directly() {
        let candidate = candidate(
            "action",
            "cluster:action@1.0.0".to_owned(),
            "rollback_detected",
            "one rollback".to_owned(),
        );
        assert!(candidate.requires_human_review);
        assert!(!candidate.publication_allowed);
    }
}
