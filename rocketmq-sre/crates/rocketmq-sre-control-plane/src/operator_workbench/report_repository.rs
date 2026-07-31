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
use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::OperationsFinding;
use rocketmq_sre_contracts::OperationsReport;
use rocketmq_sre_contracts::OperationsReportWindow;
use rocketmq_sre_contracts::ShiftHandoffSummary;
use serde_json::Value;
use sqlx::PgPool;
use sqlx::Row;
use uuid::Uuid;

use super::report_support::FETCH_LIMIT;
use super::report_support::MAX_SECTION_ITEMS;
use super::report_support::ReportSection;
use super::report_support::bounded_rows;
use super::report_support::display_optional_number;
use super::report_support::incident_link;
use super::report_support::mean_error;
use super::report_support::normalized_owner;
use super::report_support::scoped_clusters;
use crate::ControlPlaneError;
use crate::auth::AuthContext;

const HANDOFF_SCHEMA: &str = "rocketmq-sre.shift-handoff.v1";
pub(super) const REPORT_SCHEMA: &str = "rocketmq-sre.operations-report.v1";

#[derive(Clone)]
pub(super) struct OperationsReportRepository {
    pool: PgPool,
}

impl OperationsReportRepository {
    pub(super) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub(super) async fn shift_handoff(
        &self,
        auth: &AuthContext,
        cluster_id: Option<ClusterId>,
    ) -> Result<ShiftHandoffSummary, ControlPlaneError> {
        let clusters = scoped_clusters(auth, cluster_id)?;
        let generated_at = Utc::now();
        let window_start = generated_at - Duration::hours(12);
        let incidents = self
            .active_incidents(auth, &clusters, window_start, generated_at)
            .await?;
        let new_incidents = ReportSection {
            items: incidents
                .items
                .iter()
                .filter(|item| item.observed_at >= window_start)
                .cloned()
                .collect(),
            truncated: incidents.truncated,
        };
        let risk_trends = self
            .risk_trends(auth, &clusters, generated_at - Duration::hours(24), generated_at)
            .await?;
        let recent_changes = self.recent_changes(auth, &clusters, window_start, generated_at).await?;
        let expiring_certificates = self.capacity_findings(auth, &clusters, generated_at, true).await?;
        let capacity_risks = self.capacity_findings(auth, &clusters, generated_at, false).await?;
        let overdue_action_items = self.overdue_action_items(auth, &clusters, generated_at).await?;
        let source_gaps = self.source_gaps(auth, &clusters).await?;
        let sections = [
            &new_incidents,
            &incidents,
            &risk_trends,
            &recent_changes,
            &expiring_certificates,
            &capacity_risks,
            &overdue_action_items,
            &source_gaps,
        ];
        let partial = sections.iter().any(|section| section.truncated);
        Ok(ShiftHandoffSummary {
            schema_version: HANDOFF_SCHEMA.to_owned(),
            tenant_id: auth.tenant_id,
            window_start,
            generated_at,
            new_incidents: new_incidents.items,
            unresolved_incidents: incidents.items,
            risk_trends: risk_trends.items,
            recent_changes: recent_changes.items,
            expiring_certificates: expiring_certificates.items,
            capacity_risks: capacity_risks.items,
            overdue_action_items: overdue_action_items.items,
            source_gaps: source_gaps.items,
            partial,
            warnings: partial
                .then(|| "one or more handoff sections were truncated to 64 items".to_owned())
                .into_iter()
                .collect(),
        })
    }

    pub(super) async fn report(
        &self,
        auth: &AuthContext,
        cluster_id: Option<ClusterId>,
        window: OperationsReportWindow,
    ) -> Result<OperationsReport, ControlPlaneError> {
        let clusters = scoped_clusters(auth, cluster_id)?;
        let window_end = Utc::now();
        let window_start = window_end
            - match window {
                OperationsReportWindow::Daily => Duration::days(1),
                OperationsReportWindow::Weekly => Duration::days(7),
            };
        let worst_clusters = self.worst_clusters(auth, &clusters).await?;
        let slo_burns = self.slo_burns(auth, &clusters, window_start, window_end).await?;
        let diagnostic_pack_findings = self
            .diagnostic_pack_findings(auth, &clusters, window_start, window_end)
            .await?;
        let repeat_incidents = self.repeat_incidents(auth, &clusters, window_start, window_end).await?;
        let forecast_errors = self.forecast_errors(auth, &clusters, window_start, window_end).await?;
        let forecast_mean_absolute_error = mean_error(&forecast_errors.items);
        let source_gaps = self.source_gaps(auth, &clusters).await?;
        let sections = [
            &worst_clusters,
            &slo_burns,
            &diagnostic_pack_findings,
            &repeat_incidents,
            &forecast_errors,
            &source_gaps,
        ];
        let partial = sections.iter().any(|section| section.truncated);
        Ok(OperationsReport {
            schema_version: REPORT_SCHEMA.to_owned(),
            tenant_id: auth.tenant_id,
            window,
            window_start,
            window_end,
            generated_at: window_end,
            worst_clusters: worst_clusters.items,
            slo_burns: slo_burns.items,
            diagnostic_pack_findings: diagnostic_pack_findings.items,
            repeat_incidents: repeat_incidents.items,
            forecast_mean_absolute_error,
            forecast_errors: forecast_errors.items,
            source_gaps: source_gaps.items,
            partial,
            warnings: partial
                .then(|| "one or more report sections were truncated to 64 items".to_owned())
                .into_iter()
                .collect(),
            cluster_mutation_count: 0,
        })
    }

    async fn active_incidents(
        &self,
        auth: &AuthContext,
        clusters: &[Uuid],
        _window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<ReportSection, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT id, cluster_id, title, resource, severity, owner_name,
                    status, created_at, sla_ack_due_at, sla_resolve_due_at
             FROM sre_incidents
             WHERE tenant_id = $1 AND cluster_id = ANY($2)
               AND status NOT IN ('resolved', 'escalated')
               AND merged_into_incident_id IS NULL
               AND (suppressed_until IS NULL OR suppressed_until <= $3)
             ORDER BY
               CASE severity
                   WHEN 'critical' THEN 0 WHEN 'error' THEN 1
                   WHEN 'warning' THEN 2 ELSE 3
               END,
               created_at DESC, id DESC
             LIMIT $4",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(clusters)
        .bind(now)
        .bind(FETCH_LIMIT)
        .fetch_all(&self.pool)
        .await?;
        bounded_rows(rows, "active_incidents", |row| {
            let incident_id = IncidentId::from_uuid(row.try_get("id")?);
            let cluster_id = ClusterId::from_uuid(row.try_get("cluster_id")?);
            let status: String = row.try_get("status")?;
            let ack_due: DateTime<Utc> = row.try_get("sla_ack_due_at")?;
            let resolve_due: DateTime<Utc> = row.try_get("sla_resolve_due_at")?;
            Ok(OperationsFinding {
                category: "incident".to_owned(),
                severity: row
                    .try_get::<Option<String>, _>("severity")?
                    .unwrap_or_else(|| "info".to_owned()),
                title: row.try_get("title")?,
                cluster_id,
                incident_id: Some(incident_id),
                resource: row.try_get("resource")?,
                detail: format!(
                    "status={status}; ack_due={}; resolve_due={}",
                    ack_due.to_rfc3339(),
                    resolve_due.to_rfc3339()
                ),
                suggested_owner: normalized_owner(row.try_get("owner_name")?),
                observed_at: row.try_get("created_at")?,
                deep_link: incident_link(incident_id),
            })
        })
    }

    async fn risk_trends(
        &self,
        auth: &AuthContext,
        clusters: &[Uuid],
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<ReportSection, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT p.cluster_id, p.metric, p.before_value, p.after_value,
                    p.score, p.detected_at, c.owner_name
             FROM change_points p
             JOIN clusters c ON c.id = p.cluster_id
             WHERE p.tenant_id = $1 AND p.cluster_id = ANY($2)
               AND p.detected_at >= $3 AND p.detected_at <= $4
             ORDER BY p.score DESC, p.detected_at DESC
             LIMIT $5",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(clusters)
        .bind(start)
        .bind(end)
        .bind(FETCH_LIMIT)
        .fetch_all(&self.pool)
        .await?;
        bounded_rows(rows, "risk_trends", |row| {
            let metric: String = row.try_get("metric")?;
            let before: f64 = row.try_get("before_value")?;
            let after: f64 = row.try_get("after_value")?;
            Ok(OperationsFinding {
                category: "risk_trend".to_owned(),
                severity: "warning".to_owned(),
                title: format!("{metric} trend changed"),
                cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
                incident_id: None,
                resource: Some(metric.clone()),
                detail: format!("value changed from {before:.3} to {after:.3}"),
                suggested_owner: normalized_owner(row.try_get("owner_name")?),
                observed_at: row.try_get("detected_at")?,
                deep_link: "/forecasts".to_owned(),
            })
        })
    }

    async fn recent_changes(
        &self,
        auth: &AuthContext,
        clusters: &[Uuid],
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<ReportSection, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT t.cluster_id, t.incident_id, t.event_type, t.summary,
                    t.occurred_at, c.owner_name
             FROM incident_timeline t
             JOIN clusters c ON c.id = t.cluster_id
             WHERE t.tenant_id = $1 AND t.cluster_id = ANY($2)
               AND t.occurred_at >= $3 AND t.occurred_at <= $4
               AND t.event_type IN (
                   'deployment_change', 'configuration_change',
                   'certificate_change', 'incident_assign',
                   'incident_suppress'
               )
             ORDER BY t.occurred_at DESC, t.sequence_id DESC
             LIMIT $5",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(clusters)
        .bind(start)
        .bind(end)
        .bind(FETCH_LIMIT)
        .fetch_all(&self.pool)
        .await?;
        bounded_rows(rows, "recent_changes", |row| {
            let incident_id = row
                .try_get::<Option<Uuid>, _>("incident_id")?
                .map(IncidentId::from_uuid);
            Ok(OperationsFinding {
                category: "recent_change".to_owned(),
                severity: "info".to_owned(),
                title: row.try_get("summary")?,
                cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
                incident_id,
                resource: Some(row.try_get("event_type")?),
                detail: "Audited operational change recorded in the incident timeline".to_owned(),
                suggested_owner: normalized_owner(row.try_get("owner_name")?),
                observed_at: row.try_get("occurred_at")?,
                deep_link: incident_id.map_or_else(|| "/operations".to_owned(), incident_link),
            })
        })
    }

    async fn capacity_findings(
        &self,
        auth: &AuthContext,
        clusters: &[Uuid],
        now: DateTime<Utc>,
        expiry_only: bool,
    ) -> Result<ReportSection, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT f.cluster_id, f.metric, f.status, f.exhaustion_at,
                    f.coverage_ratio, f.observed_at, c.owner_name
             FROM capacity_forecasts f
             JOIN clusters c ON c.id = f.cluster_id
             WHERE f.tenant_id = $1 AND f.cluster_id = ANY($2)
               AND (
                   ($3 AND f.metric IN (
                       'certificate_expiry', 'secret_expiry', 'jwks_expiry'
                   ) AND f.exhaustion_at <= $4 + INTERVAL '30 days')
                   OR
                   (NOT $3 AND f.metric NOT IN (
                       'certificate_expiry', 'secret_expiry', 'jwks_expiry'
                   ) AND (
                       f.exhaustion_at <= $4 + INTERVAL '30 days'
                       OR f.status IN ('stale', 'unstable_trend')
                   ))
               )
             ORDER BY f.exhaustion_at NULLS LAST, f.observed_at DESC
             LIMIT $5",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(clusters)
        .bind(expiry_only)
        .bind(now)
        .bind(FETCH_LIMIT)
        .fetch_all(&self.pool)
        .await?;
        bounded_rows(
            rows,
            if expiry_only {
                "expiring_certificates"
            } else {
                "capacity_risks"
            },
            |row| {
                let metric: String = row.try_get("metric")?;
                let status: String = row.try_get("status")?;
                let exhaustion: Option<DateTime<Utc>> = row.try_get("exhaustion_at")?;
                Ok(OperationsFinding {
                    category: if expiry_only {
                        "expiring_certificate".to_owned()
                    } else {
                        "capacity_risk".to_owned()
                    },
                    severity: if exhaustion.is_some_and(|at| at <= now + Duration::days(7)) {
                        "critical".to_owned()
                    } else {
                        "warning".to_owned()
                    },
                    title: if expiry_only {
                        format!("{metric} requires rotation review")
                    } else {
                        format!("{metric} capacity runway is constrained")
                    },
                    cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
                    incident_id: None,
                    resource: Some(metric),
                    detail: format!(
                        "status={status}; exhaustion={}; coverage={:.0}%",
                        exhaustion.map_or_else(|| "unknown".to_owned(), |value| value.to_rfc3339()),
                        row.try_get::<f64, _>("coverage_ratio")? * 100.0
                    ),
                    suggested_owner: normalized_owner(row.try_get("owner_name")?),
                    observed_at: row.try_get("observed_at")?,
                    deep_link: "/forecasts".to_owned(),
                })
            },
        )
    }

    async fn overdue_action_items(
        &self,
        auth: &AuthContext,
        clusters: &[Uuid],
        now: DateTime<Utc>,
    ) -> Result<ReportSection, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT a.cluster_id, a.incident_id, a.title, a.owner_name,
                    a.due_at, a.updated_at, c.owner_name AS cluster_owner
             FROM action_items a
             JOIN clusters c ON c.id = a.cluster_id
             WHERE a.tenant_id = $1 AND a.cluster_id = ANY($2)
               AND a.status NOT IN ('completed', 'cancelled')
               AND a.due_at IS NOT NULL AND a.due_at < $3
             ORDER BY a.due_at, a.id
             LIMIT $4",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(clusters)
        .bind(now)
        .bind(FETCH_LIMIT)
        .fetch_all(&self.pool)
        .await?;
        bounded_rows(rows, "overdue_action_items", |row| {
            let incident_id = IncidentId::from_uuid(row.try_get("incident_id")?);
            let due_at: DateTime<Utc> = row.try_get("due_at")?;
            let owner = row
                .try_get::<Option<String>, _>("owner_name")?
                .unwrap_or(row.try_get("cluster_owner")?);
            Ok(OperationsFinding {
                category: "overdue_action_item".to_owned(),
                severity: "warning".to_owned(),
                title: row.try_get("title")?,
                cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
                incident_id: Some(incident_id),
                resource: None,
                detail: format!("action item was due at {}", due_at.to_rfc3339()),
                suggested_owner: normalized_owner(owner),
                observed_at: row.try_get("updated_at")?,
                deep_link: format!("/incidents/{incident_id}/postmortem"),
            })
        })
    }

    async fn source_gaps(&self, auth: &AuthContext, clusters: &[Uuid]) -> Result<ReportSection, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT c.id AS cluster_id, c.owner_name, latest.data_sources,
                    latest.observed_at
             FROM clusters c
             LEFT JOIN LATERAL (
                 SELECT s.data_sources, s.observed_at
                 FROM cluster_capability_snapshots s
                 WHERE s.cluster_id = c.id
                 ORDER BY s.observed_at DESC, s.created_at DESC
                 LIMIT 1
             ) latest ON TRUE
             WHERE c.tenant_id = $1 AND c.id = ANY($2)
               AND c.onboarding_state <> 'offboarded'
             ORDER BY c.id",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(clusters)
        .fetch_all(&self.pool)
        .await?;
        let mut items = Vec::new();
        for row in rows {
            let cluster_id = ClusterId::from_uuid(row.try_get("cluster_id")?);
            let owner: String = row.try_get("owner_name")?;
            let observed_at = row
                .try_get::<Option<DateTime<Utc>>, _>("observed_at")?
                .unwrap_or_else(Utc::now);
            let data_sources: Option<Value> = row.try_get("data_sources")?;
            let Some(sources) = data_sources.as_ref().and_then(Value::as_array) else {
                items.push(OperationsFinding {
                    category: "source_gap".to_owned(),
                    severity: "warning".to_owned(),
                    title: "Capability source manifest is missing".to_owned(),
                    cluster_id,
                    incident_id: None,
                    resource: None,
                    detail: "No queryable source state has been reported by the connector".to_owned(),
                    suggested_owner: normalized_owner(owner),
                    observed_at,
                    deep_link: format!("/clusters/{cluster_id}"),
                });
                continue;
            };
            for source in sources {
                let availability = source.get("availability").and_then(Value::as_str).unwrap_or("unknown");
                if availability == "queryable" {
                    continue;
                }
                let source_id = source.get("id").and_then(Value::as_str).unwrap_or("unknown_source");
                items.push(OperationsFinding {
                    category: "source_gap".to_owned(),
                    severity: if availability == "missing_instrumentation" {
                        "warning".to_owned()
                    } else {
                        "info".to_owned()
                    },
                    title: format!("{source_id} is not remotely queryable"),
                    cluster_id,
                    incident_id: None,
                    resource: Some(source_id.to_owned()),
                    detail: format!("availability={availability}"),
                    suggested_owner: normalized_owner(owner.clone()),
                    observed_at,
                    deep_link: format!("/clusters/{cluster_id}"),
                });
            }
        }
        let truncated = items.len() > MAX_SECTION_ITEMS;
        items.truncate(MAX_SECTION_ITEMS);
        Ok(ReportSection { items, truncated })
    }

    async fn worst_clusters(&self, auth: &AuthContext, clusters: &[Uuid]) -> Result<ReportSection, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT DISTINCT ON (h.cluster_id)
                    h.cluster_id, h.score, h.status, h.data_quality,
                    h.observed_at, c.external_cluster_key, c.owner_name
             FROM cluster_health_snapshots h
             JOIN clusters c ON c.id = h.cluster_id
             WHERE h.tenant_id = $1 AND h.cluster_id = ANY($2)
             ORDER BY h.cluster_id, h.observed_at DESC, h.id DESC",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(clusters)
        .fetch_all(&self.pool)
        .await?;
        let mut rows = rows;
        rows.sort_by_key(|row| row.try_get::<Option<i16>, _>("score").ok().flatten().unwrap_or(-1));
        bounded_rows(rows, "worst_clusters", |row| {
            let cluster_id = ClusterId::from_uuid(row.try_get("cluster_id")?);
            let score: Option<i16> = row.try_get("score")?;
            let status: String = row.try_get("status")?;
            let data_quality: String = row.try_get("data_quality")?;
            Ok(OperationsFinding {
                category: "cluster_health".to_owned(),
                severity: if status == "critical" {
                    "critical".to_owned()
                } else if status == "degraded" {
                    "warning".to_owned()
                } else {
                    "info".to_owned()
                },
                title: format!(
                    "{} health is {status}",
                    row.try_get::<String, _>("external_cluster_key")?
                ),
                cluster_id,
                incident_id: None,
                resource: None,
                detail: format!(
                    "score={}; data_quality={data_quality}",
                    score.map_or_else(|| "unknown".to_owned(), |value| value.to_string())
                ),
                suggested_owner: normalized_owner(row.try_get("owner_name")?),
                observed_at: row.try_get("observed_at")?,
                deep_link: format!("/clusters/{cluster_id}"),
            })
        })
    }

    async fn slo_burns(
        &self,
        auth: &AuthContext,
        clusters: &[Uuid],
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<ReportSection, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT h.cluster_id, h.report, h.observed_at, c.owner_name
             FROM cluster_health_snapshots h
             JOIN clusters c ON c.id = h.cluster_id
             WHERE h.tenant_id = $1 AND h.cluster_id = ANY($2)
               AND h.observed_at >= $3 AND h.observed_at <= $4
             ORDER BY h.observed_at DESC
             LIMIT 256",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(clusters)
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;
        let mut items = Vec::new();
        for row in rows {
            let report: Value = row.try_get("report")?;
            let cluster_id = ClusterId::from_uuid(row.try_get("cluster_id")?);
            let owner: String = row.try_get("owner_name")?;
            let observed_at: DateTime<Utc> = row.try_get("observed_at")?;
            for sli in report.get("slis").and_then(Value::as_array).into_iter().flatten() {
                let sli_id = sli.get("id").and_then(Value::as_str).unwrap_or("unknown_sli");
                for burn in sli.get("windows").and_then(Value::as_array).into_iter().flatten() {
                    if !burn.get("triggered").and_then(Value::as_bool).unwrap_or(false) {
                        continue;
                    }
                    let window_id = burn
                        .get("window_id")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown_window");
                    let short_rate = burn.get("short_burn_rate").and_then(Value::as_f64);
                    let long_rate = burn.get("long_burn_rate").and_then(Value::as_f64);
                    items.push(OperationsFinding {
                        category: "slo_burn".to_owned(),
                        severity: burn
                            .get("severity")
                            .and_then(Value::as_str)
                            .unwrap_or("warning")
                            .to_owned(),
                        title: format!("{sli_id} SLO burn triggered"),
                        cluster_id,
                        incident_id: None,
                        resource: Some(sli_id.to_owned()),
                        detail: format!(
                            "window={window_id}; short_rate={}; long_rate={}",
                            display_optional_number(short_rate),
                            display_optional_number(long_rate)
                        ),
                        suggested_owner: normalized_owner(owner.clone()),
                        observed_at,
                        deep_link: format!("/clusters/{cluster_id}/slo"),
                    });
                }
            }
        }
        let truncated = items.len() > MAX_SECTION_ITEMS;
        items.truncate(MAX_SECTION_ITEMS);
        Ok(ReportSection { items, truncated })
    }

    async fn diagnostic_pack_findings(
        &self,
        auth: &AuthContext,
        clusters: &[Uuid],
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<ReportSection, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT r.cluster_id, r.incident_id, r.pack_id, r.output,
                    r.completed_at, c.owner_name
             FROM diagnostic_pack_runs r
             JOIN clusters c ON c.id = r.cluster_id
             WHERE r.tenant_id = $1 AND r.cluster_id = ANY($2)
               AND r.completed_at >= $3 AND r.completed_at <= $4
             ORDER BY r.completed_at DESC
             LIMIT 256",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(clusters)
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;
        let mut items = Vec::new();
        for row in rows {
            let output: Value = row.try_get("output")?;
            let cluster_id = ClusterId::from_uuid(row.try_get("cluster_id")?);
            let incident_id = row
                .try_get::<Option<Uuid>, _>("incident_id")?
                .map(IncidentId::from_uuid);
            let pack_id: String = row.try_get("pack_id")?;
            let owner: String = row.try_get("owner_name")?;
            let observed_at: DateTime<Utc> = row.try_get("completed_at")?;
            for finding in output.get("findings").and_then(Value::as_array).into_iter().flatten() {
                let outcome = finding.get("outcome").and_then(Value::as_str).unwrap_or("unknown");
                if outcome != "fault" {
                    continue;
                }
                let reason_code = finding.get("reason_code").and_then(Value::as_str).unwrap_or("UNKNOWN");
                let root_cause = finding
                    .get("root_cause")
                    .and_then(Value::as_str)
                    .unwrap_or("Diagnostic finding");
                items.push(OperationsFinding {
                    category: "diagnostic_pack".to_owned(),
                    severity: finding
                        .get("severity")
                        .and_then(Value::as_str)
                        .unwrap_or("warning")
                        .to_owned(),
                    title: root_cause.to_owned(),
                    cluster_id,
                    incident_id,
                    resource: Some(pack_id.clone()),
                    detail: format!("pack={pack_id}; reason_code={reason_code}"),
                    suggested_owner: normalized_owner(owner.clone()),
                    observed_at,
                    deep_link: incident_id.map_or_else(|| "/inspections".to_owned(), incident_link),
                });
            }
        }
        let truncated = items.len() > MAX_SECTION_ITEMS;
        items.truncate(MAX_SECTION_ITEMS);
        Ok(ReportSection { items, truncated })
    }

    async fn repeat_incidents(
        &self,
        auth: &AuthContext,
        clusters: &[Uuid],
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<ReportSection, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT i.id, i.cluster_id, i.title, i.resource, i.severity,
                    i.owner_name, i.occurrence_count, i.reopened_from_incident_id,
                    i.created_at
             FROM sre_incidents i
             WHERE i.tenant_id = $1 AND i.cluster_id = ANY($2)
               AND i.created_at >= $3 AND i.created_at <= $4
               AND (i.occurrence_count > 1 OR i.reopened_from_incident_id IS NOT NULL)
             ORDER BY i.occurrence_count DESC, i.created_at DESC
             LIMIT $5",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(clusters)
        .bind(start)
        .bind(end)
        .bind(FETCH_LIMIT)
        .fetch_all(&self.pool)
        .await?;
        bounded_rows(rows, "repeat_incidents", |row| {
            let incident_id = IncidentId::from_uuid(row.try_get("id")?);
            let occurrence_count: i32 = row.try_get("occurrence_count")?;
            let previous = row
                .try_get::<Option<Uuid>, _>("reopened_from_incident_id")?
                .map(IncidentId::from_uuid);
            Ok(OperationsFinding {
                category: "repeat_incident".to_owned(),
                severity: row
                    .try_get::<Option<String>, _>("severity")?
                    .unwrap_or_else(|| "warning".to_owned()),
                title: row.try_get("title")?,
                cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
                incident_id: Some(incident_id),
                resource: row.try_get("resource")?,
                detail: format!(
                    "occurrence_count={occurrence_count}; previous_incident={}",
                    previous.map_or_else(|| "none".to_owned(), |id| id.to_string())
                ),
                suggested_owner: normalized_owner(row.try_get("owner_name")?),
                observed_at: row.try_get("created_at")?,
                deep_link: incident_link(incident_id),
            })
        })
    }

    async fn forecast_errors(
        &self,
        auth: &AuthContext,
        clusters: &[Uuid],
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<ReportSection, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT o.cluster_id, o.metric, o.forecast_window,
                    o.predicted_value, o.actual_value, o.absolute_error,
                    o.recorded_at, c.owner_name
             FROM forecast_actual_outcomes o
             JOIN clusters c ON c.id = o.cluster_id
             WHERE o.tenant_id = $1 AND o.cluster_id = ANY($2)
               AND o.recorded_at >= $3 AND o.recorded_at <= $4
             ORDER BY o.absolute_error DESC, o.recorded_at DESC
             LIMIT $5",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(clusters)
        .bind(start)
        .bind(end)
        .bind(FETCH_LIMIT)
        .fetch_all(&self.pool)
        .await?;
        bounded_rows(rows, "forecast_errors", |row| {
            let metric: String = row.try_get("metric")?;
            let absolute_error: f64 = row.try_get("absolute_error")?;
            Ok(OperationsFinding {
                category: "forecast_error".to_owned(),
                severity: "info".to_owned(),
                title: format!("{metric} forecast error {absolute_error:.3}"),
                cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
                incident_id: None,
                resource: Some(metric),
                detail: format!(
                    "window={}; predicted={:.3}; actual={:.3}; mae_sample={absolute_error:.3}",
                    row.try_get::<String, _>("forecast_window")?,
                    row.try_get::<f64, _>("predicted_value")?,
                    row.try_get::<f64, _>("actual_value")?
                ),
                suggested_owner: normalized_owner(row.try_get("owner_name")?),
                observed_at: row.try_get("recorded_at")?,
                deep_link: "/forecasts".to_owned(),
            })
        })
    }
}
