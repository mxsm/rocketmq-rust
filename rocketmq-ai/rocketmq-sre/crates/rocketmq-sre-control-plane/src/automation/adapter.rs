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

use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::AutomationArtifact;
use rocketmq_sre_contracts::AutomationRunStatus;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::ModelInvocationId;
use rocketmq_sre_contracts::NoSideEffectAutomationKind;
use rocketmq_sre_contracts::NoSideEffectAutomationRequest;
use rocketmq_sre_contracts::NotificationDeliveryId;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::TimeRange;
use sqlx::Row;
use uuid::Uuid;

use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;
use crate::connector_channel::PostgresConnectorChannelService;
use crate::evidence::EvidenceService;
use crate::evidence::PersistEvidenceRequest;
use crate::postmortem::CreatePostmortemRequest;
use crate::postmortem::PostmortemService;

const DEFAULT_PUBLIC_BASE_URL: &str = "http://localhost:3004";
const MAX_NOTIFICATION_TARGETS: i64 = 16;

#[derive(Clone)]
pub(super) struct AutomationDispatcher {
    repository: PostgresRepository,
    connector_channel: PostgresConnectorChannelService,
    evidence: EvidenceService,
    postmortems: PostmortemService,
    public_base_url: String,
}

#[derive(Clone, Debug)]
pub(super) struct AutomationDispatchOutcome {
    pub(super) status: AutomationRunStatus,
    pub(super) result_code: String,
    pub(super) sanitized_summary: String,
    pub(super) artifacts: Vec<AutomationArtifact>,
    pub(super) model_invocation_id: Option<ModelInvocationId>,
}

impl AutomationDispatcher {
    pub(super) fn new(
        repository: PostgresRepository,
        connector_channel: PostgresConnectorChannelService,
        evidence: EvidenceService,
        postmortems: PostmortemService,
    ) -> Result<Self, ControlPlaneError> {
        let public_base_url =
            std::env::var("ROCKETMQ_SRE_PUBLIC_URL").unwrap_or_else(|_| DEFAULT_PUBLIC_BASE_URL.to_owned());
        let parsed = url::Url::parse(&public_base_url)
            .map_err(|_| ControlPlaneError::configuration("ROCKETMQ_SRE_PUBLIC_URL is invalid"))?;
        if !matches!(parsed.scheme(), "http" | "https")
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
        {
            return Err(ControlPlaneError::configuration(
                "ROCKETMQ_SRE_PUBLIC_URL must be an HTTP(S) origin without credentials",
            ));
        }
        Ok(Self {
            repository,
            connector_channel,
            evidence,
            postmortems,
            public_base_url: public_base_url.trim_end_matches('/').to_owned(),
        })
    }

    pub(super) async fn dispatch(
        &self,
        auth: &AuthContext,
        request: &NoSideEffectAutomationRequest,
    ) -> Result<AutomationDispatchOutcome, ControlPlaneError> {
        match request.kind {
            NoSideEffectAutomationKind::AlertCorrelation => self.alert_correlation(auth, request).await,
            NoSideEffectAutomationKind::SeverityOwnerSuggestion => self.severity_owner_suggestion(auth, request).await,
            NoSideEffectAutomationKind::EvidenceCollection => self.evidence_collection(auth, request).await,
            NoSideEffectAutomationKind::ShiftSummary => self.shift_summary(auth, request).await,
            NoSideEffectAutomationKind::Notification => self.notification(auth, request).await,
            NoSideEffectAutomationKind::PostmortemDraft => self.postmortem_draft(auth, request).await,
        }
    }

    async fn alert_correlation(
        &self,
        auth: &AuthContext,
        request: &NoSideEffectAutomationRequest,
    ) -> Result<AutomationDispatchOutcome, ControlPlaneError> {
        let cluster_id = required_cluster(request)?;
        let row = sqlx::query(
            "SELECT i.id, i.occurrence_count, COUNT(ia.alert_id) AS linked_alerts
             FROM sre_incidents i
             LEFT JOIN incident_alerts ia ON ia.incident_id = i.id
             WHERE i.tenant_id = $1 AND i.cluster_id = $2
               AND ($3::UUID IS NULL OR i.id = $3)
             GROUP BY i.id, i.occurrence_count, i.updated_at
             ORDER BY i.updated_at DESC, i.id DESC
             LIMIT 1",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(request.incident_id.map(rocketmq_sre_contracts::IncidentId::as_uuid))
        .fetch_optional(&self.repository.pool)
        .await?;
        let Some(row) = row else {
            return Ok(AutomationDispatchOutcome::succeeded(
                "no_alerts_to_correlate",
                "No persisted alerts require correlation in the selected scope",
                Vec::new(),
            ));
        };
        let incident_id: Uuid = row.try_get("id")?;
        let occurrence_count: i32 = row.try_get("occurrence_count")?;
        let linked_alerts: i64 = row.try_get("linked_alerts")?;
        Ok(AutomationDispatchOutcome::succeeded(
            "alert_correlation_reconciled",
            format!(
                "Deterministic correlation links {linked_alerts} alert records across {occurrence_count} occurrences"
            ),
            vec![artifact("incident_correlation", incident_id)],
        ))
    }

    async fn severity_owner_suggestion(
        &self,
        auth: &AuthContext,
        request: &NoSideEffectAutomationRequest,
    ) -> Result<AutomationDispatchOutcome, ControlPlaneError> {
        let cluster_id = required_cluster(request)?;
        let incident_id = required_incident(request)?;
        let row = sqlx::query(
            "SELECT severity, owner_name
             FROM sre_incidents
             WHERE id = $1 AND tenant_id = $2 AND cluster_id = $3",
        )
        .bind(incident_id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .fetch_optional(&self.repository.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let severity = bounded_label(
            row.try_get::<Option<String>, _>("severity")?
                .as_deref()
                .unwrap_or("unknown"),
            "unknown",
        );
        let owner = bounded_label(&row.try_get::<String, _>("owner_name")?, "unassigned");
        Ok(AutomationDispatchOutcome::succeeded(
            "severity_owner_suggested",
            format!("Deterministic triage suggests severity {severity} and owner {owner}"),
            vec![artifact("incident_triage_suggestion", request.id.as_uuid())],
        ))
    }

    async fn evidence_collection(
        &self,
        auth: &AuthContext,
        request: &NoSideEffectAutomationRequest,
    ) -> Result<AutomationDispatchOutcome, ControlPlaneError> {
        let cluster_id = required_cluster(request)?;
        let incident_id = required_incident(request)?;
        let now = Utc::now();
        let time_range = TimeRange::new(now - Duration::hours(1), now)
            .map_err(|_| ControlPlaneError::validation("invalid_request", "evidence time range is invalid"))?;
        let query = EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: request.correlation_id,
            tenant_id: auth.tenant_id,
            cluster_id,
            source: "rocketmq-mcp".to_owned(),
            resource: "cluster/overview".to_owned(),
            time_range,
        };
        let deadline = now + Duration::seconds(i64::from(request.budget.timeout_seconds));
        let response = self
            .connector_channel
            .query_and_wait(auth.tenant_id, cluster_id, query, deadline)
            .await?;
        let snapshot = response.evidence.ok_or_else(|| {
            ControlPlaneError::validation(
                "source_unavailable",
                "Connector did not return a cluster overview Evidence snapshot",
            )
        })?;
        let snapshot = self
            .evidence
            .persist(
                auth,
                PersistEvidenceRequest {
                    investigation_id: None,
                    incident_id: Some(incident_id),
                    evidence: snapshot,
                },
            )
            .await?;
        Ok(AutomationDispatchOutcome::succeeded(
            "evidence_collection_completed",
            "Collected and persisted a fresh read-only cluster overview Evidence snapshot",
            vec![artifact("evidence_snapshot", snapshot.evidence_id.as_uuid())],
        ))
    }

    async fn shift_summary(
        &self,
        auth: &AuthContext,
        request: &NoSideEffectAutomationRequest,
    ) -> Result<AutomationDispatchOutcome, ControlPlaneError> {
        let clusters = request.cluster_id.map_or_else(
            || auth.clusters.iter().map(|id| id.as_uuid()).collect::<Vec<_>>(),
            |id| vec![id.as_uuid()],
        );
        if clusters.is_empty() {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "shift summary requires at least one authorized cluster",
            ));
        }
        let row = sqlx::query(
            "SELECT
                COUNT(*) FILTER (WHERE status NOT IN ('resolved', 'escalated')) AS active,
                COUNT(*) FILTER (
                    WHERE status NOT IN ('resolved', 'escalated') AND severity = 'critical'
                ) AS critical,
                COUNT(*) FILTER (
                    WHERE status NOT IN ('resolved', 'escalated') AND owner_name = 'unassigned'
                ) AS unassigned,
                COUNT(*) FILTER (WHERE updated_at >= NOW() - INTERVAL '8 hours') AS changed
             FROM sre_incidents
             WHERE tenant_id = $1 AND cluster_id = ANY($2)",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(&clusters)
        .fetch_one(&self.repository.pool)
        .await?;
        let active: i64 = row.try_get("active")?;
        let critical: i64 = row.try_get("critical")?;
        let unassigned: i64 = row.try_get("unassigned")?;
        let changed: i64 = row.try_get("changed")?;
        Ok(AutomationDispatchOutcome::succeeded(
            "shift_summary_generated",
            format!(
                "Eight-hour shift summary: {active} active, {critical} critical, {unassigned} unassigned, {changed} \
                 changed"
            ),
            vec![artifact("automation_shift_summary", request.id.as_uuid())],
        ))
    }

    async fn notification(
        &self,
        auth: &AuthContext,
        request: &NoSideEffectAutomationRequest,
    ) -> Result<AutomationDispatchOutcome, ControlPlaneError> {
        let cluster_id = required_cluster(request)?;
        let incident_id = required_incident(request)?;
        let incident = sqlx::query(
            "SELECT severity
             FROM sre_incidents
             WHERE id = $1 AND tenant_id = $2 AND cluster_id = $3",
        )
        .bind(incident_id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .fetch_optional(&self.repository.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let severity = bounded_label(
            incident
                .try_get::<Option<String>, _>("severity")?
                .as_deref()
                .unwrap_or("unknown"),
            "unknown",
        );
        let target_ids = sqlx::query_scalar::<_, Uuid>(
            "SELECT id
             FROM notification_targets
             WHERE tenant_id = $1 AND enabled
               AND (cluster_id IS NULL OR cluster_id = $2)
             ORDER BY CASE WHEN cluster_id IS NOT NULL THEN 0 ELSE 1 END, name, id
             LIMIT $3",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(MAX_NOTIFICATION_TARGETS)
        .fetch_all(&self.repository.pool)
        .await?;
        if target_ids.is_empty() {
            return Ok(AutomationDispatchOutcome::denied(
                "notification_target_unavailable",
                "No enabled notification target exists in the selected scope",
            ));
        }
        let summary = format!("RocketMQ SRE {severity} incident requires operator attention");
        let deep_link = format!("{}/incidents/{incident_id}", self.public_base_url);
        let mut artifacts = Vec::with_capacity(target_ids.len());
        for target_id in target_ids {
            let delivery_key = format!("automation:{}:target:{target_id}", request.id);
            let proposed_id = NotificationDeliveryId::new();
            let row = sqlx::query(
                "INSERT INTO notification_outbox (
                    id, target_id, tenant_id, cluster_id, incident_id,
                    delivery_key, status, sanitized_summary, deep_link,
                    attempt_count, next_attempt_at, created_at
                 ) VALUES (
                    $1, $2, $3, $4, $5,
                    $6, 'pending', $7, $8,
                    0, NOW(), NOW()
                 )
                 ON CONFLICT (tenant_id, delivery_key) DO UPDATE
                 SET delivery_key = EXCLUDED.delivery_key
                 RETURNING id",
            )
            .bind(proposed_id.as_uuid())
            .bind(target_id)
            .bind(auth.tenant_id.as_uuid())
            .bind(cluster_id.as_uuid())
            .bind(incident_id.as_uuid())
            .bind(delivery_key)
            .bind(&summary)
            .bind(&deep_link)
            .fetch_one(&self.repository.pool)
            .await?;
            artifacts.push(artifact("notification_delivery", row.try_get("id")?));
        }
        Ok(AutomationDispatchOutcome::succeeded(
            "notification_enqueued",
            format!(
                "Queued {} sanitized notification deliveries with incident deep links",
                artifacts.len()
            ),
            artifacts,
        ))
    }

    async fn postmortem_draft(
        &self,
        auth: &AuthContext,
        request: &NoSideEffectAutomationRequest,
    ) -> Result<AutomationDispatchOutcome, ControlPlaneError> {
        let incident_id = required_incident(request)?;
        let mut draft_auth = auth.clone();
        draft_auth.roles = BTreeSet::from(["diagnose".to_owned()]);
        let view = self
            .postmortems
            .create_bounded_draft(
                &draft_auth,
                incident_id,
                &CreatePostmortemRequest::default(),
                request.budget.max_model_calls,
            )
            .await?;
        let invocation_id = view.revisions.last().and_then(|revision| revision.model_invocation_id);
        Ok(AutomationDispatchOutcome {
            status: AutomationRunStatus::Succeeded,
            result_code: "postmortem_draft_created".to_owned(),
            sanitized_summary: "Created an immutable Postmortem draft for human review; it remains unpublished"
                .to_owned(),
            artifacts: vec![artifact("postmortem_draft", view.postmortem.id.as_uuid())],
            model_invocation_id: invocation_id,
        })
    }
}

impl AutomationDispatchOutcome {
    fn succeeded(
        result_code: impl Into<String>,
        sanitized_summary: impl Into<String>,
        artifacts: Vec<AutomationArtifact>,
    ) -> Self {
        Self {
            status: AutomationRunStatus::Succeeded,
            result_code: result_code.into(),
            sanitized_summary: sanitized_summary.into(),
            artifacts,
            model_invocation_id: None,
        }
    }

    fn denied(result_code: impl Into<String>, sanitized_summary: impl Into<String>) -> Self {
        Self {
            status: AutomationRunStatus::Denied,
            result_code: result_code.into(),
            sanitized_summary: sanitized_summary.into(),
            artifacts: Vec::new(),
            model_invocation_id: None,
        }
    }
}

fn required_cluster(
    request: &NoSideEffectAutomationRequest,
) -> Result<rocketmq_sre_contracts::ClusterId, ControlPlaneError> {
    request.cluster_id.ok_or_else(|| {
        ControlPlaneError::validation("invalid_automation_request", "automation kind requires a cluster scope")
    })
}

fn required_incident(
    request: &NoSideEffectAutomationRequest,
) -> Result<rocketmq_sre_contracts::IncidentId, ControlPlaneError> {
    request.incident_id.ok_or_else(|| {
        ControlPlaneError::validation(
            "invalid_automation_request",
            "automation kind requires an incident scope",
        )
    })
}

fn artifact(kind: &str, id: Uuid) -> AutomationArtifact {
    AutomationArtifact {
        kind: kind.to_owned(),
        id,
    }
}

fn bounded_label(value: &str, fallback: &'static str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty()
        || trimmed.chars().count() > 128
        || trimmed.chars().any(char::is_control)
        || [
            "authorization:",
            "bearer ",
            "token=",
            "secret=",
            "password=",
            "private key",
        ]
        .iter()
        .any(|marker| trimmed.to_ascii_lowercase().contains(marker))
    {
        return fallback.to_owned();
    }
    trimmed.to_owned()
}

#[cfg(test)]
mod tests {
    use super::bounded_label;

    #[test]
    fn bounded_label_rejects_sensitive_or_control_text() {
        assert_eq!(bounded_label("platform-team", "unknown"), "platform-team");
        assert_eq!(bounded_label("token=secret", "unknown"), "unknown");
        assert_eq!(bounded_label("owner\nother", "unknown"), "unknown");
    }
}
