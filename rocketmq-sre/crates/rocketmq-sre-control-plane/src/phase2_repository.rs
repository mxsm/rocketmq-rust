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

use rocketmq_sre_contracts::ActionItem;
use rocketmq_sre_contracts::AlertEvent;
use rocketmq_sre_contracts::BacklogEta;
use rocketmq_sre_contracts::CapacityForecast;
use rocketmq_sre_contracts::DrReadinessReport;
use rocketmq_sre_contracts::IncidentRelation;
use rocketmq_sre_contracts::NotificationDelivery;
use rocketmq_sre_contracts::PostmortemDraft;
use rocketmq_sre_contracts::PostmortemRevision;
use rocketmq_sre_contracts::PostmortemStatus;
use rocketmq_sre_contracts::UpgradeReadinessReport;
use rocketmq_sre_contracts::WhatIfSimulation;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use uuid::Uuid;

use crate::ControlPlaneError;
use crate::PostgresRepository;

/// Typed PostgreSQL persistence boundary for Phase 2 read-only operations and
/// SRE-owned workflow metadata.
#[allow(
    async_fn_in_trait,
    reason = "the control plane intentionally exposes a native async repository contract"
)]
pub trait Phase2Repository: Clone + Send + Sync + 'static {
    async fn store_alert(&self, event: &AlertEvent) -> Result<Uuid, ControlPlaneError>;
    async fn store_incident_relation(&self, relation: &IncidentRelation) -> Result<(), ControlPlaneError>;
    async fn enqueue_notification(&self, delivery: &NotificationDelivery) -> Result<bool, ControlPlaneError>;
    async fn store_capacity_forecast(&self, forecast: &CapacityForecast) -> Result<(), ControlPlaneError>;
    async fn store_backlog_eta(&self, forecast: &BacklogEta) -> Result<(), ControlPlaneError>;
    async fn store_simulation(&self, simulation: &WhatIfSimulation) -> Result<(), ControlPlaneError>;
    async fn store_upgrade_readiness(&self, report: &UpgradeReadinessReport) -> Result<(), ControlPlaneError>;
    async fn store_dr_readiness(&self, report: &DrReadinessReport) -> Result<(), ControlPlaneError>;
    async fn create_postmortem(&self, draft: &PostmortemDraft) -> Result<bool, ControlPlaneError>;
    async fn append_postmortem_revision(&self, revision: &PostmortemRevision) -> Result<(), ControlPlaneError>;
    async fn get_postmortem(
        &self,
        id: rocketmq_sre_contracts::PostmortemId,
    ) -> Result<PostmortemDraft, ControlPlaneError>;
    async fn list_postmortem_revisions(
        &self,
        id: rocketmq_sre_contracts::PostmortemId,
    ) -> Result<Vec<PostmortemRevision>, ControlPlaneError>;
    async fn upsert_action_item(&self, item: &ActionItem) -> Result<(), ControlPlaneError>;
}

impl Phase2Repository for PostgresRepository {
    async fn store_alert(&self, event: &AlertEvent) -> Result<Uuid, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let alert_id = sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO alert_events (
                id, tenant_id, cluster_id, source, source_event_id, fingerprint,
                correlation_key, affected_resource, symptom_family, severity,
                status, summary, labels, evidence_ids, occurrence_count,
                last_sequence, first_occurred_at, last_occurred_at, received_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, 0, $15, $16, $16, $17
             )
             ON CONFLICT (tenant_id, cluster_id, source, source_event_id)
             DO UPDATE SET
                fingerprint = CASE
                    WHEN alert_events.last_sequence <= EXCLUDED.last_sequence
                    THEN EXCLUDED.fingerprint ELSE alert_events.fingerprint END,
                correlation_key = CASE
                    WHEN alert_events.last_sequence <= EXCLUDED.last_sequence
                    THEN EXCLUDED.correlation_key ELSE alert_events.correlation_key END,
                affected_resource = CASE
                    WHEN alert_events.last_sequence <= EXCLUDED.last_sequence
                    THEN EXCLUDED.affected_resource ELSE alert_events.affected_resource END,
                symptom_family = CASE
                    WHEN alert_events.last_sequence <= EXCLUDED.last_sequence
                    THEN EXCLUDED.symptom_family ELSE alert_events.symptom_family END,
                severity = CASE
                    WHEN alert_events.last_sequence <= EXCLUDED.last_sequence
                    THEN EXCLUDED.severity ELSE alert_events.severity END,
                status = CASE
                    WHEN alert_events.last_sequence <= EXCLUDED.last_sequence
                    THEN EXCLUDED.status ELSE alert_events.status END,
                summary = CASE
                    WHEN alert_events.last_sequence <= EXCLUDED.last_sequence
                    THEN EXCLUDED.summary ELSE alert_events.summary END,
                labels = CASE
                    WHEN alert_events.last_sequence <= EXCLUDED.last_sequence
                    THEN EXCLUDED.labels ELSE alert_events.labels END,
                evidence_ids = CASE
                    WHEN alert_events.last_sequence <= EXCLUDED.last_sequence
                    THEN EXCLUDED.evidence_ids ELSE alert_events.evidence_ids END,
                last_sequence = GREATEST(alert_events.last_sequence, EXCLUDED.last_sequence),
                last_occurred_at = GREATEST(alert_events.last_occurred_at, EXCLUDED.last_occurred_at),
                received_at = GREATEST(alert_events.received_at, EXCLUDED.received_at)
             RETURNING id",
        )
        .bind(event.id.as_uuid())
        .bind(event.tenant_id.as_uuid())
        .bind(event.cluster_id.as_uuid())
        .bind(enum_value(&event.source)?)
        .bind(&event.source_event_id)
        .bind(&event.fingerprint)
        .bind(json_value(&event.correlation_key)?)
        .bind(json_value(&event.affected_resource)?)
        .bind(event.symptom_family.as_str())
        .bind(enum_value(&event.severity)?)
        .bind(enum_value(&event.status)?)
        .bind(&event.summary)
        .bind(json_value(&event.labels)?)
        .bind(event.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
        .bind(i64::try_from(event.sequence).map_err(|_| {
            ControlPlaneError::validation("invalid_alert_sequence", "alert sequence exceeds PostgreSQL BIGINT")
        })?)
        .bind(event.occurred_at)
        .bind(event.received_at)
        .fetch_one(&mut *transaction)
        .await?;

        let occurrence_id = format!("{}:{}", event.source_event_id, event.sequence);
        let occurrence_inserted = sqlx::query(
            "INSERT INTO alert_occurrences (
                alert_id, source_occurrence_id, status, severity,
                evidence_ids, occurred_at, received_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (alert_id, source_occurrence_id) DO NOTHING",
        )
        .bind(alert_id)
        .bind(occurrence_id)
        .bind(enum_value(&event.status)?)
        .bind(enum_value(&event.severity)?)
        .bind(event.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
        .bind(event.occurred_at)
        .bind(event.received_at)
        .execute(&mut *transaction)
        .await?
        .rows_affected()
            == 1;
        if occurrence_inserted {
            sqlx::query(
                "UPDATE alert_events
                 SET occurrence_count = occurrence_count + 1
                 WHERE id = $1",
            )
            .bind(alert_id)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(alert_id)
    }

    async fn store_incident_relation(&self, relation: &IncidentRelation) -> Result<(), ControlPlaneError> {
        sqlx::query(
            "INSERT INTO incident_relations (
                id, tenant_id, cluster_id, from_incident_id, to_incident_id,
                relation_kind, reason_code, evidence_ids, created_by, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (
                tenant_id, cluster_id, from_incident_id, to_incident_id, relation_kind
             ) DO NOTHING",
        )
        .bind(relation.id.as_uuid())
        .bind(relation.tenant_id.as_uuid())
        .bind(relation.cluster_id.as_uuid())
        .bind(relation.from_incident_id.as_uuid())
        .bind(relation.to_incident_id.as_uuid())
        .bind(enum_value(&relation.kind)?)
        .bind(&relation.reason_code)
        .bind(relation.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
        .bind(&relation.created_by)
        .bind(relation.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn enqueue_notification(&self, delivery: &NotificationDelivery) -> Result<bool, ControlPlaneError> {
        let result = sqlx::query(
            "INSERT INTO notification_outbox (
                id, target_id, tenant_id, cluster_id, incident_id, delivery_key,
                status, sanitized_summary, deep_link, attempt_count, next_attempt_at,
                last_error_code, delivered_at, created_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
             )
             ON CONFLICT (tenant_id, delivery_key) DO NOTHING",
        )
        .bind(delivery.id.as_uuid())
        .bind(delivery.target_id.as_uuid())
        .bind(delivery.tenant_id.as_uuid())
        .bind(delivery.cluster_id.as_uuid())
        .bind(delivery.incident_id.as_uuid())
        .bind(&delivery.delivery_key)
        .bind(enum_value(&delivery.status)?)
        .bind(&delivery.sanitized_summary)
        .bind(&delivery.deep_link)
        .bind(i32::from(delivery.attempt_count))
        .bind(delivery.next_attempt_at)
        .bind(&delivery.last_error_code)
        .bind(delivery.delivered_at)
        .bind(delivery.created_at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn store_capacity_forecast(&self, forecast: &CapacityForecast) -> Result<(), ControlPlaneError> {
        let report = json_value(forecast)?;
        sqlx::query(
            "INSERT INTO capacity_forecasts (
                id, tenant_id, cluster_id, resource, metric, status, quality,
                algorithm_version, sample_start, sample_end, coverage_ratio,
                slope_per_hour, volatility, threshold, exhaustion_at, points,
                evidence_ids, observed_at, report
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9,
                $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
             ) ON CONFLICT (id) DO NOTHING",
        )
        .bind(forecast.id.as_uuid())
        .bind(forecast.tenant_id.as_uuid())
        .bind(forecast.cluster_id.as_uuid())
        .bind(json_value(&forecast.resource)?)
        .bind(&forecast.metric)
        .bind(enum_value(&forecast.status)?)
        .bind(enum_value(&forecast.quality)?)
        .bind(&forecast.algorithm_version)
        .bind(forecast.sample_start)
        .bind(forecast.sample_end)
        .bind(forecast.coverage_ratio)
        .bind(forecast.slope_per_hour)
        .bind(forecast.volatility)
        .bind(forecast.threshold)
        .bind(forecast.exhaustion_at)
        .bind(json_value(&forecast.points)?)
        .bind(forecast.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
        .bind(forecast.observed_at)
        .bind(report)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn store_backlog_eta(&self, forecast: &BacklogEta) -> Result<(), ControlPlaneError> {
        let report = json_value(forecast)?;
        sqlx::query(
            "INSERT INTO backlog_eta_forecasts (
                id, tenant_id, cluster_id, resource, backlog_kind, status,
                current_value, arrival_rate_per_second, drain_rate_per_second,
                estimated_clear_at, coverage_ratio, algorithm_version,
                evidence_ids, observed_at, report
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
             ) ON CONFLICT (id) DO NOTHING",
        )
        .bind(forecast.id.as_uuid())
        .bind(forecast.tenant_id.as_uuid())
        .bind(forecast.cluster_id.as_uuid())
        .bind(json_value(&forecast.resource)?)
        .bind(&forecast.backlog_kind)
        .bind(enum_value(&forecast.status)?)
        .bind(forecast.current_value)
        .bind(forecast.arrival_rate_per_second)
        .bind(forecast.drain_rate_per_second)
        .bind(forecast.estimated_clear_at)
        .bind(forecast.coverage_ratio)
        .bind(&forecast.algorithm_version)
        .bind(forecast.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
        .bind(forecast.observed_at)
        .bind(report)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn store_simulation(&self, simulation: &WhatIfSimulation) -> Result<(), ControlPlaneError> {
        let report = json_value(simulation)?;
        sqlx::query(
            "INSERT INTO what_if_simulations (
                id, tenant_id, cluster_id, simulation_kind, status, input,
                assumptions, projected_utilization, bottlenecks, blast_radius,
                missing_assumptions, evidence_ids, algorithm_version, created_by,
                created_at, report, execution_eligible
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                $14, $15, $16, $17
             ) ON CONFLICT (id) DO NOTHING",
        )
        .bind(simulation.id.as_uuid())
        .bind(simulation.tenant_id.as_uuid())
        .bind(simulation.cluster_id.as_uuid())
        .bind(enum_value(&simulation.kind)?)
        .bind(enum_value(&simulation.status)?)
        .bind(&simulation.input)
        .bind(json_value(&simulation.assumptions)?)
        .bind(&simulation.projected_utilization)
        .bind(json_value(&simulation.bottlenecks)?)
        .bind(json_value(&simulation.blast_radius)?)
        .bind(json_value(&simulation.missing_assumptions)?)
        .bind(
            simulation
                .evidence_ids
                .iter()
                .map(|id| id.as_uuid())
                .collect::<Vec<_>>(),
        )
        .bind(&simulation.algorithm_version)
        .bind(&simulation.created_by)
        .bind(simulation.created_at)
        .bind(report)
        .bind(simulation.execution_eligible)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn store_upgrade_readiness(&self, report: &UpgradeReadinessReport) -> Result<(), ControlPlaneError> {
        let report_json = json_value(report)?;
        sqlx::query(
            "INSERT INTO upgrade_readiness_reports (
                id, tenant_id, cluster_id, target_version, status, findings,
                pack_versions, observed_at, expires_at, report, execution_eligible
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(report.id.as_uuid())
        .bind(report.tenant_id.as_uuid())
        .bind(report.cluster_id.as_uuid())
        .bind(&report.target_version)
        .bind(enum_value(&report.status)?)
        .bind(json_value(&report.findings)?)
        .bind(json_value(&report.pack_versions)?)
        .bind(report.observed_at)
        .bind(report.expires_at)
        .bind(report_json)
        .bind(report.execution_eligible)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn store_dr_readiness(&self, report: &DrReadinessReport) -> Result<(), ControlPlaneError> {
        let report_json = json_value(report)?;
        sqlx::query(
            "INSERT INTO dr_readiness_reports (
                id, tenant_id, cluster_id, target_region, requested_rto_seconds,
                requested_rpo_seconds, status, findings, observed_at, expires_at,
                report, execution_eligible
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(report.id.as_uuid())
        .bind(report.tenant_id.as_uuid())
        .bind(report.cluster_id.as_uuid())
        .bind(&report.target_region)
        .bind(
            i64::try_from(report.requested_rto_seconds)
                .map_err(|_| ControlPlaneError::validation("invalid_rto", "RTO exceeds PostgreSQL BIGINT"))?,
        )
        .bind(
            i64::try_from(report.requested_rpo_seconds)
                .map_err(|_| ControlPlaneError::validation("invalid_rpo", "RPO exceeds PostgreSQL BIGINT"))?,
        )
        .bind(enum_value(&report.status)?)
        .bind(json_value(&report.findings)?)
        .bind(report.observed_at)
        .bind(report.expires_at)
        .bind(report_json)
        .bind(report.execution_eligible)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn create_postmortem(&self, draft: &PostmortemDraft) -> Result<bool, ControlPlaneError> {
        let result = sqlx::query(
            "INSERT INTO postmortems (
                id, tenant_id, cluster_id, incident_id, status, current_revision,
                confirmed_by, confirmed_at, published_knowledge_item_id,
                created_by, created_at, updated_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
             ON CONFLICT (tenant_id, incident_id) DO NOTHING",
        )
        .bind(draft.id.as_uuid())
        .bind(draft.tenant_id.as_uuid())
        .bind(draft.cluster_id.as_uuid())
        .bind(draft.incident_id.as_uuid())
        .bind(enum_value(&draft.status)?)
        .bind(i32::try_from(draft.current_revision).map_err(|_| {
            ControlPlaneError::validation("invalid_revision", "postmortem revision exceeds PostgreSQL INTEGER")
        })?)
        .bind(&draft.confirmed_by)
        .bind(draft.confirmed_at)
        .bind(draft.published_knowledge_item_id.map(|id| id.as_uuid()))
        .bind(&draft.created_by)
        .bind(draft.created_at)
        .bind(draft.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn append_postmortem_revision(&self, revision: &PostmortemRevision) -> Result<(), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let current_revision: i32 =
            sqlx::query_scalar("SELECT current_revision FROM postmortems WHERE id = $1 FOR UPDATE")
                .bind(revision.postmortem_id.as_uuid())
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(ControlPlaneError::NotFound)?;
        let expected = current_revision
            .checked_add(1)
            .ok_or_else(|| ControlPlaneError::conflict("postmortem revision counter cannot be advanced"))?;
        let actual = i32::try_from(revision.revision).map_err(|_| {
            ControlPlaneError::validation("invalid_revision", "postmortem revision exceeds PostgreSQL INTEGER")
        })?;
        if actual != expected {
            return Err(ControlPlaneError::conflict(format!(
                "postmortem revision must be {expected}, received {actual}"
            )));
        }

        insert_postmortem_revision(&mut transaction, revision).await?;
        let next_status = if revision.human_confirmed {
            PostmortemStatus::Confirmed
        } else {
            PostmortemStatus::InReview
        };
        sqlx::query(
            "UPDATE postmortems
             SET current_revision = $2,
                 status = $3,
                 confirmed_by = CASE WHEN $4 THEN $5 ELSE confirmed_by END,
                 confirmed_at = CASE WHEN $4 THEN $6 ELSE confirmed_at END,
                 updated_at = $6
             WHERE id = $1",
        )
        .bind(revision.postmortem_id.as_uuid())
        .bind(actual)
        .bind(enum_value(&next_status)?)
        .bind(revision.human_confirmed)
        .bind(&revision.edited_by)
        .bind(revision.created_at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn get_postmortem(
        &self,
        id: rocketmq_sre_contracts::PostmortemId,
    ) -> Result<PostmortemDraft, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, incident_id, status, current_revision,
                    confirmed_by, confirmed_at, published_knowledge_item_id,
                    created_by, created_at, updated_at
             FROM postmortems WHERE id = $1",
        )
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        postmortem_from_row(&row)
    }

    async fn list_postmortem_revisions(
        &self,
        id: rocketmq_sre_contracts::PostmortemId,
    ) -> Result<Vec<PostmortemRevision>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT id, postmortem_id, revision, summary, impact, detection,
                    timeline, root_causes, contributing_factors, recovery,
                    effective_actions, ineffective_actions, evidence_ids,
                    model_invocation_id, edited_by, human_confirmed, created_at
             FROM postmortem_revisions
             WHERE postmortem_id = $1
             ORDER BY revision ASC",
        )
        .bind(id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(postmortem_revision_from_row).collect()
    }

    async fn upsert_action_item(&self, item: &ActionItem) -> Result<(), ControlPlaneError> {
        sqlx::query(
            "INSERT INTO action_items (
                id, tenant_id, cluster_id, postmortem_id, incident_id, title,
                owner_name, due_at, status, verification, evidence_ids,
                execution_journal, created_at, updated_at, completed_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
             )
             ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                owner_name = EXCLUDED.owner_name,
                due_at = EXCLUDED.due_at,
                status = EXCLUDED.status,
                verification = EXCLUDED.verification,
                evidence_ids = EXCLUDED.evidence_ids,
                execution_journal = EXCLUDED.execution_journal,
                updated_at = EXCLUDED.updated_at,
                completed_at = EXCLUDED.completed_at",
        )
        .bind(item.id.as_uuid())
        .bind(item.tenant_id.as_uuid())
        .bind(item.cluster_id.as_uuid())
        .bind(item.postmortem_id.as_uuid())
        .bind(item.incident_id.as_uuid())
        .bind(&item.title)
        .bind(&item.owner)
        .bind(item.due_at)
        .bind(enum_value(&item.status)?)
        .bind(&item.verification)
        .bind(item.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
        .bind(&item.execution_journal)
        .bind(item.created_at)
        .bind(item.updated_at)
        .bind(item.completed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

async fn insert_postmortem_revision(
    transaction: &mut Transaction<'_, Postgres>,
    revision: &PostmortemRevision,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO postmortem_revisions (
            id, postmortem_id, revision, summary, impact, detection, timeline,
            root_causes, contributing_factors, recovery, effective_actions,
            ineffective_actions, evidence_ids, model_invocation_id, edited_by,
            human_confirmed, created_at
         ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17
         )",
    )
    .bind(revision.id.as_uuid())
    .bind(revision.postmortem_id.as_uuid())
    .bind(i32::try_from(revision.revision).map_err(|_| {
        ControlPlaneError::validation("invalid_revision", "postmortem revision exceeds PostgreSQL INTEGER")
    })?)
    .bind(&revision.summary)
    .bind(&revision.impact)
    .bind(&revision.detection)
    .bind(&revision.timeline)
    .bind(json_value(&revision.root_causes)?)
    .bind(json_value(&revision.contributing_factors)?)
    .bind(&revision.recovery)
    .bind(json_value(&revision.effective_actions)?)
    .bind(json_value(&revision.ineffective_actions)?)
    .bind(revision.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
    .bind(revision.model_invocation_id.map(|id| id.as_uuid()))
    .bind(&revision.edited_by)
    .bind(revision.human_confirmed)
    .bind(revision.created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn postmortem_from_row(row: &sqlx::postgres::PgRow) -> Result<PostmortemDraft, ControlPlaneError> {
    let current_revision: i32 = row.try_get("current_revision")?;
    Ok(PostmortemDraft {
        id: rocketmq_sre_contracts::PostmortemId::from_uuid(row.try_get("id")?),
        tenant_id: rocketmq_sre_contracts::TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: rocketmq_sre_contracts::ClusterId::from_uuid(row.try_get("cluster_id")?),
        incident_id: rocketmq_sre_contracts::IncidentId::from_uuid(row.try_get("incident_id")?),
        status: enum_from_column(row.try_get("status")?, "postmortem status")?,
        current_revision: u32::try_from(current_revision)
            .map_err(|_| ControlPlaneError::configuration("stored postmortem revision is negative"))?,
        confirmed_by: row.try_get("confirmed_by")?,
        confirmed_at: row.try_get("confirmed_at")?,
        published_knowledge_item_id: row
            .try_get::<Option<Uuid>, _>("published_knowledge_item_id")?
            .map(rocketmq_sre_contracts::KnowledgeItemId::from_uuid),
        created_by: row.try_get("created_by")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn postmortem_revision_from_row(row: &sqlx::postgres::PgRow) -> Result<PostmortemRevision, ControlPlaneError> {
    let revision: i32 = row.try_get("revision")?;
    Ok(PostmortemRevision {
        id: rocketmq_sre_contracts::PostmortemRevisionId::from_uuid(row.try_get("id")?),
        postmortem_id: rocketmq_sre_contracts::PostmortemId::from_uuid(row.try_get("postmortem_id")?),
        revision: u32::try_from(revision)
            .map_err(|_| ControlPlaneError::configuration("stored postmortem revision is negative"))?,
        summary: row.try_get("summary")?,
        impact: row.try_get("impact")?,
        detection: row.try_get("detection")?,
        timeline: row.try_get("timeline")?,
        root_causes: value_from_column(row.try_get("root_causes")?, "root causes")?,
        contributing_factors: value_from_column(row.try_get("contributing_factors")?, "contributing factors")?,
        recovery: row.try_get("recovery")?,
        effective_actions: value_from_column(row.try_get("effective_actions")?, "effective actions")?,
        ineffective_actions: value_from_column(row.try_get("ineffective_actions")?, "ineffective actions")?,
        evidence_ids: row
            .try_get::<Vec<Uuid>, _>("evidence_ids")?
            .into_iter()
            .map(rocketmq_sre_contracts::EvidenceId::from_uuid)
            .collect(),
        model_invocation_id: row
            .try_get::<Option<Uuid>, _>("model_invocation_id")?
            .map(rocketmq_sre_contracts::ModelInvocationId::from_uuid),
        edited_by: row.try_get("edited_by")?,
        human_confirmed: row.try_get("human_confirmed")?,
        created_at: row.try_get("created_at")?,
    })
}

fn enum_value<T: Serialize>(value: &T) -> Result<String, ControlPlaneError> {
    match serde_json::to_value(value)
        .map_err(|error| ControlPlaneError::configuration(format!("Phase 2 enum serialization failed: {error}")))?
    {
        Value::String(value) => Ok(value),
        _ => Err(ControlPlaneError::configuration(
            "Phase 2 enum did not serialize as a string",
        )),
    }
}

fn enum_from_column<T: DeserializeOwned>(value: String, field: &'static str) -> Result<T, ControlPlaneError> {
    serde_json::from_value(Value::String(value))
        .map_err(|error| ControlPlaneError::configuration(format!("stored {field} is invalid: {error}")))
}

fn json_value<T: Serialize>(value: &T) -> Result<Value, ControlPlaneError> {
    serde_json::to_value(value)
        .map_err(|error| ControlPlaneError::configuration(format!("Phase 2 JSON serialization failed: {error}")))
}

fn value_from_column<T: DeserializeOwned>(value: Value, field: &'static str) -> Result<T, ControlPlaneError> {
    serde_json::from_value(value)
        .map_err(|error| ControlPlaneError::configuration(format!("stored {field} is invalid: {error}")))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::Utc;
    use rocketmq_sre_contracts::AlertEventId;
    use rocketmq_sre_contracts::AlertSeverity;
    use rocketmq_sre_contracts::AlertSource;
    use rocketmq_sre_contracts::AlertStatus;
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::CorrelationKey;
    use rocketmq_sre_contracts::IncidentId;
    use rocketmq_sre_contracts::PostmortemId;
    use rocketmq_sre_contracts::ResourceKind;
    use rocketmq_sre_contracts::ResourceRef;
    use rocketmq_sre_contracts::SymptomFamily;
    use rocketmq_sre_contracts::TenantId;

    use super::*;

    #[test]
    fn enum_encoding_matches_database_constraints() {
        assert_eq!(
            enum_value(&PostmortemStatus::InReview).expect("enum should encode"),
            "in_review"
        );
    }

    #[test]
    fn postmortem_head_defaults_to_non_published_state() {
        let now = Utc::now();
        let draft = PostmortemDraft {
            id: PostmortemId::new(),
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            incident_id: IncidentId::new(),
            status: PostmortemStatus::Draft,
            current_revision: 0,
            confirmed_by: None,
            confirmed_at: None,
            published_knowledge_item_id: None,
            created_by: "operator".into(),
            created_at: now,
            updated_at: now,
        };
        assert_eq!(draft.status, PostmortemStatus::Draft);
        assert!(draft.published_knowledge_item_id.is_none());
    }

    #[tokio::test]
    #[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to an isolated PostgreSQL database"]
    async fn alert_occurrence_retries_do_not_inflate_the_aggregate_count() {
        let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
        let repository = PostgresRepository::connect(&database_url, 2)
            .await
            .expect("database and migrations");
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        sqlx::query(
            "INSERT INTO clusters (
                id, tenant_id, external_cluster_key, environment, region,
                rocketmq_version, deployment_mode, owner_name,
                requested_access_profile, effective_access_profile, onboarding_state
             ) VALUES (
                $1, $2, $3, 'test', 'local', 'test', 'test', 'phase2-alert-test',
                'read_only', 'read_only', 'ready_read_only'
             )",
        )
        .bind(cluster_id.as_uuid())
        .bind(tenant_id.to_string())
        .bind(format!("phase2-alert-{cluster_id}"))
        .execute(&repository.pool)
        .await
        .expect("test cluster");

        let now = Utc::now();
        let mut event = AlertEvent {
            id: AlertEventId::new(),
            tenant_id,
            cluster_id,
            source: AlertSource::Alertmanager,
            source_event_id: "alert-occurrence-idempotency".into(),
            fingerprint: "sha256:alert-occurrence-idempotency".into(),
            correlation_key: CorrelationKey {
                tenant_id,
                cluster_id,
                resource_kind: ResourceKind::Broker,
                resource_key: "broker-a".into(),
                symptom_family: SymptomFamily::new("broker_unavailable"),
                window_start: now,
                window_seconds: 300,
            },
            affected_resource: ResourceRef {
                kind: ResourceKind::Broker,
                key: "broker-a".into(),
                display_name: Some("Broker A".into()),
            },
            symptom_family: SymptomFamily::new("broker_unavailable"),
            severity: AlertSeverity::Critical,
            status: AlertStatus::Firing,
            summary: "broker readiness failed".into(),
            labels: BTreeMap::new(),
            evidence_ids: Vec::new(),
            occurrence_count: 1,
            sequence: 1,
            occurred_at: now,
            received_at: now,
        };

        repository.store_alert(&event).await.expect("first occurrence");
        repository.store_alert(&event).await.expect("idempotent retry");
        event.sequence = 2;
        repository.store_alert(&event).await.expect("second occurrence");

        let (occurrence_count, last_sequence): (i32, i64) =
            sqlx::query_as("SELECT occurrence_count, last_sequence FROM alert_events WHERE id = $1")
                .bind(event.id.as_uuid())
                .fetch_one(&repository.pool)
                .await
                .expect("stored alert aggregate");
        assert_eq!(occurrence_count, 2);
        assert_eq!(last_sequence, 2);
    }
}
