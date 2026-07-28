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

use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::ApprovalRecord;
use rocketmq_sre_contracts::AuditEvent;
use rocketmq_sre_contracts::AuditEventId;
use rocketmq_sre_contracts::AuditEventKind;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::IntegrationAdapterKind;
use rocketmq_sre_contracts::IntegrationDelivery;
use rocketmq_sre_contracts::IntegrationDeliveryStatus;
use rocketmq_sre_contracts::IntegrationTarget;
use rocketmq_sre_contracts::IntegrationTargetId;
use rocketmq_sre_contracts::NotificationDeliveryId;
use rocketmq_sre_contracts::PlanStatus;
use rocketmq_sre_contracts::TenantId;
use serde_json::json;
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use super::super::model::AdapterDeliveryReceipt;
use super::super::model::ExternalApprovalView;
use super::super::model::IntegrationDeliveryClaim;
use super::super::model::IntegrationTargetView;
use super::support::adapter_kind_name;
use super::support::from_json;
use super::support::insert_audit;
use super::support::integration_event_name;
use super::support::json_value;
use super::support::parse_adapter_kind;
use super::support::parse_delivery_status;
use super::support::parse_integration_event;
use crate::ControlPlaneError;
use crate::PostgresRepository;

const MAX_DELIVERY_ATTEMPTS: u16 = 6;

impl PostgresRepository {
    pub(in crate::release_management) async fn insert_integration_target(
        &self,
        view: &IntegrationTargetView,
        audit: &AuditEvent,
    ) -> Result<(), ControlPlaneError> {
        validate_notification_target(&self.pool, view).await?;
        let target = &view.target;
        let outbound_events = target
            .outbound_events
            .iter()
            .map(|event| integration_event_name(*event))
            .collect::<Vec<_>>();
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO integration_targets (
                id, tenant_id, cluster_id, descriptor_id, descriptor_version,
                name, adapter_kind, endpoint, secret_reference,
                notification_target_id, enabled, inbound_approval,
                outbound_events, created_at, updated_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10, $11, $12,
                $13, $14, $15
             )",
        )
        .bind(target.id.as_uuid())
        .bind(target.tenant_id.as_uuid())
        .bind(target.cluster_id.map(ClusterId::as_uuid))
        .bind(&target.descriptor_id)
        .bind(&target.descriptor_version)
        .bind(&target.name)
        .bind(adapter_kind_name(target.adapter_kind))
        .bind(&target.endpoint)
        .bind(&target.secret_reference)
        .bind(view.notification_target_id.map(|id| id.as_uuid()))
        .bind(target.enabled)
        .bind(target.inbound_approval)
        .bind(outbound_events)
        .bind(target.created_at)
        .bind(target.updated_at)
        .execute(&mut *transaction)
        .await
        .map_err(map_target_insert_error)?;
        insert_audit(&mut transaction, audit).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(in crate::release_management) async fn integration_target(
        &self,
        tenant_id: TenantId,
        id: IntegrationTargetId,
    ) -> Result<IntegrationTargetView, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT *
             FROM integration_targets
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant_id.as_uuid())
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        integration_target_from_row(&row)
    }

    pub(in crate::release_management) async fn integration_targets(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        adapter_kind: Option<IntegrationAdapterKind>,
        enabled: Option<bool>,
        limit: i64,
    ) -> Result<Vec<IntegrationTargetView>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT *
             FROM integration_targets
             WHERE tenant_id = $1 AND cluster_id = $2
               AND ($3::text IS NULL OR adapter_kind = $3)
               AND ($4::boolean IS NULL OR enabled = $4)
             ORDER BY name, id
             LIMIT $5",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(adapter_kind.map(adapter_kind_name))
        .bind(enabled)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(integration_target_from_row).collect()
    }

    pub(in crate::release_management) async fn set_integration_target_state(
        &self,
        target: &IntegrationTargetView,
        enabled: bool,
        updated_at: chrono::DateTime<Utc>,
        audit: &AuditEvent,
    ) -> Result<IntegrationTargetView, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let updated = sqlx::query(
            "UPDATE integration_targets
             SET enabled = $4, updated_at = $5
             WHERE id = $1 AND tenant_id = $2 AND updated_at = $3",
        )
        .bind(target.target.id.as_uuid())
        .bind(target.target.tenant_id.as_uuid())
        .bind(target.target.updated_at)
        .bind(enabled)
        .bind(updated_at)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(ControlPlaneError::conflict_code(
                "integration_target_state_changed",
                "integration target was changed by another operator",
            ));
        }
        insert_audit(&mut transaction, audit).await?;
        transaction.commit().await?;
        self.integration_target(target.target.tenant_id, target.target.id).await
    }

    pub(in crate::release_management) async fn integration_deliveries(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        target_id: Option<IntegrationTargetId>,
        limit: i64,
    ) -> Result<Vec<IntegrationDelivery>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT delivery_snapshot, status, attempt_count, next_attempt_at,
                    last_error_code, delivered_at
             FROM integration_outbox
             WHERE tenant_id = $1 AND cluster_id = $2
               AND ($3::uuid IS NULL OR target_id = $3)
             ORDER BY created_at DESC, id
             LIMIT $4",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(target_id.map(IntegrationTargetId::as_uuid))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(integration_delivery_from_row).collect()
    }

    pub(in crate::release_management) async fn claim_integration_deliveries(
        &self,
        limit: u16,
    ) -> Result<Vec<IntegrationDeliveryClaim>, ControlPlaneError> {
        let claim_token = Uuid::new_v4();
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "UPDATE integration_outbox
             SET status = 'retry_scheduled', claim_token = NULL, claimed_at = NULL,
                 next_attempt_at = NOW()
             WHERE status = 'delivering' AND claimed_at < NOW() - INTERVAL '2 minutes'",
        )
        .execute(&mut *transaction)
        .await?;
        let rows = sqlx::query(
            "WITH candidates AS (
                SELECT outbox.id
                FROM integration_outbox outbox
                JOIN integration_targets target ON target.id = outbox.target_id
                WHERE outbox.status IN ('pending', 'retry_scheduled')
                  AND target.enabled
                  AND target.adapter_kind IN ('mock_itsm', 'signed_webhook_itsm')
                  AND COALESCE(outbox.next_attempt_at, outbox.created_at) <= NOW()
                ORDER BY outbox.created_at, outbox.id
                FOR UPDATE SKIP LOCKED
                LIMIT $1
             )
             UPDATE integration_outbox outbox
             SET status = 'delivering', claim_token = $2, claimed_at = NOW()
             FROM candidates
             WHERE outbox.id = candidates.id
             RETURNING outbox.target_id, outbox.delivery_snapshot,
                       outbox.attempt_count, outbox.next_attempt_at,
                       outbox.last_error_code, outbox.delivered_at",
        )
        .bind(i64::from(limit.min(32)))
        .bind(claim_token)
        .fetch_all(&mut *transaction)
        .await?;
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let target_id: Uuid = row.try_get("target_id")?;
            let target = sqlx::query(
                "SELECT adapter_kind, endpoint, secret_reference
                 FROM integration_targets
                 WHERE id = $1 AND enabled",
            )
            .bind(target_id)
            .fetch_one(&mut *transaction)
            .await?;
            let mut delivery: IntegrationDelivery = from_json(row.try_get("delivery_snapshot")?)?;
            delivery.status = IntegrationDeliveryStatus::Delivering;
            delivery.attempt_count = to_u16(row.try_get("attempt_count")?)?;
            delivery.next_attempt_at = row.try_get("next_attempt_at")?;
            delivery.last_error_code = row.try_get("last_error_code")?;
            delivery.delivered_at = row.try_get("delivered_at")?;
            claims.push(IntegrationDeliveryClaim {
                delivery,
                claim_token,
                adapter_kind: parse_adapter_kind(target.try_get("adapter_kind")?)?,
                endpoint: target.try_get("endpoint")?,
                secret_reference: target.try_get("secret_reference")?,
            });
        }
        transaction.commit().await?;
        Ok(claims)
    }

    pub(in crate::release_management) async fn finish_integration_delivery(
        &self,
        claim: &IntegrationDeliveryClaim,
        result: Result<AdapterDeliveryReceipt, &'static str>,
    ) -> Result<(), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let next_attempt = claim.delivery.attempt_count.saturating_add(1);
        let (status, next_attempt_at, error_code, delivered_at, receipt) = match result {
            Ok(receipt) => ("delivered", None, None, Some(Utc::now()), Some(receipt)),
            Err(code) if next_attempt < MAX_DELIVERY_ATTEMPTS => {
                let delay_seconds = 30_i64.saturating_mul(1_i64 << next_attempt.min(5));
                (
                    "retry_scheduled",
                    Some(Utc::now() + Duration::seconds(delay_seconds.min(900))),
                    Some(code),
                    None,
                    None,
                )
            }
            Err(code) => ("failed", None, Some(code), None, None),
        };
        let updated = sqlx::query(
            "UPDATE integration_outbox
             SET status = $3, attempt_count = $4, next_attempt_at = $5,
                 last_error_code = $6, delivered_at = $7,
                 claim_token = NULL, claimed_at = NULL
             WHERE id = $1 AND claim_token = $2 AND status = 'delivering'",
        )
        .bind(claim.delivery.id.as_uuid())
        .bind(claim.claim_token)
        .bind(status)
        .bind(i32::from(next_attempt))
        .bind(next_attempt_at)
        .bind(error_code)
        .bind(delivered_at)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            transaction.rollback().await?;
            return Ok(());
        }
        if let Some(ticket_key) = receipt.and_then(|receipt| receipt.external_ticket_key) {
            persist_ticket_link(&mut transaction, &claim.delivery, &ticket_key).await?;
        }
        if matches!(status, "delivered" | "failed") {
            let audit = AuditEvent {
                id: AuditEventId::new(),
                tenant_id: claim.delivery.tenant_id,
                cluster_id: claim.delivery.cluster_id,
                correlation_id: CorrelationId::from_uuid(claim.delivery.id.as_uuid()),
                event_kind: AuditEventKind::IntegrationDeliveryCompleted,
                actor_subject: "system:integration-outbox".to_owned(),
                actor_role: "integration_worker".to_owned(),
                resource_kind: "integration_delivery".to_owned(),
                resource_id: claim.delivery.id.to_string(),
                reason_code: if status == "delivered" {
                    "IntegrationDeliverySucceeded"
                } else {
                    "IntegrationDeliveryFailed"
                }
                .to_owned(),
                details: json!({
                    "target_id": claim.delivery.target_id,
                    "event_kind": claim.delivery.event_kind,
                    "status": status,
                    "attempt_count": next_attempt,
                    "error_code": error_code,
                }),
                occurred_at: Utc::now(),
            };
            insert_audit(&mut transaction, &audit).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub(in crate::release_management) async fn external_approval_result(
        &self,
        tenant_id: TenantId,
        target_id: IntegrationTargetId,
        external_event_id: &str,
    ) -> Result<Option<ExternalApprovalView>, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT approval.approval_snapshot, plan.status
             FROM external_approval_events event
             JOIN approvals approval ON approval.id = event.approval_id
             JOIN action_plans plan ON plan.id = event.plan_id
             JOIN integration_targets target ON target.id = event.target_id
             WHERE event.target_id = $1 AND event.external_event_id = $2
               AND target.tenant_id = $3",
        )
        .bind(target_id.as_uuid())
        .bind(external_event_id)
        .bind(tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            Ok(ExternalApprovalView {
                schema_version: "rocketmq-sre.external-approval-result.v1",
                duplicate: true,
                approval: from_json::<ApprovalRecord>(row.try_get("approval_snapshot")?)?,
                plan_status: parse_plan_status(row.try_get("status")?)?,
            })
        })
        .transpose()
    }
}

pub(super) async fn enqueue_delivery_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    target: &IntegrationTargetView,
    delivery: &IntegrationDelivery,
    audit: &AuditEvent,
) -> Result<bool, ControlPlaneError> {
    let queued = match target.target.adapter_kind {
        IntegrationAdapterKind::MockItsm | IntegrationAdapterKind::SignedWebhookItsm => {
            sqlx::query(
                "INSERT INTO integration_outbox (
                    id, target_id, descriptor_id, descriptor_version,
                    tenant_id, cluster_id, incident_id, plan_id, release_id,
                    event_kind, idempotency_key, status, sanitized_summary,
                    deep_link, delivery_snapshot, attempt_count,
                    next_attempt_at, created_at
                 ) VALUES (
                    $1, $2, $3, $4,
                    $5, $6, $7, $8, $9,
                    $10, $11, 'pending', $12,
                    $13, $14, 0,
                    NOW(), $15
                 )
                 ON CONFLICT (target_id, idempotency_key) DO NOTHING",
            )
            .bind(delivery.id.as_uuid())
            .bind(delivery.target_id.as_uuid())
            .bind(&delivery.descriptor_id)
            .bind(&delivery.descriptor_version)
            .bind(delivery.tenant_id.as_uuid())
            .bind(delivery.cluster_id.as_uuid())
            .bind(delivery.incident_id.as_uuid())
            .bind(delivery.plan_id.map(|id| id.as_uuid()))
            .bind(delivery.release_id.map(|id| id.as_uuid()))
            .bind(integration_event_name(delivery.event_kind))
            .bind(&delivery.idempotency_key)
            .bind(&delivery.sanitized_summary)
            .bind(&delivery.deep_link)
            .bind(json_value(delivery)?)
            .bind(delivery.created_at)
            .execute(&mut **transaction)
            .await?
            .rows_affected()
                == 1
        }
        IntegrationAdapterKind::ChatOpsWebhook | IntegrationAdapterKind::Pager | IntegrationAdapterKind::Email => {
            let notification_target_id = target.notification_target_id.ok_or_else(|| {
                ControlPlaneError::configuration(
                    "notification-backed integration does not reference a notification target",
                )
            })?;
            let delivery_key = format!("integration:{}:{}", delivery.target_id, delivery.idempotency_key);
            sqlx::query(
                "INSERT INTO notification_outbox (
                    id, target_id, tenant_id, cluster_id, incident_id,
                    delivery_key, status, sanitized_summary, deep_link,
                    attempt_count, next_attempt_at, created_at
                 ) VALUES (
                    $1, $2, $3, $4, $5,
                    $6, 'pending', $7, $8,
                    0, NOW(), $9
                 )
                 ON CONFLICT (tenant_id, delivery_key) DO NOTHING",
            )
            .bind(NotificationDeliveryId::from_uuid(delivery.id.as_uuid()).as_uuid())
            .bind(notification_target_id.as_uuid())
            .bind(delivery.tenant_id.as_uuid())
            .bind(delivery.cluster_id.as_uuid())
            .bind(delivery.incident_id.as_uuid())
            .bind(delivery_key)
            .bind(&delivery.sanitized_summary)
            .bind(&delivery.deep_link)
            .bind(delivery.created_at)
            .execute(&mut **transaction)
            .await?
            .rows_affected()
                == 1
        }
    };
    if queued {
        insert_audit(transaction, audit).await?;
    }
    Ok(queued)
}

async fn validate_notification_target(
    pool: &sqlx::PgPool,
    view: &IntegrationTargetView,
) -> Result<(), ControlPlaneError> {
    let Some(notification_target_id) = view.notification_target_id else {
        if matches!(
            view.target.adapter_kind,
            IntegrationAdapterKind::ChatOpsWebhook | IntegrationAdapterKind::Pager | IntegrationAdapterKind::Email
        ) {
            return Err(ControlPlaneError::validation(
                "integration_target_invalid",
                "notification-backed integration requires a notification target",
            ));
        }
        return Ok(());
    };
    let row = sqlx::query(
        "SELECT tenant_id, cluster_id, channel, endpoint, secret_reference, enabled
         FROM notification_targets
         WHERE id = $1",
    )
    .bind(notification_target_id.as_uuid())
    .fetch_optional(pool)
    .await?
    .ok_or(ControlPlaneError::NotFound)?;
    let notification_cluster: Option<Uuid> = row.try_get("cluster_id")?;
    let target_cluster = view.target.cluster_id.map(ClusterId::as_uuid);
    let expected_channel = match view.target.adapter_kind {
        IntegrationAdapterKind::ChatOpsWebhook => "signed_webhook",
        IntegrationAdapterKind::Pager => "pager",
        IntegrationAdapterKind::Email => "email",
        IntegrationAdapterKind::MockItsm | IntegrationAdapterKind::SignedWebhookItsm => {
            return Err(ControlPlaneError::validation(
                "integration_target_invalid",
                "ITSM adapters cannot reference the notification outbox",
            ));
        }
    };
    if row.try_get::<Uuid, _>("tenant_id")? != view.target.tenant_id.as_uuid()
        || notification_cluster.is_some_and(|cluster| Some(cluster) != target_cluster)
        || row.try_get::<String, _>("channel")? != expected_channel
        || row.try_get::<String, _>("endpoint")? != view.target.endpoint
        || row.try_get::<Option<String>, _>("secret_reference")? != view.target.secret_reference
        || !row.try_get::<bool, _>("enabled")?
    {
        return Err(ControlPlaneError::forbidden(
            "integration_scope_mismatch",
            "notification target scope, channel, endpoint, secret reference, or state is incompatible",
        ));
    }
    Ok(())
}

async fn persist_ticket_link(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    delivery: &IntegrationDelivery,
    ticket_key: &str,
) -> Result<(), ControlPlaneError> {
    let plan_id = delivery.plan_id.ok_or_else(|| {
        ControlPlaneError::validation(
            "integration_delivery_invalid",
            "ITSM ticket delivery must reference an action plan",
        )
    })?;
    sqlx::query(
        "INSERT INTO itsm_ticket_links (
            target_id, external_ticket_key, incident_id, plan_id, plan_hash,
            approval_status, sre_url, sanitized_summary, last_synced_at
         )
         SELECT $1, $2, $3, plan.id, plan.plan_hash,
                plan.status, $5, $6, NOW()
         FROM action_plans plan
         WHERE plan.id = $4
         ON CONFLICT (target_id, plan_id)
         DO UPDATE SET
            approval_status = EXCLUDED.approval_status,
            sre_url = EXCLUDED.sre_url,
            sanitized_summary = EXCLUDED.sanitized_summary,
            last_synced_at = EXCLUDED.last_synced_at",
    )
    .bind(delivery.target_id.as_uuid())
    .bind(ticket_key)
    .bind(delivery.incident_id.as_uuid())
    .bind(plan_id.as_uuid())
    .bind(&delivery.deep_link)
    .bind(&delivery.sanitized_summary)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn integration_target_from_row(row: &PgRow) -> Result<IntegrationTargetView, ControlPlaneError> {
    let outbound_events = row
        .try_get::<Vec<String>, _>("outbound_events")?
        .iter()
        .map(|value| parse_integration_event(value))
        .collect::<Result<_, _>>()?;
    Ok(IntegrationTargetView {
        target: IntegrationTarget {
            id: IntegrationTargetId::from_uuid(row.try_get("id")?),
            tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
            cluster_id: row.try_get::<Option<Uuid>, _>("cluster_id")?.map(ClusterId::from_uuid),
            descriptor_id: row.try_get("descriptor_id")?,
            descriptor_version: row.try_get("descriptor_version")?,
            name: row.try_get("name")?,
            adapter_kind: parse_adapter_kind(row.try_get("adapter_kind")?)?,
            endpoint: row.try_get("endpoint")?,
            secret_reference: row.try_get("secret_reference")?,
            enabled: row.try_get("enabled")?,
            inbound_approval: row.try_get("inbound_approval")?,
            outbound_events,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        },
        notification_target_id: row
            .try_get::<Option<Uuid>, _>("notification_target_id")?
            .map(rocketmq_sre_contracts::NotificationTargetId::from_uuid),
    })
}

fn integration_delivery_from_row(row: &PgRow) -> Result<IntegrationDelivery, ControlPlaneError> {
    let mut delivery: IntegrationDelivery = from_json(row.try_get("delivery_snapshot")?)?;
    delivery.status = parse_delivery_status(row.try_get("status")?)?;
    delivery.attempt_count = to_u16(row.try_get("attempt_count")?)?;
    delivery.next_attempt_at = row.try_get("next_attempt_at")?;
    delivery.last_error_code = row.try_get("last_error_code")?;
    delivery.delivered_at = row.try_get("delivered_at")?;
    Ok(delivery)
}

fn parse_plan_status(value: &str) -> Result<PlanStatus, ControlPlaneError> {
    match value {
        "draft" => Ok(PlanStatus::Draft),
        "needs_critic" => Ok(PlanStatus::NeedsCritic),
        "ready_for_approval" => Ok(PlanStatus::ReadyForApproval),
        "in_review" => Ok(PlanStatus::InReview),
        "approved" => Ok(PlanStatus::Approved),
        "rejected" => Ok(PlanStatus::Rejected),
        "expired" => Ok(PlanStatus::Expired),
        "superseded" => Ok(PlanStatus::Superseded),
        _ => Err(ControlPlaneError::validation(
            "invalid_persisted_state",
            "persisted action plan status is incompatible",
        )),
    }
}

fn to_u16(value: i32) -> Result<u16, ControlPlaneError> {
    u16::try_from(value).map_err(|_| {
        ControlPlaneError::validation(
            "invalid_persisted_state",
            "persisted delivery attempt count exceeds the contract bound",
        )
    })
}

fn map_target_insert_error(error: sqlx::Error) -> ControlPlaneError {
    if let sqlx::Error::Database(database) = &error
        && database.is_unique_violation()
    {
        return ControlPlaneError::conflict_code(
            "integration_target_exists",
            "an integration target with this identity or scoped name already exists",
        );
    }
    ControlPlaneError::Database(error)
}
