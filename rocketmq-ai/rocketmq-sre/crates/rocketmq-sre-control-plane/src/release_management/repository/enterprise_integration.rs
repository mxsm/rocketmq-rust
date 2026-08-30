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

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ENTERPRISE_INTEGRATION_EVENT_SCHEMA_VERSION;
use rocketmq_sre_contracts::EnterpriseIntegrationEvent;
use rocketmq_sre_contracts::EnterpriseIntegrationEventId;
use rocketmq_sre_contracts::EnterpriseIntegrationEventKind;
use rocketmq_sre_contracts::EnterpriseIntegrationPayload;
use rocketmq_sre_contracts::IntegrationHealth;
use rocketmq_sre_contracts::IntegrationHealthStatus;
use rocketmq_sre_contracts::IntegrationTargetId;
use rocketmq_sre_contracts::TenantId;
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::ControlPlaneError;
use crate::PostgresRepository;

impl PostgresRepository {
    pub(in crate::release_management) async fn store_enterprise_integration_event(
        &self,
        event: &EnterpriseIntegrationEvent,
        nonce: &str,
    ) -> Result<(EnterpriseIntegrationEvent, bool, Option<Uuid>), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let existing = sqlx::query(
            "SELECT id, target_id, tenant_id, cluster_id, event_kind,
                    external_event_id, source_version, payload_digest, payload,
                    signature_verified, occurred_at, received_at, followup_id
             FROM enterprise_integration_events
             WHERE target_id = $1 AND external_event_id = $2",
        )
        .bind(event.target_id.as_uuid())
        .bind(&event.external_event_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = existing {
            let persisted = enterprise_event_from_row(&row)?;
            if persisted.payload_digest != event.payload_digest
                || persisted.event_kind != event.event_kind
                || persisted.source_version != event.source_version
            {
                return Err(ControlPlaneError::conflict_code(
                    "integration_idempotency_conflict",
                    "external integration event identifier was reused with different content",
                ));
            }
            let followup_id = row.try_get("followup_id")?;
            transaction.commit().await?;
            return Ok((persisted, true, followup_id));
        }
        let replayed = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1
                FROM enterprise_integration_events
                WHERE target_id = $1 AND nonce = $2
             )",
        )
        .bind(event.target_id.as_uuid())
        .bind(nonce)
        .fetch_one(&mut *transaction)
        .await?;
        if replayed {
            return Err(ControlPlaneError::conflict_code(
                "integration_replay_detected",
                "integration nonce has already been consumed",
            ));
        }
        sqlx::query(
            "INSERT INTO enterprise_integration_events (
                id, target_id, tenant_id, cluster_id, event_kind,
                external_event_id, source_version, nonce, payload_digest,
                payload, signature_verified, occurred_at, received_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10, $11, $12, $13
             )",
        )
        .bind(event.id.as_uuid())
        .bind(event.target_id.as_uuid())
        .bind(event.tenant_id.as_uuid())
        .bind(event.cluster_id.as_uuid())
        .bind(event_kind_name(event.event_kind))
        .bind(&event.external_event_id)
        .bind(&event.source_version)
        .bind(nonce)
        .bind(&event.payload_digest)
        .bind(
            serde_json::to_value(&event.payload).map_err(|_| {
                ControlPlaneError::validation("integration_payload_invalid", "payload cannot be encoded")
            })?,
        )
        .bind(event.signature_verified)
        .bind(event.occurred_at)
        .bind(event.received_at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok((event.clone(), false, None))
    }

    pub(in crate::release_management) async fn record_enterprise_followup(
        &self,
        tenant_id: TenantId,
        event_id: EnterpriseIntegrationEventId,
        followup_id: Uuid,
    ) -> Result<(), ControlPlaneError> {
        let updated = sqlx::query(
            "UPDATE enterprise_integration_events
             SET followup_kind = 'upgrade_readiness', followup_id = $3
             WHERE id = $1 AND tenant_id = $2 AND followup_id IS NULL",
        )
        .bind(event_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(followup_id)
        .execute(&self.pool)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(ControlPlaneError::conflict_code(
                "integration_followup_conflict",
                "integration event follow-up was already recorded",
            ));
        }
        Ok(())
    }

    pub(in crate::release_management) async fn recent_enterprise_event_count(
        &self,
        target_id: IntegrationTargetId,
    ) -> Result<u64, ControlPlaneError> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM enterprise_integration_events
             WHERE target_id = $1
               AND received_at >= NOW() - INTERVAL '1 minute'",
        )
        .bind(target_id.as_uuid())
        .fetch_one(&self.pool)
        .await?;
        u64::try_from(count).map_err(|_| ControlPlaneError::configuration("integration event count is invalid"))
    }

    pub(in crate::release_management) async fn enterprise_events(
        &self,
        tenant_id: TenantId,
        target_id: IntegrationTargetId,
        event_kind: Option<EnterpriseIntegrationEventKind>,
        limit: i64,
    ) -> Result<Vec<EnterpriseIntegrationEvent>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT event.id, event.target_id, event.tenant_id, event.cluster_id,
                    event.event_kind, event.external_event_id, event.source_version,
                    event.payload_digest, event.payload, event.signature_verified,
                    event.occurred_at, event.received_at
             FROM enterprise_integration_events event
             WHERE event.tenant_id = $1 AND event.target_id = $2
               AND ($3::TEXT IS NULL OR event.event_kind = $3)
             ORDER BY event.received_at DESC, event.id
             LIMIT $4",
        )
        .bind(tenant_id.as_uuid())
        .bind(target_id.as_uuid())
        .bind(event_kind.map(event_kind_name))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(enterprise_event_from_row).collect()
    }

    pub(in crate::release_management) async fn store_integration_health(
        &self,
        health: &IntegrationHealth,
    ) -> Result<(), ControlPlaneError> {
        sqlx::query(
            "INSERT INTO integration_health_observations (
                id, target_id, health_status, config_valid, secret_available,
                endpoint_valid, last_delivery_at, last_error_code, observed_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(Uuid::new_v4())
        .bind(health.target_id.as_uuid())
        .bind(health_status_name(health.status))
        .bind(health.config_valid)
        .bind(health.secret_available)
        .bind(health.endpoint_valid)
        .bind(health.last_delivery_at)
        .bind(&health.last_error_code)
        .bind(health.observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(in crate::release_management) async fn latest_integration_delivery_health(
        &self,
        target_id: IntegrationTargetId,
    ) -> Result<(Option<chrono::DateTime<chrono::Utc>>, Option<String>), ControlPlaneError> {
        let row = sqlx::query(
            "SELECT delivered_at, last_error_code
             FROM integration_outbox
             WHERE target_id = $1
             ORDER BY created_at DESC, id DESC
             LIMIT 1",
        )
        .bind(target_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some(row) => Ok((row.try_get("delivered_at")?, row.try_get("last_error_code")?)),
            None => Ok((None, None)),
        }
    }

    pub(in crate::release_management) async fn integration_health(
        &self,
        tenant_id: TenantId,
        target_id: IntegrationTargetId,
    ) -> Result<IntegrationHealth, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT health.target_id, health.health_status, health.config_valid,
                    health.secret_available, health.endpoint_valid,
                    health.last_delivery_at, health.last_error_code,
                    health.observed_at
             FROM integration_health_observations health
             JOIN integration_targets target ON target.id = health.target_id
             WHERE target.tenant_id = $1 AND health.target_id = $2
             ORDER BY health.observed_at DESC, health.id DESC
             LIMIT 1",
        )
        .bind(tenant_id.as_uuid())
        .bind(target_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        integration_health_from_row(&row)
    }
}

fn enterprise_event_from_row(row: &PgRow) -> Result<EnterpriseIntegrationEvent, ControlPlaneError> {
    let payload = serde_json::from_value::<EnterpriseIntegrationPayload>(row.try_get("payload")?)
        .map_err(|_| ControlPlaneError::configuration("integration payload contains an invalid persisted value"))?;
    Ok(EnterpriseIntegrationEvent {
        schema_version: ENTERPRISE_INTEGRATION_EVENT_SCHEMA_VERSION.to_owned(),
        id: EnterpriseIntegrationEventId::from_uuid(row.try_get("id")?),
        target_id: IntegrationTargetId::from_uuid(row.try_get("target_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        event_kind: parse_event_kind(row.try_get("event_kind")?)?,
        external_event_id: row.try_get("external_event_id")?,
        source_version: row.try_get("source_version")?,
        payload_digest: row.try_get("payload_digest")?,
        payload,
        signature_verified: row.try_get("signature_verified")?,
        occurred_at: row.try_get("occurred_at")?,
        received_at: row.try_get("received_at")?,
    })
}

fn integration_health_from_row(row: &PgRow) -> Result<IntegrationHealth, ControlPlaneError> {
    Ok(IntegrationHealth {
        target_id: IntegrationTargetId::from_uuid(row.try_get("target_id")?),
        status: parse_health_status(row.try_get("health_status")?)?,
        config_valid: row.try_get("config_valid")?,
        secret_available: row.try_get("secret_available")?,
        endpoint_valid: row.try_get("endpoint_valid")?,
        last_delivery_at: row.try_get("last_delivery_at")?,
        last_error_code: row.try_get("last_error_code")?,
        observed_at: row.try_get("observed_at")?,
    })
}

pub(super) const fn event_kind_name(kind: EnterpriseIntegrationEventKind) -> &'static str {
    match kind {
        EnterpriseIntegrationEventKind::CmdbSnapshot => "cmdb_snapshot",
        EnterpriseIntegrationEventKind::GitOpsSnapshot => "gitops_snapshot",
        EnterpriseIntegrationEventKind::ReleaseStarted => "release_started",
        EnterpriseIntegrationEventKind::ReleaseCanary => "release_canary",
        EnterpriseIntegrationEventKind::ReleasePromoted => "release_promoted",
        EnterpriseIntegrationEventKind::ReleaseRolledBack => "release_rolled_back",
    }
}

fn parse_event_kind(value: String) -> Result<EnterpriseIntegrationEventKind, ControlPlaneError> {
    match value.as_str() {
        "cmdb_snapshot" => Ok(EnterpriseIntegrationEventKind::CmdbSnapshot),
        "gitops_snapshot" => Ok(EnterpriseIntegrationEventKind::GitOpsSnapshot),
        "release_started" => Ok(EnterpriseIntegrationEventKind::ReleaseStarted),
        "release_canary" => Ok(EnterpriseIntegrationEventKind::ReleaseCanary),
        "release_promoted" => Ok(EnterpriseIntegrationEventKind::ReleasePromoted),
        "release_rolled_back" => Ok(EnterpriseIntegrationEventKind::ReleaseRolledBack),
        _ => Err(ControlPlaneError::configuration(
            "integration event kind contains an invalid persisted value",
        )),
    }
}

fn health_status_name(status: IntegrationHealthStatus) -> &'static str {
    match status {
        IntegrationHealthStatus::Unknown => "unknown",
        IntegrationHealthStatus::Healthy => "healthy",
        IntegrationHealthStatus::Degraded => "degraded",
        IntegrationHealthStatus::Unavailable => "unavailable",
        IntegrationHealthStatus::Disabled => "disabled",
    }
}

fn parse_health_status(value: String) -> Result<IntegrationHealthStatus, ControlPlaneError> {
    match value.as_str() {
        "unknown" => Ok(IntegrationHealthStatus::Unknown),
        "healthy" => Ok(IntegrationHealthStatus::Healthy),
        "degraded" => Ok(IntegrationHealthStatus::Degraded),
        "unavailable" => Ok(IntegrationHealthStatus::Unavailable),
        "disabled" => Ok(IntegrationHealthStatus::Disabled),
        _ => Err(ControlPlaneError::configuration(
            "integration health contains an invalid persisted value",
        )),
    }
}
