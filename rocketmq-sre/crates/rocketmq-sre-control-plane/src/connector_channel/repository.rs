// Copyright 2023 The RocketMQ Rust Authors
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
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ConnectorCapabilityState;
use rocketmq_sre_contracts::ConnectorHeartbeat;
use rocketmq_sre_contracts::ConnectorQueryEnvelope;
use rocketmq_sre_contracts::ConnectorRegister;
use rocketmq_sre_contracts::ConnectorResponseEnvelope;
use rocketmq_sre_contracts::ConnectorSessionId;
use rocketmq_sre_contracts::ConnectorSourceStatus;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::TenantId;
use serde_json::json;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use sqlx::postgres::PgRow;

use super::ConnectorCommand;
use super::ConnectorPrincipal;
use super::ResponseDisposition;
use super::SessionScope;
use super::channel_schema;
use crate::ControlPlaneError;
use crate::PostgresRepository;

#[derive(Clone, Debug)]
pub(crate) struct RegistrationResult {
    pub resume_after_sequence: u64,
}

pub(crate) trait ConnectorChannelStore: Clone + Send + Sync + 'static {
    async fn register(
        &self,
        principal: &ConnectorPrincipal,
        request: &ConnectorRegister,
    ) -> Result<RegistrationResult, ControlPlaneError>;

    async fn heartbeat(
        &self,
        principal: &ConnectorPrincipal,
        request: &ConnectorHeartbeat,
    ) -> Result<SessionScope, ControlPlaneError>;

    async fn session_scope(
        &self,
        principal: &ConnectorPrincipal,
        session_id: ConnectorSessionId,
    ) -> Result<SessionScope, ControlPlaneError>;

    async fn commands(
        &self,
        scope: &SessionScope,
        after_sequence: u64,
        max_commands: usize,
    ) -> Result<Vec<ConnectorCommand>, ControlPlaneError>;

    async fn enqueue_query(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        query: EvidenceQuery,
        deadline: DateTime<Utc>,
        stale_before: DateTime<Utc>,
    ) -> Result<ConnectorCommand, ControlPlaneError>;

    async fn enqueue_cancel(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        correlation_id: CorrelationId,
    ) -> Result<ConnectorCommand, ControlPlaneError>;

    async fn append_response(
        &self,
        scope: &SessionScope,
        response: &ConnectorResponseEnvelope,
    ) -> Result<ResponseDisposition, ControlPlaneError>;

    async fn response(
        &self,
        session_id: ConnectorSessionId,
        sequence: u64,
    ) -> Result<Option<ConnectorResponseEnvelope>, ControlPlaneError>;

    async fn latest_session(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
    ) -> Result<Option<SessionScope>, ControlPlaneError>;

    async fn latest_sessions(&self, limit: usize) -> Result<Vec<SessionScope>, ControlPlaneError>;
}

#[derive(Clone, Debug)]
pub(crate) struct PostgresConnectorChannelStore {
    repository: PostgresRepository,
}

impl PostgresConnectorChannelStore {
    #[must_use]
    pub(crate) fn new(repository: PostgresRepository) -> Self {
        Self { repository }
    }
}

impl ConnectorChannelStore for PostgresConnectorChannelStore {
    async fn register(
        &self,
        principal: &ConnectorPrincipal,
        request: &ConnectorRegister,
    ) -> Result<RegistrationResult, ControlPlaneError> {
        let mut transaction = self.repository.pool.begin().await?;
        ensure_cluster_identity(&mut transaction, request.tenant_id, request.cluster_id, principal).await?;
        let now = Utc::now();
        let capability = serde_json::to_value(&request.capability).map_err(|_| {
            ControlPlaneError::validation("capability_mismatch", "connector capability cannot be serialized")
        })?;
        let existing = sqlx::query(
            "SELECT session_id, tenant_id, cluster_id, connector_subject, connector_issuer,
                    last_heartbeat_at, capability
             FROM connector_channel_sessions
             WHERE session_id = $1
             FOR UPDATE",
        )
        .bind(request.session_id.as_uuid())
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = existing {
            let scope = session_scope_from_row(&row)?;
            enforce_scope(&scope, request.tenant_id, request.cluster_id, principal)?;
            sqlx::query(
                "UPDATE connector_channel_sessions
                 SET capability = $2,
                     connector_observed_at = $3,
                     last_heartbeat_at = $4,
                     last_seen_at = $4
                 WHERE session_id = $1",
            )
            .bind(request.session_id.as_uuid())
            .bind(capability)
            .bind(request.observed_at)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        } else {
            sqlx::query(
                "INSERT INTO connector_channel_sessions (
                    session_id, tenant_id, cluster_id, connector_subject, connector_issuer,
                    capability, connector_observed_at, registered_at,
                    last_heartbeat_at, last_seen_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $8)",
            )
            .bind(request.session_id.as_uuid())
            .bind(request.tenant_id.as_uuid())
            .bind(request.cluster_id.as_uuid())
            .bind(&principal.subject)
            .bind(&principal.issuer)
            .bind(capability)
            .bind(request.observed_at)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        }
        append_source_capability_history(
            &mut transaction,
            request.session_id,
            request.tenant_id,
            request.cluster_id,
            &request.capability,
            request.observed_at,
        )
        .await?;
        let resume_after_sequence = resume_frontier(&mut transaction, request.session_id).await?;
        transaction.commit().await?;
        Ok(RegistrationResult { resume_after_sequence })
    }

    async fn heartbeat(
        &self,
        principal: &ConnectorPrincipal,
        request: &ConnectorHeartbeat,
    ) -> Result<SessionScope, ControlPlaneError> {
        let mut transaction = self.repository.pool.begin().await?;
        let scope = locked_session_scope(&mut transaction, request.session_id, principal).await?;
        enforce_scope(&scope, request.tenant_id, request.cluster_id, principal)?;
        let capability = serde_json::to_value(&request.capability).map_err(|_| {
            ControlPlaneError::validation("capability_mismatch", "connector capability cannot be serialized")
        })?;
        let now = Utc::now();
        sqlx::query(
            "UPDATE connector_channel_sessions
             SET capability = $2,
                 connector_observed_at = $3,
                 last_heartbeat_at = $4,
                 last_seen_at = $4
             WHERE session_id = $1",
        )
        .bind(request.session_id.as_uuid())
        .bind(capability)
        .bind(request.observed_at)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        append_source_capability_history(
            &mut transaction,
            request.session_id,
            request.tenant_id,
            request.cluster_id,
            &request.capability,
            request.observed_at,
        )
        .await?;
        transaction.commit().await?;
        Ok(SessionScope {
            last_heartbeat_at: now,
            queryable_sources: queryable_source_count(&request.capability),
            ..scope
        })
    }

    async fn session_scope(
        &self,
        principal: &ConnectorPrincipal,
        session_id: ConnectorSessionId,
    ) -> Result<SessionScope, ControlPlaneError> {
        let mut transaction = self.repository.pool.begin().await?;
        let scope = locked_session_scope(&mut transaction, session_id, principal).await?;
        transaction.commit().await?;
        Ok(scope)
    }

    async fn commands(
        &self,
        scope: &SessionScope,
        after_sequence: u64,
        max_commands: usize,
    ) -> Result<Vec<ConnectorCommand>, ControlPlaneError> {
        let after_sequence = sequence_to_i64(after_sequence)?;
        let max_commands = i64::try_from(max_commands).map_err(|_| {
            ControlPlaneError::validation("output_too_large", "command limit exceeds the database bound")
        })?;
        let highest_sequence: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(sequence), 0)
             FROM connector_channel_commands
             WHERE session_id = $1",
        )
        .bind(scope.session_id.as_uuid())
        .fetch_one(&self.repository.pool)
        .await?;
        if after_sequence > highest_sequence {
            return Err(ControlPlaneError::validation(
                "capability_mismatch",
                "after_sequence is ahead of the durable command log",
            ));
        }
        let rows = sqlx::query(
            "SELECT command_payload
             FROM connector_channel_commands
             WHERE session_id = $1 AND sequence > $2
             ORDER BY sequence
             LIMIT $3",
        )
        .bind(scope.session_id.as_uuid())
        .bind(after_sequence)
        .bind(max_commands)
        .fetch_all(&self.repository.pool)
        .await?;
        rows.iter()
            .map(|row| {
                serde_json::from_value(row.try_get("command_payload")?)
                    .map_err(|_| ControlPlaneError::configuration("stored connector command payload is invalid"))
            })
            .collect()
    }

    async fn enqueue_query(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        query: EvidenceQuery,
        deadline: DateTime<Utc>,
        stale_before: DateTime<Utc>,
    ) -> Result<ConnectorCommand, ControlPlaneError> {
        let mut transaction = self.repository.pool.begin().await?;
        let scope =
            latest_online_session_for_update(&mut transaction, tenant_id, cluster_id, stale_before, &query.source)
                .await?;
        let sequence = next_sequence(&mut transaction, scope.session_id).await?;
        let command = ConnectorCommand::Query {
            envelope: ConnectorQueryEnvelope {
                schema: channel_schema(),
                session_id: scope.session_id,
                correlation_id: query.correlation_id,
                sequence,
                deadline,
                query,
            },
        };
        insert_command(&mut transaction, &command, scope.session_id).await?;
        transaction.commit().await?;
        Ok(command)
    }

    async fn enqueue_cancel(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        correlation_id: CorrelationId,
    ) -> Result<ConnectorCommand, ControlPlaneError> {
        let mut transaction = self.repository.pool.begin().await?;
        let row = sqlx::query(
            "SELECT s.session_id, s.tenant_id, s.cluster_id, s.connector_subject,
                    s.connector_issuer, s.last_heartbeat_at, s.capability
             FROM connector_channel_sessions s
             JOIN connector_channel_commands c ON c.session_id = s.session_id
             JOIN clusters cl ON cl.id = s.cluster_id
             JOIN connector_identities ci
               ON ci.cluster_id = s.cluster_id
              AND ci.subject = s.connector_subject
              AND ci.issuer = s.connector_issuer
              AND ci.revoked_at IS NULL
             WHERE s.tenant_id = $1
               AND s.cluster_id = $2
               AND c.correlation_id = $3
               AND c.command_kind = 'query'
               AND cl.onboarding_state <> 'offboarded'
             ORDER BY c.sequence DESC
             LIMIT 1
             FOR UPDATE OF s",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(correlation_id.as_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| ControlPlaneError::conflict("no online connector owns the requested query"))?;
        let scope = session_scope_from_row(&row)?;
        let sequence = next_sequence(&mut transaction, scope.session_id).await?;
        let command = ConnectorCommand::Cancel {
            schema: channel_schema(),
            session_id: scope.session_id,
            correlation_id,
            sequence,
        };
        insert_command(&mut transaction, &command, scope.session_id).await?;
        transaction.commit().await?;
        Ok(command)
    }

    async fn append_response(
        &self,
        scope: &SessionScope,
        response: &ConnectorResponseEnvelope,
    ) -> Result<ResponseDisposition, ControlPlaneError> {
        let sequence = sequence_to_i64(response.sequence)?;
        let mut transaction = self.repository.pool.begin().await?;
        let command_correlation: uuid::Uuid = sqlx::query_scalar(
            "SELECT correlation_id
             FROM connector_channel_commands
             WHERE session_id = $1 AND sequence = $2",
        )
        .bind(scope.session_id.as_uuid())
        .bind(sequence)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| ControlPlaneError::validation("capability_mismatch", "response sequence is unknown"))?;
        if command_correlation != response.correlation_id.as_uuid() {
            return Err(ControlPlaneError::forbidden(
                "capability_mismatch",
                "response correlation does not match the durable command",
            ));
        }
        let payload = serde_json::to_value(response).map_err(|_| {
            ControlPlaneError::validation("capability_mismatch", "connector response cannot be serialized")
        })?;
        let inserted = sqlx::query(
            "INSERT INTO connector_channel_responses (
                session_id, sequence, correlation_id, response_payload, received_at
             ) VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (session_id, sequence) DO NOTHING",
        )
        .bind(scope.session_id.as_uuid())
        .bind(sequence)
        .bind(response.correlation_id.as_uuid())
        .bind(payload)
        .bind(Utc::now())
        .execute(&mut *transaction)
        .await?
        .rows_affected()
            == 1;
        if !inserted {
            let existing_correlation: uuid::Uuid = sqlx::query_scalar(
                "SELECT correlation_id
                 FROM connector_channel_responses
                 WHERE session_id = $1 AND sequence = $2",
            )
            .bind(scope.session_id.as_uuid())
            .bind(sequence)
            .fetch_one(&mut *transaction)
            .await?;
            if existing_correlation != response.correlation_id.as_uuid() {
                return Err(ControlPlaneError::conflict(
                    "a different response already owns this command sequence",
                ));
            }
        }
        transaction.commit().await?;
        Ok(if inserted {
            ResponseDisposition::Inserted
        } else {
            ResponseDisposition::Duplicate
        })
    }

    async fn response(
        &self,
        session_id: ConnectorSessionId,
        sequence: u64,
    ) -> Result<Option<ConnectorResponseEnvelope>, ControlPlaneError> {
        let sequence = sequence_to_i64(sequence)?;
        let row = sqlx::query(
            "SELECT response_payload
             FROM connector_channel_responses
             WHERE session_id = $1 AND sequence = $2",
        )
        .bind(session_id.as_uuid())
        .bind(sequence)
        .fetch_optional(&self.repository.pool)
        .await?;
        row.map(|row| {
            serde_json::from_value(row.try_get("response_payload")?)
                .map_err(|_| ControlPlaneError::configuration("stored connector response payload is invalid"))
        })
        .transpose()
    }

    async fn latest_session(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
    ) -> Result<Option<SessionScope>, ControlPlaneError> {
        sqlx::query(
            "SELECT s.session_id, s.tenant_id, s.cluster_id, s.connector_subject,
                    s.connector_issuer, s.last_heartbeat_at, s.capability
             FROM connector_channel_sessions s
             JOIN clusters c ON c.id = s.cluster_id
             JOIN connector_identities ci
               ON ci.cluster_id = s.cluster_id
              AND ci.subject = s.connector_subject
              AND ci.issuer = s.connector_issuer
              AND ci.revoked_at IS NULL
             WHERE s.tenant_id = $1
               AND s.cluster_id = $2
               AND c.onboarding_state <> 'offboarded'
             ORDER BY s.last_heartbeat_at DESC, s.registered_at DESC
             LIMIT 1",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .fetch_optional(&self.repository.pool)
        .await?
        .as_ref()
        .map(session_scope_from_row)
        .transpose()
    }

    async fn latest_sessions(&self, limit: usize) -> Result<Vec<SessionScope>, ControlPlaneError> {
        let limit = i64::try_from(limit).map_err(|_| {
            ControlPlaneError::validation("output_too_large", "connector health sample limit is invalid")
        })?;
        let rows = sqlx::query(
            "SELECT session_id, tenant_id, cluster_id, connector_subject,
                    connector_issuer, last_heartbeat_at, capability
             FROM (
                SELECT DISTINCT ON (s.tenant_id, s.cluster_id)
                    s.session_id, s.tenant_id, s.cluster_id, s.connector_subject,
                    s.connector_issuer, s.last_heartbeat_at, s.capability
                FROM connector_channel_sessions s
                JOIN clusters c ON c.id = s.cluster_id
                JOIN connector_identities ci
                  ON ci.cluster_id = s.cluster_id
                 AND ci.subject = s.connector_subject
                 AND ci.issuer = s.connector_issuer
                 AND ci.revoked_at IS NULL
                WHERE c.onboarding_state <> 'offboarded'
                ORDER BY s.tenant_id, s.cluster_id, s.last_heartbeat_at DESC,
                         s.registered_at DESC
             ) latest
             ORDER BY last_heartbeat_at DESC
             LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.repository.pool)
        .await?;
        rows.iter().map(session_scope_from_row).collect()
    }
}

async fn append_source_capability_history(
    transaction: &mut Transaction<'_, Postgres>,
    session_id: ConnectorSessionId,
    tenant_id: TenantId,
    cluster_id: ClusterId,
    capability: &ConnectorCapabilityState,
    observed_at: DateTime<Utc>,
) -> Result<(), ControlPlaneError> {
    for source in &capability.sources {
        let schema_major = i32::try_from(source.schema_major).map_err(|_| {
            ControlPlaneError::validation(
                "capability_mismatch",
                "connector source schema major exceeds the database bound",
            )
        })?;
        let freshness_seconds = source.freshness_seconds.map(i64::try_from).transpose().map_err(|_| {
            ControlPlaneError::validation(
                "capability_mismatch",
                "connector source freshness exceeds the database bound",
            )
        })?;
        let status = connector_source_status(source.status);
        let limits = json!({
            "max_rows": source.max_rows,
            "max_bytes": source.max_bytes,
            "max_time_range_seconds": source.max_time_range_seconds,
        });
        sqlx::query(
            "INSERT INTO source_capability_history (
                tenant_id, cluster_id, connector_channel_session_id, source,
                schema_major, status, limits, last_success_at,
                latency_millis, freshness_seconds, observed_at
             )
             SELECT $1, $2, $3, $4, $5, $6, $7, $8, NULL, $9, $10
             WHERE NOT EXISTS (
                SELECT 1
                FROM source_capability_history
                WHERE tenant_id = $1
                  AND cluster_id = $2
                  AND connector_channel_session_id = $3
                  AND source = $4
                  AND schema_major = $5
                  AND status = $6
                  AND limits = $7
                  AND last_success_at IS NOT DISTINCT FROM $8
                  AND freshness_seconds IS NOT DISTINCT FROM $9
                  AND observed_at >= $10 - INTERVAL '5 minutes'
             )",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(session_id.as_uuid())
        .bind(&source.source)
        .bind(schema_major)
        .bind(status)
        .bind(limits)
        .bind(source.last_success_at)
        .bind(freshness_seconds)
        .bind(observed_at)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

const fn connector_source_status(status: ConnectorSourceStatus) -> &'static str {
    match status {
        ConnectorSourceStatus::Queryable => "queryable",
        ConnectorSourceStatus::Degraded => "degraded",
        ConnectorSourceStatus::Missing => "missing",
        ConnectorSourceStatus::Unsupported => "unsupported",
    }
}

async fn ensure_cluster_identity(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    cluster_id: ClusterId,
    principal: &ConnectorPrincipal,
) -> Result<(), ControlPlaneError> {
    let row = sqlx::query(
        "SELECT c.tenant_id, c.onboarding_state,
                EXISTS (
                    SELECT 1
                    FROM connector_identities ci
                    WHERE ci.cluster_id = c.id
                      AND ci.subject = $2
                      AND ci.issuer = $3
                      AND ci.revoked_at IS NULL
                ) AS identity_active
         FROM clusters c
         WHERE c.id = $1
         FOR UPDATE",
    )
    .bind(cluster_id.as_uuid())
    .bind(&principal.subject)
    .bind(&principal.issuer)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| {
        ControlPlaneError::forbidden("cluster_not_allowed", "registered connector cluster does not exist")
    })?;
    let stored_tenant: String = row.try_get("tenant_id")?;
    if stored_tenant != tenant_id.to_string() {
        return Err(ControlPlaneError::forbidden(
            "tenant_mismatch",
            "connector tenant does not own the registered cluster",
        ));
    }
    let onboarding_state: String = row.try_get("onboarding_state")?;
    if onboarding_state == "offboarded" {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "offboarded clusters cannot register connector channels",
        ));
    }
    let identity_active: bool = row.try_get("identity_active")?;
    if !identity_active {
        return Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "connector subject or issuer is not an active cluster identity",
        ));
    }
    Ok(())
}

async fn locked_session_scope(
    transaction: &mut Transaction<'_, Postgres>,
    session_id: ConnectorSessionId,
    principal: &ConnectorPrincipal,
) -> Result<SessionScope, ControlPlaneError> {
    let row = sqlx::query(
        "SELECT s.session_id, s.tenant_id, s.cluster_id, s.connector_subject,
                s.connector_issuer, s.last_heartbeat_at, s.capability,
                c.onboarding_state,
                EXISTS (
                    SELECT 1 FROM connector_identities ci
                    WHERE ci.cluster_id = s.cluster_id
                      AND ci.subject = s.connector_subject
                      AND ci.issuer = s.connector_issuer
                      AND ci.revoked_at IS NULL
                ) AS identity_active
         FROM connector_channel_sessions s
         JOIN clusters c ON c.id = s.cluster_id
         WHERE s.session_id = $1
         FOR UPDATE OF s, c",
    )
    .bind(session_id.as_uuid())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(ControlPlaneError::NotFound)?;
    let scope = session_scope_from_row(&row)?;
    if scope.subject != principal.subject || scope.issuer != principal.issuer {
        return Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "connector subject or issuer does not own this channel session",
        ));
    }
    let onboarding_state: String = row.try_get("onboarding_state")?;
    let identity_active: bool = row.try_get("identity_active")?;
    if onboarding_state == "offboarded" || !identity_active {
        return Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "connector identity is revoked or its cluster is offboarded",
        ));
    }
    Ok(scope)
}

fn enforce_scope(
    scope: &SessionScope,
    tenant_id: TenantId,
    cluster_id: ClusterId,
    principal: &ConnectorPrincipal,
) -> Result<(), ControlPlaneError> {
    if scope.tenant_id != tenant_id {
        return Err(ControlPlaneError::forbidden(
            "tenant_mismatch",
            "connector request crosses the registered tenant boundary",
        ));
    }
    if scope.cluster_id != cluster_id {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "connector request crosses the registered cluster boundary",
        ));
    }
    if scope.subject != principal.subject || scope.issuer != principal.issuer {
        return Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "connector subject or issuer does not own this channel session",
        ));
    }
    Ok(())
}

async fn latest_online_session_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    cluster_id: ClusterId,
    stale_before: DateTime<Utc>,
    source: &str,
) -> Result<SessionScope, ControlPlaneError> {
    let row = sqlx::query(
        "SELECT s.session_id, s.tenant_id, s.cluster_id, s.connector_subject,
                s.connector_issuer, s.last_heartbeat_at, s.capability
         FROM connector_channel_sessions s
         JOIN clusters c ON c.id = s.cluster_id
         JOIN connector_identities ci
           ON ci.cluster_id = s.cluster_id
          AND ci.subject = s.connector_subject
          AND ci.issuer = s.connector_issuer
          AND ci.revoked_at IS NULL
         WHERE s.tenant_id = $1
           AND s.cluster_id = $2
           AND s.last_heartbeat_at >= $3
           AND c.onboarding_state <> 'offboarded'
           AND EXISTS (
               SELECT 1
               FROM jsonb_array_elements(s.capability -> 'sources') source_capability
               WHERE source_capability ->> 'source' = $4
                 AND source_capability ->> 'status' IN ('queryable', 'degraded')
           )
         ORDER BY s.last_heartbeat_at DESC, s.registered_at DESC
         LIMIT 1
         FOR UPDATE OF s",
    )
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(stale_before)
    .bind(source)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| ControlPlaneError::conflict("no online connector channel is available"))?;
    session_scope_from_row(&row)
}

async fn next_sequence(
    transaction: &mut Transaction<'_, Postgres>,
    session_id: ConnectorSessionId,
) -> Result<u64, ControlPlaneError> {
    let sequence: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(sequence), 0) + 1
         FROM connector_channel_commands
         WHERE session_id = $1",
    )
    .bind(session_id.as_uuid())
    .fetch_one(&mut **transaction)
    .await?;
    u64::try_from(sequence).map_err(|_| ControlPlaneError::configuration("connector command sequence is invalid"))
}

async fn insert_command(
    transaction: &mut Transaction<'_, Postgres>,
    command: &ConnectorCommand,
    session_id: ConnectorSessionId,
) -> Result<(), ControlPlaneError> {
    let payload = serde_json::to_value(command)
        .map_err(|_| ControlPlaneError::validation("capability_mismatch", "connector command cannot be serialized"))?;
    sqlx::query(
        "INSERT INTO connector_channel_commands (
            session_id, sequence, correlation_id, command_kind, command_payload, created_at
         ) VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(session_id.as_uuid())
    .bind(sequence_to_i64(command.sequence())?)
    .bind(command.correlation_id().as_uuid())
    .bind(command.kind())
    .bind(payload)
    .bind(Utc::now())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn resume_frontier(
    transaction: &mut Transaction<'_, Postgres>,
    session_id: ConnectorSessionId,
) -> Result<u64, ControlPlaneError> {
    let frontier: i64 = sqlx::query_scalar(
        "WITH command_state AS (
            SELECT
                MIN(c.sequence) FILTER (WHERE r.sequence IS NULL) AS first_missing,
                COALESCE(MAX(c.sequence), 0) AS highest
            FROM connector_channel_commands c
            LEFT JOIN connector_channel_responses r
              ON r.session_id = c.session_id AND r.sequence = c.sequence
            WHERE c.session_id = $1
         )
         SELECT CASE
             WHEN first_missing IS NULL THEN highest
             ELSE first_missing - 1
         END
         FROM command_state",
    )
    .bind(session_id.as_uuid())
    .fetch_one(&mut **transaction)
    .await?;
    u64::try_from(frontier).map_err(|_| ControlPlaneError::configuration("connector response frontier is invalid"))
}

fn session_scope_from_row(row: &PgRow) -> Result<SessionScope, ControlPlaneError> {
    let capability: ConnectorCapabilityState = serde_json::from_value(row.try_get("capability")?)
        .map_err(|_| ControlPlaneError::configuration("stored connector capability payload is invalid"))?;
    Ok(SessionScope {
        session_id: ConnectorSessionId::from_uuid(row.try_get("session_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        subject: row.try_get("connector_subject")?,
        issuer: row.try_get("connector_issuer")?,
        last_heartbeat_at: row.try_get("last_heartbeat_at")?,
        queryable_sources: queryable_source_count(&capability),
    })
}

fn queryable_source_count(capability: &ConnectorCapabilityState) -> u16 {
    u16::try_from(
        capability
            .sources
            .iter()
            .filter(|source| matches!(source.status, rocketmq_sre_contracts::ConnectorSourceStatus::Queryable))
            .count(),
    )
    .unwrap_or(u16::MAX)
}

fn sequence_to_i64(sequence: u64) -> Result<i64, ControlPlaneError> {
    i64::try_from(sequence)
        .map_err(|_| ControlPlaneError::validation("capability_mismatch", "sequence exceeds the channel bound"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(tenant_id: TenantId, cluster_id: ClusterId) -> SessionScope {
        SessionScope {
            session_id: ConnectorSessionId::new(),
            tenant_id,
            cluster_id,
            subject: "connector-a".to_owned(),
            issuer: "issuer-a".to_owned(),
            last_heartbeat_at: Utc::now(),
            queryable_sources: 0,
        }
    }

    #[test]
    fn cross_tenant_and_cluster_scope_fail_closed() {
        let tenant = TenantId::new();
        let cluster = ClusterId::new();
        let principal = ConnectorPrincipal {
            subject: "connector-a".to_owned(),
            issuer: "issuer-a".to_owned(),
        };
        assert!(enforce_scope(&scope(tenant, cluster), TenantId::new(), cluster, &principal).is_err());
        assert!(enforce_scope(&scope(tenant, cluster), tenant, ClusterId::new(), &principal).is_err());
    }

    #[test]
    fn cross_subject_scope_fails_closed() {
        let tenant = TenantId::new();
        let cluster = ClusterId::new();
        let principal = ConnectorPrincipal {
            subject: "connector-b".to_owned(),
            issuer: "issuer-a".to_owned(),
        };
        assert!(enforce_scope(&scope(tenant, cluster), tenant, cluster, &principal).is_err());
    }
}
