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
use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::ConnectorResponseEnvelope;
use rocketmq_sre_contracts::ConnectorSessionId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::canonical_sha256;
use serde_json::Value;
use serde_json::json;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;

use super::super::channel_schema;
use crate::ControlPlaneError;

const RETENTION_AGE_HOURS: i64 = 24;
const INACTIVE_SESSION_GRACE_MINUTES: i64 = 5;
const TARGET_RETAINED_COMMANDS: i64 = 4_096;
const HARD_RETAINED_COMMANDS: i64 = 8_192;
const COMPACTION_BATCH_SIZE: i64 = 64;
const MAX_SESSIONS_PER_PASS: i64 = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RetentionReason {
    Age,
    Pressure,
    InactiveSession,
}

impl RetentionReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Age => "age",
            Self::Pressure => "pressure",
            Self::InactiveSession => "inactive_session",
        }
    }
}

#[derive(Debug)]
struct SessionState {
    session_id: ConnectorSessionId,
    compacted_through: i64,
    highest_allocated: i64,
    last_seen_at: DateTime<Utc>,
}

#[derive(Debug)]
struct RetainedRecord {
    sequence: i64,
    correlation_id: uuid::Uuid,
    command_kind: String,
    command_payload: Value,
    command_created_at: DateTime<Utc>,
    response_payload: Option<Value>,
    response_received_at: Option<DateTime<Utc>>,
}

pub(super) async fn allocate_next_sequence(
    transaction: &mut Transaction<'_, Postgres>,
    session_id: ConnectorSessionId,
) -> Result<u64, ControlPlaneError> {
    let sequence = sqlx::query_scalar::<_, i64>(
        "UPDATE connector_channel_sessions
         SET next_sequence = next_sequence + 1
         WHERE session_id = $1
           AND next_sequence - compacted_through_sequence - 1 < $2
         RETURNING next_sequence - 1",
    )
    .bind(session_id.as_uuid())
    .bind(HARD_RETAINED_COMMANDS)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| {
        ControlPlaneError::conflict(
            "connector channel retained-log limit reached; wait for completed response retention or reconnect",
        )
    })?;
    u64::try_from(sequence).map_err(|_| ControlPlaneError::configuration("connector command sequence is invalid"))
}

pub(super) async fn resume_frontier(
    transaction: &mut Transaction<'_, Postgres>,
    session_id: ConnectorSessionId,
) -> Result<u64, ControlPlaneError> {
    let frontier = sqlx::query_scalar::<_, i64>(
        "WITH command_state AS (
            SELECT
                s.compacted_through_sequence AS compacted_through,
                MIN(c.sequence) FILTER (WHERE r.sequence IS NULL) AS first_missing,
                COALESCE(MAX(c.sequence), 0) AS highest
            FROM connector_channel_sessions s
            LEFT JOIN connector_channel_commands c
              ON c.session_id = s.session_id
            LEFT JOIN connector_channel_responses r
              ON r.session_id = c.session_id AND r.sequence = c.sequence
            WHERE s.session_id = $1
            GROUP BY s.compacted_through_sequence
         )
         SELECT GREATEST(
             compacted_through,
             CASE
                 WHEN first_missing IS NULL THEN highest
                 ELSE first_missing - 1
             END
         )
         FROM command_state",
    )
    .bind(session_id.as_uuid())
    .fetch_one(&mut **transaction)
    .await?;
    u64::try_from(frontier).map_err(|_| ControlPlaneError::configuration("connector response frontier is invalid"))
}

pub(super) async fn maintain_retention(
    transaction: &mut Transaction<'_, Postgres>,
    current_session_id: ConnectorSessionId,
    tenant_id: TenantId,
    cluster_id: rocketmq_sre_contracts::ClusterId,
    subject: &str,
    issuer: &str,
    now: DateTime<Utc>,
) -> Result<(), ControlPlaneError> {
    let rows = sqlx::query(
        "SELECT session_id, compacted_through_sequence, next_sequence - 1 AS highest_allocated,
                last_seen_at
         FROM connector_channel_sessions
         WHERE tenant_id = $1
           AND cluster_id = $2
           AND connector_subject = $3
           AND connector_issuer = $4
         ORDER BY
             CASE WHEN session_id = $5 THEN 0 ELSE 1 END,
             last_seen_at
         LIMIT $6
         FOR UPDATE SKIP LOCKED",
    )
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(subject)
    .bind(issuer)
    .bind(current_session_id.as_uuid())
    .bind(MAX_SESSIONS_PER_PASS)
    .fetch_all(&mut **transaction)
    .await?;

    for row in rows {
        let state = SessionState {
            session_id: ConnectorSessionId::from_uuid(row.try_get("session_id")?),
            compacted_through: row.try_get("compacted_through_sequence")?,
            highest_allocated: row.try_get("highest_allocated")?,
            last_seen_at: row.try_get("last_seen_at")?,
        };
        let inactive = state.session_id != current_session_id
            && state.last_seen_at <= now - Duration::minutes(INACTIVE_SESSION_GRACE_MINUTES);
        compact_session_prefix(transaction, &state, inactive, now).await?;
    }
    Ok(())
}

async fn compact_session_prefix(
    transaction: &mut Transaction<'_, Postgres>,
    state: &SessionState,
    inactive: bool,
    now: DateTime<Utc>,
) -> Result<(), ControlPlaneError> {
    let pressure_cutoff = state
        .highest_allocated
        .saturating_sub(TARGET_RETAINED_COMMANDS)
        .max(state.compacted_through);
    let age_cutoff = now - Duration::hours(RETENTION_AGE_HOURS);
    let rows = sqlx::query(
        "SELECT c.sequence, c.correlation_id, c.command_kind, c.command_payload,
                c.created_at AS command_created_at,
                r.response_payload, r.received_at AS response_received_at
         FROM connector_channel_commands c
         LEFT JOIN connector_channel_responses r
           ON r.session_id = c.session_id AND r.sequence = c.sequence
         WHERE c.session_id = $1
           AND c.sequence > $2
         ORDER BY c.sequence
         LIMIT $3",
    )
    .bind(state.session_id.as_uuid())
    .bind(state.compacted_through)
    .bind(COMPACTION_BATCH_SIZE)
    .fetch_all(&mut **transaction)
    .await?;

    let mut records = Vec::with_capacity(rows.len());
    let mut reason = RetentionReason::Age;
    let mut expected_sequence = state.compacted_through.saturating_add(1);
    for row in rows {
        let mut record = RetainedRecord {
            sequence: row.try_get("sequence")?,
            correlation_id: row.try_get("correlation_id")?,
            command_kind: row.try_get("command_kind")?,
            command_payload: row.try_get("command_payload")?,
            command_created_at: row.try_get("command_created_at")?,
            response_payload: row.try_get("response_payload")?,
            response_received_at: row.try_get("response_received_at")?,
        };
        if record.sequence != expected_sequence {
            return Err(ControlPlaneError::configuration(
                "connector channel retained log contains a non-contiguous sequence",
            ));
        }
        let row_reason = retention_reason(
            inactive,
            record.sequence <= pressure_cutoff,
            record.command_created_at <= age_cutoff,
        );
        let Some(row_reason) = row_reason else {
            break;
        };
        reason = strongest_reason(reason, row_reason);
        if record.response_payload.is_none() {
            if !inactive {
                break;
            }
            terminalize_inactive_command(transaction, state.session_id, &mut record, now).await?;
        }
        records.push(record);
        expected_sequence = expected_sequence.saturating_add(1);
    }
    if records.is_empty() {
        return Ok(());
    }

    let from_sequence = records[0].sequence;
    let through_sequence = records
        .last()
        .map(|record| record.sequence)
        .ok_or_else(|| ControlPlaneError::configuration("connector retention batch unexpectedly became empty"))?;
    let correlations = records
        .iter()
        .map(|record| record.correlation_id)
        .collect::<BTreeSet<_>>();
    let material = json!({
        "schema": "rocketmq-sre.connector-channel-compaction.v1",
        "session_id": state.session_id,
        "from_sequence": from_sequence,
        "through_sequence": through_sequence,
        "records": records.iter().map(|record| json!({
            "sequence": record.sequence,
            "correlation_id": record.correlation_id,
            "command_kind": &record.command_kind,
            "command_payload": &record.command_payload,
            "command_created_at": record.command_created_at,
            "response_payload": &record.response_payload,
            "response_received_at": record.response_received_at,
        })).collect::<Vec<_>>(),
    });
    let material_hash = canonical_sha256(&material)
        .map_err(|_| ControlPlaneError::configuration("connector retention material cannot be canonicalized"))?;
    let count = i64::try_from(records.len())
        .map_err(|_| ControlPlaneError::configuration("connector retention batch exceeds the database bound"))?;
    let correlation_count = i64::try_from(correlations.len())
        .map_err(|_| ControlPlaneError::configuration("connector retention correlation count is invalid"))?;

    sqlx::query(
        "INSERT INTO connector_channel_compaction_receipts (
            receipt_id, session_id, from_sequence, through_sequence,
            command_count, response_count, correlation_count,
            material_hash, retention_reason, compacted_at
         ) VALUES ($1, $2, $3, $4, $5, $5, $6, $7, $8, $9)",
    )
    .bind(uuid::Uuid::new_v4())
    .bind(state.session_id.as_uuid())
    .bind(from_sequence)
    .bind(through_sequence)
    .bind(count)
    .bind(correlation_count)
    .bind(material_hash)
    .bind(reason.as_str())
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "UPDATE connector_channel_sessions
         SET compacted_through_sequence = $2,
             last_compacted_at = $3
         WHERE session_id = $1
           AND compacted_through_sequence = $4",
    )
    .bind(state.session_id.as_uuid())
    .bind(through_sequence)
    .bind(now)
    .bind(state.compacted_through)
    .execute(&mut **transaction)
    .await?;
    sqlx::query_scalar::<_, String>(
        "SELECT set_config(
            'rocketmq_sre.connector_retention_session',
            $1,
            TRUE
         )",
    )
    .bind(state.session_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    sqlx::query(
        "DELETE FROM connector_channel_responses
         WHERE session_id = $1
           AND sequence BETWEEN $2 AND $3",
    )
    .bind(state.session_id.as_uuid())
    .bind(from_sequence)
    .bind(through_sequence)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "DELETE FROM connector_channel_commands
         WHERE session_id = $1
           AND sequence BETWEEN $2 AND $3",
    )
    .bind(state.session_id.as_uuid())
    .bind(from_sequence)
    .bind(through_sequence)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn terminalize_inactive_command(
    transaction: &mut Transaction<'_, Postgres>,
    session_id: ConnectorSessionId,
    record: &mut RetainedRecord,
    now: DateTime<Utc>,
) -> Result<(), ControlPlaneError> {
    let response = ConnectorResponseEnvelope {
        schema: channel_schema(),
        session_id,
        correlation_id: CorrelationId::from_uuid(record.correlation_id),
        sequence: u64::try_from(record.sequence)
            .map_err(|_| ControlPlaneError::configuration("connector command sequence is invalid"))?,
        evidence: None,
        error_code: Some("source_unavailable".to_owned()),
        retryable: true,
    };
    let payload = serde_json::to_value(response)
        .map_err(|_| ControlPlaneError::configuration("inactive connector terminal response cannot be serialized"))?;
    sqlx::query(
        "INSERT INTO connector_channel_responses (
            session_id, sequence, correlation_id, response_payload, received_at
         ) VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (session_id, sequence) DO NOTHING",
    )
    .bind(session_id.as_uuid())
    .bind(record.sequence)
    .bind(record.correlation_id)
    .bind(payload)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    let row = sqlx::query(
        "SELECT response_payload, received_at
         FROM connector_channel_responses
         WHERE session_id = $1 AND sequence = $2",
    )
    .bind(session_id.as_uuid())
    .bind(record.sequence)
    .fetch_one(&mut **transaction)
    .await?;
    record.response_payload = Some(row.try_get("response_payload")?);
    record.response_received_at = Some(row.try_get("received_at")?);
    Ok(())
}

const fn retention_reason(inactive: bool, pressure: bool, old_enough: bool) -> Option<RetentionReason> {
    if inactive {
        Some(RetentionReason::InactiveSession)
    } else if pressure {
        Some(RetentionReason::Pressure)
    } else if old_enough {
        Some(RetentionReason::Age)
    } else {
        None
    }
}

const fn strongest_reason(current: RetentionReason, candidate: RetentionReason) -> RetentionReason {
    match (current, candidate) {
        (_, RetentionReason::InactiveSession) | (RetentionReason::InactiveSession, _) => {
            RetentionReason::InactiveSession
        }
        (_, RetentionReason::Pressure) | (RetentionReason::Pressure, _) => RetentionReason::Pressure,
        _ => RetentionReason::Age,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_is_fail_closed_for_recent_active_commands() {
        assert_eq!(retention_reason(false, false, false), None);
        assert_eq!(retention_reason(false, false, true), Some(RetentionReason::Age));
        assert_eq!(retention_reason(false, true, false), Some(RetentionReason::Pressure));
        assert_eq!(
            retention_reason(true, false, false),
            Some(RetentionReason::InactiveSession)
        );
    }

    #[test]
    fn strongest_reason_preserves_inactive_and_pressure_evidence() {
        assert_eq!(
            strongest_reason(RetentionReason::Age, RetentionReason::Pressure),
            RetentionReason::Pressure
        );
        assert_eq!(
            strongest_reason(RetentionReason::Pressure, RetentionReason::InactiveSession),
            RetentionReason::InactiveSession
        );
    }
}
