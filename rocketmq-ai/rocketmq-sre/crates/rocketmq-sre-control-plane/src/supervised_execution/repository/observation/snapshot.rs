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

use chrono::{DateTime, Utc};
use rocketmq_sre_contracts::{ExecutionId, ExecutionStepId};
use serde_json::Value;
use sqlx::Row;

use super::{PostgresRepository, StoredExecutionProjection, decode_receipt};
use crate::{ControlPlaneRequestFailure, auth::AuthContext};

pub(in crate::supervised_execution) struct ObservationJournal {
    pub execution: StoredExecutionProjection,
    pub attempts: Vec<AttemptRecord>,
    pub verifications: Vec<VerificationRecord>,
    pub evidence: Vec<EvidenceRecord>,
    pub events: Vec<EventRecord>,
    pub quarantine: bool,
    pub partial: bool,
}

pub(in crate::supervised_execution) struct AttemptRecord {
    pub step_id: ExecutionStepId,
    pub attempt: i32,
    pub compensation: bool,
    pub kind: String,
    pub occurred_at: DateTime<Utc>,
    pub snapshot: Option<Value>,
    pub lease_id: Option<uuid::Uuid>,
    pub lease_epoch: Option<i64>,
}

pub(in crate::supervised_execution) struct VerificationRecord {
    pub step_id: ExecutionStepId,
    pub attempt: i32,
    pub compensation: bool,
    pub outcome: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub snapshot: Option<Value>,
}

pub(in crate::supervised_execution) struct EvidenceRecord {
    pub step_id: ExecutionStepId,
    pub attempt: i32,
    pub phase: String,
    pub evidence_id: uuid::Uuid,
    pub snapshot: Option<Value>,
}

pub(in crate::supervised_execution) struct EventRecord {
    pub sequence: i64,
    pub kind: String,
    pub resource_kind: String,
    pub resource_id: String,
    pub state: Option<String>,
    pub occurred_at: DateTime<Utc>,
}

impl PostgresRepository {
    /// A single read-only MVCC snapshot: polling cannot mutate leases, results,
    /// target state or acceptance. Per table payload transfer is at most 8 MiB,
    /// each individual JSON record at most 512 KiB; missing payload stays Unknown.
    pub(in crate::supervised_execution) async fn execution_observation_journal(
        &self,
        auth: &AuthContext,
        id: ExecutionId,
    ) -> Result<ObservationJournal, ControlPlaneRequestFailure> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *tx)
            .await?;
        let clusters: Vec<_> = auth.clusters.iter().map(|id| id.as_uuid()).collect();
        let row = sqlx::query(
            "SELECT id,tenant_id,cluster_id,plan_id,plan_hash,correlation_id,requested_by,idempotency_key,
                    CASE WHEN octet_length(request_snapshot::text)<=524288 THEN request_snapshot END AS request_snapshot,
                    state,started_at FROM executions WHERE id=$1 AND tenant_id=$2 AND cluster_id=ANY($3)",
        ).bind(id.as_uuid()).bind(auth.tenant_id.as_uuid()).bind(&clusters)
            .fetch_optional(&mut *tx).await?.ok_or_else(ControlPlaneRequestFailure::not_found)?;
        if row.try_get::<Option<Value>, _>("request_snapshot")?.is_none() {
            return Err(ControlPlaneRequestFailure::validation(
                "execution_history_unavailable",
                "retained execution identity exceeds the safe read bound",
            ));
        }
        let execution = decode_receipt(&row)?;
        let rows = sqlx::query(
            "WITH bounded AS (SELECT step_id,attempt,compensation,record_kind,occurred_at,lease_id,lease_epoch,
                COALESCE(intent_snapshot,result_snapshot) AS payload,sequence_id
                FROM execution_steps WHERE execution_id=$1 ORDER BY sequence_id LIMIT 129),
             sized AS (SELECT *,sum(octet_length(payload::text)) OVER(ORDER BY sequence_id) AS bytes FROM bounded)
             SELECT step_id,attempt,compensation,record_kind,occurred_at,lease_id,lease_epoch,
                CASE WHEN bytes<=8388608 AND octet_length(payload::text)<=524288 THEN payload END AS payload
                FROM sized ORDER BY sequence_id",
        )
        .bind(id.as_uuid())
        .fetch_all(&mut *tx)
        .await?;
        let mut partial = rows.len() > 128;
        let mut attempts = Vec::new();
        for row in rows.into_iter().take(128) {
            let snapshot = row.try_get("payload")?;
            attempts.push(AttemptRecord {
                step_id: ExecutionStepId::from_uuid(row.try_get("step_id")?),
                attempt: row.try_get("attempt")?,
                compensation: row.try_get("compensation")?,
                kind: row.try_get("record_kind")?,
                occurred_at: row.try_get("occurred_at")?,
                snapshot,
                lease_id: row.try_get("lease_id")?,
                lease_epoch: row.try_get("lease_epoch")?,
            });
        }
        let rows = sqlx::query(
            "WITH bounded AS (SELECT * FROM execution_verifications WHERE execution_id=$1 ORDER BY sequence_id LIMIT 65),
             sized AS (SELECT *,sum(octet_length(result_snapshot::text)) OVER(ORDER BY sequence_id) AS bytes FROM bounded)
             SELECT step_id,attempt,compensation,outcome,started_at,completed_at,
                CASE WHEN bytes<=8388608 AND octet_length(result_snapshot::text)<=524288 THEN result_snapshot END AS payload
                FROM sized ORDER BY sequence_id",
        ).bind(id.as_uuid()).fetch_all(&mut *tx).await?;
        partial |= rows.len() > 64;
        let mut verifications = Vec::new();
        for row in rows.into_iter().take(64) {
            verifications.push(VerificationRecord {
                step_id: ExecutionStepId::from_uuid(row.try_get("step_id")?),
                attempt: row.try_get("attempt")?,
                compensation: row.try_get("compensation")?,
                outcome: row.try_get("outcome")?,
                started_at: row.try_get("started_at")?,
                completed_at: row.try_get("completed_at")?,
                snapshot: row.try_get("payload")?,
            });
        }
        let rows = sqlx::query(
            "WITH bounded AS (SELECT * FROM execution_verification_evidence WHERE execution_id=$1 ORDER BY sequence_id LIMIT 257),
             sized AS (SELECT *,sum(octet_length(evidence_snapshot::text)) OVER(ORDER BY sequence_id) AS bytes FROM bounded)
             SELECT step_id,attempt,phase,evidence_id,
                CASE WHEN bytes<=8388608 AND octet_length(evidence_snapshot::text)<=524288 THEN evidence_snapshot END AS payload
                FROM sized ORDER BY sequence_id",
        ).bind(id.as_uuid()).fetch_all(&mut *tx).await?;
        partial |= rows.len() > 256;
        let mut evidence = Vec::new();
        for row in rows.into_iter().take(256) {
            evidence.push(EvidenceRecord {
                step_id: ExecutionStepId::from_uuid(row.try_get("step_id")?),
                attempt: row.try_get("attempt")?,
                phase: row.try_get("phase")?,
                evidence_id: row.try_get("evidence_id")?,
                snapshot: row.try_get("payload")?,
            });
        }
        let rows = sqlx::query(
            "SELECT a.sequence_id,a.event_kind,a.resource_kind,a.resource_id,a.occurred_at,
                    left(COALESCE(a.details->>'to',a.details->>'state'),32) AS state
             FROM audit_events a WHERE a.tenant_id=$2 AND a.cluster_id=$3 AND ((a.correlation_id=$4 AND (
                (a.resource_kind='execution' AND a.resource_id=$5) OR
                (a.resource_kind IN ('step_intent','step_result','verification') AND EXISTS (
                    SELECT 1 FROM execution_steps s WHERE s.execution_id=$1 AND s.step_id::text=a.resource_id)) OR
                (a.resource_kind='verification_evidence' AND EXISTS (
                    SELECT 1 FROM execution_verification_evidence e WHERE e.execution_id=$1 AND e.evidence_id::text=a.resource_id)))) OR
                (a.resource_kind IN ('quarantine','resource_quarantine') AND EXISTS (
                    SELECT 1 FROM resource_quarantines q WHERE q.source_execution_id=$1 AND q.id::text=a.resource_id)))
             ORDER BY a.sequence_id LIMIT 257",
        ).bind(id.as_uuid()).bind(auth.tenant_id.as_uuid()).bind(execution.request.cluster_id.as_uuid())
            .bind(execution.request.correlation_id.as_uuid()).bind(id.to_string()).fetch_all(&mut *tx).await?;
        partial |= rows.len() > 256;
        let mut events = Vec::new();
        for row in rows.into_iter().take(256) {
            events.push(EventRecord {
                sequence: row.try_get("sequence_id")?,
                kind: row.try_get("event_kind")?,
                resource_kind: row.try_get("resource_kind")?,
                resource_id: row.try_get("resource_id")?,
                state: row.try_get("state")?,
                occurred_at: row.try_get("occurred_at")?,
            });
        }
        let quarantine = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM resource_quarantines WHERE source_execution_id=$1 AND tenant_id=$2 AND cluster_id=$3 AND cleared_at IS NULL)",
        ).bind(id.as_uuid()).bind(auth.tenant_id.as_uuid()).bind(execution.request.cluster_id.as_uuid())
            .fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(ObservationJournal {
            execution,
            attempts,
            verifications,
            evidence,
            events,
            quarantine,
            partial,
        })
    }
}
