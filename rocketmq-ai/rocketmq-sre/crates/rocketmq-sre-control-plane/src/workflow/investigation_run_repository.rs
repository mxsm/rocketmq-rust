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
use rocketmq_sre_contracts::{
    ClusterId, CreateInvestigationRunRequest, CreateInvestigationRunResponse, HypothesisAssessment,
    InvestigationCommandReceipt, InvestigationCommandRequest, InvestigationDiagnosisRevision, InvestigationId,
    InvestigationRevision, InvestigationRun, InvestigationRunContext, InvestigationRunFailure, InvestigationRunId,
    InvestigationRunPermission, InvestigationRunStatus, TenantId,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use uuid::Uuid;

use super::investigation_run_service::{authorize_control, contract_failure};
use super::repository::ensure_cluster_scope;
use crate::{ControlPlaneRequestFailure, PostgresRepository, auth::AuthContext};

mod events;
mod ownership;
#[cfg(test)]
mod tests;
mod worker;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct RunCommandOutcome {
    pub receipt: InvestigationCommandReceipt,
    pub replayed: bool,
}

#[derive(Clone, Debug)]
#[cfg_attr(
    not(test),
    allow(dead_code, reason = "D15's dispatcher consumes this internal fencing token")
)]
pub(crate) struct RunLeaseFence {
    pub owner: Uuid,
    pub owner_epoch: u64,
}

#[derive(Clone, Debug)]
#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "D14 tests ownership without dispatching new work; D15 consumes leases"
    )
)]
pub(crate) struct RunLease {
    pub run: InvestigationRun,
    pub fence: RunLeaseFence,
    pub expires_at: DateTime<Utc>,
}

/// Trusted, non-deserializable worker proof. No public handler accepts it.
#[cfg_attr(
    not(test),
    allow(dead_code, reason = "D15 consumes the fenced D14 persistence operations")
)]
pub(crate) struct RunWorkerContext<'a> {
    pub auth: &'a AuthContext,
    pub context: &'a InvestigationRunContext,
    pub fence: &'a RunLeaseFence,
}

#[cfg_attr(
    not(test),
    allow(dead_code, reason = "D15 supplies authorized saved assessments to D14 persistence")
)]
pub(crate) struct RunAssessmentCommit<'a> {
    pub run_id: InvestigationRunId,
    pub expected_revision: InvestigationRevision,
    pub assessment: &'a HypothesisAssessment,
    pub saved_revision: &'a InvestigationDiagnosisRevision,
}

#[cfg_attr(
    not(test),
    allow(dead_code, reason = "D15 supplies bounded worker stop decisions to D14 persistence")
)]
pub(crate) struct RunStopCommit {
    pub run_id: InvestigationRunId,
    pub expected_revision: InvestigationRevision,
    pub status: InvestigationRunStatus,
}

#[derive(Clone, Debug)]
pub(crate) struct RunRecoveryCandidate {
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub run_id: InvestigationRunId,
}

struct LockedRun {
    run: InvestigationRun,
    stored_revision: String,
    head: i64,
    floor: i64,
    owner: Option<Uuid>,
    owner_epoch: i64,
    lease_until: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy)]
enum RunLoadMode {
    ActiveRead,
    ActiveWrite,
    RecoveryWrite,
}

impl PostgresRepository {
    pub(crate) async fn investigation_run_get(
        &self,
        auth: &AuthContext,
        id: InvestigationRunId,
    ) -> Result<InvestigationRun, ControlPlaneRequestFailure> {
        let mut tx = self.pool.begin().await?;
        let state = load_run(&mut tx, auth, id, RunLoadMode::ActiveRead).await?;
        tx.commit().await?;
        Ok(state.run)
    }

    pub(crate) async fn investigation_run_create(
        &self,
        auth: &AuthContext,
        context: &InvestigationRunContext,
        request: &CreateInvestigationRunRequest,
        at: DateTime<Utc>,
    ) -> Result<CreateInvestigationRunResponse, ControlPlaneRequestFailure> {
        authorize_control(auth)?;
        bind_context(auth, context, &request.scope)?;
        let mut tx = self.pool.begin().await?;
        lock_parent(
            &mut tx,
            auth,
            request.scope.investigation_id,
            Some(request.scope.cluster_id),
        )
        .await?;
        let previous: Option<Value> = sqlx::query_scalar("SELECT response_snapshot FROM investigation_run_creations WHERE tenant_id=$1 AND run_id=$2 AND actor_subject=$3 AND idempotency_key=$4")
            .bind(auth.tenant_id.as_uuid()).bind(request.run_id.as_uuid()).bind(&auth.subject).bind(&request.idempotency_key)
            .fetch_optional(&mut *tx).await?;
        let previous = previous.map(decode::<CreateInvestigationRunResponse>).transpose()?;
        let now = at.max(Utc::now());
        let response = InvestigationRun::create(context, request, previous.as_ref(), now).map_err(contract_failure)?;
        if response.replayed {
            tx.commit().await?;
            return Ok(response);
        }
        let inserted = sqlx::query("INSERT INTO investigation_runs(id,tenant_id,cluster_id,investigation_id,run_snapshot,revision,status,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$8) ON CONFLICT(id) DO NOTHING")
            .bind(response.run.id.as_uuid()).bind(auth.tenant_id.as_uuid()).bind(response.run.initial_scope.cluster_id.as_uuid())
            .bind(response.run.initial_scope.investigation_id.as_uuid()).bind(encode(&response.run)?)
            .bind(response.run.revision.get().to_string()).bind(status_name(response.run.status)).bind(now).execute(&mut *tx).await?;
        if inserted.rows_affected() != 1 {
            return Err(conflict("idempotency_conflict"));
        }
        sqlx::query("INSERT INTO investigation_run_creations(tenant_id,run_id,actor_subject,idempotency_key,response_snapshot,recorded_at) VALUES($1,$2,$3,$4,$5,$6)")
            .bind(auth.tenant_id.as_uuid()).bind(request.run_id.as_uuid()).bind(&auth.subject).bind(&request.idempotency_key)
            .bind(encode(&response)?).bind(now).execute(&mut *tx).await?;
        let mut state = load_run(&mut tx, auth, request.run_id, RunLoadMode::ActiveWrite).await?;
        persist_event(
            &mut tx,
            &mut state,
            rocketmq_sre_contracts::InvestigationRunEventKind::Created {},
            &auth.subject,
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(response)
    }

    pub(crate) async fn investigation_run_command(
        &self,
        auth: &AuthContext,
        context: &InvestigationRunContext,
        request: &InvestigationCommandRequest,
        at: DateTime<Utc>,
    ) -> Result<RunCommandOutcome, ControlPlaneRequestFailure> {
        authorize_control(auth)?;
        let mut tx = self.pool.begin().await?;
        let mut state = load_run(&mut tx, auth, request.run_id, RunLoadMode::ActiveWrite).await?;
        bind_context(auth, context, &state.run.initial_scope)?;
        let previous: Option<Value> = sqlx::query_scalar("SELECT receipt_snapshot FROM investigation_run_commands WHERE tenant_id=$1 AND run_id=$2 AND actor_subject=$3 AND idempotency_key=$4")
            .bind(auth.tenant_id.as_uuid()).bind(request.run_id.as_uuid()).bind(&auth.subject).bind(&request.idempotency_key)
            .fetch_optional(&mut *tx).await?;
        let previous = previous.map(decode::<InvestigationCommandReceipt>).transpose()?;
        let now = at.max(Utc::now()).max(state.run.updated_at);
        let receipt = state
            .run
            .apply_command(context, request, previous.as_ref(), now)
            .map_err(contract_failure)?;
        let replayed = previous.is_some();
        if !replayed {
            insert_command(&mut tx, auth, request, &receipt).await?;
            persist_event(
                &mut tx,
                &mut state,
                rocketmq_sre_contracts::InvestigationRunEventKind::CommandAccepted {
                    command_id: receipt.command_id,
                },
                &auth.subject,
                now,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(RunCommandOutcome { receipt, replayed })
    }
}

async fn insert_command(
    tx: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    request: &InvestigationCommandRequest,
    receipt: &InvestigationCommandReceipt,
) -> Result<(), ControlPlaneRequestFailure> {
    let inserted = sqlx::query("INSERT INTO investigation_run_commands(tenant_id,run_id,actor_subject,idempotency_key,command_id,request_snapshot,receipt_snapshot,recorded_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING")
        .bind(auth.tenant_id.as_uuid()).bind(request.run_id.as_uuid()).bind(&auth.subject).bind(&request.idempotency_key)
        .bind(request.command_id.as_uuid()).bind(encode(request)?).bind(encode(receipt)?).bind(receipt.recorded_at)
        .execute(&mut **tx).await?;
    if inserted.rows_affected() != 1 {
        return Err(conflict("idempotency_conflict"));
    }
    Ok(())
}

async fn lock_parent(
    tx: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    id: InvestigationId,
    cluster: Option<ClusterId>,
) -> Result<(), ControlPlaneRequestFailure> {
    let row = sqlx::query("SELECT cluster_id FROM investigations WHERE id=$1 AND tenant_id=$2 FOR UPDATE")
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(ControlPlaneRequestFailure::not_found)?;
    let saved_cluster = ClusterId::from_uuid(row.try_get("cluster_id")?);
    if cluster.is_some_and(|cluster| cluster != saved_cluster) || !auth.clusters.contains(&saved_cluster) {
        return Err(ControlPlaneRequestFailure::forbidden(
            "scope_mismatch",
            "investigation scope is not authorized",
        ));
    }
    ensure_cluster_scope(tx, auth.tenant_id, saved_cluster).await
}

async fn load_run(
    tx: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    id: InvestigationRunId,
    mode: RunLoadMode,
) -> Result<LockedRun, ControlPlaneRequestFailure> {
    let query = match mode {
        RunLoadMode::ActiveRead => "SELECT * FROM investigation_runs WHERE tenant_id=$1 AND id=$2 FOR SHARE",
        RunLoadMode::ActiveWrite | RunLoadMode::RecoveryWrite => {
            "SELECT * FROM investigation_runs WHERE tenant_id=$1 AND id=$2 FOR UPDATE"
        }
    };
    let row = sqlx::query(query)
        .bind(auth.tenant_id.as_uuid())
        .bind(id.as_uuid())
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(ControlPlaneRequestFailure::not_found)?;
    let cluster_id = ClusterId::from_uuid(row.try_get("cluster_id")?);
    if !auth.clusters.contains(&cluster_id) {
        return Err(ControlPlaneRequestFailure::forbidden(
            "scope_mismatch",
            "run cluster is not authorized",
        ));
    }
    if matches!(mode, RunLoadMode::RecoveryWrite) {
        // Offboarding must not block internal accounting and ownership cleanup.
        let cluster_exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM clusters WHERE id=$1 AND tenant_id=$2)")
                .bind(cluster_id.as_uuid())
                .bind(auth.tenant_id.to_string())
                .fetch_one(&mut **tx)
                .await?;
        if !cluster_exists {
            return Err(ControlPlaneRequestFailure::not_found());
        }
    } else {
        ensure_cluster_scope(tx, auth.tenant_id, cluster_id).await?;
    }
    let state = decode_run_row(&row)?;
    let parent_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM investigations WHERE id=$1 AND tenant_id=$2 AND cluster_id=$3)",
    )
    .bind(state.run.initial_scope.investigation_id.as_uuid())
    .bind(auth.tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .fetch_one(&mut **tx)
    .await?;
    if !parent_exists {
        return Err(ControlPlaneRequestFailure::not_found());
    }
    Ok(state)
}

fn decode_run_row(row: &PgRow) -> Result<LockedRun, ControlPlaneRequestFailure> {
    let run: InvestigationRun = decode(row.try_get("run_snapshot")?)?;
    run.validate()
        .map_err(|source| ControlPlaneRequestFailure::state_source("source_unavailable", source))?;
    let stored_revision: String = row.try_get("revision")?;
    if run.id.as_uuid() != row.try_get::<Uuid, _>("id")?
        || run.initial_scope.tenant_id.as_uuid() != row.try_get::<Uuid, _>("tenant_id")?
        || run.initial_scope.cluster_id.as_uuid() != row.try_get::<Uuid, _>("cluster_id")?
        || run.initial_scope.investigation_id.as_uuid() != row.try_get::<Uuid, _>("investigation_id")?
        || stored_revision != run.revision.get().to_string()
        || row.try_get::<String, _>("status")? != status_name(run.status)
    {
        return Err(ControlPlaneRequestFailure::state(
            "source_unavailable",
            "run scope metadata is inconsistent",
        ));
    }
    Ok(LockedRun {
        run,
        stored_revision,
        head: row.try_get("event_sequence")?,
        floor: row.try_get("event_floor")?,
        owner: row.try_get("worker_owner")?,
        owner_epoch: row.try_get("worker_epoch")?,
        lease_until: row.try_get("worker_lease_until")?,
    })
}

fn bind_context(
    auth: &AuthContext,
    context: &InvestigationRunContext,
    scope: &rocketmq_sre_contracts::InvestigationRunScope,
) -> Result<(), ControlPlaneRequestFailure> {
    if auth.subject != context.actor_subject
        || auth.tenant_id != scope.tenant_id
        || !auth.clusters.contains(&scope.cluster_id)
        || context.permission != InvestigationRunPermission::Control
        || !scope.narrows(&context.authorized_scope)
    {
        return Err(ControlPlaneRequestFailure::forbidden(
            "scope_mismatch",
            "trusted context is not bound to current identity",
        ));
    }
    Ok(())
}

fn status_name(status: InvestigationRunStatus) -> &'static str {
    match status {
        InvestigationRunStatus::Queued => "queued",
        InvestigationRunStatus::Running => "running",
        InvestigationRunStatus::Pausing => "pausing",
        InvestigationRunStatus::Paused => "paused",
        InvestigationRunStatus::NeedsEvidence => "needs_evidence",
        InvestigationRunStatus::Completed => "completed",
        InvestigationRunStatus::Inconclusive => "inconclusive",
        InvestigationRunStatus::Cancelling => "cancelling",
        InvestigationRunStatus::Cancelled => "cancelled",
        InvestigationRunStatus::Failed => "failed",
    }
}

fn encode<T: Serialize>(value: &T) -> Result<Value, ControlPlaneRequestFailure> {
    serde_json::to_value(value).map_err(|source| ControlPlaneRequestFailure::state_source("source_unavailable", source))
}
fn decode<T: DeserializeOwned>(value: Value) -> Result<T, ControlPlaneRequestFailure> {
    serde_json::from_value(value)
        .map_err(|source| ControlPlaneRequestFailure::state_source("source_unavailable", source))
}
fn invalid(detail: &'static str) -> ControlPlaneRequestFailure {
    ControlPlaneRequestFailure::validation("invalid_input", detail)
}
fn conflict(code: &'static str) -> ControlPlaneRequestFailure {
    ControlPlaneRequestFailure::conflict_code(code, "persistent run decision conflicted")
}

use events::persist_event;
