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
use rocketmq_sre_contracts::{
    INVESTIGATION_RUN_SCHEMA_VERSION, InvestigationCommand, InvestigationCommandId, InvestigationInterruptionReason,
    InvestigationRunEventKind,
};

use super::*;

impl PostgresRepository {
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "Only D15 dispatch claims new workers; D14 reaper cannot start work"
        )
    )]
    pub(crate) async fn investigation_run_claim(
        &self,
        auth: &AuthContext,
        id: InvestigationRunId,
        owner: Uuid,
        lease_seconds: u32,
        at: DateTime<Utc>,
    ) -> Result<Option<RunLease>, ControlPlaneRequestFailure> {
        authorize_control(auth)?;
        check_lease_input(owner, lease_seconds)?;
        let mut tx = self.pool.begin().await?;
        let mut state = load_run(&mut tx, auth, id, RunLoadMode::ActiveWrite).await?;
        let now = at.max(Utc::now()).max(state.run.updated_at);
        if state.lease_until.is_some_and(|until| until > now) || state.run.status.is_terminal() {
            tx.commit().await?;
            return Ok(None);
        }
        state.owner_epoch = state
            .owner_epoch
            .checked_add(1)
            .ok_or_else(|| contract_failure(InvestigationRunFailure::CounterExhausted))?;
        let expired = state.owner.is_some();
        state.owner = Some(owner);
        let until = lease_end(now, lease_seconds)?;
        state.lease_until = Some(until);
        if expired {
            recover_attempts(&mut tx, auth, &mut state, now).await?;
        }
        persist_event(
            &mut tx,
            &mut state,
            InvestigationRunEventKind::WorkerClaimed {},
            &auth.subject,
            now,
        )
        .await?;
        let fence = RunLeaseFence {
            owner,
            owner_epoch: state.owner_epoch as u64,
        };
        tx.commit().await?;
        Ok(Some(RunLease {
            run: state.run,
            fence,
            expires_at: until,
        }))
    }

    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "D15 workers renew the leases persisted by D14")
    )]
    pub(crate) async fn investigation_run_renew(
        &self,
        auth: &AuthContext,
        id: InvestigationRunId,
        fence: &RunLeaseFence,
        lease_seconds: u32,
        at: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, ControlPlaneRequestFailure> {
        authorize_control(auth)?;
        check_lease_input(fence.owner, lease_seconds)?;
        let mut tx = self.pool.begin().await?;
        let mut state = load_run(&mut tx, auth, id, RunLoadMode::ActiveWrite).await?;
        let now = at.max(Utc::now()).max(state.run.updated_at);
        check_fence(&state, fence, now)?;
        let until = lease_end(now, lease_seconds)?;
        // A heartbeat does not change a public control revision or append noise.
        sqlx::query("UPDATE investigation_runs SET worker_lease_until=$1 WHERE tenant_id=$2 AND id=$3")
            .bind(until)
            .bind(auth.tenant_id.as_uuid())
            .bind(id.as_uuid())
            .execute(&mut *tx)
            .await?;
        state.lease_until = Some(until);
        tx.commit().await?;
        Ok(until)
    }

    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "D15 workers explicitly release drained leases")
    )]
    pub(crate) async fn investigation_run_release(
        &self,
        auth: &AuthContext,
        id: InvestigationRunId,
        fence: &RunLeaseFence,
        at: DateTime<Utc>,
    ) -> Result<InvestigationRun, ControlPlaneRequestFailure> {
        authorize_control(auth)?;
        let mut tx = self.pool.begin().await?;
        let mut state = load_run(&mut tx, auth, id, RunLoadMode::ActiveWrite).await?;
        let now = at.max(Utc::now()).max(state.run.updated_at);
        check_fence(&state, fence, now)?;
        if state.run.in_flight_count() != 0 {
            return Err(conflict("step_in_flight"));
        }
        pause_running(&mut tx, auth, &mut state, now).await?;
        state.owner = None;
        state.lease_until = None;
        persist_event(
            &mut tx,
            &mut state,
            InvestigationRunEventKind::WorkerReleased {},
            &auth.subject,
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(state.run)
    }

    /// Internal lifecycle inventory, never a public cross-tenant list endpoint.
    pub(crate) async fn investigation_run_recoverable(
        &self,
        limit: u32,
        at: DateTime<Utc>,
    ) -> Result<Vec<RunRecoveryCandidate>, ControlPlaneRequestFailure> {
        if !(1..=32).contains(&limit) {
            return Err(invalid("recovery batch is outside bounds"));
        }
        // Rotate checked metadata even when a later decode fails. A corrupt or
        // offboarded row cannot monopolize the first batch forever. SKIP LOCKED
        // lets independently owned maintenance services share bounded inventory.
        let rows = sqlx::query("WITH candidates AS (SELECT id FROM investigation_runs WHERE worker_owner IS NOT NULL AND worker_lease_until<=$1 ORDER BY recovery_checked_at NULLS FIRST,worker_lease_until,id LIMIT $2 FOR UPDATE SKIP LOCKED) UPDATE investigation_runs r SET recovery_checked_at=clock_timestamp() FROM candidates c WHERE r.id=c.id RETURNING r.tenant_id,r.cluster_id,r.id")
            .bind(at).bind(i64::from(limit)).fetch_all(&self.pool).await?;
        rows.iter()
            .map(|row| {
                Ok(RunRecoveryCandidate {
                    tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
                    cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
                    run_id: InvestigationRunId::from_uuid(row.try_get("id")?),
                })
            })
            .collect()
    }

    /// Reaper never starts queued work or performs external reads. It only
    /// fences a lost owner and settles unknown admission reservations.
    pub(crate) async fn investigation_run_reconcile(
        &self,
        auth: &AuthContext,
        id: InvestigationRunId,
        owner: Uuid,
        lease_seconds: u32,
        at: DateTime<Utc>,
    ) -> Result<Option<InvestigationRun>, ControlPlaneRequestFailure> {
        authorize_control(auth)?;
        check_lease_input(owner, lease_seconds)?;
        let mut tx = self.pool.begin().await?;
        let mut state = load_run(&mut tx, auth, id, RunLoadMode::RecoveryWrite).await?;
        let now = at.max(Utc::now()).max(state.run.updated_at);
        if state.owner.is_none() || state.lease_until.is_some_and(|until| until > now) {
            tx.commit().await?;
            return Ok(None);
        }
        state.owner_epoch = state
            .owner_epoch
            .checked_add(1)
            .ok_or_else(|| contract_failure(InvestigationRunFailure::CounterExhausted))?;
        state.owner = Some(owner);
        state.lease_until = Some(lease_end(now, lease_seconds)?);
        recover_attempts(&mut tx, auth, &mut state, now).await?;
        state.owner = None;
        state.lease_until = None;
        persist_event(
            &mut tx,
            &mut state,
            InvestigationRunEventKind::WorkerReleased {},
            &auth.subject,
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(Some(state.run))
    }
}

#[cfg_attr(
    not(test),
    allow(dead_code, reason = "Fencing guards D15's internal worker operations")
)]
pub(super) fn check_fence(
    state: &LockedRun,
    fence: &RunLeaseFence,
    at: DateTime<Utc>,
) -> Result<(), ControlPlaneRequestFailure> {
    if state.owner != Some(fence.owner)
        || state.owner_epoch as u64 != fence.owner_epoch
        || state.lease_until.is_none_or(|until| until <= at)
    {
        return Err(conflict("worker_lease_lost"));
    }
    Ok(())
}

fn check_lease_input(owner: Uuid, seconds: u32) -> Result<(), ControlPlaneRequestFailure> {
    if owner.is_nil() || !(1..=60).contains(&seconds) {
        return Err(invalid("worker lease is outside bounds"));
    }
    Ok(())
}
fn lease_end(at: DateTime<Utc>, seconds: u32) -> Result<DateTime<Utc>, ControlPlaneRequestFailure> {
    at.checked_add_signed(Duration::seconds(i64::from(seconds)))
        .ok_or_else(|| contract_failure(InvestigationRunFailure::CounterExhausted))
}

fn recovery_context(auth: &AuthContext, run: &InvestigationRun) -> InvestigationRunContext {
    InvestigationRunContext {
        actor_subject: auth.subject.clone(),
        permission: InvestigationRunPermission::Control,
        authorized_scope: run.initial_scope.clone(),
        allowed_packs: vec![],
        allowed_requirements: vec![],
        visible_evidence: vec![],
    }
}

async fn pause_running(
    tx: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    state: &mut LockedRun,
    at: DateTime<Utc>,
) -> Result<(), ControlPlaneRequestFailure> {
    if state.run.status != InvestigationRunStatus::Running {
        return Ok(());
    }
    let request = InvestigationCommandRequest {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        command_id: InvestigationCommandId::new(),
        run_id: state.run.id,
        expected_revision: state.run.revision,
        idempotency_key: format!("worker-recovery:{}:{}", state.owner_epoch, state.run.revision.get()),
        reason: "worker ownership ended; external read cancellation is not asserted".to_owned(),
        command: InvestigationCommand::Pause {},
    };
    let receipt = state
        .run
        .apply_command(&recovery_context(auth, &state.run), &request, None, at)
        .map_err(contract_failure)?;
    insert_command(tx, auth, &request, &receipt).await?;
    persist_event(
        tx,
        state,
        InvestigationRunEventKind::CommandAccepted {
            command_id: request.command_id,
        },
        &auth.subject,
        at,
    )
    .await
}

async fn recover_attempts(
    tx: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    state: &mut LockedRun,
    at: DateTime<Utc>,
) -> Result<(), ControlPlaneRequestFailure> {
    pause_running(tx, auth, state, at).await?;
    let unresolved: Vec<_> = state
        .run
        .steps
        .iter()
        .flat_map(|step| {
            step.attempts
                .iter()
                .filter(|attempt| attempt.result.is_none() && attempt.interruption.is_none())
                .map(|attempt| (step.id, attempt.id))
        })
        .collect();
    for (step_id, attempt_id) in unresolved {
        state
            .run
            .interrupt_attempt(
                &recovery_context(auth, &state.run),
                state.run.revision,
                step_id,
                attempt_id,
                at,
            )
            .map_err(contract_failure)?;
        persist_event(
            tx,
            state,
            InvestigationRunEventKind::AttemptInterrupted {
                step_id,
                attempt_id,
                reason: InvestigationInterruptionReason::WorkerLeaseExpired,
            },
            &auth.subject,
            at,
        )
        .await?;
    }
    Ok(())
}
