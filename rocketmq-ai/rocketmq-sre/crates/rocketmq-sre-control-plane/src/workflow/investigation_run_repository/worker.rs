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

use rocketmq_sre_contracts::{InvestigationRunEventKind, InvestigationStepAdmission};

use super::ownership::check_fence;
use super::*;

mod settlement;

#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "D15 calls these fenced D14 persistence primitives; no D14 dispatcher exists"
    )
)]
impl PostgresRepository {
    pub(crate) async fn investigation_run_start(
        &self,
        worker: &RunWorkerContext<'_>,
        id: InvestigationRunId,
        expected_revision: InvestigationRevision,
        at: DateTime<Utc>,
    ) -> Result<InvestigationRun, ControlPlaneRequestFailure> {
        let mut tx = self.pool.begin().await?;
        let mut state = load_worker(&mut tx, worker, id, at).await?;
        let now = at.max(Utc::now()).max(state.run.updated_at);
        state
            .run
            .start(worker.context, expected_revision, now)
            .map_err(contract_failure)?;
        persist_event(
            &mut tx,
            &mut state,
            InvestigationRunEventKind::Started {},
            &worker.auth.subject,
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(state.run)
    }

    pub(crate) async fn investigation_run_admit(
        &self,
        worker: &RunWorkerContext<'_>,
        id: InvestigationRunId,
        admission: &InvestigationStepAdmission,
        at: DateTime<Utc>,
    ) -> Result<InvestigationRun, ControlPlaneRequestFailure> {
        let mut tx = self.pool.begin().await?;
        let mut state = load_worker(&mut tx, worker, id, at).await?;
        let now = at.max(Utc::now()).max(state.run.updated_at);
        state
            .run
            .admit_step(worker.context, admission, now)
            .map_err(contract_failure)?;
        persist_event(
            &mut tx,
            &mut state,
            InvestigationRunEventKind::StepAdmitted {
                step_id: admission.step_id,
                attempt_id: admission.attempt_id,
            },
            &worker.auth.subject,
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(state.run)
    }

    pub(crate) async fn investigation_run_finish(
        &self,
        worker: &RunWorkerContext<'_>,
        commit: RunAssessmentCommit<'_>,
        at: DateTime<Utc>,
    ) -> Result<InvestigationRun, ControlPlaneRequestFailure> {
        let mut tx = self.pool.begin().await?;
        let mut state = load_worker(&mut tx, worker, commit.run_id, at).await?;
        let now = at.max(Utc::now()).max(state.run.updated_at);
        state
            .run
            .finish_with_assessment(
                worker.context,
                commit.expected_revision,
                commit.assessment,
                commit.saved_revision,
                now,
            )
            .map_err(contract_failure)?;
        persist_event(
            &mut tx,
            &mut state,
            InvestigationRunEventKind::AssessmentRecorded {
                assessment_id: commit.assessment.id,
            },
            &worker.auth.subject,
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(state.run)
    }

    pub(crate) async fn investigation_run_stop(
        &self,
        worker: &RunWorkerContext<'_>,
        commit: RunStopCommit,
        at: DateTime<Utc>,
    ) -> Result<InvestigationRun, ControlPlaneRequestFailure> {
        let mut tx = self.pool.begin().await?;
        let mut state = load_worker(&mut tx, worker, commit.run_id, at).await?;
        let now = at.max(Utc::now()).max(state.run.updated_at);
        state
            .run
            .stop_without_assessment(worker.context, commit.expected_revision, commit.status, now)
            .map_err(contract_failure)?;
        persist_event(
            &mut tx,
            &mut state,
            InvestigationRunEventKind::Stopped {},
            &worker.auth.subject,
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(state.run)
    }
}

#[cfg_attr(
    not(test),
    allow(dead_code, reason = "Used by the D15 worker facade, not public D14 handlers")
)]
async fn load_worker(
    tx: &mut Transaction<'_, Postgres>,
    worker: &RunWorkerContext<'_>,
    id: InvestigationRunId,
    at: DateTime<Utc>,
) -> Result<LockedRun, ControlPlaneRequestFailure> {
    authorize_control(worker.auth)?;
    let state = load_run(tx, worker.auth, id, RunLoadMode::ActiveWrite).await?;
    bind_context(worker.auth, worker.context, &state.run.initial_scope)?;
    check_fence(&state, worker.fence, at.max(Utc::now()).max(state.run.updated_at))?;
    Ok(state)
}
