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

use rocketmq_sre_contracts::{InvestigationResultAcceptance, InvestigationResultDisposition, InvestigationStepResult};

use super::*;

#[cfg_attr(
    not(test),
    allow(dead_code, reason = "D15 workers use this internal D14 settlement boundary")
)]
impl PostgresRepository {
    /// Serializes distinct-reference charges and preserves the original report
    /// so changed costs or observations cannot masquerade as an exact replay.
    pub(crate) async fn investigation_run_settle(
        &self,
        worker: &RunWorkerContext<'_>,
        submitted: &InvestigationStepResult,
        at: DateTime<Utc>,
    ) -> Result<InvestigationRun, ControlPlaneRequestFailure> {
        let mut tx = self.pool.begin().await?;
        let mut state = load_worker(&mut tx, worker, submitted.run_id, at).await?;
        if submitted.evidence_ids.len() > 256 {
            return Err(contract_failure(InvestigationRunFailure::InvalidInput));
        }
        let mut incoming = submitted.clone();
        incoming.evidence_ids.sort();
        let previous: Option<Value> = sqlx::query_scalar(
            "SELECT submitted_result FROM investigation_run_results WHERE tenant_id=$1 AND run_id=$2 AND attempt_id=$3",
        )
        .bind(worker.auth.tenant_id.as_uuid())
        .bind(submitted.run_id.as_uuid())
        .bind(submitted.attempt_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await?;
        let now = at.max(Utc::now()).max(state.run.updated_at);
        if let Some(previous) = previous {
            let mut previous: InvestigationStepResult = decode(previous)?;
            previous.evidence_ids.sort();
            if previous != incoming {
                return Err(contract_failure(InvestigationRunFailure::ResultConflict));
            }
            let stored = state
                .run
                .steps
                .iter()
                .find(|step| step.id == submitted.step_id)
                .and_then(|step| step.attempts.iter().find(|attempt| attempt.id == submitted.attempt_id))
                .and_then(|attempt| attempt.result.as_ref())
                .cloned()
                .ok_or_else(|| {
                    ControlPlaneRequestFailure::state("source_unavailable", "result receipt has no settled attempt")
                })?;
            if state
                .run
                .apply_step_result(worker.context, &stored, now)
                .map_err(contract_failure)?
                != InvestigationResultAcceptance::Replay
            {
                return Err(ControlPlaneRequestFailure::state(
                    "source_unavailable",
                    "result receipt is inconsistent",
                ));
            }
            tx.commit().await?;
            return Ok(state.run);
        }
        let attempt = state
            .run
            .steps
            .iter()
            .find(|step| step.id == submitted.step_id)
            .ok_or_else(|| contract_failure(InvestigationRunFailure::StepNotFound))?
            .attempts
            .iter()
            .find(|attempt| attempt.id == submitted.attempt_id)
            .ok_or_else(|| contract_failure(InvestigationRunFailure::AttemptMismatch))?;
        // An excessive original charge must not be washed away by normalization.
        if !incoming.actual_cost.fits_within(attempt.reservation)
            || incoming.actual_cost.evidence_items > incoming.evidence_ids.len() as u64
        {
            return Err(contract_failure(InvestigationRunFailure::InvalidInput));
        }
        let observed: BTreeSet<_> = state
            .run
            .supplied_evidence_ids
            .iter()
            .copied()
            .chain(
                state
                    .run
                    .steps
                    .iter()
                    .flat_map(|step| &step.attempts)
                    .filter_map(|attempt| attempt.result.as_ref())
                    .flat_map(|result| result.evidence_ids.iter().copied()),
            )
            .collect();
        let mut canonical = incoming.clone();
        canonical.actual_cost.evidence_items =
            incoming.evidence_ids.iter().filter(|id| !observed.contains(id)).count() as u64;
        let disposition = match state
            .run
            .apply_step_result(worker.context, &canonical, now)
            .map_err(contract_failure)?
        {
            InvestigationResultAcceptance::Replay => {
                return Err(ControlPlaneRequestFailure::state(
                    "source_unavailable",
                    "settled attempt lacks its result receipt",
                ));
            }
            InvestigationResultAcceptance::Applied => InvestigationResultDisposition::Applied,
            InvestigationResultAcceptance::Superseded => InvestigationResultDisposition::Superseded,
        };
        sqlx::query("INSERT INTO investigation_run_results(tenant_id,run_id,attempt_id,submitted_result,recorded_at) VALUES($1,$2,$3,$4,$5)")
            .bind(worker.auth.tenant_id.as_uuid()).bind(submitted.run_id.as_uuid()).bind(submitted.attempt_id.as_uuid()).bind(encode(&incoming)?).bind(now).execute(&mut *tx).await?;
        persist_event(
            &mut tx,
            &mut state,
            InvestigationRunEventKind::StepSettled {
                step_id: submitted.step_id,
                attempt_id: submitted.attempt_id,
                disposition,
            },
            &worker.auth.subject,
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(state.run)
    }
}
