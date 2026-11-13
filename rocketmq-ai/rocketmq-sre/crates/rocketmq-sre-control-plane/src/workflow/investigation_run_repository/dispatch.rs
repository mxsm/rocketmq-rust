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

use rocketmq_sre_contracts::{
    EvidenceQuery, InvestigationRequirementRef, InvestigationRunRevisionId, InvestigationStepAdmission,
    InvestigationStepKind, InvestigationStepResult, SavedInvestigationRunRevision,
};
use rocketmq_sre_core::diagnostics::{DiagnosticPackRegistry, InvestigationReadBinding};
use serde::Deserialize;

use super::*;

/// Server-created query identity and canonical target, committed before I/O.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunReadProof {
    pub requirement: InvestigationRequirementRef,
    pub consumers: Vec<InvestigationRequirementRef>,
    pub scope: rocketmq_sre_contracts::InvestigationRunScope,
    pub query: EvidenceQuery,
}

pub(crate) enum RunAdmissionProof<'a> {
    Read(&'a RunReadProof),
    Model(rocketmq_sre_contracts::CorrelationId),
    Rules,
}

impl RunReadProof {
    pub(crate) fn binding(&self) -> InvestigationReadBinding {
        InvestigationReadBinding {
            requirement: self.requirement.clone(),
            source: self.query.source.clone(),
            resource: self.query.resource.clone(),
            scope: self.scope.clone(),
        }
    }
}

impl PostgresRepository {
    pub(crate) async fn investigation_run_dispatchable(
        &self,
    ) -> Result<Vec<InvestigationRunId>, ControlPlaneRequestFailure> {
        // Rotate malformed rows as well as valid ones. Workers never deserialize
        // arbitrary tenants into an operator; the immutable receipt is next.
        let rows = sqlx::query("WITH candidates AS (SELECT id FROM investigation_runs WHERE status IN ('queued','running') AND worker_owner IS NULL ORDER BY dispatch_checked_at NULLS FIRST,created_at,id LIMIT 4 FOR UPDATE SKIP LOCKED) UPDATE investigation_runs r SET dispatch_checked_at=clock_timestamp() FROM candidates c WHERE r.id=c.id RETURNING r.id")
            .fetch_all(&self.pool).await?;
        rows.iter()
            .map(|row| Ok(InvestigationRunId::from_uuid(row.try_get("id")?)))
            .collect()
    }

    pub(crate) async fn investigation_run_read_proofs(
        &self,
        auth: &AuthContext,
        run: &InvestigationRun,
    ) -> Result<Vec<RunReadProof>, ControlPlaneRequestFailure> {
        if auth.tenant_id != run.initial_scope.tenant_id || !auth.clusters.contains(&run.initial_scope.cluster_id) {
            return Err(ControlPlaneRequestFailure::not_found());
        }
        let rows: Vec<Value> = sqlx::query_scalar("SELECT read_snapshot FROM investigation_run_reads WHERE tenant_id=$1 AND run_id=$2 ORDER BY recorded_at,attempt_id LIMIT 13")
            .bind(auth.tenant_id.as_uuid()).bind(run.id.as_uuid()).fetch_all(&self.pool).await?;
        if rows.len() > 12 {
            return Err(invalid("read history exceeds the shared budget"));
        }
        rows.into_iter().map(decode).collect()
    }

    /// Rechecks delegation, saved parent, current topology and exact compiled
    /// canonical target under the same lock as the shared budget reservation.
    pub(crate) async fn investigation_run_dispatch_admit(
        &self,
        delegation: &RunDelegation,
        worker: &RunWorkerContext<'_>,
        registry: &DiagnosticPackRegistry,
        run_id: InvestigationRunId,
        admission: &InvestigationStepAdmission,
        proof: RunAdmissionProof<'_>,
    ) -> Result<InvestigationRun, ControlPlaneRequestFailure> {
        let mut tx = self.pool.begin().await?;
        let mut state = worker::load_worker(&mut tx, worker, run_id, Utc::now()).await?;
        delegation.validate_for(&state.run)?;
        validate_parent(&mut tx, worker.auth, &state.run).await?;
        let read = match &proof {
            RunAdmissionProof::Read(read) => Some(*read),
            _ => None,
        };
        if let Some(read) = read {
            let current =
                super::super::investigation_run_service::bindings::resolve(&mut tx, registry, &state.run).await?;
            if read.scope != state.run.effective_scope
                || read.scope != admission.spec.scope
                || read.query.tenant_id != read.scope.tenant_id
                || read.query.cluster_id != read.scope.cluster_id
                || read.query.time_range != read.scope.time_range
                || read.query.query_id.as_uuid().is_nil()
                || read.query.correlation_id.as_uuid().is_nil()
                || read.requirement.pack != admission.spec.pack
                || !matches!(&admission.spec.work, InvestigationStepKind::CollectEvidence { requirement_id } if requirement_id == &read.requirement.requirement_id)
                || !current.iter().any(|binding| binding == &read.binding())
                || read.consumers.is_empty()
                || read.consumers.len() > 64
                || read.consumers.iter().any(|consumer| {
                    !current.iter().any(|binding| {
                        &binding.requirement == consumer
                            && binding.source == read.query.source
                            && binding.resource == read.query.resource
                            && binding.scope == read.scope
                    })
                })
            {
                return Err(contract_failure(InvestigationRunFailure::ScopeMismatch));
            }
        } else if matches!(admission.spec.work, InvestigationStepKind::CollectEvidence { .. }) {
            return Err(invalid("collection requires a committed canonical query proof"));
        }
        match (&admission.spec.work, &proof) {
            (InvestigationStepKind::ExplainEvidence { .. }, RunAdmissionProof::Model(correlation))
                if !correlation.as_uuid().is_nil() && admission.reservation.output_bytes >= 65536 => {}
            (InvestigationStepKind::EvaluateRules { .. }, RunAdmissionProof::Rules) => {}
            (InvestigationStepKind::CollectEvidence { .. }, RunAdmissionProof::Read(_)) => {}
            _ => return Err(invalid("admission proof does not match the fixed work kind")),
        }
        let now = Utc::now().max(state.run.updated_at);
        state
            .run
            .admit_step(worker.context, admission, now)
            .map_err(contract_failure)?;
        if let Some(read) = read {
            sqlx::query("INSERT INTO investigation_run_reads(tenant_id,run_id,attempt_id,read_snapshot,recorded_at) VALUES($1,$2,$3,$4,$5)")
                .bind(worker.auth.tenant_id.as_uuid()).bind(run_id.as_uuid()).bind(admission.attempt_id.as_uuid()).bind(encode(read)?).bind(now).execute(&mut *tx).await?;
        }
        if let RunAdmissionProof::Model(correlation) = proof {
            sqlx::query("INSERT INTO investigation_run_model_requests(tenant_id,run_id,attempt_id,correlation_id,response_byte_limit,recorded_at) VALUES($1,$2,$3,$4,65536,$5)").bind(worker.auth.tenant_id.as_uuid()).bind(run_id.as_uuid()).bind(admission.attempt_id.as_uuid()).bind(correlation.as_uuid()).bind(now).execute(&mut *tx).await?;
        }
        persist_event(
            &mut tx,
            &mut state,
            rocketmq_sre_contracts::InvestigationRunEventKind::StepAdmitted {
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

    /// A real rules revision and its successful Evaluate result are indivisible.
    pub(crate) async fn investigation_run_save_evaluation(
        &self,
        worker: &RunWorkerContext<'_>,
        result: &InvestigationStepResult,
        revision: &SavedInvestigationRunRevision,
    ) -> Result<InvestigationRun, ControlPlaneRequestFailure> {
        let mut tx = self.pool.begin().await?;
        let mut state = worker::load_worker(&mut tx, worker, result.run_id, Utc::now()).await?;
        revision.validate().map_err(contract_failure)?;
        let original: Option<Value> = sqlx::query_scalar(
            "SELECT submitted_result FROM investigation_run_results WHERE tenant_id=$1 AND run_id=$2 AND attempt_id=$3",
        )
        .bind(worker.auth.tenant_id.as_uuid())
        .bind(result.run_id.as_uuid())
        .bind(result.attempt_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(original) = original {
            let previous:Option<Value>=sqlx::query_scalar("SELECT revision_snapshot FROM investigation_run_revisions WHERE tenant_id=$1 AND run_id=$2 AND attempt_id=$3").bind(worker.auth.tenant_id.as_uuid()).bind(result.run_id.as_uuid()).bind(result.attempt_id.as_uuid()).fetch_optional(&mut *tx).await?;
            if decode::<InvestigationStepResult>(original)? != *result
                || previous
                    .map(decode::<SavedInvestigationRunRevision>)
                    .transpose()?
                    .is_some_and(|previous| previous != *revision)
            {
                return Err(conflict("result_conflict"));
            }
            tx.commit().await?;
            return Ok(state.run);
        }
        if revision.run_id != state.run.id
            || revision.step_id != result.step_id
            || revision.attempt_id != result.attempt_id
            || result.actual_cost.evidence_items != 0
            || result.outcome != rocketmq_sre_contracts::InvestigationStepOutcome::Succeeded
        {
            return Err(invalid("evaluation revision and result differ"));
        }
        let now = Utc::now().max(state.run.updated_at);
        let disposition = match state
            .run
            .apply_step_result(worker.context, result, now)
            .map_err(contract_failure)?
        {
            rocketmq_sre_contracts::InvestigationResultAcceptance::Applied => {
                rocketmq_sre_contracts::InvestigationResultDisposition::Applied
            }
            rocketmq_sre_contracts::InvestigationResultAcceptance::Superseded => {
                rocketmq_sre_contracts::InvestigationResultDisposition::Superseded
            }
            rocketmq_sre_contracts::InvestigationResultAcceptance::Replay => return Err(conflict("result_conflict")),
        };
        if disposition == rocketmq_sre_contracts::InvestigationResultDisposition::Applied {
            state
                .run
                .record_pack_result(worker.context, state.run.revision, revision, now)
                .map_err(contract_failure)?;
            sqlx::query("INSERT INTO investigation_run_revisions(id,tenant_id,cluster_id,investigation_id,run_id,step_id,attempt_id,revision_snapshot,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)")
            .bind(revision.id.as_uuid()).bind(revision.scope.tenant_id.as_uuid()).bind(revision.scope.cluster_id.as_uuid()).bind(revision.scope.investigation_id.as_uuid()).bind(revision.run_id.as_uuid()).bind(revision.step_id.as_uuid()).bind(revision.attempt_id.as_uuid()).bind(encode(revision)?).bind(revision.created_at).execute(&mut *tx).await?;
        }
        sqlx::query("INSERT INTO investigation_run_results(tenant_id,run_id,attempt_id,submitted_result,recorded_at) VALUES($1,$2,$3,$4,$5)")
            .bind(worker.auth.tenant_id.as_uuid()).bind(result.run_id.as_uuid()).bind(result.attempt_id.as_uuid()).bind(encode(result)?).bind(now).execute(&mut *tx).await?;
        persist_event(
            &mut tx,
            &mut state,
            rocketmq_sre_contracts::InvestigationRunEventKind::StepSettled {
                step_id: result.step_id,
                attempt_id: result.attempt_id,
                disposition,
            },
            &worker.auth.subject,
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(state.run)
    }

    pub(crate) async fn investigation_run_revision(
        &self,
        auth: &AuthContext,
        run_id: InvestigationRunId,
        id: InvestigationRunRevisionId,
    ) -> Result<SavedInvestigationRunRevision, ControlPlaneRequestFailure> {
        let run = self.investigation_run_get(auth, run_id).await?;
        let value: Value = sqlx::query_scalar(
            "SELECT revision_snapshot FROM investigation_run_revisions WHERE tenant_id=$1 AND run_id=$2 AND id=$3",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(run_id.as_uuid())
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(ControlPlaneRequestFailure::not_found)?;
        let revision: SavedInvestigationRunRevision = decode(value)?;
        revision.validate().map_err(contract_failure)?;
        if revision.id != id || revision.run_id != run_id || !revision.scope.narrows(&run.initial_scope) {
            return Err(ControlPlaneRequestFailure::not_found());
        }
        Ok(revision)
    }

    pub(crate) async fn investigation_run_finish_planned(
        &self,
        worker: &RunWorkerContext<'_>,
        run_id: InvestigationRunId,
        expected: InvestigationRevision,
        assessment: &HypothesisAssessment,
    ) -> Result<InvestigationRun, ControlPlaneRequestFailure> {
        let mut tx = self.pool.begin().await?;
        let mut state = worker::load_worker(&mut tx, worker, run_id, Utc::now()).await?;
        // Do not treat an in-memory projection or a guessed revision ID as a
        // saved diagnosis. Every selected result is rebound to its immutable row.
        for pack in &state.run.effective_packs {
            if let Some(result) = state.run.current_pack_result(pack) {
                let value: Value = sqlx::query_scalar("SELECT revision_snapshot FROM investigation_run_revisions WHERE tenant_id=$1 AND run_id=$2 AND id=$3")
                    .bind(worker.auth.tenant_id.as_uuid()).bind(run_id.as_uuid()).bind(result.run_revision_id.as_uuid()).fetch_one(&mut *tx).await?;
                let saved: SavedInvestigationRunRevision = decode(value)?;
                if saved.pack_result().map_err(contract_failure)? != *result {
                    return Err(conflict("result_conflict"));
                }
            }
        }
        let basis = assessment
            .run_revision_id
            .ok_or_else(|| invalid("assessment needs a saved run revision"))?;
        let value: Value = sqlx::query_scalar(
            "SELECT revision_snapshot FROM investigation_run_revisions WHERE tenant_id=$1 AND run_id=$2 AND id=$3",
        )
        .bind(worker.auth.tenant_id.as_uuid())
        .bind(run_id.as_uuid())
        .bind(basis.as_uuid())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(ControlPlaneRequestFailure::not_found)?;
        let saved: SavedInvestigationRunRevision = decode(value)?;
        let now = Utc::now().max(state.run.updated_at);
        state
            .run
            .finish_planned_assessment(worker.context, expected, assessment, &saved, now)
            .map_err(contract_failure)?;
        persist_event(
            &mut tx,
            &mut state,
            rocketmq_sre_contracts::InvestigationRunEventKind::AssessmentRecorded {
                assessment_id: assessment.id,
            },
            &worker.auth.subject,
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(state.run)
    }

    pub(crate) async fn investigation_run_save_explanation(
        &self,
        worker: &RunWorkerContext<'_>,
        result: &InvestigationStepResult,
        invocation: rocketmq_sre_contracts::ModelInvocationId,
        summary: Option<&str>,
    ) -> Result<InvestigationRun, ControlPlaneRequestFailure> {
        let mut tx = self.pool.begin().await?;
        let mut state = worker::load_worker(&mut tx, worker, result.run_id, Utc::now()).await?;
        let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM model_invocations m JOIN investigation_run_model_requests q ON q.correlation_id=m.correlation_id AND q.tenant_id=m.tenant_id WHERE m.id=$1 AND m.tenant_id=$2 AND m.cluster_id=$3 AND m.investigation_id=$4 AND m.purpose='summary' AND m.schema_version='rocketmq-sre.run-explanation.v1' AND q.run_id=$5 AND q.attempt_id=$6)")
            .bind(invocation.as_uuid()).bind(worker.auth.tenant_id.as_uuid()).bind(state.run.initial_scope.cluster_id.as_uuid()).bind(state.run.initial_scope.investigation_id.as_uuid()).bind(result.run_id.as_uuid()).bind(result.attempt_id.as_uuid()).fetch_one(&mut *tx).await?;
        if !exists {
            return Err(invalid("explanation has no bound model invocation"));
        }
        if let Some(row)=sqlx::query("SELECT invocation_id,summary FROM investigation_run_explanations WHERE tenant_id=$1 AND run_id=$2 AND attempt_id=$3").bind(worker.auth.tenant_id.as_uuid()).bind(result.run_id.as_uuid()).bind(result.attempt_id.as_uuid()).fetch_optional(&mut *tx).await? {
            let original:Value=sqlx::query_scalar("SELECT submitted_result FROM investigation_run_results WHERE tenant_id=$1 AND run_id=$2 AND attempt_id=$3").bind(worker.auth.tenant_id.as_uuid()).bind(result.run_id.as_uuid()).bind(result.attempt_id.as_uuid()).fetch_one(&mut *tx).await?;
            if row.try_get::<Uuid,_>("invocation_id")?!=invocation.as_uuid() || row.try_get::<Option<String>,_>("summary")?.as_deref()!=summary || decode::<InvestigationStepResult>(original)?!=*result { return Err(conflict("result_conflict")); }
            tx.commit().await?; return Ok(state.run);
        }
        let step = state
            .run
            .steps
            .iter()
            .find(|step| step.id == result.step_id)
            .ok_or_else(|| invalid("explanation step is missing"))?;
        if !matches!(step.spec.work, InvestigationStepKind::ExplainEvidence { .. })
            || result.actual_cost.evidence_items != 0
            || summary.is_some_and(|summary| summary.chars().count() > 8000)
        {
            return Err(invalid("explanation is not bound to an admitted explanation step"));
        }
        let now = Utc::now().max(state.run.updated_at);
        let disposition = match state
            .run
            .apply_step_result(worker.context, result, now)
            .map_err(contract_failure)?
        {
            rocketmq_sre_contracts::InvestigationResultAcceptance::Applied => {
                rocketmq_sre_contracts::InvestigationResultDisposition::Applied
            }
            rocketmq_sre_contracts::InvestigationResultAcceptance::Superseded => {
                rocketmq_sre_contracts::InvestigationResultDisposition::Superseded
            }
            rocketmq_sre_contracts::InvestigationResultAcceptance::Replay => return Err(conflict("result_conflict")),
        };
        sqlx::query("INSERT INTO investigation_run_explanations(tenant_id,run_id,attempt_id,invocation_id,summary,recorded_at) VALUES($1,$2,$3,$4,$5,$6)").bind(worker.auth.tenant_id.as_uuid()).bind(result.run_id.as_uuid()).bind(result.attempt_id.as_uuid()).bind(invocation.as_uuid()).bind(summary).bind(now).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO investigation_run_results(tenant_id,run_id,attempt_id,submitted_result,recorded_at) VALUES($1,$2,$3,$4,$5)").bind(worker.auth.tenant_id.as_uuid()).bind(result.run_id.as_uuid()).bind(result.attempt_id.as_uuid()).bind(encode(result)?).bind(now).execute(&mut *tx).await?;
        persist_event(
            &mut tx,
            &mut state,
            rocketmq_sre_contracts::InvestigationRunEventKind::StepSettled {
                step_id: result.step_id,
                attempt_id: result.attempt_id,
                disposition,
            },
            &worker.auth.subject,
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(state.run)
    }

    pub(crate) async fn investigation_run_stop_planned(
        &self,
        worker: &RunWorkerContext<'_>,
        run_id: InvestigationRunId,
        expected: InvestigationRevision,
        reason: rocketmq_sre_contracts::InvestigationStopReason,
    ) -> Result<InvestigationRun, ControlPlaneRequestFailure> {
        let mut tx = self.pool.begin().await?;
        let mut state = worker::load_worker(&mut tx, worker, run_id, Utc::now()).await?;
        let now = Utc::now().max(state.run.updated_at);
        state
            .run
            .stop_with_reason(worker.context, expected, reason, now)
            .map_err(contract_failure)?;
        persist_event(
            &mut tx,
            &mut state,
            rocketmq_sre_contracts::InvestigationRunEventKind::Stopped {},
            &worker.auth.subject,
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(state.run)
    }

    pub(crate) async fn investigation_run_interrupt_owned(
        &self,
        worker: &RunWorkerContext<'_>,
        run_id: InvestigationRunId,
        reason: rocketmq_sre_contracts::InvestigationInterruptionReason,
    ) -> Result<InvestigationRun, ControlPlaneRequestFailure> {
        let mut tx = self.pool.begin().await?;
        let mut state = worker::load_worker(&mut tx, worker, run_id, Utc::now()).await?;
        let pending = state
            .run
            .steps
            .iter()
            .flat_map(|step| {
                step.attempts
                    .iter()
                    .filter(|attempt| attempt.result.is_none() && attempt.interruption.is_none())
                    .map(move |attempt| (step.id, attempt.id))
            })
            .collect::<Vec<_>>();
        let now = Utc::now().max(state.run.updated_at);
        for (step_id, attempt_id) in pending {
            state
                .run
                .interrupt_with_reason(
                    worker.context,
                    rocketmq_sre_contracts::InvestigationInterruptAttemptRequest {
                        expected_revision: state.run.revision,
                        step_id,
                        attempt_id,
                        reason,
                    },
                    now,
                )
                .map_err(contract_failure)?;
            persist_event(
                &mut tx,
                &mut state,
                rocketmq_sre_contracts::InvestigationRunEventKind::AttemptInterrupted {
                    step_id,
                    attempt_id,
                    reason,
                },
                &worker.auth.subject,
                now,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(state.run)
    }
}

async fn validate_parent(
    tx: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    run: &InvestigationRun,
) -> Result<(), ControlPlaneRequestFailure> {
    // The share locks serialize admission with offboarding and parent changes.
    let cluster: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM clusters WHERE id=$1 AND tenant_id=$2 AND onboarding_state<>'offboarded' FOR SHARE",
    )
    .bind(run.initial_scope.cluster_id.as_uuid())
    .bind(auth.tenant_id.to_string())
    .fetch_optional(&mut **tx)
    .await?;
    if cluster.is_none() {
        return Err(ControlPlaneRequestFailure::not_found());
    }
    let saved: Option<Option<String>> = sqlx::query_scalar(
        "SELECT resource FROM investigations WHERE id=$1 AND tenant_id=$2 AND cluster_id=$3 FOR SHARE",
    )
    .bind(run.initial_scope.investigation_id.as_uuid())
    .bind(auth.tenant_id.as_uuid())
    .bind(run.initial_scope.cluster_id.as_uuid())
    .fetch_optional(&mut **tx)
    .await?;
    let saved = saved.ok_or_else(ControlPlaneRequestFailure::not_found)?;
    if let Some(saved) = saved.filter(|resource| resource != "cluster/overview")
        && super::super::investigation_run_service::scope::resource_subject(&saved, run.initial_scope.cluster_id)
            .as_ref()
            != run.initial_scope.resource.as_ref()
    {
        return Err(contract_failure(InvestigationRunFailure::ScopeMismatch));
    }
    Ok(())
}
