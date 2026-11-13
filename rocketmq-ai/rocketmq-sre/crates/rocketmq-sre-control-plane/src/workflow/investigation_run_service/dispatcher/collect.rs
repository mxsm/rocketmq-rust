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

use std::future::Future;

use super::*;
use crate::{evidence::PersistEvidenceRequest, workflow::investigation_run_repository::RunReadProof};

impl RunDispatcher {
    pub(super) async fn collect(
        &self,
        execution: &RunExecution<'_>,
        run: &InvestigationRun,
        plan: InvestigationPlannedCollection,
    ) -> Result<(), ControlPlaneRequestFailure> {
        let RunExecution {
            service,
            group,
            delegation,
            lease,
        } = *execution;
        let auth = delegation.read_auth(run)?;
        let context = self.context(service, auth, run, &[]).await?;
        let worker = RunWorkerContext {
            auth,
            context: &context,
            fence: &lease.fence,
        };
        let admission = InvestigationStepAdmission {
            step_id: plan.retry_step_id.unwrap_or_default(),
            attempt_id: StepAttemptId::new(),
            expected_revision: run.revision,
            spec: plan.spec,
            reservation: plan.reservation,
        };
        let InvestigationStepKind::CollectEvidence { requirement_id } = &admission.spec.work else {
            return Err(contract_failure(InvestigationRunFailure::InvalidInput));
        };
        let query = EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: run.initial_scope.tenant_id,
            cluster_id: run.initial_scope.cluster_id,
            source: plan.source,
            resource: plan.resource,
            time_range: admission.spec.scope.time_range.clone(),
        };
        let proof = RunReadProof {
            requirement: InvestigationRequirementRef {
                pack: admission.spec.pack.clone(),
                requirement_id: requirement_id.clone(),
            },
            consumers: plan.consumers,
            scope: admission.spec.scope.clone(),
            query: query.clone(),
        };
        let admitted = service
            .repository
            .investigation_run_dispatch_admit(
                delegation,
                &worker,
                &service.catalog,
                run.id,
                &admission,
                RunAdmissionProof::Read(&proof),
            )
            .await?;
        let deadline = run.deadline.min(Utc::now() + chrono::Duration::seconds(10));
        let response = self
            .wait_operation(
                service,
                group,
                delegation,
                lease,
                &admitted,
                self.channel
                    .query_and_wait(auth.tenant_id, query.cluster_id, query.clone(), deadline),
            )
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                // Cancel is best effort and never treated as remote accounting.
                let _ = tokio::time::timeout(
                    Duration::from_millis(250),
                    self.channel
                        .enqueue_cancel(auth.tenant_id, query.cluster_id, query.correlation_id),
                )
                .await;
                return Err(error);
            }
        };
        if response.correlation_id != query.correlation_id
            || response.error_code.as_deref() == Some("deadline_exceeded")
        {
            return Err(unknown_source());
        }
        let response_bytes = serde_json::to_vec(&response).map_err(|_| unknown_source())?.len() as u64;
        if response_bytes > admission.reservation.output_bytes {
            return Err(unknown_source());
        }
        let mut result = step_result(
            &admitted,
            &admission,
            InvestigationStepOutcome::Failed,
            Some(
                if response.retryable
                    && matches!(
                        response.error_code.as_deref(),
                        Some("source_unavailable" | "rate_limited" | "channel_unavailable")
                    )
                {
                    InvestigationStepFailure::SourceUnavailable
                } else {
                    InvestigationStepFailure::SourceRejected
                },
            ),
            vec![],
            InvestigationBudgetCost {
                read_queries: 1,
                output_bytes: response_bytes,
                ..Default::default()
            },
        );
        let extra = if let Some(snapshot) = response.evidence {
            if response.error_code.is_some() || !query_matches(&query, &snapshot) {
                return Err(unknown_source());
            }
            if !matches!(snapshot.content, EvidenceContent::Inline(_))
                || snapshot.verify_content_hash().is_err()
                || snapshot.observed_at > Utc::now()
            {
                return Err(unknown_source());
            }
            let persisted = self
                .evidence
                .persist(
                    auth,
                    PersistEvidenceRequest {
                        investigation_id: Some(run.initial_scope.investigation_id),
                        incident_id: None,
                        evidence: snapshot,
                    },
                )
                .await?;
            result.outcome = InvestigationStepOutcome::Succeeded;
            result.failure = None;
            result.evidence_ids = vec![persisted.evidence_id];
            result.actual_cost = InvestigationBudgetCost {
                read_queries: 1,
                evidence_items: 1,
                output_bytes: response_bytes,
                ..Default::default()
            };
            vec![persisted]
        } else if response.error_code.is_some() {
            vec![]
        } else {
            return Err(unknown_source());
        };
        result.finished_at = Utc::now();
        let current = service.repository.investigation_run_get(auth, run.id).await?;
        let context = self.context(service, auth, &current, &extra).await?;
        let worker = RunWorkerContext {
            auth,
            context: &context,
            fence: &lease.fence,
        };
        service
            .repository
            .investigation_run_settle(&worker, &result, Utc::now())
            .await?;
        Ok(())
    }

    /// Heartbeats are also control checkpoints. Losing a fence or observing a
    /// pause/cancel drops the local future, while cleanup retains unknown cost.
    pub(super) async fn wait_operation<T>(
        &self,
        service: &InvestigationRunService,
        group: &TaskGroup,
        delegation: &RunDelegation,
        lease: &RunLease,
        admitted: &InvestigationRun,
        operation: impl Future<Output = Result<T, ControlPlaneRequestFailure>>,
    ) -> Result<T, ControlPlaneRequestFailure> {
        tokio::pin!(operation);
        let cancellation = group.cancellation_token();
        let mut heartbeat = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
        );
        loop {
            tokio::select! {
                biased;
                ()=cancellation.cancelled()=>return Err(ControlPlaneRequestFailure::state("owner_shutdown","investigation dispatcher owner stopped")),
                result=&mut operation=>return result,
                _=heartbeat.tick()=>{
                    let current=service.repository.investigation_run_get(delegation.auth(),admitted.id).await?;
                    delegation.validate_for(&current)?;
                    if current.status!=InvestigationRunStatus::Running || current.control_epoch!=admitted.control_epoch || Utc::now()>=current.deadline { return Err(unknown_source()); }
                    service.repository.investigation_run_renew(delegation.auth(),admitted.id,&lease.fence,LEASE_SECONDS,Utc::now()).await?;
                }
            }
        }
    }
}

pub(super) fn step_result(
    run: &InvestigationRun,
    admission: &InvestigationStepAdmission,
    outcome: InvestigationStepOutcome,
    failure: Option<InvestigationStepFailure>,
    evidence_ids: Vec<EvidenceId>,
    actual_cost: InvestigationBudgetCost,
) -> InvestigationStepResult {
    InvestigationStepResult {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        run_id: run.id,
        step_id: admission.step_id,
        attempt_id: admission.attempt_id,
        control_epoch: run.control_epoch,
        outcome,
        failure,
        evidence_ids,
        actual_cost,
        finished_at: Utc::now(),
    }
}

pub(super) fn query_matches(query: &EvidenceQuery, evidence: &EvidenceSnapshot) -> bool {
    !evidence.evidence_id.as_uuid().is_nil()
        && evidence.query_id == query.query_id
        && evidence.correlation_id == query.correlation_id
        && evidence.tenant_id == query.tenant_id
        && evidence.cluster_id == query.cluster_id
        && evidence.source == query.source
        && evidence.resource == query.resource
        && evidence.time_range == query.time_range
}

fn unknown_source() -> ControlPlaneRequestFailure {
    ControlPlaneRequestFailure::state(
        "source_outcome_unknown",
        "run source outcome or control boundary changed",
    )
}
