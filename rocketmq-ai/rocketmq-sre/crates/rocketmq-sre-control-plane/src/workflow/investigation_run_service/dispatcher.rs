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

use std::time::Duration;

use chrono::Utc;
use rocketmq_runtime::{ChildServiceContext, ScheduledExecutionPolicy, ScheduledTaskConfig, TaskGroup};
use rocketmq_sre_contracts::*;
use rocketmq_sre_core::diagnostics::*;

use super::{InvestigationRunService, contract_failure};
use crate::workflow::investigation_run_repository::{RunAdmissionProof, RunDelegation, RunLease, RunWorkerContext};
use crate::{
    ControlPlaneRequestFailure, auth::AuthContext, connector_channel::PostgresConnectorChannelService,
    evidence::EvidenceService, models::ModelGatewayService,
};

mod collect;
mod context;
mod evaluate;
mod finalize;
#[cfg(test)]
mod tests;

const LEASE_SECONDS: u32 = 30;
const OUTPUT_BYTES: u64 = 64 * 1024;

pub(super) struct RunDispatcher {
    channel: PostgresConnectorChannelService,
    evidence: EvidenceService,
    models: ModelGatewayService,
}

#[derive(Clone, Copy)]
struct RunExecution<'a> {
    service: &'a InvestigationRunService,
    group: &'a TaskGroup,
    delegation: &'a RunDelegation,
    lease: &'a RunLease,
}

impl RunDispatcher {
    pub(super) fn new(
        channel: PostgresConnectorChannelService,
        evidence: EvidenceService,
        models: ModelGatewayService,
    ) -> Self {
        Self {
            channel,
            evidence,
            models,
        }
    }
}

impl InvestigationRunService {
    pub(crate) fn schedule_dispatch(&self, context: &ChildServiceContext) -> Result<(), ControlPlaneRequestFailure> {
        if self.dispatcher.is_none() {
            return Ok(());
        }
        let mut config =
            ScheduledTaskConfig::fixed_rate_no_overlap("investigation-run-dispatch", Duration::from_secs(1));
        config.initial_delay = Duration::ZERO;
        let service = self.clone();
        let group = context.task_group().clone();
        context.scheduled_tasks("investigation-run-dispatch").schedule(config,ScheduledExecutionPolicy::default(),move || {
            let service = service.clone(); let group = group.clone();
            async move {
                let cancellation = group.cancellation_token();
                let candidates = tokio::select! { biased; () = cancellation.cancelled() => return, candidates = service.repository.investigation_run_dispatchable() => candidates };
                match candidates {
                    Ok(ids) => {
                        // Futures remain inside this owned scheduled task. A
                        // fixed width of four bounds SQL, remote reads and leases.
                        let mut ids = ids.into_iter();
                        tokio::join!(service.dispatch_optional(&group,ids.next()),service.dispatch_optional(&group,ids.next()),service.dispatch_optional(&group,ids.next()),service.dispatch_optional(&group,ids.next()));
                    }
                    Err(error) => tracing::warn!(error_code=error.code(),"investigation dispatcher inventory failed"),
                }
            }
        }).map_err(ControlPlaneRequestFailure::configuration_source)?;
        Ok(())
    }

    async fn dispatch_optional(&self, group: &TaskGroup, id: Option<InvestigationRunId>) {
        let Some(id) = id else {
            return;
        };
        if let Err(error) = self.dispatch_one(group, id).await {
            tracing::warn!(error_code = error.code(), "bounded investigation dispatch failed");
        }
    }

    pub(super) async fn dispatch_one(
        &self,
        group: &TaskGroup,
        id: InvestigationRunId,
    ) -> Result<(), ControlPlaneRequestFailure> {
        let Some(runtime) = &self.dispatcher else {
            return Ok(());
        };
        let cancellation = group.cancellation_token();
        let delegation = tokio::select! { biased; () = cancellation.cancelled() => return Ok(()), result=self.repository.investigation_run_delegation(id)=>result? };
        let auth = delegation.auth();
        let lease = tokio::select! { biased; () = cancellation.cancelled()=>return Ok(()), result=self.repository.investigation_run_claim(auth,id,uuid::Uuid::new_v4(),LEASE_SECONDS,Utc::now())=>result? };
        let Some(lease) = lease else {
            return Ok(());
        };
        delegation.validate_for(&lease.run)?;
        let result = tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(ControlPlaneRequestFailure::state("owner_shutdown","investigation dispatcher owner stopped")),
            result = runtime.drive(self,group,&delegation,&lease) => result,
        };
        // Dropping the owned drive cancels local SQL/RPC waits. The remote read
        // may still have run; explicitly preserve unknown reservations. If SQL
        // itself is unavailable the persisted lease lets recovery do the same.
        let cleanup = async {
            let current = self.repository.investigation_run_get(auth, id).await?;
            let context = context::catalog_context(self, auth, &current);
            let worker = RunWorkerContext {
                auth,
                context: &context,
                fence: &lease.fence,
            };
            if current.in_flight_count() != 0 {
                self.repository
                    .investigation_run_interrupt_owned(
                        &worker,
                        id,
                        if cancellation.is_cancelled() {
                            InvestigationInterruptionReason::OwnerShutdown
                        } else if current.steps.iter().any(|step| {
                            matches!(step.spec.work, InvestigationStepKind::ExplainEvidence { .. })
                                && step
                                    .attempts
                                    .iter()
                                    .any(|attempt| attempt.result.is_none() && attempt.interruption.is_none())
                        }) {
                            InvestigationInterruptionReason::ModelOutcomeUnknown
                        } else {
                            InvestigationInterruptionReason::SourceOutcomeUnknown
                        },
                    )
                    .await?;
            } else if result.is_err() && current.status == InvestigationRunStatus::Running {
                let reason = match result.as_ref().err().map(ControlPlaneRequestFailure::code) {
                    Some("output_too_large") => InvestigationStopReason::BudgetExhausted,
                    Some("not_found" | "invalid_evidence" | "invalid_content_hash") => {
                        InvestigationStopReason::EvidenceInsufficient
                    }
                    Some("source_unavailable" | "source_outcome_unknown") => InvestigationStopReason::SourceUnavailable,
                    _ => InvestigationStopReason::NoProgress,
                };
                self.repository
                    .investigation_run_stop_planned(&worker, id, current.revision, reason)
                    .await?;
            }
            self.repository
                .investigation_run_release(auth, id, &lease.fence, Utc::now())
                .await?;
            Ok::<_, ControlPlaneRequestFailure>(())
        };
        let _ = tokio::time::timeout(Duration::from_secs(2), cleanup).await;
        result
    }
}

impl RunDispatcher {
    async fn drive(
        &self,
        service: &InvestigationRunService,
        group: &TaskGroup,
        delegation: &RunDelegation,
        lease: &RunLease,
    ) -> Result<(), ControlPlaneRequestFailure> {
        let auth = delegation.auth();
        let id = lease.run.id;
        let execution = RunExecution {
            service,
            group,
            delegation,
            lease,
        };
        for _ in 0..64 {
            let run = service.repository.investigation_run_get(auth, id).await?;
            delegation.validate_for(&run)?;
            service
                .repository
                .investigation_run_renew(auth, id, &lease.fence, LEASE_SECONDS, Utc::now())
                .await?;
            let context = self.context(service, auth, &run, &[]).await?;
            let worker = RunWorkerContext {
                auth,
                context: &context,
                fence: &lease.fence,
            };
            if run.status == InvestigationRunStatus::Queued {
                if Utc::now() >= run.deadline {
                    service
                        .repository
                        .investigation_run_stop_planned(
                            &worker,
                            id,
                            run.revision,
                            InvestigationStopReason::DeadlineExceeded,
                        )
                        .await?;
                    return Ok(());
                }
                service
                    .repository
                    .investigation_run_start(&worker, id, run.revision, Utc::now())
                    .await?;
                continue;
            }
            if run.status != InvestigationRunStatus::Running {
                return Ok(());
            }
            let evidence = self.snapshots(auth, &run.evidence_ids).await?;
            let mut connection = service.repository.pool.acquire().await?;
            let mut bindings = super::bindings::resolve(&mut connection, &service.catalog, &run).await?;
            drop(connection);
            for proof in service.repository.investigation_run_read_proofs(auth, &run).await? {
                let binding = proof.binding();
                if !bindings.contains(&binding) {
                    bindings.push(binding);
                }
            }
            let input = self.planning_input(&run, &bindings, &evidence);
            match plan_investigation(&service.catalog, input).map_err(contract_failure)? {
                InvestigationPlanDecision::Collect(plan) => self.collect(&execution, &run, plan).await?,
                InvestigationPlanDecision::Evaluate(plan) => self.evaluate(&execution, &run, plan, &evidence).await?,
                InvestigationPlanDecision::Explain(plan) => self.explain(&execution, &run, plan, &evidence).await?,
                InvestigationPlanDecision::Finalize { stop_reason } => {
                    self.finalize(service, lease, &run, stop_reason, &bindings, &evidence)
                        .await?;
                    return Ok(());
                }
                InvestigationPlanDecision::Stop { reason } => {
                    service
                        .repository
                        .investigation_run_stop_planned(&worker, id, run.revision, reason)
                        .await?;
                    return Ok(());
                }
                InvestigationPlanDecision::Wait => return Ok(()),
            }
        }
        let current = service.repository.investigation_run_get(auth, id).await?;
        if current.status == InvestigationRunStatus::Running && current.in_flight_count() == 0 {
            let context = self.context(service, auth, &current, &[]).await?;
            service
                .repository
                .investigation_run_stop_planned(
                    &RunWorkerContext {
                        auth,
                        context: &context,
                        fence: &lease.fence,
                    },
                    id,
                    current.revision,
                    InvestigationStopReason::NoProgress,
                )
                .await?;
        }
        Ok(())
    }

    fn planning_input<'a>(
        &self,
        run: &'a InvestigationRun,
        bindings: &'a [InvestigationReadBinding],
        evidence: &'a [EvidenceSnapshot],
    ) -> InvestigationPlanningInput<'a> {
        InvestigationPlanningInput {
            run,
            bindings,
            evidence,
            as_of_unix_ms: Utc::now().timestamp_millis(),
            explanation: if self.models.investigation_explanation_enabled() {
                InvestigationExplanationPolicy::ExplainOnce
            } else {
                InvestigationExplanationPolicy::RuleOnly
            },
            reservations: InvestigationReservationPolicy {
                collection_output_bytes: crate::connector_channel::MAX_RESPONSE_BYTES as u64,
                evaluation_output_bytes: OUTPUT_BYTES,
                explanation_output_bytes: OUTPUT_BYTES,
            },
        }
    }
}
