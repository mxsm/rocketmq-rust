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

use rocketmq_sre_contracts::*;

use super::*;
use crate::workflow::investigation_run_repository::RunListFilter;

impl InvestigationRunService {
    pub(crate) async fn list(
        &self,
        auth: &AuthContext,
        id: InvestigationId,
        filter: &mut RunListFilter,
    ) -> Result<InvestigationRunListPage, ControlPlaneRequestFailure> {
        let page = self.repository.investigation_run_list(auth, id, filter).await?;
        let parent_resource = match page.saved_resource.as_deref() {
            None | Some("cluster/overview") => None,
            Some(value) => scope::resource_subject(value, page.cluster_id),
        };
        let unsupported = page
            .saved_resource
            .as_deref()
            .is_some_and(|value| value != "cluster/overview")
            && parent_resource.is_none();
        let mut compiled_packs = self
            .catalog
            .active_packs()
            .map(|pack| InvestigationPackRef {
                pack_id: pack.qualified_id(),
                pack_version: pack.version().to_string(),
            })
            .collect::<Vec<_>>();
        compiled_packs.sort();
        let blocked_reason = if authorize_control(auth).is_err() {
            Some(InvestigationRunControlBlockReason::ControlPermissionRequired)
        } else if unsupported {
            Some(InvestigationRunControlBlockReason::UnsupportedResource)
        } else if compiled_packs.is_empty() {
            Some(InvestigationRunControlBlockReason::PackUnavailable)
        } else {
            None
        };
        let maximum_budget = InvestigationBudget {
            limit: InvestigationBudgetCost {
                read_queries: 12,
                model_calls: 8,
                evidence_items: 256,
                output_bytes: 4_194_304,
            },
            max_steps: 64,
            max_in_flight: 4,
            max_attempts_per_step: 3,
            max_query_retries: 2,
        };
        let default_budget = InvestigationBudget {
            limit: InvestigationBudgetCost {
                model_calls: 1,
                evidence_items: 64,
                ..maximum_budget.limit
            },
            max_steps: 32,
            max_in_flight: 1,
            ..maximum_budget.clone()
        };
        Ok(InvestigationRunListPage {
            schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
            tenant_id: auth.tenant_id,
            cluster_id: page.cluster_id,
            investigation_id: id,
            observed_at: Utc::now(),
            items: page.items,
            next_cursor: page.next_cursor,
            creation: InvestigationRunCreationOptions {
                allowed: blocked_reason.is_none(),
                blocked_reason,
                parent_resource,
                compiled_packs,
                default_budget,
                maximum_budget,
                default_duration_seconds: 900,
                max_duration_seconds: 3600,
                max_window_seconds: 604800,
            },
        })
    }

    pub(crate) async fn controls(
        &self,
        auth: &AuthContext,
        id: InvestigationRunId,
    ) -> Result<InvestigationRunControls, ControlPlaneRequestFailure> {
        let run = self.get(auth, id).await?;
        let mut context = self.context(auth, &run.initial_scope, &run.initial_scope, &[]).await?;
        context.permission = if authorize_control(auth).is_ok() {
            InvestigationRunPermission::Control
        } else {
            InvestigationRunPermission::Read
        };
        let observed_at = Utc::now().max(run.updated_at);
        let (settled, reserved) = run.budget_usage().map_err(contract_failure)?;
        let commands = [
            InvestigationRunCommandKind::Pause,
            InvestigationRunCommandKind::Resume,
            InvestigationRunCommandKind::Cancel,
            InvestigationRunCommandKind::Steer,
        ]
        .into_iter()
        .map(|kind| command_availability(&run, &context, kind, observed_at, settled, reserved))
        .collect();
        Ok(InvestigationRunControls {
            schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
            run,
            observed_at,
            commands,
            receipt_replay_allowed: context.permission == InvestigationRunPermission::Control,
            budget_usage: InvestigationRunBudgetUsage { settled, reserved },
        })
    }
}

fn command_availability(
    run: &InvestigationRun,
    context: &InvestigationRunContext,
    kind: InvestigationRunCommandKind,
    at: chrono::DateTime<Utc>,
    settled: InvestigationBudgetCost,
    reserved: InvestigationBudgetCost,
) -> InvestigationRunCommandAvailability {
    use InvestigationRunControlBlockReason as Reason;
    let mut blocked_reason = if context.permission != InvestigationRunPermission::Control {
        Some(Reason::ControlPermissionRequired)
    } else if run.status.is_terminal() {
        Some(Reason::Terminal)
    } else {
        None
    };
    let state_eligible = match kind {
        InvestigationRunCommandKind::Pause => run.status == InvestigationRunStatus::Running,
        InvestigationRunCommandKind::Resume => run.status == InvestigationRunStatus::Paused,
        InvestigationRunCommandKind::Cancel => run.status != InvestigationRunStatus::Cancelling,
        InvestigationRunCommandKind::Steer => matches!(
            run.status,
            InvestigationRunStatus::Running | InvestigationRunStatus::Paused | InvestigationRunStatus::NeedsEvidence
        ),
    };
    if blocked_reason.is_none() && !state_eligible {
        blocked_reason = Some(Reason::StateNotEligible);
    }
    if blocked_reason.is_none() {
        let packs = run
            .effective_packs
            .iter()
            .filter(|pack| context.allowed_packs.contains(pack))
            .cloned()
            .collect::<Vec<_>>();
        if kind == InvestigationRunCommandKind::Steer && packs.is_empty() {
            return InvestigationRunCommandAvailability {
                kind,
                allowed: false,
                blocked_reason: Some(Reason::PackUnavailable),
            };
        }
        let command = match kind {
            InvestigationRunCommandKind::Pause => InvestigationCommand::Pause {},
            InvestigationRunCommandKind::Resume => InvestigationCommand::Resume {},
            InvestigationRunCommandKind::Cancel => InvestigationCommand::Cancel {},
            InvestigationRunCommandKind::Steer => InvestigationCommand::Steer {
                refinement: InvestigationRefinement {
                    scope: run.effective_scope.clone(),
                    packs,
                    evidence_ids: vec![],
                },
            },
        };
        // This unpersisted preview reuses the actual pure admission checks. It
        // never writes a receipt or treats future Steer references as authorized.
        let request = InvestigationCommandRequest {
            schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
            command_id: InvestigationCommandId::from_uuid(uuid::Uuid::from_u128(1)),
            run_id: run.id,
            expected_revision: run.revision,
            idempotency_key: "read-only-affordance".to_owned(),
            reason: "Check current command admission without external effects".to_owned(),
            command,
        };
        blocked_reason = run
            .clone()
            .apply_command(context, &request, None, at)
            .err()
            .map(block_reason);
        if blocked_reason.is_none() && kind == InvestigationRunCommandKind::Resume {
            // The managed dispatcher needs at least an evaluation admission to
            // make progress; the public command still validates independently.
            let output_remaining = run
                .budget
                .limit
                .output_bytes
                .checked_sub(settled.output_bytes)
                .and_then(|remaining| remaining.checked_sub(reserved.output_bytes));
            if run.steps.iter().map(|step| step.attempts.len()).sum::<usize>() >= run.budget.max_steps as usize
                || output_remaining.is_none_or(|remaining| remaining < 64 * 1024)
            {
                blocked_reason = Some(Reason::BudgetExhausted);
            }
        }
    }
    InvestigationRunCommandAvailability {
        kind,
        allowed: blocked_reason.is_none(),
        blocked_reason,
    }
}

fn block_reason(failure: InvestigationRunFailure) -> InvestigationRunControlBlockReason {
    use InvestigationRunControlBlockReason as Reason;
    match failure {
        InvestigationRunFailure::Unauthorized => Reason::ControlPermissionRequired,
        InvestigationRunFailure::DeadlineExceeded => Reason::DeadlineExceeded,
        InvestigationRunFailure::BudgetExceeded => Reason::BudgetExhausted,
        InvestigationRunFailure::CounterExhausted => Reason::CounterExhausted,
        InvestigationRunFailure::PackNotAllowed => Reason::PackUnavailable,
        InvestigationRunFailure::ScopeMismatch => Reason::ScopeUnavailable,
        InvestigationRunFailure::InvalidInput
        | InvestigationRunFailure::InvalidStateTransition
        | InvestigationRunFailure::RevisionConflict { .. }
        | InvestigationRunFailure::IdempotencyConflict
        | InvestigationRunFailure::StepIdentityConflict
        | InvestigationRunFailure::StepAlreadyCompleted
        | InvestigationRunFailure::StepInFlight
        | InvestigationRunFailure::StepNotFound
        | InvestigationRunFailure::AttemptMismatch
        | InvestigationRunFailure::ResultConflict
        | InvestigationRunFailure::InvalidAssessment => Reason::StateNotEligible,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn availability_reuses_control_transitions_deadlines_counters_and_shared_accounting() {
        let at: chrono::DateTime<Utc> = "2026-11-14T00:00:00Z".parse().unwrap();
        let scope = InvestigationRunScope {
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            investigation_id: InvestigationId::new(),
            resource: None,
            time_range: TimeRange::new(at - chrono::Duration::minutes(5), at).unwrap(),
        };
        let pack = InvestigationPackRef {
            pack_id: "fixture.v1".to_owned(),
            pack_version: "1.0.0".to_owned(),
        };
        let mut context = InvestigationRunContext {
            actor_subject: "controlled-test".to_owned(),
            permission: InvestigationRunPermission::Control,
            authorized_scope: scope.clone(),
            allowed_packs: vec![pack.clone()],
            allowed_requirements: vec![],
            visible_evidence: vec![],
        };
        let request = CreateInvestigationRunRequest {
            schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
            run_id: InvestigationRunId::new(),
            scope,
            packs: vec![pack],
            budget: InvestigationBudget {
                limit: InvestigationBudgetCost {
                    read_queries: 12,
                    model_calls: 1,
                    evidence_items: 2,
                    output_bytes: 131072,
                },
                max_steps: 4,
                max_in_flight: 1,
                max_attempts_per_step: 3,
                max_query_retries: 2,
            },
            deadline: at + chrono::Duration::minutes(5),
            idempotency_key: "affordance-fixture".to_owned(),
        };
        let mut run = InvestigationRun::create(&context, &request, None, at).unwrap().run;
        run.start(&context, run.revision, at).unwrap();
        let project = |run: &InvestigationRun, ctx: &InvestigationRunContext, kind, at| {
            let (settled, reserved) = run.budget_usage().unwrap();
            command_availability(run, ctx, kind, at, settled, reserved)
        };
        assert!(project(&run, &context, InvestigationRunCommandKind::Pause, at).allowed);
        context.permission = InvestigationRunPermission::Read;
        assert_eq!(
            project(&run, &context, InvestigationRunCommandKind::Cancel, at).blocked_reason,
            Some(InvestigationRunControlBlockReason::ControlPermissionRequired)
        );
        context.permission = InvestigationRunPermission::Control;
        let pause = InvestigationCommandRequest {
            schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
            command_id: InvestigationCommandId::new(),
            run_id: run.id,
            expected_revision: run.revision,
            idempotency_key: "pause-fixture".to_owned(),
            reason: "Explicit pause".to_owned(),
            command: InvestigationCommand::Pause {},
        };
        run.apply_command(&context, &pause, None, at).unwrap();
        assert!(project(&run, &context, InvestigationRunCommandKind::Resume, at).allowed);
        assert_eq!(
            project(&run, &context, InvestigationRunCommandKind::Resume, run.deadline).blocked_reason,
            Some(InvestigationRunControlBlockReason::DeadlineExceeded)
        );
        assert!(project(&run, &context, InvestigationRunCommandKind::Cancel, run.deadline).allowed);
        run.budget.limit.output_bytes = 1;
        assert_eq!(
            project(&run, &context, InvestigationRunCommandKind::Resume, at).blocked_reason,
            Some(InvestigationRunControlBlockReason::BudgetExhausted)
        );
        run.revision = InvestigationRevision::from_u64(u64::MAX);
        assert_eq!(
            project(&run, &context, InvestigationRunCommandKind::Cancel, at).blocked_reason,
            Some(InvestigationRunControlBlockReason::CounterExhausted)
        );
    }
}
