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

use chrono::{DateTime, Duration, Utc};
use rocketmq_sre_contracts::*;
use serde_json::json;
use uuid::Uuid;

fn at() -> DateTime<Utc> {
    "2026-11-12T10:00:00Z".parse().unwrap()
}
fn uuid(value: u128) -> Uuid {
    Uuid::from_u128(value)
}
fn evidence(value: u128) -> EvidenceId {
    EvidenceId::from_uuid(uuid(value))
}
fn scope() -> InvestigationRunScope {
    InvestigationRunScope {
        tenant_id: TenantId::from_uuid(uuid(1)),
        cluster_id: ClusterId::from_uuid(uuid(2)),
        investigation_id: InvestigationId::from_uuid(uuid(3)),
        resource: None,
        time_range: TimeRange::new(at() - Duration::hours(1), at()).unwrap(),
    }
}
fn packs() -> Vec<InvestigationPackRef> {
    vec![
        InvestigationPackRef {
            pack_id: "consumer-lag.v2".into(),
            pack_version: "2.0.0".into(),
        },
        InvestigationPackRef {
            pack_id: "store-pressure.v1".into(),
            pack_version: "1.0.0".into(),
        },
    ]
}
fn context() -> InvestigationRunContext {
    InvestigationRunContext {
        actor_subject: "operator-one".into(),
        permission: InvestigationRunPermission::Control,
        authorized_scope: scope(),
        allowed_packs: packs(),
        allowed_requirements: packs()
            .into_iter()
            .zip(["consumer-lag", "store-pressure"])
            .map(|(pack, requirement_id)| InvestigationRequirementRef {
                pack,
                requirement_id: requirement_id.into(),
            })
            .collect(),
        visible_evidence: Vec::new(),
    }
}
fn create_request() -> CreateInvestigationRunRequest {
    CreateInvestigationRunRequest {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.into(),
        run_id: InvestigationRunId::from_uuid(uuid(4)),
        scope: scope(),
        packs: packs(),
        deadline: at() + Duration::minutes(15),
        idempotency_key: "create-one".into(),
        budget: InvestigationBudget {
            limit: InvestigationBudgetCost {
                read_queries: 4,
                model_calls: 2,
                evidence_items: 8,
                output_bytes: 4096,
            },
            max_steps: 8,
            max_in_flight: 2,
            max_attempts_per_step: 2,
            max_query_retries: 2,
        },
    }
}
fn fixture() -> (InvestigationRunContext, InvestigationRun) {
    let context = context();
    let mut run = InvestigationRun::create(&context, &create_request(), None, at())
        .unwrap()
        .run;
    run.start(&context, run.revision, at()).unwrap();
    (context, run)
}
fn proof(context: &mut InvestigationRunContext, id: EvidenceId, target: &InvestigationRunScope) {
    context.visible_evidence.push(InvestigationEvidenceContext {
        evidence_id: id,
        scope: target.clone(),
    });
}
fn collect(run: &InvestigationRun, number: u128, pack: usize) -> InvestigationStepAdmission {
    InvestigationStepAdmission {
        step_id: InvestigationStepId::from_uuid(uuid(100 + number)),
        attempt_id: StepAttemptId::from_uuid(uuid(200 + number)),
        expected_revision: run.revision,
        spec: InvestigationStepSpec {
            scope: run.effective_scope.clone(),
            pack: run.effective_packs[pack].clone(),
            work: InvestigationStepKind::CollectEvidence {
                requirement_id: if pack == 0 { "consumer-lag" } else { "store-pressure" }.into(),
            },
        },
        reservation: InvestigationBudgetCost {
            read_queries: 1,
            model_calls: 0,
            evidence_items: 1,
            output_bytes: 100,
        },
    }
}
fn result(
    run: &InvestigationRun,
    request: &InvestigationStepAdmission,
    ids: Vec<EvidenceId>,
) -> InvestigationStepResult {
    let admitted = run
        .steps
        .iter()
        .find(|step| step.id == request.step_id)
        .unwrap()
        .attempts
        .last()
        .unwrap();
    InvestigationStepResult {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.into(),
        run_id: run.id,
        step_id: request.step_id,
        attempt_id: request.attempt_id,
        control_epoch: admitted.control_epoch,
        outcome: InvestigationStepOutcome::Succeeded,
        failure: None,
        actual_cost: InvestigationBudgetCost {
            read_queries: request.reservation.read_queries,
            model_calls: request.reservation.model_calls,
            evidence_items: ids.len() as u64,
            output_bytes: 20,
        },
        evidence_ids: ids,
        finished_at: run.updated_at + Duration::seconds(1),
    }
}
fn command(run: &InvestigationRun, kind: InvestigationCommand, key: &str) -> InvestigationCommandRequest {
    InvestigationCommandRequest {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.into(),
        command_id: InvestigationCommandId::new(),
        run_id: run.id,
        expected_revision: run.revision,
        idempotency_key: key.into(),
        reason: "Operator reviewed the bounded investigation".into(),
        command: kind,
    }
}
fn apply(
    run: &mut InvestigationRun,
    context: &InvestigationRunContext,
    kind: InvestigationCommand,
    key: &str,
) -> (InvestigationCommandRequest, InvestigationCommandReceipt) {
    let request = command(run, kind, key);
    let receipt = run
        .apply_command(context, &request, None, run.updated_at + Duration::seconds(1))
        .unwrap();
    (request, receipt)
}
fn seed(context: &mut InvestigationRunContext, run: &mut InvestigationRun, ids: &[EvidenceId]) {
    for id in ids {
        proof(context, *id, &run.effective_scope);
    }
    let refinement = InvestigationRefinement {
        scope: run.effective_scope.clone(),
        packs: run.effective_packs.clone(),
        evidence_ids: ids.to_vec(),
    };
    apply(
        run,
        context,
        InvestigationCommand::Steer { refinement },
        "seed-evidence",
    );
}
fn revision(run: &InvestigationRun, ids: Vec<EvidenceId>) -> InvestigationDiagnosisRevision {
    InvestigationDiagnosisRevision {
        id: DiagnosisRevisionId::from_uuid(uuid(500)),
        investigation_id: run.initial_scope.investigation_id,
        conversation_id: ConversationId::from_uuid(uuid(501)),
        turn_id: ConversationTurnId::from_uuid(uuid(502)),
        answer_revision_id: ConversationAnswerRevisionId::from_uuid(uuid(503)),
        revision: 1,
        pack_id: run.effective_packs[0].pack_id.clone(),
        pack_version: run.effective_packs[0].pack_version.clone(),
        status: InvestigationDiagnosisStatus::Inconclusive,
        rule_result: json!({}),
        hypotheses: json!([]),
        evidence_ids: ids,
        primary_model_invocation_id: None,
        execution_eligible: false,
        partial: false,
        correlation_id: CorrelationId::from_uuid(uuid(504)),
        created_at: run.updated_at,
    }
}
fn assessment(
    run: &InvestigationRun,
    revision: &InvestigationDiagnosisRevision,
    conclusion: HypothesisAssessmentConclusion,
) -> HypothesisAssessment {
    HypothesisAssessment {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.into(),
        id: HypothesisAssessmentId::from_uuid(uuid(600)),
        run_id: run.id,
        scope: run.effective_scope.clone(),
        control_epoch: run.control_epoch,
        diagnosis_revision_id: revision.id,
        conclusion,
        summary: "Advisory assessment bound to saved evidence".into(),
        created_at: run.updated_at,
    }
}

#[path = "investigation_run_contracts/control.rs"]
mod control;

#[path = "investigation_run_contracts/steps.rs"]
mod steps;

#[path = "investigation_run_contracts/assessment.rs"]
mod assessment;

#[path = "investigation_run_contracts/validation.rs"]
mod validation;
