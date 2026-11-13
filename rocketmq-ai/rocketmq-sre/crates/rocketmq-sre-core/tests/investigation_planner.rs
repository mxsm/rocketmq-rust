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
use rocketmq_sre_core::diagnostics::*;
use serde_json::json;

#[path = "investigation_planner/compiled_fields.rs"]
mod compiled_fields;

const fn requirement(source: &'static str) -> EvidenceRequirement {
    EvidenceRequirement {
        key: "shared",
        source,
        resource_prefix: "broker-runtime/",
        purpose: "Synthetic shared readonly fixture",
    }
}
const REQUIRED: &[EvidenceRequirement] = &[requirement("rocketmq-mcp")];

#[derive(Debug)]
struct SharedPack {
    id: &'static str,
    version: PackVersion,
    requirements: &'static [EvidenceRequirement],
}
impl DiagnosticPack for SharedPack {
    fn id(&self) -> &'static str {
        self.id
    }
    fn version(&self) -> PackVersion {
        self.version
    }
    fn applicable_components(&self) -> &'static [&'static str] {
        &["broker"]
    }
    fn required_evidence(&self) -> &'static [EvidenceRequirement] {
        self.requirements
    }
    fn optional_evidence(&self) -> &'static [EvidenceRequirement] {
        &[]
    }
    fn rule_codes(&self) -> &'static [&'static str] {
        &["READY", "MISSING"]
    }
    fn follow_up_queries(&self) -> &'static [FollowUpQuery] {
        &[]
    }
    fn max_evidence_freshness_seconds(&self) -> u64 {
        60
    }
    fn evidence_fields(&self, _: &str) -> Vec<EvidenceFieldContract> {
        vec![EvidenceFieldContract {
            path: "ready".to_owned(),
            kind: EvidenceFieldKind::Boolean,
        }]
    }
    fn evaluate(&self, context: &DiagnosticContext<'_>) -> Result<Vec<RuleMatch>, SreContractError> {
        Ok(match context.cite("shared", "Explicit fixture source") {
            Some(citation) => vec![
                RuleMatch::new(
                    "READY",
                    "Fixture broker is ready",
                    Severity::Info,
                    FindingOutcome::Healthy,
                )
                .with_support(citation),
            ],
            None => vec![
                RuleMatch::new(
                    "MISSING",
                    "No root cause established",
                    Severity::Warning,
                    FindingOutcome::Inconclusive,
                )
                .with_missing("shared"),
            ],
        })
    }
}
fn at() -> DateTime<Utc> {
    "2026-11-12T10:00:00Z".parse().unwrap()
}
fn fixture() -> (DiagnosticPackRegistry, InvestigationRunContext, InvestigationRun) {
    fixture_with_requirements(REQUIRED)
}
fn fixture_with_requirements(
    requirements: &'static [EvidenceRequirement],
) -> (DiagnosticPackRegistry, InvestigationRunContext, InvestigationRun) {
    let mut registry = DiagnosticPackRegistry::default();
    for id in ["pack-a", "pack-b"] {
        registry
            .register(SharedPack {
                id,
                version: PackVersion::new(1, 0, 0),
                requirements,
            })
            .unwrap();
    }
    let packs: Vec<_> = registry
        .active_packs()
        .map(|pack| InvestigationPackRef {
            pack_id: pack.qualified_id(),
            pack_version: pack.version().to_string(),
        })
        .collect();
    let scope = InvestigationRunScope {
        tenant_id: TenantId::new(),
        cluster_id: ClusterId::new(),
        investigation_id: InvestigationId::new(),
        resource: None,
        time_range: TimeRange::new(at() - Duration::minutes(5), at()).unwrap(),
    };
    let ctx = InvestigationRunContext {
        actor_subject: "planner-fixture".to_owned(),
        permission: InvestigationRunPermission::Control,
        authorized_scope: scope.clone(),
        allowed_packs: packs.clone(),
        allowed_requirements: packs
            .iter()
            .map(|pack| InvestigationRequirementRef {
                pack: pack.clone(),
                requirement_id: "shared".to_owned(),
            })
            .collect(),
        visible_evidence: vec![],
    };
    let request = CreateInvestigationRunRequest {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        run_id: InvestigationRunId::new(),
        scope,
        packs,
        budget: InvestigationBudget {
            limit: InvestigationBudgetCost {
                read_queries: 12,
                model_calls: 8,
                evidence_items: 16,
                output_bytes: 65536,
            },
            max_steps: 32,
            max_in_flight: 4,
            max_attempts_per_step: 3,
            max_query_retries: 2,
        },
        deadline: at() + Duration::minutes(15),
        idempotency_key: "planner-create".to_owned(),
    };
    let mut run = InvestigationRun::create(&ctx, &request, None, at()).unwrap().run;
    run.start(&ctx, run.revision, at()).unwrap();
    (registry, ctx, run)
}
fn bindings(run: &InvestigationRun) -> Vec<InvestigationReadBinding> {
    run.initial_packs
        .iter()
        .map(|pack| InvestigationReadBinding {
            requirement: InvestigationRequirementRef {
                pack: pack.clone(),
                requirement_id: "shared".to_owned(),
            },
            source: "rocketmq-mcp".to_owned(),
            resource: "broker-runtime/broker-a".to_owned(),
            scope: run.effective_scope.clone(),
        })
        .collect()
}
fn input<'a>(
    run: &'a InvestigationRun,
    bindings: &'a [InvestigationReadBinding],
    evidence: &'a [EvidenceSnapshot],
) -> InvestigationPlanningInput<'a> {
    InvestigationPlanningInput {
        run,
        bindings,
        evidence,
        as_of_unix_ms: run.updated_at.timestamp_millis(),
        explanation: InvestigationExplanationPolicy::RuleOnly,
        reservations: InvestigationReservationPolicy {
            collection_output_bytes: 64,
            evaluation_output_bytes: 128,
            explanation_output_bytes: 64,
        },
    }
}
fn plan(
    registry: &DiagnosticPackRegistry,
    run: &InvestigationRun,
    evidence: &[EvidenceSnapshot],
) -> InvestigationPlanDecision {
    plan_investigation(registry, input(run, &bindings(run), evidence)).unwrap()
}
fn snapshot(run: &InvestigationRun, partial: bool) -> EvidenceSnapshot {
    let mut evidence = EvidenceSnapshot::capture(
        EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: run.initial_scope.tenant_id,
            cluster_id: run.initial_scope.cluster_id,
            source: "rocketmq-mcp".to_owned(),
            resource: "broker-runtime/broker-a".to_owned(),
            time_range: run.effective_scope.time_range.clone(),
        },
        current_evidence_schema(),
        run.updated_at,
        EvidenceContent::Inline(json!({"ready":true})),
    )
    .unwrap();
    evidence.partial = partial;
    evidence.exposure = EvidenceExposure::Synthetic;
    evidence
}
fn proof(ctx: &mut InvestigationRunContext, run: &InvestigationRun, evidence: &EvidenceSnapshot) {
    ctx.visible_evidence.push(InvestigationEvidenceContext {
        evidence_id: evidence.evidence_id,
        scope: run.effective_scope.clone(),
    });
}
fn admission(
    run: &InvestigationRun,
    spec: InvestigationStepSpec,
    reservation: InvestigationBudgetCost,
    step: Option<InvestigationStepId>,
) -> InvestigationStepAdmission {
    InvestigationStepAdmission {
        step_id: step.unwrap_or_default(),
        attempt_id: StepAttemptId::new(),
        expected_revision: run.revision,
        spec,
        reservation,
    }
}
fn settle(
    ctx: &InvestigationRunContext,
    run: &mut InvestigationRun,
    admission: &InvestigationStepAdmission,
    ids: Vec<EvidenceId>,
    failed: bool,
) {
    let result = InvestigationStepResult {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        run_id: run.id,
        step_id: admission.step_id,
        attempt_id: admission.attempt_id,
        control_epoch: run.control_epoch,
        outcome: if failed {
            InvestigationStepOutcome::Failed
        } else {
            InvestigationStepOutcome::Succeeded
        },
        failure: failed.then_some(InvestigationStepFailure::SourceUnavailable),
        actual_cost: InvestigationBudgetCost {
            evidence_items: ids.iter().filter(|id| !run.evidence_ids.contains(id)).count() as u64,
            output_bytes: 1,
            ..Default::default()
        },
        evidence_ids: ids,
        finished_at: run.updated_at + Duration::milliseconds(1),
    };
    run.apply_step_result(ctx, &result, result.finished_at).unwrap();
}
fn collect(
    ctx: &mut InvestigationRunContext,
    run: &mut InvestigationRun,
    planned: InvestigationPlannedCollection,
    evidence: &EvidenceSnapshot,
) {
    let step = admission(run, planned.spec, planned.reservation, planned.retry_step_id);
    run.admit_step(ctx, &step, run.updated_at).unwrap();
    proof(ctx, run, evidence);
    settle(ctx, run, &step, vec![evidence.evidence_id], false);
}
fn evaluate(
    ctx: &InvestigationRunContext,
    run: &mut InvestigationRun,
    planned: InvestigationPlannedEvaluation,
) -> SavedInvestigationRunRevision {
    let step = admission(run, planned.spec, planned.reservation, None);
    let ids = match &step.spec.work {
        InvestigationStepKind::EvaluateRules { evidence_ids } => evidence_ids.clone(),
        _ => panic!("evaluation"),
    };
    run.admit_step(ctx, &step, run.updated_at).unwrap();
    settle(ctx, run, &step, ids.clone(), false);
    let partial = planned
        .requirements
        .iter()
        .any(|quality| quality.required && quality.quality != DiagnosticEvidenceQuality::Available);
    let status = if partial {
        InvestigationDiagnosisStatus::Inconclusive
    } else {
        InvestigationDiagnosisStatus::Healthy
    };
    let saved = SavedInvestigationRunRevision {
        schema_version: INVESTIGATION_RUN_REVISION_SCHEMA_VERSION.to_owned(),
        id: InvestigationRunRevisionId::new(),
        run_id: run.id,
        scope: step.spec.scope,
        control_epoch: run.control_epoch,
        pack: step.spec.pack,
        step_id: step.step_id,
        attempt_id: step.attempt_id,
        status,
        partial,
        input_fingerprint: planned.input_fingerprint,
        evidence_ids: ids.clone(),
        requirements: planned.requirements,
        findings: vec![InvestigationRunFinding {
            reason_code: "READY".to_owned(),
            root_cause: "Fixture rule output".to_owned(),
            outcome: status,
            supporting_evidence_ids: ids,
            counter_evidence_ids: vec![],
        }],
        created_at: run.updated_at,
    };
    run.record_pack_result(ctx, run.revision, &saved, run.updated_at)
        .unwrap();
    saved
}
fn steer(
    ctx: &InvestigationRunContext,
    run: &mut InvestigationRun,
    packs: Vec<InvestigationPackRef>,
    ids: Vec<EvidenceId>,
) {
    let request = InvestigationCommandRequest {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        command_id: InvestigationCommandId::new(),
        run_id: run.id,
        expected_revision: run.revision,
        idempotency_key: format!("steer-{}", run.revision.get()),
        reason: "Explicit fixture refinement".to_owned(),
        command: InvestigationCommand::Steer {
            refinement: InvestigationRefinement {
                scope: run.effective_scope.clone(),
                packs,
                evidence_ids: ids,
            },
        },
    };
    run.apply_command(ctx, &request, None, run.updated_at).unwrap();
}

#[test]
fn all_selected_packs_share_one_read_but_each_gets_a_real_evaluation() {
    let (registry, mut ctx, mut run) = fixture();
    let sample = snapshot(&run, false);
    let InvestigationPlanDecision::Collect(read) = plan(&registry, &run, &[]) else {
        panic!("one shared read")
    };
    assert_eq!(read.consumers.len(), 2);
    collect(&mut ctx, &mut run, read, &sample);
    for expected in run.effective_packs.clone() {
        let InvestigationPlanDecision::Evaluate(evaluation) = plan(&registry, &run, std::slice::from_ref(&sample))
        else {
            panic!("evaluate every selected pack")
        };
        assert_eq!(evaluation.spec.pack, expected);
        evaluate(&ctx, &mut run, evaluation);
    }
    assert_eq!(run.budget_usage().unwrap().0.read_queries, 1);
    assert_eq!(run.pack_results.len(), 2);
    assert_eq!(
        plan(&registry, &run, &[sample]),
        InvestigationPlanDecision::Finalize { stop_reason: None }
    );
}

#[test]
fn removing_shared_owner_neither_redispatches_success_nor_resets_global_retries() {
    let (registry, mut ctx, mut run) = fixture();
    let sample = snapshot(&run, false);
    let InvestigationPlanDecision::Collect(read) = plan(&registry, &run, &[]) else {
        panic!()
    };
    collect(&mut ctx, &mut run, read, &sample);
    let remaining = vec![run.effective_packs[1].clone()];
    steer(&ctx, &mut run, remaining, vec![sample.evidence_id]);
    assert!(matches!(
        plan(&registry, &run, &[sample]),
        InvestigationPlanDecision::Evaluate(_)
    ));
    let (registry, ctx, mut run) = fixture();
    for _ in 0..3 {
        let InvestigationPlanDecision::Collect(read) = plan(&registry, &run, &[]) else {
            panic!("bounded retry")
        };
        let step = admission(&run, read.spec, read.reservation, read.retry_step_id);
        run.admit_step(&ctx, &step, run.updated_at).unwrap();
        settle(&ctx, &mut run, &step, vec![], true);
    }
    assert_eq!(run.query_retry_count().unwrap(), 2);
    let remaining = vec![run.effective_packs[1].clone()];
    steer(&ctx, &mut run, remaining, vec![]);
    assert!(!matches!(
        plan(&registry, &run, &[]),
        InvestigationPlanDecision::Collect(_)
    ));
    assert_eq!(run.query_retry_count().unwrap(), 2);
}

#[test]
fn same_facts_with_new_ids_do_not_count_as_progress_but_partial_recovery_does() {
    for partial in [false, true] {
        let (registry, mut ctx, mut run) = fixture();
        let first = snapshot(&run, partial);
        let InvestigationPlanDecision::Collect(read) = plan(&registry, &run, &[]) else {
            panic!()
        };
        collect(&mut ctx, &mut run, read, &first);
        let InvestigationPlanDecision::Evaluate(evaluation) = plan(&registry, &run, std::slice::from_ref(&first))
        else {
            panic!()
        };
        evaluate(&ctx, &mut run, evaluation);
        let mut replacement = snapshot(&run, false);
        if partial {
            replacement.observed_at = first.observed_at;
        }
        proof(&mut ctx, &run, &replacement);
        let packs = run.effective_packs.clone();
        steer(&ctx, &mut run, packs, vec![replacement.evidence_id]);
        let result = plan(&registry, &run, &[replacement]);
        if partial {
            assert!(matches!(result, InvestigationPlanDecision::Evaluate(_)));
        } else {
            assert_eq!(
                result,
                InvestigationPlanDecision::Stop {
                    reason: InvestigationStopReason::NoProgress
                }
            );
        }
    }
}

#[test]
fn real_field_types_age_and_exact_resource_are_required_and_pg_microseconds_match() {
    let (registry, mut ctx, mut run) = fixture();
    let mut sample = snapshot(&run, false);
    proof(&mut ctx, &run, &sample);
    let packs = run.effective_packs.clone();
    steer(&ctx, &mut run, packs, vec![sample.evidence_id]);
    let bound = bindings(&run);
    let quality = investigation_requirements(
        &registry,
        &run.effective_packs[0],
        &input(&run, &bound, std::slice::from_ref(&sample)),
    )
    .unwrap();
    assert_eq!(quality[0].quality, DiagnosticEvidenceQuality::Available);
    for case in 0..4 {
        let mut evidence = sample.clone();
        match case {
            0 => {
                evidence.resource = "broker-runtime/broker-ab".to_owned();
                evidence.content_hash = evidence.compute_content_hash().unwrap();
            }
            1 => {
                evidence.content = EvidenceContent::Inline(json!({"ready":0}));
                evidence.content_hash = evidence.compute_content_hash().unwrap();
            }
            2 => evidence.freshness_seconds = 61,
            _ => evidence.partial = true,
        };
        let q =
            investigation_requirements(&registry, &run.effective_packs[0], &input(&run, &bound, &[evidence])).unwrap();
        assert_ne!(q[0].quality, DiagnosticEvidenceQuality::Available);
    }
    let mut foreign = sample.clone();
    foreign.cluster_id = ClusterId::new();
    assert_eq!(
        plan_investigation(&registry, input(&run, &bound, &[foreign])),
        Err(InvestigationRunFailure::ScopeMismatch)
    );
    run.effective_scope.time_range.start += Duration::nanoseconds(1);
    run.initial_scope.time_range.start += Duration::nanoseconds(1);
    ctx.authorized_scope = run.initial_scope.clone();
    sample.time_range = run.effective_scope.time_range.clone();
    sample.time_range.start = DateTime::from_timestamp_micros(sample.time_range.start.timestamp_micros()).unwrap();
    sample.content_hash = sample.compute_content_hash().unwrap();
    let q = investigation_requirements(
        &registry,
        &run.effective_packs[0],
        &input(&run, &bindings(&run), &[sample]),
    )
    .unwrap();
    assert_eq!(q[0].quality, DiagnosticEvidenceQuality::Available);
}

#[test]
fn deadline_and_exhausted_step_budget_stop_without_fabricating_an_evaluation_or_revision() {
    let (registry, ctx, mut run) = fixture();
    let bound = bindings(&run);
    let mut request = input(&run, &bound, &[]);
    request.as_of_unix_ms = run.deadline.timestamp_millis();
    assert_eq!(
        plan_investigation(&registry, request).unwrap(),
        InvestigationPlanDecision::Stop {
            reason: InvestigationStopReason::DeadlineExceeded
        }
    );
    run.budget.max_steps = 1;
    let InvestigationPlanDecision::Collect(read) = plan(&registry, &run, &[]) else {
        panic!()
    };
    let step = admission(&run, read.spec, read.reservation, None);
    run.admit_step(&ctx, &step, run.updated_at).unwrap();
    settle(&ctx, &mut run, &step, vec![], true);
    assert_eq!(
        plan(&registry, &run, &[]),
        InvestigationPlanDecision::Stop {
            reason: InvestigationStopReason::BudgetExhausted
        }
    );
    assert!(run.pack_results.is_empty());
    assert_eq!(run.steps.len(), 1);
}

#[test]
fn insufficient_output_quota_does_not_lower_the_collectors_real_bound() {
    let (registry, _ctx, mut run) = fixture();
    run.budget.limit.output_bytes = 63;
    assert_eq!(
        plan(&registry, &run, &[]),
        InvestigationPlanDecision::Stop {
            reason: InvestigationStopReason::BudgetExhausted
        }
    );
    assert!(run.steps.is_empty());
    run.budget.limit.output_bytes = 64;
    let InvestigationPlanDecision::Collect(read) = plan(&registry, &run, &[]) else {
        panic!("full bound fits")
    };
    assert_eq!(read.reservation.output_bytes, 64);
}

#[test]
fn an_exact_compiled_version_is_never_replaced_by_the_newer_active_version() {
    let (mut registry, _ctx, run) = fixture();
    registry
        .register(SharedPack {
            id: "pack-a",
            version: PackVersion::new(1, 1, 0),
            requirements: REQUIRED,
        })
        .unwrap();
    let InvestigationPlanDecision::Collect(read) = plan(&registry, &run, &[]) else {
        panic!("exact older compiled version")
    };
    assert_eq!(read.spec.pack.pack_version, "1.0.0");
    let mut invalid = run.clone();
    invalid.effective_packs[0].pack_version = "9.0.0".to_owned();
    invalid.initial_packs[0].pack_version = "9.0.0".to_owned();
    assert_eq!(
        plan_investigation(&registry, input(&invalid, &bindings(&invalid), &[])),
        Err(InvestigationRunFailure::PackNotAllowed)
    );
}

#[test]
fn permanent_refusals_and_unknown_outcomes_are_not_automatic_query_retries() {
    for unknown in [false, true] {
        let (registry, ctx, mut run) = fixture();
        let InvestigationPlanDecision::Collect(read) = plan(&registry, &run, &[]) else {
            panic!()
        };
        let admission = admission(&run, read.spec, read.reservation, read.retry_step_id);
        run.admit_step(&ctx, &admission, run.updated_at).unwrap();
        if unknown {
            run.interrupt_with_reason(
                &ctx,
                InvestigationInterruptAttemptRequest {
                    expected_revision: run.revision,
                    step_id: admission.step_id,
                    attempt_id: admission.attempt_id,
                    reason: InvestigationInterruptionReason::SourceOutcomeUnknown,
                },
                run.updated_at,
            )
            .unwrap();
            let packs = run.effective_packs.clone();
            steer(&ctx, &mut run, packs, vec![]);
            let resume = InvestigationCommandRequest {
                schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
                command_id: InvestigationCommandId::new(),
                run_id: run.id,
                expected_revision: run.revision,
                idempotency_key: "resume-unknown".to_owned(),
                reason: "Explicit resumed state is not permission to repeat unknown RPC".to_owned(),
                command: InvestigationCommand::Resume {},
            };
            run.apply_command(&ctx, &resume, None, run.updated_at).unwrap();
        } else {
            let result = InvestigationStepResult {
                schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
                run_id: run.id,
                step_id: admission.step_id,
                attempt_id: admission.attempt_id,
                control_epoch: run.control_epoch,
                outcome: InvestigationStepOutcome::Failed,
                failure: Some(InvestigationStepFailure::SourceRejected),
                evidence_ids: vec![],
                actual_cost: InvestigationBudgetCost {
                    output_bytes: 1,
                    ..Default::default()
                },
                finished_at: run.updated_at,
            };
            run.apply_step_result(&ctx, &result, run.updated_at).unwrap();
        }
        assert!(!matches!(
            plan(&registry, &run, &[]),
            InvestigationPlanDecision::Collect(_)
        ));
        assert_eq!(run.query_retry_count().unwrap(), 0);
        assert_eq!(run.steps[0].attempts.len(), 1);
    }
}

#[test]
fn elapsed_age_is_added_and_new_partial_observations_do_not_fall_back_to_old_green() {
    let (registry, mut ctx, mut run) = fixture();
    let mut old = snapshot(&run, false);
    old.observed_at -= Duration::milliseconds(20);
    old.freshness_seconds = 55;
    let mut newer = snapshot(&run, true);
    newer.observed_at -= Duration::milliseconds(10);
    proof(&mut ctx, &run, &old);
    proof(&mut ctx, &run, &newer);
    let packs = run.effective_packs.clone();
    steer(&ctx, &mut run, packs, vec![old.evidence_id, newer.evidence_id]);
    let bound = bindings(&run);
    let evidence = [old.clone(), newer];
    let mut request = input(&run, &bound, &evidence);
    request.as_of_unix_ms += 10;
    let qualities = investigation_requirements(&registry, &run.effective_packs[0], &request).unwrap();
    assert_eq!(qualities[0].quality, DiagnosticEvidenceQuality::Partial);
    assert_eq!(qualities[0].evidence_ids, vec![evidence[1].evidence_id]);
    let evidence = [old];
    let mut request = input(&run, &bound, &evidence);
    request.as_of_unix_ms += 10_000;
    let qualities = investigation_requirements(&registry, &run.effective_packs[0], &request).unwrap();
    assert_eq!(qualities[0].quality, DiagnosticEvidenceQuality::Stale);
}

#[test]
fn instant_reads_qualify_only_their_actual_point_inside_the_requested_window() {
    const SOURCES: &[&[EvidenceRequirement]] = &[&[requirement("rocketmq-mcp")], &[requirement("admin-query")]];
    for requirements in SOURCES {
        for point in [
            at() - Duration::seconds(10) - Duration::microseconds(1),
            at(),
            at() + Duration::milliseconds(1),
        ] {
            let (registry, mut ctx, mut run) = fixture_with_requirements(requirements);
            run.initial_scope.time_range.start = at() - Duration::seconds(10);
            run.effective_scope.time_range = run.initial_scope.time_range.clone();
            ctx.authorized_scope = run.initial_scope.clone();
            let mut sample = snapshot(&run, false);
            sample.source = requirements[0].source.to_owned();
            sample.observed_at = point;
            sample.content_hash = sample.compute_content_hash().unwrap();
            proof(&mut ctx, &run, &sample);
            let packs = run.effective_packs.clone();
            steer(&ctx, &mut run, packs, vec![sample.evidence_id]);
            let mut bound = bindings(&run);
            for binding in &mut bound {
                binding.source = sample.source.clone();
            }
            let evidence = [sample];
            let mut request = input(&run, &bound, &evidence);
            request.as_of_unix_ms = point.max(at()).timestamp_millis();
            let quality = investigation_requirements(&registry, &run.effective_packs[0], &request).unwrap();
            if point == at() {
                assert_eq!(quality[0].quality, DiagnosticEvidenceQuality::Available);
                assert_eq!(quality[0].reason_code, "instant_point_in_requested_window");
            } else {
                assert_eq!(quality[0].quality, DiagnosticEvidenceQuality::Unknown);
                assert_eq!(quality[0].reason_code, "instant_observation_outside_requested_window");
            }
            assert_eq!(quality[0].evidence_ids, vec![evidence[0].evidence_id]);
        }
    }
}

#[test]
fn historical_window_adapters_allow_late_collection_but_unknown_time_contracts_do_not() {
    const SOURCES: &[&[EvidenceRequirement]] = &[
        &[requirement("prometheus")],
        &[requirement("loki")],
        &[requirement("tempo")],
        &[requirement("unrecognized-fixture-source")],
    ];
    for requirements in SOURCES {
        let (registry, mut ctx, mut run) = fixture_with_requirements(requirements);
        let mut sample = snapshot(&run, false);
        sample.source = requirements[0].source.to_owned();
        sample.observed_at += Duration::milliseconds(1);
        sample.content_hash = sample.compute_content_hash().unwrap();
        proof(&mut ctx, &run, &sample);
        let packs = run.effective_packs.clone();
        steer(&ctx, &mut run, packs, vec![sample.evidence_id]);
        let mut bound = bindings(&run);
        for binding in &mut bound {
            binding.source = sample.source.clone();
        }
        let evidence = [sample];
        let mut request = input(&run, &bound, &evidence);
        request.as_of_unix_ms += 1;
        let quality = investigation_requirements(&registry, &run.effective_packs[0], &request).unwrap();
        if requirements[0].source == "unrecognized-fixture-source" {
            assert_eq!(quality[0].quality, DiagnosticEvidenceQuality::Unknown);
            assert_eq!(quality[0].reason_code, "source_temporal_contract_unknown");
        } else {
            assert_eq!(quality[0].quality, DiagnosticEvidenceQuality::Available);
            assert_eq!(quality[0].reason_code, "compiled_fields_qualified");
        }
        assert_eq!(quality[0].evidence_ids, vec![evidence[0].evidence_id]);
    }
}
