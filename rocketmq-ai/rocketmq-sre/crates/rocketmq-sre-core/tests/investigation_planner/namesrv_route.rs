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

use super::*;

fn setup() -> (
    DiagnosticPackRegistry,
    InvestigationRunContext,
    InvestigationRun,
    EvidenceSnapshot,
    Vec<InvestigationReadBinding>,
) {
    let registry = full_registry().unwrap();
    let pack = InvestigationPackRef {
        pack_id: "namesrv-route.v2".to_owned(),
        pack_version: "2.0.0".to_owned(),
    };
    let (_, mut ctx, mut run) = fixture();
    let mut content: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/diagnostics/namesrv-route-observation.v1.json"
    ))
    .unwrap();
    let observed: DateTime<Utc> = content["collected_at"].as_str().unwrap().parse().unwrap();
    let scope = InvestigationRunScope {
        time_range: TimeRange::new(
            content["requested_window"]["start"].as_str().unwrap().parse().unwrap(),
            content["requested_window"]["end"].as_str().unwrap().parse().unwrap(),
        )
        .unwrap(),
        ..run.initial_scope.clone()
    };
    ctx.authorized_scope = scope.clone();
    run.initial_scope = scope.clone();
    run.effective_scope = scope.clone();
    run.created_at = observed;
    run.updated_at = observed;
    run.deadline = observed + Duration::minutes(15);
    run.initial_packs = vec![pack.clone()];
    run.effective_packs = run.initial_packs.clone();
    ctx.allowed_packs = run.initial_packs.clone();
    ctx.allowed_requirements = vec![InvestigationRequirementRef {
        pack: pack.clone(),
        requirement_id: "namesrv-route".to_owned(),
    }];
    content["tenant_id"] = json!(scope.tenant_id);
    content["cluster_id"] = json!(scope.cluster_id);
    let mut sample = EvidenceSnapshot::capture(
        EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: scope.tenant_id,
            cluster_id: scope.cluster_id,
            source: "admin-query".to_owned(),
            resource: "namesrv-route/orders".to_owned(),
            time_range: scope.time_range.clone(),
        },
        current_evidence_schema(),
        observed,
        EvidenceContent::Inline(content),
    )
    .unwrap();
    sample.freshness_seconds = 1;
    sample.exposure = EvidenceExposure::AdminRpc;
    sample.partial = false;
    sample.coverage = CoverageStatus::Available;
    let bound = vec![InvestigationReadBinding {
        requirement: ctx.allowed_requirements[0].clone(),
        source: sample.source.clone(),
        resource: sample.resource.clone(),
        scope,
    }];
    (registry, ctx, run, sample, bound)
}
fn recapture(original: &EvidenceSnapshot, seconds: i64) -> EvidenceSnapshot {
    fn clocks(value: &mut serde_json::Value, shift: Duration) {
        match value {
            serde_json::Value::Object(object) => {
                for (key, value) in object {
                    if ["collection_started_at", "collected_at", "started_at", "finished_at"].contains(&key.as_str())
                        && !value.is_null()
                    {
                        let at: DateTime<Utc> = value.as_str().unwrap().parse().unwrap();
                        *value = json!(at + shift);
                    } else {
                        clocks(value, shift);
                    }
                }
            }
            serde_json::Value::Array(array) => {
                for value in array {
                    clocks(value, shift);
                }
            }
            _ => {}
        }
    }
    let mut sample = original.clone();
    sample.evidence_id = EvidenceId::new();
    sample.query_id = QueryId::new();
    sample.correlation_id = CorrelationId::new();
    sample.observed_at += Duration::seconds(seconds);
    let EvidenceContent::Inline(content) = &mut sample.content else {
        panic!("inline fixture")
    };
    clocks(content, Duration::seconds(seconds));
    sample.content_hash = sample.compute_content_hash().unwrap();
    sample
}
fn refine(ctx: &InvestigationRunContext, run: &mut InvestigationRun, sample: &EvidenceSnapshot) {
    let request = InvestigationCommandRequest {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        command_id: InvestigationCommandId::new(),
        run_id: run.id,
        expected_revision: run.revision,
        idempotency_key: format!("namesrv-steer-{}", run.revision.get()),
        reason: "Explicit same-scope supplied fixture".to_owned(),
        command: InvestigationCommand::Steer {
            refinement: InvestigationRefinement {
                scope: run.effective_scope.clone(),
                packs: run.effective_packs.clone(),
                evidence_ids: vec![sample.evidence_id],
            },
        },
    };
    run.apply_command(ctx, &request, None, sample.observed_at + Duration::milliseconds(10))
        .unwrap();
}

#[test]
fn namesrv_ended_window_keeps_actual_current_advisory_input_and_hard_missing_facts() {
    let (registry, mut ctx, mut run, sample, bound) = setup();
    assert!(run.initial_scope.time_range.end < sample.observed_at);
    let InvestigationPlanDecision::Collect(read) = plan_investigation(&registry, input(&run, &bound, &[])).unwrap()
    else {
        panic!("one canonical read")
    };
    collect(&mut ctx, &mut run, read, &sample);
    let evidence = [sample.clone()];
    let planning = input(&run, &bound, &evidence);
    let quality = investigation_requirements(&registry, &run.effective_packs[0], &planning).unwrap();
    assert_eq!(quality[0].quality, DiagnosticEvidenceQuality::Partial);
    assert!(
        quality[0]
            .missing_fields
            .iter()
            .any(|fact| fact == "historical_window_coverage")
    );
    let selected = investigation_rule_inputs(&registry, &run.effective_packs[0], &planning).unwrap();
    assert_eq!(selected.len(), 1);
    let report = DiagnosticEngine::new(full_registry().unwrap())
        .evaluate("namesrv-route.v2", &selected)
        .unwrap();
    assert_eq!(report.status, DiagnosticStatus::Inconclusive);
    assert!(report.findings.iter().any(
        |finding| finding.reason_code == "NAMESRV_CURRENT_ROUTE_DIFFERENCE_OBSERVED"
            && finding.supporting_evidence[0].evidence_id == sample.evidence_id
    ));
}

#[test]
fn namesrv_old_business_age_does_not_discard_domain_anchor_or_renew_its_old_business_facts() {
    for change_route in [false, true] {
        let (registry, mut ctx, mut run, old, bound) = setup();
        let InvestigationPlanDecision::Collect(read) = plan_investigation(&registry, input(&run, &bound, &[])).unwrap()
        else {
            panic!()
        };
        collect(&mut ctx, &mut run, read, &old);
        let InvestigationPlanDecision::Evaluate(evaluation) =
            plan_investigation(&registry, input(&run, &bound, std::slice::from_ref(&old))).unwrap()
        else {
            panic!()
        };
        evaluate(&ctx, &mut run, evaluation);
        let mut fresh = recapture(&old, 400);
        if change_route {
            if let EvidenceContent::Inline(content) = &mut fresh.content {
                for round in content["rounds"].as_array_mut().unwrap() {
                    round["peers"][0]["route"]["normalized"]["queues"]["broker-a"]["perm"] = json!(4);
                }
            }
            fresh.content_hash = fresh.compute_content_hash().unwrap();
        }
        proof(&mut ctx, &run, &fresh);
        refine(&ctx, &mut run, &fresh);
        let pool = [old.clone(), fresh.clone()];
        let planning = input(&run, &bound, &pool);
        assert_eq!(
            investigation_namesrv_domain_continuity(&planning),
            NamesrvDomainContinuity::Matched
        );
        let selected = investigation_rule_inputs(&registry, &run.effective_packs[0], &planning).unwrap();
        assert_eq!(
            selected.iter().map(|item| item.evidence_id).collect::<Vec<_>>(),
            vec![fresh.evidence_id]
        );
        let decision = plan_investigation(&registry, planning).unwrap();
        if change_route {
            assert!(matches!(decision, InvestigationPlanDecision::Evaluate(_)));
        } else {
            assert_eq!(
                decision,
                InvestigationPlanDecision::Stop {
                    reason: InvestigationStopReason::NoProgress
                }
            );
        }
    }
}

#[test]
fn namesrv_missing_conflicting_or_corrupt_original_metadata_stops_before_evaluation_or_model() {
    for kind in ["domain", "missing", "hash"] {
        let (registry, mut ctx, mut run, mut old, bound) = setup();
        let InvestigationPlanDecision::Collect(read) = plan_investigation(&registry, input(&run, &bound, &[])).unwrap()
        else {
            panic!()
        };
        collect(&mut ctx, &mut run, read, &old);
        let mut fresh = recapture(&old, 400);
        if kind == "domain" {
            if let EvidenceContent::Inline(content) = &mut fresh.content {
                content["configuration"]["fingerprint_domain"] = json!(format!("sha256:{}", "f".repeat(64)));
            }
            fresh.content_hash = fresh.compute_content_hash().unwrap();
        }
        proof(&mut ctx, &run, &fresh);
        refine(&ctx, &mut run, &fresh);
        let before = run.budget_usage().unwrap();
        if kind == "hash" {
            old.content_hash = "invalid".to_owned();
        }
        let pool = if kind == "missing" {
            vec![fresh]
        } else {
            vec![old, fresh]
        };
        assert_eq!(
            plan_investigation(&registry, input(&run, &bound, &pool)).unwrap(),
            InvestigationPlanDecision::Stop {
                reason: InvestigationStopReason::NoProgress
            }
        );
        assert_eq!(run.budget_usage().unwrap(), before);
        assert!(
            run.steps
                .iter()
                .all(|step| matches!(step.spec.work, InvestigationStepKind::CollectEvidence { .. }))
        );
    }
}
