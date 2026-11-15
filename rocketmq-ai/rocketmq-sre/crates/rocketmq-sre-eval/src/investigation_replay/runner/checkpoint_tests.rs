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
use serde_json::json;

fn state() -> (InvestigationReplayCase, State, Vec<InvestigationReadBinding>) {
    let mut case = super::super::tests::fixtures::case();
    let mut content: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/diagnostics/namesrv-route-observation.v1.json"
    ))
    .unwrap();
    let old_at = content["collected_at"]
        .as_str()
        .unwrap()
        .parse::<chrono::DateTime<chrono::Utc>>()
        .unwrap();
    case.scope.time_range = TimeRange::new(
        content["requested_window"]["start"].as_str().unwrap().parse().unwrap(),
        content["requested_window"]["end"].as_str().unwrap().parse().unwrap(),
    )
    .unwrap();
    case.scope.resource = Some(InvestigationResource {
        kind: ResourceKind::Topic,
        key: "orders".to_owned(),
    });
    case.as_of = old_at + chrono::Duration::seconds(400);
    case.deadline = old_at + chrono::Duration::minutes(15);
    case.packs = vec![InvestigationPackRef {
        pack_id: "namesrv-route.v2".to_owned(),
        pack_version: "2.0.0".to_owned(),
    }];
    case.baseline_pack = case.packs[0].clone();
    case.bindings = vec![ReplayBinding {
        requirement: InvestigationRequirementRef {
            pack: case.packs[0].clone(),
            requirement_id: "namesrv-route".to_owned(),
        },
        source: "admin-query".to_owned(),
        resource: "namesrv-route/orders".to_owned(),
    }];
    case.reads.clear();
    content["tenant_id"] = json!(case.scope.tenant_id);
    content["cluster_id"] = json!(case.scope.cluster_id);
    let mut old = EvidenceSnapshot::capture(
        EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: case.scope.tenant_id,
            cluster_id: case.scope.cluster_id,
            source: "admin-query".to_owned(),
            resource: "namesrv-route/orders".to_owned(),
            time_range: case.scope.time_range.clone(),
        },
        current_evidence_schema(),
        old_at,
        EvidenceContent::Inline(content),
    )
    .unwrap();
    old.exposure = EvidenceExposure::AdminRpc;
    old.freshness_seconds = 1;
    old.partial = true;
    old.coverage = CoverageStatus::Partial;
    let mut fresh = old.clone();
    fresh.evidence_id = EvidenceId::new();
    fresh.observed_at = case.as_of;
    let EvidenceContent::Inline(value) = &mut fresh.content else {
        panic!()
    };
    value["collection_started_at"] = json!(case.as_of - chrono::Duration::milliseconds(100));
    value["collected_at"] = json!(case.as_of);
    for (index, round) in value["rounds"].as_array_mut().unwrap().iter_mut().enumerate() {
        round["started_at"] = json!(case.as_of - chrono::Duration::milliseconds(100));
        round["finished_at"] = json!(if index == 0 {
            case.as_of - chrono::Duration::milliseconds(100)
        } else {
            case.as_of
        });
        for peer in round["peers"].as_array_mut().unwrap() {
            for kind in ["configuration", "route"] {
                peer[kind]["started_at"] = json!(case.as_of - chrono::Duration::milliseconds(100));
                peer[kind]["finished_at"] = json!(case.as_of - chrono::Duration::milliseconds(100));
            }
        }
    }
    fresh.content_hash = fresh.compute_content_hash().unwrap();
    let authority = context(&case, &case.packs, &[old.clone(), fresh.clone()]).unwrap();
    let mut run = InvestigationRun::create(&authority, &create_request(&case, &case.packs), None, old_at)
        .unwrap()
        .run;
    run.start(&authority, run.revision, old_at).unwrap();
    let admission = InvestigationStepAdmission {
        step_id: InvestigationStepId::new(),
        attempt_id: StepAttemptId::new(),
        expected_revision: run.revision,
        spec: InvestigationStepSpec {
            scope: case.scope.clone(),
            pack: case.packs[0].clone(),
            work: InvestigationStepKind::CollectEvidence {
                requirement_id: "namesrv-route".to_owned(),
            },
        },
        reservation: InvestigationBudgetCost {
            read_queries: 1,
            evidence_items: 1,
            output_bytes: COLLECTION_BYTES,
            model_calls: 0,
        },
    };
    run.admit_step(&authority, &admission, old_at).unwrap();
    let result = InvestigationStepResult {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        run_id: run.id,
        step_id: admission.step_id,
        attempt_id: admission.attempt_id,
        control_epoch: run.control_epoch,
        outcome: InvestigationStepOutcome::Succeeded,
        failure: None,
        evidence_ids: vec![old.evidence_id],
        actual_cost: InvestigationBudgetCost {
            read_queries: 1,
            evidence_items: 1,
            output_bytes: 1,
            model_calls: 0,
        },
        finished_at: old_at,
    };
    run.apply_step_result(&authority, &result, old_at).unwrap();
    let command = InvestigationCommandRequest {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        command_id: InvestigationCommandId::new(),
        run_id: run.id,
        expected_revision: run.revision,
        idempotency_key: "checkpoint-steer".to_owned(),
        reason: "Synthetic checkpoint metadata fixture".to_owned(),
        command: InvestigationCommand::Steer {
            refinement: InvestigationRefinement {
                scope: case.scope.clone(),
                packs: case.packs.clone(),
                evidence_ids: vec![fresh.evidence_id],
            },
        },
    };
    run.apply_command(&authority, &command, None, case.as_of).unwrap();
    let bindings = vec![InvestigationReadBinding {
        requirement: case.bindings[0].requirement.clone(),
        source: "admin-query".to_owned(),
        resource: "namesrv-route/orders".to_owned(),
        scope: case.scope.clone(),
    }];
    (
        case,
        State {
            run,
            revisions: vec![],
            evidence: vec![old, fresh],
            reads: vec![],
            model_callbacks: 0,
            model_invocations: 0,
            accepted_output_bytes: 0,
            checkpointed: false,
            refined: true,
        },
        bindings,
    )
}

#[test]
fn namesrv_checkpoint_keeps_old_domain_metadata_without_renewing_business_inputs() {
    let (case, state, bindings) = state();
    let old = state.evidence[0].evidence_id;
    let fresh = state.evidence[1].evidence_id;
    let restored = restore_checkpoint(&case, state).unwrap();
    let registry = full_registry().unwrap();
    let input = input(&case, &restored, &bindings);
    assert_eq!(
        investigation_namesrv_domain_continuity(&input),
        NamesrvDomainContinuity::Matched
    );
    let selected = investigation_rule_inputs(&registry, &case.packs[0], &input).unwrap();
    assert_eq!(
        selected.iter().map(|snapshot| snapshot.evidence_id).collect::<Vec<_>>(),
        vec![fresh]
    );
    assert!(!restored.run.evidence_ids.contains(&old));
    assert_eq!(restored.model_callbacks, 0);
    assert_eq!(restored.run.budget_usage().unwrap().0.read_queries, 1);
}

#[test]
fn namesrv_checkpoint_cannot_omit_or_change_original_domain_to_reset_first_read() {
    for failure in ["missing", "changed", "hash"] {
        let (case, mut state, _) = state();
        match failure {
            "missing" => {
                state.evidence.remove(0);
            }
            "hash" => state.evidence[0].content_hash = "invalid".to_owned(),
            _ => {
                let EvidenceContent::Inline(value) = &mut state.evidence[1].content else {
                    panic!()
                };
                value["configuration"]["fingerprint_domain"] = json!(format!("sha256:{}", "f".repeat(64)));
                state.evidence[1].content_hash = state.evidence[1].compute_content_hash().unwrap();
            }
        }
        assert!(matches!(
            restore_checkpoint(&case, state),
            Err(InvestigationReplayRejection::InvalidCheckpoint)
        ));
    }
}
