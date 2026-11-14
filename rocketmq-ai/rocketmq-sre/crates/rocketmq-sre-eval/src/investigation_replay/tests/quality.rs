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
use rocketmq_sre_core::diagnostics::*;

#[test]
fn final_aggregation_rechecks_used_optional_support_at_a_fixed_later_clock() {
    let mut case = fixtures::broker_case();
    let runtime = case
        .reads
        .iter_mut()
        .find(|read| read.source == "rocketmq-mcp")
        .unwrap();
    runtime.outcomes = vec![ReplayReadOutcome::SavedEvidence {
        snapshot: snapshot(
            &case.scope,
            "rocketmq-mcp",
            "broker-runtime/broker-a",
            case.as_of - chrono::Duration::seconds(299),
            json!({"broker_up":false}),
        ),
    }];
    let pair = replay_pair(&case).unwrap();
    assert_eq!(pair.multi_pack.observation.status, InvestigationRunStatus::Completed);
    assert!(
        pair.multi_pack
            .observation
            .findings
            .iter()
            .any(|finding| finding.reason_code == "BROKER_NOT_READY")
    );
    let evidence = case
        .reads
        .iter()
        .flat_map(|read| &read.outcomes)
        .filter_map(|outcome| match outcome {
            ReplayReadOutcome::SavedEvidence { snapshot } => Some(snapshot.as_ref().clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let bindings = case
        .bindings
        .iter()
        .map(|binding| InvestigationReadBinding {
            requirement: binding.requirement.clone(),
            source: binding.source.clone(),
            resource: binding.resource.clone(),
            scope: case.scope.clone(),
        })
        .collect::<Vec<_>>();
    let registry = full_registry().unwrap();
    let input = InvestigationPlanningInput {
        run: &pair.multi_pack.snapshot,
        bindings: &bindings,
        evidence: &evidence,
        as_of_unix_ms: (case.as_of + chrono::Duration::seconds(2)).timestamp_millis(),
        explanation: InvestigationExplanationPolicy::RuleOnly,
        reservations: InvestigationReservationPolicy {
            collection_output_bytes: validation::COLLECTION_BYTES,
            evaluation_output_bytes: validation::LOCAL_BYTES,
            explanation_output_bytes: validation::LOCAL_BYTES,
        },
    };
    let qualities = investigation_requirements(&registry, &case.baseline_pack, &input).unwrap();
    assert_eq!(
        qualities
            .iter()
            .find(|quality| quality.id == "broker-metrics")
            .unwrap()
            .quality,
        DiagnosticEvidenceQuality::Available
    );
    assert_eq!(
        qualities
            .iter()
            .find(|quality| quality.id == "broker-runtime")
            .unwrap()
            .quality,
        DiagnosticEvidenceQuality::Stale
    );
    let InvestigationAggregateDecision::Assess {
        conclusion: HypothesisAssessmentConclusion::Inconclusive {
            missing_requirements, ..
        },
        ..
    } = aggregate_investigation(&registry, &input, &pair.multi_pack.revisions, None).unwrap()
    else {
        panic!("stale actual support must degrade");
    };
    assert!(
        missing_requirements
            .iter()
            .any(|requirement| requirement.requirement_id == "broker-runtime")
    );
}

#[test]
fn late_instant_snapshots_are_context_while_a_real_requested_window_source_remains_qualified() {
    let mut case = fixtures::broker_case();
    case.scope.time_range.end = case.as_of - chrono::Duration::seconds(1);
    for read in &mut case.reads {
        if let ReplayReadOutcome::SavedEvidence { snapshot: old } = &read.outcomes[0] {
            let EvidenceContent::Inline(content) = old.content.clone() else {
                unreachable!();
            };
            let content = if read.source == "rocketmq-mcp" {
                json!({"broker_up":false})
            } else {
                content
            };
            read.outcomes = vec![ReplayReadOutcome::SavedEvidence {
                snapshot: snapshot(&case.scope, &read.source, &read.resource, case.as_of, content),
            }];
        }
    }
    let result = replay_pair(&case).unwrap().multi_pack;
    assert_eq!(result.observation.status, InvestigationRunStatus::Completed);
    assert!(matches!(
        result.observation.conclusion,
        Some(HypothesisAssessmentConclusion::Healthy { .. })
    ));
    let requirements = &result.revisions[0].requirements;
    assert_eq!(
        requirements
            .iter()
            .find(|quality| quality.id == "broker-metrics")
            .unwrap()
            .quality,
        DiagnosticEvidenceQuality::Available
    );
    let runtime = requirements
        .iter()
        .find(|quality| quality.id == "broker-runtime")
        .unwrap();
    assert_eq!(runtime.quality, DiagnosticEvidenceQuality::Unknown);
    assert_eq!(runtime.reason_code, "instant_observation_outside_requested_window");
    assert_eq!(runtime.evidence_ids.len(), 1);
    assert!(
        !result
            .observation
            .findings
            .iter()
            .any(|finding| finding.reason_code == "BROKER_NOT_READY")
    );
}

#[test]
fn response_byte_accounting_uses_the_complete_success_and_error_envelopes() {
    let mut case = case();
    case.budget.limit.output_bytes = 600_000; // fits runtime's 512 KiB; not a 640 KiB reservation
    let result = replay_pair(&case).unwrap().multi_pack;
    assert_eq!(result.read_callbacks, 3);
    for (position, step) in result
        .snapshot
        .steps
        .iter()
        .filter(|step| matches!(step.spec.work, InvestigationStepKind::CollectEvidence { .. }))
        .enumerate()
    {
        let response = case
            .reads
            .iter()
            .find(|read| match &step.spec.work {
                InvestigationStepKind::CollectEvidence { requirement_id } => case.bindings.iter().any(|binding| {
                    binding.requirement.pack == step.spec.pack
                        && &binding.requirement.requirement_id == requirement_id
                        && binding.source == read.source
                        && binding.resource == read.resource
                }),
                _ => false,
            })
            .unwrap();
        let frame = validation::response_frame(&response.outcomes[0], position as u64 + 1);
        let expected = serde_json::to_vec(&frame).unwrap().len() as u64;
        assert_eq!(
            step.attempts[0].result.as_ref().unwrap().actual_cost.output_bytes,
            expected
        );
        assert!(expected > serde_json::to_vec(frame.evidence.as_ref().unwrap()).unwrap().len() as u64);
    }
    case.reads[0].outcomes = vec![ReplayReadOutcome::Rejected {}];
    let rejected = replay_pair(&case).unwrap().multi_pack;
    assert!(
        rejected.snapshot.steps[0].attempts[0]
            .result
            .as_ref()
            .unwrap()
            .actual_cost
            .output_bytes
            > 0
    );
}
