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
use rocketmq_sre_contracts::*;
use serde_json::json;

mod adjudication;
mod files;
mod fixtures;
mod quality;

use fixtures::{case, counter_case, snapshot};

#[test]
fn same_conditions_drive_real_admissions_and_share_the_cross_pack_read() {
    let case = case();
    let pair = replay_pair(&case).unwrap();
    assert_eq!(pair.baseline.read_callbacks, 2);
    assert_eq!(pair.multi_pack.read_callbacks, 3);
    assert_eq!(
        pair.multi_pack
            .read_observations
            .iter()
            .find(|read| read.resource == "store-trend/broker-a")
            .unwrap()
            .callbacks,
        1
    );
    for arm in [&pair.baseline, &pair.multi_pack] {
        assert_eq!(arm.snapshot.initial_scope, case.scope);
        assert_eq!(arm.snapshot.budget, case.budget);
        assert_eq!(arm.snapshot.deadline, case.deadline);
        assert_eq!(arm.observation.as_of, case.as_of);
        assert_eq!(arm.observation.status, InvestigationRunStatus::Completed);
        assert!(
            matches!(&arm.observation.conclusion,Some(HypothesisAssessmentConclusion::RootCause { statement,supporting_evidence_ids,counter_evidence_ids }) if statement == "Store disk usage has crossed the configured pressure threshold" && supporting_evidence_ids.len()==1 && counter_evidence_ids.is_empty())
        );
        assert_eq!(arm.revisions.len(), arm.observation.packs.len());
        assert_eq!(arm.settled.read_queries, u64::from(arm.read_callbacks));
        assert_eq!(arm.known_reported_cost.read_queries, u64::from(arm.read_callbacks));
        assert_eq!(arm.known_reported_cost.output_bytes, arm.accepted_output_bytes);
        assert_eq!(arm.reserved, InvestigationBudgetCost::default());
        assert_eq!(arm.model_callbacks, 0);
        for revision in &arm.revisions {
            let step = arm
                .snapshot
                .steps
                .iter()
                .find(|step| step.id == revision.step_id)
                .unwrap();
            assert!(matches!(step.spec.work, InvestigationStepKind::EvaluateRules { .. }));
            assert_eq!(
                step.attempts[0].result.as_ref().unwrap().outcome,
                InvestigationStepOutcome::Succeeded
            );
            assert_eq!(
                arm.snapshot.current_pack_result(&revision.pack),
                Some(&revision.pack_result().unwrap())
            );
        }
    }
}

#[test]
fn actual_counterevidence_is_preserved_and_prevents_a_strong_cross_pack_conclusion() {
    let case = counter_case();
    let pair = replay_pair(&case).unwrap();
    assert_eq!(pair.multi_pack.observation.status, InvestigationRunStatus::Inconclusive);
    assert!(matches!(
        pair.multi_pack.observation.conclusion,
        Some(HypothesisAssessmentConclusion::Inconclusive { .. })
    ));
    assert!(
        pair.multi_pack
            .observation
            .findings
            .iter()
            .any(|finding| !finding.counter_evidence_ids.is_empty())
    );
    assert!(
        pair.multi_pack
            .revisions
            .iter()
            .any(|revision| revision.pack_result().unwrap().counter_evidence_ids.len() == 1)
    );
}

#[test]
fn absent_and_null_required_signals_keep_their_actual_gap_in_the_saved_assessment() {
    let mut case = case();
    case.bindings
        .retain(|binding| binding.requirement.requirement_id != "store-integrity");
    case.reads.retain(|read| !read.resource.starts_with("store-integrity/"));
    let mut content = json!({"disk_pressure":null,"flush_or_dispatch_stalled":false,"tiered_wal_pressure":false});
    let read = case
        .reads
        .iter_mut()
        .find(|read| read.resource.starts_with("store-pressure/"))
        .unwrap();
    read.outcomes = vec![ReplayReadOutcome::SavedEvidence {
        snapshot: snapshot(
            &case.scope,
            "admin-query",
            "store-pressure/broker-a",
            case.as_of,
            content.take(),
        ),
    }];
    let pair = replay_pair(&case).unwrap();
    assert_eq!(pair.multi_pack.observation.status, InvestigationRunStatus::Inconclusive);
    let Some(HypothesisAssessmentConclusion::Inconclusive {
        reason,
        missing_requirements,
        considered_evidence_ids,
    }) = &pair.multi_pack.observation.conclusion
    else {
        panic!("real inconclusive assessment");
    };
    assert_eq!(*reason, InvestigationStopReason::EvidenceInsufficient);
    assert_eq!(
        missing_requirements
            .iter()
            .map(|requirement| requirement.requirement_id.as_str())
            .collect::<Vec<_>>(),
        vec!["store-pressure", "store-integrity"]
    );
    assert!(!considered_evidence_ids.is_empty());
    let requirement = &pair.multi_pack.revisions[0].requirements[0];
    assert_eq!(requirement.quality, DiagnosticEvidenceQuality::Partial);
    assert!(requirement.missing_fields.contains(&"disk_pressure".to_owned()));
    assert!(pair.multi_pack.revisions.iter().all(|revision| revision.partial));
}

#[test]
fn model_unavailable_is_once_only_and_keeps_rule_only_results_while_unknown_keeps_reservation() {
    let mut case = case();
    let rules = replay_pair(&case).unwrap();
    case.model = ReplayModel::InvokedFailure;
    let failed = replay_pair(&case).unwrap();
    assert_eq!(failed.multi_pack.model_callbacks, 1);
    assert_eq!(failed.multi_pack.model_invocations, 1);
    assert_eq!(failed.multi_pack.known_reported_cost.model_calls, 1);
    assert_eq!(
        failed.multi_pack.observation.conclusion,
        rules.multi_pack.observation.conclusion
    );
    assert_eq!(failed.multi_pack.revisions.len(), 2);
    case.model = ReplayModel::NoEligibleProvider;
    let no_provider = replay_pair(&case).unwrap();
    assert_eq!(no_provider.multi_pack.model_callbacks, 1);
    assert_eq!(no_provider.multi_pack.model_invocations, 0);
    assert_eq!(no_provider.multi_pack.known_reported_cost.model_calls, 0);
    assert_eq!(no_provider.multi_pack.settled.model_calls, 1);
    case.model = ReplayModel::OutcomeUnknown;
    let unknown = replay_pair(&case).unwrap();
    assert_eq!(unknown.multi_pack.model_callbacks, 1);
    assert_eq!(unknown.multi_pack.known_reported_cost.model_calls, 0);
    assert_eq!(unknown.multi_pack.reserved.model_calls, 1);
    assert_eq!(unknown.multi_pack.model_invocations, 1);
    assert_eq!(unknown.multi_pack.observation.status, InvestigationRunStatus::Completed);
    assert_eq!(unknown.multi_pack.observation.stop_reason, None);
    assert_eq!(
        unknown.multi_pack.observation.conclusion,
        rules.multi_pack.observation.conclusion
    );
}

#[test]
fn shared_retry_cap_applies_across_packs_and_permanent_refusal_is_never_retried() {
    let mut case = case();
    for read in &mut case.reads {
        read.outcomes = vec![ReplayReadOutcome::RetryableUnavailable {}];
    }
    let temporary = replay_pair(&case).unwrap();
    assert_eq!(temporary.multi_pack.snapshot.query_retry_count().unwrap(), 2);
    assert_eq!(temporary.multi_pack.read_callbacks, 5); // three unique reads + two shared retries
    assert_eq!(temporary.multi_pack.settled.read_queries, 5);
    assert_eq!(
        temporary.multi_pack.observation.stop_reason,
        Some(InvestigationStopReason::BudgetExhausted)
    );
    for read in &mut case.reads {
        read.outcomes = vec![ReplayReadOutcome::Rejected {}];
    }
    let permanent = replay_pair(&case).unwrap();
    assert_eq!(permanent.multi_pack.read_callbacks, 3);
    assert_eq!(permanent.multi_pack.snapshot.query_retry_count().unwrap(), 0);
    assert!(
        permanent
            .multi_pack
            .read_observations
            .iter()
            .all(|read| read.callbacks == 1)
    );
}

#[test]
fn uncertain_source_never_reports_zero_measured_cost_or_automatically_reissues() {
    let mut case = case();
    case.reads[0].outcomes = vec![ReplayReadOutcome::OutcomeUnknown {}];
    let result = replay_pair(&case).unwrap().multi_pack;
    assert_eq!(result.read_callbacks, 1);
    assert_eq!(result.known_reported_cost, InvestigationBudgetCost::default());
    assert_eq!(result.reserved.read_queries, 1);
    assert_eq!(result.reserved.output_bytes, validation::COLLECTION_BYTES);
    assert_eq!(
        result.observation.stop_reason,
        Some(InvestigationStopReason::SourceUnavailable)
    );
    assert_eq!(result.snapshot.query_retry_count().unwrap(), 0);
    assert!(result.snapshot.steps[0].attempts[0].result.is_none());
    assert!(result.snapshot.steps[0].attempts[0].interruption.is_some());
}

#[test]
fn insufficient_transport_reservation_stops_before_the_fixture_callback() {
    let mut case = case();
    case.budget.limit.output_bytes = validation::COLLECTION_BYTES - 1;
    let result = replay_pair(&case).unwrap().multi_pack;
    assert_eq!(result.read_callbacks, 0);
    assert_eq!(
        result.observation.stop_reason,
        Some(InvestigationStopReason::BudgetExhausted)
    );
    assert_ne!(result.observation.status, InvestigationRunStatus::Completed);
    assert!(result.settled.output_bytes < case.budget.limit.output_bytes);
}

#[test]
fn same_facts_after_a_real_steer_stop_without_readmission_or_fabricated_current_epoch_revision() {
    let mut case = case();
    case.refinement = ReplayRefinement::ReassessSameFactsOnce;
    let result = replay_pair(&case).unwrap().multi_pack;
    assert_eq!(result.read_callbacks, 3);
    assert_eq!(
        result.observation.stop_reason,
        Some(InvestigationStopReason::NoProgress)
    );
    assert_eq!(result.observation.status, InvestigationRunStatus::NeedsEvidence);
    assert_eq!(result.revisions.len(), 1);
    assert!(result.revisions[0].control_epoch < result.snapshot.control_epoch);
    assert!(result.observation.conclusion.is_none());
}

#[test]
fn serialized_checkpoint_hydration_reuses_successful_steps_and_is_labelled_separately() {
    let mut case = case();
    let uninterrupted = replay_pair(&case).unwrap();
    case.checkpoint = ReplayCheckpoint::AfterFirstCollection;
    let restored = replay_pair(&case).unwrap();
    for (normal, recovered) in [
        (&uninterrupted.baseline, &restored.baseline),
        (&uninterrupted.multi_pack, &restored.multi_pack),
    ] {
        assert!(recovered.serialized_checkpoint_restored);
        assert!(!normal.serialized_checkpoint_restored);
        assert_eq!(normal.observation_digest, recovered.observation_digest);
        assert_eq!(normal.read_observations, recovered.read_observations);
        assert_eq!(normal.settled, recovered.settled);
    }
}

#[test]
fn exact_version_scope_content_integrity_and_case_bounds_are_enforced_before_replay() {
    let original = case();
    let mut malformed = original.clone();
    malformed.packs[0].pack_version = "1.0.99".to_owned();
    malformed.baseline_pack = malformed.packs[0].clone();
    assert_eq!(
        replay_pair(&malformed).unwrap_err(),
        InvestigationReplayRejection::UnknownPack
    );
    let mut malformed = original.clone();
    if let ReplayReadOutcome::SavedEvidence { snapshot } = &mut malformed.reads[0].outcomes[0] {
        snapshot.cluster_id = ClusterId::new();
    }
    assert_eq!(
        replay_pair(&malformed).unwrap_err(),
        InvestigationReplayRejection::ScopeMismatch
    );
    let mut malformed = original.clone();
    if let ReplayReadOutcome::SavedEvidence { snapshot } = &mut malformed.reads[0].outcomes[0] {
        snapshot.content = EvidenceContent::Inline(json!({"tampered":true}));
    }
    assert_eq!(
        replay_pair(&malformed).unwrap_err(),
        InvestigationReplayRejection::InvalidEvidence
    );
    let mut malformed = original.clone();
    malformed.bindings.push(malformed.bindings[0].clone());
    assert_eq!(
        replay_pair(&malformed).unwrap_err(),
        InvestigationReplayRejection::AmbiguousFixture
    );
    let mut malformed = original;
    malformed.reads[0].outcomes = vec![ReplayReadOutcome::SavedEvidence {
        snapshot: snapshot(
            &malformed.scope,
            "admin-query",
            "store-pressure/broker-a",
            malformed.as_of,
            json!({"oversized":"x".repeat(validation::COLLECTION_BYTES as usize)}),
        ),
    }];
    assert_eq!(
        replay_pair(&malformed).unwrap_err(),
        InvestigationReplayRejection::InputTooLarge
    );
}
