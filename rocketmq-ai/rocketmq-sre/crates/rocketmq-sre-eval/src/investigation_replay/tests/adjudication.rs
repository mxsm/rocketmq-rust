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

fn supplied(pair: &InvestigationReplayPair, arm: ReplayArm, verdict: ReplayVerdict) -> SuppliedReplayAdjudication {
    let observed = match arm {
        ReplayArm::SinglePack => &pair.baseline,
        ReplayArm::MultiPack => &pair.multi_pack,
    };
    SuppliedReplayAdjudication {
        case_id: pair.case_id.clone(),
        case_digest: pair.case_digest.clone(),
        arm,
        observation: observed.observation.clone(),
        observation_digest: observed.observation_digest.clone(),
        verdict,
        rationale: "Synthetic maintenance expectation from the compiled predicates, not an SRE review".to_owned(),
        provenance: AdjudicationProvenance::SyntheticFixture {
            fixture_ref: "compiled-store-rules".to_owned(),
        },
    }
}

#[test]
fn missing_annotations_never_become_correct_or_independently_reviewed() {
    let pair = replay_pair(&case()).unwrap();
    let report = adjudicate_pair(&pair, &[]).unwrap();
    assert!(matches!(report.baseline, ReplayAdjudication::Unadjudicated {}));
    assert!(matches!(report.multi_pack, ReplayAdjudication::Unadjudicated {}));
    let annotation = supplied(&pair, ReplayArm::MultiPack, ReplayVerdict::Correct);
    let report = adjudicate_pair(&pair, &[annotation]).unwrap();
    assert!(matches!(
        report.multi_pack,
        ReplayAdjudication::Supplied {
            provenance: AdjudicationProvenance::SyntheticFixture { .. },
            ..
        }
    ));
    assert!(matches!(report.baseline, ReplayAdjudication::Unadjudicated {}));
}

#[test]
fn competing_saved_faults_preserve_context_and_accept_a_separate_wrong_baseline_verdict() {
    let mut case = case();
    let read = case
        .reads
        .iter_mut()
        .find(|read| read.resource.starts_with("store-integrity/"))
        .unwrap();
    read.outcomes = vec![ReplayReadOutcome::SavedEvidence {
        snapshot: snapshot(
            &case.scope,
            "admin-query",
            "store-integrity/broker-a",
            case.as_of,
            json!({"offset_inconsistent":true,"recovery_incomplete":false,"index_rebuild_failed":false}),
        ),
    }];
    let pair = replay_pair(&case).unwrap();
    assert_eq!(pair.baseline.observation.status, InvestigationRunStatus::Completed);
    assert_eq!(pair.multi_pack.observation.status, InvestigationRunStatus::Inconclusive);
    assert!(
        pair.multi_pack
            .observation
            .findings
            .iter()
            .any(|finding| finding.reason_code == "STORE_DISK_PRESSURE")
    );
    assert!(
        pair.multi_pack
            .observation
            .findings
            .iter()
            .any(|finding| finding.reason_code == "STORE_OFFSET_INCONSISTENT")
    );
    let annotations = [
        supplied(&pair, ReplayArm::SinglePack, ReplayVerdict::Wrong),
        supplied(&pair, ReplayArm::MultiPack, ReplayVerdict::ReasonableInconclusive),
    ];
    let report = adjudicate_pair(&pair, &annotations).unwrap();
    assert!(matches!(
        report.baseline,
        ReplayAdjudication::Supplied {
            verdict: ReplayVerdict::Wrong,
            ..
        }
    ));
    assert!(matches!(
        report.multi_pack,
        ReplayAdjudication::Supplied {
            verdict: ReplayVerdict::ReasonableInconclusive,
            ..
        }
    ));
}

#[test]
fn external_provenance_requires_an_explicit_reference_and_is_not_invented_by_replay() {
    let pair = replay_pair(&case()).unwrap();
    let mut annotation = supplied(&pair, ReplayArm::MultiPack, ReplayVerdict::Correct);
    annotation.provenance = AdjudicationProvenance::ExternallySupplied {
        adjudicator_ref: String::new(),
        source_ref: "test-input-only".to_owned(),
    };
    assert_eq!(
        adjudicate_pair(&pair, &[annotation.clone()]).unwrap_err(),
        InvestigationReplayRejection::InvalidAdjudication
    );
    // Tests exercise caller-supplied provenance parsing; this is not a real
    // external review and is never emitted as shipped operator evidence.
    annotation.provenance = AdjudicationProvenance::ExternallySupplied {
        adjudicator_ref: "test-supplied-ref".to_owned(),
        source_ref: "test-input-only".to_owned(),
    };
    let accepted = adjudicate_pair(&pair, &[annotation]).unwrap();
    assert!(matches!(
        accepted.multi_pack,
        ReplayAdjudication::Supplied {
            provenance: AdjudicationProvenance::ExternallySupplied { .. },
            ..
        }
    ));
}

#[test]
fn stale_scope_evidence_conclusions_and_duplicate_or_conflicting_verdicts_are_rejected() {
    let pair = replay_pair(&case()).unwrap();
    let annotation = supplied(&pair, ReplayArm::MultiPack, ReplayVerdict::Correct);
    let mut changes = vec![];
    let mut changed = annotation.clone();
    changed.case_id.push_str("-other");
    changes.push(changed);
    let mut changed = annotation.clone();
    changed.arm = ReplayArm::SinglePack;
    changes.push(changed);
    let mut changed = annotation.clone();
    changed.observation.scope.cluster_id = ClusterId::new();
    changes.push(changed);
    let mut changed = annotation.clone();
    changed.observation.evidence_ids.clear();
    changes.push(changed);
    let mut changed = annotation.clone();
    changed.observation.conclusion = None;
    changes.push(changed);
    let mut changed = annotation.clone();
    changed.observation_digest = "sha256:wrong".to_owned();
    changes.push(changed);
    let mut changed = annotation.clone();
    changed.verdict = ReplayVerdict::ReasonableInconclusive;
    changes.push(changed);
    for changed in changes {
        assert_eq!(
            adjudicate_pair(&pair, &[changed]).unwrap_err(),
            InvestigationReplayRejection::InvalidAdjudication
        );
    }
    assert!(adjudicate_pair(&pair, &[annotation.clone(), annotation.clone()]).is_err());
    let mut conflicting = annotation.clone();
    conflicting.verdict = ReplayVerdict::Wrong;
    assert!(adjudicate_pair(&pair, &[annotation, conflicting]).is_err());
}

#[test]
fn an_annotation_cannot_be_reused_after_inputs_change_even_with_the_same_observed_conclusion() {
    let original = case();
    let pair = replay_pair(&original).unwrap();
    let annotation = supplied(&pair, ReplayArm::MultiPack, ReplayVerdict::Correct);
    let mut changed = original;
    changed.budget.limit.read_queries -= 1;
    let changed_pair = replay_pair(&changed).unwrap();
    assert_eq!(
        pair.multi_pack.observation_digest,
        changed_pair.multi_pack.observation_digest
    );
    assert_ne!(pair.case_digest, changed_pair.case_digest);
    assert_eq!(
        adjudicate_pair(&changed_pair, &[annotation]).unwrap_err(),
        InvestigationReplayRejection::InvalidAdjudication
    );
}
