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

#[test]
fn no_conclusion_is_a_real_terminal_assessment_without_fabricated_root_cause() {
    let (context, mut run) = fixture();
    let revision = revision(&run, vec![]);
    let assessment = assessment(
        &run,
        &revision,
        HypothesisAssessmentConclusion::Inconclusive {
            reason: InvestigationStopReason::EvidenceInsufficient,
            considered_evidence_ids: vec![],
            missing_requirements: context.allowed_requirements.clone(),
        },
    );
    run.finish_with_assessment(
        &context,
        run.revision,
        &assessment,
        &revision,
        run.deadline + Duration::seconds(1),
    )
    .unwrap();
    assert_eq!(run.status, InvestigationRunStatus::Inconclusive);
    let wire = serde_json::to_value(&run.assessments[0]).unwrap();
    assert_eq!(wire["conclusion"]["kind"], "inconclusive");
    assert!(wire["conclusion"].get("statement").is_none());
    assert!(wire.get("execution_eligible").is_none());
    let unchanged = run.clone();
    assert!(run.start(&context, run.revision, run.updated_at).is_err());
    assert_eq!(run, unchanged);
}

#[test]
fn strong_assessments_require_current_saved_revision_support_and_preserve_counterevidence() {
    let (mut context, mut run) = fixture();
    seed(&mut context, &mut run, &[evidence(700), evidence(701)]);
    let mut revision = revision(&run, vec![evidence(700), evidence(701)]);
    revision.status = InvestigationDiagnosisStatus::Fault;
    let assessment = assessment(
        &run,
        &revision,
        HypothesisAssessmentConclusion::RootCause {
            statement: "Bounded supported hypothesis".into(),
            supporting_evidence_ids: vec![evidence(700)],
            counter_evidence_ids: vec![evidence(701)],
        },
    );
    let mut wrong_revision = revision.clone();
    wrong_revision.investigation_id = InvestigationId::new();
    assert_eq!(
        assessment.validate_for(&run, &context, &wrong_revision).unwrap_err(),
        InvestigationRunFailure::ScopeMismatch
    );
    wrong_revision = revision.clone();
    wrong_revision.evidence_ids.pop();
    assert_eq!(
        assessment.validate_for(&run, &context, &wrong_revision).unwrap_err(),
        InvestigationRunFailure::InvalidAssessment
    );
    let mut old = assessment.clone();
    old.control_epoch = InvestigationControlEpoch::default();
    assert_eq!(
        old.validate_for(&run, &context, &revision).unwrap_err(),
        InvestigationRunFailure::ScopeMismatch
    );
    run.finish_with_assessment(&context, run.revision, &assessment, &revision, run.updated_at)
        .unwrap();
    assert_eq!(run.status, InvestigationRunStatus::Completed);
    assert_eq!(run.assessments[0], assessment);
    let mut expired = run.clone();
    expired.updated_at = expired.deadline;
    assert!(expired.validate().is_err());
    let mut tampered = run.clone();
    if let HypothesisAssessmentConclusion::RootCause {
        supporting_evidence_ids,
        ..
    } = &mut tampered.assessments[0].conclusion
    {
        supporting_evidence_ids[0] = evidence(999);
    }
    assert_eq!(
        tampered.validate().unwrap_err(),
        InvestigationRunFailure::InvalidAssessment
    );
    tampered = run.clone();
    tampered.evidence_ids.retain(|id| *id != evidence(700));
    assert_eq!(
        tampered.validate().unwrap_err(),
        InvestigationRunFailure::InvalidAssessment
    );
    // History may retain an observed reference after a newer control epoch clears active work.
    tampered.status = InvestigationRunStatus::Running;
    tampered.control_epoch = InvestigationControlEpoch::from_u64(tampered.control_epoch.get() + 1);
    tampered.evidence_ids.clear();
    tampered.validate().unwrap();
}

#[test]
fn conclusive_assessments_cannot_upgrade_partial_inconclusive_unsupported_or_opposite_revisions() {
    for status in [
        InvestigationDiagnosisStatus::Healthy,
        InvestigationDiagnosisStatus::Fault,
        InvestigationDiagnosisStatus::Inconclusive,
        InvestigationDiagnosisStatus::Unsupported,
    ] {
        for partial in [false, true] {
            for healthy in [false, true] {
                let (mut context, mut run) = fixture();
                seed(&mut context, &mut run, &[evidence(700)]);
                let mut revision = revision(&run, vec![evidence(700)]);
                revision.status = status;
                revision.partial = partial;
                let conclusion = if healthy {
                    HypothesisAssessmentConclusion::Healthy {
                        supporting_evidence_ids: vec![evidence(700)],
                    }
                } else {
                    HypothesisAssessmentConclusion::RootCause {
                        statement: "Saved supported cause".into(),
                        supporting_evidence_ids: vec![evidence(700)],
                        counter_evidence_ids: vec![],
                    }
                };
                let assessment = assessment(&run, &revision, conclusion);
                let allowed = !partial
                    && status
                        == if healthy {
                            InvestigationDiagnosisStatus::Healthy
                        } else {
                            InvestigationDiagnosisStatus::Fault
                        };
                let unchanged = run.clone();
                let result = run.finish_with_assessment(&context, run.revision, &assessment, &revision, run.updated_at);
                if allowed {
                    result.unwrap();
                    assert_eq!(run.status, InvestigationRunStatus::Completed);
                } else {
                    assert_eq!(result.unwrap_err(), InvestigationRunFailure::InvalidAssessment);
                    assert_eq!(run, unchanged);
                }
            }
        }
    }
}

#[test]
fn a_partial_or_unsupported_saved_revision_can_still_end_honestly_inconclusive() {
    for status in [
        InvestigationDiagnosisStatus::Inconclusive,
        InvestigationDiagnosisStatus::Unsupported,
    ] {
        let (context, mut run) = fixture();
        let mut revision = revision(&run, vec![]);
        revision.status = status;
        revision.partial = true;
        let assessment = assessment(
            &run,
            &revision,
            HypothesisAssessmentConclusion::Inconclusive {
                reason: InvestigationStopReason::SourceUnavailable,
                considered_evidence_ids: vec![],
                missing_requirements: context.allowed_requirements.clone(),
            },
        );
        run.finish_with_assessment(&context, run.revision, &assessment, &revision, run.updated_at)
            .unwrap();
        assert_eq!(run.status, InvestigationRunStatus::Inconclusive);
    }
}

#[test]
fn a_new_strong_assessment_is_not_admitted_after_deadline() {
    let (mut context, mut run) = fixture();
    seed(&mut context, &mut run, &[evidence(700)]);
    let mut revision = revision(&run, vec![evidence(700)]);
    revision.status = InvestigationDiagnosisStatus::Healthy;
    let assessment = assessment(
        &run,
        &revision,
        HypothesisAssessmentConclusion::Healthy {
            supporting_evidence_ids: vec![evidence(700)],
        },
    );
    let unchanged = run.clone();
    assert_eq!(
        run.finish_with_assessment(&context, run.revision, &assessment, &revision, run.deadline)
            .unwrap_err(),
        InvestigationRunFailure::DeadlineExceeded
    );
    assert_eq!(run, unchanged);
}
