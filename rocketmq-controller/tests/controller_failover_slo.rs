// Copyright 2023 The RocketMQ Rust Authors
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

//! Deterministic contracts for Controller failover qualification evidence.

use std::time::Duration;

use rocketmq_controller::ConfirmOffsetAudit;
use rocketmq_controller::ConfirmOffsetViolationKind;
use rocketmq_controller::DurabilityEvidence;
use rocketmq_controller::FailoverMilestone;
use rocketmq_controller::FailoverQualificationReport;
use rocketmq_controller::FailoverTimeline;
use rocketmq_controller::PutOkMessageAudit;
use rocketmq_controller::QualificationError;

fn complete_timeline() -> FailoverTimeline {
    let mut timeline = FailoverTimeline::new();
    for (milestone, millis) in [
        (FailoverMilestone::ControllerLeaderElected, 900),
        (FailoverMilestone::BrokerMasterElected, 1_400),
        (FailoverMilestone::StoreWriteAuthorityGranted, 1_700),
        (FailoverMilestone::RouteConverged, 2_200),
        (FailoverMilestone::ProducerRecovered, 2_400),
    ] {
        timeline
            .record_elapsed(milestone, Duration::from_millis(millis))
            .expect("record ordered milestone");
    }
    timeline
}

#[test]
fn timeline_reports_t0_to_t5_segments_without_hiding_partial_runs() {
    let snapshot = complete_timeline().snapshot();
    assert!(snapshot.complete);
    assert_eq!(snapshot.total_rto_micros, Some(2_400_000));
    assert_eq!(snapshot.milestones.len(), 6);
    assert_eq!(snapshot.segments.len(), 5);
    assert_eq!(snapshot.segments[0].duration_micros, 900_000);
    assert_eq!(snapshot.segments[4].duration_micros, 200_000);

    let partial = FailoverTimeline::new().snapshot();
    assert!(!partial.complete);
    assert_eq!(partial.total_rto_micros, None);
}

#[test]
fn timeline_rejects_missing_duplicate_and_regressing_milestones() {
    let mut timeline = FailoverTimeline::new();
    assert_eq!(
        timeline.record_elapsed(FailoverMilestone::BrokerMasterElected, Duration::from_millis(1)),
        Err(QualificationError::UnexpectedMilestone {
            expected: FailoverMilestone::ControllerLeaderElected,
            actual: FailoverMilestone::BrokerMasterElected,
        })
    );
    timeline
        .record_elapsed(FailoverMilestone::ControllerLeaderElected, Duration::from_millis(2))
        .expect("record first post-fault milestone");
    assert_eq!(
        timeline.record_elapsed(FailoverMilestone::BrokerMasterElected, Duration::from_millis(1),),
        Err(QualificationError::ElapsedTimeRegression {
            previous_micros: 2_000,
            actual_micros: 1_000,
        })
    );
}

#[test]
fn put_ok_audit_distinguishes_rpo_zero_from_exact_recovery() {
    let mut audit = PutOkMessageAudit::with_limits(4, 2);
    audit.record_put_ok("message-a", 100).expect("record PutOk message");
    audit.record_put_ok("message-b", 200).expect("record PutOk message");
    audit.observe_recovered("message-a", 100);
    audit.observe_recovered("message-a", 100);
    audit.observe_recovered("message-b", 201);
    audit.observe_recovered("message-unexpected", 300);

    let report = audit.report();
    assert!(report.rpo_zero, "both PutOk messages were recovered");
    assert!(!report.exact_recovery);
    assert_eq!(report.put_ok_count, 2);
    assert_eq!(report.recovered_once_count, 2);
    assert_eq!(report.missing_count, 0);
    assert_eq!(report.duplicate_count, 1);
    assert_eq!(report.unexpected_count, 1);
    assert_eq!(report.offset_mismatch_count, 1);
}

#[test]
fn put_ok_audit_is_bounded_and_rejects_invalid_sources() {
    let mut audit = PutOkMessageAudit::with_limits(1, 1);
    assert_eq!(audit.record_put_ok("", 1), Err(QualificationError::EmptyMessageId));
    audit.record_put_ok("message-a", 1).expect("record first message");
    assert_eq!(
        audit.record_put_ok("message-a", 1),
        Err(QualificationError::DuplicatePutOkMessageId("message-a".to_string()))
    );
    assert_eq!(
        audit.record_put_ok("message-b", 2),
        Err(QualificationError::AuditCapacityExceeded { capacity: 1 })
    );
}

#[test]
fn confirm_offset_audit_detects_regression_and_in_sync_overrun() {
    let mut audit = ConfirmOffsetAudit::with_sample_limit(8);
    audit.observe(1, 100, 120);
    audit.observe(2, 110, 120);
    audit.observe(1, 90, 80);

    let report = audit.report();
    assert!(!report.valid);
    assert_eq!(report.violation_count, 3);
    assert_eq!(
        report
            .violations
            .iter()
            .map(|violation| violation.kind)
            .collect::<Vec<_>>(),
        vec![
            ConfirmOffsetViolationKind::AuthorityEpochRegression,
            ConfirmOffsetViolationKind::ConfirmOffsetRegression,
            ConfirmOffsetViolationKind::ExceedsInSyncAck,
        ]
    );
}

#[test]
fn strict_report_requires_complete_timeline_durability_and_exact_recovery() {
    let mut message_audit = PutOkMessageAudit::default();
    message_audit
        .record_put_ok("message-a", 100)
        .expect("record PutOk message");
    message_audit.observe_recovered("message-a", 100);
    let mut confirm_audit = ConfirmOffsetAudit::default();
    confirm_audit.observe(1, 100, 100);

    let report = FailoverQualificationReport::new(
        "clean-master-kill",
        DurabilityEvidence {
            synchronous_local_flush: true,
            required_replica_acks: true,
            clean_election: true,
        },
        complete_timeline().snapshot(),
        message_audit.report(),
        confirm_audit.report(),
    );

    assert!(report.strict_qualification_passed);
    assert!(report.rejection_reasons.is_empty());
    let json = report.to_pretty_json().expect("serialize qualification report");
    assert!(json.contains("controller_failover_qualification_evidence"));
    assert!(json.contains("total_rto_micros"));
    assert!(json.contains("strict_qualification_passed"));
}

#[test]
fn async_or_unclean_runs_cannot_be_reported_as_strict_rpo_zero() {
    let report = FailoverQualificationReport::new(
        "async-unclean-negative-control",
        DurabilityEvidence {
            synchronous_local_flush: false,
            required_replica_acks: false,
            clean_election: false,
        },
        FailoverTimeline::new().snapshot(),
        PutOkMessageAudit::default().report(),
        ConfirmOffsetAudit::default().report(),
    );

    assert!(!report.strict_qualification_passed);
    assert!(report
        .rejection_reasons
        .iter()
        .any(|reason| reason.contains("durability preconditions")));
    assert!(report
        .rejection_reasons
        .iter()
        .any(|reason| reason.contains("timeline is incomplete")));
    assert!(report
        .rejection_reasons
        .iter()
        .any(|reason| reason.contains("no PutOk messages")));
    assert!(report
        .rejection_reasons
        .iter()
        .any(|reason| reason.contains("no confirmOffset observations")));
}

#[test]
fn strict_settings_do_not_hide_missing_messages_or_authority_violations() {
    let mut message_audit = PutOkMessageAudit::default();
    message_audit
        .record_put_ok("missing-after-failover", 500)
        .expect("record PutOk message");
    let mut confirm_audit = ConfirmOffsetAudit::default();
    confirm_audit.observe(3, 510, 500);

    let report = FailoverQualificationReport::new(
        "strict-negative-control",
        DurabilityEvidence {
            synchronous_local_flush: true,
            required_replica_acks: true,
            clean_election: true,
        },
        complete_timeline().snapshot(),
        message_audit.report(),
        confirm_audit.report(),
    );

    assert!(!report.strict_qualification_passed);
    assert_eq!(report.put_ok_messages.missing_count, 1);
    assert_eq!(report.confirm_offset.violation_count, 1);
    assert!(report
        .rejection_reasons
        .iter()
        .any(|reason| reason.contains("PutOk messages were missing")));
    assert!(report
        .rejection_reasons
        .iter()
        .any(|reason| reason.contains("confirmOffset violated")));
}
