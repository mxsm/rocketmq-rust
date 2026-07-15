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

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use rocketmq_store_local::commit_log::abnormal_recovery::AbnormalRecoveryObservation;
use rocketmq_store_local::commit_log::abnormal_recovery::AbnormalRecoveryRecord;
use rocketmq_store_local::commit_log::abnormal_recovery::AbnormalRecoverySegmentOutcome;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryDispatchGate;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryOffsetError;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryPolicy;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryState;

type Events = Rc<RefCell<Vec<String>>>;

struct Record {
    name: &'static str,
    observed: bool,
    events: Events,
}

impl Record {
    fn new(name: &'static str, events: &Events) -> Self {
        Self {
            name,
            observed: false,
            events: Rc::clone(events),
        }
    }
}

impl Drop for Record {
    fn drop(&mut self) {
        self.events
            .borrow_mut()
            .push(format!("drop:{}:{}", self.name, self.observed));
    }
}

#[derive(Debug, PartialEq, Eq)]
struct AdapterError(&'static str);

fn state(policy: AbnormalRecoveryPolicy) -> AbnormalRecoveryState {
    AbnormalRecoveryState::try_new(0, policy).expect("zero seed is valid")
}

#[test]
fn segment_state_error_skips_started_and_preserves_adapter_error_identity() {
    let events: Events = Rc::new(RefCell::new(Vec::new()));
    let mut invalid = state(AbnormalRecoveryPolicy::Standard);
    let outcome: AbnormalRecoverySegmentOutcome<AdapterError> = invalid.drive_abnormal_segment(
        i64::MAX as u64 + 1,
        || panic!("next must not run"),
        || events.borrow_mut().push("started".into()),
        |_, _: &mut Record| panic!("observe must not run"),
    );
    assert_eq!(
        outcome,
        AbnormalRecoverySegmentOutcome::StateFailed(AbnormalRecoveryOffsetError::OffsetExceedsI64 {
            offset: i64::MAX as u64 + 1,
        })
    );
    assert!(events.borrow().is_empty());

    let mut recovery = state(AbnormalRecoveryPolicy::Standard);
    let outcome = recovery.drive_abnormal_segment(
        13,
        || {
            events.borrow_mut().push("next".into());
            Err(AdapterError("read"))
        },
        || events.borrow_mut().push("started".into()),
        |_, _: &mut Record| panic!("observe must not run"),
    );
    assert_eq!(
        outcome,
        AbnormalRecoverySegmentOutcome::AdapterFailed(AdapterError("read"))
    );
    assert_eq!(&*events.borrow(), &["started", "next"]);
}

#[test]
fn dispatch_and_skip_observe_after_state_and_before_payload_drop() {
    for (gate, expected) in [
        (
            AbnormalRecoveryDispatchGate::Ungated,
            AbnormalRecoveryObservation::DispatchMessage,
        ),
        (
            AbnormalRecoveryDispatchGate::ConfirmBounded { confirm_offset: 4 },
            AbnormalRecoveryObservation::SkipMessageDispatch,
        ),
    ] {
        let events: Events = Rc::new(RefCell::new(Vec::new()));
        let mut records = VecDeque::from([
            Ok::<_, AdapterError>(AbnormalRecoveryRecord::Message {
                relative_start: 2,
                validated_size: 3,
                confirm_candidate_end: 5,
                dispatch_gate: gate,
                record: Record::new("message", &events),
            }),
            Ok(AbnormalRecoveryRecord::SourceEnded),
        ]);
        let mut recovery = state(AbnormalRecoveryPolicy::Optimized);
        let outcome = recovery.drive_abnormal_segment(
            10,
            || records.pop_front().expect("bounded next call"),
            || events.borrow_mut().push("started".into()),
            |observation, record| {
                assert_eq!(observation, expected);
                record.observed = true;
                events.borrow_mut().push(format!("observe:{observation:?}"));
            },
        );
        assert_eq!(outcome, AbnormalRecoverySegmentOutcome::ContinueNextSegment);
        assert_eq!(
            &*events.borrow(),
            &[
                "started".to_owned(),
                format!("observe:{expected:?}"),
                "drop:message:true".to_owned(),
            ]
        );
    }
}

#[test]
fn blank_observes_once_and_stops_reading() {
    for policy in [AbnormalRecoveryPolicy::Standard, AbnormalRecoveryPolicy::Optimized] {
        let events: Events = Rc::new(RefCell::new(Vec::new()));
        let mut records = VecDeque::from([
            Ok::<_, AdapterError>(AbnormalRecoveryRecord::Blank {
                record: Record::new("blank", &events),
            }),
            Ok(AbnormalRecoveryRecord::SourceEnded),
        ]);
        let mut recovery = state(policy);
        let mut calls = 0;
        let outcome = recovery.drive_abnormal_segment(
            20,
            || {
                calls += 1;
                records.pop_front().expect("bounded next call")
            },
            || events.borrow_mut().push("started".into()),
            |observation, record| {
                assert_eq!(observation, AbnormalRecoveryObservation::Blank);
                record.observed = true;
                events.borrow_mut().push("observe:blank".into());
            },
        );
        assert_eq!(outcome, AbnormalRecoverySegmentOutcome::ContinueNextSegment);
        assert_eq!(calls, 1);
        assert_eq!(records.len(), 1);
        assert_eq!(&*events.borrow(), &["started", "observe:blank", "drop:blank:true"]);
    }
}

#[test]
fn invalid_is_observed_before_policy_action() {
    for (policy, expected) in [
        (
            AbnormalRecoveryPolicy::Standard,
            AbnormalRecoverySegmentOutcome::StopRecovery,
        ),
        (
            AbnormalRecoveryPolicy::Optimized,
            AbnormalRecoverySegmentOutcome::ContinueNextSegment,
        ),
    ] {
        let events: Events = Rc::new(RefCell::new(Vec::new()));
        let mut next = Some(Ok::<_, AdapterError>(AbnormalRecoveryRecord::Invalid {
            relative_start: Some(9),
            record: Record::new("invalid", &events),
        }));
        let mut recovery = state(policy);
        let outcome = recovery.drive_abnormal_segment(
            50,
            || next.take().expect("one next call"),
            || events.borrow_mut().push("started".into()),
            |observation, record| {
                assert_eq!(
                    observation,
                    AbnormalRecoveryObservation::Invalid {
                        relative_start: Some(9),
                    }
                );
                record.observed = true;
                events.borrow_mut().push("observe:invalid".into());
            },
        );
        assert_eq!(outcome, expected);
        assert_eq!(&*events.borrow(), &["started", "observe:invalid", "drop:invalid:true"]);
    }
}

#[test]
fn message_state_failure_drops_unobserved_payload_and_stops_reading() {
    let events: Events = Rc::new(RefCell::new(Vec::new()));
    let mut records = VecDeque::from([
        Ok::<_, AdapterError>(AbnormalRecoveryRecord::Message {
            relative_start: 1,
            validated_size: 1,
            confirm_candidate_end: 1,
            dispatch_gate: AbnormalRecoveryDispatchGate::Ungated,
            record: Record::new("overflow", &events),
        }),
        Ok(AbnormalRecoveryRecord::SourceEnded),
    ]);
    let mut recovery = state(AbnormalRecoveryPolicy::Optimized);
    let mut calls = 0;
    let outcome = recovery.drive_abnormal_segment(
        u64::MAX,
        || {
            calls += 1;
            records.pop_front().expect("bounded next call")
        },
        || events.borrow_mut().push("started".into()),
        |_, _| events.borrow_mut().push("observe".into()),
    );
    assert_eq!(
        outcome,
        AbnormalRecoverySegmentOutcome::StateFailed(AbnormalRecoveryOffsetError::BaseRelativeOverflow {
            base_offset: u64::MAX,
            relative_start: 1,
        })
    );
    assert_eq!(calls, 1);
    assert_eq!(records.len(), 1);
    assert_eq!(&*events.borrow(), &["started", "drop:overflow:false"]);
}

#[test]
fn source_ended_never_observes_and_returns_policy_action() {
    for (policy, expected) in [
        (
            AbnormalRecoveryPolicy::Standard,
            AbnormalRecoverySegmentOutcome::StopRecovery,
        ),
        (
            AbnormalRecoveryPolicy::Optimized,
            AbnormalRecoverySegmentOutcome::ContinueNextSegment,
        ),
    ] {
        let mut recovery = state(policy);
        let mut calls = 0;
        let outcome = recovery.drive_abnormal_segment(
            0,
            || {
                calls += 1;
                Ok::<_, AdapterError>(AbnormalRecoveryRecord::<Record>::SourceEnded)
            },
            || {},
            |_, _| panic!("source end must not be observed"),
        );
        assert_eq!(outcome, expected);
        assert_eq!(calls, 1);
    }
}
