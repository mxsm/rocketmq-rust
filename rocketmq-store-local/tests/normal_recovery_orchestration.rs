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

use rocketmq_store_local::commit_log::normal_recovery::NormalRecoveryObservation;
use rocketmq_store_local::commit_log::normal_recovery::NormalRecoveryRecord;
use rocketmq_store_local::commit_log::normal_recovery::NormalRecoverySegmentOutcome;
use rocketmq_store_local::commit_log::recovery::NormalRecoveryOffsetError;
use rocketmq_store_local::commit_log::recovery::NormalRecoveryPolicy;
use rocketmq_store_local::commit_log::recovery::NormalRecoveryState;

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

fn state(policy: NormalRecoveryPolicy) -> NormalRecoveryState {
    NormalRecoveryState::try_new(0, policy).expect("zero seed is valid")
}

#[test]
fn segment_started_state_error_skips_started_and_next() {
    let events: Events = Rc::new(RefCell::new(Vec::new()));
    let mut recovery = state(NormalRecoveryPolicy::Standard);

    let outcome: NormalRecoverySegmentOutcome<AdapterError> = recovery.drive_segment(
        i64::MAX as u64 + 1,
        || panic!("next must not run"),
        || events.borrow_mut().push("started".into()),
        |_, _: &mut Record| panic!("observe must not run"),
    );

    assert_eq!(
        outcome,
        NormalRecoverySegmentOutcome::StateFailed(NormalRecoveryOffsetError::OffsetExceedsI64 {
            offset: i64::MAX as u64 + 1,
        })
    );
    assert!(events.borrow().is_empty());
}

#[test]
fn segment_started_runs_before_next_and_preserves_adapter_error_identity() {
    let events: Events = Rc::new(RefCell::new(Vec::new()));
    let mut recovery = state(NormalRecoveryPolicy::Standard);

    let outcome = recovery.drive_segment(
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
        NormalRecoverySegmentOutcome::AdapterFailed(AdapterError("read"))
    );
    assert_eq!(&*events.borrow(), &["started", "next"]);
    assert_eq!(recovery.summary().truncate_offset, 13);
}

fn valid_then_source_ended(
    policy: NormalRecoveryPolicy,
    expected_outcome: NormalRecoverySegmentOutcome<AdapterError>,
    expected_last_valid: u64,
) {
    let events = Rc::new(RefCell::new(Vec::new()));
    let mut records = VecDeque::from([
        Ok(NormalRecoveryRecord::Message {
            relative_start: 3,
            size: 5,
            record: Record::new("message", &events),
        }),
        Ok(NormalRecoveryRecord::SourceEnded),
    ]);
    let mut recovery = state(policy);

    let outcome = recovery.drive_segment(
        100,
        || {
            let next = records.pop_front().expect("bounded next call");
            let label = match &next {
                Ok(NormalRecoveryRecord::Message { .. }) => "next:message",
                Ok(NormalRecoveryRecord::SourceEnded) => "next:source-ended",
                _ => panic!("unexpected scripted record"),
            };
            events.borrow_mut().push(label.into());
            next
        },
        || events.borrow_mut().push("started".into()),
        |observation, record| {
            assert_eq!(observation, NormalRecoveryObservation::MessageAccepted);
            assert_eq!(record.name, "message");
            record.observed = true;
            events.borrow_mut().push("observe:message".into());
        },
    );

    assert_eq!(outcome, expected_outcome);
    assert_eq!(recovery.summary().last_valid_offset, expected_last_valid);
    assert_eq!(recovery.summary().truncate_offset, 108);
    assert_eq!(
        &*events.borrow(),
        &[
            "started",
            "next:message",
            "observe:message",
            "drop:message:true",
            "next:source-ended",
        ]
    );
    assert!(records.is_empty());
}

#[test]
fn standard_and_optimized_valid_then_source_ended_preserve_policy_outcomes() {
    valid_then_source_ended(
        NormalRecoveryPolicy::Standard,
        NormalRecoverySegmentOutcome::StopRecovery,
        103,
    );
    valid_then_source_ended(
        NormalRecoveryPolicy::Optimized,
        NormalRecoverySegmentOutcome::ContinueNextSegment,
        108,
    );
}

#[test]
fn valid_then_blank_observes_each_payload_before_drop_and_stops_reading() {
    for policy in [NormalRecoveryPolicy::Standard, NormalRecoveryPolicy::Optimized] {
        let events = Rc::new(RefCell::new(Vec::new()));
        let mut records = VecDeque::from([
            Ok::<_, AdapterError>(NormalRecoveryRecord::Message {
                relative_start: 2,
                size: 7,
                record: Record::new("message", &events),
            }),
            Ok(NormalRecoveryRecord::Blank {
                record: Record::new("blank", &events),
            }),
            Ok(NormalRecoveryRecord::SourceEnded),
        ]);
        let mut recovery = state(policy);
        let next_calls = Rc::new(RefCell::new(0));

        let outcome = recovery.drive_segment(
            20,
            || {
                *next_calls.borrow_mut() += 1;
                records.pop_front().expect("bounded next call")
            },
            || events.borrow_mut().push("started".into()),
            |observation, record| {
                record.observed = true;
                events.borrow_mut().push(format!("observe:{observation:?}"));
            },
        );

        assert_eq!(outcome, NormalRecoverySegmentOutcome::ContinueNextSegment);
        assert_eq!(*next_calls.borrow(), 2);
        assert_eq!(records.len(), 1);
        assert_eq!(
            &*events.borrow(),
            &[
                "started",
                "observe:MessageAccepted",
                "drop:message:true",
                "observe:Blank",
                "drop:blank:true",
            ]
        );
    }
}

#[test]
fn invalid_some_and_none_are_observed_before_policy_action() {
    for relative_start in [Some(9), None] {
        let events = Rc::new(RefCell::new(Vec::new()));
        let mut next = Some(Ok::<_, AdapterError>(NormalRecoveryRecord::Invalid {
            relative_start,
            record: Record::new("invalid", &events),
        }));
        let mut recovery = state(NormalRecoveryPolicy::Optimized);

        let outcome = recovery.drive_segment(
            50,
            || next.take().expect("one next call"),
            || events.borrow_mut().push("started".into()),
            |observation, record| {
                assert_eq!(observation, NormalRecoveryObservation::Invalid { relative_start });
                record.observed = true;
                events.borrow_mut().push("observe:invalid".into());
            },
        );

        assert_eq!(outcome, NormalRecoverySegmentOutcome::ContinueNextSegment);
        assert_eq!(&*events.borrow(), &["started", "observe:invalid", "drop:invalid:true"]);
    }
}

#[test]
fn message_state_overflow_skips_observe_drops_payload_and_stops_reading() {
    let events = Rc::new(RefCell::new(Vec::new()));
    let mut records = VecDeque::from([
        Ok::<_, AdapterError>(NormalRecoveryRecord::Message {
            relative_start: 1,
            size: 1,
            record: Record::new("overflow", &events),
        }),
        Ok(NormalRecoveryRecord::SourceEnded),
    ]);
    let mut recovery = state(NormalRecoveryPolicy::Optimized);
    let mut next_calls = 0;

    let outcome = recovery.drive_segment(
        u64::MAX,
        || {
            next_calls += 1;
            records.pop_front().expect("bounded next call")
        },
        || events.borrow_mut().push("started".into()),
        |_, _| events.borrow_mut().push("observe".into()),
    );

    assert_eq!(
        outcome,
        NormalRecoverySegmentOutcome::StateFailed(NormalRecoveryOffsetError::BaseRelativeOverflow {
            base_offset: u64::MAX,
            relative_start: 1,
        })
    );
    assert_eq!(next_calls, 1);
    assert_eq!(records.len(), 1);
    assert_eq!(&*events.borrow(), &["started", "drop:overflow:false"]);
}

#[test]
fn source_ended_never_observes_and_stop_or_next_is_bounded() {
    for (policy, expected) in [
        (
            NormalRecoveryPolicy::Standard,
            NormalRecoverySegmentOutcome::StopRecovery,
        ),
        (
            NormalRecoveryPolicy::Optimized,
            NormalRecoverySegmentOutcome::ContinueNextSegment,
        ),
    ] {
        let mut recovery = state(policy);
        let mut next_calls = 0;
        let outcome = recovery.drive_segment(
            0,
            || {
                next_calls += 1;
                Ok::<_, AdapterError>(NormalRecoveryRecord::<Record>::SourceEnded)
            },
            || {},
            |_, _| panic!("SourceEnded must not be observed"),
        );

        assert_eq!(outcome, expected);
        assert_eq!(next_calls, 1);
    }
}
