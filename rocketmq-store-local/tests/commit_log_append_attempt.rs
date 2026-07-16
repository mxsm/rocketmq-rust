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
use std::rc::Rc;

use rocketmq_store_local::commit_log::append::AppendMessageResult;
use rocketmq_store_local::commit_log::append::AppendMessageStatus;
use rocketmq_store_local::commit_log::append_attempt::CommitLogAppendAborted;
use rocketmq_store_local::commit_log::append_attempt::CommitLogAppendAttempt;
use rocketmq_store_local::commit_log::append_attempt::CommitLogAppendCompleted;
use rocketmq_store_local::commit_log::append_attempt::CommitLogAppendFailure;
use rocketmq_store_local::commit_log::append_attempt::CommitLogAppendOutcome;
use rocketmq_store_local::commit_log::append_attempt::CommitLogAppendResolution;
use rocketmq_store_local::commit_log::append_attempt::CommitLogAppendStatus;

struct Segment {
    name: &'static str,
    events: Rc<RefCell<Vec<String>>>,
}

impl Segment {
    fn new(name: &'static str, events: &Rc<RefCell<Vec<String>>>) -> Self {
        Self {
            name,
            events: Rc::clone(events),
        }
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        self.events.borrow_mut().push(format!("drop:{}", self.name));
    }
}

fn result(status: AppendMessageStatus, wrote_offset: i64) -> AppendMessageResult {
    AppendMessageResult {
        status,
        wrote_offset,
        ..AppendMessageResult::default()
    }
}

#[test]
fn initial_present_put_ok_observes_strict_order_without_acquiring() {
    let events = Rc::new(RefCell::new(Vec::new()));
    let segment = Segment::new("initial", &events);

    let outcome = CommitLogAppendAttempt::run(
        Some(segment),
        |segment| {
            events.borrow_mut().push(format!("full:{}", segment.name));
            false
        },
        || {
            events.borrow_mut().push("acquire".to_string());
            None
        },
        |segment| {
            events.borrow_mut().push(format!("lock:{}", segment.name));
            Ok::<_, &'static str>(())
        },
        |segment| {
            events.borrow_mut().push(format!("append:{}", segment.name));
            result(AppendMessageStatus::PutOk, 11)
        },
    );

    match outcome {
        CommitLogAppendOutcome::Completed(CommitLogAppendCompleted::PutOk { result, rolled_segment }) => {
            assert_eq!(AppendMessageStatus::PutOk, result.status);
            assert_eq!(11, result.wrote_offset);
            assert!(rolled_segment.is_none());
        }
        _ => panic!("initial PutOk must complete without a roll"),
    }
    assert_eq!(
        ["full:initial", "lock:initial", "append:initial", "drop:initial"],
        events.borrow().as_slice()
    );
}

#[test]
fn missing_or_full_initial_segment_acquires_exactly_once() {
    for initial_is_missing in [true, false] {
        let events = Rc::new(RefCell::new(Vec::new()));
        let initial = (!initial_is_missing).then(|| Segment::new("full", &events));
        let acquired = Segment::new("acquired", &events);
        let mut acquired = Some(acquired);

        let outcome = CommitLogAppendAttempt::run(
            initial,
            |segment| {
                events.borrow_mut().push(format!("full:{}", segment.name));
                true
            },
            || {
                events.borrow_mut().push("acquire".to_string());
                acquired.take()
            },
            |segment| {
                events.borrow_mut().push(format!("lock:{}", segment.name));
                Ok::<_, &'static str>(())
            },
            |segment| {
                events.borrow_mut().push(format!("append:{}", segment.name));
                result(AppendMessageStatus::PutOk, 22)
            },
        );

        assert!(matches!(
            outcome,
            CommitLogAppendOutcome::Completed(CommitLogAppendCompleted::PutOk {
                rolled_segment: None,
                ..
            })
        ));
        let expected = if initial_is_missing {
            vec!["acquire", "lock:acquired", "append:acquired", "drop:acquired"]
        } else {
            vec![
                "full:full",
                "acquire",
                "drop:full",
                "lock:acquired",
                "append:acquired",
                "drop:acquired",
            ]
        };
        assert_eq!(expected, *events.borrow());
    }
}

#[test]
fn full_initial_acquire_failure_drops_old_only_after_acquire() {
    let events = Rc::new(RefCell::new(Vec::new()));
    let outcome = CommitLogAppendAttempt::run(
        Some(Segment::new("full", &events)),
        |segment| {
            events.borrow_mut().push(format!("full:{}", segment.name));
            true
        },
        || {
            events.borrow_mut().push("acquire".to_string());
            None
        },
        |_| -> Result<(), &'static str> { panic!("unavailable segment cannot be locked") },
        |_| panic!("unavailable segment cannot be appended"),
    );

    assert!(matches!(
        outcome,
        CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::InitialSegmentUnavailable)
    ));
    assert_eq!(["full:full", "acquire", "drop:full"], events.borrow().as_slice());
}

#[test]
fn initial_lock_failure_drops_segment_after_the_failed_lock() {
    let events = Rc::new(RefCell::new(Vec::new()));
    let outcome = CommitLogAppendAttempt::run(
        Some(Segment::new("initial", &events)),
        |_| false,
        || panic!("initial segment is available"),
        |segment| {
            events.borrow_mut().push(format!("lock:{}", segment.name));
            Err("lock-error")
        },
        |_| panic!("lock failure cannot append"),
    );

    assert!(matches!(
        outcome,
        CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::InitialActiveLockFailed { error: "lock-error" })
    ));
    assert_eq!(["lock:initial", "drop:initial"], events.borrow().as_slice());
}

#[test]
fn initial_abort_outcomes_are_explicit_and_do_not_retry() {
    for status in [
        AppendMessageStatus::MessageSizeExceeded,
        AppendMessageStatus::PropertiesSizeExceeded,
        AppendMessageStatus::UnknownError,
    ] {
        let events = Rc::new(RefCell::new(Vec::new()));
        let outcome = CommitLogAppendAttempt::run(
            Some(Segment::new("initial", &events)),
            |_| false,
            || panic!("initial segment is available"),
            |_| Ok::<_, &'static str>(()),
            |_| result(status, 31),
        );
        match status {
            AppendMessageStatus::MessageSizeExceeded | AppendMessageStatus::PropertiesSizeExceeded => {
                assert!(matches!(
                    outcome,
                    CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::InitialMessageIllegal {
                        result: AppendMessageResult { status: actual, .. }
                    }) if actual == status
                ));
            }
            AppendMessageStatus::UnknownError => assert!(matches!(
                outcome,
                CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::InitialUnknown {
                    result: AppendMessageResult {
                        status: AppendMessageStatus::UnknownError,
                        ..
                    }
                })
            )),
            _ => unreachable!(),
        }
    }

    let unavailable = CommitLogAppendAttempt::run(
        None::<()>,
        |_| panic!("None must not be inspected"),
        || None,
        |_| -> Result<(), &'static str> { panic!("unavailable segment cannot be locked") },
        |_| panic!("unavailable segment cannot be appended"),
    );
    assert!(matches!(
        unavailable,
        CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::InitialSegmentUnavailable)
    ));

    let lock_failed = CommitLogAppendAttempt::run(
        Some(()),
        |_| false,
        || panic!("initial segment is available"),
        |_| Err("initial-lock"),
        |_| panic!("lock failure cannot append"),
    );
    assert!(matches!(
        lock_failed,
        CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::InitialActiveLockFailed { error: "initial-lock" })
    ));
}

#[test]
fn first_eof_rolls_once_and_retry_put_ok_returns_old_segment() {
    let events = Rc::new(RefCell::new(Vec::new()));
    let mut rolled = Some(Segment::new("rolled", &events));
    let mut append_count = 0;

    let outcome = CommitLogAppendAttempt::run(
        Some(Segment::new("old", &events)),
        |_| false,
        || {
            events.borrow_mut().push("acquire".to_string());
            rolled.take()
        },
        |segment| {
            events.borrow_mut().push(format!("lock:{}", segment.name));
            Ok::<_, &'static str>(())
        },
        |segment| {
            append_count += 1;
            events.borrow_mut().push(format!("append:{}", segment.name));
            if append_count == 1 {
                result(AppendMessageStatus::EndOfFile, 41)
            } else {
                result(AppendMessageStatus::PutOk, 42)
            }
        },
    );

    let old = match outcome {
        CommitLogAppendOutcome::Completed(CommitLogAppendCompleted::PutOk {
            result,
            rolled_segment: Some(old),
        }) => {
            assert_eq!(42, result.wrote_offset);
            old
        }
        _ => panic!("retry PutOk must complete with the rolled old segment"),
    };
    assert_eq!(
        [
            "lock:old",
            "append:old",
            "acquire",
            "lock:rolled",
            "append:rolled",
            "drop:rolled"
        ],
        events.borrow().as_slice()
    );
    drop(old);
    assert_eq!("drop:old", events.borrow().last().unwrap());
}

#[test]
fn rolled_abort_outcomes_retain_first_eof_and_old_segment() {
    let events = Rc::new(RefCell::new(Vec::new()));
    let unavailable = CommitLogAppendAttempt::run(
        Some(Segment::new("old-unavailable", &events)),
        |_| false,
        || None,
        |_| Ok::<_, &'static str>(()),
        |_| result(AppendMessageStatus::EndOfFile, 51),
    );
    match unavailable {
        CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::RolledSegmentUnavailable { first_eof, old }) => {
            assert_eq!(51, first_eof.wrote_offset);
            assert_eq!("old-unavailable", old.name);
            drop(old);
        }
        _ => panic!("missing rolled segment must retain the first EOF and old segment"),
    }

    let mut rolled = Some(Segment::new("rolled-lock", &events));
    let lock_failed = CommitLogAppendAttempt::run(
        Some(Segment::new("old-lock", &events)),
        |_| false,
        || rolled.take(),
        |segment| {
            if segment.name == "rolled-lock" {
                Err("rolled-lock-error")
            } else {
                Ok(())
            }
        },
        |_| result(AppendMessageStatus::EndOfFile, 52),
    );
    match lock_failed {
        CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::RolledActiveLockFailed { first_eof, old, error }) => {
            assert_eq!(52, first_eof.wrote_offset);
            assert_eq!("old-lock", old.name);
            assert_eq!("rolled-lock-error", error);
            drop(old);
        }
        _ => panic!("rolled lock failure must retain the first EOF and old segment"),
    }
    assert!(events.borrow().contains(&"drop:rolled-lock".to_string()));
}

#[test]
fn every_non_put_ok_retry_is_rejected_without_a_third_attempt() {
    for retry_status in [
        AppendMessageStatus::EndOfFile,
        AppendMessageStatus::MessageSizeExceeded,
        AppendMessageStatus::PropertiesSizeExceeded,
        AppendMessageStatus::UnknownError,
    ] {
        let events = Rc::new(RefCell::new(Vec::new()));
        let mut rolled = Some(Segment::new("rolled", &events));
        let mut attempts = 0;
        let outcome = CommitLogAppendAttempt::run(
            Some(Segment::new("old", &events)),
            |_| false,
            || rolled.take(),
            |_| Ok::<_, &'static str>(()),
            |_| {
                attempts += 1;
                result(
                    if attempts == 1 {
                        AppendMessageStatus::EndOfFile
                    } else {
                        retry_status
                    },
                    attempts,
                )
            },
        );

        match outcome {
            CommitLogAppendOutcome::Completed(CommitLogAppendCompleted::RetryRejected { result, rolled_segment }) => {
                assert_eq!(retry_status, result.status);
                assert_eq!("old", rolled_segment.name);
                drop(rolled_segment);
            }
            _ => panic!("every non-PutOk retry must be rejected"),
        }
        assert_eq!(2, attempts);
    }
}

#[test]
fn completed_outcomes_resolve_to_continue_status_and_unlock_identity() {
    let put_ok = CommitLogAppendOutcome::<(), &'static str>::Completed(CommitLogAppendCompleted::PutOk {
        result: result(AppendMessageStatus::PutOk, 61),
        rolled_segment: None,
    });
    assert!(matches!(
        put_ok.resolve(),
        CommitLogAppendResolution::Continue {
            status: CommitLogAppendStatus::PutOk,
            result: AppendMessageResult { wrote_offset: 61, .. },
            unlock_segment: None,
        }
    ));

    let retry_rejected =
        CommitLogAppendOutcome::<&'static str, &'static str>::Completed(CommitLogAppendCompleted::RetryRejected {
            result: result(AppendMessageStatus::UnknownError, 62),
            rolled_segment: "old",
        });
    assert!(matches!(
        retry_rejected.resolve(),
        CommitLogAppendResolution::Continue {
            status: CommitLogAppendStatus::UnknownError,
            result: AppendMessageResult { wrote_offset: 62, .. },
            unlock_segment: Some("old"),
        }
    ));
}

#[test]
fn aborted_outcomes_resolve_every_return_status_and_owned_detail() {
    let cases = [
        CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::InitialSegmentUnavailable),
        CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::InitialActiveLockFailed { error: "initial-lock" }),
        CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::InitialMessageIllegal {
            result: result(AppendMessageStatus::MessageSizeExceeded, 71),
        }),
        CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::InitialUnknown {
            result: result(AppendMessageStatus::UnknownError, 72),
        }),
        CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::RolledSegmentUnavailable {
            first_eof: result(AppendMessageStatus::EndOfFile, 73),
            old: "old-unavailable",
        }),
        CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::RolledActiveLockFailed {
            first_eof: result(AppendMessageStatus::EndOfFile, 74),
            old: "old-lock",
            error: "rolled-lock",
        }),
    ];

    let [initial_unavailable, initial_lock, initial_illegal, initial_unknown, rolled_unavailable, rolled_lock] =
        cases.map(CommitLogAppendOutcome::resolve);
    assert!(matches!(
        initial_unavailable,
        CommitLogAppendResolution::Return {
            status: CommitLogAppendStatus::CreateSegmentFailed,
            append_result: None,
            abandoned_segment: None,
            failure: CommitLogAppendFailure::InitialSegmentUnavailable,
        }
    ));
    assert!(matches!(
        initial_lock,
        CommitLogAppendResolution::Return {
            status: CommitLogAppendStatus::CreateSegmentFailed,
            failure: CommitLogAppendFailure::InitialActiveLockFailed { error: "initial-lock" },
            ..
        }
    ));
    assert!(matches!(
        initial_illegal,
        CommitLogAppendResolution::Return {
            status: CommitLogAppendStatus::MessageIllegal,
            append_result: Some(AppendMessageResult { wrote_offset: 71, .. }),
            failure: CommitLogAppendFailure::InitialMessageIllegal,
            ..
        }
    ));
    assert!(matches!(
        initial_unknown,
        CommitLogAppendResolution::Return {
            status: CommitLogAppendStatus::UnknownError,
            append_result: Some(AppendMessageResult { wrote_offset: 72, .. }),
            failure: CommitLogAppendFailure::InitialUnknown,
            ..
        }
    ));
    assert!(matches!(
        rolled_unavailable,
        CommitLogAppendResolution::Return {
            status: CommitLogAppendStatus::CreateSegmentFailed,
            append_result: Some(AppendMessageResult { wrote_offset: 73, .. }),
            abandoned_segment: Some("old-unavailable"),
            failure: CommitLogAppendFailure::RolledSegmentUnavailable,
        }
    ));
    assert!(matches!(
        rolled_lock,
        CommitLogAppendResolution::Return {
            status: CommitLogAppendStatus::CreateSegmentFailed,
            append_result: Some(AppendMessageResult { wrote_offset: 74, .. }),
            abandoned_segment: Some("old-lock"),
            failure: CommitLogAppendFailure::RolledActiveLockFailed { error: "rolled-lock" },
        }
    ));
}
