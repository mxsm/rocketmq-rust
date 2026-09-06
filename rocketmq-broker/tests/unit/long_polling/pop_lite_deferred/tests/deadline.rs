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

use std::time::Duration;

use crate::long_polling::pop_lite_deferred::deadline::PopLiteWaitDeadline;
use crate::long_polling::pop_lite_deferred::deadline::PopLiteWaitDeadlineOperationalError;
use crate::long_polling::pop_lite_deferred::deadline::PopLiteWaitDeadlineOutcome;
use crate::long_polling::pop_lite_deferred::deadline::PopLiteWaitDeadlineRejectionReason;

fn expect_deadline(
    result: Result<PopLiteWaitDeadlineOutcome, PopLiteWaitDeadlineOperationalError>,
) -> PopLiteWaitDeadline {
    match result {
        Ok(PopLiteWaitDeadlineOutcome::Pending(deadline)) => deadline,
        Ok(PopLiteWaitDeadlineOutcome::Rejected(rejection)) => {
            panic!("expected pending PopLite deadline, got {:?}", rejection.reason())
        }
        Err(error) => panic!("expected pending PopLite deadline: {error}"),
    }
}

fn expect_deadline_rejection(
    result: Result<PopLiteWaitDeadlineOutcome, PopLiteWaitDeadlineOperationalError>,
) -> PopLiteWaitDeadlineRejectionReason {
    match result {
        Ok(PopLiteWaitDeadlineOutcome::Rejected(rejection)) => rejection.reason(),
        Ok(PopLiteWaitDeadlineOutcome::Pending(_)) => panic!("expected rejected PopLite deadline"),
        Err(error) => panic!("expected rejected PopLite deadline: {error}"),
    }
}

#[test]
fn pop_lite_deferred_deadline_applies_30_second_business_cap() {
    let monotonic = tokio::time::Instant::now();
    let deadline = expect_deadline(PopLiteWaitDeadline::checked(
        1_000,
        90_000,
        1_000,
        monotonic,
        Duration::from_secs(30),
    ));

    assert_eq!(deadline.effective_end_millis(), 31_000);
    assert_eq!(deadline.protocol_millis(), 30_951);
    assert_eq!(deadline.protocol_at(), monotonic + Duration::from_millis(29_951));
}

#[test]
fn pop_lite_deferred_deadline_preserves_strict_50ms_boundary() {
    let monotonic = tokio::time::Instant::now();
    let equal = expect_deadline(PopLiteWaitDeadline::checked(
        1_000,
        1_000,
        1_950,
        monotonic,
        Duration::from_secs(30),
    ));
    let expired = expect_deadline_rejection(PopLiteWaitDeadline::checked(
        1_000,
        1_000,
        1_951,
        monotonic,
        Duration::from_secs(30),
    ));

    assert_eq!(equal.protocol_at(), monotonic + Duration::from_millis(1));
    assert_eq!(expired, PopLiteWaitDeadlineRejectionReason::AlreadyExpired);
}

#[test]
fn pop_lite_deferred_deadline_rejects_signed_invalid_and_overflow_inputs() {
    let now = tokio::time::Instant::now();
    assert_eq!(
        expect_deadline_rejection(PopLiteWaitDeadline::checked(-1, 1, 0, now, Duration::from_secs(30))),
        PopLiteWaitDeadlineRejectionReason::NegativeBornTime
    );
    assert_eq!(
        expect_deadline_rejection(PopLiteWaitDeadline::checked(0, 0, 0, now, Duration::from_secs(30))),
        PopLiteWaitDeadlineRejectionReason::NonPositivePollTime
    );
    assert_eq!(
        PopLiteWaitDeadline::checked(i64::MAX, 1, 0, now, Duration::from_secs(30))
            .expect_err("signed requested end overflow is operational"),
        PopLiteWaitDeadlineOperationalError::RequestedEnd
    );
    assert_eq!(
        expect_deadline_rejection(PopLiteWaitDeadline::checked(
            i64::MAX - 1,
            1,
            u64::MAX - 5,
            now,
            Duration::from_secs(30),
        )),
        PopLiteWaitDeadlineRejectionReason::AlreadyExpired
    );
}
