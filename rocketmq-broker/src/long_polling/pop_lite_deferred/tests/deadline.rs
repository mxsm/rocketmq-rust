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
use crate::long_polling::pop_lite_deferred::deadline::PopLiteWaitDeadlineErrorKind;

#[test]
fn pop_lite_deferred_deadline_applies_30_second_business_cap() {
    let monotonic = tokio::time::Instant::now();
    let deadline = PopLiteWaitDeadline::checked(1_000, 90_000, 1_000, monotonic, Duration::from_secs(30))
        .expect("30 second cap should be representable");

    assert_eq!(deadline.effective_end_millis(), 31_000);
    assert_eq!(deadline.protocol_millis(), 30_951);
    assert_eq!(deadline.protocol_at(), monotonic + Duration::from_millis(29_951));
}

#[test]
fn pop_lite_deferred_deadline_preserves_strict_50ms_boundary() {
    let monotonic = tokio::time::Instant::now();
    let equal = PopLiteWaitDeadline::checked(1_000, 1_000, 1_950, monotonic, Duration::from_secs(30))
        .expect("equal cutoff remains live for one millisecond");
    let expired = PopLiteWaitDeadline::checked(1_000, 1_000, 1_951, monotonic, Duration::from_secs(30))
        .expect_err("first millisecond after cutoff is expired");

    assert_eq!(equal.protocol_at(), monotonic + Duration::from_millis(1));
    assert_eq!(expired.kind(), PopLiteWaitDeadlineErrorKind::AlreadyExpired);
}

#[test]
fn pop_lite_deferred_deadline_rejects_signed_invalid_and_overflow_inputs() {
    let now = tokio::time::Instant::now();
    assert_eq!(
        PopLiteWaitDeadline::checked(-1, 1, 0, now, Duration::from_secs(30))
            .expect_err("negative born time")
            .kind(),
        PopLiteWaitDeadlineErrorKind::NegativeBornTime
    );
    assert_eq!(
        PopLiteWaitDeadline::checked(0, 0, 0, now, Duration::from_secs(30))
            .expect_err("zero poll time")
            .kind(),
        PopLiteWaitDeadlineErrorKind::NonPositivePollTime
    );
    assert_eq!(
        PopLiteWaitDeadline::checked(i64::MAX, 1, 0, now, Duration::from_secs(30))
            .expect_err("signed requested end overflow")
            .kind(),
        PopLiteWaitDeadlineErrorKind::RequestedEndOverflow
    );
    assert_eq!(
        PopLiteWaitDeadline::checked(i64::MAX - 1, 1, u64::MAX - 5, now, Duration::from_secs(30),)
            .expect_err("saturating admission cap still observes an expired requested end")
            .kind(),
        PopLiteWaitDeadlineErrorKind::AlreadyExpired
    );
}
