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

#![cfg(feature = "extended_timeline")]

use rocketmq_model::common::message::timer_request::normalize_timer_request_fields;
use rocketmq_model::common::message::timer_request::TimerNormalizeError;
use rocketmq_model::common::message::timer_request::TimerPolicySnapshot;

#[test]
fn long_horizon_deadlines_are_checked_with_one_caller_clock_sample() {
    const DAY_MS: u64 = 86_400_000;
    let now_ms = 1_800_000_000_000;
    let policy = TimerPolicySnapshot::try_new(1_000, 400 * DAY_MS).expect("400-day policy");

    for days in [180, 366, 400] {
        let deadline = now_ms + days * DAY_MS;
        let normalized = normalize_timer_request_fields(None, None, Some(&deadline.to_string()), now_ms, policy)
            .expect("deadline inside the physical horizon");
        assert_eq!(normalized.original_deliver_ms, deadline);
        assert!(normalized.timer_out_ms < normalized.original_deliver_ms);
    }

    let outside = now_ms + 401 * DAY_MS;
    assert_eq!(
        normalize_timer_request_fields(None, None, Some(&outside.to_string()), now_ms, policy),
        Err(TimerNormalizeError::ExceedsMaximum {
            delay_ms: 401 * DAY_MS,
            max_delay_ms: 400 * DAY_MS,
        })
    );
}
