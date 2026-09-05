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

//! Java-compatible timer behavior oracle.
//!
//! This suite freezes wire/property and scheduling behavior. Rust and Java TimerLog/TimerWheel
//! directories intentionally remain different physical formats and are not interchangeable.

use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_model::common::message::timer_request::normalize_timer_request;
use rocketmq_model::common::message::timer_request::TimerPolicySnapshot;
use rocketmq_model::common::message::timer_request::JAVA_COMPAT_TIMER_PRECISIONS_MS;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::ModelContractViolation;
use rocketmq_store_local::timer::service::TimerSchedulePolicy;

fn deliver_at(value: u64) -> HashMap<CheetahString, CheetahString> {
    HashMap::from([(
        CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELIVER_MS),
        CheetahString::from_string(value.to_string()),
    )])
}

#[test]
fn java_compat_exact_and_non_exact_targets_map_to_expected_slots() {
    const NOW_MS: u64 = 10_000;
    const TARGET_MS: u64 = 12_000;

    for precision_ms in JAVA_COMPAT_TIMER_PRECISIONS_MS {
        let admission = TimerPolicySnapshot::try_new(precision_ms, 3 * 24 * 60 * 60 * 1_000).unwrap();
        let wheel = TimerSchedulePolicy::new(precision_ms, 7 * 24 * 60 * 60, 60, 2 * 24 * 60 * 60);

        let exact = normalize_timer_request(&deliver_at(TARGET_MS), NOW_MS, admission).unwrap();
        assert_eq!(exact.timer_out_ms, TARGET_MS - precision_ms);
        assert_eq!(
            wheel.plan_slot(exact.timer_out_ms as i64, NOW_MS as i64, 0, 1, 2),
            (exact.timer_out_ms as i64, 1)
        );

        let non_exact = normalize_timer_request(&deliver_at(TARGET_MS + 1), NOW_MS, admission).unwrap();
        assert_eq!(non_exact.timer_out_ms, (TARGET_MS + 1) / precision_ms * precision_ms);
        assert_eq!(
            wheel.plan_slot(non_exact.timer_out_ms as i64, NOW_MS as i64, 0, 1, 2),
            (non_exact.timer_out_ms as i64, 1)
        );
    }
}

#[test]
fn java_compat_three_day_boundary_is_inclusive_and_next_millisecond_fails() {
    const NOW_MS: u64 = 10_000;
    const MAX_DELAY_MS: u64 = 3 * 24 * 60 * 60 * 1_000;
    let policy = TimerPolicySnapshot::try_new(1_000, MAX_DELAY_MS).unwrap();

    assert!(normalize_timer_request(&deliver_at(NOW_MS + MAX_DELAY_MS), NOW_MS, policy).is_ok());
    assert!(matches!(
        normalize_timer_request(&deliver_at(NOW_MS + MAX_DELAY_MS + 1), NOW_MS, policy),
        Err(ModelContractViolation::TimerDelayExceedsMaximum { .. })
    ));
    assert!(matches!(
        normalize_timer_request(&deliver_at(NOW_MS - 1), NOW_MS, policy),
        Err(ModelContractViolation::TimerDeliveryTimeIsNotInFuture { .. })
    ));
}
