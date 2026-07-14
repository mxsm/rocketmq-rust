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

use rocketmq_store_local::commit_log::recovery::should_truncate_recovery_consume_queue;

#[test]
fn recovery_consume_queue_truncation_preserves_signed_legacy_semantics() {
    let cases = [
        (i64::MIN, 10, true),
        (-1, 10, true),
        (0, 0, true),
        (0, 10, false),
        (9, 10, false),
        (10, 10, true),
        (11, 10, true),
        (i64::MAX, u64::MAX, false),
    ];

    for (max_phy_offset, truncate_offset, expected) in cases {
        assert_eq!(
            should_truncate_recovery_consume_queue(max_phy_offset, truncate_offset),
            expected,
            "max_phy_offset={max_phy_offset}, truncate_offset={truncate_offset}",
        );
    }
}
