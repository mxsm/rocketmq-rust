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

use rocketmq_store_local::commit_log::recovery::plan_normal_recovery_file_window;
use rocketmq_store_local::commit_log::recovery::NormalRecoveryFileWindow;

#[test]
fn normal_recovery_file_window_preserves_default_and_explicit_limits() {
    let cases = [
        (0, 0, 0, 3),
        (1, 0, 0, 3),
        (3, 0, 0, 3),
        (5, 0, 2, 3),
        (5, 1, 4, 1),
        (5, 2, 3, 2),
        (5, 10, 0, 10),
        (usize::MAX, 0, usize::MAX - 3, 3),
        (5, usize::MAX, 0, usize::MAX),
    ];

    for (mapped_file_count, configured_limit, start_index, file_count_limit) in cases {
        assert_eq!(
            plan_normal_recovery_file_window(mapped_file_count, configured_limit),
            NormalRecoveryFileWindow {
                start_index,
                file_count_limit,
            },
        );
    }
}
