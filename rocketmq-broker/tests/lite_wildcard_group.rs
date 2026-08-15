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

use rocketmq_broker::test_support::run_lite_wildcard_probe;

#[test]
fn wildcard_group_follows_actual_lmq_without_changing_explicit_groups() {
    let result = run_lite_wildcard_probe();

    assert!(result.wildcard_matches_first_lmq);
    assert!(result.wildcard_matches_second_lmq);
    assert!(!result.explicit_group_matches_second_lmq);
    assert!(result.removed_client_is_absent);
}
