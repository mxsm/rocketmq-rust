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

use rocketmq_store_local::commit_log::recovery_orchestration::drive_commit_log_recovery;
use rocketmq_store_local::commit_log::recovery_orchestration::optimized_recovery_requested;
use rocketmq_store_local::commit_log::recovery_orchestration::CommitLogRecoveryStep;

#[test]
fn optimized_recovery_value_preserves_legacy_truth_table() {
    for value in [None, Some("true"), Some("invalid"), Some("TRUE"), Some(" false")] {
        assert!(optimized_recovery_requested(value), "{value:?}");
    }
    assert!(!optimized_recovery_requested(Some("false")));
}

#[test]
fn optimized_route_executes_exactly_once_and_returns_adapter_output() {
    let mut calls = 0;
    let output = drive_commit_log_recovery(true, |step| {
        calls += 1;
        assert_eq!(step, CommitLogRecoveryStep::Optimized);
        "optimized-output"
    });

    assert_eq!(calls, 1);
    assert_eq!(output, "optimized-output");
}

#[test]
fn standard_route_executes_exactly_once_and_returns_adapter_output() {
    let mut calls = 0;
    let output = drive_commit_log_recovery(false, |step| {
        calls += 1;
        assert_eq!(step, CommitLogRecoveryStep::Standard);
        "standard-output"
    });

    assert_eq!(calls, 1);
    assert_eq!(output, "standard-output");
}
