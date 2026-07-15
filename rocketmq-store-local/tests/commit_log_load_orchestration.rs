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

use rocketmq_store_local::commit_log::load_orchestration::drive_commit_log_load;
use rocketmq_store_local::commit_log::load_orchestration::safe_load_requested;
use rocketmq_store_local::commit_log::load_orchestration::CommitLogLoadObservation;
use rocketmq_store_local::commit_log::load_orchestration::CommitLogLoadStep;

#[derive(Debug, PartialEq, Eq)]
struct AdapterError(&'static str);

#[test]
fn safe_load_value_preserves_legacy_truth_table() {
    for value in [Some("1"), Some("true"), Some("TRUE"), Some("True")] {
        assert!(safe_load_requested(value), "{value:?}");
    }
    for value in [None, Some("0"), Some("false"), Some(" true"), Some("yes")] {
        assert!(!safe_load_requested(value), "{value:?}");
    }
}

#[test]
fn forced_sequential_never_attempts_optimized() {
    let mut steps = Vec::new();
    let mut observations = Vec::new();
    let loaded = drive_commit_log_load(
        true,
        |step| {
            steps.push(step);
            assert_eq!(step, CommitLogLoadStep::Sequential);
            Ok::<_, AdapterError>(true)
        },
        |observation| observations.push(observation),
    );

    assert!(loaded);
    assert_eq!(steps, [CommitLogLoadStep::Sequential]);
    assert_eq!(observations, [CommitLogLoadObservation::ForcedSequential]);
}

#[test]
fn optimized_success_is_terminal() {
    let mut steps = Vec::new();
    let mut observations = Vec::new();
    let loaded = drive_commit_log_load(
        false,
        |step| {
            steps.push(step);
            Ok::<_, AdapterError>(true)
        },
        |observation| observations.push(observation),
    );

    assert!(loaded);
    assert_eq!(steps, [CommitLogLoadStep::Optimized]);
    assert_eq!(observations, [CommitLogLoadObservation::OptimizedLoaded]);
}

#[test]
fn optimized_rejection_does_not_fallback() {
    let mut steps = Vec::new();
    let mut observations = Vec::new();
    let loaded = drive_commit_log_load(
        false,
        |step| {
            steps.push(step);
            Ok::<_, AdapterError>(false)
        },
        |observation| observations.push(observation),
    );

    assert!(!loaded);
    assert_eq!(steps, [CommitLogLoadStep::Optimized]);
    assert_eq!(observations, [CommitLogLoadObservation::OptimizedRejected]);
}

#[test]
fn optimized_error_observes_before_sequential_fallback() {
    let events = RefCell::new(Vec::new());
    let loaded = drive_commit_log_load(
        false,
        |step| {
            events.borrow_mut().push(format!("step:{step:?}"));
            match step {
                CommitLogLoadStep::Optimized => Err(AdapterError("optimized")),
                CommitLogLoadStep::Sequential => Ok(true),
            }
        },
        |observation| events.borrow_mut().push(format!("observe:{observation:?}")),
    );

    assert!(loaded);
    assert_eq!(
        events.into_inner(),
        [
            "step:Optimized",
            "observe:OptimizedFailed(AdapterError(\"optimized\"))",
            "step:Sequential",
        ]
    );
}

#[test]
fn sequential_adapter_error_is_observed_and_fails_closed() {
    for force_sequential in [false, true] {
        let mut observations = Vec::new();
        let loaded = drive_commit_log_load(
            force_sequential,
            |step| match step {
                CommitLogLoadStep::Optimized => Err(AdapterError("optimized")),
                CommitLogLoadStep::Sequential => Err(AdapterError("sequential")),
            },
            |observation| observations.push(observation),
        );

        assert!(!loaded);
        assert_eq!(
            observations.last(),
            Some(&CommitLogLoadObservation::SequentialFailed(AdapterError("sequential")))
        );
        assert_eq!(
            observations.first(),
            Some(if force_sequential {
                &CommitLogLoadObservation::ForcedSequential
            } else {
                &CommitLogLoadObservation::OptimizedFailed(AdapterError("optimized"))
            })
        );
    }
}
