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

use std::collections::HashSet;

use serde_json::Value;

const PROFILE: &str = include_str!("../../scripts/remoting-command-baseline/profile-v1.json");
const CORPUS: &str = include_str!("../../scripts/request-header-codec/perf-corpus-v1.json");
const COLLECTOR: &str = include_str!("../../scripts/remoting-command-baseline/collect.ps1");
const WRITE_PIPELINE_BENCH: &str = include_str!("../../rocketmq-transport/benches/write_pipeline.rs");
const HOOKS_BENCH: &str = include_str!("../../rocketmq-transport/benches/admission_pending_hooks.rs");

fn string_set(value: &Value, field: &str) -> HashSet<String> {
    value[field]
        .as_array()
        .unwrap_or_else(|| panic!("{field} must be an array"))
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .unwrap_or_else(|| panic!("{field} entries must be strings"))
                .to_owned()
        })
        .collect()
}

fn integer_set(value: &Value, field: &str) -> HashSet<u64> {
    value[field]
        .as_array()
        .unwrap_or_else(|| panic!("{field} must be an array"))
        .iter()
        .map(|entry| {
            entry
                .as_u64()
                .unwrap_or_else(|| panic!("{field} entries must be unsigned integers"))
        })
        .collect()
}

#[test]
fn baseline_profile_freezes_the_complete_non_quick_measurement_contract() {
    let profile: Value = serde_json::from_str(PROFILE).expect("valid remoting command baseline profile");

    assert_eq!(profile["schemaVersion"], 1);
    assert_eq!(profile["caseCount"], 48);
    assert_eq!(profile["rustProcessSamples"], 10);
    assert_eq!(profile["diagnosticQuickResultsAccepted"], false);
    assert_eq!(profile["rawOutputPolicy"], "target-only");

    let criterion = &profile["criterion"];
    assert_eq!(criterion["warmupSeconds"], 5);
    assert_eq!(criterion["measurementSeconds"], 10);
    assert_eq!(criterion["sampleSize"], 100);

    let jmh = &profile["javaJmh"];
    assert_eq!(jmh["forks"], 5);
    assert_eq!(jmh["warmupIterations"], 10);
    assert_eq!(jmh["warmupSeconds"], 1);
    assert_eq!(jmh["measurementIterations"], 15);
    assert_eq!(jmh["measurementSeconds"], 1);
    assert_eq!(jmh["profiler"], "gc");

    let dimensions = &profile["dimensions"];
    assert_eq!(
        string_set(dimensions, "protocols"),
        HashSet::from(["JSON".into(), "ROCKETMQ".into()])
    );
    assert_eq!(
        integer_set(dimensions, "extFieldCounts"),
        HashSet::from([0, 1, 8, 16, 32, 128, 256])
    );
    assert_eq!(
        integer_set(dimensions, "bodyBytes"),
        HashSet::from([0, 128, 4096, 65536, 1048576, 4194304])
    );
    assert_eq!(integer_set(dimensions, "threads"), HashSet::from([1, 2, 4, 8, 16, 32]));
    assert_eq!(integer_set(dimensions, "hooks"), HashSet::from([0, 1, 4]));
    assert_eq!(
        string_set(dimensions, "operations"),
        HashSet::from([
            "construct".into(),
            "header_encode".into(),
            "frame_assemble".into(),
            "envelope_decode".into(),
            "typed_decode".into(),
            "round_trip".into(),
            "clone".into(),
            "raw_forward".into(),
            "display".into(),
            "limits_rejection".into(),
            "hook_snapshot".into(),
        ])
    );
    assert_eq!(
        string_set(dimensions, "inputs"),
        HashSet::from([
            "complete_frame".into(),
            "one_byte_fragmentation".into(),
            "prefix_boundary".into(),
            "random_fragments".into(),
            "consecutive_32_frames".into(),
        ])
    );
}

#[test]
fn request_header_corpus_contains_exactly_48_unique_cross_runtime_cases() {
    let corpus: Value = serde_json::from_str(CORPUS).expect("valid request header performance corpus");
    let cases = corpus["cases"].as_array().expect("corpus cases");
    let ids = cases
        .iter()
        .map(|case| case["id"].as_str().expect("case id"))
        .collect::<HashSet<_>>();

    assert_eq!(corpus["weightProfile"]["operationCount"], 48);
    assert_eq!(cases.len(), 48);
    assert_eq!(ids.len(), cases.len(), "performance case IDs must be unique");
    assert_eq!(
        cases
            .iter()
            .map(|case| case["serializeType"].as_str().expect("serialize type"))
            .collect::<HashSet<_>>(),
        HashSet::from(["JSON", "ROCKETMQ"])
    );
    assert_eq!(
        cases
            .iter()
            .map(|case| case["operation"].as_str().expect("operation"))
            .collect::<HashSet<_>>(),
        HashSet::from(["encode", "decode"])
    );
}

#[test]
fn collector_applies_the_formal_profile_to_group_level_benchmark_overrides() {
    for variable in [
        "ROCKETMQ_REMOTING_COMMAND_BASELINE_WARMUP_SECONDS",
        "ROCKETMQ_REMOTING_COMMAND_BASELINE_MEASUREMENT_SECONDS",
        "ROCKETMQ_REMOTING_COMMAND_BASELINE_SAMPLE_SIZE",
    ] {
        assert!(
            COLLECTOR.contains(variable),
            "collector must export the formal {variable} setting"
        );
    }

    for (name, source) in [
        ("write_pipeline", WRITE_PIPELINE_BENCH),
        ("admission_pending_hooks", HOOKS_BENCH),
    ] {
        assert!(
            source.contains("apply_remoting_command_baseline_profile"),
            "{name} must apply the formal profile after creating each benchmark group"
        );
    }
}
