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

#[path = "../benches/support/operation_mix.rs"]
mod operation_mix;

use operation_mix::OperationMix;

#[test]
fn ninety_ten_mix_contains_expected_operation_counts() {
    let counts = OperationMix::NinetyTen.counts(10_000);

    assert_eq!(counts.reads, 9_000);
    assert_eq!(counts.writes, 1_000);
}

#[test]
fn ninety_ten_trace_is_deterministic_and_starts_with_a_write() {
    let trace = OperationMix::NinetyTen.trace(20);

    assert_eq!(trace, OperationMix::NinetyTen.trace(20));
    assert!(trace[0].is_write());
    assert!(trace[1..10].iter().all(|operation| operation.is_read()));
}

#[test]
fn workload_manifest_records_reproducible_fixture_inputs() {
    let manifest_path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/namesrv-parity/manifest.json");
    let manifest = std::fs::read_to_string(manifest_path).expect("workload manifest should be readable");
    let manifest: serde_json::Value = serde_json::from_str(&manifest).expect("workload manifest should be valid JSON");

    assert_eq!(manifest["schemaVersion"], 1);
    assert_eq!(manifest["seed"], 9189);
    assert_eq!(manifest["mixedWorkload"]["readPercent"], 90);
    assert_eq!(manifest["mixedWorkload"]["writePercent"], 10);
    assert_eq!(manifest["fixture"]["topicCount"], 10);
}
