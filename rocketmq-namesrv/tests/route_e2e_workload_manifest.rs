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

#[path = "../benches/support/namesrv_harness.rs"]
mod namesrv_harness;

use std::path::PathBuf;

use namesrv_harness::load_workload_manifest;
use namesrv_harness::validate_workload_manifest;
use namesrv_harness::WorkloadOperation;

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("benches/fixtures/route_workloads.json")
}

#[test]
fn route_workload_manifest_covers_production_dimensions() {
    let manifest = load_workload_manifest(&fixture_path()).expect("workload fixture should parse");

    validate_workload_manifest(&manifest).expect("workload fixture should cover the required dimensions");
}

#[test]
fn smoke_trace_is_deterministic_and_preserves_exact_mix() {
    let manifest = load_workload_manifest(&fixture_path()).expect("workload fixture should parse");
    let profile = manifest.profile("smoke").expect("smoke profile should exist");

    let first = profile.trace(manifest.seed);
    let second = profile.trace(manifest.seed);
    assert_eq!(first, second);
    assert_eq!(first.len(), profile.operations);
    assert_eq!(
        first
            .iter()
            .filter(|entry| entry.operation == WorkloadOperation::RegistrationWrite)
            .count(),
        profile.operations * profile.write_percent as usize / 100
    );
    assert_eq!(
        first.iter().filter(|entry| entry.zone).count(),
        profile.operations * profile.zone_percent as usize / 100
    );
    assert_eq!(
        first.iter().filter(|entry| entry.standard_json).count(),
        profile.operations * profile.standard_json_percent as usize / 100
    );
}
