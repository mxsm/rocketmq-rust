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

use std::path::PathBuf;

use rocketmq_sre_core::diagnostics::DiagnosticStatus;
use rocketmq_sre_eval::assertions::assert_phase2_quality;
use rocketmq_sre_eval::phase2::run_phase2_dataset;
use rocketmq_sre_eval::replay::load_dataset;

fn manifest_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/phase2/dataset-manifest.v1.yaml")
}

#[test]
fn phase2_saved_evidence_dataset_meets_the_fixed_quality_contract() {
    let dataset = load_dataset(&manifest_path()).expect("checked-in Phase 2 dataset should load");
    let report = run_phase2_dataset(&dataset).expect("saved Evidence should replay deterministically");

    assert_eq!(dataset.manifest.fixtures.len(), 20);
    assert_eq!(report.evaluable_fixtures, 18);
    assert_eq!(report.root_cause_top3_hits, 18);
    assert_eq!(report.root_cause_top3_rate, 1.0);
    assert_eq!(report.citation_coverage, Some(1.0));
    assert_eq!(report.mutation_calls, 0);
    assert_eq!(report.model_calls, 0);
    assert!(report.fixture_results.iter().all(|result| result.readonly_calls <= 12));
    assert_phase2_quality(&report, &dataset.quality).expect("fixed quality thresholds should pass");
}

#[test]
fn every_declared_scenario_keeps_a_saved_timeline_and_rules_only_input() {
    let dataset = load_dataset(&manifest_path()).expect("checked-in Phase 2 dataset should load");

    for entry in &dataset.manifest.fixtures {
        let fixture = dataset
            .fixture(&entry.fixture_id)
            .expect("manifest fixture should exist");
        assert!(!fixture.timeline.is_empty(), "{} must preserve a timeline", fixture.id);
        assert!(
            fixture.pack_runs.iter().all(|run| !run.pack.trim().is_empty()),
            "{} must name a compiled diagnostic pack",
            fixture.id
        );
    }
}

#[test]
fn missing_evidence_degrades_confidence_without_inventing_a_healthy_result() {
    let dataset = load_dataset(&manifest_path()).expect("checked-in Phase 2 dataset should load");
    let report = run_phase2_dataset(&dataset).expect("saved Evidence should replay deterministically");
    let result = |id: &str| {
        report
            .fixture_results
            .iter()
            .find(|result| result.fixture_id == id)
            .unwrap_or_else(|| panic!("{id} result must exist"))
    };
    let fault = result("disk-pressure-fault");
    let normal = result("disk-pressure-normal");
    let missing = result("disk-pressure-missing");

    assert!(fault.statuses.contains(&DiagnosticStatus::Fault));
    assert!(normal.statuses.contains(&DiagnosticStatus::Healthy));
    assert!(missing.statuses.contains(&DiagnosticStatus::Inconclusive));
    assert!(fault.max_confidence_percent() > missing.max_confidence_percent());
    assert!(missing.ranked_root_causes.is_empty());
}
