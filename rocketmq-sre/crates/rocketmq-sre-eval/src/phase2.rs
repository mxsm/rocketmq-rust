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

//! Phase 2 dataset runner and aggregate quality report.

use crate::replay::LoadedReplayDataset;
use crate::replay::ReplayError;
use crate::replay::ReplayFixtureResult;
use crate::replay::replay_fixture;

/// Aggregate result with a fixed manifest denominator.
#[derive(Clone, Debug, PartialEq)]
pub struct Phase2ReplayReport {
    pub fixture_results: Vec<ReplayFixtureResult>,
    pub evaluable_fixtures: usize,
    pub root_cause_top3_hits: usize,
    pub root_cause_top3_rate: f64,
    pub high_confidence_conclusions: usize,
    pub cited_high_confidence_conclusions: usize,
    pub citation_coverage: Option<f64>,
    pub mutation_calls: usize,
    pub model_calls: usize,
}

/// Replays every declared fixture twice and aggregates deterministic quality.
///
/// # Errors
///
/// Returns an error when a fixture cannot be replayed or the same saved input
/// produces different deterministic outputs.
pub fn run_phase2_dataset(dataset: &LoadedReplayDataset) -> Result<Phase2ReplayReport, ReplayError> {
    let mut fixture_results = Vec::with_capacity(dataset.manifest.fixtures.len());
    let mut evaluable_fixtures = 0;
    let mut top3_hits = 0;
    let mut high_confidence = 0;
    let mut cited_high_confidence = 0;
    let mut mutation_calls = 0;
    let mut model_calls = 0;
    let confidence_threshold = (dataset.quality.high_confidence_threshold.clamp(0.0, 1.0) * 100.0).round() as u8;

    for entry in &dataset.manifest.fixtures {
        let fixture = dataset
            .fixture(&entry.fixture_id)
            .ok_or_else(|| ReplayError::UnknownFixture(entry.fixture_id.clone()))?;
        let result = replay_fixture(fixture)?;
        let repeated = replay_fixture(fixture)?;
        if result != repeated {
            return Err(ReplayError::Diagnostic {
                fixture_id: fixture.id.clone(),
                pack: "determinism".to_owned(),
                reason: "identical saved input produced a different result".to_owned(),
            });
        }

        if entry.evaluable {
            evaluable_fixtures += 1;
            let top3 = result
                .ranked_root_causes
                .iter()
                .take(3)
                .map(|cause| cause.reason_code.as_str())
                .collect::<Vec<_>>();
            if entry
                .expected_root_causes
                .iter()
                .any(|expected| top3.contains(&expected.as_str()))
            {
                top3_hits += 1;
            }
        }
        for cause in &result.ranked_root_causes {
            if cause.confidence_percent >= confidence_threshold {
                high_confidence += 1;
                if !cause.supporting_evidence_ids.is_empty() {
                    cited_high_confidence += 1;
                }
            }
        }
        mutation_calls += result.mutation_calls;
        model_calls += result.model_calls;
        fixture_results.push(result);
    }

    let root_cause_top3_rate = if evaluable_fixtures == 0 {
        0.0
    } else {
        top3_hits as f64 / evaluable_fixtures as f64
    };
    let citation_coverage = (high_confidence > 0).then(|| cited_high_confidence as f64 / high_confidence as f64);

    Ok(Phase2ReplayReport {
        fixture_results,
        evaluable_fixtures,
        root_cause_top3_hits: top3_hits,
        root_cause_top3_rate,
        high_confidence_conclusions: high_confidence,
        cited_high_confidence_conclusions: cited_high_confidence,
        citation_coverage,
        mutation_calls,
        model_calls,
    })
}
