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

use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use rocketmq_sre_eval::assertions::assert_phase2_quality;
use rocketmq_sre_eval::phase2::run_phase2_dataset;
use rocketmq_sre_eval::replay::load_dataset;
use serde::Serialize;

#[derive(Serialize)]
struct ReplayQualityOutput {
    evaluable_fixtures: usize,
    root_cause_top3_hits: usize,
    root_cause_top3_rate: f64,
    high_confidence_conclusions: usize,
    cited_high_confidence_conclusions: usize,
    citation_coverage: Option<f64>,
    mutation_calls: usize,
    model_calls: usize,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("replay_quality_failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut manifest = PathBuf::from("tests/fixtures/phase2/dataset-manifest.v1.yaml");
    let mut compact = false;
    let mut arguments = env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--manifest" => {
                manifest = PathBuf::from(
                    arguments
                        .next()
                        .ok_or_else(|| "--manifest requires a path".to_owned())?,
                );
            }
            "--compact" => compact = true,
            other => return Err(format!("unknown argument `{other}`")),
        }
    }
    let dataset = load_dataset(&manifest).map_err(|error| error.to_string())?;
    let report = run_phase2_dataset(&dataset).map_err(|error| error.to_string())?;
    assert_phase2_quality(&report, &dataset.quality).map_err(|error| error.to_string())?;
    let output = ReplayQualityOutput {
        evaluable_fixtures: report.evaluable_fixtures,
        root_cause_top3_hits: report.root_cause_top3_hits,
        root_cause_top3_rate: report.root_cause_top3_rate,
        high_confidence_conclusions: report.high_confidence_conclusions,
        cited_high_confidence_conclusions: report.cited_high_confidence_conclusions,
        citation_coverage: report.citation_coverage,
        mutation_calls: report.mutation_calls,
        model_calls: report.model_calls,
    };
    let output = if compact {
        serde_json::to_string(&output)
    } else {
        serde_json::to_string_pretty(&output)
    }
    .map_err(|error| error.to_string())?;
    println!("{output}");
    Ok(())
}
