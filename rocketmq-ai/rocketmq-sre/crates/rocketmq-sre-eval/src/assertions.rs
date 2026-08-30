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

//! Simple, explicit Phase 2 replay quality assertions.

use thiserror::Error;

use crate::phase2::Phase2ReplayReport;
use crate::replay::ReplayQualityConfig;

/// Quality threshold failure from a deterministic replay.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum ReplayAssertionError {
    #[error("root-cause Top-3 rate {actual:.3} is below {minimum:.3}")]
    RootCauseTop3 { actual: f64, minimum: f64 },
    #[error("citation coverage {actual:.3} is below {minimum:.3}")]
    CitationCoverage { actual: f64, minimum: f64 },
    #[error("fixture `{fixture_id}` used {actual} read-only calls; limit is {maximum}")]
    TooManyReadOnlyCalls {
        fixture_id: String,
        actual: usize,
        maximum: usize,
    },
    #[error("replay recorded {actual} mutation calls; allowed count is {allowed}")]
    MutationCalls { actual: usize, allowed: usize },
    #[error("rules-only replay unexpectedly recorded {0} model calls")]
    ModelCalls(usize),
    #[error("no evaluable replay fixtures were declared")]
    NoEvaluableFixtures,
}

/// Enforces the checked-in Phase 2 quality configuration.
///
/// Citation coverage remains `None` when there are no high-confidence
/// conclusions; that is reported as not applicable and is not rewritten to
/// 100 percent.
///
/// # Errors
///
/// Returns the first stable threshold violation.
pub fn assert_phase2_quality(
    report: &Phase2ReplayReport,
    quality: &ReplayQualityConfig,
) -> Result<(), ReplayAssertionError> {
    if report.evaluable_fixtures == 0 {
        return Err(ReplayAssertionError::NoEvaluableFixtures);
    }
    if report.root_cause_top3_rate < quality.root_cause_top3_min {
        return Err(ReplayAssertionError::RootCauseTop3 {
            actual: report.root_cause_top3_rate,
            minimum: quality.root_cause_top3_min,
        });
    }
    if let Some(actual) = report.citation_coverage
        && actual < quality.citation_coverage_min
    {
        return Err(ReplayAssertionError::CitationCoverage {
            actual,
            minimum: quality.citation_coverage_min,
        });
    }
    if let Some(result) = report
        .fixture_results
        .iter()
        .find(|result| result.readonly_calls > quality.max_readonly_tool_calls)
    {
        return Err(ReplayAssertionError::TooManyReadOnlyCalls {
            fixture_id: result.fixture_id.clone(),
            actual: result.readonly_calls,
            maximum: quality.max_readonly_tool_calls,
        });
    }
    if report.mutation_calls > quality.mutation_calls_allowed {
        return Err(ReplayAssertionError::MutationCalls {
            actual: report.mutation_calls,
            allowed: quality.mutation_calls_allowed,
        });
    }
    if report.model_calls != 0 {
        return Err(ReplayAssertionError::ModelCalls(report.model_calls));
    }
    Ok(())
}
