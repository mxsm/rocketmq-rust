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

use rocketmq_sre_contracts::ForecastStatus;
use rocketmq_sre_contracts::ForecastTrend;

use super::trend::ObservedPoint;
use super::trend::ThresholdDirection;
use super::trend::TrendEvaluation;
use super::trend::TrendPolicy;
use super::trend::evaluate_trend;

/// Capacity result plus deterministic operator-facing advice.
#[derive(Clone, Debug, PartialEq)]
pub struct CapacityEvaluation {
    pub trend: TrendEvaluation,
    pub advisories: Vec<String>,
}

/// Evaluates a resource whose unsafe boundary is an upper capacity threshold.
///
/// # Errors
///
/// Returns a stable reason when the trend policy is invalid.
pub fn evaluate_capacity(
    points: &[ObservedPoint],
    policy: TrendPolicy,
    threshold: f64,
    now_seconds: i64,
) -> Result<CapacityEvaluation, &'static str> {
    let trend = evaluate_trend(
        points,
        policy,
        Some((threshold, ThresholdDirection::Upper)),
        now_seconds,
    )?;
    let advisories = match (trend.status, trend.trend, trend.exhaustion_at_seconds) {
        (ForecastStatus::Ready, ForecastTrend::Increasing, Some(_)) => {
            vec!["review_capacity_before_projected_threshold".to_owned()]
        }
        (ForecastStatus::Ready, _, Some(_)) => vec!["review_threshold_breach".to_owned()],
        (ForecastStatus::Ready, _, None) => vec!["continue_observation".to_owned()],
        (ForecastStatus::InsufficientData, _, _) => vec!["improve_metric_coverage".to_owned()],
        (ForecastStatus::Stale, _, _) => vec!["restore_metric_freshness".to_owned()],
        (ForecastStatus::UnstableTrend | ForecastStatus::Unsupported, _, _) => {
            vec!["manual_capacity_review_required".to_owned()]
        }
    };
    Ok(CapacityEvaluation { trend, advisories })
}
