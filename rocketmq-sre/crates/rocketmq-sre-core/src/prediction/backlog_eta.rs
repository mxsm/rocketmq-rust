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
use super::trend::TrendEvaluation;
use super::trend::TrendPolicy;
use super::trend::evaluate_trend;

const SECONDS_PER_HOUR: f64 = 3_600.0;

/// Explainable backlog growth or drain estimate.
#[derive(Clone, Debug, PartialEq)]
pub struct BacklogEvaluation {
    pub trend: TrendEvaluation,
    pub current_value: f64,
    pub arrival_rate_per_second: Option<f64>,
    pub drain_rate_per_second: Option<f64>,
    pub estimated_clear_at_seconds: Option<i64>,
}

/// Evaluates net backlog change. Because a single backlog gauge does not
/// identify independent arrival and service rates, exactly one of the exposed
/// rates is populated from the fitted net slope.
///
/// # Errors
///
/// Returns a stable reason when the trend policy is invalid.
pub fn evaluate_backlog(
    points: &[ObservedPoint],
    policy: TrendPolicy,
    now_seconds: i64,
) -> Result<BacklogEvaluation, &'static str> {
    let trend = evaluate_trend(points, policy, None, now_seconds)?;
    let current_value = trend.observed_points.last().map_or(0.0, |point| point.value.max(0.0));
    let slope_per_second = trend.slope_per_hour.map(|slope| slope / SECONDS_PER_HOUR);
    let arrival_rate_per_second = matches!(trend.status, ForecastStatus::Ready)
        .then(|| slope_per_second.filter(|slope| *slope > 0.0))
        .flatten();
    let drain_rate_per_second = matches!(trend.status, ForecastStatus::Ready)
        .then(|| slope_per_second.filter(|slope| *slope < 0.0).map(f64::abs))
        .flatten();
    let estimated_clear_at_seconds = if current_value <= f64::EPSILON && matches!(trend.status, ForecastStatus::Ready) {
        Some(now_seconds)
    } else if matches!(trend.trend, ForecastTrend::Decreasing) {
        drain_rate_per_second.and_then(|rate| {
            let seconds = current_value / rate;
            (seconds.is_finite() && seconds >= 0.0 && seconds <= i64::MAX as f64)
                .then(|| now_seconds.checked_add(seconds.ceil() as i64))
                .flatten()
        })
    } else {
        None
    };
    Ok(BacklogEvaluation {
        trend,
        current_value,
        arrival_rate_per_second,
        drain_rate_per_second,
        estimated_clear_at_seconds,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> TrendPolicy {
        TrendPolicy {
            window_seconds: 9 * 3_600,
            expected_interval_seconds: 3_600,
            min_samples: 8,
            min_coverage_ratio: 0.8,
            freshness_seconds: 7_200,
            max_normalized_volatility: 0.1,
            min_absolute_slope_per_hour: 0.01,
            projection_horizon_hours: 24,
            projection_points: 4,
        }
    }

    #[test]
    fn draining_backlog_has_clear_eta_without_inventing_arrival_rate() {
        let points = (0..10)
            .map(|index| ObservedPoint {
                at_seconds: index * 3_600,
                value: 100.0 - index as f64 * 10.0,
            })
            .collect::<Vec<_>>();
        let result = evaluate_backlog(&points, policy(), 9 * 3_600).expect("backlog");

        assert_eq!(result.current_value, 10.0);
        assert!(result.arrival_rate_per_second.is_none());
        assert!(result.drain_rate_per_second.is_some_and(|rate| rate > 0.0));
        assert_eq!(result.estimated_clear_at_seconds, Some(10 * 3_600));
    }

    #[test]
    fn growing_backlog_never_claims_a_clear_eta() {
        let points = (0..10)
            .map(|index| ObservedPoint {
                at_seconds: index * 3_600,
                value: index as f64 * 10.0,
            })
            .collect::<Vec<_>>();
        let result = evaluate_backlog(&points, policy(), 9 * 3_600).expect("backlog");

        assert!(result.arrival_rate_per_second.is_some());
        assert!(result.drain_rate_per_second.is_none());
        assert!(result.estimated_clear_at_seconds.is_none());
    }
}
