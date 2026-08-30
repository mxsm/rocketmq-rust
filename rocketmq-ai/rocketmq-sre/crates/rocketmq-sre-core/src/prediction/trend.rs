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

use std::collections::BTreeMap;

use rocketmq_sre_contracts::ForecastBacktest;
use rocketmq_sre_contracts::ForecastQuality;
use rocketmq_sre_contracts::ForecastStatus;
use rocketmq_sre_contracts::ForecastTrend;

const SECONDS_PER_HOUR: f64 = 3_600.0;

/// One finite historical observation represented without a wall-clock SDK.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ObservedPoint {
    pub at_seconds: i64,
    pub value: f64,
}

/// Direction in which a bounded threshold becomes unsafe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThresholdDirection {
    Upper,
    Lower,
}

/// Validated inputs for explainable linear trend evaluation.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TrendPolicy {
    pub window_seconds: u64,
    pub expected_interval_seconds: u64,
    pub min_samples: usize,
    pub min_coverage_ratio: f64,
    pub freshness_seconds: u64,
    pub max_normalized_volatility: f64,
    pub min_absolute_slope_per_hour: f64,
    pub projection_horizon_hours: u64,
    pub projection_points: usize,
}

impl TrendPolicy {
    /// Checks policy bounds before any evidence is evaluated.
    ///
    /// # Errors
    ///
    /// Returns a stable reason when a numeric or cardinality bound is invalid.
    pub fn validate(self) -> Result<(), &'static str> {
        if self.window_seconds == 0
            || self.expected_interval_seconds == 0
            || self.window_seconds < self.expected_interval_seconds
            || self.min_samples < 3
            || !self.min_coverage_ratio.is_finite()
            || !(0.0..=1.0).contains(&self.min_coverage_ratio)
            || !self.max_normalized_volatility.is_finite()
            || self.max_normalized_volatility <= 0.0
            || !self.min_absolute_slope_per_hour.is_finite()
            || self.min_absolute_slope_per_hour < 0.0
            || self.projection_horizon_hours == 0
            || !(1..=64).contains(&self.projection_points)
        {
            return Err("invalid_trend_policy");
        }
        Ok(())
    }

    fn expected_samples(self) -> usize {
        usize::try_from(self.window_seconds / self.expected_interval_seconds)
            .unwrap_or(usize::MAX)
            .saturating_add(1)
    }
}

/// Full deterministic result used to construct public forecast contracts.
#[derive(Clone, Debug, PartialEq)]
pub struct TrendEvaluation {
    pub status: ForecastStatus,
    pub quality: ForecastQuality,
    pub trend: ForecastTrend,
    pub sample_start_seconds: Option<i64>,
    pub sample_end_seconds: Option<i64>,
    pub coverage_ratio: f64,
    pub slope_per_hour: Option<f64>,
    pub normalized_volatility: Option<f64>,
    pub threshold: Option<f64>,
    pub exhaustion_at_seconds: Option<i64>,
    pub observed_points: Vec<ObservedPoint>,
    pub projected_points: Vec<ObservedPoint>,
    pub backtest: ForecastBacktest,
}

/// Removes invalid points, merges duplicate timestamps and evaluates a linear
/// trend. An exhaustion estimate is emitted only for a fresh, sufficiently
/// covered, low-volatility trend moving toward the supplied threshold.
///
/// # Errors
///
/// Returns a stable reason when the policy itself is invalid.
pub fn evaluate_trend(
    points: &[ObservedPoint],
    policy: TrendPolicy,
    threshold: Option<(f64, ThresholdDirection)>,
    now_seconds: i64,
) -> Result<TrendEvaluation, &'static str> {
    policy.validate()?;
    let points = sanitize(points);
    let coverage_ratio = ratio(points.len(), policy.expected_samples());
    let sample_start_seconds = points.first().map(|point| point.at_seconds);
    let sample_end_seconds = points.last().map(|point| point.at_seconds);
    let threshold_value = threshold.map(|value| value.0);
    let empty_backtest = ForecastBacktest {
        evaluated_points: 0,
        mean_absolute_error: None,
        bias: None,
        interval_coverage_ratio: None,
    };
    if points.len() < policy.min_samples || coverage_ratio < policy.min_coverage_ratio {
        return Ok(TrendEvaluation {
            status: ForecastStatus::InsufficientData,
            quality: ForecastQuality::Low,
            trend: ForecastTrend::Unknown,
            sample_start_seconds,
            sample_end_seconds,
            coverage_ratio,
            slope_per_hour: None,
            normalized_volatility: None,
            threshold: threshold_value,
            exhaustion_at_seconds: None,
            observed_points: points,
            projected_points: Vec::new(),
            backtest: empty_backtest,
        });
    }
    let latest = points.last().copied().ok_or("missing_sanitized_points")?;
    if now_seconds.saturating_sub(latest.at_seconds) > i64::try_from(policy.freshness_seconds).unwrap_or(i64::MAX) {
        return Ok(TrendEvaluation {
            status: ForecastStatus::Stale,
            quality: ForecastQuality::Low,
            trend: ForecastTrend::Unknown,
            sample_start_seconds,
            sample_end_seconds,
            coverage_ratio,
            slope_per_hour: None,
            normalized_volatility: None,
            threshold: threshold_value,
            exhaustion_at_seconds: None,
            observed_points: points,
            projected_points: Vec::new(),
            backtest: empty_backtest,
        });
    }

    let fit = linear_fit(&points).ok_or("trend_fit_failed")?;
    let normalized_volatility = fit.rmse / fit.mean_abs.max(1.0);
    let quality = quality(coverage_ratio, normalized_volatility, policy);
    let backtest = backtest(&points);
    if normalized_volatility > policy.max_normalized_volatility {
        return Ok(TrendEvaluation {
            status: ForecastStatus::InsufficientData,
            quality: ForecastQuality::Low,
            trend: ForecastTrend::Unstable,
            sample_start_seconds,
            sample_end_seconds,
            coverage_ratio,
            slope_per_hour: Some(fit.slope_per_hour),
            normalized_volatility: Some(normalized_volatility),
            threshold: threshold_value,
            exhaustion_at_seconds: None,
            observed_points: points,
            projected_points: Vec::new(),
            backtest,
        });
    }
    let trend = classify_trend(fit.slope_per_hour, policy.min_absolute_slope_per_hour);
    let exhaustion_at_seconds = threshold
        .and_then(|(threshold, direction)| threshold_time(latest, fit.slope_per_hour, threshold, direction, policy));
    let projected_points = project(latest, fit.slope_per_hour, policy);
    Ok(TrendEvaluation {
        status: ForecastStatus::Ready,
        quality,
        trend,
        sample_start_seconds,
        sample_end_seconds,
        coverage_ratio,
        slope_per_hour: Some(fit.slope_per_hour),
        normalized_volatility: Some(normalized_volatility),
        threshold: threshold_value,
        exhaustion_at_seconds,
        observed_points: points,
        projected_points,
        backtest,
    })
}

fn sanitize(points: &[ObservedPoint]) -> Vec<ObservedPoint> {
    let mut grouped = BTreeMap::<i64, (f64, u32)>::new();
    for point in points
        .iter()
        .filter(|point| point.at_seconds >= 0 && point.value.is_finite())
    {
        let entry = grouped.entry(point.at_seconds).or_insert((0.0, 0));
        entry.0 += point.value;
        entry.1 = entry.1.saturating_add(1);
    }
    grouped
        .into_iter()
        .filter_map(|(at_seconds, (sum, count))| {
            let value = sum / f64::from(count);
            value.is_finite().then_some(ObservedPoint { at_seconds, value })
        })
        .collect()
}

#[derive(Clone, Copy)]
struct LinearFit {
    intercept: f64,
    slope_per_hour: f64,
    rmse: f64,
    mean_abs: f64,
    origin_seconds: i64,
}

fn linear_fit(points: &[ObservedPoint]) -> Option<LinearFit> {
    let origin_seconds = points.first()?.at_seconds;
    let n = points.len() as f64;
    let mean_x = points
        .iter()
        .map(|point| (point.at_seconds - origin_seconds) as f64 / SECONDS_PER_HOUR)
        .sum::<f64>()
        / n;
    let mean_y = points.iter().map(|point| point.value).sum::<f64>() / n;
    let (numerator, denominator) = points.iter().fold((0.0, 0.0), |(num, den), point| {
        let x = (point.at_seconds - origin_seconds) as f64 / SECONDS_PER_HOUR;
        let centered_x = x - mean_x;
        (num + centered_x * (point.value - mean_y), den + centered_x * centered_x)
    });
    if denominator <= f64::EPSILON {
        return None;
    }
    let slope_per_hour = numerator / denominator;
    let intercept = mean_y - slope_per_hour * mean_x;
    let squared_error = points
        .iter()
        .map(|point| {
            let x = (point.at_seconds - origin_seconds) as f64 / SECONDS_PER_HOUR;
            let residual = point.value - (intercept + slope_per_hour * x);
            residual * residual
        })
        .sum::<f64>();
    Some(LinearFit {
        intercept,
        slope_per_hour,
        rmse: (squared_error / n).sqrt(),
        mean_abs: points.iter().map(|point| point.value.abs()).sum::<f64>() / n,
        origin_seconds,
    })
}

fn quality(coverage: f64, volatility: f64, policy: TrendPolicy) -> ForecastQuality {
    if coverage >= 0.95 && volatility <= policy.max_normalized_volatility / 2.0 {
        ForecastQuality::High
    } else if coverage >= policy.min_coverage_ratio && volatility <= policy.max_normalized_volatility {
        ForecastQuality::Medium
    } else {
        ForecastQuality::Low
    }
}

fn classify_trend(slope: f64, minimum: f64) -> ForecastTrend {
    if slope > minimum {
        ForecastTrend::Increasing
    } else if slope < -minimum {
        ForecastTrend::Decreasing
    } else {
        ForecastTrend::Stable
    }
}

fn threshold_time(
    latest: ObservedPoint,
    slope_per_hour: f64,
    threshold: f64,
    direction: ThresholdDirection,
    policy: TrendPolicy,
) -> Option<i64> {
    if !threshold.is_finite() {
        return None;
    }
    let hours = match direction {
        ThresholdDirection::Upper if latest.value >= threshold => 0.0,
        ThresholdDirection::Upper if slope_per_hour > policy.min_absolute_slope_per_hour => {
            (threshold - latest.value) / slope_per_hour
        }
        ThresholdDirection::Lower if latest.value <= threshold => 0.0,
        ThresholdDirection::Lower if slope_per_hour < -policy.min_absolute_slope_per_hour => {
            (threshold - latest.value) / slope_per_hour
        }
        ThresholdDirection::Upper | ThresholdDirection::Lower => return None,
    };
    if !hours.is_finite() || hours < 0.0 || hours > i64::MAX as f64 / SECONDS_PER_HOUR {
        return None;
    }
    latest.at_seconds.checked_add((hours * SECONDS_PER_HOUR).round() as i64)
}

fn project(latest: ObservedPoint, slope_per_hour: f64, policy: TrendPolicy) -> Vec<ObservedPoint> {
    let count = policy.projection_points;
    (1..=count)
        .filter_map(|index| {
            let hours = policy.projection_horizon_hours as f64 * index as f64 / count as f64;
            let seconds = (hours * SECONDS_PER_HOUR).round() as i64;
            let value = latest.value + slope_per_hour * hours;
            latest
                .at_seconds
                .checked_add(seconds)
                .filter(|_| value.is_finite())
                .map(|at_seconds| ObservedPoint { at_seconds, value })
        })
        .collect()
}

fn backtest(points: &[ObservedPoint]) -> ForecastBacktest {
    let holdout = (points.len() / 5).max(2);
    let train_len = points.len().saturating_sub(holdout);
    if train_len < 3 {
        return ForecastBacktest {
            evaluated_points: 0,
            mean_absolute_error: None,
            bias: None,
            interval_coverage_ratio: None,
        };
    }
    let Some(fit) = linear_fit(&points[..train_len]) else {
        return ForecastBacktest {
            evaluated_points: 0,
            mean_absolute_error: None,
            bias: None,
            interval_coverage_ratio: None,
        };
    };
    let interval = (fit.rmse * 1.96).max(f64::EPSILON);
    let errors = points[train_len..]
        .iter()
        .map(|point| {
            let hours = (point.at_seconds - fit.origin_seconds) as f64 / SECONDS_PER_HOUR;
            fit.intercept + fit.slope_per_hour * hours - point.value
        })
        .collect::<Vec<_>>();
    let count = errors.len() as f64;
    ForecastBacktest {
        evaluated_points: u32::try_from(errors.len()).unwrap_or(u32::MAX),
        mean_absolute_error: Some(errors.iter().map(|error| error.abs()).sum::<f64>() / count),
        bias: Some(errors.iter().sum::<f64>() / count),
        interval_coverage_ratio: Some(errors.iter().filter(|error| error.abs() <= interval).count() as f64 / count),
    }
}

fn ratio(actual: usize, expected: usize) -> f64 {
    if expected == 0 {
        0.0
    } else {
        (actual as f64 / expected as f64).clamp(0.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(window_hours: u64, min_samples: usize) -> TrendPolicy {
        TrendPolicy {
            window_seconds: window_hours * 3_600,
            expected_interval_seconds: 3_600,
            min_samples,
            min_coverage_ratio: 0.8,
            freshness_seconds: 7_200,
            max_normalized_volatility: 0.08,
            min_absolute_slope_per_hour: 0.01,
            projection_horizon_hours: 24,
            projection_points: 4,
        }
    }

    fn series(count: usize, initial: f64, slope: f64) -> Vec<ObservedPoint> {
        (0..count)
            .map(|index| ObservedPoint {
                at_seconds: index as i64 * 3_600,
                value: initial + slope * index as f64,
            })
            .collect()
    }

    #[test]
    fn stable_positive_growth_estimates_upper_threshold_and_backtests() {
        let points = series(25, 50.0, 2.0);
        let result = evaluate_trend(
            &points,
            policy(24, 20),
            Some((100.0, ThresholdDirection::Upper)),
            24 * 3_600,
        )
        .expect("trend");

        assert_eq!(result.status, ForecastStatus::Ready);
        assert_eq!(result.trend, ForecastTrend::Increasing);
        assert_eq!(result.slope_per_hour, Some(2.0));
        assert_eq!(result.exhaustion_at_seconds, Some(25 * 3_600));
        assert!(
            result
                .backtest
                .mean_absolute_error
                .is_some_and(|value| value < 0.000_001)
        );
    }

    #[test]
    fn decreasing_and_unchanged_series_never_claim_upper_exhaustion() {
        for points in [series(25, 100.0, -2.0), series(25, 50.0, 0.0)] {
            let result = evaluate_trend(
                &points,
                policy(24, 20),
                Some((120.0, ThresholdDirection::Upper)),
                24 * 3_600,
            )
            .expect("trend");
            assert_eq!(result.status, ForecastStatus::Ready);
            assert_eq!(result.exhaustion_at_seconds, None);
        }
    }

    #[test]
    fn low_coverage_and_stale_series_fail_closed() {
        let low_coverage = evaluate_trend(&series(8, 1.0, 1.0), policy(24, 5), None, 7 * 3_600).expect("trend");
        assert_eq!(low_coverage.status, ForecastStatus::InsufficientData);

        let stale = evaluate_trend(&series(25, 1.0, 1.0), policy(24, 20), None, 40 * 3_600).expect("trend");
        assert_eq!(stale.status, ForecastStatus::Stale);
    }

    #[test]
    fn noisy_slope_is_explicitly_insufficient() {
        let mut points = series(25, 50.0, 1.0);
        for (index, point) in points.iter_mut().enumerate() {
            point.value += if index % 2 == 0 { 100.0 } else { -100.0 };
        }
        let result = evaluate_trend(&points, policy(24, 20), None, 24 * 3_600).expect("trend");
        assert_eq!(result.status, ForecastStatus::InsufficientData);
        assert_eq!(result.trend, ForecastTrend::Unstable);
    }

    #[test]
    fn invalid_and_duplicate_points_are_sanitized_deterministically() {
        let mut points = series(25, 1.0, 1.0);
        points.push(ObservedPoint {
            at_seconds: 0,
            value: 3.0,
        });
        points.push(ObservedPoint {
            at_seconds: 1,
            value: f64::NAN,
        });
        let result = evaluate_trend(&points, policy(24, 20), None, 24 * 3_600).expect("trend");

        assert_eq!(result.observed_points.len(), 25);
        assert_eq!(result.observed_points[0].value, 2.0);
    }
}
