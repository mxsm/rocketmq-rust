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
use rocketmq_sre_contracts::Seasonality;

use super::trend::ObservedPoint;

/// Robust seasonal baseline controls.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BaselinePolicy {
    pub seasonality: Seasonality,
    pub period_seconds: u64,
    pub bucket_width_seconds: u64,
    pub history_window_seconds: u64,
    pub min_samples: usize,
    pub robust_z_threshold: f64,
    pub lower_quantile_threshold: f64,
    pub upper_quantile_threshold: f64,
}

impl BaselinePolicy {
    /// Checks that seasonal and anomaly bounds are coherent.
    ///
    /// # Errors
    ///
    /// Returns a stable reason for invalid periods or thresholds.
    pub fn validate(self) -> Result<(), &'static str> {
        if self.period_seconds == 0
            || self.bucket_width_seconds == 0
            || self.bucket_width_seconds > self.period_seconds
            || self.history_window_seconds < self.period_seconds
            || self.min_samples < 3
            || !self.robust_z_threshold.is_finite()
            || self.robust_z_threshold <= 0.0
            || !self.lower_quantile_threshold.is_finite()
            || !self.upper_quantile_threshold.is_finite()
            || !(0.0..=1.0).contains(&self.lower_quantile_threshold)
            || !(0.0..=1.0).contains(&self.upper_quantile_threshold)
            || self.lower_quantile_threshold >= self.upper_quantile_threshold
        {
            return Err("invalid_baseline_policy");
        }
        Ok(())
    }
}

/// Baseline statistics for a matching seasonal bucket.
#[derive(Clone, Debug, PartialEq)]
pub struct BaselineEvaluation {
    pub status: ForecastStatus,
    pub median: Option<f64>,
    pub median_absolute_deviation: Option<f64>,
    pub sample_count: usize,
    pub coverage_ratio: f64,
    pub values: Vec<f64>,
}

/// Anomaly output. It is an investigation hint and never a root-cause claim.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AnomalyEvaluation {
    pub status: ForecastStatus,
    pub observed_value: Option<f64>,
    pub baseline_median: Option<f64>,
    pub robust_z_score: Option<f64>,
    pub empirical_quantile: Option<f64>,
    pub anomaly: bool,
}

/// Builds a seasonal reference bucket aligned with `reference_seconds`.
///
/// # Errors
///
/// Returns a stable reason when the policy is invalid.
pub fn build_baseline(
    points: &[ObservedPoint],
    reference_seconds: i64,
    policy: BaselinePolicy,
) -> Result<BaselineEvaluation, &'static str> {
    policy.validate()?;
    let lower_bound =
        reference_seconds.saturating_sub(i64::try_from(policy.history_window_seconds).unwrap_or(i64::MAX));
    let period = i64::try_from(policy.period_seconds).map_err(|_| "period_too_large")?;
    let half_bucket = i64::try_from(policy.bucket_width_seconds / 2).map_err(|_| "bucket_too_large")?;
    let reference_phase = reference_seconds.rem_euclid(period);
    let mut values = points
        .iter()
        .filter(|point| {
            point.value.is_finite()
                && point.at_seconds >= lower_bound
                && point.at_seconds < reference_seconds
                && circular_distance(point.at_seconds.rem_euclid(period), reference_phase, period) <= half_bucket
        })
        .map(|point| point.value)
        .collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    let expected_samples = usize::try_from(policy.history_window_seconds / policy.period_seconds)
        .unwrap_or(usize::MAX)
        .max(policy.min_samples);
    let coverage_ratio = ratio(values.len(), expected_samples);
    if values.len() < policy.min_samples {
        return Ok(BaselineEvaluation {
            status: ForecastStatus::InsufficientData,
            median: None,
            median_absolute_deviation: None,
            sample_count: values.len(),
            coverage_ratio,
            values,
        });
    }
    let center = median(&values).ok_or("baseline_median_failed")?;
    let mut deviations = values.iter().map(|value| (value - center).abs()).collect::<Vec<_>>();
    deviations.sort_by(f64::total_cmp);
    let mad = median(&deviations).ok_or("baseline_mad_failed")?;
    Ok(BaselineEvaluation {
        status: ForecastStatus::Ready,
        median: Some(center),
        median_absolute_deviation: Some(mad),
        sample_count: values.len(),
        coverage_ratio,
        values,
    })
}

/// Assesses one current observation against a robust seasonal baseline.
#[must_use]
pub fn assess_anomaly(
    baseline: &BaselineEvaluation,
    observed_value: Option<f64>,
    policy: BaselinePolicy,
) -> AnomalyEvaluation {
    let observed_value = observed_value.filter(|value| value.is_finite());
    let (Some(value), Some(median), Some(mad)) = (observed_value, baseline.median, baseline.median_absolute_deviation)
    else {
        return AnomalyEvaluation {
            status: ForecastStatus::InsufficientData,
            observed_value,
            baseline_median: baseline.median,
            robust_z_score: None,
            empirical_quantile: None,
            anomaly: false,
        };
    };
    if baseline.status != ForecastStatus::Ready {
        return AnomalyEvaluation {
            status: baseline.status,
            observed_value,
            baseline_median: Some(median),
            robust_z_score: None,
            empirical_quantile: None,
            anomaly: false,
        };
    }
    let robust_z_score = if mad <= f64::EPSILON {
        (value - median)
            .abs()
            .gt(&f64::EPSILON)
            .then_some(if value >= median {
                f64::INFINITY
            } else {
                f64::NEG_INFINITY
            })
            .or(Some(0.0))
    } else {
        Some(0.674_489_75 * (value - median) / mad)
    };
    let empirical_quantile =
        Some(baseline.values.partition_point(|sample| *sample <= value) as f64 / baseline.values.len() as f64);
    let anomaly = robust_z_score.is_some_and(|score| score.abs() >= policy.robust_z_threshold)
        && empirical_quantile.is_some_and(|quantile| {
            quantile <= policy.lower_quantile_threshold || quantile >= policy.upper_quantile_threshold
        });
    AnomalyEvaluation {
        status: ForecastStatus::Ready,
        observed_value: Some(value),
        baseline_median: Some(median),
        robust_z_score,
        empirical_quantile,
        anomaly,
    }
}

fn circular_distance(left: i64, right: i64, period: i64) -> i64 {
    let direct = (left - right).abs();
    direct.min(period - direct)
}

fn median(sorted: &[f64]) -> Option<f64> {
    let middle = sorted.len() / 2;
    if sorted.is_empty() {
        None
    } else if sorted.len().is_multiple_of(2) {
        Some((sorted[middle - 1] + sorted[middle]) / 2.0)
    } else {
        Some(sorted[middle])
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

    fn daily_policy() -> BaselinePolicy {
        BaselinePolicy {
            seasonality: Seasonality::Daily,
            period_seconds: 86_400,
            bucket_width_seconds: 3_600,
            history_window_seconds: 14 * 86_400,
            min_samples: 7,
            robust_z_threshold: 3.5,
            lower_quantile_threshold: 0.01,
            upper_quantile_threshold: 0.99,
        }
    }

    #[test]
    fn daily_seasonal_bucket_detects_robust_outlier() {
        let points = (1_i32..=14)
            .map(|day| ObservedPoint {
                at_seconds: i64::from(day) * 86_400,
                value: 100.0 + f64::from(day % 2),
            })
            .collect::<Vec<_>>();
        let reference = 15 * 86_400;
        let baseline = build_baseline(&points, reference, daily_policy()).expect("baseline");
        let anomaly = assess_anomaly(&baseline, Some(500.0), daily_policy());

        assert_eq!(baseline.sample_count, 14);
        assert_eq!(anomaly.status, ForecastStatus::Ready);
        assert!(anomaly.anomaly);
        assert!(anomaly.robust_z_score.is_some_and(|score| score > 3.5));
    }

    #[test]
    fn seasonal_baseline_refuses_too_few_matching_samples() {
        let baseline = build_baseline(
            &[ObservedPoint {
                at_seconds: 86_400,
                value: 10.0,
            }],
            2 * 86_400,
            daily_policy(),
        )
        .expect("baseline");

        assert_eq!(baseline.status, ForecastStatus::InsufficientData);
        assert!(!assess_anomaly(&baseline, Some(20.0), daily_policy()).anomaly);
    }
}
