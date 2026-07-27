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

use super::trend::ObservedPoint;

/// Non-causal median-shift hint.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ChangePointEvaluation {
    pub detected_at_seconds: i64,
    pub before_value: f64,
    pub after_value: f64,
    pub score: f64,
}

/// Compares two adjacent bounded windows using a robust pooled MAD scale.
/// The result is only an investigation hint.
#[must_use]
pub fn detect_change_point(
    points: &[ObservedPoint],
    window_samples: usize,
    score_threshold: f64,
) -> Option<ChangePointEvaluation> {
    if window_samples < 3 || !score_threshold.is_finite() || score_threshold <= 0.0 {
        return None;
    }
    let clean = points
        .iter()
        .copied()
        .filter(|point| point.at_seconds >= 0 && point.value.is_finite())
        .collect::<Vec<_>>();
    if clean.len() < window_samples.saturating_mul(2) {
        return None;
    }
    let split = clean.len() - window_samples;
    let before_start = split - window_samples;
    let before = &clean[before_start..split];
    let after = &clean[split..];
    let before_median = median_values(before)?;
    let after_median = median_values(after)?;
    let mut deviations = clean[before_start..]
        .iter()
        .map(|point| {
            let center = if point.at_seconds < after[0].at_seconds {
                before_median
            } else {
                after_median
            };
            (point.value - center).abs()
        })
        .collect::<Vec<_>>();
    deviations.sort_by(f64::total_cmp);
    let scale = median(&deviations)?.max(1.0e-9);
    let score = (after_median - before_median).abs() / scale;
    (score >= score_threshold).then_some(ChangePointEvaluation {
        detected_at_seconds: after[0].at_seconds,
        before_value: before_median,
        after_value: after_median,
        score,
    })
}

fn median_values(points: &[ObservedPoint]) -> Option<f64> {
    let mut values = points.iter().map(|point| point.value).collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    median(&values)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sustained_level_shift_is_reported_as_hint() {
        let points = (0_i32..12)
            .map(|index| ObservedPoint {
                at_seconds: i64::from(index) * 3_600,
                value: if index < 6 {
                    10.0 + f64::from(index % 2)
                } else {
                    30.0 + f64::from(index % 2)
                },
            })
            .collect::<Vec<_>>();
        let hint = detect_change_point(&points, 6, 3.5).expect("change hint");

        assert_eq!(hint.detected_at_seconds, 6 * 3_600);
        assert!(hint.score > 3.5);
    }

    #[test]
    fn stable_series_has_no_change_hint() {
        let points = (0_i32..12)
            .map(|index| ObservedPoint {
                at_seconds: i64::from(index) * 3_600,
                value: 10.0 + f64::from(index % 2),
            })
            .collect::<Vec<_>>();
        assert!(detect_change_point(&points, 6, 3.5).is_none());
    }
}
