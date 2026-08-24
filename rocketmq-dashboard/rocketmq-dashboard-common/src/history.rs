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

//! Real metric history points and deterministic gap/retention operations.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// Metric kinds persisted by the native Dashboard collector.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryMetricKind {
    /// Total messages derived from a successful Topic offset response.
    TopicMessages,
    /// Produce TPS present in a successful Broker runtime response.
    BrokerProduceTps,
    /// Consume TPS present in a successful Broker runtime response.
    BrokerConsumeTps,
}

/// One real metric observation. Missing samples have no point representation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryPoint {
    /// Metric kind.
    pub metric: HistoryMetricKind,
    /// Topic name or complete serialized Broker series identity.
    pub series_identity: String,
    /// Source timestamp retained without bucket interpolation.
    pub timestamp_epoch_ms: u64,
    /// Real observed value.
    pub value: f64,
    /// Connection/config revision that produced the sample.
    pub source_revision: u64,
}

/// One contiguous series segment suitable for a chart line.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistorySeries {
    /// Metric kind.
    pub metric: HistoryMetricKind,
    /// Stable series identity.
    pub series_identity: String,
    /// Contiguous points sorted by timestamp.
    pub points: Vec<HistoryPoint>,
}

/// Hard retention limits applied after every successful persisted sample.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryRetention {
    /// Maximum observations retained for one metric/identity pair.
    pub max_points_per_series: usize,
    /// Maximum distinct metric/identity pairs retained globally.
    pub max_series: usize,
    /// Maximum observations retained across the complete file.
    pub max_total_points: usize,
}

/// Filters actual points by inclusive timestamp bounds without filling gaps.
pub fn filter_history_range(points: &[HistoryPoint], start_epoch_ms: u64, end_epoch_ms: u64) -> Vec<HistoryPoint> {
    let mut filtered = points
        .iter()
        .filter(|point| point.timestamp_epoch_ms >= start_epoch_ms && point.timestamp_epoch_ms <= end_epoch_ms)
        .cloned()
        .collect::<Vec<_>>();
    sort_history(&mut filtered);
    filtered
}

/// Applies a bounded per-series retention limit and preserves deterministic order.
pub fn retain_history_points(mut points: Vec<HistoryPoint>, max_points_per_series: usize) -> Vec<HistoryPoint> {
    if max_points_per_series == 0 {
        return Vec::new();
    }
    sort_history(&mut points);
    let mut grouped: BTreeMap<(HistoryMetricKind, String), Vec<HistoryPoint>> = BTreeMap::new();
    for point in points {
        grouped
            .entry((point.metric, point.series_identity.clone()))
            .or_default()
            .push(point);
    }
    let mut retained = grouped
        .into_values()
        .flat_map(|series| {
            let skip = series.len().saturating_sub(max_points_per_series);
            series.into_iter().skip(skip)
        })
        .collect::<Vec<_>>();
    sort_history(&mut retained);
    retained
}

/// Applies per-series, global-series, and global-point limits, always evicting oldest data first.
pub fn retain_history_points_bounded(points: Vec<HistoryPoint>, retention: HistoryRetention) -> Vec<HistoryPoint> {
    if retention.max_points_per_series == 0 || retention.max_series == 0 || retention.max_total_points == 0 {
        return Vec::new();
    }
    let per_series = retain_history_points(points, retention.max_points_per_series);
    let mut grouped: BTreeMap<(HistoryMetricKind, String), Vec<HistoryPoint>> = BTreeMap::new();
    for point in per_series {
        grouped
            .entry((point.metric, point.series_identity.clone()))
            .or_default()
            .push(point);
    }

    if grouped.len() > retention.max_series {
        let mut ages = grouped
            .iter()
            .map(|(key, points)| {
                let newest = points.last().map_or(0, |point| point.timestamp_epoch_ms);
                (newest, key.clone())
            })
            .collect::<Vec<_>>();
        ages.sort();
        let evict = ages.len().saturating_sub(retention.max_series);
        let evicted = ages
            .into_iter()
            .take(evict)
            .map(|(_, key)| key)
            .collect::<BTreeSet<_>>();
        grouped.retain(|key, _| !evicted.contains(key));
    }

    let mut retained = grouped.into_values().flatten().collect::<Vec<_>>();
    retained.sort_by(|left, right| {
        left.timestamp_epoch_ms
            .cmp(&right.timestamp_epoch_ms)
            .then(left.metric.cmp(&right.metric))
            .then(left.series_identity.cmp(&right.series_identity))
            .then(left.source_revision.cmp(&right.source_revision))
            .then(left.value.total_cmp(&right.value))
    });
    let evict = retained.len().saturating_sub(retention.max_total_points);
    retained.drain(..evict);
    sort_history(&mut retained);
    retained
}

/// Splits independently keyed series whenever adjacent points exceed `max_gap_ms`.
pub fn split_history_segments(points: &[HistoryPoint], max_gap_ms: u64) -> Vec<HistorySeries> {
    let mut sorted = points.to_vec();
    sort_history(&mut sorted);
    let mut result = Vec::new();
    let mut current: Option<HistorySeries> = None;
    for point in sorted {
        let must_split = current.as_ref().is_some_and(|series| {
            series.metric != point.metric
                || series.series_identity != point.series_identity
                || series.points.last().is_some_and(|previous| {
                    point.timestamp_epoch_ms.saturating_sub(previous.timestamp_epoch_ms) > max_gap_ms
                })
        });
        if must_split {
            if let Some(series) = current.take() {
                result.push(series);
            }
        }
        current
            .get_or_insert_with(|| HistorySeries {
                metric: point.metric,
                series_identity: point.series_identity.clone(),
                points: Vec::new(),
            })
            .points
            .push(point);
    }
    if let Some(series) = current {
        result.push(series);
    }
    result
}

fn sort_history(points: &mut [HistoryPoint]) {
    points.sort_by(|left, right| {
        left.metric
            .cmp(&right.metric)
            .then(left.series_identity.cmp(&right.series_identity))
            .then(left.timestamp_epoch_ms.cmp(&right.timestamp_epoch_ms))
            .then(left.source_revision.cmp(&right.source_revision))
            .then(left.value.total_cmp(&right.value))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point(series: &str, timestamp: u64, value: f64) -> HistoryPoint {
        HistoryPoint {
            metric: HistoryMetricKind::BrokerProduceTps,
            series_identity: series.into(),
            timestamp_epoch_ms: timestamp,
            value,
            source_revision: 3,
        }
    }

    #[test]
    fn retention_is_bounded_per_series_and_keeps_real_zeroes() {
        let retained = retain_history_points(
            vec![
                point("a", 1, 1.0),
                point("a", 2, 0.0),
                point("a", 3, 3.0),
                point("b", 1, 4.0),
            ],
            2,
        );
        assert_eq!(
            retained,
            vec![point("a", 2, 0.0), point("a", 3, 3.0), point("b", 1, 4.0)]
        );
    }

    #[test]
    fn gap_split_never_inserts_or_connects_missing_intervals() {
        let segments = split_history_segments(&[point("a", 0, 1.0), point("a", 60, 2.0), point("a", 500, 3.0)], 120);
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].points.len(), 2);
        assert_eq!(segments[1].points, vec![point("a", 500, 3.0)]);
    }

    #[test]
    fn empty_history_remains_empty() {
        assert!(split_history_segments(&[], 60).is_empty());
        assert!(filter_history_range(&[], 0, u64::MAX).is_empty());
        assert!(retain_history_points(vec![point("a", 1, 1.0)], 0).is_empty());
    }

    #[test]
    fn global_retention_evicts_oldest_series_and_points_deterministically() {
        let retained = retain_history_points_bounded(
            vec![
                point("old-series", 1, 1.0),
                point("old-series", 2, 2.0),
                point("middle-series", 3, 3.0),
                point("new-series", 4, 4.0),
                point("new-series", 5, 5.0),
            ],
            HistoryRetention {
                max_points_per_series: 2,
                max_series: 2,
                max_total_points: 2,
            },
        );
        assert_eq!(retained, vec![point("new-series", 4, 4.0), point("new-series", 5, 5.0)]);
    }
}
