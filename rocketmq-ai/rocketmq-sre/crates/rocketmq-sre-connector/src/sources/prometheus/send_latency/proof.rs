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

use super::*;
use rocketmq_sre_core::diagnostics::packs::{
    LATENCY_GRID, LatencyBucket, LatencyCounterSample, LatencyHistogram, LatencyWindowIssue,
};

type Labels = BTreeMap<String, String>;
type Increases = BTreeMap<Labels, f64>;

pub(super) struct Budget {
    pub(super) bytes_remaining: usize,
    pub(super) bytes_read: usize,
    pub(super) requests: u8,
    rows_remaining: usize,
    pub(super) rows_read: usize,
}
impl Budget {
    pub(super) fn series_limit(&self) -> usize {
        // A sentinel row distinguishes complete responses from server-side
        // truncation at our remaining local work bound.
        self.rows_remaining.saturating_add(1)
    }
    pub(super) fn new(rows: usize, bytes: usize) -> Self {
        Self {
            bytes_remaining: bytes,
            bytes_read: 0,
            requests: 0,
            rows_remaining: rows,
            rows_read: 0,
        }
    }
    fn consume(&mut self, count: usize) -> Result<(), ReadIssue> {
        self.rows_remaining = self.rows_remaining.checked_sub(count).ok_or(ReadIssue::BoundExceeded)?;
        self.rows_read += count;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ReadIssue {
    Scope,
    AmbiguousTopic,
    FutureWindow,
    UnsupportedWindow,
    MissingHistogram,
    InvalidResponse,
    ResponseAnnotations,
    DuplicateSeries,
    InvalidNumber,
    BoundExceeded,
    AliasConflict,
    IncoherentReads,
    IncompleteIdentitySet,
    Histogram(LatencyWindowIssue),
}
impl ReadIssue {
    pub(super) const fn reason(self) -> &'static str {
        match self {
            Self::Scope => "histogram_scope_mismatch",
            Self::AmbiguousTopic => "ambiguous_topic_label",
            Self::FutureWindow => "future_window_unobserved",
            Self::UnsupportedWindow => "unsupported_window_resolution",
            Self::MissingHistogram => "missing_histogram",
            Self::InvalidResponse => "invalid_histogram_distribution",
            Self::ResponseAnnotations => "source_response_incomplete",
            Self::DuplicateSeries => "ambiguous_metric_population",
            Self::InvalidNumber => "invalid_histogram_distribution",
            Self::BoundExceeded => "source_response_bounded",
            Self::AliasConflict => "ambiguous_metric_population",
            Self::IncoherentReads => "source_window_changed",
            Self::IncompleteIdentitySet => "invalid_histogram_distribution",
            Self::Histogram(issue) => issue.reason_code(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct RawSet(BTreeMap<(String, Labels), Vec<LatencyCounterSample>>);
impl RawSet {
    pub(super) fn parse(
        value: &Value,
        cluster_label: &str,
        cluster: &str,
        topic: &str,
        budget: &mut Budget,
    ) -> Result<Self, ReadIssue> {
        let mut result = BTreeMap::new();
        for row in response_rows(value, "matrix")? {
            budget.consume(1)?;
            let mut labels = parse_labels(&row["metric"], cluster_label, cluster, topic)?;
            let family = labels.remove("__name__").ok_or(ReadIssue::InvalidResponse)?;
            if !LATENCY_FAMILIES.contains(&family.as_str()) {
                return Err(ReadIssue::InvalidResponse);
            }
            let raw = row["values"].as_array().ok_or(ReadIssue::InvalidResponse)?;
            budget.consume(raw.len())?;
            let samples = raw
                .iter()
                .map(|sample| {
                    let sample = pair(sample)?;
                    Ok(LatencyCounterSample {
                        at_millis: timestamp_millis(&sample[0])?,
                        count: counter(&sample[1])?,
                    })
                })
                .collect::<Result<Vec<_>, ReadIssue>>()?;
            if result.insert((family, labels), samples).is_some() {
                return Err(ReadIssue::DuplicateSeries);
            }
        }
        Ok(Self(result))
    }

    pub(super) fn selected_family(&self) -> Option<&'static str> {
        LATENCY_FAMILIES
            .into_iter()
            .find(|family| self.0.keys().any(|(name, _)| name == family))
    }
}

pub(super) fn parse_increases(
    value: &Value,
    cluster_label: &str,
    cluster: &str,
    topic: &str,
    end: i64,
    budget: &mut Budget,
) -> Result<Increases, ReadIssue> {
    let mut result = BTreeMap::new();
    for row in response_rows(value, "vector")? {
        budget.consume(1)?;
        let mut labels = parse_labels(&row["metric"], cluster_label, cluster, topic)?;
        // Prometheus increase removes the metric name. Some compatible servers
        // retain it; only one of the fixed aliases is accepted in that case.
        if labels
            .remove("__name__")
            .is_some_and(|name| !LATENCY_FAMILIES.contains(&name.as_str()))
        {
            return Err(ReadIssue::InvalidResponse);
        }
        let sample = pair(&row["value"])?;
        if timestamp_millis(&sample[0])? != end {
            return Err(ReadIssue::IncoherentReads);
        }
        if result.insert(labels, numeric(&sample[1])?).is_some() {
            return Err(ReadIssue::DuplicateSeries);
        }
    }
    Ok(result)
}

pub(super) fn select_histograms(
    raw: &RawSet,
    family: &str,
    mut increases: Increases,
) -> Result<Vec<LatencyHistogram>, ReadIssue> {
    let selected: BTreeSet<_> = raw
        .0
        .keys()
        .filter(|(name, _)| name == family)
        .map(|(_, labels)| labels.clone())
        .collect();
    if selected != increases.keys().cloned().collect() {
        return Err(ReadIssue::IncompleteIdentitySet);
    }
    let selected_nodes: BTreeSet<_> = selected.iter().filter_map(|labels| labels.get("node_id")).collect();
    if raw
        .0
        .keys()
        .any(|(_, labels)| !selected_nodes.contains(&labels["node_id"]))
    {
        return Err(ReadIssue::AliasConflict);
    }
    let mut histograms: BTreeMap<String, (Labels, BTreeMap<usize, LatencyBucket>)> = BTreeMap::new();
    for ((name, labels), samples) in &raw.0 {
        if name != family {
            continue;
        }
        let mut identity = labels.clone();
        let bound = identity.remove("le").ok_or(ReadIssue::InvalidResponse)?;
        let upper = if bound == "+Inf" {
            None
        } else {
            Some(bound.parse::<f64>().map_err(|_| ReadIssue::InvalidNumber)?)
        };
        let position = LATENCY_GRID
            .iter()
            .position(|expected| *expected == upper)
            .ok_or(ReadIssue::Histogram(LatencyWindowIssue::UnrecognizedGrid))?;
        let node = identity.get("node_id").ok_or(ReadIssue::Scope)?.clone();
        let (original_identity, buckets) = histograms
            .entry(node)
            .or_insert_with(|| (identity.clone(), BTreeMap::new()));
        if original_identity != &identity {
            return Err(ReadIssue::DuplicateSeries);
        }
        let bucket = LatencyBucket {
            upper_bound_millis: upper,
            samples: samples.clone(),
            increase: increases.remove(labels).ok_or(ReadIssue::IncompleteIdentitySet)?,
        };
        if buckets.insert(position, bucket).is_some() {
            return Err(ReadIssue::DuplicateSeries);
        }
    }
    Ok(histograms
        .into_iter()
        .map(|(node_id, (_, buckets))| LatencyHistogram {
            node_id,
            buckets: buckets.into_values().collect(),
        })
        .collect())
}

fn response_rows<'a>(value: &'a Value, kind: &str) -> Result<&'a Vec<Value>, ReadIssue> {
    if value["status"].as_str() != Some("success") || value["data"]["resultType"].as_str() != Some(kind) {
        return Err(ReadIssue::InvalidResponse);
    }
    for field in ["warnings", "infos"] {
        if value
            .get(field)
            .is_some_and(|value| value.as_array().is_none_or(|rows| !rows.is_empty()))
        {
            return Err(ReadIssue::ResponseAnnotations);
        }
    }
    value["data"]["result"].as_array().ok_or(ReadIssue::InvalidResponse)
}
fn parse_labels(value: &Value, cluster_label: &str, cluster: &str, topic: &str) -> Result<Labels, ReadIssue> {
    let values = value
        .as_object()
        .filter(|labels| labels.len() <= 32)
        .ok_or(ReadIssue::InvalidResponse)?;
    let mut labels = Labels::new();
    for (key, value) in values {
        let value = value
            .as_str()
            .filter(|value| value.len() <= 512)
            .ok_or(ReadIssue::InvalidResponse)?;
        if key.len() > 128 {
            return Err(ReadIssue::InvalidResponse);
        }
        labels.insert(key.clone(), value.to_owned());
    }
    if labels.get(cluster_label).map(String::as_str) != Some(cluster)
        || labels.get("topic").map(String::as_str) != Some(topic)
        || labels
            .get("node_id")
            .is_none_or(|node| node == "other" || validate_identifier(node, "node").is_err())
    {
        return Err(ReadIssue::Scope);
    }
    Ok(labels)
}
fn pair(value: &Value) -> Result<&Vec<Value>, ReadIssue> {
    value
        .as_array()
        .filter(|values| values.len() == 2)
        .ok_or(ReadIssue::InvalidResponse)
}
fn numeric(value: &Value) -> Result<f64, ReadIssue> {
    value
        .as_str()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0)
        .ok_or(ReadIssue::InvalidNumber)
}

fn counter(value: &Value) -> Result<f64, ReadIssue> {
    let count =
        scaled_decimal(value.as_str().ok_or(ReadIssue::InvalidNumber)?, 0).map_err(|_| ReadIssue::InvalidNumber)?;
    if !(0..=9_007_199_254_740_991).contains(&count) {
        return Err(ReadIssue::InvalidNumber);
    }
    Ok(count as f64)
}

/// Prometheus timestamps describe millisecond TSDB points. Parse the decimal
/// representation exactly rather than rounding a floating Unix-second value.
pub(super) fn timestamp_millis(value: &Value) -> Result<i64, ReadIssue> {
    if !value.is_number() {
        return Err(ReadIssue::InvalidResponse);
    }
    i64::try_from(scaled_decimal(&value.to_string(), 3)?).map_err(|_| ReadIssue::InvalidResponse)
}

fn scaled_decimal(text: &str, decimal_places: i32) -> Result<i128, ReadIssue> {
    let (mantissa, exponent) = text.split_once(['e', 'E']).map_or((text, 0), |(mantissa, exponent)| {
        (mantissa, exponent.parse::<i32>().unwrap_or(i32::MAX))
    });
    let negative = mantissa.starts_with('-');
    let mantissa = mantissa.strip_prefix('-').unwrap_or(mantissa);
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{whole}{fraction}");
    let mut scaled = digits.parse::<i128>().map_err(|_| ReadIssue::InvalidResponse)?;
    let scale = exponent
        .checked_add(decimal_places)
        .and_then(|scale| scale.checked_sub(fraction.len() as i32))
        .ok_or(ReadIssue::InvalidResponse)?;
    let power = 10_i128
        .checked_pow(scale.unsigned_abs())
        .ok_or(ReadIssue::InvalidResponse)?;
    if scale >= 0 {
        scaled = scaled.checked_mul(power).ok_or(ReadIssue::InvalidResponse)?;
    } else {
        if scaled % power != 0 {
            return Err(ReadIssue::Histogram(LatencyWindowIssue::InvalidSampleTime));
        }
        scaled /= power;
    }
    if negative {
        scaled = -scaled;
    }
    Ok(scaled)
}
