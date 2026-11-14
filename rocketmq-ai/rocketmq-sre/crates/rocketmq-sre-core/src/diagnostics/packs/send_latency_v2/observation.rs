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

use super::estimate::*;
use rocketmq_sre_contracts::{ContractJsonValue as Value, EvidenceContent, EvidenceSnapshot, TimeRange};

/// Fixed compatibility aliases, in whole-family selection order.
pub const LATENCY_FAMILIES: [&str; 4] = [
    "rocketmq_send_message_latency_bucket",
    "rocketmq_rocketmq_send_message_latency_bucket",
    "rocketmq_send_message_latency_milliseconds_bucket",
    "rocketmq_rocketmq_send_message_latency_milliseconds_bucket",
];

/// A predicate over this observed estimate, not a Broker-enforced SLO.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LatencyPolicyState {
    NotConfigured,
    Exceeded,
    WithinObservedPolicy,
    NotQualified,
}

/// Independently recomputed result; serialized availability/predicate flags are ignored.
#[derive(Clone, Debug, PartialEq)]
pub struct SendLatencyObservation {
    pub window: LatencyWindowAssessment,
    pub policy: LatencyPolicyState,
    pub threshold_millis: Option<u64>,
    pub source_age_seconds: u64,
}

/// Closed validation errors for the version-owned histogram contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LatencyObservationIssue {
    Schema,
    Scope,
    Window,
    Future,
    Stale,
    Policy,
    Histogram(LatencyWindowIssue),
}
impl LatencyObservationIssue {
    /// Stable reason without raw source error or configuration text.
    pub const fn reason_code(self) -> &'static str {
        match self {
            Self::Schema => "invalid_latency_observation_contract",
            Self::Scope => "latency_observation_scope_mismatch",
            Self::Window => "invalid_histogram_sample_time",
            Self::Future => "future_window_unobserved",
            Self::Stale => "historical_data_stale",
            Self::Policy => "policy_scope_mismatch",
            Self::Histogram(issue) => issue.reason_code(),
        }
    }
}

/// Revalidates immutable histogram proof and actual data age for a selected topic.
///
/// # Errors
/// Rejects malformed/mismatched scope, precision/window/clock claims, altered
/// content, stale used data and invalid histogram or operator-policy contracts.
pub fn assess_send_latency_observation(
    snapshot: &EvidenceSnapshot,
    as_of_unix_ms: i64,
) -> Result<SendLatencyObservation, LatencyObservationIssue> {
    use LatencyObservationIssue as Issue;
    if snapshot.verify_content_hash().is_err() {
        return Err(Issue::Schema);
    }
    let EvidenceContent::Inline(content) = &snapshot.content else {
        return Err(Issue::Schema);
    };
    if content["schema_version"].as_str() != Some("rocketmq.sre-diagnostic-source.v1")
        || content["profile"].as_str() != Some("send_latency")
        || content["observation_schema"].as_str() != Some("rocketmq.send-latency-window.v1")
        || content["unit"].as_str() != Some("milliseconds")
        || snapshot.source != "prometheus"
        || content["measurement_kind"].as_str() != Some("classic_histogram_window_estimate")
    {
        return Err(Issue::Schema);
    }
    let topic = snapshot
        .resource
        .strip_prefix("send-latency/")
        .filter(|topic| !topic.is_empty() && !topic.contains('/') && *topic != "other")
        .ok_or(Issue::Scope)?;
    let scope = &content["scope"];
    if scope["tenant_id"].as_str() != Some(snapshot.tenant_id.to_string().as_str())
        || scope["cluster_id"].as_str() != Some(snapshot.cluster_id.to_string().as_str())
        || scope["canonical_resource"].as_str() != Some(snapshot.resource.as_str())
        || scope["topic"].as_str() != Some(topic)
    {
        return Err(Issue::Scope);
    }
    if !LATENCY_FAMILIES.contains(&content["metric_family"].as_str().ok_or(Issue::Schema)?) {
        return Err(Issue::Schema);
    }
    let method = &content["method"];
    if method["name"].as_str() != Some("classic_linear_p99_strict.v1")
        || method["counter_method"].as_str() != Some("prometheus_increase_per_series")
        || method["grid_ref"].as_str() != Some("otel_default_explicit_ms.v1")
        || method["quantile"].as_f64() != Some(0.99)
        || method["minimum_scrape_samples"].as_u64() != Some(2)
        || method["minimum_observed_increments"].as_u64() != Some(100)
        || method["minimum_span_ratio"].as_f64() != Some(0.8)
        || method["maximum_gap_millis"].as_u64() != Some(60_000)
    {
        return Err(Issue::Schema);
    }
    let window = &content["window"];
    let requested = parse_range(&window["requested_start"], &window["requested_end"])?;
    if requested.start.timestamp_micros() != snapshot.time_range.start.timestamp_micros()
        || requested.end.timestamp_micros() != snapshot.time_range.end.timestamp_micros()
        || window["interval"].as_str() != Some("left_open_right_closed")
        || window["time_resolution_millis"].as_u64() != Some(1)
    {
        return Err(Issue::Window);
    }
    let grid = parse_range(&window["query_start"], &window["query_end"])?;
    let start = requested.start.timestamp_millis();
    let end = requested.end.timestamp_millis();
    if grid.start.timestamp_millis() != start
        || grid.end.timestamp_millis() != end
        || grid.start.timestamp_subsec_nanos() % 1_000_000 != 0
        || grid.end.timestamp_subsec_nanos() % 1_000_000 != 0
        || end <= start
    {
        return Err(Issue::Window);
    }
    let collection = parse_range(&content["collection_started_at"], &content["collected_at"])?;
    if collection.end.timestamp_micros() != snapshot.observed_at.timestamp_micros()
        || collection.end.timestamp_millis() > as_of_unix_ms
        || requested.end > collection.start
    {
        return Err(Issue::Future);
    }
    let histograms = parse_histograms(&content["raw_histograms"])?;
    let assessed = assess_latency_histograms(start, end, &histograms).map_err(Issue::Histogram)?;
    let data_span = parse_range(&window["data_first_at"], &window["data_last_at"])?;
    if data_span.start.timestamp_millis() != assessed.data_first_millis
        || data_span.end.timestamp_millis() != assessed.data_last_millis
        || data_span.start.timestamp_subsec_nanos() % 1_000_000 != 0
        || data_span.end.timestamp_subsec_nanos() % 1_000_000 != 0
        || parse_range(&window["oldest_last_sample_at"], &window["oldest_last_sample_at"])?
            .start
            .timestamp_millis()
            != assessed.oldest_last_millis
    {
        return Err(Issue::Window);
    }
    let complete_ns =
        i128::from(collection.end.timestamp()) * 1_000_000_000 + i128::from(collection.end.timestamp_subsec_nanos());
    let age_ns = complete_ns
        .checked_sub(i128::from(assessed.oldest_last_millis) * 1_000_000)
        .filter(|age| *age >= 0)
        .ok_or(Issue::Future)?;
    let source_age = u64::try_from((age_ns + 999_999_999) / 1_000_000_000).map_err(|_| Issue::Window)?;
    if snapshot.freshness_seconds < source_age || content["source_age_seconds"].as_u64() != Some(source_age) {
        return Err(Issue::Window);
    }
    let elapsed = as_of_unix_ms
        .checked_sub(snapshot.observed_at.timestamp_millis())
        .filter(|elapsed| *elapsed >= 0)
        .ok_or(Issue::Future)?;
    let age = snapshot
        .freshness_seconds
        .checked_mul(1000)
        .and_then(|age| age.checked_add(elapsed as u64));
    if age.is_none_or(|age| age > 300_000) {
        return Err(Issue::Stale);
    }
    let threshold = policy_threshold(&content["policy"], snapshot, topic)?;
    let policy = match threshold {
        None => LatencyPolicyState::NotConfigured,
        Some(_) if assessed.state != LatencyWindowState::Available => LatencyPolicyState::NotQualified,
        Some(threshold)
            if assessed
                .quantile
                .is_some_and(|quantile| quantile.estimate_millis > threshold as f64) =>
        {
            LatencyPolicyState::Exceeded
        }
        Some(_) => LatencyPolicyState::WithinObservedPolicy,
    };
    Ok(SendLatencyObservation {
        window: assessed,
        policy,
        threshold_millis: threshold,
        source_age_seconds: source_age,
    })
}

fn parse_range(start: &Value, end: &Value) -> Result<TimeRange, LatencyObservationIssue> {
    let parse = || TimeRange::new(start.as_str()?.parse().ok()?, end.as_str()?.parse().ok()?).ok();
    parse().ok_or(LatencyObservationIssue::Window)
}

fn parse_histograms(raw: &Value) -> Result<Vec<LatencyHistogram>, LatencyObservationIssue> {
    use LatencyObservationIssue as Issue;
    let rows = raw.as_array().filter(|rows| rows.len() <= 64).ok_or(Issue::Schema)?;
    let mut points = 0_usize;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let buckets = row["buckets"]
            .as_array()
            .filter(|rows| rows.len() == LATENCY_GRID.len())
            .ok_or(Issue::Histogram(LatencyWindowIssue::UnrecognizedGrid))?;
        let mut parsed = Vec::with_capacity(buckets.len());
        for bucket in buckets {
            let upper = if bucket["upper_bound_millis"].as_str() == Some("+Inf") {
                None
            } else {
                Some(
                    bucket["upper_bound_millis"]
                        .as_f64()
                        .filter(|value| value.is_finite())
                        .ok_or(Issue::Schema)?,
                )
            };
            let samples = bucket["samples"].as_array().ok_or(Issue::Schema)?;
            points = points
                .checked_add(samples.len())
                .filter(|points| *points <= 10000)
                .ok_or(Issue::Histogram(LatencyWindowIssue::BoundExceeded))?;
            let samples = samples
                .iter()
                .map(|sample| {
                    let values = sample
                        .as_array()
                        .filter(|values| values.len() == 2)
                        .ok_or(Issue::Schema)?;
                    Ok(LatencyCounterSample {
                        at_millis: values[0].as_i64().ok_or(Issue::Schema)?,
                        count: values[1].as_f64().ok_or(Issue::Schema)?,
                    })
                })
                .collect::<Result<Vec<_>, Issue>>()?;
            parsed.push(LatencyBucket {
                upper_bound_millis: upper,
                samples,
                increase: bucket["increase"].as_f64().ok_or(Issue::Schema)?,
            });
        }
        result.push(LatencyHistogram {
            node_id: row["node_id"].as_str().ok_or(Issue::Scope)?.to_owned(),
            buckets: parsed,
        });
    }
    Ok(result)
}

fn policy_threshold(
    policy: &Value,
    snapshot: &EvidenceSnapshot,
    topic: &str,
) -> Result<Option<u64>, LatencyObservationIssue> {
    use LatencyObservationIssue as Issue;
    if policy["source"].is_null() && policy["p99_millis"].is_null() {
        return Ok(None);
    }
    if policy["source"].as_str() != Some("operator_configuration")
        || policy["unit"].as_str() != Some("milliseconds")
        || policy["comparison"].as_str() != Some("strictly_greater")
        || !policy["configuration_generation"].is_null()
        || policy["scope"]["cluster_id"].as_str() != Some(snapshot.cluster_id.to_string().as_str())
        || policy["scope"]["topic"].as_str() != Some(topic)
        || policy["scope"]["canonical_resource"].as_str() != Some(snapshot.resource.as_str())
    {
        return Err(Issue::Policy);
    }
    policy["p99_millis"]
        .as_u64()
        .filter(|value| (1..=3_600_000).contains(value))
        .map(Some)
        .ok_or(Issue::Policy)
}
