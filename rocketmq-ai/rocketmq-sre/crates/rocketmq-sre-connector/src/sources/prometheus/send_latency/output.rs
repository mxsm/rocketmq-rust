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
    LatencyHistogram, LatencyResolution, LatencyWindowAssessment, LatencyWindowState,
};

pub(super) struct WindowProof {
    pub(super) family: &'static str,
    pub(super) histograms: Vec<LatencyHistogram>,
    pub(super) assessment: LatencyWindowAssessment,
}

pub(super) fn project(
    request: &SendLatencyRequest<'_>,
    topic: &str,
    started: DateTime<Utc>,
    completed: DateTime<Utc>,
    budget: &Budget,
    mut result: Result<WindowProof, ReadIssue>,
) -> SourceOutput {
    let age = result
        .as_ref()
        .ok()
        .and_then(|proof| source_age(completed, proof.assessment.oldest_last_millis));
    if result.is_ok() && age.is_none() {
        result = Err(ReadIssue::FutureWindow);
    }
    let policy = request
        .policy
        .filter(|policy| policy.cluster_id == request.query.cluster_id && policy.topic == topic);
    let (availability, reason) = match &result {
        Ok(_) if age.is_some_and(|age| age > 300) => ("stale", "historical_data_stale"),
        Ok(proof) => match proof.assessment.state {
            LatencyWindowState::Available => ("available", "broker_histogram_window_available"),
            LatencyWindowState::NoRecordedTraffic => ("no_traffic", "no_recorded_traffic"),
            LatencyWindowState::InsufficientPopulation => ("insufficient_data", "insufficient_observed_population"),
            LatencyWindowState::TailLimited => ("insufficient_data", "tail_resolution_limited"),
        },
        Err(ReadIssue::MissingHistogram) => ("missing", "missing_histogram"),
        Err(issue) => ("unknown", issue.reason()),
    };
    let qualified = availability == "available";
    let observation = result.as_ref().ok().map(|proof| {
        let assessment = &proof.assessment;
        assessment.quantile.map(|quantile| json!({
            "quantile":"p99", "estimate_millis":quantile.estimate_millis,
            "lower_bound_millis":quantile.lower_bound_millis,
            "upper_bound_millis":quantile.upper_bound_millis,
            "resolution":match quantile.resolution { LatencyResolution::FiniteBucket => "finite_bucket", LatencyResolution::TailLimited => "tail_limited" },
            "observed_increments":assessment.observed_increments,
            "estimated_increments":assessment.estimated_increments,
            "observed_nodes":assessment.observed_nodes,
            "scrape_samples":assessment.scrape_samples,
            "observed_resets":assessment.observed_resets
        })).unwrap_or(Value::Null)
    }).unwrap_or(Value::Null);
    let (policy_status, policy_reason) = match policy {
        None => ("not_configured", "policy_not_configured"),
        Some(_) if !qualified => ("not_qualified", reason),
        Some(policy)
            if result
                .as_ref()
                .ok()
                .and_then(|proof| proof.assessment.quantile)
                .is_some_and(|quantile| quantile.estimate_millis > policy.p99_millis as f64) =>
        {
            ("exceeded", "estimate_exceeds_operator_policy")
        }
        Some(_) => ("within_observed_policy", "estimate_within_operator_policy"),
    };
    let millis_at = |millis| {
        DateTime::<Utc>::from_timestamp_millis(millis).map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
    };
    let assessment = result.as_ref().ok().map(|proof| &proof.assessment);
    let mut segments: Vec<_> = [
        ("client", "client_send_attempts"),
        ("proxy", "proxy_grpc_requests"),
        ("remoting", "transport_rpc_requests"),
        ("store", "store_flush_dispatch_operations"),
    ]
    .into_iter()
    .map(|(id, population)| {
        json!({
            "id":id,"availability":"unsupported_scope","reason_code":"exact_topic_scope_unavailable",
            "population":population,"observation":null
        })
    })
    .collect();
    segments.push(json!({
        "id":"broker","availability":availability,"reason_code":reason,
        "population":"recorded_broker_store_submit_result_attempts","observation":observation
    }));
    let raw_histograms: Vec<_> = result
        .as_ref()
        .ok()
        .map(|proof| {
            proof.histograms.iter().map(|histogram| json!({
        "node_id":histogram.node_id,
        "buckets":histogram.buckets.iter().map(|bucket| json!({
            "upper_bound_millis":bucket.upper_bound_millis.map_or(json!("+Inf"), |bound| json!(bound)),
            "samples":bucket.samples.iter().map(|sample|json!([sample.at_millis,sample.count])).collect::<Vec<_>>(),
            "increase":bucket.increase
        })).collect::<Vec<_>>()
    })).collect()
        })
        .unwrap_or_default();
    let content = json!({
        "schema_version":"rocketmq.sre-diagnostic-source.v1",
        "profile":"send_latency", "observation_schema":"rocketmq.send-latency-window.v1",
        "measurement_kind":"classic_histogram_window_estimate", "unit":"milliseconds",
        "scope":{
            "tenant_id":request.query.tenant_id,"cluster_id":request.query.cluster_id,
            "canonical_resource":request.query.resource,"topic":topic
        },
        "window":{
            "requested_start":request.query.time_range.start,"requested_end":request.query.time_range.end,
            "query_start":millis_at(request.query.time_range.start.timestamp_millis()),
            "query_end":millis_at(request.query.time_range.end.timestamp_millis()),
            "interval":"left_open_right_closed","time_resolution_millis":1,
            "data_first_at":assessment.and_then(|value|millis_at(value.data_first_millis)),
            "data_last_at":assessment.and_then(|value|millis_at(value.data_last_millis)),
            "oldest_last_sample_at":assessment.and_then(|value|millis_at(value.oldest_last_millis))
        },
        "collection_started_at":started,"collected_at":completed,"source_age_seconds":age,
        "metric_family":result.as_ref().ok().map(|proof|proof.family),
        "method":{
            "name":"classic_linear_p99_strict.v1","counter_method":"prometheus_increase_per_series",
            "grid_ref":"otel_default_explicit_ms.v1","quantile":0.99,
            "minimum_scrape_samples":2,"minimum_observed_increments":100,
            "minimum_span_ratio":0.8,"maximum_gap_millis":60000
        },
        "segments":segments,
        "policy":{
            "status":policy_status,"reason_code":policy_reason,
            "source":policy.map(|_|"operator_configuration"),
            "scope":policy.map(|_|json!({"cluster_id":request.query.cluster_id,"topic":topic,"canonical_resource":request.query.resource})),
            "unit":"milliseconds","comparison":"strictly_greater","p99_millis":policy.map(|policy|policy.p99_millis),
            "configuration_generation":null,"generation_reason":"operator_policy_without_runtime_generation"
        },
        "missing_dimensions":["client_exact_topic_latency","proxy_exact_topic_latency","remoting_exact_topic_latency","store_exact_topic_latency","correlated_segment_population"],
        "partial":true,"raw_histograms":raw_histograms,
        "http_statistics":{"physical_requests":budget.requests,"response_bytes":budget.bytes_read,"work_rows":budget.rows_read}
    });
    let mut output = SourceOutput::available(content, completed).with_exposure(EvidenceExposure::PrometheusApi);
    // Capture time and data age are separate. Reloading a historical window
    // cannot renew its last actual scrape by assigning it the HTTP finish time.
    output.freshness_seconds = age.unwrap_or(0);
    output.partial = true;
    output.coverage = if availability == "missing" {
        CoverageStatus::Missing
    } else {
        CoverageStatus::Partial
    };
    output.warnings = vec!["send_latency_uncorrelated_segments".to_owned()];
    if !qualified {
        output.warnings.push(reason.to_owned());
    }
    output
}

fn source_age(completed: DateTime<Utc>, oldest_millis: i64) -> Option<u64> {
    let completed_ns =
        i128::from(completed.timestamp()) * 1_000_000_000 + i128::from(completed.timestamp_subsec_nanos());
    let age = completed_ns
        .checked_sub(i128::from(oldest_millis) * 1_000_000)?
        .checked_add(999_999_999)?;
    if age < 999_999_999 {
        return None;
    }
    u64::try_from(age / 1_000_000_000).ok()
}
