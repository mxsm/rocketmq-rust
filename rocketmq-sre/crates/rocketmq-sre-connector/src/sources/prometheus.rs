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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use reqwest::Client;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceExposure;
use serde_json::Value;
use serde_json::json;
use url::Url;

use super::common::CancelSignal;
use super::common::SourceOutput;
use super::common::bounded_future;
use super::common::bounded_response;
use super::common::parse_json;
use super::common::require_label;
use super::common::validate_identifier;
use crate::ConnectorError;
use crate::ConnectorErrorCode;

const PROMETHEUS_EVIDENCE_SCHEMA: &str = "rocketmq.prometheus-evidence.v1";
const TELEMETRY_CLUSTER_LABEL: &str = "rocketmq_cluster";
const SEVEN_DAYS: Duration = Duration::days(7);
const THIRTY_DAYS: Duration = Duration::days(30);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrometheusQueryKind {
    Instant,
    Range,
    Trend7d,
    Trend30d,
}

impl PrometheusQueryKind {
    const fn wire_name(self) -> &'static str {
        match self {
            Self::Instant => "instant",
            Self::Range => "range",
            Self::Trend7d => "trend_7d",
            Self::Trend30d => "trend_30d",
        }
    }

    const fn endpoint(self) -> &'static str {
        match self {
            Self::Instant => "api/v1/query",
            Self::Range | Self::Trend7d | Self::Trend30d => "api/v1/query_range",
        }
    }

    fn effective_range(
        self,
        requested_start: DateTime<Utc>,
        requested_end: DateTime<Utc>,
    ) -> (DateTime<Utc>, DateTime<Utc>) {
        match self {
            Self::Trend7d => (requested_end - SEVEN_DAYS, requested_end),
            Self::Trend30d => (requested_end - THIRTY_DAYS, requested_end),
            Self::Instant | Self::Range => (requested_start, requested_end),
        }
    }
}

pub(crate) struct PrometheusSource {
    client: Client,
    base_url: Option<Url>,
    label_allowlist: BTreeSet<String>,
    max_time_range: std::time::Duration,
}

impl PrometheusSource {
    pub(crate) fn new(
        client: Client,
        base_url: Option<Url>,
        label_allowlist: BTreeSet<String>,
        max_time_range: std::time::Duration,
    ) -> Self {
        Self {
            client,
            base_url,
            label_allowlist,
            max_time_range,
        }
    }

    pub(crate) fn configured(&self) -> bool {
        self.base_url.is_some()
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "Prometheus query bounds remain explicit at the source boundary"
    )]
    pub(crate) async fn query(
        &self,
        cluster: &str,
        resource: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        max_rows: usize,
        max_bytes: usize,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError> {
        self.query_with_matchers(
            cluster,
            resource,
            &[],
            start,
            end,
            max_rows,
            max_bytes,
            deadline,
            cancel,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "Prometheus query bounds and fixed label matchers remain explicit"
    )]
    pub(crate) async fn query_with_matchers(
        &self,
        cluster: &str,
        resource: &str,
        matchers: &[(String, String)],
        requested_start: DateTime<Utc>,
        requested_end: DateTime<Utc>,
        max_rows: usize,
        max_bytes: usize,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError> {
        let base_url = self
            .base_url
            .as_ref()
            .ok_or_else(|| ConnectorError::source("Prometheus source is not configured"))?;
        require_label(&self.label_allowlist, TELEMETRY_CLUSTER_LABEL)?;
        let (kind, metric) = parse_resource(resource)?;
        validate_metric(metric)?;
        validate_identifier(cluster, "cluster")?;
        let mut selector = vec![format!(r#"{TELEMETRY_CLUSTER_LABEL}="{cluster}""#)];
        for (label, value) in matchers {
            validate_label(label)?;
            require_label(&self.label_allowlist, label)?;
            validate_identifier(value, "Prometheus label value")?;
            if label == TELEMETRY_CLUSTER_LABEL {
                return Err(ConnectorError::new(
                    ConnectorErrorCode::InvalidEvidenceQuery,
                    false,
                    "canonical Prometheus query cannot override the cluster matcher",
                ));
            }
            selector.push(format!(r#"{label}="{value}""#));
        }
        let endpoint = base_url
            .join(kind.endpoint())
            .map_err(|_| ConnectorError::configuration("Prometheus query URL cannot be constructed"))?;
        let expression = format!("{metric}{{{}}}", selector.join(","));
        let (start, end) = kind.effective_range(requested_start, requested_end);
        if end
            .signed_duration_since(start)
            .to_std()
            .map_or(true, |range| range > self.max_time_range)
        {
            return Err(ConnectorError::new(
                ConnectorErrorCode::InvalidEvidenceQuery,
                false,
                "Prometheus effective query range exceeds the configured source bound",
            ));
        }
        let mut request = self.client.get(endpoint).query(&[("query", expression)]);
        if kind == PrometheusQueryKind::Instant {
            request = request.query(&[("time", end.timestamp().to_string())]);
        } else {
            request = request.query(&[
                ("start", start.timestamp().to_string()),
                ("end", end.timestamp().to_string()),
                ("step", query_step(start, end, max_rows).to_string()),
            ]);
        }
        let response = bounded_future(deadline, cancel, async {
            request
                .send()
                .await
                .map_err(|_| ConnectorError::source("Prometheus query failed"))
        })
        .await?;
        if !response.status().is_success() {
            return Err(ConnectorError::source("Prometheus rejected the bounded query"));
        }
        let body = bounded_response(response, max_bytes, deadline, cancel).await?;
        let value = parse_json(&body)?;
        let projected = project_response(&value, kind, metric, start, end, max_rows, &self.label_allowlist)?;
        let mut output = SourceOutput::available(projected.content, projected.observed_at)
            .with_exposure(EvidenceExposure::PrometheusApi);
        if projected.truncated {
            output.partial = true;
            output.coverage = CoverageStatus::Partial;
            output.warnings.push("prometheus_samples_bounded".to_owned());
        }
        if projected.non_numeric {
            output.partial = true;
            output.coverage = CoverageStatus::Partial;
            output.warnings.push("prometheus_non_numeric_sample_omitted".to_owned());
        }
        Ok(output)
    }
}

struct ProjectedPrometheus {
    content: Value,
    observed_at: DateTime<Utc>,
    truncated: bool,
    non_numeric: bool,
}

#[allow(
    clippy::too_many_arguments,
    reason = "projection carries explicit query metadata and output bounds"
)]
fn project_response(
    raw: &Value,
    kind: PrometheusQueryKind,
    metric: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    max_rows: usize,
    allowed_labels: &BTreeSet<String>,
) -> Result<ProjectedPrometheus, ConnectorError> {
    if raw.get("status").and_then(Value::as_str) != Some("success") {
        return Err(ConnectorError::source("Prometheus returned an unsuccessful response"));
    }
    let data = raw
        .get("data")
        .and_then(Value::as_object)
        .ok_or_else(|| ConnectorError::source("Prometheus response data is invalid"))?;
    let result_type = data
        .get("resultType")
        .and_then(Value::as_str)
        .ok_or_else(|| ConnectorError::source("Prometheus response result type is missing"))?;
    let result = data
        .get("result")
        .and_then(Value::as_array)
        .ok_or_else(|| ConnectorError::source("Prometheus response result is invalid"))?;
    if !matches!(result_type, "matrix" | "vector") {
        return Err(ConnectorError::source(
            "Prometheus response is not a bounded vector or matrix",
        ));
    }

    let mut remaining = max_rows.max(1);
    let mut series = Vec::with_capacity(result.len().min(max_rows));
    let mut truncated = false;
    let mut non_numeric = false;
    let mut latest = None;
    for raw_series in result {
        if remaining == 0 {
            truncated = true;
            break;
        }
        let Some(raw_series) = raw_series.as_object() else {
            return Err(ConnectorError::source("Prometheus series is invalid"));
        };
        let labels = project_labels(raw_series.get("metric"), allowed_labels);
        let raw_samples = match result_type {
            "matrix" => raw_series.get("values").and_then(Value::as_array).map(Vec::as_slice),
            "vector" => raw_series.get("value").map(std::slice::from_ref),
            _ => None,
        }
        .ok_or_else(|| ConnectorError::source("Prometheus series samples are invalid"))?;
        let available = raw_samples.len();
        let mut samples = Vec::with_capacity(available.min(remaining));
        for sample in raw_samples.iter().take(remaining) {
            remaining = remaining.saturating_sub(1);
            match project_sample(sample)? {
                Some((observed_at, value)) => {
                    latest = Some(latest.map_or(observed_at, |current: DateTime<Utc>| current.max(observed_at)));
                    samples.push(json!({
                        "observed_at": observed_at,
                        "value": value
                    }));
                }
                None => non_numeric = true,
            }
        }
        if samples.len() < available {
            truncated = true;
        }
        series.push(json!({
            "labels": labels,
            "samples": samples
        }));
    }
    Ok(ProjectedPrometheus {
        content: json!({
            "schema_version": PROMETHEUS_EVIDENCE_SCHEMA,
            "query_kind": kind.wire_name(),
            "metric": metric,
            "start": start,
            "end": end,
            "series": series
        }),
        observed_at: latest.unwrap_or(end),
        truncated,
        non_numeric,
    })
}

fn project_labels(raw: Option<&Value>, allowed_labels: &BTreeSet<String>) -> BTreeMap<String, String> {
    raw.and_then(Value::as_object)
        .map(|labels| {
            labels
                .iter()
                .filter(|(key, _)| allowed_labels.contains(*key))
                .filter_map(|(key, value)| value.as_str().map(|value| (key.clone(), value.to_owned())))
                .collect()
        })
        .unwrap_or_default()
}

fn project_sample(raw: &Value) -> Result<Option<(DateTime<Utc>, Value)>, ConnectorError> {
    let sample = raw
        .as_array()
        .filter(|sample| sample.len() == 2)
        .ok_or_else(|| ConnectorError::source("Prometheus sample is invalid"))?;
    let timestamp = sample[0]
        .as_f64()
        .ok_or_else(|| ConnectorError::source("Prometheus sample timestamp is invalid"))?;
    let seconds = timestamp.floor() as i64;
    let nanos = ((timestamp - timestamp.floor()) * 1_000_000_000.0).round() as u32;
    let observed_at = DateTime::from_timestamp(seconds, nanos)
        .ok_or_else(|| ConnectorError::source("Prometheus sample timestamp is out of range"))?;
    let Some(raw_value) = sample[1].as_str() else {
        return Err(ConnectorError::source("Prometheus sample value is invalid"));
    };
    let Ok(value) = raw_value.parse::<f64>() else {
        return Ok(None);
    };
    if !value.is_finite() {
        return Ok(None);
    }
    let number = serde_json::Number::from_f64(value)
        .ok_or_else(|| ConnectorError::source("Prometheus sample cannot be represented"))?;
    Ok(Some((observed_at, Value::Number(number))))
}

fn parse_resource(resource: &str) -> Result<(PrometheusQueryKind, &str), ConnectorError> {
    let candidates = [
        ("prometheus/trend/30d/", PrometheusQueryKind::Trend30d),
        ("trend/30d/", PrometheusQueryKind::Trend30d),
        ("prometheus/trend/7d/", PrometheusQueryKind::Trend7d),
        ("trend/7d/", PrometheusQueryKind::Trend7d),
        ("prometheus/instant/", PrometheusQueryKind::Instant),
        ("instant/", PrometheusQueryKind::Instant),
        ("prometheus/range/", PrometheusQueryKind::Range),
        ("range/", PrometheusQueryKind::Range),
        ("prometheus/metrics/", PrometheusQueryKind::Range),
        ("metrics/", PrometheusQueryKind::Range),
        ("prometheus/", PrometheusQueryKind::Range),
    ];
    candidates
        .into_iter()
        .find_map(|(prefix, kind)| resource.strip_prefix(prefix).map(|metric| (kind, metric)))
        .or_else(|| (!resource.contains('/')).then_some((PrometheusQueryKind::Range, resource)))
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorCode::InvalidEvidenceQuery,
                false,
                "Prometheus resource must be an instant, range, 7d trend, or 30d trend metric",
            )
        })
}

fn validate_metric(metric: &str) -> Result<(), ConnectorError> {
    if metric.is_empty()
        || metric.len() > 255
        || !metric
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':' | b'.'))
    {
        return Err(ConnectorError::new(
            ConnectorErrorCode::InvalidEvidenceQuery,
            false,
            "Prometheus metric name is invalid",
        ));
    }
    Ok(())
}

fn validate_label(label: &str) -> Result<(), ConnectorError> {
    if label.is_empty()
        || label.len() > 255
        || !label
            .bytes()
            .enumerate()
            .all(|(index, byte)| byte.is_ascii_alphabetic() || byte == b'_' || (index > 0 && byte.is_ascii_digit()))
    {
        return Err(ConnectorError::new(
            ConnectorErrorCode::InvalidEvidenceQuery,
            false,
            "Prometheus label name is invalid",
        ));
    }
    Ok(())
}

fn query_step(start: DateTime<Utc>, end: DateTime<Utc>, max_rows: usize) -> i64 {
    let seconds = end.signed_duration_since(start).num_seconds().max(1);
    let desired_points = i64::try_from(max_rows.max(1)).unwrap_or(i64::MAX);
    seconds
        .saturating_add(desired_points - 1)
        .saturating_div(desired_points)
        .max(1)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::Json;
    use axum::Router;
    use axum::extract::OriginalUri;
    use axum::extract::Query;
    use axum::extract::State;
    use axum::routing::get;
    use tokio::sync::Mutex;

    use super::*;

    #[test]
    fn metric_query_has_a_fixed_cluster_matcher_and_bounded_step() {
        assert!(validate_metric("rocketmq_broker_up").is_ok());
        assert!(validate_metric("up or vector(1)").is_err());
        assert!(validate_label("node_id").is_ok());
        assert!(validate_label("service.name").is_err());
        let start = Utc::now();
        assert_eq!(query_step(start, start + Duration::seconds(600), 240), 3);
    }

    #[test]
    fn instant_and_trend_resources_are_explicit_and_do_not_accept_promql() {
        let instant = parse_resource("instant/rocketmq_broker_up").expect("instant resource");
        assert_eq!(instant, (PrometheusQueryKind::Instant, "rocketmq_broker_up"));
        let seven_day = parse_resource("trend/7d/rocketmq_consumer_lag").expect("seven-day trend resource");
        assert_eq!(seven_day, (PrometheusQueryKind::Trend7d, "rocketmq_consumer_lag"));
        let thirty_day = parse_resource("prometheus/trend/30d/rocketmq_store_size").expect("thirty-day trend resource");
        assert_eq!(thirty_day, (PrometheusQueryKind::Trend30d, "rocketmq_store_size"));
        assert!(parse_resource("query/up or vector(1)").is_err());
    }

    #[test]
    fn projection_bounds_samples_filters_labels_and_omits_non_finite_values() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).expect("timestamp");
        let end = start + Duration::days(7);
        let raw = json!({
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [{
                    "metric": {
                        "cluster": "local",
                        "node_id": "broker-a",
                        "sli": "delivery_ratio",
                        "dimension": "traffic",
                        "window_pair": "fast",
                        "window_role": "short",
                        "secret": "drop"
                    },
                    "values": [
                        [start.timestamp(), "1"],
                        [start.timestamp() + 1, "NaN"],
                        [start.timestamp() + 2, "2"]
                    ]
                }]
            }
        });
        let projected = project_response(
            &raw,
            PrometheusQueryKind::Trend7d,
            "rocketmq_broker_up",
            start,
            end,
            1,
            &BTreeSet::from([
                "cluster".to_owned(),
                "node_id".to_owned(),
                "sli".to_owned(),
                "dimension".to_owned(),
                "window_pair".to_owned(),
                "window_role".to_owned(),
            ]),
        )
        .expect("projection");

        assert!(projected.truncated);
        assert!(!projected.non_numeric);
        assert_eq!(projected.content["query_kind"], "trend_7d");
        assert_eq!(
            projected.content["series"][0]["samples"].as_array().map(Vec::len),
            Some(1)
        );
        assert_eq!(projected.content["series"][0]["labels"]["sli"], "delivery_ratio");
        assert_eq!(projected.content["series"][0]["labels"]["window_role"], "short");
        assert!(projected.content["series"][0]["labels"].get("secret").is_none());
    }

    #[test]
    fn thirty_day_trend_uses_exact_window() {
        let end = Utc::now();
        let (start, effective_end) = PrometheusQueryKind::Trend30d.effective_range(end - Duration::hours(1), end);
        assert_eq!(effective_end, end);
        assert_eq!(effective_end - start, THIRTY_DAYS);
    }

    #[test]
    fn non_numeric_samples_still_consume_the_bounded_scan_budget() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).expect("timestamp");
        let raw = json!({
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [{
                    "metric": {"cluster": "local"},
                    "values": [
                        [start.timestamp(), "NaN"],
                        [start.timestamp() + 1, "1"]
                    ]
                }]
            }
        });
        let projected = project_response(
            &raw,
            PrometheusQueryKind::Range,
            "rocketmq_broker_up",
            start,
            start + Duration::seconds(1),
            1,
            &BTreeSet::from(["cluster".to_owned()]),
        )
        .expect("projection");

        assert!(projected.truncated);
        assert!(projected.non_numeric);
        assert_eq!(
            projected.content["series"][0]["samples"].as_array().map(Vec::len),
            Some(0)
        );
    }

    #[tokio::test]
    async fn thirty_day_adapter_uses_the_bounded_query_range_api_and_cluster_matcher() {
        type Requests = Arc<Mutex<Vec<(String, BTreeMap<String, String>)>>>;

        async fn fixture(
            State(requests): State<Requests>,
            OriginalUri(uri): OriginalUri,
            Query(parameters): Query<BTreeMap<String, String>>,
        ) -> Json<Value> {
            requests.lock().await.push((uri.path().to_owned(), parameters));
            Json(json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [{
                        "metric": {"rocketmq_cluster": "local"},
                        "values": [[1_700_000_000, "1"]]
                    }]
                }
            }))
        }

        let requests = Requests::default();
        let app = Router::new().fallback(get(fixture)).with_state(requests.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Prometheus fixture");
        let address = listener.local_addr().expect("fixture address");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve Prometheus fixture");
        });
        let source = PrometheusSource::new(
            Client::new(),
            Some(Url::parse(&format!("http://{address}/")).expect("fixture URL")),
            BTreeSet::from([TELEMETRY_CLUSTER_LABEL.to_owned()]),
            std::time::Duration::from_secs(30 * 24 * 60 * 60),
        );
        let end = DateTime::from_timestamp(1_800_000_000, 0).expect("timestamp");
        let output = source
            .query(
                "local",
                "trend/30d/rocketmq_broker_up",
                end - Duration::hours(1),
                end,
                100,
                64 * 1024,
                Utc::now() + Duration::seconds(5),
                &CancelSignal::default(),
            )
            .await
            .expect("bounded trend query");

        let requests = requests.lock().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, "/api/v1/query_range");
        assert_eq!(
            requests[0].1.get("query").map(String::as_str),
            Some("rocketmq_broker_up{rocketmq_cluster=\"local\"}")
        );
        let start = requests[0].1["start"].parse::<i64>().expect("start");
        let end = requests[0].1["end"].parse::<i64>().expect("end");
        assert_eq!(end - start, 30 * 24 * 60 * 60);
        assert_eq!(output.exposure, EvidenceExposure::PrometheusApi);

        server.abort();
        let _ = server.await;
    }
}
