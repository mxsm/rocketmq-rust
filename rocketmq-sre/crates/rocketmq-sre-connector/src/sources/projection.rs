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

use std::collections::BTreeSet;

use rocketmq_sre_contracts::CoverageStatus;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;

use super::canonical::CanonicalProjection;
use super::common::SourceOutput;
use crate::ConnectorError;
use crate::ConnectorErrorCode;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Completeness {
    Available,
    Partial,
    Missing,
}

struct Projected {
    content: Value,
    completeness: Completeness,
    warnings: Vec<&'static str>,
}

/// Converts a source-owned, already validated wire response into the exact
/// field names consumed by Wave A. A projection never supplies neutral
/// defaults for unavailable fields.
pub(super) fn apply(mut output: SourceOutput, projection: CanonicalProjection) -> Result<SourceOutput, ConnectorError> {
    let projected = match projection {
        CanonicalProjection::BrokerRuntime => broker_runtime(&output.content)?,
        CanonicalProjection::ConsumerLag => consumer_lag(&output.content)?,
        CanonicalProjection::TopicRoute => topic_route(&output.content)?,
        CanonicalProjection::ConsumerRuntime => consumer_runtime(&output.content)?,
        CanonicalProjection::ProducerConnectivity => producer_connectivity(&output.content)?,
        CanonicalProjection::ClientConnections => client_connections(&output.content)?,
        CanonicalProjection::BrokerHealth => prometheus_broker_health(&output.content)?,
        CanonicalProjection::ConsumerRuntimeMetrics => prometheus_consumer_runtime(&output.content)?,
        CanonicalProjection::CollectorMetrics => prometheus_collector(&output.content)?,
        CanonicalProjection::KubernetesWorkloads => kubernetes_workloads(&output.content, false)?,
        CanonicalProjection::DeploymentDriftBasis => deployment_drift_basis(&output.content)?,
        CanonicalProjection::CollectorWorkload => kubernetes_workloads(&output.content, true)?,
        CanonicalProjection::RuntimeObservability => runtime_observability(&output.content)?,
        CanonicalProjection::ClusterTopology => cluster_topology(&output.content)?,
    };

    output.content = projected.content;
    output
        .warnings
        .extend(projected.warnings.into_iter().map(str::to_owned));
    match projected.completeness {
        Completeness::Available if !output.partial => {
            output.coverage = CoverageStatus::Available;
        }
        Completeness::Available | Completeness::Partial => {
            output.partial = true;
            output.coverage = CoverageStatus::Partial;
        }
        Completeness::Missing => {
            output.partial = true;
            output.coverage = CoverageStatus::Missing;
        }
    }
    Ok(output)
}

fn broker_runtime(raw: &Value) -> Result<Projected, ConnectorError> {
    let root = object(raw)?;
    required_string(root, "broker_name")?;
    let brokers = required_array(root, "brokers")?;
    if brokers.is_empty() {
        return Ok(missing("broker_runtime_empty"));
    }
    let mut active = 0_u64;
    for broker in brokers {
        let broker = object(broker)?;
        required_string(broker, "broker_name")?;
        if required_bool(broker, "broker_active")? {
            active = active.saturating_add(1);
        }
    }
    Ok(Projected {
        content: json!({
            "broker_up": active > 0,
            "broker_rows": brokers.len(),
            "active_broker_rows": active,
        }),
        completeness: Completeness::Available,
        warnings: Vec::new(),
    })
}

fn consumer_lag(raw: &Value) -> Result<Projected, ConnectorError> {
    let root = object(raw)?;
    required_string(root, "topic")?;
    required_string(root, "consumer_group")?;
    let total_lag = required_i64(root, "total_lag")?;
    let consume_rate = required_f64(root, "consume_tps")?;
    let items = required_array(root, "items")?;
    let total_count = required_u64(root, "total_count")?;
    let has_more = required_bool(root, "has_more")?;
    if items.len() as u64 > total_count || (has_more && items.len() as u64 >= total_count) {
        return Err(schema_mismatch());
    }

    let mut queue_lags = Vec::with_capacity(items.len());
    for item in items {
        queue_lags.push(required_i64(object(item)?, "lag")?);
    }
    let complete_queue_set = !has_more && items.len() as u64 == total_count;
    let queue_skew_ratio = complete_queue_set.then(|| skew_ratio(&queue_lags)).flatten();
    let mut content = Map::from_iter([
        ("total_lag".to_owned(), Value::from(total_lag)),
        ("consume_rate_per_sec".to_owned(), finite_number(consume_rate)?),
        ("observed_queue_count".to_owned(), Value::from(items.len() as u64)),
        ("total_queue_count".to_owned(), Value::from(total_count)),
    ]);
    if let Some(max_queue_lag) = optional_i64(root, "max_queue_lag")? {
        content.insert("max_queue_lag".to_owned(), Value::from(max_queue_lag));
    }
    if let Some(inflight) = optional_i64(root, "inflight_total")? {
        content.insert("inflight_total".to_owned(), Value::from(inflight));
    }
    if let Some(ratio) = queue_skew_ratio {
        content.insert("queue_skew_ratio".to_owned(), finite_number(ratio)?);
    }
    Ok(Projected {
        content: Value::Object(content),
        completeness: Completeness::Partial,
        warnings: vec!["consumer_lag_rate_history_unavailable"],
    })
}

fn topic_route(raw: &Value) -> Result<Projected, ConnectorError> {
    let object = object(raw)?;
    required_string(object, "topic")?;
    let read_queues = required_u64(object, "read_queue_count")?;
    let write_queues = required_u64(object, "write_queue_count")?;
    let brokers = object
        .get("broker_names")
        .or_else(|| object.get("brokers"))
        .and_then(Value::as_array)
        .ok_or_else(schema_mismatch)?;
    Ok(Projected {
        content: json!({
            "route_available": !brokers.is_empty() && read_queues.max(write_queues) > 0,
            "queue_count": read_queues.max(write_queues),
            "read_queue_count": read_queues,
            "write_queue_count": write_queues,
            "broker_count": brokers.len(),
        }),
        completeness: Completeness::Available,
        warnings: Vec::new(),
    })
}

fn consumer_runtime(raw: &Value) -> Result<Projected, ConnectorError> {
    let object = object(raw)?;
    required_string(object, "consumer_group")?;
    let connections = required_array(object, "connections")?;
    let connected_clients = object
        .get("connection_count")
        .and_then(Value::as_u64)
        .unwrap_or(connections.len() as u64);
    let subscription_count = object
        .get("subscriptions")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let queried_brokers = object.get("queried_broker_count").and_then(Value::as_u64);
    let failed_brokers = object
        .get("failed_brokers")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let truncated = object.get("truncated").and_then(Value::as_bool).unwrap_or(false);
    Ok(Projected {
        content: json!({
            "connected": connected_clients > 0,
            "connected_clients": connected_clients,
            "observed_connections": connections.len(),
            "subscription_count": subscription_count,
            "queried_brokers": queried_brokers,
            "unavailable_brokers": failed_brokers,
            "truncated": truncated,
        }),
        completeness: Completeness::Partial,
        warnings: vec!["consumer_runtime_detail_unavailable"],
    })
}

fn producer_connectivity(raw: &Value) -> Result<Projected, ConnectorError> {
    let root = object(raw)?;
    let connections = required_array(root, "connections")?;
    let queried_brokers = required_u64(root, "queried_broker_count")?;
    let failed_brokers = required_array(root, "failed_brokers")?;
    let truncated = required_bool(root, "truncated")?;
    let mut groups = BTreeSet::new();
    let mut brokers = BTreeSet::new();
    for row in connections {
        let row = object(row)?;
        groups.insert(required_string(row, "producer_group")?);
        let connection = object(row.get("connection").ok_or_else(schema_mismatch)?)?;
        brokers.insert(required_string(connection, "broker_name")?);
    }
    Ok(Projected {
        content: json!({
            "connected": !connections.is_empty(),
            "connected_clients": connections.len(),
            "producer_groups": groups.len(),
            "connected_brokers": brokers.len(),
            "queried_brokers": queried_brokers,
            "unavailable_brokers": failed_brokers.len(),
            "truncated": truncated,
        }),
        completeness: Completeness::Partial,
        warnings: vec![
            "producer_route_age_unavailable",
            "producer_send_error_rate_unavailable",
            "producer_queue_selection_unavailable",
        ],
    })
}

fn client_connections(raw: &Value) -> Result<Projected, ConnectorError> {
    let root = object(raw)?;
    let producer = object(root.get("producer").ok_or_else(schema_mismatch)?)?;
    let producer_connections = required_array(producer, "connections")?;
    let consumers = required_array(root, "consumers")?;
    let failed_consumer_groups = required_array(root, "failed_consumer_groups")?;
    let truncated = required_bool(root, "truncated")?;
    let mut brokers = BTreeSet::new();
    for row in producer_connections {
        let connection = object(object(row)?.get("connection").ok_or_else(schema_mismatch)?)?;
        brokers.insert(required_string(connection, "broker_name")?);
    }
    let mut consumer_connections = 0usize;
    for consumer in consumers {
        let consumer = object(consumer)?;
        for connection in required_array(consumer, "connections")? {
            brokers.insert(required_string(object(connection)?, "broker_name")?);
            consumer_connections = consumer_connections.saturating_add(1);
        }
    }
    Ok(Projected {
        content: json!({
            "connected_clients": producer_connections.len().saturating_add(consumer_connections),
            "producer_connections": producer_connections.len(),
            "consumer_connections": consumer_connections,
            "connected_brokers": brokers.len(),
            "failed_consumer_groups": failed_consumer_groups.len(),
            "truncated": truncated,
        }),
        completeness: Completeness::Partial,
        warnings: vec!["disconnected_client_count_not_queryable"],
    })
}

fn prometheus_broker_health(raw: &Value) -> Result<Projected, ConnectorError> {
    let samples = prometheus_latest_values(raw)?;
    if samples.is_empty() {
        return Ok(missing("broker_health_no_samples"));
    }
    Ok(Projected {
        content: json!({
            "ready": samples.iter().all(|value| *value > 0.0),
            "observed_series": samples.len(),
        }),
        completeness: Completeness::Partial,
        warnings: vec!["broker_health_metrics_incomplete"],
    })
}

fn prometheus_consumer_runtime(raw: &Value) -> Result<Projected, ConnectorError> {
    let samples = prometheus_latest_values(raw)?;
    if samples.is_empty() {
        return Ok(missing("consumer_runtime_no_samples"));
    }
    let connected_clients = samples.iter().try_fold(0.0_f64, |total, value| {
        let next = total + value;
        next.is_finite().then_some(next).ok_or_else(schema_mismatch)
    })?;
    Ok(Projected {
        content: json!({
            "connected_clients": connected_clients,
            "observed_series": samples.len(),
        }),
        completeness: Completeness::Partial,
        warnings: vec!["consumer_runtime_metrics_incomplete"],
    })
}

fn prometheus_collector(raw: &Value) -> Result<Projected, ConnectorError> {
    let samples = prometheus_latest_values(raw)?;
    if samples.is_empty() {
        return Ok(missing("collector_metrics_no_samples"));
    }
    let failures = samples.iter().try_fold(0.0_f64, |total, value| {
        let next = total + value;
        next.is_finite().then_some(next).ok_or_else(schema_mismatch)
    })?;
    Ok(Projected {
        content: json!({
            "export_failures_total": failures,
            "observed_series": samples.len(),
        }),
        completeness: Completeness::Partial,
        warnings: vec!["collector_queue_metric_unavailable"],
    })
}

fn prometheus_latest_values(raw: &Value) -> Result<Vec<f64>, ConnectorError> {
    let root = object(raw)?;
    if root.get("schema_version").and_then(Value::as_str) == Some("rocketmq.prometheus-evidence.v1") {
        return required_array(root, "series")?
            .iter()
            .filter_map(|series| {
                let series = match object(series) {
                    Ok(series) => series,
                    Err(error) => return Some(Err(error)),
                };
                let samples = match required_array(series, "samples") {
                    Ok(samples) => samples,
                    Err(error) => return Some(Err(error)),
                };
                samples.last().map(|sample| {
                    object(sample)?
                        .get("value")
                        .and_then(Value::as_f64)
                        .filter(|value| value.is_finite())
                        .ok_or_else(schema_mismatch)
                })
            })
            .collect();
    }
    if required_string(root, "status")? != "success" {
        return Err(schema_mismatch());
    }
    let data = object(root.get("data").ok_or_else(schema_mismatch)?)?;
    let result_type = required_string(data, "resultType")?;
    let result = required_array(data, "result")?;
    result
        .iter()
        .map(|series| {
            let series = object(series)?;
            match result_type {
                "matrix" => {
                    let values = required_array(series, "values")?;
                    let latest = values.last().ok_or_else(schema_mismatch)?;
                    sample_value(latest)
                }
                "vector" => sample_value(series.get("value").ok_or_else(schema_mismatch)?),
                _ => Err(schema_mismatch()),
            }
        })
        .collect()
}

fn sample_value(raw: &Value) -> Result<f64, ConnectorError> {
    let sample = raw
        .as_array()
        .filter(|sample| sample.len() == 2)
        .ok_or_else(schema_mismatch)?;
    if !sample[0].is_number() {
        return Err(schema_mismatch());
    }
    let value = sample[1]
        .as_str()
        .ok_or_else(schema_mismatch)?
        .parse::<f64>()
        .map_err(|_| schema_mismatch())?;
    value.is_finite().then_some(value).ok_or_else(schema_mismatch)
}

fn kubernetes_workloads(raw: &Value, collector: bool) -> Result<Projected, ConnectorError> {
    let summary = workload_summary(raw)?;
    if summary.observed == 0 {
        return Ok(missing(if collector {
            "collector_workload_not_observed"
        } else {
            "rocketmq_workload_not_observed"
        }));
    }
    Ok(Projected {
        content: json!({
            "observed_workloads": summary.observed,
            "ready_workloads": summary.ready,
            "unready_workloads": summary.unready,
            "unknown_readiness_workloads": summary.unknown,
        }),
        completeness: if summary.unknown == 0 {
            Completeness::Available
        } else {
            Completeness::Partial
        },
        warnings: (summary.unknown > 0)
            .then_some("workload_readiness_incomplete")
            .into_iter()
            .collect(),
    })
}

fn deployment_drift_basis(raw: &Value) -> Result<Projected, ConnectorError> {
    let summary = workload_summary(raw)?;
    if summary.observed == 0 {
        return Ok(missing("deployment_workload_not_observed"));
    }
    Ok(Projected {
        content: json!({
            "comparison_status": "desired_state_unavailable",
            "observed_workloads": summary.observed,
            "ready_workloads": summary.ready,
            "unready_workloads": summary.unready,
            "unknown_readiness_workloads": summary.unknown,
        }),
        completeness: Completeness::Partial,
        warnings: vec!["deployment_desired_state_unavailable"],
    })
}

struct WorkloadSummary {
    observed: u64,
    ready: u64,
    unready: u64,
    unknown: u64,
}

fn workload_summary(raw: &Value) -> Result<WorkloadSummary, ConnectorError> {
    let root = object(raw)?;
    if required_string(root, "kind")? != "pods" {
        return Err(schema_mismatch());
    }
    let items = required_array(root, "items")?;
    let mut summary = WorkloadSummary {
        observed: items.len() as u64,
        ready: 0,
        unready: 0,
        unknown: 0,
    };
    for item in items {
        let item = object(item)?;
        let phase = item.get("phase").and_then(Value::as_str);
        let containers = required_array(item, "containers")?;
        let readiness = if phase.is_some_and(|phase| phase != "Running") {
            Some(false)
        } else if phase != Some("Running") || containers.is_empty() {
            None
        } else {
            let ready = containers
                .iter()
                .map(|container| {
                    object(container)?
                        .get("ready")
                        .and_then(Value::as_bool)
                        .ok_or_else(schema_mismatch)
                })
                .collect::<Result<Vec<_>, _>>();
            match ready {
                Ok(ready) => Some(ready.into_iter().all(|ready| ready)),
                Err(_) => None,
            }
        };
        match readiness {
            Some(true) => summary.ready = summary.ready.saturating_add(1),
            Some(false) => summary.unready = summary.unready.saturating_add(1),
            None => summary.unknown = summary.unknown.saturating_add(1),
        }
    }
    Ok(summary)
}

fn runtime_observability(raw: &Value) -> Result<Projected, ConnectorError> {
    let root = object(raw)?;
    if required_string(root, "schema_version")? != "rocketmq.observability-status.v1" {
        return Err(schema_mismatch());
    }
    let enabled = required_bool(root, "enabled")?;
    let compiled = object(root.get("compiled_features").ok_or_else(schema_mismatch)?)?;
    let compiled_signal = ["metrics", "traces", "logs"]
        .into_iter()
        .map(|field| required_bool(compiled, field))
        .collect::<Result<Vec<_>, _>>()?;
    let signal_enabled = ["metrics", "traces", "logs"]
        .into_iter()
        .map(|field| {
            let signal = object(root.get(field).ok_or_else(schema_mismatch)?)?;
            required_string(signal, "exporter")?;
            required_bool(signal, "enabled")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let initialization = required_string(root, "initialization")?;
    let export = required_string(root, "export")?;
    Ok(Projected {
        content: json!({
            "build_feature_enabled": compiled_signal.into_iter().any(|value| value),
            "exporter_enabled": enabled && signal_enabled.into_iter().any(|value| value),
            "initialization_status": initialization,
            "export_status": export,
        }),
        completeness: Completeness::Partial,
        warnings: vec!["exporter_runtime_counters_not_instrumented"],
    })
}

fn cluster_topology(raw: &Value) -> Result<Projected, ConnectorError> {
    let root = object(raw)?;
    required_string(root, "cluster")?;
    let brokers = required_array(root, "brokers")?;
    let mut broker_names = BTreeSet::new();
    for broker in brokers {
        broker_names.insert(required_string(object(broker)?, "broker_name")?);
    }
    let topic_count = required_u64(root, "topic_count")?;
    let consumer_group_count = required_u64(root, "consumer_group_count")?;
    Ok(Projected {
        content: json!({
            "brokers": broker_names.len(),
            "broker_rows": brokers.len(),
            "topics": topic_count,
            "consumer_groups": consumer_group_count,
        }),
        completeness: Completeness::Partial,
        warnings: vec!["topology_edge_state_unavailable"],
    })
}

fn skew_ratio(values: &[i64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut values = values.iter().map(|value| (*value).max(0) as f64).collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    let median = if values.len() % 2 == 0 {
        let upper = values.len() / 2;
        (values[upper - 1] + values[upper]) / 2.0
    } else {
        values[values.len() / 2]
    };
    let maximum = *values.last()?;
    (median > 0.0).then_some(maximum / median)
}

fn missing(reason_code: &'static str) -> Projected {
    Projected {
        content: json!({
            "status": "missing",
            "reason_code": reason_code,
        }),
        completeness: Completeness::Missing,
        warnings: vec!["canonical_source_data_missing"],
    }
}

fn object(value: &Value) -> Result<&Map<String, Value>, ConnectorError> {
    value.as_object().ok_or_else(schema_mismatch)
}

fn required_array<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a [Value], ConnectorError> {
    object
        .get(field)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(schema_mismatch)
}

fn required_string<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a str, ConnectorError> {
    object.get(field).and_then(Value::as_str).ok_or_else(schema_mismatch)
}

fn required_bool(object: &Map<String, Value>, field: &str) -> Result<bool, ConnectorError> {
    object.get(field).and_then(Value::as_bool).ok_or_else(schema_mismatch)
}

fn required_i64(object: &Map<String, Value>, field: &str) -> Result<i64, ConnectorError> {
    object.get(field).and_then(Value::as_i64).ok_or_else(schema_mismatch)
}

fn optional_i64(object: &Map<String, Value>, field: &str) -> Result<Option<i64>, ConnectorError> {
    match object.get(field) {
        Some(value) => value.as_i64().map(Some).ok_or_else(schema_mismatch),
        None => Ok(None),
    }
}

fn required_u64(object: &Map<String, Value>, field: &str) -> Result<u64, ConnectorError> {
    object.get(field).and_then(Value::as_u64).ok_or_else(schema_mismatch)
}

fn required_f64(object: &Map<String, Value>, field: &str) -> Result<f64, ConnectorError> {
    object
        .get(field)
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
        .ok_or_else(schema_mismatch)
}

fn finite_number(value: f64) -> Result<Value, ConnectorError> {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(schema_mismatch)
}

fn schema_mismatch() -> ConnectorError {
    ConnectorError::capability(
        ConnectorErrorCode::CapabilityMismatch,
        "canonical source response does not match the supported projection schema",
    )
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use chrono::Utc;
    use rocketmq_sre_contracts::EvidenceExposure;
    use rocketmq_sre_contracts::Sensitivity;

    use super::*;

    fn raw(content: Value) -> SourceOutput {
        SourceOutput {
            observed_at: Utc.with_ymd_and_hms(2026, 7, 27, 8, 0, 0).single().expect("time"),
            freshness_seconds: 2,
            partial: false,
            warnings: Vec::new(),
            sensitivity: Sensitivity::Internal,
            coverage: CoverageStatus::Available,
            exposure: EvidenceExposure::Unknown,
            content,
        }
    }

    #[test]
    fn consumer_lag_projects_only_observed_and_computable_fields() {
        let output = apply(
            raw(json!({
                "cluster": "local",
                "topic": "orders",
                "consumer_group": "billing",
                "total_lag": 40,
                "max_queue_lag": 30,
                "consume_tps": 12.5,
                "inflight_total": 2,
                "items": [{"lag": 10}, {"lag": 30}],
                "count": 2,
                "total_count": 2,
                "has_more": false,
                "next_cursor": null,
                "generated_at": "2026-07-27T08:00:00Z"
            })),
            CanonicalProjection::ConsumerLag,
        )
        .expect("projection");

        assert_eq!(output.coverage, CoverageStatus::Partial);
        assert_eq!(output.content["total_lag"], 40);
        assert_eq!(output.content["consume_rate_per_sec"], 12.5);
        assert_eq!(output.content["queue_skew_ratio"], 1.5);
        assert!(output.content.get("lag_slope_per_min").is_none());
        assert!(output.content.get("produce_rate_per_sec").is_none());
    }

    #[test]
    fn broker_health_projects_fault_and_missing_prometheus_shapes() {
        let fault =
            apply(raw(prometheus_matrix(vec![0.0])), CanonicalProjection::BrokerHealth).expect("fault projection");
        assert_eq!(fault.content["ready"], false);
        assert_eq!(fault.coverage, CoverageStatus::Partial);

        let missing =
            apply(raw(prometheus_matrix(Vec::new())), CanonicalProjection::BrokerHealth).expect("missing projection");
        assert_eq!(missing.coverage, CoverageStatus::Missing);
        assert_eq!(missing.content["status"], "missing");
    }

    #[test]
    fn topology_and_runtime_projections_never_invent_health_or_edges() {
        let topology = apply(
            raw(json!({
                "cluster": "local",
                "brokers": [
                    {"broker_name": "broker-a", "broker_active": true},
                    {"broker_name": "broker-a", "broker_active": true}
                ],
                "topic_count": 4,
                "consumer_group_count": 2,
                "generated_at": "2026-07-27T08:00:00Z"
            })),
            CanonicalProjection::ClusterTopology,
        )
        .expect("topology projection");
        assert_eq!(topology.content["brokers"], 1);
        assert!(topology.content.get("broken_edges").is_none());
        assert_eq!(topology.coverage, CoverageStatus::Partial);

        let telemetry = apply(
            raw(json!({
                "schema_version": "rocketmq.observability-status.v1",
                "enabled": true,
                "compiled_features": {
                    "metrics": true, "traces": true, "logs": false,
                    "otlp_grpc": true, "prometheus": false
                },
                "metrics": {"enabled": true, "exporter": "otlp_grpc"},
                "traces": {"enabled": true, "exporter": "otlp_grpc"},
                "logs": {"enabled": false, "exporter": "disabled"},
                "initialization": "ready",
                "export": "unknown"
            })),
            CanonicalProjection::RuntimeObservability,
        )
        .expect("telemetry projection");
        assert_eq!(telemetry.content["build_feature_enabled"], true);
        assert_eq!(telemetry.content["exporter_enabled"], true);
        assert!(telemetry.content.get("collector_reachable").is_none());
    }

    #[test]
    fn kubernetes_live_and_drift_projections_distinguish_observed_from_unknown() {
        let wire = json!({
            "kind": "pods",
            "namespace": "rocketmq",
            "items": [
                {"phase": "Running", "containers": [{"ready": true}]},
                {"phase": "Pending", "containers": []}
            ]
        });
        let live = apply(raw(wire.clone()), CanonicalProjection::KubernetesWorkloads).expect("live projection");
        assert_eq!(live.content["unready_workloads"], 1);
        assert_eq!(live.coverage, CoverageStatus::Available);

        let drift = apply(raw(wire), CanonicalProjection::DeploymentDriftBasis).expect("drift basis");
        assert_eq!(drift.coverage, CoverageStatus::Partial);
        assert_eq!(drift.content["comparison_status"], "desired_state_unavailable");
        assert!(drift.content.get("image_drift").is_none());
    }

    #[test]
    fn malformed_wire_fails_closed_instead_of_becoming_healthy() {
        let error = apply(
            raw(json!({
                "status": "success",
                "data": {"resultType": "matrix", "result": [{"values": [["bad", "NaN"]]}]}
            })),
            CanonicalProjection::BrokerHealth,
        )
        .expect_err("invalid sample");
        assert_eq!(error.code, ConnectorErrorCode::CapabilityMismatch);
    }

    #[test]
    fn connection_projections_emit_counts_without_raw_client_identifiers() {
        let producer = apply(
            raw(json!({
                "connections": [{
                    "producer_group": "producer-a",
                    "connection": {
                        "broker_name": "broker-a",
                        "client_id": "raw-client",
                        "client_addr": "10.0.0.1:12000",
                        "language": "RUST",
                        "version": 1,
                        "last_update_timestamp": 1
                    }
                }],
                "queried_broker_count": 1,
                "failed_brokers": [],
                "truncated": false
            })),
            CanonicalProjection::ProducerConnectivity,
        )
        .expect("producer projection");
        let encoded = serde_json::to_string(&producer.content).expect("JSON");
        assert_eq!(producer.content["connected"], true);
        assert_eq!(producer.content["connected_clients"], 1);
        assert!(!encoded.contains("raw-client"));
        assert!(!encoded.contains("10.0.0.1"));

        let clients = apply(
            raw(json!({
                "producer": {
                    "connections": [{
                        "producer_group": "producer-a",
                        "connection": {
                            "broker_name": "broker-a",
                            "client_id": "raw-producer",
                            "client_addr": "10.0.0.1:12000",
                            "language": "RUST",
                            "version": 1,
                            "last_update_timestamp": 1
                        }
                    }],
                    "queried_broker_count": 1,
                    "failed_brokers": [],
                    "truncated": false
                },
                "consumers": [{
                    "consumer_group": "consumer-a",
                    "connections": [{
                        "broker_name": "broker-a",
                        "client_id": "raw-consumer",
                        "client_addr": "10.0.0.2:12000",
                        "language": "JAVA",
                        "version": 2,
                        "last_update_timestamp": null
                    }],
                    "queried_broker_count": 1,
                    "failed_brokers": [],
                    "truncated": false
                }],
                "failed_consumer_groups": [],
                "truncated": false
            })),
            CanonicalProjection::ClientConnections,
        )
        .expect("client projection");
        let encoded = serde_json::to_string(&clients.content).expect("JSON");
        assert_eq!(clients.content["connected_clients"], 2);
        assert!(!encoded.contains("raw-producer"));
        assert!(!encoded.contains("raw-consumer"));
        assert!(!encoded.contains("10.0.0."));
    }

    fn prometheus_matrix(values: Vec<f64>) -> Value {
        json!({
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": values.into_iter().map(|value| {
                    json!({"metric": {}, "values": [[1_721_000_000, value.to_string()]]})
                }).collect::<Vec<_>>()
            }
        })
    }
}
