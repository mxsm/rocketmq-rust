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

use rocketmq_sre_contracts::CoverageStatus;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;

use super::canonical::BrokerDiagnosticProfile;
use super::canonical::CanonicalProjection;
use super::canonical::KubernetesDiagnosticProfile;
use super::canonical::MetricDiagnosticProfile;
use super::canonical::RouteDiagnosticProfile;
use super::common::SourceOutput;
use crate::ConnectorError;
use crate::ConnectorErrorCode;

const DIAGNOSTIC_SCHEMA: &str = "rocketmq.sre-diagnostic-source.v1";

pub(super) fn apply(mut output: SourceOutput, projection: CanonicalProjection) -> Result<SourceOutput, ConnectorError> {
    let projected = match projection {
        CanonicalProjection::BrokerDiagnostics(profile) => broker_diagnostics(&output.content, profile)?,
        CanonicalProjection::MetricDiagnostics(profile) => metric_diagnostics(&output.content, profile)?,
        CanonicalProjection::RouteDiagnostics(profile) => route_diagnostics(&output.content, profile)?,
        CanonicalProjection::TopicSubscriptionConfig => topic_subscription_config(&output.content)?,
        CanonicalProjection::MessageMetadata => message_metadata(&output.content)?,
        CanonicalProjection::RuntimeSaturation => runtime_saturation(&output.content)?,
        CanonicalProjection::KubernetesDiagnostics(profile) => kubernetes_diagnostics(&output.content, profile)?,
        _ => return Err(schema_mismatch()),
    };
    output.content = projected.content;
    output.warnings.push(projected.warning.to_owned());
    output.partial = true;
    output.coverage = if projected.observed {
        CoverageStatus::Partial
    } else {
        CoverageStatus::Missing
    };
    Ok(output)
}

struct Projected {
    content: Value,
    observed: bool,
    warning: &'static str,
}

fn broker_diagnostics(raw: &Value, profile: BrokerDiagnosticProfile) -> Result<Projected, ConnectorError> {
    let root = object(raw)?;
    let brokers = array(root, "brokers")?;
    let mut rows = Vec::with_capacity(brokers.len());
    for broker in brokers {
        let broker = object(broker)?;
        let mut row = Map::new();
        copy_fields(broker, &mut row, &["broker_name", "broker_id", "coverage"]);
        match profile {
            BrokerDiagnosticProfile::StorePressure => {
                copy_nested(broker, &mut row, "store_health")?;
                copy_nested(broker, &mut row, "tiered")?;
            }
            BrokerDiagnosticProfile::StoreIntegrity => {
                copy_nested(broker, &mut row, "recovery")?;
                copy_nested(broker, &mut row, "background_index_rebuild")?;
            }
            BrokerDiagnosticProfile::RocksDbHealth => copy_nested(broker, &mut row, "rocksdb")?,
            BrokerDiagnosticProfile::TieredStore | BrokerDiagnosticProfile::ColdDataFlow => {
                copy_nested(broker, &mut row, "tiered")?;
                copy_nested(broker, &mut row, "config")?;
            }
            BrokerDiagnosticProfile::BrokerHa | BrokerDiagnosticProfile::DrReadiness => {
                copy_nested(broker, &mut row, "ha")?;
                copy_nested(broker, &mut row, "readiness")?;
                if profile == BrokerDiagnosticProfile::DrReadiness {
                    copy_nested(broker, &mut row, "recovery")?;
                }
            }
            BrokerDiagnosticProfile::AuthFailure | BrokerDiagnosticProfile::SecurityPosture => {
                copy_auth_fields(broker, &mut row);
            }
        }
        rows.push(Value::Object(row));
    }
    Ok(Projected {
        content: json!({
            "schema_version": DIAGNOSTIC_SCHEMA,
            "profile": broker_profile_name(profile),
            "observed_brokers": rows.len(),
            "brokers": rows
        }),
        observed: !brokers.is_empty(),
        warning: "diagnostic_profile_fields_incomplete",
    })
}

fn copy_auth_fields(source: &Map<String, Value>, target: &mut Map<String, Value>) {
    if let Some(auth) = source.get("auth") {
        target.insert("auth".to_owned(), auth.clone());
        return;
    }
    copy_fields(
        source,
        target,
        &[
            "authentication_enabled",
            "authorization_enabled",
            "acl_file_watch_enabled",
            "acl_generation",
            "reload",
            "credential_rotation",
        ],
    );
}

fn metric_diagnostics(raw: &Value, profile: MetricDiagnosticProfile) -> Result<Projected, ConnectorError> {
    let root = object(raw)?;
    if string(root, "schema_version")? != "rocketmq.prometheus-evidence.v1" {
        return Err(schema_mismatch());
    }
    let metric = string(root, "metric")?;
    let series = array(root, "series")?;
    let mut first_values = Vec::new();
    let mut latest_values = Vec::new();
    let mut sample_count = 0_u64;
    for series in series {
        let series = object(series)?;
        let samples = array(series, "samples")?;
        sample_count = sample_count.saturating_add(samples.len() as u64);
        if let Some(value) = samples.first().and_then(sample_number) {
            first_values.push(value);
        }
        if let Some(value) = samples.last().and_then(sample_number) {
            latest_values.push(value);
        }
    }
    let mut summary = Map::new();
    if let Some(value) = finite_sum(&latest_values) {
        summary.insert("latest_sum".to_owned(), number(value)?);
    }
    if let Some(value) = latest_values.iter().copied().reduce(f64::max) {
        summary.insert("latest_max".to_owned(), number(value)?);
    }
    if let Some(value) = latest_values.iter().copied().reduce(f64::min) {
        summary.insert("latest_min".to_owned(), number(value)?);
    }
    if first_values.len() == latest_values.len()
        && let (Some(first), Some(latest)) = (finite_sum(&first_values), finite_sum(&latest_values))
    {
        summary.insert("window_delta".to_owned(), number(latest - first)?);
    }
    Ok(Projected {
        content: json!({
            "schema_version": DIAGNOSTIC_SCHEMA,
            "profile": metric_profile_name(profile),
            "metric": metric,
            "observed_series": latest_values.len(),
            "sample_count": sample_count,
            "summary": summary
        }),
        observed: !latest_values.is_empty(),
        warning: "diagnostic_threshold_not_configured",
    })
}

fn route_diagnostics(raw: &Value, profile: RouteDiagnosticProfile) -> Result<Projected, ConnectorError> {
    let root = object(raw)?;
    let topic = string(root, "topic")?;
    let read_queues = unsigned(root, "read_queue_count")?;
    let write_queues = unsigned(root, "write_queue_count")?;
    let brokers = root
        .get("broker_names")
        .or_else(|| root.get("brokers"))
        .and_then(Value::as_array)
        .ok_or_else(schema_mismatch)?;
    Ok(Projected {
        content: json!({
            "schema_version": DIAGNOSTIC_SCHEMA,
            "profile": route_profile_name(profile),
            "topic": topic,
            "route_available": !brokers.is_empty() && read_queues.max(write_queues) > 0,
            "read_queue_count": read_queues,
            "write_queue_count": write_queues,
            "broker_count": brokers.len()
        }),
        observed: true,
        warning: match profile {
            RouteDiagnosticProfile::NameServer => "nameserver_cross_node_comparison_unavailable",
            RouteDiagnosticProfile::StaticTopic => "static_mapping_detail_unavailable",
        },
    })
}

fn topic_subscription_config(raw: &Value) -> Result<Projected, ConnectorError> {
    let root = object(raw)?;
    if string(root, "schema_version")? != "rocketmq.sre-topic-subscription-config.v1" {
        return Err(schema_mismatch());
    }
    let topic = object(root.get("topic").ok_or_else(schema_mismatch)?)?;
    let group = root.get("consumer_group").and_then(Value::as_object);
    let mut content = Map::from_iter([
        ("schema_version".to_owned(), Value::String(DIAGNOSTIC_SCHEMA.to_owned())),
        (
            "profile".to_owned(),
            Value::String("topic_subscription_config".to_owned()),
        ),
        ("topic".to_owned(), Value::Object(topic.clone())),
    ]);
    if let Some(group) = group {
        content.insert("consumer_group".to_owned(), Value::Object(group.clone()));
        if let (Some(topic_name), Some(subscriptions)) = (
            topic.get("name").and_then(Value::as_str),
            group.get("subscription_topics").and_then(Value::as_array),
        ) {
            content.insert(
                "filter_consistent".to_owned(),
                Value::Bool(subscriptions.iter().any(|value| value.as_str() == Some(topic_name))),
            );
        }
        if let (Some(topic_ordered), Some(group_ordered)) = (
            topic.get("ordered").and_then(Value::as_bool),
            group.get("consume_message_orderly").and_then(Value::as_bool),
        ) {
            content.insert(
                "mode_consistent".to_owned(),
                Value::Bool(topic_ordered == group_ordered),
            );
        }
    }
    Ok(Projected {
        content: Value::Object(content),
        observed: true,
        warning: "topic_group_permission_semantics_unavailable",
    })
}

fn message_metadata(raw: &Value) -> Result<Projected, ConnectorError> {
    let root = object(raw)?;
    if string(root, "schema_version")? != "rocketmq.sre-message-metadata.v1" {
        return Err(schema_mismatch());
    }
    let message_id = string(root, "message_id")?;
    if !is_pseudonym(message_id) {
        return Err(schema_mismatch());
    }
    let mut content = Map::from_iter([
        ("schema_version".to_owned(), Value::String(DIAGNOSTIC_SCHEMA.to_owned())),
        ("profile".to_owned(), Value::String("message_metadata".to_owned())),
        ("message_id_hash".to_owned(), Value::String(message_id.to_owned())),
    ]);
    copy_fields(
        root,
        &mut content,
        &[
            "topic",
            "born_timestamp",
            "store_timestamp",
            "queue_id",
            "queue_offset",
            "store_size",
            "reconsume_times",
            "sys_flag",
            "flag",
            "prepared_transaction_offset",
        ],
    );
    content.insert("transaction_status".to_owned(), Value::String("unknown".to_owned()));
    Ok(Projected {
        content: Value::Object(content),
        observed: true,
        warning: "message_body_and_properties_excluded",
    })
}

fn runtime_saturation(raw: &Value) -> Result<Projected, ConnectorError> {
    let root = object(raw)?;
    if string(root, "schema_version")? != "rocketmq.sre-runtime-evidence.v1" {
        return Err(schema_mismatch());
    }
    let diagnostics = object(root.get("diagnostics").ok_or_else(schema_mismatch)?)?;
    let task_kinds = array(diagnostics, "task_kinds")?;
    let blocking_lanes = array(diagnostics, "blocking_lanes")?;
    let long_running = task_kinds
        .iter()
        .filter_map(|value| value.get("long_running"))
        .filter_map(Value::as_u64)
        .sum::<u64>();
    let mut queued = 0_u64;
    let mut running = 0_u64;
    let mut timeouts = 0_u64;
    let mut pressure = false;
    for lane in blocking_lanes {
        let lane = object(lane)?;
        let lane_queued = lane.get("queued").and_then(Value::as_u64).ok_or_else(schema_mismatch)?;
        let lane_running = lane
            .get("running")
            .and_then(Value::as_u64)
            .ok_or_else(schema_mismatch)?;
        let lane_timeouts = lane
            .get("timed_out_still_running")
            .and_then(Value::as_u64)
            .ok_or_else(schema_mismatch)?;
        queued = queued.saturating_add(lane_queued);
        running = running.saturating_add(lane_running);
        timeouts = timeouts.saturating_add(lane_timeouts);
        pressure |= lane_timeouts > 0
            || lane
                .get("max_queue_depth")
                .and_then(Value::as_u64)
                .is_some_and(|limit| limit > 0 && lane_queued >= limit)
            || lane
                .get("max_concurrency")
                .and_then(Value::as_u64)
                .is_some_and(|limit| limit > 0 && lane_running >= limit);
    }
    let lifecycle = string(diagnostics, "lifecycle_state")?;
    Ok(Projected {
        content: json!({
            "schema_version": DIAGNOSTIC_SCHEMA,
            "profile": "runtime_saturation",
            "component": diagnostics.get("component"),
            "task_group_count": diagnostics.get("task_group_count"),
            "task_count": diagnostics.get("task_count"),
            "long_running_tasks": long_running,
            "blocking_queued": queued,
            "blocking_running": running,
            "blocking_timeouts": timeouts,
            "blocking_executor_saturated": pressure,
            "lifecycle_state": lifecycle,
            "truncated": diagnostics.get("truncated")
        }),
        observed: true,
        warning: "runtime_capacity_and_lifecycle_progress_incomplete",
    })
}

fn kubernetes_diagnostics(raw: &Value, profile: KubernetesDiagnosticProfile) -> Result<Projected, ConnectorError> {
    let root = object(raw)?;
    if string(root, "schema_version")? != "rocketmq.kubernetes-evidence.v1" {
        return Err(schema_mismatch());
    }
    let items = array(root, "items")?;
    let content = match profile {
        KubernetesDiagnosticProfile::UpgradeReadiness => {
            if string(root, "kind")? != "stateful_sets" {
                return Err(schema_mismatch());
            }
            let ready = items
                .iter()
                .filter(|item| item.get("rollout_state").and_then(Value::as_str) == Some("ready"))
                .count();
            json!({
                "schema_version": DIAGNOSTIC_SCHEMA,
                "profile": "upgrade_readiness",
                "observed_workloads": items.len(),
                "ready_workloads": ready,
                "rollout_ready": !items.is_empty() && ready == items.len()
            })
        }
        KubernetesDiagnosticProfile::ChangeRegression => {
            if string(root, "kind")? != "change_timeline" {
                return Err(schema_mismatch());
            }
            let failures = items
                .iter()
                .filter(|item| {
                    item.get("reason")
                        .and_then(Value::as_str)
                        .is_some_and(|reason| reason == "ProgressDeadlineExceeded" || reason.starts_with("Failed"))
                })
                .count();
            json!({
                "schema_version": DIAGNOSTIC_SCHEMA,
                "profile": "change_regression",
                "observed_changes": items.len(),
                "failed_changes": failures
            })
        }
    };
    Ok(Projected {
        content,
        observed: !items.is_empty(),
        warning: match profile {
            KubernetesDiagnosticProfile::UpgradeReadiness => "upgrade_pdb_and_protocol_evidence_incomplete",
            KubernetesDiagnosticProfile::ChangeRegression => "change_slo_comparison_unavailable",
        },
    })
}

fn object(value: &Value) -> Result<&Map<String, Value>, ConnectorError> {
    value.as_object().ok_or_else(schema_mismatch)
}

fn array<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a [Value], ConnectorError> {
    object
        .get(field)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(schema_mismatch)
}

fn string<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a str, ConnectorError> {
    object.get(field).and_then(Value::as_str).ok_or_else(schema_mismatch)
}

fn unsigned(object: &Map<String, Value>, field: &str) -> Result<u64, ConnectorError> {
    object.get(field).and_then(Value::as_u64).ok_or_else(schema_mismatch)
}

fn copy_fields(source: &Map<String, Value>, target: &mut Map<String, Value>, fields: &[&str]) {
    for field in fields {
        if let Some(value) = source.get(*field) {
            target.insert((*field).to_owned(), value.clone());
        }
    }
}

fn copy_nested(
    source: &Map<String, Value>,
    target: &mut Map<String, Value>,
    field: &str,
) -> Result<(), ConnectorError> {
    if let Some(value) = source.get(field) {
        if !value.is_null() && !value.is_object() {
            return Err(schema_mismatch());
        }
        target.insert(field.to_owned(), value.clone());
    }
    Ok(())
}

fn sample_number(value: &Value) -> Option<f64> {
    value
        .get("value")
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
}

fn finite_sum(values: &[f64]) -> Option<f64> {
    (!values.is_empty())
        .then(|| values.iter().sum::<f64>())
        .filter(|value| value.is_finite())
}

fn number(value: f64) -> Result<Value, ConnectorError> {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(schema_mismatch)
}

fn is_pseudonym(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

const fn broker_profile_name(profile: BrokerDiagnosticProfile) -> &'static str {
    match profile {
        BrokerDiagnosticProfile::StorePressure => "store_pressure",
        BrokerDiagnosticProfile::StoreIntegrity => "store_integrity",
        BrokerDiagnosticProfile::RocksDbHealth => "rocksdb_health",
        BrokerDiagnosticProfile::TieredStore => "tiered_store",
        BrokerDiagnosticProfile::BrokerHa => "broker_ha",
        BrokerDiagnosticProfile::AuthFailure => "auth_failure",
        BrokerDiagnosticProfile::ColdDataFlow => "cold_data_flow",
        BrokerDiagnosticProfile::DrReadiness => "dr_readiness",
        BrokerDiagnosticProfile::SecurityPosture => "security_posture",
    }
}

const fn metric_profile_name(profile: MetricDiagnosticProfile) -> &'static str {
    match profile {
        MetricDiagnosticProfile::CapacityRunway => "capacity_runway",
        MetricDiagnosticProfile::ControllerHa => "controller_ha",
        MetricDiagnosticProfile::SendLatency => "send_latency",
        MetricDiagnosticProfile::ProxyConnectivity => "proxy_connectivity",
        MetricDiagnosticProfile::RetryDlq => "retry_dlq",
        MetricDiagnosticProfile::TransactionMessage => "transaction_message",
        MetricDiagnosticProfile::PopRevive => "pop_revive",
        MetricDiagnosticProfile::TimerBacklog => "timer_backlog",
        MetricDiagnosticProfile::QueueHotspot => "queue_hotspot",
    }
}

const fn route_profile_name(profile: RouteDiagnosticProfile) -> &'static str {
    match profile {
        RouteDiagnosticProfile::NameServer => "nameserver_route",
        RouteDiagnosticProfile::StaticTopic => "static_topic_route",
    }
}

fn schema_mismatch() -> ConnectorError {
    ConnectorError::capability(
        ConnectorErrorCode::CapabilityMismatch,
        "diagnostic source response does not match the supported projection schema",
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
            observed_at: Utc.with_ymd_and_hms(2026, 8, 3, 8, 0, 0).single().expect("time"),
            freshness_seconds: 1,
            partial: false,
            warnings: Vec::new(),
            sensitivity: Sensitivity::Internal,
            coverage: CoverageStatus::Available,
            exposure: EvidenceExposure::Unknown,
            content,
        }
    }

    #[test]
    fn metric_profile_uses_only_observed_samples_and_never_invents_thresholds() {
        let output = apply(
            raw(json!({
                "schema_version": "rocketmq.prometheus-evidence.v1",
                "metric": "rocketmq_pop_revive_lag",
                "series": [{"labels": {}, "samples": [
                    {"observed_at": "2026-08-03T07:59:00Z", "value": 4.0},
                    {"observed_at": "2026-08-03T08:00:00Z", "value": 9.0}
                ]}]
            })),
            CanonicalProjection::MetricDiagnostics(MetricDiagnosticProfile::PopRevive),
        )
        .expect("metric projection");

        assert_eq!(output.content["summary"]["latest_max"], 9.0);
        assert_eq!(output.content["summary"]["window_delta"], 5.0);
        assert!(output.content.get("revive_lag_high").is_none());
        assert_eq!(output.coverage, CoverageStatus::Partial);
    }

    #[test]
    fn message_profile_requires_a_gateway_pseudonym_and_has_no_body_surface() {
        let output = apply(
            raw(json!({
                "schema_version": "rocketmq.sre-message-metadata.v1",
                "topic": "orders",
                "message_id": format!("sha256:{}", "a".repeat(64)),
                "queue_id": 1,
                "queue_offset": 8
            })),
            CanonicalProjection::MessageMetadata,
        )
        .expect("message projection");

        assert_eq!(output.content["queue_offset"], 8);
        assert!(output.content.get("body").is_none());
        assert!(output.content.get("properties").is_none());
        assert!(output.content.get("keys").is_none());
        assert!(output.content.get("tags").is_none());
    }

    #[test]
    fn open_runtime_does_not_invent_a_schedule_or_shutdown_stall() {
        let output = apply(
            raw(json!({
                "schema_version": "rocketmq.sre-runtime-evidence.v1",
                "diagnostics": {
                    "component": "broker",
                    "lifecycle_state": "open",
                    "task_group_count": 2,
                    "task_count": 1,
                    "task_kinds": [{
                        "kind": "worker",
                        "active": 1,
                        "long_running": 0,
                        "max_elapsed_millis": 5
                    }],
                    "blocking_lanes": [{
                        "lane": "short_io",
                        "max_concurrency": 4,
                        "max_queue_depth": 8,
                        "queued": 0,
                        "running": 1,
                        "timed_out_still_running": 0,
                        "blocking_still_running": 1,
                        "task_kinds": []
                    }],
                    "truncated": false
                }
            })),
            CanonicalProjection::RuntimeSaturation,
        )
        .expect("runtime projection");

        assert_eq!(output.content["lifecycle_state"], "open");
        assert_eq!(output.content["blocking_executor_saturated"], false);
        assert!(output.content.get("schedule_or_shutdown_stalled").is_none());
    }
}
