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

use serde_json::Map;
use serde_json::Value;

use crate::model::contract::MAX_SOURCE_FAILURES;
use crate::tools::executor::ToolExecutionError;

const MAX_STRUCTURED_OUTPUT_BYTES: usize = 1024 * 1024;
const MAX_STRUCTURED_OUTPUT_ROWS: usize = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OutputPolicy {
    pub max_bytes: usize,
    pub max_rows: usize,
}

impl Default for OutputPolicy {
    fn default() -> Self {
        Self {
            max_bytes: MAX_STRUCTURED_OUTPUT_BYTES,
            max_rows: MAX_STRUCTURED_OUTPUT_ROWS,
        }
    }
}

pub(crate) fn apply(value: Value) -> Result<Value, ToolExecutionError> {
    apply_with_policy(value, OutputPolicy::default())
}

pub(crate) fn apply_with_policy(mut value: Value, policy: OutputPolicy) -> Result<Value, ToolExecutionError> {
    let source_failures_overflow = value
        .get("source_failures")
        .and_then(Value::as_array)
        .is_some_and(|failures| failures.len() > MAX_SOURCE_FAILURES);
    let source_metadata_changed = normalize_source_failure_metadata(&mut value);
    if source_metadata_changed {
        add_warning(&mut value, "source_failure_metadata_sanitized");
    }
    if source_failures_overflow {
        add_warning(&mut value, "source_failures_truncated");
    }
    remove_internal_topology(&mut value);
    let truncated = bound_rows(&mut value, policy.max_rows);
    if truncated {
        mark_partial(&mut value);
    }
    let size = serde_json::to_vec(&value).map_err(ToolExecutionError::internal)?.len();
    if size > policy.max_bytes {
        return Err(ToolExecutionError::OutputTooLarge {
            actual_bytes: size,
            max_bytes: policy.max_bytes,
        });
    }
    Ok(value)
}

fn bound_rows(value: &mut Value, max_rows: usize) -> bool {
    let mut remaining = max_rows;
    bound_rows_with_remaining(value, &mut remaining)
}

fn bound_rows_with_remaining(value: &mut Value, remaining: &mut usize) -> bool {
    match value {
        Value::Object(object) => {
            let mut changed = false;
            for (key, value) in object.iter_mut() {
                if !matches!(key.as_str(), "warnings" | "source_failures") {
                    changed |= bound_rows_with_remaining(value, remaining);
                }
            }
            changed
        }
        Value::Array(values) => {
            let retained = values.len().min(*remaining);
            let mut changed = retained != values.len();
            values.truncate(retained);
            *remaining -= retained;
            for value in values {
                changed |= bound_rows_with_remaining(value, remaining);
            }
            changed
        }
        _ => false,
    }
}

fn mark_partial(value: &mut Value) {
    let Value::Object(object) = value else {
        return;
    };
    object.insert("partial".to_string(), Value::Bool(true));
    add_warning(value, "output_rows_truncated");
}

fn add_warning(value: &mut Value, warning: &str) {
    let Value::Object(object) = value else {
        return;
    };
    let warning = Value::String(warning.to_string());
    match object.get_mut("warnings") {
        Some(Value::Array(warnings)) if !warnings.contains(&warning) => warnings.push(warning),
        Some(_) => {
            object.insert("warnings".to_string(), Value::Array(vec![warning]));
        }
        None => {
            object.insert("warnings".to_string(), Value::Array(vec![warning]));
        }
    }
}

fn normalize_source_failure_metadata(value: &mut Value) -> bool {
    match value {
        Value::Object(object) => {
            let mut changed = object
                .get_mut("source_failures")
                .map(normalize_source_failure_array)
                .unwrap_or(false);
            for (key, value) in object.iter_mut() {
                if key != "source_failures" {
                    changed |= normalize_source_failure_metadata(value);
                }
            }
            changed
        }
        Value::Array(values) => values.iter_mut().fold(false, |changed, value| {
            normalize_source_failure_metadata(value) | changed
        }),
        _ => false,
    }
}

fn normalize_source_failure_array(value: &mut Value) -> bool {
    let Value::Array(failures) = value else {
        *value = Value::Array(Vec::new());
        return true;
    };
    let original = failures.clone();
    let mut normalized = failures.iter().filter_map(normalize_source_failure).collect::<Vec<_>>();
    normalized.sort_by_key(source_failure_key);
    normalized.dedup();
    normalized.truncate(MAX_SOURCE_FAILURES);
    let changed = normalized != original;
    *failures = normalized;
    changed
}

fn normalize_source_failure(value: &Value) -> Option<Value> {
    let object = value.as_object()?;
    let source = object.get("source")?.as_str()?;
    let code = object.get("code")?.as_str()?;
    let retryable = object.get("retryable")?.as_bool()?;
    let logical_target = safe_logical_target(object.get("logical_target")?.as_str()?);
    if !matches!(
        source,
        "broker_runtime"
            | "broker_config"
            | "broker_log_filter"
            | "consumer_statistics"
            | "consumer_connection"
            | "producer_connection"
            | "subscription_groups"
            | "topic_route"
            | "topic_config"
            | "consumer_group_config"
            | "topic_stats"
            | "broker_ha_runtime"
            | "controller_sync_state"
            | "controller_metadata"
            | "nameserver_config"
    ) || !matches!(
        code,
        "source_unavailable" | "timeout" | "permission_denied" | "not_found" | "rate_limited" | "invalid_response"
    ) {
        return None;
    }
    Some(serde_json::json!({
        "source": source,
        "code": code,
        "retryable": retryable,
        "logical_target": logical_target,
    }))
}

fn source_failure_key(value: &Value) -> (String, String, String, bool) {
    (
        value["source"].as_str().unwrap_or_default().to_string(),
        value["code"].as_str().unwrap_or_default().to_string(),
        value["logical_target"].as_str().unwrap_or_default().to_string(),
        value["retryable"].as_bool().unwrap_or_default(),
    )
}

fn safe_logical_target(target: &str) -> String {
    let target = target.trim();
    if target.is_empty()
        || target.len() > 128
        || target.parse::<std::net::IpAddr>().is_ok()
        || target.parse::<std::net::SocketAddr>().is_ok()
        || target.contains([':', '/', '\\', '@', '=', '&', '?'])
        || target.chars().any(char::is_control)
    {
        return "unknown".to_string();
    }
    target
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn remove_internal_topology(value: &mut Value) {
    match value {
        Value::Object(object) => {
            remove_sensitive_keys(object);
            for value in object.values_mut() {
                remove_internal_topology(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                remove_internal_topology(value);
            }
        }
        _ => {}
    }
}

fn remove_sensitive_keys(object: &mut Map<String, Value>) {
    object.retain(|key, _| !is_internal_topology_key(key));
}

fn is_internal_topology_key(key: &str) -> bool {
    let normalized = key
        .chars()
        .filter(|character| !matches!(character, '_' | '-' | '.'))
        .flat_map(char::to_lowercase)
        .collect::<String>();
    matches!(
        normalized.as_str(),
        "namesrvaddr"
            | "namesrvaddrs"
            | "brokeraddr"
            | "brokeraddrs"
            | "proxyaddr"
            | "proxyaddrs"
            | "proxyendpoint"
            | "proxyendpoints"
            | "endpoint"
            | "endpoints"
            | "clientip"
            | "clientaddr"
            | "remoteaddr"
            | "remoteaddress"
            | "localaddr"
            | "localaddress"
            | "storehost"
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn default_policy_removes_internal_topology_recursively() {
        let output = apply(json!({
            "namesrv_addr": "127.0.0.1:9876",
            "data": {
                "brokers": [{
                    "broker_name": "broker-a",
                    "broker_addr": "127.0.0.1:10911",
                    "broker_addrs": {"0": "127.0.0.1:10911"},
                    "client_ip": "127.0.0.1",
                    "proxy_endpoint": "private-proxy.internal:8081",
                    "endpoint": "private-proxy.internal:8081"
                }]
            }
        }))
        .unwrap();

        let serialized = output.to_string();
        assert!(!serialized.contains("namesrv_addr"));
        assert!(!serialized.contains("broker_addr"));
        assert!(!serialized.contains("client_ip"));
        assert!(!serialized.contains("private-proxy.internal"));
        assert!(serialized.contains("broker-a"));
    }

    #[test]
    fn row_policy_marks_truncated_envelopes_partial() {
        let output = apply_with_policy(
            serde_json::json!({
                "partial": false,
                "warnings": [],
                "data": [1, 2, 3],
            }),
            OutputPolicy {
                max_bytes: 1024,
                max_rows: 2,
            },
        )
        .unwrap();

        assert_eq!(output["partial"], true);
        assert_eq!(output["warnings"][0], "output_rows_truncated");
        assert_eq!(output["data"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn nested_row_policy_uses_one_query_wide_exact_budget() {
        let exact = apply_with_policy(
            json!({
                "partial": false,
                "warnings": [],
                "data": {"brokers": [{"connections": [1, 2]}]}
            }),
            OutputPolicy {
                max_bytes: 1024,
                max_rows: 3,
            },
        )
        .unwrap();
        assert_eq!(exact["partial"], false);
        assert_eq!(exact["data"]["brokers"][0]["connections"].as_array().unwrap().len(), 2);

        let overflow = apply_with_policy(
            json!({
                "partial": false,
                "warnings": [],
                "data": {"brokers": [{"connections": [1, 2, 3]}]}
            }),
            OutputPolicy {
                max_bytes: 1024,
                max_rows: 3,
            },
        )
        .unwrap();
        assert_eq!(overflow["partial"], true);
        assert_eq!(
            overflow["data"]["brokers"][0]["connections"]
                .as_array()
                .unwrap()
                .as_slice(),
            [json!(1), json!(2)]
        );
        assert!(overflow["warnings"]
            .as_array()
            .unwrap()
            .contains(&json!("output_rows_truncated")));
    }

    #[test]
    fn source_failure_policy_sanitizes_deduplicates_orders_and_caps_metadata() {
        let mut failures = (0..20)
            .rev()
            .map(|index| {
                json!({
                    "source": "broker_runtime",
                    "code": "source_unavailable",
                    "retryable": true,
                    "logical_target": format!("broker-{index:02}"),
                    "raw_error": "secret backend body"
                })
            })
            .collect::<Vec<_>>();
        failures.push(failures[19].clone());
        failures.push(json!({
            "source": "broker_runtime",
            "code": "source_unavailable",
            "retryable": true,
            "logical_target": "10.0.0.1:10911"
        }));
        let output = apply(json!({
            "partial": true,
            "warnings": ["source_failures_present"],
            "source_failures": failures,
            "data": {}
        }))
        .unwrap();

        let normalized = output["source_failures"].as_array().unwrap();
        assert_eq!(normalized.len(), MAX_SOURCE_FAILURES);
        assert_eq!(normalized[0]["logical_target"], "broker-00");
        assert!(output["warnings"]
            .as_array()
            .unwrap()
            .contains(&json!("source_failures_truncated")));
        let serialized = output.to_string();
        assert!(!serialized.contains("10.0.0.1"));
        assert!(!serialized.contains("secret backend body"));
        assert!(!serialized.contains("raw_error"));
    }

    #[test]
    fn config_and_topic_stats_sources_keep_only_closed_failure_metadata() {
        let output = apply(json!({
            "partial": true,
            "warnings": ["source_failures_present"],
            "source_failures": [
                {
                    "source": "topic_config",
                    "code": "timeout",
                    "retryable": true,
                    "logical_target": "broker-a",
                    "backend_text": "must not escape"
                },
                {
                    "source": "consumer_group_config",
                    "code": "not_found",
                    "retryable": false,
                    "logical_target": "broker-b",
                    "address": "10.0.0.1:10911"
                },
                {
                    "source": "topic_stats",
                    "code": "invalid_response",
                    "retryable": false,
                    "logical_target": "broker-c",
                    "attributes": { "secret": "value" }
                },
                {
                    "source": "consumer_connection",
                    "code": "source_unavailable",
                    "retryable": true,
                    "logical_target": "broker-d",
                    "client_id": "must not escape"
                }
            ],
            "data": {}
        }))
        .unwrap();

        let failures = output["source_failures"].as_array().unwrap();
        assert_eq!(failures.len(), 4);
        assert_eq!(failures[0]["source"], "consumer_connection");
        assert_eq!(failures[1]["source"], "consumer_group_config");
        assert_eq!(failures[2]["source"], "topic_config");
        assert_eq!(failures[3]["source"], "topic_stats");
        let serialized = output.to_string();
        assert!(!serialized.contains("backend_text"));
        assert!(!serialized.contains("address"));
        assert!(!serialized.contains("attributes"));
        assert!(!serialized.contains("must not escape"));
    }

    #[test]
    fn infrastructure_sources_preserve_only_closed_metadata_and_mixed_cap_order() {
        let sources = [
            "broker_ha_runtime",
            "controller_sync_state",
            "controller_metadata",
            "nameserver_config",
        ];
        for source in sources {
            let output = apply(json!({
                "source_failures": [{
                    "source": source,
                    "code": "invalid_response",
                    "retryable": false,
                    "logical_target": "logical-a",
                    "raw_error": "private endpoint and backend body"
                }]
            }))
            .unwrap();
            assert_eq!(
                output["source_failures"][0],
                json!({
                    "source": source,
                    "code": "invalid_response",
                    "retryable": false,
                    "logical_target": "logical-a"
                })
            );
        }

        let failures = (0..17)
            .rev()
            .map(|index| {
                json!({
                    "source": sources[index % sources.len()],
                    "code": if index % 2 == 0 { "timeout" } else { "source_unavailable" },
                    "retryable": index % 2 == 0,
                    "logical_target": format!("logical-{index:02}"),
                    "address": "10.0.0.1:9878",
                    "backend_text": "must not survive"
                })
            })
            .collect::<Vec<_>>();
        let output = apply(json!({
            "partial": true,
            "warnings": ["source_failures_present"],
            "source_failures": failures
        }))
        .unwrap();
        let failures = output["source_failures"].as_array().unwrap();
        assert_eq!(failures.len(), MAX_SOURCE_FAILURES);
        assert!(failures
            .windows(2)
            .all(|pair| source_failure_key(&pair[0]) <= source_failure_key(&pair[1])));
        for source in sources {
            assert!(failures.iter().any(|failure| failure["source"] == source));
        }
        assert!(failures.iter().all(|failure| failure.as_object().unwrap().len() == 4));
        let serialized = output.to_string();
        assert!(!serialized.contains("10.0.0.1"));
        assert!(!serialized.contains("backend_text"));
        assert!(!serialized.contains("must not survive"));
    }

    #[test]
    fn row_truncation_composes_with_existing_source_failure_metadata() {
        let output = apply_with_policy(
            json!({
                "partial": true,
                "warnings": ["source_failures_present"],
                "source_failures": [{
                    "source": "consumer_statistics",
                    "code": "timeout",
                    "retryable": true,
                    "logical_target": "broker-a"
                }],
                "data": [1, 2, 3]
            }),
            OutputPolicy {
                max_bytes: 1024,
                max_rows: 2,
            },
        )
        .unwrap();

        assert_eq!(output["partial"], true);
        let warnings = output["warnings"].as_array().unwrap();
        assert!(warnings.contains(&json!("source_failures_present")));
        assert!(warnings.contains(&json!("output_rows_truncated")));
        assert_eq!(output["source_failures"].as_array().unwrap().len(), 1);
        assert_eq!(output["data"].as_array().unwrap().len(), 2);
    }
}
