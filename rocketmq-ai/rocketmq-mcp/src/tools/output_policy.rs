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
    match value {
        Value::Object(object) => {
            let mut changed = false;
            for value in object.values_mut() {
                changed |= bound_rows(value, max_rows);
            }
            changed
        }
        Value::Array(values) => {
            let mut changed = values.len() > max_rows;
            values.truncate(max_rows);
            for value in values {
                changed |= bound_rows(value, max_rows);
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
    let warning = Value::String("output_rows_truncated".to_string());
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
                    "client_ip": "127.0.0.1"
                }]
            }
        }))
        .unwrap();

        let serialized = output.to_string();
        assert!(!serialized.contains("namesrv_addr"));
        assert!(!serialized.contains("broker_addr"));
        assert!(!serialized.contains("client_ip"));
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
}
