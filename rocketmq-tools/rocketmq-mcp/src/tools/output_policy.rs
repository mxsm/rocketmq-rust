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
const INTERNAL_TOPOLOGY_KEYS: [&str; 4] = ["namesrv_addr", "broker_addr", "broker_addrs", "client_ip"];

pub(crate) fn apply(mut value: Value) -> Result<Value, ToolExecutionError> {
    remove_internal_topology(&mut value);
    let size = serde_json::to_vec(&value).map_err(ToolExecutionError::internal)?.len();
    if size > MAX_STRUCTURED_OUTPUT_BYTES {
        return Err(ToolExecutionError::OutputTooLarge {
            actual_bytes: size,
            max_bytes: MAX_STRUCTURED_OUTPUT_BYTES,
        });
    }
    Ok(value)
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
    for key in INTERNAL_TOPOLOGY_KEYS {
        object.remove(key);
    }
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
}
