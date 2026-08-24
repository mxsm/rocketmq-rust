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

use serde_json::Map;
use serde_json::Value;

const MAX_DEPTH: usize = 12;
const MAX_NODES: usize = 1_024;
const MAX_STRING_BYTES: usize = 4_096;
const MAX_CONTAINER_ENTRIES: usize = 128;
const MAX_RECORD_BYTES: usize = 64 * 1_024;

/// Redacts an allowlisted audit detail projection recursively. It normalises
/// key separators and case so nested `database-url`, `database_url`, and
/// `databaseUrl` receive the same protection.
pub fn redact_audit_value(value: Value) -> Value {
    let mut remaining = MAX_NODES;
    let redacted = redact(value, 0, &mut remaining);
    if serde_json::to_vec(&redacted)
        .map(|encoded| encoded.len() > MAX_RECORD_BYTES)
        .unwrap_or(true)
    {
        Value::String("<truncated>".to_string())
    } else {
        redacted
    }
}

fn redact(value: Value, depth: usize, remaining: &mut usize) -> Value {
    if *remaining == 0 || depth >= MAX_DEPTH {
        return Value::String("<truncated>".to_string());
    }
    *remaining -= 1;
    match value {
        Value::Object(object) => {
            let truncated = object.len() > MAX_CONTAINER_ENTRIES;
            let mut redacted = object
                .into_iter()
                .take(MAX_CONTAINER_ENTRIES)
                .map(|(key, value)| {
                    if sensitive_key(&key) {
                        (key, Value::String("<redacted>".to_string()))
                    } else {
                        (key, redact(value, depth + 1, remaining))
                    }
                })
                .collect::<Map<_, _>>();
            if truncated {
                redacted.insert("<truncated>".to_string(), Value::String("<truncated>".to_string()));
            }
            Value::Object(redacted)
        }
        Value::Array(values) => {
            let truncated = values.len() > MAX_CONTAINER_ENTRIES;
            let mut redacted = values
                .into_iter()
                .take(MAX_CONTAINER_ENTRIES)
                .map(|value| redact(value, depth + 1, remaining))
                .collect::<Vec<_>>();
            if truncated {
                redacted.push(Value::String("<truncated>".to_string()));
            }
            Value::Array(redacted)
        }
        Value::String(value) if value.len() > MAX_STRING_BYTES => {
            let mut end = MAX_STRING_BYTES;
            while !value.is_char_boundary(end) {
                end -= 1;
            }
            Value::String(format!("{}<truncated>", &value[..end]))
        }
        value => value,
    }
}

fn sensitive_key(key: &str) -> bool {
    let normalized = key
        .bytes()
        .filter(u8::is_ascii_alphanumeric)
        .map(char::from)
        .collect::<String>()
        .to_ascii_lowercase();
    [
        "password",
        "passwd",
        "pwd",
        "secret",
        "token",
        "authorization",
        "cookie",
        "accesskey",
        "databaseurl",
        "connectionstring",
        "privatekey",
        "tlskey",
        "clientkey",
        "credential",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::redact_audit_value;
    use serde_json::json;

    #[test]
    fn recursively_redacts_normalized_secret_keys() {
        let redacted = redact_audit_value(json!({
            "database-url": "mysql://secret",
            "nested": [{"TLS_Key": "private"}],
            "safe": "visible"
        }));
        assert_eq!(redacted["database-url"], "<redacted>");
        assert_eq!(redacted["nested"][0]["TLS_Key"], "<redacted>");
        assert_eq!(redacted["safe"], "visible");
    }

    #[test]
    fn truncates_utf8_and_container_values_without_panicking() {
        let redacted = redact_audit_value(json!({
            "text": "界".repeat(2_000),
            "values": (0..256).collect::<Vec<_>>(),
        }));
        assert!(
            redacted["text"]
                .as_str()
                .is_some_and(|value| value.ends_with("<truncated>"))
        );
        assert_eq!(redacted["values"].as_array().map(Vec::len), Some(129));
    }
}
