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

use once_cell::sync::Lazy;
use regex::Regex;
use rmcp::model::CallToolResult;
use rmcp::model::ContentBlock;
use serde_json::Value;

const REDACTED: &str = "[REDACTED]";

static SENSITIVE_ASSIGNMENT: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"(?i)(access[_-]?key|secret[_-]?key|token|password)(["'\s:=]+)([^,\s"'}]+)"#).unwrap());

pub fn sanitize_call_tool_result(mut result: CallToolResult) -> CallToolResult {
    for content in &mut result.content {
        if let ContentBlock::Text(text) = content {
            text.text = sanitize_text(&text.text);
        }
    }

    if let Some(structured_content) = result.structured_content.as_mut() {
        sanitize_value(structured_content);
    }

    result
}

pub fn sanitize_value(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                if is_sensitive_key(key) {
                    *value = Value::String(REDACTED.to_string());
                } else {
                    sanitize_value(value);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                sanitize_value(value);
            }
        }
        Value::String(value) => {
            *value = sanitize_text(value);
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

pub fn sanitize_text(value: &str) -> String {
    SENSITIVE_ASSIGNMENT
        .replace_all(value, |captures: &regex::Captures<'_>| {
            format!("{}{}{}", &captures[1], &captures[2], REDACTED)
        })
        .into_owned()
}

fn is_sensitive_key(key: &str) -> bool {
    let normalized = key
        .chars()
        .filter(|ch| !matches!(ch, '_' | '-' | '.'))
        .flat_map(char::to_lowercase)
        .collect::<String>();

    normalized.contains("accesskey")
        || normalized.contains("secretkey")
        || normalized.contains("token")
        || normalized.contains("password")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_sensitive_json_fields_recursively() {
        let mut value = serde_json::json!({
            "access_key": "ak",
            "nested": {
                "secretKey": "sk",
                "token": "token-value",
                "broker_name": "broker-a"
            }
        });

        sanitize_value(&mut value);

        assert_eq!(value["access_key"], REDACTED);
        assert_eq!(value["nested"]["secretKey"], REDACTED);
        assert_eq!(value["nested"]["token"], REDACTED);
        assert_eq!(value["nested"]["broker_name"], "broker-a");
    }

    #[test]
    fn sanitizes_sensitive_text_assignments() {
        let value = sanitize_text("access_key=ak secret-key:sk token=bearer password=pw");

        assert!(!value.contains("ak"));
        assert!(!value.contains("sk"));
        assert!(!value.contains("bearer"));
        assert!(!value.contains("pw"));
        assert!(value.contains(REDACTED));
    }
}
