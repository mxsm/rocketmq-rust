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

use std::sync::LazyLock;

use regex::Regex;
use rmcp::model::CallToolResult;
use rmcp::model::ContentBlock;
use rmcp::model::ReadResourceResult;
use rmcp::model::ResourceContents;
use rmcp::ErrorData;
use serde_json::Value;

use crate::tools::executor::ToolExecutionError;
use crate::tools::output_policy;

const REDACTED: &str = "[REDACTED]";

static SENSITIVE_ASSIGNMENT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
            r#"(?i)(access[_-]?key|secret[_-]?key|client[_-]?secret|token|password|private[_-]?key|message[_-]?body)(["'\s:=]+)([^,\s"'}]+)"#,
        )
        .expect("sensitive assignment regex is a compile-time invariant")
});

static NETWORK_ADDRESS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?ix)
        \b(?:\d{1,3}\.){3}\d{1,3}(?::\d{1,5})?\b
        |
        \[[0-9a-f:]+\](?::\d{1,5})?
        |
        \b(?:localhost|[a-z0-9-]+(?:\.[a-z0-9-]+)*\.(?:internal|local))(?::\d{1,5})?\b
        ",
    )
    .expect("network address regex is a compile-time invariant")
});

pub fn process_call_tool_result(mut result: CallToolResult, request_id: &str, sanitize_output: bool) -> CallToolResult {
    result.content.retain_mut(|content| match content {
        ContentBlock::Text(text) => {
            if sanitize_output {
                text.text = sanitize_text(&text.text);
            }
            true
        }
        ContentBlock::ResourceLink(resource) => sanitize_resource_link(resource, sanitize_output),
        ContentBlock::Image(_) | ContentBlock::Audio(_) | ContentBlock::Resource(_) => true,
        _ => false,
    });
    if sanitize_output {
        if let Some(structured_content) = result.structured_content.as_mut() {
            sanitize_value(structured_content);
        }
    }

    if let Some(structured_content) = result.structured_content.take() {
        match output_policy::apply(structured_content) {
            Ok(structured_content) => result.structured_content = Some(structured_content),
            Err(error) => {
                let payload = serde_json::json!({
                    "schema_version": crate::model::contract::SCHEMA_VERSION,
                    "request_id": request_id,
                    "correlation_id": request_id,
                    "code": error.code(),
                    "retryable": false,
                    "message": "tool output exceeds the configured output policy",
                });
                return CallToolResult::error(vec![ContentBlock::text(payload.to_string())]);
            }
        }
    }

    result
}

fn sanitize_resource_link(resource: &mut rmcp::model::Resource, sanitize_output: bool) -> bool {
    let Some(uri) = crate::resources::uri::RocketmqResourceUri::parse(&resource.uri)
        .filter(crate::resources::uri::RocketmqResourceUri::is_safe)
    else {
        return false;
    };
    resource.uri = uri.as_string();
    if sanitize_output {
        resource.name = sanitize_text(&resource.name);
        resource.title = resource.title.take().map(|value| sanitize_text(&value));
        resource.description = resource.description.take().map(|value| sanitize_text(&value));
    }
    true
}

pub fn process_read_resource_result(
    mut result: ReadResourceResult,
    request_id: &str,
    sanitize_output: bool,
) -> Result<ReadResourceResult, ErrorData> {
    for content in &mut result.contents {
        if let ResourceContents::TextResourceContents { text, .. } = content {
            let mut value: Value = serde_json::from_str(text).map_err(|error| {
                ErrorData::internal_error(format!("resource output is not valid JSON: {error}"), None)
            })?;
            if let Value::Object(object) = &mut value {
                object.insert("request_id".to_string(), Value::String(request_id.to_string()));
                object.insert("correlation_id".to_string(), Value::String(request_id.to_string()));
            }
            if sanitize_output {
                sanitize_value(&mut value);
            }
            let value = output_policy::apply(value).map_err(|error| output_policy_error(error, request_id))?;
            *text = serde_json::to_string(&value).map_err(|error| {
                ErrorData::internal_error(format!("failed to encode resource output: {error}"), None)
            })?;
        }
    }
    Ok(result)
}

fn output_policy_error(error: ToolExecutionError, request_id: &str) -> ErrorData {
    match error {
        ToolExecutionError::OutputTooLarge {
            actual_bytes,
            max_bytes,
        } => ErrorData::internal_error(
            "resource output exceeds the configured byte budget",
            Some(serde_json::json!({
                "code": "output_too_large",
                "retryable": false,
                "correlation_id": request_id,
                "actual_bytes": actual_bytes,
                "max_bytes": max_bytes,
            })),
        ),
        other => ErrorData::internal_error(
            "resource output policy failed",
            Some(serde_json::json!({
                "code": "resource_output_policy_failed",
                "retryable": false,
                "correlation_id": request_id,
                "reason": other.to_string(),
            })),
        ),
    }
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
    let value = SENSITIVE_ASSIGNMENT
        .replace_all(value, |captures: &regex::Captures<'_>| {
            format!("{}{}{}", &captures[1], &captures[2], REDACTED)
        })
        .into_owned();
    NETWORK_ADDRESS.replace_all(&value, REDACTED).into_owned()
}

fn is_sensitive_key(key: &str) -> bool {
    let normalized = key
        .chars()
        .filter(|ch| !matches!(ch, '_' | '-' | '.'))
        .flat_map(char::to_lowercase)
        .collect::<String>();

    normalized.contains("accesskey")
        || normalized.contains("secretkey")
        || normalized.contains("clientsecret")
        || normalized.contains("token")
        || normalized.contains("password")
        || normalized.contains("privatekey")
        || normalized.contains("tlsmaterial")
        || normalized.contains("aclconfig")
        || normalized.contains("plainaccessconfig")
        || normalized.contains("certificate")
        || normalized == "body"
        || normalized.ends_with("messagebody")
        || normalized.ends_with("rawbody")
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
        let value = sanitize_text(
            "access_key=ak secret-key:sk token=bearer password=pw source=10.24.7.9:9876 endpoint=[::1]:4317",
        );

        assert!(!value.contains("ak"));
        assert!(!value.contains("sk"));
        assert!(!value.contains("bearer"));
        assert!(!value.contains("pw"));
        assert!(!value.contains("10.24.7.9"));
        assert!(!value.contains("[::1]"));
        assert!(value.contains(REDACTED));
    }

    #[test]
    fn resource_wire_encoding_respects_the_compact_byte_budget() {
        let policy = output_policy::OutputPolicy::default();
        let empty_rows = vec![String::new(); policy.max_rows];
        let mut sized = serde_json::json!({
            "schema_version": "rocketmq-mcp.v2",
            "partial": false,
            "warnings": [],
            "data": empty_rows,
        });
        let Value::Object(object) = &mut sized else {
            unreachable!("fixture is an object");
        };
        object.insert("request_id".to_string(), Value::String("resource-test".to_string()));
        object.insert("correlation_id".to_string(), Value::String("resource-test".to_string()));
        let empty_size = serde_json::to_vec(&sized).unwrap().len();
        let row_width = (policy.max_bytes - empty_size - 2_000) / policy.max_rows;
        let rows = vec!["x".repeat(row_width); policy.max_rows];
        let input = serde_json::json!({
            "schema_version": "rocketmq-mcp.v2",
            "partial": false,
            "warnings": [],
            "data": rows,
        });
        let result = ReadResourceResult::new(vec![ResourceContents::text(
            input.to_string(),
            "rocketmq://clusters/local-dev/topics",
        )]);

        let result = process_read_resource_result(result, "resource-test", true).unwrap();
        let ResourceContents::TextResourceContents { text, .. } = &result.contents[0] else {
            unreachable!("fixture produces text");
        };
        let value: Value = serde_json::from_str(text).unwrap();
        let pretty_size = serde_json::to_string_pretty(&value).unwrap().len();

        assert!(text.len() <= policy.max_bytes);
        assert!(pretty_size > policy.max_bytes);
        assert!(!text.contains("\n  "));
    }

    #[test]
    fn resource_links_are_canonicalized_or_removed_and_text_fields_are_sanitized() {
        let valid = rmcp::model::Resource::new(
            "rocketmq://clusters/local-dev/topics/%25RETRY%25orders/config",
            "token=secret",
        )
        .with_description("source=127.0.0.1:9876");
        let unsafe_link =
            rmcp::model::Resource::new("rocketmq://clusters/local-dev/topics/token%3Dsecret/config", "unsafe");
        let result = CallToolResult::success(vec![
            ContentBlock::resource_link(valid),
            ContentBlock::resource_link(unsafe_link),
        ]);

        let result = process_call_tool_result(result, "request", true);

        assert_eq!(result.content.len(), 1);
        let link = result.content[0].as_resource_link().unwrap();
        assert_eq!(
            link.uri,
            "rocketmq://clusters/local-dev/topics/%25RETRY%25orders/config"
        );
        assert!(!link.name.contains("secret"));
        assert!(!link.description.as_deref().unwrap().contains("127.0.0.1"));
    }
}
