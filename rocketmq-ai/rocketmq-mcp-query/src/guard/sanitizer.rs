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

use crate::tools::executor::ToolRejection;
use std::sync::LazyLock;

use regex::Regex;
use rmcp::model::CallToolResult;
use rmcp::model::ContentBlock;
use rmcp::model::ReadResourceResult;
use rmcp::model::Resource;
use rmcp::model::ResourceContents;
use rmcp::ErrorData;
use serde_json::Value;

use crate::tools::catalog::ToolId;
use crate::tools::executor::ToolExecutionError;
use crate::tools::executor::ToolFailure;
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

/// A successful Tool result as its Tool composed it, before the shared output processing.
pub(crate) struct ToolOutput {
    pub(crate) tool: ToolId,
    /// One sentence for a reader, published as the first text block.
    pub(crate) summary: String,
    /// The response envelope, published as structured content and as the second text block.
    pub(crate) structured: Value,
    /// The Resource that holds the same observation, when the Tool has one.
    pub(crate) link: Option<Resource>,
}

/// Applies the shared output processing to a successful Tool result and publishes it.
///
/// The structured value is sanitized and held to the output policy once. The text block that
/// mirrors it is written from the processed value, so the two cannot differ, and no pattern is
/// ever replaced inside serialized JSON.
///
/// # Errors
///
/// Fails when the content exceeds the output policy, or when the processed content does not
/// match the output schema its Tool publishes.
pub(crate) fn process_tool_output(output: ToolOutput, sanitize_output: bool) -> Result<CallToolResult, ToolFailure> {
    let schema = output.tool.validators().output();
    publish_tool_output(output, sanitize_output, |structured| {
        schema.is_some_and(|validator| validator.is_valid(structured))
    })
}

fn publish_tool_output(
    output: ToolOutput,
    sanitize_output: bool,
    matches_schema: impl FnOnce(&Value) -> bool,
) -> Result<CallToolResult, ToolFailure> {
    let ToolOutput {
        tool: _,
        mut summary,
        mut structured,
        link,
    } = output;
    if sanitize_output {
        summary = sanitize_text(&summary);
        sanitize_value(&mut structured);
    }
    let (structured, json_text) = output_policy::apply_rendered(structured)?;
    // Content that its own published schema rejects is a defect of this server, not of the call.
    if !matches_schema(&structured) {
        return Err(ToolFailure::Operational(ToolExecutionError::Internal(None)));
    }
    let mut content = vec![ContentBlock::text(summary), ContentBlock::text(json_text)];
    if let Some(mut link) = link {
        if sanitize_resource_link(&mut link, sanitize_output) {
            content.push(ContentBlock::resource_link(link));
        }
    }
    let mut result = CallToolResult::success(content);
    result.structured_content = Some(structured);
    Ok(result)
}

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
            let value: Value =
                serde_json::from_str(text).map_err(|source| crate::McpError::from_source(source).into_error_data())?;
            *text = processed_resource_text(value, request_id, sanitize_output)?;
        }
    }
    Ok(result)
}

/// Applies the shared output processing to a Resource that is still a value and publishes it.
///
/// The content is written as text here and nowhere before, so a read serializes it once.
///
/// # Errors
///
/// Fails when the content exceeds the output policy or cannot be written as JSON.
pub fn process_resource_document(
    document: crate::resources::ResourceDocument,
    request_id: &str,
    sanitize_output: bool,
) -> Result<ReadResourceResult, ErrorData> {
    let text = processed_resource_text(document.value, request_id, sanitize_output)?;
    Ok(ReadResourceResult::new(vec![ResourceContents::text(
        text,
        document.uri,
    )
    .with_mime_type(crate::resources::uri::JSON_MIME_TYPE)]))
}

fn processed_resource_text(mut value: Value, request_id: &str, sanitize_output: bool) -> Result<String, ErrorData> {
    if let Value::Object(object) = &mut value {
        object.insert("request_id".to_string(), Value::String(request_id.to_string()));
        object.insert("correlation_id".to_string(), Value::String(request_id.to_string()));
    }
    if sanitize_output {
        sanitize_value(&mut value);
    }
    let (_, text) = output_policy::apply_rendered(value).map_err(|error| output_policy_error(error, request_id))?;
    Ok(text)
}

fn output_policy_error(error: ToolFailure, request_id: &str) -> ErrorData {
    match error {
        ToolFailure::Rejected(ToolRejection::OutputTooLarge {
            actual_bytes,
            max_bytes,
        }) => ErrorData::internal_error(
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

    fn composed(data: Value) -> ToolOutput {
        ToolOutput {
            tool: ToolId::ListClusters,
            summary: "Read through token=abc at 10.24.7.9:9876.".to_string(),
            structured: serde_json::json!({ "partial": false, "warnings": [], "data": data }),
            link: None,
        }
    }

    fn block_text(result: &CallToolResult, index: usize) -> &str {
        &result.content[index].as_text().expect("a text block").text
    }

    #[test]
    fn text_block_is_derived_from_the_sanitized_structured_value() {
        // Replacing a pattern inside the JSON text would write `"session_token":[REDACTED]`,
        // which is not JSON, and would leave the structured value with the number.
        let data = serde_json::json!({
            "session_token": 7,
            "note": "source=10.24.7.9:9876 password: \"quoted\"",
            "broker_name": "broker-a",
        });

        let published = publish_tool_output(composed(data.clone()), true, |_| true).unwrap();

        let structured = published.structured_content.clone().unwrap();
        assert_eq!(structured["data"]["session_token"], REDACTED);
        assert_eq!(structured["data"]["broker_name"], "broker-a");
        let mirror = block_text(&published, 1);
        assert_eq!(
            serde_json::from_str::<Value>(mirror).expect("the mirror is JSON"),
            structured
        );
        assert!(!mirror.contains("10.24.7.9") && !mirror.contains("quoted"));
        assert_eq!(
            block_text(&published, 0),
            "Read through token=[REDACTED] at [REDACTED]."
        );

        // Without sanitization the same holds: one value, published twice.
        let raw = publish_tool_output(composed(data.clone()), false, |_| true).unwrap();
        let structured = raw.structured_content.clone().unwrap();
        assert_eq!(structured["data"], data);
        assert_eq!(serde_json::from_str::<Value>(block_text(&raw, 1)).unwrap(), structured);
        assert_eq!(block_text(&raw, 0), "Read through token=abc at 10.24.7.9:9876.");
    }

    #[test]
    fn a_tool_output_is_held_to_the_policy_once_and_published_as_bounded() {
        let rows = (0..1_001).collect::<Vec<u32>>();
        let published = publish_tool_output(composed(serde_json::json!({ "rows": rows })), true, |_| true).unwrap();

        let structured = published.structured_content.clone().unwrap();
        assert_eq!(structured["data"]["rows"].as_array().unwrap().len(), 1_000);
        assert_eq!(structured["partial"], true);
        assert_eq!(structured["warnings"], serde_json::json!(["output_rows_truncated"]));
        // The mirror is the bounded value, not the one the Tool composed.
        assert_eq!(
            serde_json::from_str::<Value>(block_text(&published, 1)).unwrap(),
            structured
        );

        let oversized = composed(serde_json::json!({ "text": "x".repeat(1024 * 1024) }));
        let refused = publish_tool_output(oversized, true, |_| true).unwrap_err();
        assert_eq!(refused.code(), "output_too_large");
    }

    #[test]
    fn a_tool_output_that_its_schema_rejects_is_not_published() {
        // The schema is asked about the processed value, the one a client would receive.
        let seen = std::cell::RefCell::new(Value::Null);
        let refused = publish_tool_output(
            composed(serde_json::json!({ "api_token": "abc" })),
            true,
            |structured| {
                seen.replace(structured.clone());
                false
            },
        )
        .unwrap_err();

        assert_eq!(refused.code(), "internal_error");
        assert_eq!(seen.borrow()["data"]["api_token"], REDACTED);

        // The real schema of a Tool refuses content that is not its envelope.
        let refused = process_tool_output(composed(serde_json::json!({ "unexpected": true })), true).unwrap_err();
        assert_eq!(refused.code(), "internal_error");
    }

    #[test]
    fn an_unsafe_resource_link_is_dropped_and_a_safe_one_is_published_last() {
        let link = |uri: &str| Resource::new(uri.to_string(), "overview".to_string());
        let with_link = |uri: &str| ToolOutput {
            link: Some(link(uri)),
            ..composed(serde_json::json!({}))
        };

        let published =
            publish_tool_output(with_link("rocketmq://clusters/local-dev/overview"), true, |_| true).unwrap();
        assert_eq!(published.content.len(), 3);
        assert_eq!(
            published.content[2].as_resource_link().expect("the link is last").uri,
            "rocketmq://clusters/local-dev/overview"
        );

        let dropped = publish_tool_output(with_link("https://example.invalid/overview"), true, |_| true).unwrap();
        assert_eq!(dropped.content.len(), 2);
    }

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
    fn a_resource_value_is_published_like_the_text_it_used_to_pass_through() {
        let value = serde_json::json!({
            "schema_version": "rocketmq-mcp.v2",
            "resource": "rocketmq://clusters/local-dev/brokers",
            "partial": true,
            "warnings": ["source=10.0.0.7:10911 timed out"],
            "data": {
                "zeta": [1, 2.5, -3, 18446744073709551615u64],
                "alpha": {"secret_key": "hidden", "note": "token=abc"},
                "nothing": null,
                "text": "plain \"quoted\" \u{4e2d}\u{6587}\n",
            },
        });
        let uri = "rocketmq://clusters/local-dev/brokers";
        for sanitize_output in [true, false] {
            let document = crate::resources::ResourceDocument {
                uri: uri.to_string(),
                value: value.clone(),
            };
            let through_text = process_read_resource_result(
                document.clone().into_read_result().unwrap(),
                "resource-test",
                sanitize_output,
            )
            .unwrap();
            let direct = process_resource_document(document, "resource-test", sanitize_output).unwrap();
            assert_eq!(
                serde_json::to_value(&direct).unwrap(),
                serde_json::to_value(&through_text).unwrap(),
                "sanitize_output={sanitize_output}"
            );
            let ResourceContents::TextResourceContents { text, .. } = &direct.contents[0] else {
                unreachable!("a document is published as text");
            };
            assert!(text.contains("\"correlation_id\":\"resource-test\""));
            assert_eq!(text.contains("hidden"), !sanitize_output);
            assert!(!text.contains("\n  "));
        }

        // The output policy bounds this path as well: rows are cut, and bytes are refused.
        let policy = output_policy::OutputPolicy::default();
        let rows = vec!["x".to_string(); policy.max_rows + 1];
        let truncated = crate::resources::ResourceDocument {
            uri: uri.to_string(),
            value: serde_json::json!({"partial": false, "warnings": [], "data": rows}),
        };
        let truncated = process_resource_document(truncated, "resource-test", true).unwrap();
        let ResourceContents::TextResourceContents { text, .. } = &truncated.contents[0] else {
            unreachable!("a document is published as text");
        };
        let published: Value = serde_json::from_str(text).unwrap();
        assert_eq!(published["data"].as_array().unwrap().len(), policy.max_rows);
        assert_eq!(published["partial"], true);

        let oversized = crate::resources::ResourceDocument {
            uri: uri.to_string(),
            value: serde_json::json!({"data": "x".repeat(policy.max_bytes)}),
        };
        let refused = process_resource_document(oversized, "resource-test", true).unwrap_err();
        assert_eq!(refused.data.unwrap()["code"], "output_too_large");
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
