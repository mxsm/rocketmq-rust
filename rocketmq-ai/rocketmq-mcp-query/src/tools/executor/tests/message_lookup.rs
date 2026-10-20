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

//! `rocketmq_query_message_trace` and `rocketmq_find_messages_by_key`: the two Tools that look
//! messages up through what Brokers index under a key.

use super::*;

const TRACE: &str = "rocketmq_query_message_trace";
const SEARCH: &str = "rocketmq_find_messages_by_key";

/// What the fake adapter answers for the trace Tool: one publish record, or a failing read.
pub(super) fn trace(
    adapter: &FakeAdapter,
    args: message_tools::QueryMessageTraceArgs,
) -> Result<QueryResult<message_tools::QueryMessageTraceOutput>, ToolFailure> {
    if adapter.fail {
        return Err(ToolFailure::Operational(ToolExecutionError::Backend(None)));
    }
    let mut result = QueryResult::bypass(message_tools::QueryMessageTraceOutput {
        cluster: args.cluster,
        unique_message_alias: "unique-message-0123456789abcdef0123456789abcdef".to_string(),
        message_alias: Some("message-0123456789abcdef0123456789abcdef".to_string()),
        trace_topic: args.trace_topic.unwrap_or_else(|| "RMQ_SYS_TRACE_TOPIC".to_string()),
        trace_topic_found: true,
        redacted: true,
        records: vec![message_tools::MessageTraceRecord {
            step: message_tools::MessageTraceStep::Publish,
            at: Some("2023-11-14T22:13:20.000Z".to_string()),
            group: Some("producers".to_string()),
            cost_time_ms: 12,
            success: true,
            topic: Some("orders".to_string()),
            retry_times: 0,
            body_length: 128,
            from_transaction_check: false,
            store_broker_name: Some("broker-a".to_string()),
            client_host_alias: None,
            tags: None,
            key_aliases: Vec::new(),
        }],
        truncated: false,
        queried_broker_count: 2,
        unanswered_brokers: Vec::new(),
        generated_at: "1".to_string(),
    });
    if adapter.partial {
        result.partial = true;
        result.warnings = vec!["message_index_brokers_unanswered".to_string()];
        result.data.unanswered_brokers = vec!["broker-b".to_string()];
    }
    Ok(result)
}

/// What the fake adapter answers for the search Tool: one match, or a failing read.
pub(super) fn matches(
    adapter: &FakeAdapter,
    args: message_tools::FindMessagesByKeyArgs,
) -> Result<QueryResult<message_tools::FindMessagesByKeyOutput>, ToolFailure> {
    if adapter.fail {
        return Err(ToolFailure::Operational(ToolExecutionError::Backend(None)));
    }
    Ok(QueryResult::bypass(message_tools::FindMessagesByKeyOutput {
        cluster: args.cluster,
        topic: args.topic,
        window_begin_at: "2023-11-14T21:13:20.000Z".to_string(),
        window_end_at: "2023-11-14T22:13:20.000Z".to_string(),
        messages: vec![message_tools::MessageKeyMatch {
            message_alias: "message-0123456789abcdef0123456789abcdef".to_string(),
            unique_message_alias: None,
            broker_name: "broker-a".to_string(),
            born_at: None,
            stored_at: Some("2023-11-14T22:13:20.050Z".to_string()),
            queue_id: 3,
            queue_offset: 42,
            store_size: 128,
            reconsume_times: 0,
            sys_flag: 0,
            flag: 0,
            prepared_transaction_offset: 0,
        }],
        truncated: false,
        queried_broker_count: 2,
        unanswered_brokers: Vec::new(),
        generated_at: "1".to_string(),
    }))
}

async fn call(adapter: FakeAdapter, guard: &Guard, tool: &str, arguments: serde_json::Value) -> CallToolResult {
    ToolExecutor::new(adapter, guard.clone())
        .call(CallToolRequestParams::new(tool.to_string()).with_arguments(arguments.as_object().unwrap().clone()))
        .await
        .unwrap()
}

fn adapter(fail: bool, partial: bool) -> FakeAdapter {
    FakeAdapter { fail, partial }
}

fn error(result: &CallToolResult) -> serde_json::Value {
    serde_json::from_str(&content_text(result)).unwrap()
}

fn trace_arguments() -> serde_json::Value {
    serde_json::json!({"cluster": "local-dev", "message_id": "7F0000010000B4AAC21828A0B9B50000"})
}

fn search_arguments() -> serde_json::Value {
    serde_json::json!({"cluster": "local-dev", "topic": "orders", "key": "order-42"})
}

#[tokio::test]
async fn a_trace_is_a_read_that_never_repeats_the_identifier_it_was_asked_for() {
    // The example `read_only` role grants the Tool and the lookup of messages.
    let guard = test_guard("read_only");
    let result = call(adapter(false, false), &guard, TRACE, trace_arguments()).await;

    assert_eq!(result.is_error, Some(false), "{}", content_text(&result));
    let data = &result.structured_content.as_ref().unwrap()["data"];
    assert_eq!(data["trace_topic"], "RMQ_SYS_TRACE_TOPIC");
    assert_eq!(data["redacted"], true);
    assert_eq!(data["records"][0]["step"], "publish");
    assert_eq!(data["records"][0]["body_length"], 128);
    assert_eq!(data["records"][0]["client_host_alias"], serde_json::Value::Null);
    assert!(content_text(&result).starts_with(
        "Message unique-message-0123456789abcdef0123456789abcdef on cluster local-dev has 1 trace records."
    ));
    assert!(!serde_json::to_string(&result)
        .unwrap()
        .contains("7F0000010000B4AAC21828A0B9B50000"));
    assert!(result
        .content
        .iter()
        .all(|content| content.as_resource_link().is_none()));

    let partial = call(adapter(false, true), &guard, TRACE, trace_arguments()).await;
    let structured = partial.structured_content.as_ref().unwrap();
    assert_eq!(structured["partial"], true);
    assert_eq!(
        structured["warnings"],
        serde_json::json!(["message_index_brokers_unanswered"])
    );
    assert_eq!(
        structured["data"]["unanswered_brokers"],
        serde_json::json!(["broker-b"])
    );

    let down = call(adapter(true, false), &guard, TRACE, trace_arguments()).await;
    assert_eq!(error(&down)["code"], "source_unavailable");
}

#[tokio::test]
async fn a_key_search_needs_its_own_scope_and_never_repeats_the_key() {
    // The local `operator` profile holds the scope, and the example `operator` role the Tool.
    let guard = test_guard("operator");
    let result = call(adapter(false, false), &guard, SEARCH, search_arguments()).await;

    assert_eq!(result.is_error, Some(false), "{}", content_text(&result));
    let data = &result.structured_content.as_ref().unwrap()["data"];
    assert_eq!(data["topic"], "orders");
    assert_eq!(data["messages"][0]["broker_name"], "broker-a");
    assert_eq!(data["messages"][0]["queue_offset"], 42);
    assert!(content_text(&result).starts_with(
        "Topic orders on cluster local-dev returned 1 messages that carry the key, stored between \
         2023-11-14T21:13:20.000Z and 2023-11-14T22:13:20.000Z."
    ));
    assert!(!serde_json::to_string(&result).unwrap().contains("order-42"));

    // Reading and diagnosing are not searching.
    for profile in ["read_only", "diagnose"] {
        let denied = call(adapter(false, false), &test_guard(profile), SEARCH, search_arguments()).await;
        assert_eq!(error(&denied)["code"], "unauthorized_scope", "{profile}");
    }

    // Two searches for two keys leave the same audit hash: the key is not what is recorded.
    let other_key = serde_json::json!({"cluster": "local-dev", "topic": "orders", "key": "order-43"});
    call(adapter(false, false), &guard, SEARCH, other_key).await;
    let records = guard.audit_log().records();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].arguments_hash, records[1].arguments_hash);
    assert_eq!(records[0].risk_level, crate::guard::RiskLevel::MessageSearch);
    let audit = serde_json::to_string(&records).unwrap();
    assert!(!audit.contains("order-42") && !audit.contains("order-43"));
}

/// A Guard whose only role, `investigator`, holds exactly `tools`.
fn guard_granting(tools: &str) -> (Guard, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let permissions = directory.path().join("permissions.toml");
    std::fs::write(
        &permissions,
        format!("[roles.investigator]\nallowed_clusters = [\"*\"]\nallow_tools = [{tools}]\ndeny_tools = [\"*\"]\n"),
    )
    .unwrap();
    let guard = test_guard_with_permissions("investigator", false, permissions.to_string_lossy().into_owned());
    (guard, directory)
}

async fn call_as_investigator(guard: &Guard, tool: &str, arguments: serde_json::Value) -> CallToolResult {
    let investigator = RequestContext {
        principal: crate::guard::context::Principal {
            id: "investigator".to_string(),
            tenant: None,
            roles: ["investigator".to_string()].into_iter().collect(),
            scopes: ["rocketmq:read", "rocketmq:message-search"]
                .map(str::to_string)
                .into_iter()
                .collect(),
            allowed_clusters: None,
        },
        client: None,
    };
    ToolExecutor::new(adapter(false, false), guard.clone())
        .with_request_context(investigator)
        .call(CallToolRequestParams::new(tool.to_string()).with_arguments(arguments.as_object().unwrap().clone()))
        .await
        .unwrap()
}

#[tokio::test]
async fn looking_messages_up_takes_the_permission_to_read_message_metadata() {
    for (tool, arguments, argument) in [
        (TRACE, trace_arguments(), "/message_id"),
        (SEARCH, search_arguments(), "/key"),
    ] {
        // The role holds the Tool, and not the lookup of messages.
        let (guard, _permissions) = guard_granting(&format!("\"{tool}\""));
        let denied = call_as_investigator(&guard, tool, arguments.clone()).await;
        assert_eq!(denied.is_error, Some(true), "{tool}");
        let denied = error(&denied);
        assert_eq!(denied["code"], "permission_denied", "{tool}");
        assert_eq!(
            denied["violations"],
            serde_json::json!([{"path": argument, "constraint": "other"}]),
            "{tool}"
        );
        let records = guard.audit_log().records();
        assert_eq!(records.len(), 1, "{tool}");
        assert_eq!(records[0].status, AuditStatus::Failure, "{tool}");

        let (guard, _permissions) = guard_granting(&format!("\"{tool}\", \"rocketmq_get_message_metadata\""));
        let allowed = call_as_investigator(&guard, tool, arguments).await;
        assert_eq!(allowed.is_error, Some(false), "{tool}: {}", content_text(&allowed));
    }
}

#[tokio::test]
async fn neither_lookup_takes_an_address_a_pattern_or_an_unbounded_window() {
    let guard = test_guard("operator");
    for (tool, base, change, path, constraint) in [
        (
            TRACE,
            trace_arguments(),
            serde_json::json!({"message_id": ""}),
            "/message_id",
            "min_length",
        ),
        (
            TRACE,
            trace_arguments(),
            serde_json::json!({"broker_addr": "10.0.0.1:10911"}),
            "/*",
            "additional_property",
        ),
        (
            TRACE,
            trace_arguments(),
            serde_json::json!({"include_body": true}),
            "/*",
            "additional_property",
        ),
        (
            SEARCH,
            search_arguments(),
            serde_json::json!({"key": "k".repeat(256)}),
            "/key",
            "max_length",
        ),
        (
            SEARCH,
            search_arguments(),
            serde_json::json!({"window_minutes": 1441}),
            "/window_minutes",
            "maximum",
        ),
        (
            SEARCH,
            search_arguments(),
            serde_json::json!({"window_minutes": 0}),
            "/window_minutes",
            "minimum",
        ),
        (
            SEARCH,
            search_arguments(),
            serde_json::json!({"limit": 500}),
            "/*",
            "additional_property",
        ),
        (
            SEARCH,
            search_arguments(),
            serde_json::json!({"begin_at": "2020-01-01T00:00:00Z"}),
            "/*",
            "additional_property",
        ),
    ] {
        let mut arguments = base;
        arguments
            .as_object_mut()
            .unwrap()
            .extend(change.as_object().unwrap().clone());
        let result = call(adapter(false, false), &guard, tool, arguments.clone()).await;
        assert_eq!(result.is_error, Some(true), "{tool} {change}");
        let rejected = error(&result);
        assert_eq!(rejected["code"], "invalid_arguments", "{tool} {change}");
        assert_eq!(
            rejected["violations"],
            serde_json::json!([{"path": path, "constraint": constraint}]),
            "{tool} {change}"
        );
    }
}
