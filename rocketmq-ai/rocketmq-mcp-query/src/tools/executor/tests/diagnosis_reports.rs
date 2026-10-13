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

//! The server-side diagnosis Tools: each returns a diagnosis report in the shared envelope.

use super::*;

/// Every server-side diagnosis Tool, with arguments that select a target and the type of the
/// report it returns.
fn tools() -> Vec<(&'static str, serde_json::Value, &'static str)> {
    vec![
        (
            "rocketmq_diagnose_consumer_group",
            serde_json::json!({ "cluster": "local-dev", "consumer_group": "order-service" }),
            "consumer_group",
        ),
        (
            "rocketmq_diagnose_broker_health",
            serde_json::json!({ "cluster": "local-dev", "broker_name": "broker-a" }),
            "broker_health",
        ),
        (
            "rocketmq_get_cluster_health",
            serde_json::json!({ "cluster": "local-dev" }),
            "cluster_health",
        ),
        (
            "rocketmq_diagnose_message_delivery",
            serde_json::json!({
                "cluster": "local-dev",
                "topic": "orders",
                "consumer_group": "order-service",
                "message_id": "RAW-MESSAGE-ID-A",
            }),
            "message_delivery",
        ),
    ]
}

async fn call(adapter: FakeAdapter, guard: &Guard, tool: &str, arguments: serde_json::Value) -> CallToolResult {
    ToolExecutor::new(adapter, guard.clone())
        .call(CallToolRequestParams::new(tool.to_string()).with_arguments(arguments.as_object().unwrap().clone()))
        .await
        .unwrap()
}

fn error_code(result: &CallToolResult) -> String {
    let error: serde_json::Value = serde_json::from_str(&content_text(result)).unwrap();
    error["code"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn diagnosis_tools_return_a_report_in_the_shared_envelope_without_a_resource_link() {
    for (tool, arguments, report_type) in tools() {
        // The example `diagnose` role grants every diagnosis Tool.
        let guard = test_guard("diagnose");
        let adapter = FakeAdapter {
            fail: false,
            partial: false,
        };

        let result = call(adapter, &guard, tool, arguments).await;

        assert_eq!(result.is_error, Some(false), "{tool}: {}", content_text(&result));
        let structured = result.structured_content.as_ref().unwrap();
        assert_eq!(structured["schema_version"], "rocketmq-mcp.v2", "{tool}");
        assert_eq!(structured["cluster"], "local-dev", "{tool}");
        assert_eq!(structured["partial"], false, "{tool}");
        assert_eq!(structured["data"]["report_type"], report_type, "{tool}");
        assert!(
            content_text(&result).starts_with(&format!("The {report_type} diagnosis found nothing to report.")),
            "{tool}: the text block leads with the summary of the report"
        );
        assert!(
            result
                .content
                .iter()
                .all(|content| content.as_resource_link().is_none()),
            "{tool}"
        );
        let records = guard.audit_log().records();
        assert_eq!(records.len(), 1, "{tool}");
        assert_eq!(records[0].tool, tool);
        assert_eq!(records[0].cluster.as_deref(), Some("local-dev"), "{tool}");
        assert_eq!(records[0].status, AuditStatus::Success, "{tool}");
    }
}

#[tokio::test]
async fn diagnosis_tools_report_partial_evidence_and_backend_failure() {
    for (tool, arguments, _) in tools() {
        let guard = test_guard("diagnose");
        let partial = FakeAdapter {
            fail: false,
            partial: true,
        };
        let result = call(partial, &guard, tool, arguments.clone()).await;
        assert_eq!(result.is_error, Some(false), "{tool}");
        let structured = result.structured_content.as_ref().unwrap();
        assert_eq!(structured["partial"], true, "{tool}");
        assert_eq!(
            structured["warnings"],
            serde_json::json!(["diagnosis_evidence_incomplete"]),
            "{tool}"
        );

        let failing = FakeAdapter {
            fail: true,
            partial: false,
        };
        let result = call(failing, &guard, tool, arguments).await;
        assert_eq!(result.is_error, Some(true), "{tool}");
        assert_eq!(error_code(&result), "source_unavailable", "{tool}");
    }
}

#[tokio::test]
async fn diagnosis_tools_need_the_diagnose_scope_and_take_only_declared_arguments() {
    for (tool, arguments, _) in tools() {
        let adapter = || FakeAdapter {
            fail: false,
            partial: false,
        };

        let denied = call(adapter(), &test_guard("read_only"), tool, arguments.clone()).await;
        assert_eq!(denied.is_error, Some(true), "{tool}");
        assert_eq!(error_code(&denied), "unauthorized_scope", "{tool}");

        let guard = test_guard("diagnose");
        let mut undeclared = arguments.clone();
        undeclared["namesrv_addr"] = serde_json::json!("127.0.0.1:9876");
        let result = call(adapter(), &guard, tool, undeclared).await;
        assert_eq!(result.is_error, Some(true), "{tool}");
        assert_eq!(error_code(&result), "invalid_arguments", "{tool}");

        let mut without_cluster = arguments;
        without_cluster.as_object_mut().unwrap().remove("cluster");
        let result = call(adapter(), &guard, tool, without_cluster).await;
        assert_eq!(result.is_error, Some(true), "{tool}");
        assert_eq!(error_code(&result), "invalid_arguments", "{tool}");
    }
}

/// A Guard whose only role, `investigator`, may diagnose deliveries, and may look up a message
/// when `may_look_up_messages` is set.
fn delivery_guard(may_look_up_messages: bool) -> (Guard, tempfile::TempDir) {
    let tools = if may_look_up_messages {
        r#""rocketmq_diagnose_message_delivery", "rocketmq_get_message_metadata""#
    } else {
        r#""rocketmq_diagnose_message_delivery""#
    };
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

async fn diagnose_delivery(guard: &Guard, message_id: Option<&str>) -> CallToolResult {
    let mut arguments = serde_json::json!({
        "cluster": "local-dev",
        "topic": "orders",
        "consumer_group": "order-service",
    });
    if let Some(message_id) = message_id {
        arguments["message_id"] = serde_json::json!(message_id);
    }
    let investigator = RequestContext {
        principal: crate::guard::context::Principal {
            id: "investigator".to_string(),
            tenant: None,
            roles: ["investigator".to_string()].into_iter().collect(),
            scopes: ["rocketmq:read".to_string(), "rocketmq:diagnose".to_string()]
                .into_iter()
                .collect(),
            allowed_clusters: None,
        },
        client: None,
    };
    ToolExecutor::new(
        FakeAdapter {
            fail: false,
            partial: false,
        },
        guard.clone(),
    )
    .with_request_context(investigator)
    .call(
        CallToolRequestParams::new("rocketmq_diagnose_message_delivery")
            .with_arguments(arguments.as_object().unwrap().clone()),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn naming_a_message_takes_the_permission_to_look_up_messages() {
    // The role may diagnose a delivery, but not look up a message.
    let (guard, _permissions) = delivery_guard(false);
    let path_only = diagnose_delivery(&guard, None).await;
    assert_eq!(path_only.is_error, Some(false), "{}", content_text(&path_only));

    let denied = diagnose_delivery(&guard, Some("RAW-MESSAGE-ID-A")).await;
    assert_eq!(denied.is_error, Some(true));
    let error: serde_json::Value = serde_json::from_str(&content_text(&denied)).unwrap();
    assert_eq!(error["code"], "permission_denied");
    // The error names the argument that needs the permission, and never its value.
    assert_eq!(
        error["violations"],
        serde_json::json!([{ "path": "/message_id", "constraint": "other" }])
    );
    assert!(!content_text(&denied).contains("RAW-MESSAGE-ID-A"));
    let records = guard.audit_log().records();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].status, AuditStatus::Success);
    assert_eq!(records[1].status, AuditStatus::Failure);
    assert!(!serde_json::to_string(&records).unwrap().contains("RAW-MESSAGE-ID-A"));

    // With that permission, the same call is served.
    let (guard, _permissions) = delivery_guard(true);
    let served = diagnose_delivery(&guard, Some("RAW-MESSAGE-ID-A")).await;
    assert_eq!(served.is_error, Some(false), "{}", content_text(&served));
}
