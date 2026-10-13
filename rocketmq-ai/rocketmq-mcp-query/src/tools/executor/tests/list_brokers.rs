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

//! `rocketmq_list_brokers`: the Broker rows of one cluster, in the shared envelope.

use super::*;

const TOOL: &str = "rocketmq_list_brokers";

async fn call(adapter: FakeAdapter, guard: &Guard, arguments: serde_json::Value) -> CallToolResult {
    ToolExecutor::new(adapter, guard.clone())
        .call(CallToolRequestParams::new(TOOL).with_arguments(arguments.as_object().unwrap().clone()))
        .await
        .unwrap()
}

#[tokio::test]
async fn list_brokers_returns_rows_without_addresses_or_a_resource_link() {
    // The example `read_only` role grants the Tool.
    let guard = test_guard("read_only");
    let adapter = FakeAdapter {
        fail: false,
        partial: false,
    };
    let result = call(adapter, &guard, serde_json::json!({"cluster": "local-dev"})).await;

    assert_eq!(result.is_error, Some(false), "{}", content_text(&result));
    let structured = result.structured_content.as_ref().unwrap();
    assert_eq!(structured["schema_version"], "rocketmq-mcp.v2");
    assert_eq!(structured["cluster"], "local-dev");
    assert_eq!(structured["partial"], false);
    assert_eq!(structured["data"]["cluster"], "local-dev");
    assert_eq!(structured["data"]["brokers"][0]["broker_name"], "broker-a");
    assert_eq!(structured["data"]["brokers"][0]["broker_active"], true);
    let published = serde_json::to_string(&result).unwrap();
    for private in ["broker_addr", "namesrv_addr", "127.0.0.1", "10911", "9876"] {
        assert!(!published.contains(private), "the result holds {private}");
    }
    assert!(content_text(&result).starts_with("Cluster local-dev has 1 broker rows, 0 of them inactive."));
    // The `brokers` Resource is authorized as the overview, so this Tool does not point at it.
    assert!(result
        .content
        .iter()
        .all(|content| content.as_resource_link().is_none()));

    let records = guard.audit_log().records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].tool, TOOL);
    assert_eq!(records[0].cluster.as_deref(), Some("local-dev"));
    assert_eq!(records[0].status, AuditStatus::Success);
}

#[tokio::test]
async fn list_brokers_reports_partial_evidence_and_backend_failure() {
    let guard = test_guard("read_only");
    let partial = FakeAdapter {
        fail: false,
        partial: true,
    };
    let result = call(partial, &guard, serde_json::json!({"cluster": "local-dev"})).await;
    let structured = result.structured_content.as_ref().unwrap();
    assert_eq!(structured["partial"], true);
    assert_eq!(structured["warnings"], serde_json::json!(["source_failures_present"]));
    assert_eq!(structured["source_failures"][0]["source"], "broker_runtime");
    assert_eq!(structured["source_failures"][0]["logical_target"], "broker-b");
    assert_eq!(structured["data"]["brokers"].as_array().unwrap().len(), 1);

    let failing = FakeAdapter {
        fail: true,
        partial: false,
    };
    let result = call(failing, &guard, serde_json::json!({"cluster": "local-dev"})).await;
    assert_eq!(result.is_error, Some(true));
    let error = serde_json::from_str::<serde_json::Value>(&content_text(&result)).unwrap();
    assert_eq!(error["code"], "source_unavailable");
}

#[tokio::test]
async fn list_brokers_takes_only_a_configured_cluster() {
    let guard = test_guard("read_only");
    let adapter = || FakeAdapter {
        fail: false,
        partial: false,
    };
    for (arguments, code) in [
        (serde_json::json!({}), "invalid_arguments"),
        (serde_json::json!({"cluster": "unconfigured"}), "cluster_not_allowed"),
        (
            serde_json::json!({"cluster": "local-dev", "broker_addr": "127.0.0.1:10911"}),
            "invalid_arguments",
        ),
    ] {
        let result = call(adapter(), &guard, arguments.clone()).await;
        assert_eq!(result.is_error, Some(true), "{arguments}");
        let error = serde_json::from_str::<serde_json::Value>(&content_text(&result)).unwrap();
        assert_eq!(error["code"], code, "{arguments}");
    }
}

#[test]
fn list_brokers_output_is_bounded_by_the_shared_row_policy() {
    let rows = |count: usize| {
        (0..count)
            .map(|index| cluster_tools::BrokerSummary {
                broker_name: format!("broker-{index:04}"),
                ..broker_summary()
            })
            .collect::<Vec<_>>()
    };
    let render = |count: usize| {
        let output = QueryResult::bypass(broker_tools::ListBrokersOutput {
            cluster: "local-dev".to_string(),
            brokers: rows(count),
            generated_at: "1".to_string(),
        });
        let composed = success_unlinked_result(
            ToolId::ListBrokers.descriptor(),
            "test-request",
            "local-dev".to_string(),
            "summary".to_string(),
            output,
        )
        .unwrap();
        crate::guard::sanitizer::process_tool_output(composed, true)
            .unwrap()
            .structured_content
            .unwrap()
    };

    let within = render(1_000);
    assert_eq!(within["data"]["brokers"].as_array().unwrap().len(), 1_000);
    assert_eq!(within["partial"], false);

    let beyond = render(1_001);
    assert_eq!(beyond["data"]["brokers"].as_array().unwrap().len(), 1_000);
    assert_eq!(beyond["partial"], true);
    assert!(beyond["warnings"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!("output_rows_truncated")));
}
