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

//! `rocketmq_list_topic_consumers`: the Consumer Groups of one Topic, in the shared envelope.

use super::*;

const TOOL: &str = "rocketmq_list_topic_consumers";

/// What the fake adapter answers for the Tool: two groups, read in full or in part, or no answer.
pub(super) fn answer(
    adapter: &FakeAdapter,
    args: topic_tools::ListTopicConsumersArgs,
) -> Result<QueryResult<topic_tools::ListTopicConsumersOutput>, ToolFailure> {
    if adapter.fail {
        return Err(ToolFailure::Operational(ToolExecutionError::Backend(None)));
    }
    let items = ["billing", "workers"]
        .map(|group| topic_tools::TopicConsumerGroup {
            group: group.to_string(),
        })
        .to_vec();
    let mut result = QueryResult::bypass(topic_tools::ListTopicConsumersOutput {
        cluster: args.cluster,
        topic: args.topic,
        truncated: false,
        page: crate::model::contract::Page {
            count: items.len(),
            total_count: items.len(),
            items,
            has_more: false,
            next_cursor: None,
        },
        generated_at: "1".to_string(),
    });
    if adapter.partial {
        result.partial = true;
        result.warnings = vec!["source_failures_present".to_string()];
        result.source_failures = vec![crate::model::contract::SourceFailure::new(
            crate::model::contract::QuerySource::SubscriptionGroups,
            crate::model::contract::SourceFailureCode::Timeout,
            true,
            "broker-b",
        )];
    }
    Ok(result)
}

async fn call(adapter: FakeAdapter, guard: &Guard, arguments: serde_json::Value) -> CallToolResult {
    ToolExecutor::new(adapter, guard.clone())
        .call(CallToolRequestParams::new(TOOL).with_arguments(arguments.as_object().unwrap().clone()))
        .await
        .unwrap()
}

fn adapter(fail: bool, partial: bool) -> FakeAdapter {
    FakeAdapter { fail, partial }
}

#[tokio::test]
async fn topic_consumers_are_listed_by_name_without_a_resource_link() {
    // The example `read_only` role grants the Tool.
    let guard = test_guard("read_only");
    let arguments = serde_json::json!({"cluster": "local-dev", "topic": "orders", "limit": 10});
    let result = call(adapter(false, false), &guard, arguments).await;

    assert_eq!(result.is_error, Some(false), "{}", content_text(&result));
    let structured = result.structured_content.as_ref().unwrap();
    assert_eq!(structured["schema_version"], "rocketmq-mcp.v2");
    assert_eq!(structured["cluster"], "local-dev");
    assert_eq!(structured["partial"], false);
    assert_eq!(
        structured["data"],
        serde_json::json!({
            "cluster": "local-dev",
            "topic": "orders",
            "truncated": false,
            "items": [{"group": "billing"}, {"group": "workers"}],
            "count": 2,
            "total_count": 2,
            "has_more": false,
            "next_cursor": null,
            "generated_at": "1",
        })
    );
    assert!(content_text(&result).starts_with("Topic orders on cluster local-dev returned 2 of 2 consumer groups."));
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
async fn topic_consumers_report_partial_evidence_and_a_source_that_is_down() {
    let guard = test_guard("read_only");
    let arguments = serde_json::json!({"cluster": "local-dev", "topic": "orders"});

    let result = call(adapter(false, true), &guard, arguments.clone()).await;
    let structured = result.structured_content.as_ref().unwrap();
    assert_eq!(structured["partial"], true);
    assert_eq!(structured["source_failures"][0]["source"], "subscription_groups");
    assert_eq!(structured["source_failures"][0]["logical_target"], "broker-b");
    assert_eq!(structured["data"]["items"].as_array().unwrap().len(), 2);

    let result = call(adapter(true, false), &guard, arguments).await;
    assert_eq!(result.is_error, Some(true));
    let error = serde_json::from_str::<serde_json::Value>(&content_text(&result)).unwrap();
    assert_eq!(error["code"], "source_unavailable");
}

#[tokio::test]
async fn topic_consumers_take_a_configured_cluster_and_a_topic_and_no_address() {
    let guard = test_guard("read_only");
    for (arguments, code) in [
        (serde_json::json!({"cluster": "local-dev"}), "invalid_arguments"),
        (serde_json::json!({"topic": "orders"}), "invalid_arguments"),
        (
            serde_json::json!({"cluster": "unconfigured", "topic": "orders"}),
            "cluster_not_allowed",
        ),
        (
            serde_json::json!({"cluster": "local-dev", "topic": "orders", "broker_addr": "127.0.0.1:10911"}),
            "invalid_arguments",
        ),
        (
            serde_json::json!({"cluster": "local-dev", "topic": "orders", "response_format": "concise"}),
            "invalid_arguments",
        ),
    ] {
        let result = call(adapter(false, false), &guard, arguments.clone()).await;
        assert_eq!(result.is_error, Some(true), "{arguments}");
        let error = serde_json::from_str::<serde_json::Value>(&content_text(&result)).unwrap();
        assert_eq!(error["code"], code, "{arguments}");
    }
}
