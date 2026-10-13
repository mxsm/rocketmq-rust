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

//! `rocketmq_get_dlq_summary`: counts and times of the dead letters of one Consumer Group.

use super::*;

const TOOL: &str = "rocketmq_get_dlq_summary";

/// What the fake adapter answers for the Tool: twelve dead letters on one Broker.
pub(super) fn answer(
    adapter: &FakeAdapter,
    args: consumer_tools::GetDlqSummaryArgs,
) -> Result<QueryResult<consumer_tools::GetDlqSummaryOutput>, ToolFailure> {
    if adapter.fail {
        return Err(ToolFailure::not_found(NotFoundEntity::ConsumerGroup));
    }
    let mut result = QueryResult::bypass(consumer_tools::GetDlqSummaryOutput {
        cluster: args.cluster,
        dlq_topic: format!("%DLQ%{}", args.consumer_group),
        consumer_group: args.consumer_group,
        exists: true,
        queue_count: 1,
        message_count: 12,
        last_message_at: Some("2026-10-01T00:00:05.000Z".to_string()),
        last_message_age_ms: Some(60_000),
        brokers: vec![consumer_tools::DlqBrokerSummary {
            broker_name: "broker-a".to_string(),
            queue_count: 1,
            message_count: 12,
            last_message_at: Some("2026-10-01T00:00:05.000Z".to_string()),
        }],
        truncated: false,
        generated_at: "1".to_string(),
    });
    if adapter.partial {
        result.partial = true;
        result.warnings = vec!["source_failures_present".to_string()];
        result.source_failures = vec![crate::model::contract::SourceFailure::new(
            crate::model::contract::QuerySource::TopicStats,
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
async fn the_summary_holds_counts_and_times_and_nothing_of_a_message() {
    // The example `read_only` role grants the Tool.
    let guard = test_guard("read_only");
    let arguments = serde_json::json!({"cluster": "local-dev", "consumer_group": "workers"});
    let result = call(adapter(false, false), &guard, arguments).await;

    assert_eq!(result.is_error, Some(false), "{}", content_text(&result));
    let data = &result.structured_content.as_ref().unwrap()["data"];
    assert_eq!(data["dlq_topic"], "%DLQ%workers");
    assert_eq!(data["exists"], true);
    assert_eq!(data["message_count"], 12);
    assert_eq!(data["brokers"][0]["broker_name"], "broker-a");
    // The members of the result are the ones a count needs: none can carry a message.
    let members = |value: &serde_json::Value| value.as_object().unwrap().keys().cloned().collect::<Vec<_>>();
    assert_eq!(
        members(data),
        [
            "brokers",
            "cluster",
            "consumer_group",
            "dlq_topic",
            "exists",
            "generated_at",
            "last_message_age_ms",
            "last_message_at",
            "message_count",
            "queue_count",
            "truncated",
        ]
    );
    assert_eq!(
        members(&data["brokers"][0]),
        ["broker_name", "last_message_at", "message_count", "queue_count"]
    );
    assert!(content_text(&result)
        .starts_with("Consumer group workers on cluster local-dev holds 12 dead letters in 1 queues on 1 Brokers."));
    assert!(result
        .content
        .iter()
        .all(|content| content.as_resource_link().is_none()));
    assert_eq!(guard.audit_log().records()[0].status, AuditStatus::Success);
}

#[tokio::test]
async fn the_summary_reports_partial_statistics_and_a_name_that_is_no_group() {
    let guard = test_guard("read_only");
    let arguments = serde_json::json!({"cluster": "local-dev", "consumer_group": "workers"});

    let result = call(adapter(false, true), &guard, arguments.clone()).await;
    let structured = result.structured_content.as_ref().unwrap();
    assert_eq!(structured["partial"], true);
    assert_eq!(structured["source_failures"][0]["source"], "topic_stats");

    let result = call(adapter(true, false), &guard, arguments).await;
    assert_eq!(result.is_error, Some(true));
    let error = serde_json::from_str::<serde_json::Value>(&content_text(&result)).unwrap();
    assert_eq!(error["code"], "not_found");
    assert_eq!(error["entity"], "consumer_group");
}

#[tokio::test]
async fn the_summary_takes_a_cluster_and_a_group_and_nothing_that_selects_a_message() {
    let guard = test_guard("read_only");
    for (arguments, code) in [
        (serde_json::json!({"cluster": "local-dev"}), "invalid_arguments"),
        (
            serde_json::json!({"cluster": "unconfigured", "consumer_group": "workers"}),
            "cluster_not_allowed",
        ),
        (
            serde_json::json!({"cluster": "local-dev", "consumer_group": "workers", "message_id": "A"}),
            "invalid_arguments",
        ),
        (
            serde_json::json!({"cluster": "local-dev", "consumer_group": "workers", "limit": 10}),
            "invalid_arguments",
        ),
    ] {
        let result = call(adapter(false, false), &guard, arguments.clone()).await;
        assert_eq!(result.is_error, Some(true), "{arguments}");
        let error = serde_json::from_str::<serde_json::Value>(&content_text(&result)).unwrap();
        assert_eq!(error["code"], code, "{arguments}");
    }
}
