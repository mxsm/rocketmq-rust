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

//! `rocketmq_get_lite_broker_info`: what one Broker holds for Lite Topics.

use super::*;

const TOOL: &str = "rocketmq_get_lite_broker_info";

/// What the fake adapter answers for the Tool: one Lite Topic with one bound group.
pub(super) fn answer(
    adapter: &FakeAdapter,
    args: broker_tools::GetLiteBrokerInfoArgs,
) -> Result<QueryResult<broker_tools::GetLiteBrokerInfoOutput>, ToolFailure> {
    if adapter.fail {
        return Err(ToolFailure::not_found(NotFoundEntity::Broker));
    }
    Ok(QueryResult::bypass(broker_tools::GetLiteBrokerInfoOutput {
        cluster: args.cluster,
        broker_name: args.broker_name,
        store_type: Some("RocksDB".to_string()),
        max_lmq_count: 1_000,
        current_lmq_count: 42,
        lite_subscription_count: 7,
        order_info_count: 3,
        consume_queue_count: 50,
        offset_count: 60,
        event_count: 5,
        lite_topic_count: 1,
        lite_topics: vec![broker_tools::LiteTopicSummary {
            topic: "lite-orders".to_string(),
            expiration_minutes: 30,
            bound_group_count: 1,
            bound_groups: vec!["workers".to_string()],
        }],
        truncated: false,
        generated_at: "1".to_string(),
    }))
}

async fn call(fail: bool, guard: &Guard, arguments: serde_json::Value) -> CallToolResult {
    ToolExecutor::new(FakeAdapter { fail, partial: false }, guard.clone())
        .call(CallToolRequestParams::new(TOOL).with_arguments(arguments.as_object().unwrap().clone()))
        .await
        .unwrap()
}

#[tokio::test]
async fn the_lite_state_of_a_broker_is_returned_by_logical_names() {
    // The example `read_only` role grants the Tool.
    let guard = test_guard("read_only");
    let arguments = serde_json::json!({"cluster": "local-dev", "broker_name": "broker-a"});
    let result = call(false, &guard, arguments).await;

    assert_eq!(result.is_error, Some(false), "{}", content_text(&result));
    let data = &result.structured_content.as_ref().unwrap()["data"];
    assert_eq!(data["broker_name"], "broker-a");
    assert_eq!(data["store_type"], "RocksDB");
    assert_eq!(data["max_lmq_count"], 1_000);
    assert_eq!(data["current_lmq_count"], 42);
    assert_eq!(data["lite_topics"][0]["topic"], "lite-orders");
    assert_eq!(data["lite_topics"][0]["bound_groups"], serde_json::json!(["workers"]));
    assert!(content_text(&result).starts_with(
        "Broker broker-a on cluster local-dev holds 42 of at most 1000 Lite Topic queues and knows 1 Lite Topics."
    ));
    assert!(result
        .content
        .iter()
        .all(|content| content.as_resource_link().is_none()));
    assert_eq!(guard.audit_log().records()[0].status, AuditStatus::Success);
}

#[tokio::test]
async fn an_unknown_broker_is_not_found_and_an_address_is_never_taken() {
    let guard = test_guard("read_only");
    let result = call(
        true,
        &guard,
        serde_json::json!({"cluster": "local-dev", "broker_name": "broker-z"}),
    )
    .await;
    let error = serde_json::from_str::<serde_json::Value>(&content_text(&result)).unwrap();
    assert_eq!(error["code"], "not_found");
    assert_eq!(error["entity"], "broker");

    for (arguments, code) in [
        (serde_json::json!({"cluster": "local-dev"}), "invalid_arguments"),
        (
            serde_json::json!({"cluster": "unconfigured", "broker_name": "broker-a"}),
            "cluster_not_allowed",
        ),
        (
            serde_json::json!({"cluster": "local-dev", "broker_name": "broker-a", "broker_addr": "10.0.0.1:10911"}),
            "invalid_arguments",
        ),
    ] {
        let result = call(false, &guard, arguments.clone()).await;
        assert_eq!(result.is_error, Some(true), "{arguments}");
        let error = serde_json::from_str::<serde_json::Value>(&content_text(&result)).unwrap();
        assert_eq!(error["code"], code, "{arguments}");
    }
}
