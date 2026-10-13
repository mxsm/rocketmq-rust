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

//! `rocketmq_get_consumer_running_info`: what one consumer client reports, named by its pseudonym.

use super::*;

const TOOL: &str = "rocketmq_get_consumer_running_info";
const ALIAS: &str = "client-0123456789abcdef0123456789abcdef";

/// What the fake adapter answers for the Tool: one client with one subscription and one queue.
pub(super) fn answer(
    adapter: &FakeAdapter,
    args: consumer_tools::GetConsumerRunningInfoArgs,
) -> Result<QueryResult<consumer_tools::GetConsumerRunningInfoOutput>, ToolFailure> {
    if adapter.fail {
        return Err(ToolFailure::not_found(NotFoundEntity::Client));
    }
    let items = vec![consumer_tools::ConsumerClientQueueRow {
        topic: "orders".to_string(),
        broker_name: "broker-a".to_string(),
        queue_id: 0,
        mode: consumer_tools::ConsumerClientQueueMode::Pull,
        dropped: false,
        last_pull_at: Some("2023-11-14T22:13:20.500Z".to_string()),
        commit_offset: Some(40),
        cached_message_count: Some(5),
        cached_message_size_mib: Some(1),
        locked: Some(false),
        try_unlock_times: Some(0),
        last_lock_at: None,
        last_consume_at: None,
        wait_ack_count: None,
    }];
    Ok(QueryResult::bypass(consumer_tools::GetConsumerRunningInfoOutput {
        cluster: args.cluster,
        consumer_group: args.consumer_group,
        client_alias: args.client_alias,
        broker_name: "broker-a".to_string(),
        consume_type: Some("CONSUME_PASSIVELY".to_string()),
        consume_orderly: Some(false),
        thread_pool_core_size: Some(20),
        client_version: Some("V5_3_0".to_string()),
        started_at: Some("2023-11-14T22:13:20.000Z".to_string()),
        subscriptions: vec![consumer_tools::ConsumerClientSubscription {
            topic: "orders".to_string(),
            expression_type: "TAG".to_string(),
            expression: Some("TagA || TagB".to_string()),
            sub_version: 7,
        }],
        topic_status: vec![consumer_tools::ConsumerClientTopicStatus {
            topic: "orders".to_string(),
            pull_rt_ms: 1.5,
            pull_tps: 12.0,
            consume_rt_ms: 2.5,
            consume_ok_tps: 12.0,
            consume_failed_tps: 0.0,
            consume_failed_messages: 3,
        }],
        queue_count: 1,
        truncated: false,
        page: crate::model::contract::Page {
            count: items.len(),
            total_count: items.len(),
            items,
            has_more: false,
            next_cursor: None,
        },
        generated_at: "1".to_string(),
    }))
}

async fn call(fail: bool, guard: &Guard, arguments: serde_json::Value) -> CallToolResult {
    ToolExecutor::new(FakeAdapter { fail, partial: false }, guard.clone())
        .call(CallToolRequestParams::new(TOOL).with_arguments(arguments.as_object().unwrap().clone()))
        .await
        .unwrap()
}

fn arguments() -> serde_json::Value {
    serde_json::json!({"cluster": "local-dev", "consumer_group": "workers", "client_alias": ALIAS})
}

#[tokio::test]
async fn running_info_is_a_diagnosis_read_that_names_the_client_by_its_pseudonym() {
    // The example `diagnose` role grants the Tool, and the `read_only` role does not.
    let guard = test_guard("diagnose");
    let result = call(false, &guard, arguments()).await;

    assert_eq!(result.is_error, Some(false), "{}", content_text(&result));
    let data = &result.structured_content.as_ref().unwrap()["data"];
    assert_eq!(data["client_alias"], ALIAS);
    assert_eq!(data["broker_name"], "broker-a");
    assert_eq!(data["subscriptions"][0]["expression"], "TagA || TagB");
    assert_eq!(data["topic_status"][0]["consume_failed_messages"], 3);
    assert_eq!(data["items"][0]["mode"], "pull");
    assert_eq!(data["items"][0]["wait_ack_count"], serde_json::Value::Null);
    assert_eq!(data["total_count"], 1);
    assert!(content_text(&result).starts_with(&format!(
        "Client {ALIAS} of consumer group workers on cluster local-dev holds 1 subscriptions and returned 1 of 1 \
         queues."
    )));
    assert!(result
        .content
        .iter()
        .all(|content| content.as_resource_link().is_none()));
    assert_eq!(guard.audit_log().records()[0].status, AuditStatus::Success);

    let denied = test_guard("read_only");
    let result = call(false, &denied, arguments()).await;
    assert_eq!(result.is_error, Some(true));
    let error = serde_json::from_str::<serde_json::Value>(&content_text(&result)).unwrap();
    assert_eq!(error["code"], "unauthorized_scope");
}

#[tokio::test]
async fn a_client_that_is_gone_is_a_not_found_error_that_names_the_listing_tool() {
    let guard = test_guard("diagnose");
    let result = call(true, &guard, arguments()).await;

    assert_eq!(result.is_error, Some(true));
    let error = serde_json::from_str::<serde_json::Value>(&content_text(&result)).unwrap();
    assert_eq!(error["code"], "not_found");
    assert_eq!(error["entity"], "client");
    assert_eq!(error["retryable"], false);
    assert!(error["suggestions"]
        .to_string()
        .contains("rocketmq_list_consumer_connections"));
}

#[tokio::test]
async fn running_info_takes_a_pseudonym_and_never_an_identifier_or_an_address() {
    let guard = test_guard("diagnose");
    for (change, path, constraint) in [
        (
            serde_json::json!({"client_alias": "consumer@10.0.0.7@4242"}),
            "/client_alias",
            "pattern",
        ),
        (serde_json::json!({"client_alias": null}), "/client_alias", "type"),
        (
            serde_json::json!({"client_id": "consumer@10.0.0.7@4242"}),
            "/*",
            "additional_property",
        ),
        (
            serde_json::json!({"broker_addr": "10.0.0.7:10911"}),
            "/*",
            "additional_property",
        ),
        (serde_json::json!({"jstack": true}), "/*", "additional_property"),
    ] {
        let mut changed = arguments();
        changed
            .as_object_mut()
            .unwrap()
            .extend(change.as_object().unwrap().clone());
        let result = call(false, &guard, changed.clone()).await;
        assert_eq!(result.is_error, Some(true), "{changed}");
        let error = serde_json::from_str::<serde_json::Value>(&content_text(&result)).unwrap();
        assert_eq!(error["code"], "invalid_arguments", "{changed}");
        assert_eq!(
            error["violations"],
            serde_json::json!([{"path": path, "constraint": constraint}]),
            "{changed}"
        );
        // An error never repeats what the caller sent in the place of the pseudonym.
        assert!(!content_text(&result).contains("10.0.0.7"), "{changed}");
    }
}
