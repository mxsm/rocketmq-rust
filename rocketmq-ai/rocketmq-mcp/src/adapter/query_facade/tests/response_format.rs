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

//! The `response_format` argument of the list Tools, from a call down to the reads it causes.

use rmcp::model::CallToolRequestParams;
use rmcp::model::CallToolResult;
use serde_json::json;
use serde_json::Value;

use super::*;
use crate::tools::catalog::ToolId;
use crate::tools::executor::ToolExecutor;

type Executor = ToolExecutor<QueryFacade<FakeSessionFactory>>;

fn executor(factory: FakeSessionFactory) -> Executor {
    let config = example_config();
    let guard = crate::guard::Guard::new(
        crate::config::SecurityConfig {
            profile: "read_only".to_string(),
            allow_change_planning: false,
            sanitize_output: true,
            rate_limit_per_minute: 600,
            permissions_file: config.security.permissions_file.clone(),
            max_concurrent_requests_per_cluster: 8,
            pseudonym_key_env: None,
            http_rate_limit_per_minute: None,
        },
        crate::config::AuditConfig {
            enabled: true,
            sink: "memory".to_string(),
            path: String::new(),
            queue_capacity: 256,
            max_record_bytes: 16 * 1024,
            queue_max_bytes: 1024 * 1024,
            max_file_bytes: 0,
        },
        &config.clusters,
    )
    .unwrap();
    ToolExecutor::new(QueryFacade::with_factory(config, factory), guard)
}

async fn call(executor: &Executor, tool: ToolId, arguments: Value) -> CallToolResult {
    let arguments = arguments.as_object().cloned().expect("the arguments are an object");
    executor
        .call(CallToolRequestParams::new(tool.descriptor().name).with_arguments(arguments))
        .await
        .unwrap()
}

/// The structured content of a successful call, which the pipeline has checked against the
/// output schema of the Tool.
async fn listed(executor: &Executor, tool: ToolId, arguments: Value) -> Value {
    let result = call(executor, tool, arguments.clone()).await;
    assert_eq!(result.is_error, Some(false), "{}: {arguments}", tool.descriptor().name);
    let structured = result.structured_content.clone().expect("structured content");
    let mirror = result.content[1].as_text().expect("the mirror is text");
    assert_eq!(serde_json::from_str::<Value>(&mirror.text).unwrap(), structured);
    structured
}

fn error(result: &CallToolResult) -> Value {
    assert_eq!(result.is_error, Some(true));
    serde_json::from_str(&result.content[0].as_text().expect("an error is text").text).unwrap()
}

fn members(item: &Value) -> Vec<&str> {
    item.as_object()
        .expect("an item is an object")
        .keys()
        .map(String::as_str)
        .collect()
}

#[tokio::test(start_paused = true)]
async fn concise_format_omits_optional_detail_fields() {
    let factory = FakeSessionFactory {
        many_groups: true,
        ..Default::default()
    };
    let executor = executor(factory);
    let cluster = json!("local-dev");
    // Each list Tool with its own arguments, where its items are, and what names an item.
    let tools = [
        (ToolId::ListTopics, json!({}), "items", vec!["topic"]),
        (ToolId::ListConsumerGroups, json!({}), "items", vec!["group"]),
        (
            ToolId::ListBrokers,
            json!({}),
            "brokers",
            vec!["broker_active", "broker_id", "broker_name"],
        ),
        (
            ToolId::ListConsumerConnections,
            json!({ "consumer_group": "group-1" }),
            "items",
            vec!["broker_name", "client_alias"],
        ),
        (
            ToolId::ListProducerConnections,
            json!({ "topic": "orders", "producer_group": "producers" }),
            "items",
            vec!["broker_name", "client_alias"],
        ),
    ];

    for (tool, arguments, array, names) in tools {
        let name = tool.descriptor().name;
        let with_format = |format: Option<&str>| {
            let mut arguments = arguments.clone();
            arguments["cluster"] = cluster.clone();
            if let Some(format) = format {
                arguments["response_format"] = json!(format);
            }
            arguments
        };

        let default = listed(&executor, tool, with_format(None)).await;
        let detailed = listed(&executor, tool, with_format(Some("detailed"))).await;
        let concise = listed(&executor, tool, with_format(Some("concise"))).await;

        let detailed_items = detailed["data"][array].as_array().expect("detailed items");
        let concise_items = concise["data"][array].as_array().expect("concise items");
        assert!(!detailed_items.is_empty(), "{name}: the fake serves items");
        assert_eq!(concise_items.len(), detailed_items.len(), "{name}");
        for (concise_item, detailed_item) in concise_items.iter().zip(detailed_items) {
            // A concise item is what names it and nothing else; the detailed one has more.
            assert_eq!(members(concise_item), names, "{name}");
            assert!(members(detailed_item).len() > names.len(), "{name}");
            for member in &names {
                assert_eq!(concise_item[member], detailed_item[member], "{name}: {member}");
            }
        }
        // Nothing outside the items differs, and leaving the argument out means `detailed`.
        let without_items = |mut structured: Value| {
            structured["data"][array] = Value::Null;
            structured["data"]["generated_at"] = Value::Null;
            structured["request_id"] = Value::Null;
            structured["freshness_ms"] = Value::Null;
            structured["cache_status"] = Value::Null;
            structured
        };
        assert_eq!(default["data"][array], detailed["data"][array], "{name}");
        assert_eq!(
            without_items(concise.clone()),
            without_items(detailed.clone()),
            "{name}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn concise_consumer_groups_read_only_the_names_and_share_cursors_with_the_detailed_format() {
    let factory = FakeSessionFactory {
        many_groups: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let executor = executor(factory);
    let page = |format: &str, cursor: Option<&Value>| {
        let mut arguments = json!({ "cluster": "local-dev", "limit": 2, "response_format": format });
        if let Some(cursor) = cursor {
            arguments["cursor"] = cursor.clone();
        }
        arguments
    };

    let first = listed(&executor, ToolId::ListConsumerGroups, page("concise", None)).await;
    assert_eq!(
        first["data"]["items"],
        json!([{ "group": "group-0" }, { "group": "group-1" }])
    );
    assert_eq!(first["data"]["total_count"], 5);
    assert_eq!(counters.consumer_group_inventory_queries.load(Ordering::SeqCst), 1);
    assert_eq!(
        counters.consumer_group_enrichment_queries.load(Ordering::SeqCst),
        0,
        "the names need no read about each group"
    );

    // The cursor of a concise page continues in the detailed format, and the other way round.
    let second = listed(
        &executor,
        ToolId::ListConsumerGroups,
        page("detailed", Some(&first["data"]["next_cursor"])),
    )
    .await;
    assert_eq!(second["data"]["items"][0]["group"], "group-2");
    assert!(second["data"]["items"][0].get("client_count").is_some());
    assert_eq!(counters.consumer_group_enrichment_queries.load(Ordering::SeqCst), 1);
    let third = listed(
        &executor,
        ToolId::ListConsumerGroups,
        page("concise", Some(&second["data"]["next_cursor"])),
    )
    .await;
    assert_eq!(third["data"]["items"], json!([{ "group": "group-4" }]));
    assert_eq!(third["data"]["has_more"], false);
    assert_eq!(counters.consumer_group_inventory_queries.load(Ordering::SeqCst), 1);
    assert_eq!(counters.consumer_group_enrichment_queries.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn the_format_is_validated_like_any_other_argument() {
    let executor = executor(FakeSessionFactory::default());

    for unknown in [json!("brief"), json!("CONCISE"), json!(1), Value::Null] {
        let refused = call(
            &executor,
            ToolId::ListTopics,
            json!({ "cluster": "local-dev", "response_format": unknown }),
        )
        .await;
        let refused = error(&refused);
        assert_eq!(refused["code"], "invalid_arguments", "{unknown}");
        assert_eq!(refused["violations"][0]["path"], "/response_format", "{unknown}");
    }

    // A Tool that returns one thing has no such argument.
    let refused = call(
        &executor,
        ToolId::GetClusterOverview,
        json!({ "cluster": "local-dev", "response_format": "concise" }),
    )
    .await;
    assert_eq!(error(&refused)["code"], "invalid_arguments");
}
