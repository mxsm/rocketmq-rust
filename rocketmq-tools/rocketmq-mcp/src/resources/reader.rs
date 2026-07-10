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

use rmcp::model::ReadResourceResult;
use rmcp::model::ResourceContents;
use rmcp::ErrorData;
use serde_json::json;
use serde_json::Value;

use crate::adapter::query_facade::ReadOnlyQuery;
use crate::model::contract::PageRequest;
use crate::model::contract::SCHEMA_VERSION;
use crate::resources::uri::ResourceKind;
use crate::resources::uri::RocketmqResourceUri;
use crate::resources::uri::JSON_MIME_TYPE;
use crate::tools::broker_tools::DescribeBrokerArgs;
use crate::tools::cluster_tools::ClusterOverviewArgs;
use crate::tools::consumer_tools::ListConsumerGroupsArgs;
use crate::tools::consumer_tools::QueryConsumerLagArgs;
use crate::tools::executor::ToolExecutionError;
use crate::tools::topic_tools::DescribeTopicArgs;
use crate::tools::topic_tools::ListTopicsArgs;
use crate::tools::topic_tools::QueryTopicRouteArgs;

pub(crate) async fn read_resource<Q>(query: &Q, uri: &str) -> Result<ReadResourceResult, ErrorData>
where
    Q: ReadOnlyQuery,
{
    let resource_uri = RocketmqResourceUri::parse(uri)
        .ok_or_else(|| ErrorData::resource_not_found(format!("resource not found: {uri}"), None))?;
    let payload = resource_payload(query, &resource_uri)
        .await
        .map_err(|error| resource_error(uri, error))?;
    let text = serde_json::to_string_pretty(&payload)
        .map_err(|err| ErrorData::internal_error(format!("failed to serialize resource {uri}: {err}"), None))?;

    Ok(ReadResourceResult::new(vec![
        ResourceContents::text(text, uri).with_mime_type(JSON_MIME_TYPE)
    ]))
}

async fn resource_payload<Q>(query: &Q, uri: &RocketmqResourceUri) -> Result<Value, ToolExecutionError>
where
    Q: ReadOnlyQuery,
{
    match &uri.kind {
        ResourceKind::Overview => {
            let output = query
                .cluster_overview(ClusterOverviewArgs {
                    cluster: uri.cluster.clone(),
                })
                .await?;
            Ok(live_payload(
                uri,
                "overview",
                output.observed_at,
                output.freshness_ms,
                output.cache_status,
                json!(output.data),
            ))
        }
        ResourceKind::Topics => {
            let output = query
                .list_topics(ListTopicsArgs {
                    cluster: Some(uri.cluster.clone()),
                    filter: None,
                    page: PageRequest::default(),
                })
                .await?;
            Ok(live_payload(
                uri,
                "topics",
                output.observed_at,
                output.freshness_ms,
                output.cache_status,
                json!(output.data.page),
            ))
        }
        ResourceKind::Topic(topic) => {
            let output = query
                .describe_topic(DescribeTopicArgs {
                    cluster: uri.cluster.clone(),
                    topic: topic.clone(),
                    page: PageRequest::default(),
                })
                .await?;
            Ok(live_payload(
                uri,
                "topic",
                output.observed_at,
                output.freshness_ms,
                output.cache_status,
                json!(output.data),
            ))
        }
        ResourceKind::TopicRoute(topic) => {
            let output = query
                .query_topic_route(QueryTopicRouteArgs {
                    cluster: uri.cluster.clone(),
                    topic: topic.clone(),
                    page: PageRequest::default(),
                })
                .await?;
            Ok(live_payload(
                uri,
                "route",
                output.observed_at,
                output.freshness_ms,
                output.cache_status,
                json!(output.data),
            ))
        }
        ResourceKind::Brokers => {
            let output = query
                .cluster_overview(ClusterOverviewArgs {
                    cluster: uri.cluster.clone(),
                })
                .await?;
            Ok(live_payload(
                uri,
                "brokers",
                output.observed_at,
                output.freshness_ms,
                output.cache_status,
                json!(output.data.brokers),
            ))
        }
        ResourceKind::Broker(broker) => {
            let output = query
                .describe_broker(DescribeBrokerArgs {
                    cluster: uri.cluster.clone(),
                    broker_name: broker.clone(),
                })
                .await?;
            if output.data.brokers.is_empty() {
                return Err(ToolExecutionError::InvalidArguments(format!(
                    "broker not found in cluster {}: {broker}",
                    uri.cluster
                )));
            }
            Ok(live_payload(
                uri,
                "broker",
                output.observed_at,
                output.freshness_ms,
                output.cache_status,
                json!(output.data),
            ))
        }
        ResourceKind::ConsumerGroups => {
            let output = query
                .list_consumer_groups(ListConsumerGroupsArgs {
                    cluster: Some(uri.cluster.clone()),
                    filter: None,
                    page: PageRequest::default(),
                })
                .await?;
            Ok(live_payload(
                uri,
                "consumer_groups",
                output.observed_at,
                output.freshness_ms,
                output.cache_status,
                json!(output.data.page),
            ))
        }
        ResourceKind::ConsumerGroup(group) => {
            let output = query
                .list_consumer_groups(ListConsumerGroupsArgs {
                    cluster: Some(uri.cluster.clone()),
                    filter: Some(group.clone()),
                    page: PageRequest::default(),
                })
                .await?;
            let consumer_group = output
                .data
                .page
                .items
                .iter()
                .find(|item| item.group == *group)
                .cloned()
                .ok_or_else(|| {
                    ToolExecutionError::InvalidArguments(format!(
                        "consumer group not found in cluster {}: {group}",
                        uri.cluster
                    ))
                })?;
            Ok(live_payload(
                uri,
                "consumer_group",
                output.observed_at,
                output.freshness_ms,
                output.cache_status,
                json!(consumer_group),
            ))
        }
        ResourceKind::ConsumerLag { group, topic } => {
            let output = query
                .query_consumer_lag(QueryConsumerLagArgs {
                    cluster: uri.cluster.clone(),
                    topic: topic.clone(),
                    consumer_group: group.clone(),
                    page: PageRequest::default(),
                })
                .await?;
            Ok(live_payload(
                uri,
                "consumer_lag",
                output.observed_at,
                output.freshness_ms,
                output.cache_status,
                json!(output.data),
            ))
        }
    }
}

fn live_payload(
    uri: &RocketmqResourceUri,
    field: &str,
    observed_at: String,
    freshness_ms: u64,
    cache_status: crate::model::contract::CacheStatus,
    data: Value,
) -> Value {
    let mut payload = serde_json::Map::from_iter([
        ("schema_version".to_string(), json!(SCHEMA_VERSION)),
        ("resource".to_string(), json!(uri.as_string())),
        ("cluster".to_string(), json!(uri.cluster)),
        ("observed_at".to_string(), json!(observed_at)),
        ("freshness_ms".to_string(), json!(freshness_ms)),
        ("cache_status".to_string(), json!(cache_status)),
        ("source".to_string(), json!("live")),
        ("partial".to_string(), json!(false)),
        ("warnings".to_string(), json!([])),
    ]);
    payload.insert(field.to_string(), data);
    Value::Object(payload)
}

fn resource_error(uri: &str, error: ToolExecutionError) -> ErrorData {
    match error {
        ToolExecutionError::InvalidArguments(message) => ErrorData::resource_not_found(message, None),
        ToolExecutionError::TimedOut { timeout_ms } => ErrorData::internal_error(
            format!("live RocketMQ resource query timed out: {uri}"),
            Some(json!({
                "code": "resource_query_timeout",
                "retryable": true,
                "timeout_ms": timeout_ms,
            })),
        ),
        ToolExecutionError::Cancelled => ErrorData::internal_error(
            format!("live RocketMQ resource query was cancelled: {uri}"),
            Some(json!({ "code": "resource_query_cancelled", "retryable": true })),
        ),
        ToolExecutionError::PermissionDenied(_) => ErrorData::internal_error(
            format!("permission denied for RocketMQ resource: {uri}"),
            Some(json!({ "code": "resource_permission_denied", "retryable": false })),
        ),
        ToolExecutionError::RateLimited(_) => ErrorData::internal_error(
            format!("rate limit exceeded for RocketMQ resource: {uri}"),
            Some(json!({ "code": "resource_rate_limited", "retryable": true })),
        ),
        ToolExecutionError::Backend(_) => ErrorData::internal_error(
            format!("live RocketMQ resource query failed: {uri}"),
            Some(json!({ "code": "resource_backend_unavailable", "retryable": true })),
        ),
        ToolExecutionError::ChangePlanningDisabled(_)
        | ToolExecutionError::Internal(_)
        | ToolExecutionError::OutputTooLarge { .. } => ErrorData::internal_error(
            format!("live RocketMQ resource query failed: {uri}"),
            Some(json!({ "code": "resource_query_failed", "retryable": false })),
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::model::contract::Page;
    use crate::model::contract::QueryResult;
    use crate::model::diagnosis::DiagnosisReport;
    use crate::tools::broker_tools::DescribeBrokerArgs;
    use crate::tools::broker_tools::DescribeBrokerOutput;
    use crate::tools::cluster_tools::BrokerSummary;
    use crate::tools::consumer_tools::ConsumerGroupSummary;
    use crate::tools::consumer_tools::ListConsumerGroupsOutput;
    use crate::tools::consumer_tools::QueryConsumerLagArgs;
    use crate::tools::consumer_tools::QueryConsumerLagOutput;
    use crate::tools::diagnosis_tools::DiagnoseConsumerLagArgs;
    use crate::tools::topic_tools::DescribeTopicArgs;
    use crate::tools::topic_tools::DescribeTopicOutput;
    use crate::tools::topic_tools::ListTopicsOutput;
    use crate::tools::topic_tools::QueryTopicRouteArgs;
    use crate::tools::topic_tools::QueryTopicRouteOutput;

    #[derive(Clone)]
    struct FakeQuery;

    #[async_trait::async_trait]
    impl ReadOnlyQuery for FakeQuery {
        async fn cluster_overview(
            &self,
            args: ClusterOverviewArgs,
        ) -> Result<QueryResult<crate::tools::cluster_tools::ClusterOverviewOutput>, ToolExecutionError> {
            if args.cluster != "local-dev" {
                return Err(ToolExecutionError::InvalidArguments(format!(
                    "unknown cluster: {}",
                    args.cluster
                )));
            }
            Ok(QueryResult::bypass(
                crate::tools::cluster_tools::ClusterOverviewOutput {
                    cluster: args.cluster,
                    namesrv_addr: "hidden".to_string(),
                    brokers: Vec::new(),
                    topic_count: 0,
                    consumer_group_count: 0,
                    generated_at: "2026-07-10T00:00:00.000Z".to_string(),
                },
            ))
        }

        async fn list_topics(
            &self,
            _args: ListTopicsArgs,
        ) -> Result<QueryResult<ListTopicsOutput>, ToolExecutionError> {
            unimplemented!("not needed by reader tests")
        }

        async fn describe_topic(
            &self,
            args: DescribeTopicArgs,
        ) -> Result<QueryResult<DescribeTopicOutput>, ToolExecutionError> {
            if args.topic != "orders" {
                return Err(ToolExecutionError::InvalidArguments(format!(
                    "topic not found: {}",
                    args.topic
                )));
            }
            Ok(QueryResult::bypass(DescribeTopicOutput {
                cluster: args.cluster,
                namesrv_addr: "hidden".to_string(),
                topic: args.topic,
                broker_names: vec!["broker-a".to_string()],
                read_queue_count: 0,
                write_queue_count: 0,
                brokers: Vec::new(),
                page: empty_page(),
                generated_at: "2026-07-10T00:00:00.000Z".to_string(),
            }))
        }

        async fn query_topic_route(
            &self,
            args: QueryTopicRouteArgs,
        ) -> Result<QueryResult<QueryTopicRouteOutput>, ToolExecutionError> {
            Ok(QueryResult::bypass(QueryTopicRouteOutput {
                cluster: args.cluster,
                namesrv_addr: "hidden".to_string(),
                topic: args.topic,
                brokers: Vec::new(),
                read_queue_count: 0,
                write_queue_count: 0,
                page: empty_page(),
                generated_at: "2026-07-10T00:00:00.000Z".to_string(),
            }))
        }

        async fn list_consumer_groups(
            &self,
            args: ListConsumerGroupsArgs,
        ) -> Result<QueryResult<ListConsumerGroupsOutput>, ToolExecutionError> {
            let items = (args.filter.as_deref().is_none() || args.filter.as_deref() == Some("order-service"))
                .then(|| ConsumerGroupSummary {
                    group: "order-service".to_string(),
                    version: 1,
                    client_count: 1,
                    consume_type: "CONSUME_PASSIVELY".to_string(),
                    message_model: "CLUSTERING".to_string(),
                    consume_tps: 1.0,
                    diff_total: 0,
                })
                .into_iter()
                .collect::<Vec<_>>();
            let count = items.len();
            Ok(QueryResult::bypass(ListConsumerGroupsOutput {
                cluster: args.cluster.unwrap_or_else(|| "local-dev".to_string()),
                namesrv_addr: "hidden".to_string(),
                page: Page {
                    items,
                    count,
                    total_count: count,
                    has_more: false,
                    next_cursor: None,
                },
                generated_at: "2026-07-10T00:00:00.000Z".to_string(),
            }))
        }

        async fn query_consumer_lag(
            &self,
            args: QueryConsumerLagArgs,
        ) -> Result<QueryResult<QueryConsumerLagOutput>, ToolExecutionError> {
            Ok(QueryResult::bypass(QueryConsumerLagOutput {
                cluster: args.cluster,
                namesrv_addr: "hidden".to_string(),
                topic: args.topic,
                consumer_group: args.consumer_group,
                total_lag: 0,
                max_queue_lag: 0,
                consume_tps: 1.0,
                inflight_total: 0,
                page: empty_page(),
                generated_at: "2026-07-10T00:00:00.000Z".to_string(),
            }))
        }

        async fn describe_broker(
            &self,
            args: DescribeBrokerArgs,
        ) -> Result<QueryResult<DescribeBrokerOutput>, ToolExecutionError> {
            let brokers = (args.broker_name == "broker-a")
                .then(|| broker_summary(&args.cluster, &args.broker_name))
                .into_iter()
                .collect();
            Ok(QueryResult::bypass(DescribeBrokerOutput {
                cluster: args.cluster,
                namesrv_addr: "hidden".to_string(),
                broker_name: args.broker_name,
                brokers,
                generated_at: "2026-07-10T00:00:00.000Z".to_string(),
            }))
        }

        async fn diagnose_consumer_lag(
            &self,
            _args: DiagnoseConsumerLagArgs,
        ) -> Result<QueryResult<DiagnosisReport>, ToolExecutionError> {
            unimplemented!("not needed by reader tests")
        }
    }

    use super::*;

    #[tokio::test]
    async fn read_cluster_overview_is_cluster_scoped_and_hides_nameserver() {
        let result = read_resource(&FakeQuery, "rocketmq://clusters/local-dev/overview")
            .await
            .unwrap();
        let payload = read_json_payload(&result);

        assert_eq!(payload["cluster"], "local-dev");
        assert_eq!(payload["source"], "live");
        assert!(payload.get("namesrv_addr").is_none());
        assert!(chrono::DateTime::parse_from_rfc3339(payload["observed_at"].as_str().unwrap()).is_ok());
    }

    #[tokio::test]
    async fn read_inventory_resource_returns_live_data() {
        let result = read_resource(&FakeQuery, "rocketmq://clusters/local-dev/consumer-groups")
            .await
            .unwrap();
        let payload = read_json_payload(&result);

        assert_eq!(payload["schema_version"], SCHEMA_VERSION);
        assert_eq!(payload["source"], "live");
        assert_eq!(payload["partial"], false);
        assert_eq!(payload["consumer_groups"]["total_count"], 1);
    }

    #[tokio::test]
    async fn read_parameterized_resources_returns_live_data() {
        let cases = [
            ("rocketmq://clusters/local-dev/topics/orders", "topic"),
            ("rocketmq://clusters/local-dev/topics/orders/route", "route"),
            (
                "rocketmq://clusters/local-dev/consumer-groups/order-service",
                "consumer_group",
            ),
            (
                "rocketmq://clusters/local-dev/consumer-groups/order-service/lag?topic=orders",
                "consumer_lag",
            ),
            ("rocketmq://clusters/local-dev/brokers/broker-a", "broker"),
        ];

        for (uri, field) in cases {
            let result = read_resource(&FakeQuery, uri).await.unwrap();
            let payload = read_json_payload(&result);

            assert_eq!(payload["resource"], uri);
            assert_eq!(payload["source"], "live");
            assert!(payload.get(field).is_some());
        }
    }

    #[tokio::test]
    async fn read_unknown_parameterized_resource_returns_not_found() {
        let error = read_resource(&FakeQuery, "rocketmq://clusters/local-dev/brokers/missing")
            .await
            .unwrap_err();

        assert_eq!(error.code, rmcp::model::ErrorCode::RESOURCE_NOT_FOUND);
    }

    #[tokio::test]
    async fn read_unknown_or_unconfigured_resource_returns_not_found() {
        let unknown_resource = read_resource(&FakeQuery, "rocketmq://clusters/local-dev/unknown")
            .await
            .unwrap_err();
        let unknown_cluster = read_resource(&FakeQuery, "rocketmq://clusters/missing/overview")
            .await
            .unwrap_err();

        assert_eq!(unknown_resource.code, rmcp::model::ErrorCode::RESOURCE_NOT_FOUND);
        assert_eq!(unknown_cluster.code, rmcp::model::ErrorCode::RESOURCE_NOT_FOUND);
    }

    #[test]
    fn resource_errors_distinguish_permission_timeout_and_backend_failure() {
        let permission = resource_error(
            "rocketmq://clusters/local-dev/topics",
            ToolExecutionError::PermissionDenied("missing scope".to_string()),
        );
        let timeout = resource_error(
            "rocketmq://clusters/local-dev/topics",
            ToolExecutionError::TimedOut { timeout_ms: 5000 },
        );
        let backend = resource_error(
            "rocketmq://clusters/local-dev/topics",
            ToolExecutionError::backend("nameserver unavailable secret_key=hidden"),
        );

        assert_eq!(permission.data.unwrap()["code"], "resource_permission_denied");
        assert_eq!(timeout.data.unwrap()["code"], "resource_query_timeout");
        assert_eq!(backend.data.unwrap()["code"], "resource_backend_unavailable");
        assert!(!backend.message.contains("secret_key"));
    }

    fn read_json_payload(result: &ReadResourceResult) -> Value {
        assert_eq!(result.contents.len(), 1);
        match &result.contents[0] {
            ResourceContents::TextResourceContents { mime_type, text, .. } => {
                assert_eq!(mime_type.as_deref(), Some(JSON_MIME_TYPE));
                serde_json::from_str(text).unwrap()
            }
            ResourceContents::BlobResourceContents { .. } => panic!("resource should be returned as text"),
            _ => panic!("unsupported resource content variant"),
        }
    }

    fn empty_page<T>() -> Page<T> {
        Page {
            items: Vec::new(),
            count: 0,
            total_count: 0,
            has_more: false,
            next_cursor: None,
        }
    }

    fn broker_summary(cluster: &str, broker_name: &str) -> BrokerSummary {
        BrokerSummary {
            cluster: cluster.to_string(),
            broker_name: broker_name.to_string(),
            broker_id: 0,
            broker_addr: "hidden".to_string(),
            version: "test".to_string(),
            in_tps: "0".to_string(),
            out_tps: "0".to_string(),
            timer_progress: "0".to_string(),
            page_cache_lock_time_millis: "0".to_string(),
            hour: "0".to_string(),
            space: "0".to_string(),
            broker_active: true,
        }
    }
}
