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
use crate::model::contract::SCHEMA_VERSION;
use crate::resources::uri::ResourceKind;
use crate::resources::uri::RocketmqResourceUri;
use crate::resources::uri::JSON_MIME_TYPE;
use crate::tools::broker_tools::BrokerDiagnosticsArgs;
use crate::tools::broker_tools::DescribeBrokerArgs;
use crate::tools::cluster_tools::ClusterOverviewArgs;
use crate::tools::config_tools::BrokerConfigSummaryArgs;
use crate::tools::config_tools::GetTopicConfigArgs;
use crate::tools::consumer_tools::GetConsumerProgressArgs;
use crate::tools::consumer_tools::ListConsumerGroupsArgs;
use crate::tools::consumer_tools::QueryConsumerLagArgs;
use crate::tools::executor::ToolExecutionError;
use crate::tools::topic_tools::DescribeTopicArgs;
use crate::tools::topic_tools::GetTopicStatsArgs;
use crate::tools::topic_tools::ListTopicsArgs;
use crate::tools::topic_tools::QueryTopicRouteArgs;

pub(crate) async fn read_resource<Q>(query: &Q, uri: &str) -> Result<ReadResourceResult, ErrorData>
where
    Q: ReadOnlyQuery,
{
    let resource_uri = RocketmqResourceUri::parse(uri)
        .ok_or_else(|| ErrorData::resource_not_found("resource is unavailable", None))?;
    let canonical_uri = resource_uri.as_string();
    let payload = resource_payload(query, &resource_uri).await.map_err(resource_error)?;
    let text = serde_json::to_string_pretty(&payload)
        .map_err(|_| ErrorData::internal_error("failed to serialize resource", None))?;

    Ok(ReadResourceResult::new(vec![ResourceContents::text(
        text,
        canonical_uri,
    )
    .with_mime_type(JSON_MIME_TYPE)]))
}

async fn resource_payload<Q>(query: &Q, uri: &RocketmqResourceUri) -> Result<Value, ToolExecutionError>
where
    Q: ReadOnlyQuery,
{
    let cluster = uri.cluster().unwrap_or_default().to_string();
    match &uri.kind {
        ResourceKind::Capabilities => Err(ToolExecutionError::Internal(
            "capability resources are rendered by the authenticated protocol handler".to_string(),
        )),
        ResourceKind::SystemRuntimeV1 | ResourceKind::SystemObservabilityV1 => Err(ToolExecutionError::Internal(
            "system resources are rendered by the authenticated protocol handler".to_string(),
        )),
        ResourceKind::Overview => {
            let output = query
                .cluster_overview(ClusterOverviewArgs {
                    cluster: cluster.clone(),
                })
                .await?;
            Ok(live_payload(uri, "overview", output, |data| json!(data)))
        }
        ResourceKind::Topics => {
            let output = query
                .list_topics(ListTopicsArgs {
                    cluster: Some(cluster.clone()),
                    filter: uri.query().filter.clone(),
                    page: uri.query().page.clone(),
                })
                .await?;
            Ok(live_payload(uri, "topics", output, |data| json!(data.page)))
        }
        ResourceKind::Topic(topic) => {
            let output = query
                .describe_topic(DescribeTopicArgs {
                    cluster: cluster.clone(),
                    topic: topic.clone(),
                    page: uri.query().page.clone(),
                })
                .await?;
            Ok(live_payload(uri, "topic", output, |data| json!(data)))
        }
        ResourceKind::TopicRoute(topic) => {
            let output = query
                .query_topic_route(QueryTopicRouteArgs {
                    cluster: cluster.clone(),
                    topic: topic.clone(),
                    page: uri.query().page.clone(),
                })
                .await?;
            Ok(live_payload(uri, "route", output, |data| json!(data)))
        }
        ResourceKind::Brokers => {
            let output = query
                .cluster_overview(ClusterOverviewArgs {
                    cluster: cluster.clone(),
                })
                .await?;
            Ok(live_payload(uri, "brokers", output, |data| json!(data.brokers)))
        }
        ResourceKind::Broker(broker) => {
            let output = query
                .describe_broker(DescribeBrokerArgs {
                    cluster: cluster.clone(),
                    broker_name: broker.clone(),
                })
                .await?;
            if output.data.brokers.is_empty() {
                return Err(ToolExecutionError::InvalidArguments(format!(
                    "broker not found in cluster {}: {broker}",
                    cluster
                )));
            }
            Ok(live_payload(uri, "broker", output, |data| json!(data)))
        }
        ResourceKind::BrokerDiagnostics(broker) => {
            let output = query
                .broker_diagnostics(BrokerDiagnosticsArgs {
                    cluster: cluster.clone(),
                    broker_name: broker.clone(),
                })
                .await?;
            Ok(live_payload(uri, "diagnostics", output, |data| json!(data)))
        }
        ResourceKind::BrokerConfigSummary(broker) => {
            let output = query
                .broker_config_summary(BrokerConfigSummaryArgs {
                    cluster: cluster.clone(),
                    broker_name: broker.clone(),
                })
                .await?;
            Ok(live_payload(uri, "config_summary", output, |data| json!(data)))
        }
        ResourceKind::ConsumerGroups => {
            let output = query
                .list_consumer_groups(ListConsumerGroupsArgs {
                    cluster: Some(cluster.clone()),
                    filter: uri.query().filter.clone(),
                    page: uri.query().page.clone(),
                })
                .await?;
            Ok(live_payload(uri, "consumer_groups", output, |data| json!(data.page)))
        }
        ResourceKind::ConsumerGroup(group) => {
            let output = query.describe_consumer_group(cluster.clone(), group.clone()).await?;
            Ok(live_payload(uri, "consumer_group", output, |data| json!(data)))
        }
        ResourceKind::ConsumerLag { group, topic } => {
            let output = query
                .query_consumer_lag(QueryConsumerLagArgs {
                    cluster: cluster.clone(),
                    topic: topic.clone(),
                    consumer_group: group.clone(),
                    page: uri.query().page.clone(),
                })
                .await?;
            Ok(live_payload(uri, "consumer_lag", output, |data| json!(data)))
        }
        ResourceKind::TopicStats(topic) => {
            let output = query
                .topic_stats(GetTopicStatsArgs {
                    cluster: cluster.clone(),
                    topic: topic.clone(),
                    page: uri.query().page.clone(),
                })
                .await?;
            Ok(live_payload(uri, "topic_stats", output, |data| json!(data)))
        }
        ResourceKind::TopicConfig(topic) => {
            let output = query
                .topic_config(GetTopicConfigArgs {
                    cluster: cluster.clone(),
                    topic: topic.clone(),
                })
                .await?;
            Ok(live_payload(uri, "topic_config", output, |data| json!(data)))
        }
        ResourceKind::ConsumerProgress(group) => {
            let output = query
                .consumer_progress(GetConsumerProgressArgs {
                    cluster,
                    consumer_group: group.clone(),
                    page: uri.query().page.clone(),
                })
                .await?;
            Ok(live_payload(uri, "consumer_progress", output, |data| json!(data)))
        }
    }
}

fn live_payload<T>(
    uri: &RocketmqResourceUri,
    field: &str,
    output: crate::model::contract::QueryResult<T>,
    project: impl FnOnce(T) -> Value,
) -> Value {
    let data = project(output.data);
    let mut payload = serde_json::Map::from_iter([
        ("schema_version".to_string(), json!(SCHEMA_VERSION)),
        ("resource".to_string(), json!(uri.as_string())),
        ("cluster".to_string(), json!(uri.cluster())),
        ("observed_at".to_string(), json!(output.observed_at)),
        ("freshness_ms".to_string(), json!(output.freshness_ms)),
        ("cache_status".to_string(), json!(output.cache_status)),
        ("source".to_string(), json!("live")),
        ("partial".to_string(), json!(output.partial)),
        ("warnings".to_string(), json!(output.warnings)),
    ]);
    if !output.source_failures.is_empty() {
        payload.insert("source_failures".to_string(), json!(output.source_failures));
    }
    payload.insert(field.to_string(), data);
    Value::Object(payload)
}

fn resource_error(error: ToolExecutionError) -> ErrorData {
    match error {
        ToolExecutionError::InvalidArguments(_) => ErrorData::resource_not_found("resource not found", None),
        ToolExecutionError::TimedOut { timeout_ms } => ErrorData::internal_error(
            "live RocketMQ resource query timed out",
            Some(json!({
                "code": "resource_query_timeout",
                "retryable": true,
                "timeout_ms": timeout_ms,
            })),
        ),
        ToolExecutionError::Cancelled => ErrorData::internal_error(
            "live RocketMQ resource query was cancelled",
            Some(json!({ "code": "resource_query_cancelled", "retryable": true })),
        ),
        ToolExecutionError::PermissionDenied(_) => ErrorData::internal_error(
            "RocketMQ resource is unavailable",
            Some(json!({ "code": "permission_denied", "retryable": false })),
        ),
        ToolExecutionError::UnauthorizedScope(_) => ErrorData::internal_error(
            "RocketMQ resource is unavailable",
            Some(json!({ "code": "unauthorized_scope", "retryable": false })),
        ),
        ToolExecutionError::TenantMismatch(_) => ErrorData::internal_error(
            "RocketMQ resource is unavailable",
            Some(json!({ "code": "tenant_mismatch", "retryable": false })),
        ),
        ToolExecutionError::ClusterNotAllowed(_) => ErrorData::internal_error(
            "RocketMQ resource is unavailable",
            Some(json!({ "code": "cluster_not_allowed", "retryable": false })),
        ),
        ToolExecutionError::RateLimited(_) => ErrorData::internal_error(
            "rate limit exceeded for RocketMQ resource",
            Some(json!({ "code": "resource_rate_limited", "retryable": true })),
        ),
        ToolExecutionError::Backend(_) => ErrorData::internal_error(
            "live RocketMQ resource query failed",
            Some(json!({ "code": "source_unavailable", "retryable": true })),
        ),
        ToolExecutionError::OutputTooLarge { .. } => ErrorData::internal_error(
            "live RocketMQ resource query output is too large",
            Some(json!({ "code": "output_too_large", "retryable": false })),
        ),
        ToolExecutionError::ChangePlanningDisabled(_) | ToolExecutionError::Internal(_) => ErrorData::internal_error(
            "live RocketMQ resource query failed",
            Some(json!({ "code": "resource_query_failed", "retryable": false })),
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::model::contract::Page;
    use crate::model::contract::QueryResult;
    use crate::model::contract::QuerySource;
    use crate::model::contract::SourceFailure;
    use crate::model::contract::SourceFailureCode;
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

        async fn topic_stats(
            &self,
            args: GetTopicStatsArgs,
        ) -> Result<QueryResult<crate::tools::topic_tools::GetTopicStatsOutput>, ToolExecutionError> {
            if args.cluster != "local-dev"
                || args.topic != "orders"
                || args.page.limit != Some(2)
                || args.page.cursor.as_deref() != Some("topic-page")
            {
                return Err(ToolExecutionError::InvalidArguments(
                    "unexpected topic-statistics mapping".to_string(),
                ));
            }
            Ok(QueryResult::bypass(crate::tools::topic_tools::GetTopicStatsOutput {
                cluster: args.cluster,
                topic: args.topic,
                total_message_count: 0,
                queue_count: 0,
                truncated: false,
                page: empty_page(),
                generated_at: "2026-07-10T00:00:00.000Z".to_string(),
            }))
        }

        async fn topic_config(
            &self,
            args: GetTopicConfigArgs,
        ) -> Result<QueryResult<crate::tools::config_tools::GetTopicConfigOutput>, ToolExecutionError> {
            if args.cluster != "local-dev" || args.topic != "orders" {
                return Err(ToolExecutionError::InvalidArguments(
                    "unexpected topic-configuration mapping".to_string(),
                ));
            }
            Ok(QueryResult::bypass(crate::tools::config_tools::GetTopicConfigOutput {
                cluster: args.cluster,
                topic: args.topic,
                brokers: Vec::new(),
                inconsistent_fields: Vec::new(),
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

        async fn consumer_progress(
            &self,
            args: GetConsumerProgressArgs,
        ) -> Result<QueryResult<crate::tools::consumer_tools::GetConsumerProgressOutput>, ToolExecutionError> {
            if args.cluster != "local-dev"
                || args.consumer_group != "order-service"
                || args.page.limit != Some(3)
                || args.page.cursor.as_deref() != Some("progress-page")
            {
                return Err(ToolExecutionError::InvalidArguments(
                    "unexpected consumer-progress mapping".to_string(),
                ));
            }
            Ok(QueryResult::bypass(
                crate::tools::consumer_tools::GetConsumerProgressOutput {
                    cluster: args.cluster,
                    consumer_group: args.consumer_group,
                    state: crate::tools::consumer_tools::ConsumerProgressState::NoConsumption,
                    topic_count: 0,
                    queue_count: 0,
                    total_lag: 0,
                    max_queue_lag: 0,
                    total_inflight: 0,
                    consume_tps: 0.0,
                    truncated: false,
                    page: empty_page(),
                    generated_at: "2026-07-10T00:00:00.000Z".to_string(),
                },
            ))
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

        async fn broker_diagnostics(
            &self,
            args: BrokerDiagnosticsArgs,
        ) -> Result<QueryResult<crate::tools::broker_tools::BrokerDiagnosticsOutput>, ToolExecutionError> {
            if args.cluster != "local-dev" || args.broker_name != "broker-a" {
                return Err(ToolExecutionError::InvalidArguments(
                    "unexpected broker-diagnostics mapping".to_string(),
                ));
            }
            Ok(QueryResult::bypass(
                crate::tools::broker_tools::BrokerDiagnosticsOutput {
                    cluster: args.cluster,
                    broker_name: args.broker_name,
                    diagnostics_schema_version: "rocketmq-mcp.broker-diagnostics.v1".to_string(),
                    observed_at_millis: 0,
                    brokers: Vec::new(),
                    unavailable_brokers: 0,
                },
            ))
        }

        async fn broker_config_summary(
            &self,
            args: BrokerConfigSummaryArgs,
        ) -> Result<QueryResult<crate::tools::config_tools::BrokerConfigSummaryOutput>, ToolExecutionError> {
            if args.cluster != "local-dev" || args.broker_name != "broker-a" {
                return Err(ToolExecutionError::InvalidArguments(
                    "unexpected broker-configuration mapping".to_string(),
                ));
            }
            Ok(QueryResult::bypass(
                crate::tools::config_tools::BrokerConfigSummaryOutput {
                    cluster: args.cluster,
                    broker_name: args.broker_name,
                    brokers: Vec::new(),
                },
            ))
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

    #[test]
    fn query_backed_resource_payload_preserves_partial_evidence() {
        let uri = RocketmqResourceUri::parse("rocketmq://clusters/local-dev/overview").unwrap();
        let mut output = QueryResult::bypass(serde_json::json!({"brokers": []}));
        output.partial = true;
        output.warnings = vec!["source_failures_present".to_string()];
        output.source_failures = vec![SourceFailure::new(
            QuerySource::BrokerRuntime,
            SourceFailureCode::Timeout,
            true,
            "broker-b",
        )];

        let payload = live_payload(&uri, "overview", output, |data| data);

        assert_eq!(payload["partial"], true);
        assert_eq!(payload["warnings"], json!(["source_failures_present"]));
        assert_eq!(payload["source_failures"][0]["source"], "broker_runtime");
        assert_eq!(payload["source_failures"][0]["logical_target"], "broker-b");
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
            assert_eq!(resource_contents_uri(&result), uri);
            let payload = read_json_payload(&result);

            assert_eq!(payload["resource"], uri);
            assert_eq!(payload["source"], "live");
            assert!(payload.get(field).is_some());
        }
    }

    #[tokio::test]
    async fn five_scoped_resources_map_exact_names_and_pagination_to_read_only_queries() {
        let cases = [
            (
                "rocketmq://clusters/local-dev/brokers/broker-a/diagnostics",
                "diagnostics",
            ),
            (
                "rocketmq://clusters/local-dev/brokers/broker-a/config-summary",
                "config_summary",
            ),
            (
                "rocketmq://clusters/local-dev/topics/orders/stats?limit=2&cursor=topic-page",
                "topic_stats",
            ),
            ("rocketmq://clusters/local-dev/topics/orders/config", "topic_config"),
            (
                "rocketmq://clusters/local-dev/consumer-groups/order-service/progress?limit=3&cursor=progress-page",
                "consumer_progress",
            ),
        ];

        for (uri, field) in cases {
            let result = read_resource(&FakeQuery, uri).await.unwrap();
            assert_eq!(resource_contents_uri(&result), uri);
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
        let permission = resource_error(ToolExecutionError::PermissionDenied(
            "missing scope for token=secret at 127.0.0.1:9876".to_string(),
        ));
        let timeout = resource_error(ToolExecutionError::TimedOut { timeout_ms: 5000 });
        let backend = resource_error(ToolExecutionError::backend("nameserver unavailable secret_key=hidden"));

        assert_eq!(permission.data.as_ref().unwrap()["code"], "permission_denied");
        assert_eq!(timeout.data.as_ref().unwrap()["code"], "resource_query_timeout");
        assert_eq!(backend.data.as_ref().unwrap()["code"], "source_unavailable");
        assert!(!backend.message.contains("secret_key"));
        let wire = format!("{} {}", permission.message, permission.data.as_ref().unwrap());
        assert!(!wire.contains("token=secret"));
        assert!(!wire.contains("127.0.0.1"));
    }

    fn resource_contents_uri(result: &ReadResourceResult) -> &str {
        match &result.contents[0] {
            ResourceContents::TextResourceContents { uri, .. } | ResourceContents::BlobResourceContents { uri, .. } => {
                uri
            }
            _ => panic!("unsupported resource content variant"),
        }
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
