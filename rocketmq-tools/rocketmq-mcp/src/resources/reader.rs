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
use crate::tools::cluster_tools::ClusterOverviewArgs;
use crate::tools::consumer_tools::ListConsumerGroupsArgs;
use crate::tools::executor::ToolExecutionError;
use crate::tools::topic_tools::ListTopicsArgs;

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
    match uri.kind {
        ResourceKind::Overview => {
            let output = query
                .cluster_overview(ClusterOverviewArgs {
                    cluster: uri.cluster.clone(),
                })
                .await?;
            Ok(live_payload(
                uri,
                "overview",
                output.generated_at.clone(),
                json!(output),
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
                output.generated_at.clone(),
                json!(output.page),
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
                output.generated_at.clone(),
                json!(output.brokers),
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
                output.generated_at.clone(),
                json!(output.page),
            ))
        }
    }
}

fn live_payload(uri: &RocketmqResourceUri, field: &str, observed_at: String, data: Value) -> Value {
    let mut payload = serde_json::Map::from_iter([
        ("schema_version".to_string(), json!(SCHEMA_VERSION)),
        ("resource".to_string(), json!(uri.as_string())),
        ("cluster".to_string(), json!(uri.cluster)),
        ("observed_at".to_string(), json!(observed_at)),
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
        _ => ErrorData::internal_error(
            format!("live RocketMQ resource query failed: {uri}"),
            Some(json!({ "code": "resource_query_failed" })),
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::model::contract::Page;
    use crate::model::diagnosis::DiagnosisReport;
    use crate::tools::broker_tools::DescribeBrokerArgs;
    use crate::tools::broker_tools::DescribeBrokerOutput;
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
        ) -> Result<crate::tools::cluster_tools::ClusterOverviewOutput, ToolExecutionError> {
            if args.cluster != "local-dev" {
                return Err(ToolExecutionError::InvalidArguments(format!(
                    "unknown cluster: {}",
                    args.cluster
                )));
            }
            Ok(crate::tools::cluster_tools::ClusterOverviewOutput {
                cluster: args.cluster,
                namesrv_addr: "hidden".to_string(),
                brokers: Vec::new(),
                topic_count: 0,
                consumer_group_count: 0,
                generated_at: "2026-07-10T00:00:00.000Z".to_string(),
            })
        }

        async fn list_topics(&self, _args: ListTopicsArgs) -> Result<ListTopicsOutput, ToolExecutionError> {
            unimplemented!("not needed by reader tests")
        }

        async fn describe_topic(&self, _args: DescribeTopicArgs) -> Result<DescribeTopicOutput, ToolExecutionError> {
            unimplemented!("not needed by reader tests")
        }

        async fn query_topic_route(
            &self,
            _args: QueryTopicRouteArgs,
        ) -> Result<QueryTopicRouteOutput, ToolExecutionError> {
            unimplemented!("not needed by reader tests")
        }

        async fn list_consumer_groups(
            &self,
            args: ListConsumerGroupsArgs,
        ) -> Result<ListConsumerGroupsOutput, ToolExecutionError> {
            Ok(ListConsumerGroupsOutput {
                cluster: args.cluster.unwrap_or_else(|| "local-dev".to_string()),
                namesrv_addr: "hidden".to_string(),
                page: Page {
                    items: Vec::new(),
                    count: 0,
                    total_count: 0,
                    has_more: false,
                    next_cursor: None,
                },
                generated_at: "2026-07-10T00:00:00.000Z".to_string(),
            })
        }

        async fn query_consumer_lag(
            &self,
            _args: QueryConsumerLagArgs,
        ) -> Result<QueryConsumerLagOutput, ToolExecutionError> {
            unimplemented!("not needed by reader tests")
        }

        async fn describe_broker(&self, _args: DescribeBrokerArgs) -> Result<DescribeBrokerOutput, ToolExecutionError> {
            unimplemented!("not needed by reader tests")
        }

        async fn diagnose_consumer_lag(
            &self,
            _args: DiagnoseConsumerLagArgs,
        ) -> Result<DiagnosisReport, ToolExecutionError> {
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
        assert_eq!(payload["consumer_groups"]["total_count"], 0);
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
}
