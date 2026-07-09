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

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use serde_json::Value;

use rmcp::model::CallToolRequestParams;
use rmcp::model::CallToolResult;
use rmcp::model::ContentBlock;
use rmcp::model::JsonObject;
use rmcp::ErrorData;

use crate::adapter::admin_core_adapter::ReadOnlyAdminAdapter;
use crate::guard::Guard;
use crate::guard::GuardError;
use crate::service::diagnosis_service;
use crate::tools::broker_tools;
use crate::tools::cluster_tools;
use crate::tools::consumer_tools;
use crate::tools::diagnosis_tools;
use crate::tools::registry;
use crate::tools::topic_tools;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ToolExecutionError {
    #[error("invalid arguments: {0}")]
    InvalidArguments(String),

    #[error("backend error: {0}")]
    Backend(String),

    #[error("permission denied: {0}")]
    PermissionDenied(String),

    #[error("rate limit exceeded: {0}")]
    RateLimited(String),

    #[error("dangerous tool disabled: {0}")]
    DangerousToolDisabled(String),

    #[error("confirmation required: {0}")]
    ConfirmationRequired(String),
}

impl ToolExecutionError {
    pub(crate) fn backend(error: impl ToString) -> Self {
        Self::Backend(error.to_string())
    }
}

impl From<GuardError> for ToolExecutionError {
    fn from(error: GuardError) -> Self {
        match error {
            GuardError::InvalidArgument(message) => Self::InvalidArguments(message),
            GuardError::PermissionDenied(message) => Self::PermissionDenied(message),
            GuardError::RateLimited(message) => Self::RateLimited(message),
            GuardError::DangerousToolDisabled(message) => Self::DangerousToolDisabled(message),
            GuardError::ConfirmationRequired(message) => Self::ConfirmationRequired(message),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ToolExecutor<A> {
    adapter: A,
    guard: Guard,
}

impl<A> ToolExecutor<A>
where
    A: ReadOnlyAdminAdapter,
{
    pub(crate) fn new(adapter: A, guard: Guard) -> Self {
        Self { adapter, guard }
    }

    pub(crate) async fn call(&self, request: CallToolRequestParams) -> Result<CallToolResult, ErrorData> {
        let tool_name = request.name.to_string();
        let risk_level = registry::tool_risk_level(&tool_name)
            .ok_or_else(|| ErrorData::invalid_params(format!("unknown tool: {tool_name}"), None))?;
        let arguments = request.arguments.unwrap_or_default();
        let guarded_call = match self.guard.begin_tool_call(&tool_name, risk_level, &arguments) {
            Ok(guarded_call) => guarded_call,
            Err(error) => return Ok(error_result(&tool_name, error.into())),
        };

        let result = match tool_name.as_str() {
            cluster_tools::CLUSTER_OVERVIEW_TOOL => {
                let args = match decode_args::<cluster_tools::ClusterOverviewArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        guarded_call.record_protocol_error(error.message.to_string());
                        return Err(error);
                    }
                };
                self.adapter
                    .cluster_overview(args)
                    .await
                    .map(|output| success_result(summary_cluster_overview(&output), &output))
                    .unwrap_or_else(|error| Ok(error_result(cluster_tools::CLUSTER_OVERVIEW_TOOL, error)))
            }
            topic_tools::LIST_TOPICS_TOOL => {
                let args = match decode_args::<topic_tools::ListTopicsArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        guarded_call.record_protocol_error(error.message.to_string());
                        return Err(error);
                    }
                };
                self.adapter
                    .list_topics(args)
                    .await
                    .map(|output| success_result(summary_list_topics(&output), &output))
                    .unwrap_or_else(|error| Ok(error_result(topic_tools::LIST_TOPICS_TOOL, error)))
            }
            topic_tools::DESCRIBE_TOPIC_TOOL => {
                let args = match decode_args::<topic_tools::DescribeTopicArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        guarded_call.record_protocol_error(error.message.to_string());
                        return Err(error);
                    }
                };
                self.adapter
                    .describe_topic(args)
                    .await
                    .map(|output| success_result(summary_describe_topic(&output), &output))
                    .unwrap_or_else(|error| Ok(error_result(topic_tools::DESCRIBE_TOPIC_TOOL, error)))
            }
            topic_tools::QUERY_TOPIC_ROUTE_TOOL => {
                let args = match decode_args::<topic_tools::QueryTopicRouteArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        guarded_call.record_protocol_error(error.message.to_string());
                        return Err(error);
                    }
                };
                self.adapter
                    .query_topic_route(args)
                    .await
                    .map(|output| success_result(summary_topic_route(&output), &output))
                    .unwrap_or_else(|error| Ok(error_result(topic_tools::QUERY_TOPIC_ROUTE_TOOL, error)))
            }
            consumer_tools::LIST_CONSUMER_GROUPS_TOOL => {
                let args = match decode_args::<consumer_tools::ListConsumerGroupsArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        guarded_call.record_protocol_error(error.message.to_string());
                        return Err(error);
                    }
                };
                self.adapter
                    .list_consumer_groups(args)
                    .await
                    .map(|output| success_result(summary_consumer_groups(&output), &output))
                    .unwrap_or_else(|error| Ok(error_result(consumer_tools::LIST_CONSUMER_GROUPS_TOOL, error)))
            }
            consumer_tools::QUERY_CONSUMER_LAG_TOOL => {
                let args = match decode_args::<consumer_tools::QueryConsumerLagArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        guarded_call.record_protocol_error(error.message.to_string());
                        return Err(error);
                    }
                };
                self.adapter
                    .query_consumer_lag(args)
                    .await
                    .map(|output| success_result(summary_consumer_lag(&output), &output))
                    .unwrap_or_else(|error| Ok(error_result(consumer_tools::QUERY_CONSUMER_LAG_TOOL, error)))
            }
            broker_tools::DESCRIBE_BROKER_TOOL => {
                let args = match decode_args::<broker_tools::DescribeBrokerArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        guarded_call.record_protocol_error(error.message.to_string());
                        return Err(error);
                    }
                };
                self.adapter
                    .describe_broker(args)
                    .await
                    .map(|output| success_result(summary_describe_broker(&output), &output))
                    .unwrap_or_else(|error| Ok(error_result(broker_tools::DESCRIBE_BROKER_TOOL, error)))
            }
            diagnosis_tools::DIAGNOSE_CONSUMER_LAG_TOOL => {
                let args = match decode_args::<diagnosis_tools::DiagnoseConsumerLagArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        guarded_call.record_protocol_error(error.message.to_string());
                        return Err(error);
                    }
                };
                diagnosis_service::diagnose_consumer_lag(&self.adapter, args)
                    .await
                    .map(|output| success_result(output.summary.clone(), &output))
                    .unwrap_or_else(|error| Ok(error_result(diagnosis_tools::DIAGNOSE_CONSUMER_LAG_TOOL, error)))
            }
            _ => unreachable!("unknown tool was rejected before dispatch"),
        }?;

        Ok(guarded_call.finish_result(result))
    }
}

fn decode_args<T>(arguments: JsonObject) -> Result<T, ErrorData>
where
    T: DeserializeOwned,
{
    serde_json::from_value(Value::Object(arguments))
        .map_err(|error| ErrorData::invalid_params(format!("invalid tool arguments: {error}"), None))
}

fn success_result<T>(summary: String, output: &T) -> Result<CallToolResult, ErrorData>
where
    T: Serialize,
{
    let structured = serde_json::to_value(output)
        .map_err(|error| ErrorData::internal_error(format!("failed to serialize tool output: {error}"), None))?;
    let mut result = CallToolResult::success(vec![ContentBlock::text(summary)]);
    result.structured_content = Some(structured);
    Ok(result)
}

fn error_result(tool_name: &str, error: ToolExecutionError) -> CallToolResult {
    let message = format!("{tool_name} failed: {error}");
    let mut result = CallToolResult::error(vec![ContentBlock::text(message)]);
    result.structured_content = Some(json!({
        "tool": tool_name,
        "error": error.to_string(),
    }));
    result
}

fn summary_cluster_overview(output: &cluster_tools::ClusterOverviewOutput) -> String {
    format!(
        "Cluster {} has {} broker rows, {} topics, and {} consumer groups.",
        output.cluster,
        output.brokers.len(),
        output.topic_count,
        output.consumer_group_count
    )
}

fn summary_list_topics(output: &topic_tools::ListTopicsOutput) -> String {
    format!("Cluster {} has {} topics.", output.cluster, output.topic_count)
}

fn summary_describe_topic(output: &topic_tools::DescribeTopicOutput) -> String {
    format!(
        "Topic {} on cluster {} has {} brokers, {} read queues, and {} write queues.",
        output.topic,
        output.cluster,
        output.broker_names.len(),
        output.read_queue_count,
        output.write_queue_count
    )
}

fn summary_topic_route(output: &topic_tools::QueryTopicRouteOutput) -> String {
    format!(
        "Topic {} route on cluster {} has {} brokers and {} queue entries.",
        output.topic,
        output.cluster,
        output.brokers.len(),
        output.queues.len()
    )
}

fn summary_consumer_groups(output: &consumer_tools::ListConsumerGroupsOutput) -> String {
    format!(
        "Cluster {} has {} consumer groups.",
        output.cluster, output.consumer_group_count
    )
}

fn summary_consumer_lag(output: &consumer_tools::QueryConsumerLagOutput) -> String {
    format!(
        "Consumer group {} has total lag {} on topic {} across {} queues.",
        output.consumer_group, output.total_lag, output.topic, output.queue_count
    )
}

fn summary_describe_broker(output: &broker_tools::DescribeBrokerOutput) -> String {
    format!(
        "Broker {} on cluster {} has {} broker rows.",
        output.broker_name,
        output.cluster,
        output.brokers.len()
    )
}

#[cfg(test)]
mod tests {
    use crate::config::AuditConfig;
    use crate::config::ClusterConfig;
    use crate::config::SecurityConfig;
    use crate::guard::audit::AuditStatus;
    use crate::guard::Guard;

    use super::*;

    #[derive(Clone)]
    struct FakeAdapter {
        fail: bool,
    }

    impl ReadOnlyAdminAdapter for FakeAdapter {
        async fn cluster_overview(
            &self,
            args: cluster_tools::ClusterOverviewArgs,
        ) -> Result<cluster_tools::ClusterOverviewOutput, ToolExecutionError> {
            if self.fail {
                return Err(ToolExecutionError::backend(
                    "nameserver unavailable secret_key=super-secret",
                ));
            }
            Ok(cluster_tools::ClusterOverviewOutput {
                cluster: args.cluster,
                namesrv_addr: "127.0.0.1:9876".to_string(),
                brokers: vec![broker_summary()],
                topic_count: 2,
                consumer_group_count: 1,
                generated_at: "1".to_string(),
            })
        }

        async fn list_topics(
            &self,
            _args: topic_tools::ListTopicsArgs,
        ) -> Result<topic_tools::ListTopicsOutput, ToolExecutionError> {
            unimplemented!("not needed by this test")
        }

        async fn describe_topic(
            &self,
            _args: topic_tools::DescribeTopicArgs,
        ) -> Result<topic_tools::DescribeTopicOutput, ToolExecutionError> {
            unimplemented!("not needed by this test")
        }

        async fn query_topic_route(
            &self,
            _args: topic_tools::QueryTopicRouteArgs,
        ) -> Result<topic_tools::QueryTopicRouteOutput, ToolExecutionError> {
            unimplemented!("not needed by this test")
        }

        async fn list_consumer_groups(
            &self,
            _args: consumer_tools::ListConsumerGroupsArgs,
        ) -> Result<consumer_tools::ListConsumerGroupsOutput, ToolExecutionError> {
            unimplemented!("not needed by this test")
        }

        async fn query_consumer_lag(
            &self,
            _args: consumer_tools::QueryConsumerLagArgs,
        ) -> Result<consumer_tools::QueryConsumerLagOutput, ToolExecutionError> {
            unimplemented!("not needed by this test")
        }

        async fn describe_broker(
            &self,
            _args: broker_tools::DescribeBrokerArgs,
        ) -> Result<broker_tools::DescribeBrokerOutput, ToolExecutionError> {
            unimplemented!("not needed by this test")
        }
    }

    #[tokio::test]
    async fn call_returns_summary_and_structured_content() {
        let guard = test_guard("diagnose");
        let result = ToolExecutor::new(FakeAdapter { fail: false }, guard.clone())
            .call(
                CallToolRequestParams::new(cluster_tools::CLUSTER_OVERVIEW_TOOL).with_arguments(
                    serde_json::json!({
                        "cluster": "local-dev",
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                ),
            )
            .await
            .unwrap();

        assert_eq!(result.is_error, Some(false));
        assert_eq!(result.structured_content.as_ref().unwrap()["cluster"], "local-dev");
        assert!(!result.content.is_empty());
        let records = guard.audit_log().records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].tool, cluster_tools::CLUSTER_OVERVIEW_TOOL);
        assert_eq!(records[0].cluster.as_deref(), Some("local-dev"));
        assert_eq!(records[0].status, AuditStatus::Success);
    }

    #[tokio::test]
    async fn backend_error_is_returned_as_tool_error() {
        let guard = test_guard("diagnose");
        let result = ToolExecutor::new(FakeAdapter { fail: true }, guard.clone())
            .call(
                CallToolRequestParams::new(cluster_tools::CLUSTER_OVERVIEW_TOOL).with_arguments(
                    serde_json::json!({
                        "cluster": "local-dev",
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                ),
            )
            .await
            .unwrap();

        assert_eq!(result.is_error, Some(true));
        assert!(result.structured_content.as_ref().unwrap()["error"]
            .as_str()
            .unwrap()
            .contains("nameserver unavailable"));
        assert!(!result
            .structured_content
            .as_ref()
            .unwrap()
            .to_string()
            .contains("super-secret"));
        assert!(!content_text(&result).contains("super-secret"));
        let records = guard.audit_log().records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, AuditStatus::Failure);
    }

    #[tokio::test]
    async fn unknown_tool_returns_protocol_error() {
        let err = ToolExecutor::new(FakeAdapter { fail: false }, test_guard("diagnose"))
            .call(CallToolRequestParams::new("unknown_tool"))
            .await
            .unwrap_err();

        assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn read_only_guard_denies_diagnosis_tool() {
        let guard = test_guard("read_only");
        let result = ToolExecutor::new(FakeAdapter { fail: false }, guard.clone())
            .call(
                CallToolRequestParams::new(diagnosis_tools::DIAGNOSE_CONSUMER_LAG_TOOL).with_arguments(
                    serde_json::json!({
                        "cluster": "local-dev",
                        "topic": "orders",
                        "consumer_group": "order-service",
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                ),
            )
            .await
            .unwrap();

        assert_eq!(result.is_error, Some(true));
        assert!(content_text(&result).contains("permission denied"));
        let records = guard.audit_log().records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, AuditStatus::Failure);
    }

    fn content_text(result: &CallToolResult) -> String {
        result
            .content
            .iter()
            .filter_map(|content| match content {
                ContentBlock::Text(text) => Some(text.text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn test_guard(profile: &str) -> Guard {
        Guard::new(
            SecurityConfig {
                profile: profile.to_string(),
                allow_dangerous_tools: false,
                require_confirmation: true,
                sanitize_output: true,
                rate_limit_per_minute: 60,
            },
            AuditConfig {
                enabled: true,
                sink: "memory".to_string(),
                path: String::new(),
            },
            &[ClusterConfig {
                name: "local-dev".to_string(),
                namesrv_addr: "127.0.0.1:9876".to_string(),
                default: Some(true),
            }],
        )
    }

    fn broker_summary() -> cluster_tools::BrokerSummary {
        cluster_tools::BrokerSummary {
            cluster: "local-dev".to_string(),
            broker_name: "broker-a".to_string(),
            broker_id: 0,
            broker_addr: "127.0.0.1:10911".to_string(),
            version: "5.3.0".to_string(),
            in_tps: "1.0".to_string(),
            out_tps: "1.0".to_string(),
            timer_progress: "0".to_string(),
            page_cache_lock_time_millis: "0".to_string(),
            hour: "0".to_string(),
            space: "0".to_string(),
            broker_active: true,
        }
    }
}
