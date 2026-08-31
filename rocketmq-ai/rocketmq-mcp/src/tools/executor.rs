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
use serde_json::Value;
use std::time::Instant;

use rmcp::model::CallToolRequestParams;
use rmcp::model::CallToolResult;
use rmcp::model::ContentBlock;
use rmcp::model::JsonObject;
use rmcp::model::Resource;
use rmcp::ErrorData;
use rocketmq_observability::metrics::mcp::McpErrorKind;
use rocketmq_observability::metrics::mcp::McpMetricsRecorder;
use rocketmq_observability::metrics::mcp::McpOperationKind;
use rocketmq_observability::metrics::mcp::McpOperationOutcome;
use tracing::Instrument;

use crate::adapter::query_facade::ReadOnlyQuery;
use crate::guard::context::RequestContext;
use crate::guard::Guard;
use crate::guard::GuardError;
use crate::model::contract::QueryResult;
use crate::model::contract::ToolResponse;
use crate::resources::uri::ResourceKind;
use crate::resources::uri::RocketmqResourceUri;
use crate::resources::uri::JSON_MIME_TYPE;
use crate::tools::broker_tools;
use crate::tools::catalog::ToolDescriptor;
use crate::tools::catalog::ToolId;
#[cfg(feature = "change-planning")]
use crate::tools::change_tools;
use crate::tools::cluster_tools;
use crate::tools::config_tools;
use crate::tools::connection_tools;
use crate::tools::consumer_tools;
use crate::tools::diagnosis_tools;
use crate::tools::message_tools;
use crate::tools::output_policy;
use crate::tools::proxy_tools;
use crate::tools::topic_tools;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ToolExecutionError {
    #[error("invalid arguments: {0}")]
    InvalidArguments(String),

    #[error("backend error: {0}")]
    Backend(String),

    #[error("permission denied: {0}")]
    PermissionDenied(String),

    #[error("permission denied: {0}")]
    UnauthorizedScope(String),

    #[error("permission denied: {0}")]
    TenantMismatch(String),

    #[error("permission denied: {0}")]
    ClusterNotAllowed(String),

    #[error("rate limit exceeded: {0}")]
    RateLimited(String),

    #[error("change planning disabled: {0}")]
    ChangePlanningDisabled(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("structured output is {actual_bytes} bytes; maximum is {max_bytes} bytes")]
    OutputTooLarge { actual_bytes: usize, max_bytes: usize },

    #[error("query workflow timed out after {timeout_ms} ms")]
    TimedOut { timeout_ms: u64 },

    #[error("query workflow was cancelled")]
    Cancelled,
}

impl ToolExecutionError {
    pub(crate) fn backend(error: impl ToString) -> Self {
        Self::Backend(error.to_string())
    }

    pub(crate) fn internal(error: impl ToString) -> Self {
        Self::Internal(error.to_string())
    }

    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::InvalidArguments(_) => "invalid_arguments",
            Self::Backend(_) => "source_unavailable",
            Self::PermissionDenied(_) => "permission_denied",
            Self::UnauthorizedScope(_) => "unauthorized_scope",
            Self::TenantMismatch(_) => "tenant_mismatch",
            Self::ClusterNotAllowed(_) => "cluster_not_allowed",
            Self::RateLimited(_) => "rate_limited",
            Self::ChangePlanningDisabled(_) => "change_planning_disabled",
            Self::Internal(_) => "internal_error",
            Self::OutputTooLarge { .. } => "output_too_large",
            Self::TimedOut { .. } => "backend_timeout",
            Self::Cancelled => "cancelled",
        }
    }

    fn retryable(&self) -> bool {
        matches!(self, Self::Backend(_) | Self::RateLimited(_) | Self::TimedOut { .. })
    }

    fn suggestions(&self) -> Vec<&'static str> {
        match self {
            Self::InvalidArguments(_) => vec!["Correct the arguments using the Tool input schema and retry."],
            Self::Backend(_) => vec!["Retry after verifying the selected cluster and RocketMQ availability."],
            Self::PermissionDenied(_) => vec!["Use a principal or profile authorized for this Tool."],
            Self::UnauthorizedScope(_) => vec!["Request the required OAuth scope and retry."],
            Self::TenantMismatch(_) => vec!["Use credentials issued for the selected cluster tenant."],
            Self::ClusterNotAllowed(_) => vec!["Select a cluster present in the caller allow-list."],
            Self::RateLimited(_) => vec!["Retry after the rate-limit window resets."],
            Self::ChangePlanningDisabled(_) => {
                vec!["Enable change planning explicitly and use an operator-authorized profile."]
            }
            Self::Internal(_) => vec!["Report the request identifier to the server operator."],
            Self::OutputTooLarge { .. } => vec!["Reduce the page limit or narrow the query filter."],
            Self::TimedOut { .. } => vec!["Retry after checking RocketMQ availability or narrow the workflow scope."],
            Self::Cancelled => vec!["Retry the request if the cancellation was not intentional."],
        }
    }

    fn metric_kind(&self) -> McpErrorKind {
        match self {
            Self::InvalidArguments(_) => McpErrorKind::InvalidRequest,
            Self::PermissionDenied(_)
            | Self::UnauthorizedScope(_)
            | Self::TenantMismatch(_)
            | Self::ClusterNotAllowed(_)
            | Self::ChangePlanningDisabled(_) => McpErrorKind::PermissionDenied,
            Self::RateLimited(_) => McpErrorKind::RateLimited,
            Self::Backend(_) | Self::TimedOut { .. } => McpErrorKind::SourceUnavailable,
            Self::OutputTooLarge { .. } => McpErrorKind::OutputTooLarge,
            Self::Internal(_) | Self::Cancelled => McpErrorKind::Internal,
        }
    }

    fn public_message(&self) -> String {
        match self {
            Self::InvalidArguments(message) => format!("invalid arguments: {message}"),
            Self::Backend(_) => "RocketMQ source is unavailable".to_string(),
            Self::PermissionDenied(_) => "permission denied for this Tool".to_string(),
            Self::UnauthorizedScope(_) => "permission denied: required OAuth scope is unavailable".to_string(),
            Self::TenantMismatch(_) => "permission denied: tenant boundary mismatch".to_string(),
            Self::ClusterNotAllowed(_) => "permission denied: cluster is not allowed".to_string(),
            Self::RateLimited(_) => "rate limit exceeded for this Tool".to_string(),
            Self::ChangePlanningDisabled(_) => "change planning disabled by server policy".to_string(),
            Self::Internal(_) => "MCP request failed internally".to_string(),
            Self::OutputTooLarge {
                actual_bytes,
                max_bytes,
            } => format!("tool output is {actual_bytes} bytes; maximum is {max_bytes} bytes"),
            Self::TimedOut { timeout_ms } => {
                format!("RocketMQ source timed out after {timeout_ms} ms")
            }
            Self::Cancelled => "RocketMQ source query was cancelled".to_string(),
        }
    }

    fn has_private_detail(&self) -> bool {
        matches!(self, Self::Backend(_) | Self::Internal(_))
    }
}

struct ToolOperationRecorder {
    metrics: McpMetricsRecorder,
    operation: &'static str,
    started_at: Instant,
    outcome: McpOperationOutcome,
    span: tracing::Span,
}

impl ToolOperationRecorder {
    fn new(metrics: McpMetricsRecorder, operation: &'static str) -> Self {
        Self {
            metrics,
            operation,
            started_at: Instant::now(),
            outcome: McpOperationOutcome::Failure,
            span: rocketmq_observability::trace::mcp::tool_span(operation),
        }
    }

    fn span(&self) -> tracing::Span {
        self.span.clone()
    }

    fn denied(&mut self) {
        self.outcome = McpOperationOutcome::Denied;
    }

    fn observe_call_result(&mut self, result: &Result<CallToolResult, ErrorData>) {
        if self.outcome == McpOperationOutcome::Denied {
            return;
        }
        self.outcome = match result {
            Ok(result) if !result.is_error.unwrap_or(false) => McpOperationOutcome::Success,
            Ok(_) | Err(_) => McpOperationOutcome::Failure,
        };
    }
}

impl Drop for ToolOperationRecorder {
    fn drop(&mut self) {
        rocketmq_observability::trace::mcp::record_outcome(&self.span, self.outcome);
        self.metrics.record_operation(
            McpOperationKind::Tool,
            self.operation,
            self.outcome,
            self.started_at.elapsed(),
        );
    }
}

#[derive(Debug, Serialize)]
struct ToolErrorContent<'a> {
    schema_version: &'static str,
    request_id: &'a str,
    correlation_id: &'a str,
    tool: &'a str,
    code: &'static str,
    retryable: bool,
    message: String,
    suggestions: Vec<&'static str>,
}

impl From<GuardError> for ToolExecutionError {
    fn from(error: GuardError) -> Self {
        match error {
            GuardError::InvalidArgument(message) => Self::InvalidArguments(message),
            GuardError::PermissionDenied(message) => Self::PermissionDenied(message),
            GuardError::UnauthorizedScope(message) => Self::UnauthorizedScope(message),
            GuardError::TenantMismatch(message) => Self::TenantMismatch(message),
            GuardError::ClusterNotAllowed(message) => Self::ClusterNotAllowed(message),
            GuardError::RateLimited(message) => Self::RateLimited(message),
            GuardError::ChangePlanningDisabled(message) => Self::ChangePlanningDisabled(message),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ToolExecutor<A> {
    adapter: A,
    guard: Guard,
    context: RequestContext,
    metrics: McpMetricsRecorder,
}

impl<A> ToolExecutor<A>
where
    A: ReadOnlyQuery,
{
    pub(crate) fn new(adapter: A, guard: Guard) -> Self {
        let context = guard.local_request_context();
        Self {
            adapter,
            guard,
            context,
            metrics: McpMetricsRecorder::noop(),
        }
    }

    pub(crate) fn with_metrics(mut self, metrics: McpMetricsRecorder) -> Self {
        self.metrics = metrics;
        self
    }

    pub(crate) fn with_request_context(mut self, context: RequestContext) -> Self {
        self.context = context;
        self
    }

    #[cfg(test)]
    pub(crate) async fn call(&self, request: CallToolRequestParams) -> Result<CallToolResult, ErrorData> {
        self.call_with_request_id(request, "test-request").await
    }

    pub(crate) async fn call_with_request_id(
        &self,
        request: CallToolRequestParams,
        request_id: &str,
    ) -> Result<CallToolResult, ErrorData> {
        let operation = ToolId::resolve(request.name.as_ref())
            .map(|tool_id| tool_id.descriptor().name)
            .unwrap_or("unknown_tool");
        let mut operation_recorder = ToolOperationRecorder::new(self.metrics.clone(), operation);
        let span = operation_recorder.span();
        let result = self
            .execute(request, request_id, &mut operation_recorder)
            .instrument(span)
            .await;
        operation_recorder.observe_call_result(&result);
        result
    }

    async fn execute(
        &self,
        request: CallToolRequestParams,
        request_id: &str,
        operation_recorder: &mut ToolOperationRecorder,
    ) -> Result<CallToolResult, ErrorData> {
        let tool_name = request.name.to_string();
        let tool_id = match ToolId::resolve(&tool_name) {
            Some(tool_id) => tool_id,
            None => {
                rocketmq_observability::metrics::mcp::record_error(
                    McpOperationKind::Tool,
                    "unknown_tool",
                    McpErrorKind::InvalidRequest,
                );
                return Err(ErrorData::invalid_params(format!("unknown tool: {tool_name}"), None));
            }
        };
        let descriptor = tool_id.descriptor();
        let arguments = request.arguments.unwrap_or_default();
        let audit_arguments = audit_arguments(tool_id, &arguments);
        let guarded_call =
            match self
                .guard
                .begin_tool_call(&self.context, &tool_name, descriptor.risk_level, &audit_arguments)
            {
                Ok(guarded_call) => guarded_call,
                Err(error) => {
                    operation_recorder.denied();
                    return Ok(error_result(descriptor.name, &tool_name, request_id, error.into()));
                }
            };

        if let Err(error) = validate_input(&descriptor, &arguments) {
            return Ok(guarded_call.finish_result(error_result(descriptor.name, &tool_name, request_id, error)));
        }

        let result = match tool_id {
            ToolId::GetClusterOverview => {
                let args = decode_args::<cluster_tools::ClusterOverviewArgs>(arguments.clone());
                let args = match args {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.cluster_overview(args).await.and_then(|output| {
                    let summary = summary_cluster_overview(&output);
                    let cluster = output.cluster.clone();
                    let resource = RocketmqResourceUri::new(cluster.clone(), ResourceKind::Overview);
                    success_result(descriptor, request_id, cluster, summary, output, resource)
                })
            }
            ToolId::ListTopics => {
                let args = match decode_args::<topic_tools::ListTopicsArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.list_topics(args).await.and_then(|output| {
                    let summary = summary_list_topics(&output);
                    let cluster = output.cluster.clone();
                    let resource = RocketmqResourceUri::new(cluster.clone(), ResourceKind::Topics);
                    success_result(descriptor, request_id, cluster, summary, output, resource)
                })
            }
            ToolId::DescribeTopic => {
                let args = match decode_args::<topic_tools::DescribeTopicArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.describe_topic(args).await.and_then(|output| {
                    let summary = summary_describe_topic(&output);
                    let cluster = output.cluster.clone();
                    let resource = RocketmqResourceUri::new(cluster.clone(), ResourceKind::Topic(output.topic.clone()));
                    success_result(descriptor, request_id, cluster, summary, output, resource)
                })
            }
            ToolId::GetTopicRoute => {
                let args = match decode_args::<topic_tools::QueryTopicRouteArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.query_topic_route(args).await.and_then(|output| {
                    let summary = summary_topic_route(&output);
                    let cluster = output.cluster.clone();
                    let resource =
                        RocketmqResourceUri::new(cluster.clone(), ResourceKind::TopicRoute(output.topic.clone()));
                    success_result(descriptor, request_id, cluster, summary, output, resource)
                })
            }
            ToolId::ListConsumerGroups => {
                let args = match decode_args::<consumer_tools::ListConsumerGroupsArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.list_consumer_groups(args).await.and_then(|output| {
                    let summary = summary_consumer_groups(&output);
                    let cluster = output.cluster.clone();
                    let resource = RocketmqResourceUri::new(cluster.clone(), ResourceKind::ConsumerGroups);
                    success_result(descriptor, request_id, cluster, summary, output, resource)
                })
            }
            ToolId::GetConsumerLag => {
                let args = match decode_args::<consumer_tools::QueryConsumerLagArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.query_consumer_lag(args).await.and_then(|output| {
                    let summary = summary_consumer_lag(&output);
                    let cluster = output.cluster.clone();
                    let resource = RocketmqResourceUri::new(
                        cluster.clone(),
                        ResourceKind::ConsumerLag {
                            group: output.consumer_group.clone(),
                            topic: output.topic.clone(),
                        },
                    );
                    success_result(descriptor, request_id, cluster, summary, output, resource)
                })
            }
            ToolId::DescribeBroker => {
                let args = match decode_args::<broker_tools::DescribeBrokerArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.describe_broker(args).await.and_then(|output| {
                    let summary = summary_describe_broker(&output);
                    let cluster = output.cluster.clone();
                    let resource =
                        RocketmqResourceUri::new(cluster.clone(), ResourceKind::Broker(output.broker_name.clone()));
                    success_result(descriptor, request_id, cluster, summary, output, resource)
                })
            }
            ToolId::GetBrokerDiagnostics => {
                let args = match decode_args::<broker_tools::BrokerDiagnosticsArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.broker_diagnostics(args).await.and_then(|output| {
                    let summary = summary_broker_diagnostics(&output);
                    let cluster = output.cluster.clone();
                    success_unlinked_result(descriptor, request_id, cluster, summary, output)
                })
            }
            ToolId::GetBrokerConfigSummary => {
                let args = match decode_args::<config_tools::BrokerConfigSummaryArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.broker_config_summary(args).await.and_then(|output| {
                    let summary = summary_broker_config(&output);
                    let cluster = output.cluster.clone();
                    success_unlinked_result(descriptor, request_id, cluster, summary, output)
                })
            }
            ToolId::GetBrokerLogFilterState => {
                let args = match decode_args::<config_tools::BrokerLogFilterStateArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.broker_log_filter_state(args).await.and_then(|output| {
                    let summary = summary_broker_log_filter(&output);
                    let cluster = output.cluster.clone();
                    success_unlinked_result(descriptor, request_id, cluster, summary, output)
                })
            }
            ToolId::GetProxyDrainState => {
                let args = match decode_args::<proxy_tools::ProxyDrainStateArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.proxy_drain_state(args).await.and_then(|output| {
                    let summary = summary_proxy_drain(&output);
                    let cluster = output.cluster.clone();
                    success_unlinked_result(descriptor, request_id, cluster, summary, output)
                })
            }
            ToolId::DiagnoseConsumerLag => {
                let args = match decode_args::<diagnosis_tools::DiagnoseConsumerLagArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                let cluster = args.cluster.clone();
                let resource = RocketmqResourceUri::new(
                    cluster.clone(),
                    ResourceKind::ConsumerLag {
                        group: args.consumer_group.clone(),
                        topic: args.topic.clone(),
                    },
                );
                self.adapter.diagnose_consumer_lag(args).await.and_then(|output| {
                    let summary = output.summary.clone();
                    success_result(descriptor, request_id, cluster, summary, output, resource)
                })
            }
            ToolId::ListConsumerConnections => {
                let args = match decode_args::<connection_tools::ListConsumerConnectionsArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.list_consumer_connections(args).await.and_then(|output| {
                    let summary = summary_consumer_connections(&output);
                    let cluster = output.cluster.clone();
                    success_unlinked_result(descriptor, request_id, cluster, summary, output)
                })
            }
            ToolId::ListProducerConnections => {
                let args = match decode_args::<connection_tools::ListProducerConnectionsArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.list_producer_connections(args).await.and_then(|output| {
                    let summary = summary_producer_connections(&output);
                    let cluster = output.cluster.clone();
                    success_unlinked_result(descriptor, request_id, cluster, summary, output)
                })
            }
            ToolId::GetMessageMetadata => {
                let args = match decode_args::<message_tools::MessageMetadataArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.message_metadata(args).await.and_then(|output| {
                    let summary = summary_message_metadata(&output);
                    let cluster = output.cluster.clone();
                    success_unlinked_result(descriptor, request_id, cluster, summary, output)
                })
            }
            ToolId::GetTopicConfigState => {
                let args = match decode_args::<config_tools::TopicConfigStateArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.topic_config_state(args).await.and_then(|output| {
                    let summary = summary_topic_config_state(&output);
                    let cluster = output.cluster.clone();
                    success_unlinked_result(descriptor, request_id, cluster, summary, output)
                })
            }
            ToolId::GetConsumerGroupConfigState => {
                let args = match decode_args::<config_tools::ConsumerGroupConfigStateArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.consumer_group_config_state(args).await.and_then(|output| {
                    let summary = summary_consumer_group_config_state(&output);
                    let cluster = output.cluster.clone();
                    success_unlinked_result(descriptor, request_id, cluster, summary, output)
                })
            }
            ToolId::GetTopicStats => {
                let args = match decode_args::<topic_tools::GetTopicStatsArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.topic_stats(args).await.and_then(|output| {
                    let summary = summary_topic_stats(&output);
                    let cluster = output.cluster.clone();
                    success_unlinked_result(descriptor, request_id, cluster, summary, output)
                })
            }
            ToolId::GetTopicConfig => {
                let args = match decode_args::<config_tools::GetTopicConfigArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.topic_config(args).await.and_then(|output| {
                    let summary = summary_topic_config(&output);
                    let cluster = output.cluster.clone();
                    success_unlinked_result(descriptor, request_id, cluster, summary, output)
                })
            }
            ToolId::GetConsumerGroupDetails => {
                let args = match decode_args::<consumer_tools::GetConsumerGroupDetailsArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.consumer_group_details(args).await.and_then(|output| {
                    let summary = summary_consumer_group_details(&output);
                    let cluster = output.cluster.clone();
                    success_unlinked_result(descriptor, request_id, cluster, summary, output)
                })
            }
            ToolId::GetConsumerProgress => {
                let args = match decode_args::<consumer_tools::GetConsumerProgressArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                self.adapter.consumer_progress(args).await.and_then(|output| {
                    let summary = summary_consumer_progress(&output);
                    let cluster = output.cluster.clone();
                    success_unlinked_result(descriptor, request_id, cluster, summary, output)
                })
            }
            #[cfg(feature = "change-planning")]
            ToolId::PlanCreateTopic => {
                let args = match decode_args::<change_tools::CreateTopicArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                let cluster = args.cluster.clone();
                self.adapter
                    .list_topics(topic_tools::ListTopicsArgs {
                        cluster: Some(cluster.clone()),
                        filter: None,
                        page: crate::model::contract::PageRequest::default(),
                    })
                    .await
                    .and_then(|current| {
                        let current = canonical_current_state(&current)?;
                        let output = change_tools::plan_create_topic_with_current_state(args, current);
                        let summary = summary_change_plan(&output);
                        success_live_result(descriptor, request_id, cluster, summary, output)
                    })
            }
            #[cfg(feature = "change-planning")]
            ToolId::PlanUpdateTopicConfig => {
                let args = match decode_args::<change_tools::UpdateTopicConfigArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                let cluster = args.cluster.clone();
                self.adapter
                    .describe_topic(topic_tools::DescribeTopicArgs {
                        cluster: cluster.clone(),
                        topic: args.desired.topic.clone(),
                        page: crate::model::contract::PageRequest::default(),
                    })
                    .await
                    .and_then(|current| {
                        let current = canonical_current_state(&current)?;
                        let output = change_tools::plan_update_topic_config_with_current_state(args, current);
                        let summary = summary_change_plan(&output);
                        success_live_result(descriptor, request_id, cluster, summary, output)
                    })
            }
            #[cfg(feature = "change-planning")]
            ToolId::PlanUpdateTopicPermissions => {
                let args = match decode_args::<change_tools::UpdateTopicPermArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                let cluster = args.cluster.clone();
                self.adapter
                    .describe_topic(topic_tools::DescribeTopicArgs {
                        cluster: cluster.clone(),
                        topic: args.desired.topic.clone(),
                        page: crate::model::contract::PageRequest::default(),
                    })
                    .await
                    .and_then(|current| {
                        let current = canonical_current_state(&current)?;
                        let output = change_tools::plan_update_topic_perm_with_current_state(args, current);
                        let summary = summary_change_plan(&output);
                        success_live_result(descriptor, request_id, cluster, summary, output)
                    })
            }
            #[cfg(feature = "change-planning")]
            ToolId::PlanUpdateBrokerConfig => {
                let args = match decode_args::<change_tools::UpdateBrokerConfigArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                let cluster = args.cluster.clone();
                self.adapter
                    .describe_broker(broker_tools::DescribeBrokerArgs {
                        cluster: cluster.clone(),
                        broker_name: args.desired.broker_name.clone(),
                    })
                    .await
                    .and_then(|current| {
                        let current = canonical_current_state(&current)?;
                        let output = change_tools::plan_update_broker_config_with_current_state(args, current);
                        let summary = summary_change_plan(&output);
                        success_live_result(descriptor, request_id, cluster, summary, output)
                    })
            }
            #[cfg(feature = "change-planning")]
            ToolId::PlanResetConsumerOffset => {
                let args = match decode_args::<change_tools::ResetConsumerOffsetArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(guarded_call.finish_result(error_result(
                            descriptor.name,
                            &tool_name,
                            request_id,
                            error,
                        )));
                    }
                };
                let cluster = args.cluster.clone();
                self.adapter
                    .query_consumer_lag(consumer_tools::QueryConsumerLagArgs {
                        cluster: cluster.clone(),
                        topic: args.desired.topic.clone(),
                        consumer_group: args.desired.consumer_group.clone(),
                        page: crate::model::contract::PageRequest::default(),
                    })
                    .await
                    .and_then(|current| {
                        let current = canonical_current_state(&current)?;
                        let output = change_tools::plan_reset_consumer_offset_with_current_state(args, current);
                        let summary = summary_change_plan(&output);
                        success_live_result(descriptor, request_id, cluster, summary, output)
                    })
            }
        };

        let result = result.unwrap_or_else(|error| error_result(descriptor.name, &tool_name, request_id, error));
        let result = guarded_call.finish_result(result);

        Ok(result)
    }
}

fn decode_args<T>(arguments: JsonObject) -> Result<T, ToolExecutionError>
where
    T: DeserializeOwned,
{
    serde_json::from_value(Value::Object(arguments))
        .map_err(|error| ToolExecutionError::InvalidArguments(error.to_string()))
}

fn validate_input(descriptor: &ToolDescriptor, arguments: &JsonObject) -> Result<(), ToolExecutionError> {
    let definition = descriptor.id.definition();
    validate_schema(
        definition.input_schema.as_ref(),
        &Value::Object(arguments.clone()),
        "input",
    )
    .map_err(ToolExecutionError::InvalidArguments)
}

fn success_result<T>(
    descriptor: ToolDescriptor,
    request_id: &str,
    cluster: String,
    summary: String,
    output: QueryResult<T>,
    resource: RocketmqResourceUri,
) -> Result<CallToolResult, ToolExecutionError>
where
    T: Serialize,
{
    let envelope = ToolResponse::from_query(request_id, cluster, output);
    render_success(descriptor, summary, envelope, Some(resource))
}

fn success_unlinked_result<T>(
    descriptor: ToolDescriptor,
    request_id: &str,
    cluster: String,
    summary: String,
    output: QueryResult<T>,
) -> Result<CallToolResult, ToolExecutionError>
where
    T: Serialize,
{
    let envelope = ToolResponse::from_query(request_id, cluster, output);
    render_success(descriptor, summary, envelope, None)
}

#[cfg(feature = "change-planning")]
fn success_live_result<T>(
    descriptor: ToolDescriptor,
    request_id: &str,
    cluster: String,
    summary: String,
    output: T,
) -> Result<CallToolResult, ToolExecutionError>
where
    T: Serialize,
{
    render_success(
        descriptor,
        summary,
        ToolResponse::live(request_id, cluster, output),
        None,
    )
}

fn render_success<T>(
    descriptor: ToolDescriptor,
    summary: String,
    envelope: ToolResponse<T>,
    resource: Option<RocketmqResourceUri>,
) -> Result<CallToolResult, ToolExecutionError>
where
    T: Serialize,
{
    let structured = serde_json::to_value(envelope).map_err(ToolExecutionError::internal)?;
    let structured = output_policy::apply(structured)?;
    let definition = descriptor.id.definition();
    let output_schema = definition
        .output_schema
        .as_ref()
        .ok_or_else(|| ToolExecutionError::Internal("Tool output schema is missing".to_string()))?;
    validate_schema(output_schema.as_ref(), &structured, "output").map_err(ToolExecutionError::internal)?;
    let json_text = serde_json::to_string(&structured).map_err(ToolExecutionError::internal)?;
    let mut content = vec![ContentBlock::text(summary), ContentBlock::text(json_text)];
    if let Some(resource) = resource {
        content.push(resource_link(resource));
    }
    let mut result = CallToolResult::success(content);
    result.structured_content = Some(structured);
    Ok(result)
}

fn resource_link(uri: RocketmqResourceUri) -> ContentBlock {
    let resource = Resource::new(uri.as_string(), uri.name())
        .with_title(uri.kind.title())
        .with_description(uri.kind.description())
        .with_mime_type(JSON_MIME_TYPE);
    ContentBlock::resource_link(resource)
}

fn validate_schema(schema: &JsonObject, value: &Value, label: &str) -> Result<(), String> {
    let schema = Value::Object(schema.clone());
    let validator = jsonschema::validator_for(&schema).map_err(|error| format!("invalid {label} schema: {error}"))?;
    let errors = validator
        .iter_errors(value)
        .take(3)
        .map(|error| format!("{}: {error}", error.instance_path()))
        .collect::<Vec<_>>();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(format!("{label} does not match schema: {}", errors.join("; ")))
    }
}

fn error_result(
    operation: &'static str,
    tool_name: &str,
    request_id: &str,
    error: ToolExecutionError,
) -> CallToolResult {
    rocketmq_observability::metrics::mcp::record_error(McpOperationKind::Tool, operation, error.metric_kind());
    if error.has_private_detail() {
        let detail = crate::guard::sanitizer::sanitize_text(&error.to_string());
        tracing::warn!(
            correlation_id = request_id,
            tool = operation,
            code = error.code(),
            detail = %detail,
            "MCP Tool execution failed"
        );
    }
    let content = ToolErrorContent {
        schema_version: crate::model::contract::SCHEMA_VERSION,
        request_id,
        correlation_id: request_id,
        tool: tool_name,
        code: error.code(),
        retryable: error.retryable(),
        message: error.public_message(),
        suggestions: error.suggestions(),
    };
    let text =
        serde_json::to_string(&content).unwrap_or_else(|_| format!("{tool_name} failed; request_id={request_id}"));
    CallToolResult::error(vec![ContentBlock::text(text)])
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
    format!(
        "Cluster {} returned {} of {} topics.",
        output.cluster, output.page.count, output.page.total_count
    )
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
        output.page.total_count
    )
}

fn summary_consumer_groups(output: &consumer_tools::ListConsumerGroupsOutput) -> String {
    format!(
        "Cluster {} returned {} of {} consumer groups.",
        output.cluster, output.page.count, output.page.total_count
    )
}

fn summary_consumer_lag(output: &consumer_tools::QueryConsumerLagOutput) -> String {
    format!(
        "Consumer group {} has total lag {} on topic {} across {} queues.",
        output.consumer_group, output.total_lag, output.topic, output.page.total_count
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

fn summary_broker_diagnostics(output: &broker_tools::BrokerDiagnosticsOutput) -> String {
    format!(
        "Broker {} on cluster {} returned {} diagnostic rows.",
        output.broker_name,
        output.cluster,
        output.brokers.len()
    )
}

fn summary_broker_config(output: &config_tools::BrokerConfigSummaryOutput) -> String {
    format!(
        "Broker {} on cluster {} returned {} allowlisted configuration rows.",
        output.broker_name,
        output.cluster,
        output.brokers.len()
    )
}

fn summary_broker_log_filter(output: &config_tools::BrokerLogFilterStateOutput) -> String {
    format!(
        "Broker {} on cluster {} returned {} log-filter state rows for {}.",
        output.broker_name,
        output.cluster,
        output.brokers.len(),
        output.logger
    )
}

fn summary_proxy_drain(output: &proxy_tools::ProxyDrainStateOutput) -> String {
    format!(
        "Proxy {} on cluster {} is in {:?} drain phase.",
        output.proxy_name, output.cluster, output.phase
    )
}

fn audit_arguments(tool_id: ToolId, arguments: &JsonObject) -> JsonObject {
    let mut audit_arguments = arguments.clone();
    if tool_id == ToolId::GetMessageMetadata && audit_arguments.contains_key("message_id") {
        audit_arguments.insert("message_id".to_string(), Value::String("<redacted>".to_string()));
    }
    audit_arguments
}

fn summary_consumer_connections(output: &connection_tools::ListConsumerConnectionsOutput) -> String {
    format!(
        "Consumer group {} on cluster {} returned {} of {} pseudonymous connections.",
        output.consumer_group, output.cluster, output.page.count, output.page.total_count
    )
}

fn summary_producer_connections(output: &connection_tools::ListProducerConnectionsOutput) -> String {
    format!(
        "Producer group {} for Topic {} on cluster {} returned {} of {} pseudonymous connections.",
        output.producer_group, output.topic, output.cluster, output.page.count, output.page.total_count
    )
}

fn summary_message_metadata(output: &message_tools::MessageMetadataOutput) -> String {
    format!(
        "Message {} on cluster {} belongs to Topic {} without body or property disclosure.",
        output.message_alias, output.cluster, output.topic
    )
}

fn summary_topic_config_state(output: &config_tools::TopicConfigStateOutput) -> String {
    format!(
        "Topic {} on cluster {} returned {} logical Broker version states.",
        output.topic,
        output.cluster,
        output.brokers.len()
    )
}

fn summary_consumer_group_config_state(output: &config_tools::ConsumerGroupConfigStateOutput) -> String {
    format!(
        "Consumer group {} on cluster {} returned {} logical Broker version states.",
        output.group,
        output.cluster,
        output.brokers.len()
    )
}

fn summary_topic_stats(output: &topic_tools::GetTopicStatsOutput) -> String {
    format!(
        "Topic {} on cluster {} returned {} of {} queue statistics.",
        output.topic, output.cluster, output.page.count, output.queue_count
    )
}

fn summary_topic_config(output: &config_tools::GetTopicConfigOutput) -> String {
    format!(
        "Topic {} on cluster {} returned {} Broker configurations with {} semantic differences.",
        output.topic,
        output.cluster,
        output.brokers.len(),
        output.inconsistent_fields.len()
    )
}

fn summary_consumer_group_details(output: &consumer_tools::GetConsumerGroupDetailsOutput) -> String {
    format!(
        "Consumer group {} on cluster {} returned {} Broker observations and {} total connections.",
        output.consumer_group,
        output.cluster,
        output.brokers.len(),
        output.total_connection_count
    )
}

fn summary_consumer_progress(output: &consumer_tools::GetConsumerProgressOutput) -> String {
    format!(
        "Consumer group {} on cluster {} returned {} of {} queue progress rows with total lag {}.",
        output.consumer_group, output.cluster, output.page.count, output.queue_count, output.total_lag
    )
}

#[cfg(feature = "change-planning")]
fn summary_change_plan(output: &change_tools::ChangePlan) -> String {
    format!(
        "Generated {:?} with {} planned changes for cluster {}; no mutation was applied.",
        output.plan_type,
        output.planned_changes.len(),
        output.cluster
    )
}

#[cfg(feature = "change-planning")]
fn canonical_current_state<T: Serialize>(current: &QueryResult<T>) -> Result<Value, ToolExecutionError> {
    let mut state = serde_json::to_value(&current.data).map_err(ToolExecutionError::internal)?;
    remove_transient_state_fields(&mut state);
    Ok(state)
}

#[cfg(feature = "change-planning")]
fn remove_transient_state_fields(value: &mut Value) {
    match value {
        Value::Array(values) => values.iter_mut().for_each(remove_transient_state_fields),
        Value::Object(values) => {
            values.remove("generated_at");
            values.values_mut().for_each(remove_transient_state_fields);
        }
        _ => {}
    }
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
        partial: bool,
    }

    impl ReadOnlyQuery for FakeAdapter {
        async fn cluster_overview(
            &self,
            args: cluster_tools::ClusterOverviewArgs,
        ) -> Result<QueryResult<cluster_tools::ClusterOverviewOutput>, ToolExecutionError> {
            if self.fail {
                return Err(ToolExecutionError::backend(
                    "nameserver 10.24.7.9:9876 unavailable token=super-secret",
                ));
            }
            let mut result = QueryResult::bypass(cluster_tools::ClusterOverviewOutput {
                cluster: args.cluster,
                namesrv_addr: "127.0.0.1:9876".to_string(),
                brokers: vec![broker_summary()],
                topic_count: 2,
                consumer_group_count: 1,
                generated_at: "1".to_string(),
            });
            if self.partial {
                result.partial = true;
                result.warnings = vec!["source_failures_present".to_string()];
                result.source_failures = vec![crate::model::contract::SourceFailure::new(
                    crate::model::contract::QuerySource::BrokerRuntime,
                    crate::model::contract::SourceFailureCode::Timeout,
                    true,
                    "broker-b",
                )];
            }
            Ok(result)
        }

        async fn list_topics(
            &self,
            args: topic_tools::ListTopicsArgs,
        ) -> Result<QueryResult<topic_tools::ListTopicsOutput>, ToolExecutionError> {
            Ok(QueryResult::bypass(topic_tools::ListTopicsOutput {
                cluster: args.cluster.unwrap_or_else(|| "local-dev".to_string()),
                namesrv_addr: "127.0.0.1:9876".to_string(),
                page: crate::model::contract::Page {
                    items: Vec::new(),
                    count: 0,
                    total_count: 0,
                    has_more: false,
                    next_cursor: None,
                },
                generated_at: "transient-test-time".to_string(),
            }))
        }

        async fn describe_topic(
            &self,
            _args: topic_tools::DescribeTopicArgs,
        ) -> Result<QueryResult<topic_tools::DescribeTopicOutput>, ToolExecutionError> {
            unimplemented!("not needed by this test")
        }

        async fn query_topic_route(
            &self,
            _args: topic_tools::QueryTopicRouteArgs,
        ) -> Result<QueryResult<topic_tools::QueryTopicRouteOutput>, ToolExecutionError> {
            unimplemented!("not needed by this test")
        }

        async fn list_consumer_groups(
            &self,
            _args: consumer_tools::ListConsumerGroupsArgs,
        ) -> Result<QueryResult<consumer_tools::ListConsumerGroupsOutput>, ToolExecutionError> {
            unimplemented!("not needed by this test")
        }

        async fn query_consumer_lag(
            &self,
            _args: consumer_tools::QueryConsumerLagArgs,
        ) -> Result<QueryResult<consumer_tools::QueryConsumerLagOutput>, ToolExecutionError> {
            unimplemented!("not needed by this test")
        }

        async fn describe_broker(
            &self,
            _args: broker_tools::DescribeBrokerArgs,
        ) -> Result<QueryResult<broker_tools::DescribeBrokerOutput>, ToolExecutionError> {
            unimplemented!("not needed by this test")
        }

        async fn broker_diagnostics(
            &self,
            args: broker_tools::BrokerDiagnosticsArgs,
        ) -> Result<QueryResult<broker_tools::BrokerDiagnosticsOutput>, ToolExecutionError> {
            if self.fail {
                return Err(ToolExecutionError::backend(
                    "private-proxy-endpoint.example:18081 token=super-secret",
                ));
            }
            Ok(QueryResult::bypass(broker_tools::BrokerDiagnosticsOutput {
                cluster: args.cluster,
                broker_name: args.broker_name,
                diagnostics_schema_version: "rocketmq.admin-broker-diagnostics.v1".to_string(),
                observed_at_millis: 1,
                brokers: Vec::new(),
                unavailable_brokers: 0,
            }))
        }

        async fn broker_config_summary(
            &self,
            args: config_tools::BrokerConfigSummaryArgs,
        ) -> Result<QueryResult<config_tools::BrokerConfigSummaryOutput>, ToolExecutionError> {
            Ok(QueryResult::bypass(config_tools::BrokerConfigSummaryOutput {
                cluster: args.cluster,
                broker_name: args.broker_name,
                brokers: Vec::new(),
            }))
        }

        async fn broker_log_filter_state(
            &self,
            args: config_tools::BrokerLogFilterStateArgs,
        ) -> Result<QueryResult<config_tools::BrokerLogFilterStateOutput>, ToolExecutionError> {
            Ok(QueryResult::bypass(config_tools::BrokerLogFilterStateOutput {
                cluster: args.cluster,
                broker_name: args.broker_name,
                logger: args.logger,
                brokers: Vec::new(),
            }))
        }

        async fn proxy_drain_state(
            &self,
            args: proxy_tools::ProxyDrainStateArgs,
        ) -> Result<QueryResult<proxy_tools::ProxyDrainStateOutput>, ToolExecutionError> {
            if self.fail {
                return Err(ToolExecutionError::backend(
                    "private-proxy-endpoint.example:18081 token=super-secret",
                ));
            }
            Ok(QueryResult::bypass(proxy_tools::ProxyDrainStateOutput {
                cluster: args.cluster,
                proxy_name: args.proxy_name,
                state_schema_version: "rocketmq.proxy-drain.v1".to_string(),
                phase: proxy_tools::ProxyDrainPhase::Accepting,
                operation_id: None,
                admission_open: true,
                routing_open: true,
                readiness_published: true,
                zero_pending: true,
                pending: proxy_tools::ProxyDrainPending {
                    active_connections: 0,
                    sessions: 0,
                    receipt_handles: 0,
                    prepared_transactions: 0,
                    telemetry_links: 0,
                    remoting_channels: 0,
                    telemetry_commands: 0,
                    rpc_in_flight: 0,
                },
            }))
        }

        async fn list_consumer_connections(
            &self,
            args: connection_tools::ListConsumerConnectionsArgs,
        ) -> Result<QueryResult<connection_tools::ListConsumerConnectionsOutput>, ToolExecutionError> {
            let oversized = args.consumer_group == "oversized";
            let items = oversized
                .then(|| connection_tools::ConnectionRow {
                    broker_name: "broker-a".to_string(),
                    client_alias: format!("client-{}", "x".repeat(2 * 1024 * 1024)),
                    language: "RUST".to_string(),
                    version: 1,
                    last_update_at: None,
                })
                .into_iter()
                .collect::<Vec<_>>();
            Ok(QueryResult::bypass(connection_tools::ListConsumerConnectionsOutput {
                cluster: args.cluster,
                consumer_group: args.consumer_group,
                queried_broker_count: 1,
                page: crate::model::contract::Page {
                    count: items.len(),
                    total_count: items.len(),
                    items,
                    has_more: false,
                    next_cursor: None,
                },
                generated_at: "transient-test-time".to_string(),
            }))
        }

        async fn list_producer_connections(
            &self,
            args: connection_tools::ListProducerConnectionsArgs,
        ) -> Result<QueryResult<connection_tools::ListProducerConnectionsOutput>, ToolExecutionError> {
            let oversized = args.producer_group == "oversized";
            let items = oversized
                .then(|| connection_tools::ConnectionRow {
                    broker_name: "broker-a".to_string(),
                    client_alias: format!("client-{}", "x".repeat(2 * 1024 * 1024)),
                    language: "RUST".to_string(),
                    version: 1,
                    last_update_at: None,
                })
                .into_iter()
                .collect::<Vec<_>>();
            Ok(QueryResult::bypass(connection_tools::ListProducerConnectionsOutput {
                cluster: args.cluster,
                topic: args.topic,
                producer_group: args.producer_group,
                queried_broker_count: 1,
                page: crate::model::contract::Page {
                    count: items.len(),
                    total_count: items.len(),
                    items,
                    has_more: false,
                    next_cursor: None,
                },
                generated_at: "transient-test-time".to_string(),
            }))
        }

        async fn message_metadata(
            &self,
            args: message_tools::MessageMetadataArgs,
        ) -> Result<QueryResult<message_tools::MessageMetadataOutput>, ToolExecutionError> {
            let topic = if args.message_id == "oversized" {
                "x".repeat(2 * 1024 * 1024)
            } else {
                "orders".to_string()
            };
            Ok(QueryResult::bypass(message_tools::MessageMetadataOutput {
                cluster: args.cluster,
                message_alias: "message-00000000000000000000000000000000".to_string(),
                unique_message_alias: None,
                topic,
                born_at: None,
                stored_at: None,
                queue_id: 0,
                queue_offset: 0,
                store_size: 0,
                reconsume_times: 0,
                sys_flag: 0,
                flag: 0,
                prepared_transaction_offset: 0,
            }))
        }

        async fn topic_config_state(
            &self,
            args: config_tools::TopicConfigStateArgs,
        ) -> Result<QueryResult<config_tools::TopicConfigStateOutput>, ToolExecutionError> {
            let brokers = (args.topic == "oversized")
                .then(|| config_tools::TopicConfigStateRow {
                    broker_name: "x".repeat(2 * 1024 * 1024),
                    version: 1,
                    read_queue_nums: 1,
                    write_queue_nums: 1,
                    order: false,
                })
                .into_iter()
                .collect();
            Ok(QueryResult::bypass(config_tools::TopicConfigStateOutput {
                cluster: args.cluster,
                topic: args.topic,
                brokers,
            }))
        }

        async fn consumer_group_config_state(
            &self,
            args: config_tools::ConsumerGroupConfigStateArgs,
        ) -> Result<QueryResult<config_tools::ConsumerGroupConfigStateOutput>, ToolExecutionError> {
            let brokers = (args.group == "oversized")
                .then(|| config_tools::ConsumerGroupConfigStateRow {
                    broker_name: "x".repeat(2 * 1024 * 1024),
                    version: 1,
                    retry_max_times: 1,
                    retry_queue_nums: 1,
                    consume_timeout_minutes: 1,
                    consume_enable: true,
                    consume_from_min_enable: false,
                    consume_broadcast_enable: false,
                    consume_message_orderly: false,
                    broker_id: 0,
                    which_broker_when_consume_slowly: 1,
                    notify_consumer_ids_changed_enable: true,
                    group_sys_flag: 0,
                })
                .into_iter()
                .collect();
            Ok(QueryResult::bypass(config_tools::ConsumerGroupConfigStateOutput {
                cluster: args.cluster,
                group: args.group,
                brokers,
            }))
        }

        async fn topic_stats(
            &self,
            args: topic_tools::GetTopicStatsArgs,
        ) -> Result<QueryResult<topic_tools::GetTopicStatsOutput>, ToolExecutionError> {
            let broker_name = if args.topic == "oversized" {
                "x".repeat(2 * 1024 * 1024)
            } else {
                "broker-a".to_string()
            };
            let items = vec![topic_tools::TopicStatsQueueRow {
                broker_name,
                queue_id: 0,
                min_offset: 0,
                max_offset: 10,
                message_count: 10,
                last_update_at: None,
            }];
            Ok(QueryResult::bypass(topic_tools::GetTopicStatsOutput {
                cluster: args.cluster,
                topic: args.topic,
                total_message_count: 10,
                queue_count: 1,
                truncated: false,
                page: crate::model::contract::Page {
                    count: items.len(),
                    total_count: items.len(),
                    items,
                    has_more: false,
                    next_cursor: None,
                },
                generated_at: "transient-test-time".to_string(),
            }))
        }

        async fn topic_config(
            &self,
            args: config_tools::GetTopicConfigArgs,
        ) -> Result<QueryResult<config_tools::GetTopicConfigOutput>, ToolExecutionError> {
            let broker_name = if args.topic == "oversized" {
                "x".repeat(2 * 1024 * 1024)
            } else {
                "broker-a".to_string()
            };
            Ok(QueryResult::bypass(config_tools::GetTopicConfigOutput {
                cluster: args.cluster,
                topic: args.topic,
                brokers: vec![config_tools::TopicConfigObservationRow {
                    broker_name,
                    version: 1,
                    read_queue_nums: 8,
                    write_queue_nums: 8,
                    perm: 6,
                    order: false,
                    message_type: "NORMAL".to_string(),
                }],
                inconsistent_fields: Vec::new(),
                generated_at: "transient-test-time".to_string(),
            }))
        }

        async fn consumer_group_details(
            &self,
            args: consumer_tools::GetConsumerGroupDetailsArgs,
        ) -> Result<QueryResult<consumer_tools::GetConsumerGroupDetailsOutput>, ToolExecutionError> {
            let broker_name = if args.consumer_group == "oversized" {
                "x".repeat(2 * 1024 * 1024)
            } else {
                "broker-a".to_string()
            };
            Ok(QueryResult::bypass(consumer_tools::GetConsumerGroupDetailsOutput {
                cluster: args.cluster,
                consumer_group: args.consumer_group,
                total_connection_count: 0,
                brokers: vec![consumer_tools::ConsumerGroupDetailsBrokerRow {
                    broker_name,
                    config_state: consumer_tools::ConsumerGroupConfigPresence::Present,
                    config_version: Some(1),
                    consume_enable: Some(true),
                    consume_from_min_enable: Some(false),
                    consume_broadcast_enable: Some(false),
                    consume_message_orderly: Some(false),
                    retry_queue_nums: Some(1),
                    retry_max_times: Some(1),
                    notify_consumer_ids_changed_enable: Some(true),
                    consume_timeout_minutes: Some(1),
                    connection_state: Some(consumer_tools::ConsumerConnectionState::Offline),
                    connection_count: 0,
                    consume_type: None,
                    message_model: None,
                    consume_from_where: None,
                }],
                generated_at: "transient-test-time".to_string(),
            }))
        }

        async fn consumer_progress(
            &self,
            args: consumer_tools::GetConsumerProgressArgs,
        ) -> Result<QueryResult<consumer_tools::GetConsumerProgressOutput>, ToolExecutionError> {
            let broker_name = if args.consumer_group == "oversized" {
                "x".repeat(2 * 1024 * 1024)
            } else {
                "broker-a".to_string()
            };
            let items = vec![consumer_tools::ConsumerProgressQueueRow {
                topic: "orders".to_string(),
                broker_name,
                queue_id: 0,
                broker_offset: 1,
                consumer_offset: 1,
                pull_offset: 1,
                lag: 0,
                inflight: 0,
                last_observed_at: None,
            }];
            Ok(QueryResult::bypass(consumer_tools::GetConsumerProgressOutput {
                cluster: args.cluster,
                consumer_group: args.consumer_group,
                state: consumer_tools::ConsumerProgressState::Observed,
                topic_count: 1,
                queue_count: 1,
                total_lag: 0,
                max_queue_lag: 0,
                total_inflight: 0,
                consume_tps: 0.0,
                truncated: false,
                page: crate::model::contract::Page {
                    count: items.len(),
                    total_count: items.len(),
                    items,
                    has_more: false,
                    next_cursor: None,
                },
                generated_at: "transient-test-time".to_string(),
            }))
        }

        async fn diagnose_consumer_lag(
            &self,
            _args: diagnosis_tools::DiagnoseConsumerLagArgs,
        ) -> Result<QueryResult<crate::model::diagnosis::DiagnosisReport>, ToolExecutionError> {
            unimplemented!("not needed by this test")
        }
    }

    #[tokio::test]
    async fn call_returns_summary_and_structured_content() {
        let guard = test_guard("diagnose");
        let result = ToolExecutor::new(
            FakeAdapter {
                fail: false,
                partial: false,
            },
            guard.clone(),
        )
        .call(
            CallToolRequestParams::new(ToolId::GetClusterOverview.descriptor().name).with_arguments(
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
        let structured = result.structured_content.as_ref().unwrap();
        assert_eq!(structured["schema_version"], "rocketmq-mcp.v2");
        assert_eq!(structured["request_id"], "test-request");
        assert_eq!(structured["cluster"], "local-dev");
        assert!(chrono::DateTime::parse_from_rfc3339(structured["observed_at"].as_str().unwrap()).is_ok());
        assert!(structured["data"].get("namesrv_addr").is_none());
        assert!(structured["data"]["brokers"][0].get("broker_addr").is_none());
        assert!(!result.content.is_empty());
        let resource_link = result.content.iter().find_map(ContentBlock::as_resource_link).unwrap();
        assert_eq!(resource_link.uri, "rocketmq://clusters/local-dev/overview");
        let records = guard.audit_log().records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].tool, ToolId::GetClusterOverview.descriptor().name);
        assert_eq!(records[0].cluster.as_deref(), Some("local-dev"));
        assert_eq!(records[0].status, AuditStatus::Success);
    }

    #[tokio::test]
    async fn tool_response_propagates_partial_source_failures() {
        let result = ToolExecutor::new(
            FakeAdapter {
                fail: false,
                partial: true,
            },
            test_guard("diagnose"),
        )
        .call(
            CallToolRequestParams::new(ToolId::GetClusterOverview.descriptor().name)
                .with_arguments(serde_json::json!({"cluster": "local-dev"}).as_object().unwrap().clone()),
        )
        .await
        .unwrap();

        let structured = result.structured_content.unwrap();
        assert_eq!(structured["partial"], true);
        assert_eq!(structured["warnings"], serde_json::json!(["source_failures_present"]));
        assert_eq!(structured["source_failures"][0]["source"], "broker_runtime");
        assert_eq!(structured["source_failures"][0]["logical_target"], "broker-b");
    }

    #[tokio::test]
    async fn backend_error_is_returned_as_source_unavailable() {
        let guard = test_guard("diagnose");
        let result = ToolExecutor::new(
            FakeAdapter {
                fail: true,
                partial: false,
            },
            guard.clone(),
        )
        .call(
            CallToolRequestParams::new(ToolId::GetClusterOverview.descriptor().name).with_arguments(
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
        assert!(result.structured_content.is_none());
        let error: serde_json::Value = serde_json::from_str(&content_text(&result)).unwrap();
        assert_eq!(error["code"], "source_unavailable");
        assert_eq!(error["retryable"], true);
        assert_eq!(error["request_id"], "test-request");
        assert_eq!(error["message"], "RocketMQ source is unavailable");
        assert!(!content_text(&result).contains("super-secret"));
        assert!(!content_text(&result).contains("10.24.7.9"));
        assert!(!content_text(&result).contains("nameserver"));
        let records = guard.audit_log().records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, AuditStatus::Failure);
    }

    #[tokio::test]
    async fn invalid_arguments_are_actionable_tool_errors() {
        let result = ToolExecutor::new(
            FakeAdapter {
                fail: false,
                partial: false,
            },
            test_guard("diagnose"),
        )
        .call(CallToolRequestParams::new(ToolId::GetClusterOverview.descriptor().name))
        .await
        .unwrap();

        assert_eq!(result.is_error, Some(true));
        assert!(result.structured_content.is_none());
        let error: serde_json::Value = serde_json::from_str(&content_text(&result)).unwrap();
        assert_eq!(error["code"], "invalid_arguments");
        assert_eq!(error["retryable"], false);
        assert_eq!(error["request_id"], "test-request");
        assert!(!error["suggestions"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unknown_tool_returns_protocol_error() {
        let err = ToolExecutor::new(
            FakeAdapter {
                fail: false,
                partial: false,
            },
            test_guard("diagnose"),
        )
        .call(CallToolRequestParams::new("unknown_tool"))
        .await
        .unwrap_err();

        assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn read_only_guard_denies_diagnosis_tool() {
        let guard = test_guard("read_only");
        let result = ToolExecutor::new(
            FakeAdapter {
                fail: false,
                partial: false,
            },
            guard.clone(),
        )
        .call(
            CallToolRequestParams::new(ToolId::DiagnoseConsumerLag.descriptor().name).with_arguments(
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
        let error: serde_json::Value = serde_json::from_str(&content_text(&result)).unwrap();
        assert_eq!(error["code"], "unauthorized_scope");
        assert_eq!(error["correlation_id"], "test-request");
        assert!(error["message"].as_str().unwrap().contains("permission denied"));
        let records = guard.audit_log().records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, AuditStatus::Failure);
    }

    #[tokio::test]
    async fn broker_and_proxy_tools_dispatch_without_resource_links() {
        let executor = ToolExecutor::new(
            FakeAdapter {
                fail: false,
                partial: false,
            },
            test_guard("diagnose"),
        );
        let calls = [
            (
                ToolId::GetBrokerDiagnostics,
                serde_json::json!({"cluster": "local-dev", "broker_name": "broker-a"}),
            ),
            (
                ToolId::GetBrokerConfigSummary,
                serde_json::json!({"cluster": "local-dev", "broker_name": "broker-a"}),
            ),
            (
                ToolId::GetBrokerLogFilterState,
                serde_json::json!({
                    "cluster": "local-dev",
                    "broker_name": "broker-a",
                    "logger": "rocketmq_broker::processor"
                }),
            ),
            (
                ToolId::GetProxyDrainState,
                serde_json::json!({"cluster": "local-dev", "proxy_name": "proxy-a"}),
            ),
        ];
        for (tool, arguments) in calls {
            let result = executor
                .call(
                    CallToolRequestParams::new(tool.descriptor().name)
                        .with_arguments(arguments.as_object().unwrap().clone()),
                )
                .await
                .unwrap();
            assert_eq!(result.is_error, Some(false), "{}", tool.descriptor().name);
            assert!(result
                .content
                .iter()
                .all(|content| content.as_resource_link().is_none()));
            assert!(result.structured_content.is_some());
        }
    }

    #[tokio::test]
    async fn broker_and_proxy_tools_reject_unknown_fields_and_enforce_scopes() {
        let diagnose = ToolExecutor::new(
            FakeAdapter {
                fail: false,
                partial: false,
            },
            test_guard("diagnose"),
        );
        let invalid_calls = [
            (
                ToolId::GetBrokerDiagnostics,
                serde_json::json!({"cluster": "local-dev", "broker_name": "broker-a", "extra": true}),
            ),
            (
                ToolId::GetBrokerConfigSummary,
                serde_json::json!({"cluster": "local-dev", "broker_name": "broker-a", "extra": true}),
            ),
            (
                ToolId::GetBrokerLogFilterState,
                serde_json::json!({
                    "cluster": "local-dev",
                    "broker_name": "broker-a",
                    "logger": "rocketmq_broker::processor",
                    "extra": true
                }),
            ),
            (
                ToolId::GetProxyDrainState,
                serde_json::json!({"cluster": "local-dev", "proxy_name": "proxy-a", "endpoint": "secret"}),
            ),
        ];
        for (tool, arguments) in invalid_calls {
            let invalid = diagnose
                .call(
                    CallToolRequestParams::new(tool.descriptor().name)
                        .with_arguments(arguments.as_object().unwrap().clone()),
                )
                .await
                .unwrap();
            assert_eq!(invalid.is_error, Some(true), "{}", tool.descriptor().name);
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&content_text(&invalid)).unwrap()["code"],
                "invalid_arguments"
            );
        }

        let read_only = ToolExecutor::new(
            FakeAdapter {
                fail: false,
                partial: false,
            },
            test_guard("read_only"),
        );
        let denied = read_only
            .call(
                CallToolRequestParams::new(ToolId::GetBrokerDiagnostics.descriptor().name).with_arguments(
                    serde_json::json!({"cluster": "local-dev", "broker_name": "broker-a"})
                        .as_object()
                        .unwrap()
                        .clone(),
                ),
            )
            .await
            .unwrap();
        assert_eq!(denied.is_error, Some(true));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&content_text(&denied)).unwrap()["code"],
            "unauthorized_scope"
        );

        let allowed = read_only
            .call(
                CallToolRequestParams::new(ToolId::GetBrokerConfigSummary.descriptor().name).with_arguments(
                    serde_json::json!({"cluster": "local-dev", "broker_name": "broker-a"})
                        .as_object()
                        .unwrap()
                        .clone(),
                ),
            )
            .await
            .unwrap();
        assert_eq!(allowed.is_error, Some(false));
    }

    #[tokio::test]
    async fn new_read_tools_dispatch_without_resource_links_and_reject_unknown_fields() {
        let executor = ToolExecutor::new(
            FakeAdapter {
                fail: false,
                partial: false,
            },
            test_guard("read_only"),
        );
        let calls = [
            (
                ToolId::ListConsumerConnections,
                serde_json::json!({"cluster":"local-dev","consumer_group":"group-a"}),
            ),
            (
                ToolId::ListProducerConnections,
                serde_json::json!({"cluster":"local-dev","topic":"orders","producer_group":"producer-a"}),
            ),
            (
                ToolId::GetMessageMetadata,
                serde_json::json!({"cluster":"local-dev","message_id":"raw-message-a"}),
            ),
            (
                ToolId::GetTopicConfigState,
                serde_json::json!({"cluster":"local-dev","topic":"orders","broker_names":["broker-a"]}),
            ),
            (
                ToolId::GetConsumerGroupConfigState,
                serde_json::json!({"cluster":"local-dev","group":"group-a","broker_names":["broker-a"]}),
            ),
            (
                ToolId::GetTopicStats,
                serde_json::json!({"cluster":"local-dev","topic":"orders","limit":1}),
            ),
            (
                ToolId::GetTopicConfig,
                serde_json::json!({"cluster":"local-dev","topic":"orders"}),
            ),
            (
                ToolId::GetConsumerGroupDetails,
                serde_json::json!({"cluster":"local-dev","consumer_group":"group-a"}),
            ),
            (
                ToolId::GetConsumerProgress,
                serde_json::json!({"cluster":"local-dev","consumer_group":"group-a","limit":1}),
            ),
        ];
        for (tool, arguments) in calls {
            let result = executor
                .call(
                    CallToolRequestParams::new(tool.descriptor().name)
                        .with_arguments(arguments.as_object().unwrap().clone()),
                )
                .await
                .unwrap();
            assert_eq!(result.is_error, Some(false), "{}", tool.descriptor().name);
            assert!(result.content.iter().all(|item| item.as_resource_link().is_none()));

            let mut invalid_arguments = arguments.as_object().unwrap().clone();
            invalid_arguments.insert("unexpected".to_string(), serde_json::Value::Bool(true));
            let invalid = executor
                .call(CallToolRequestParams::new(tool.descriptor().name).with_arguments(invalid_arguments))
                .await
                .unwrap();
            assert_eq!(invalid.is_error, Some(true), "{}", tool.descriptor().name);
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&content_text(&invalid)).unwrap()["code"],
                "invalid_arguments"
            );
        }
    }

    #[tokio::test]
    async fn new_read_tools_require_read_scope() {
        let guard = test_guard("read_only");
        let mut context = guard.local_request_context();
        context.principal.scopes.clear();
        let executor = ToolExecutor::new(
            FakeAdapter {
                fail: false,
                partial: false,
            },
            guard,
        )
        .with_request_context(context);
        let calls = [
            (
                ToolId::ListConsumerConnections,
                serde_json::json!({"cluster":"local-dev","consumer_group":"group-a"}),
            ),
            (
                ToolId::ListProducerConnections,
                serde_json::json!({"cluster":"local-dev","topic":"orders","producer_group":"producer-a"}),
            ),
            (
                ToolId::GetMessageMetadata,
                serde_json::json!({"cluster":"local-dev","message_id":"raw-message-a"}),
            ),
            (
                ToolId::GetTopicConfigState,
                serde_json::json!({"cluster":"local-dev","topic":"orders","broker_names":["broker-a"]}),
            ),
            (
                ToolId::GetConsumerGroupConfigState,
                serde_json::json!({"cluster":"local-dev","group":"group-a","broker_names":["broker-a"]}),
            ),
            (
                ToolId::GetTopicStats,
                serde_json::json!({"cluster":"local-dev","topic":"orders","limit":1}),
            ),
            (
                ToolId::GetTopicConfig,
                serde_json::json!({"cluster":"local-dev","topic":"orders"}),
            ),
            (
                ToolId::GetConsumerGroupDetails,
                serde_json::json!({"cluster":"local-dev","consumer_group":"group-a"}),
            ),
            (
                ToolId::GetConsumerProgress,
                serde_json::json!({"cluster":"local-dev","consumer_group":"group-a","limit":1}),
            ),
        ];
        for (tool, arguments) in calls {
            let denied = executor
                .call(
                    CallToolRequestParams::new(tool.descriptor().name)
                        .with_arguments(arguments.as_object().unwrap().clone()),
                )
                .await
                .unwrap();
            assert_eq!(denied.is_error, Some(true), "{}", tool.descriptor().name);
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&content_text(&denied)).unwrap()["code"],
                "unauthorized_scope"
            );
        }
    }

    #[tokio::test]
    async fn message_identifiers_are_redacted_before_audit_hashing() {
        let guard = test_guard("read_only");
        let executor = ToolExecutor::new(
            FakeAdapter {
                fail: false,
                partial: false,
            },
            guard.clone(),
        );
        for message_id in ["RAW-MESSAGE-ID-A", "RAW-MESSAGE-ID-B"] {
            let result = executor
                .call(
                    CallToolRequestParams::new(ToolId::GetMessageMetadata.descriptor().name).with_arguments(
                        serde_json::json!({"cluster":"local-dev","message_id":message_id})
                            .as_object()
                            .unwrap()
                            .clone(),
                    ),
                )
                .await
                .unwrap();
            assert_eq!(result.is_error, Some(false));
            assert!(!content_text(&result).contains(message_id));
        }
        let records = guard.audit_log().records();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].arguments_hash, records[1].arguments_hash);
        let audit_json = serde_json::to_string(&records).unwrap();
        assert!(!audit_json.contains("RAW-MESSAGE-ID-A"));
        assert!(!audit_json.contains("RAW-MESSAGE-ID-B"));
    }

    #[tokio::test]
    async fn new_read_tool_output_limit_is_enforced_by_executor() {
        let executor = ToolExecutor::new(
            FakeAdapter {
                fail: false,
                partial: false,
            },
            test_guard("read_only"),
        );
        let calls = [
            (
                ToolId::ListConsumerConnections,
                serde_json::json!({"cluster":"local-dev","consumer_group":"oversized"}),
            ),
            (
                ToolId::ListProducerConnections,
                serde_json::json!({"cluster":"local-dev","topic":"orders","producer_group":"oversized"}),
            ),
            (
                ToolId::GetMessageMetadata,
                serde_json::json!({"cluster":"local-dev","message_id":"oversized"}),
            ),
            (
                ToolId::GetTopicConfigState,
                serde_json::json!({"cluster":"local-dev","topic":"oversized","broker_names":["broker-a"]}),
            ),
            (
                ToolId::GetConsumerGroupConfigState,
                serde_json::json!({"cluster":"local-dev","group":"oversized","broker_names":["broker-a"]}),
            ),
            (
                ToolId::GetTopicStats,
                serde_json::json!({"cluster":"local-dev","topic":"oversized"}),
            ),
            (
                ToolId::GetTopicConfig,
                serde_json::json!({"cluster":"local-dev","topic":"oversized"}),
            ),
            (
                ToolId::GetConsumerGroupDetails,
                serde_json::json!({"cluster":"local-dev","consumer_group":"oversized"}),
            ),
            (
                ToolId::GetConsumerProgress,
                serde_json::json!({"cluster":"local-dev","consumer_group":"oversized"}),
            ),
        ];
        for (tool, arguments) in calls {
            let result = executor
                .call(
                    CallToolRequestParams::new(tool.descriptor().name)
                        .with_arguments(arguments.as_object().unwrap().clone()),
                )
                .await
                .unwrap();
            assert_eq!(result.is_error, Some(true), "{}", tool.descriptor().name);
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&content_text(&result)).unwrap()["code"],
                "output_too_large"
            );
        }
    }

    #[tokio::test]
    async fn proxy_backend_failure_never_exposes_internal_endpoint() {
        let result = ToolExecutor::new(
            FakeAdapter {
                fail: true,
                partial: false,
            },
            test_guard("diagnose"),
        )
        .call(
            CallToolRequestParams::new(ToolId::GetProxyDrainState.descriptor().name).with_arguments(
                serde_json::json!({"cluster": "local-dev", "proxy_name": "proxy-a"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
        let text = content_text(&result);
        assert_eq!(result.is_error, Some(true));
        assert!(!text.contains("private-proxy-endpoint"));
        assert!(!text.contains("super-secret"));
    }

    #[cfg(feature = "change-planning")]
    #[tokio::test]
    async fn runtime_policy_denies_change_planning_by_default() {
        let guard = test_guard_with_policy("operator", false);
        let result = ToolExecutor::new(
            FakeAdapter {
                fail: false,
                partial: false,
            },
            guard.clone(),
        )
        .call(plan_create_topic_request())
        .await
        .unwrap();

        assert_eq!(result.is_error, Some(true));
        assert!(content_text(&result).contains("change planning disabled"));
        assert_eq!(guard.audit_log().records()[0].status, AuditStatus::Failure);
    }

    #[cfg(feature = "change-planning")]
    #[tokio::test]
    async fn change_plan_is_non_mutating_and_schema_validated() {
        let guard = test_guard_with_policy("operator", true);
        let result = ToolExecutor::new(
            FakeAdapter {
                fail: false,
                partial: false,
            },
            guard.clone(),
        )
        .call(plan_create_topic_request())
        .await
        .unwrap();

        assert_eq!(result.is_error, Some(false));
        let structured = result.structured_content.as_ref().unwrap();
        assert_eq!(structured["data"]["plan_type"], "create_topic");
        assert_eq!(structured["data"]["mutates_cluster"], false);
        assert_eq!(structured["data"]["ephemeral"], true);
        assert_eq!(structured["data"]["immutable"], true);
        assert!(structured["data"]["current_state"].get("generated_at").is_none());
        assert_eq!(guard.audit_log().records()[0].status, AuditStatus::Success);
    }

    #[cfg(feature = "change-planning")]
    #[test]
    fn current_state_snapshot_excludes_transient_fields() {
        let first = QueryResult {
            data: serde_json::json!({
                "value": "unchanged",
                "nested": { "generated_at": "first" },
                "generated_at": "first",
            }),
            observed_at: "first".to_string(),
            freshness_ms: 0,
            cache_status: crate::model::contract::CacheStatus::Bypass,
            partial: false,
            warnings: Vec::new(),
            source_failures: Vec::new(),
        };
        let second = QueryResult {
            data: serde_json::json!({
                "value": "unchanged",
                "nested": { "generated_at": "second" },
                "generated_at": "second",
            }),
            observed_at: "second".to_string(),
            freshness_ms: 100,
            cache_status: crate::model::contract::CacheStatus::Hit,
            partial: false,
            warnings: Vec::new(),
            source_failures: Vec::new(),
        };

        let first = canonical_current_state(&first).unwrap();
        let second = canonical_current_state(&second).unwrap();

        assert_eq!(first, serde_json::json!({ "value": "unchanged", "nested": {} }));
        assert_eq!(first, second);
    }

    #[cfg(feature = "change-planning")]
    fn plan_create_topic_request() -> CallToolRequestParams {
        CallToolRequestParams::new(ToolId::PlanCreateTopic.descriptor().name).with_arguments(
            serde_json::json!({
                "cluster": "local-dev",
                "reason": "capacity preparation",
                "desired": {
                    "topic": "orders",
                    "read_queue_nums": 8,
                    "write_queue_nums": 8,
                    "perm": "read_write"
                }
            })
            .as_object()
            .unwrap()
            .clone(),
        )
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
        test_guard_with_policy(profile, false)
    }

    fn test_guard_with_policy(profile: &str, allow_change_planning: bool) -> Guard {
        Guard::new(
            SecurityConfig {
                profile: profile.to_string(),
                allow_change_planning,
                sanitize_output: true,
                rate_limit_per_minute: 60,
                permissions_file: permission_path(),
                max_concurrent_requests_per_cluster: 8,
            },
            AuditConfig {
                enabled: true,
                sink: "memory".to_string(),
                path: String::new(),
                queue_capacity: 16,
                max_record_bytes: 16 * 1024,
                queue_max_bytes: 1024 * 1024,
            },
            &[ClusterConfig {
                name: "local-dev".to_string(),
                namesrv_addr: "127.0.0.1:9876".to_string(),
                default: Some(true),
                rocketmq_cluster_name: None,
                tenant: None,
                credentials: None,
                proxies: Vec::new(),
            }],
        )
        .unwrap()
    }

    fn permission_path() -> String {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("conf")
            .join("permissions.example.toml")
            .to_string_lossy()
            .into_owned()
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
