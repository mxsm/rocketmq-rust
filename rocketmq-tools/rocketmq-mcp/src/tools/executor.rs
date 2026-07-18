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

use rmcp::model::CallToolRequestParams;
use rmcp::model::CallToolResult;
use rmcp::model::ContentBlock;
use rmcp::model::JsonObject;
use rmcp::model::Resource;
use rmcp::ErrorData;

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
use crate::tools::consumer_tools;
use crate::tools::diagnosis_tools;
use crate::tools::output_policy;
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
            Self::Backend(_) => "backend_error",
            Self::PermissionDenied(_) => "permission_denied",
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
}

#[derive(Debug, Serialize)]
struct ToolErrorContent<'a> {
    schema_version: &'static str,
    request_id: &'a str,
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
            GuardError::RateLimited(message) => Self::RateLimited(message),
            GuardError::ChangePlanningDisabled(message) => Self::ChangePlanningDisabled(message),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ToolExecutor<A> {
    adapter: A,
    guard: Guard,
    context: RequestContext,
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
        }
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
        let tool_name = request.name.to_string();
        let tool_id = ToolId::resolve(&tool_name)
            .ok_or_else(|| ErrorData::invalid_params(format!("unknown tool: {tool_name}"), None))?;
        let descriptor = tool_id.descriptor();
        let arguments = request.arguments.unwrap_or_default();
        let guarded_call =
            match self
                .guard
                .begin_tool_call(&self.context, &tool_name, descriptor.risk_level, &arguments)
            {
                Ok(guarded_call) => guarded_call,
                Err(error) => return Ok(error_result(&tool_name, request_id, error.into())),
            };

        if let Err(error) = validate_input(&descriptor, &arguments) {
            return Ok(guarded_call.finish_result(error_result(&tool_name, request_id, error)));
        }

        let result = match tool_id {
            ToolId::GetClusterOverview => {
                let args = decode_args::<cluster_tools::ClusterOverviewArgs>(arguments.clone());
                let args = match args {
                    Ok(args) => args,
                    Err(error) => return Ok(guarded_call.finish_result(error_result(&tool_name, request_id, error))),
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
                    Err(error) => return Ok(guarded_call.finish_result(error_result(&tool_name, request_id, error))),
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
                    Err(error) => return Ok(guarded_call.finish_result(error_result(&tool_name, request_id, error))),
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
                    Err(error) => return Ok(guarded_call.finish_result(error_result(&tool_name, request_id, error))),
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
                    Err(error) => return Ok(guarded_call.finish_result(error_result(&tool_name, request_id, error))),
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
                    Err(error) => return Ok(guarded_call.finish_result(error_result(&tool_name, request_id, error))),
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
                    Err(error) => return Ok(guarded_call.finish_result(error_result(&tool_name, request_id, error))),
                };
                self.adapter.describe_broker(args).await.and_then(|output| {
                    let summary = summary_describe_broker(&output);
                    let cluster = output.cluster.clone();
                    let resource =
                        RocketmqResourceUri::new(cluster.clone(), ResourceKind::Broker(output.broker_name.clone()));
                    success_result(descriptor, request_id, cluster, summary, output, resource)
                })
            }
            ToolId::DiagnoseConsumerLag => {
                let args = match decode_args::<diagnosis_tools::DiagnoseConsumerLagArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => return Ok(guarded_call.finish_result(error_result(&tool_name, request_id, error))),
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
            #[cfg(feature = "change-planning")]
            ToolId::PlanCreateTopic => {
                let args = match decode_args::<change_tools::CreateTopicArgs>(arguments.clone()) {
                    Ok(args) => args,
                    Err(error) => return Ok(guarded_call.finish_result(error_result(&tool_name, request_id, error))),
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
                    Err(error) => return Ok(guarded_call.finish_result(error_result(&tool_name, request_id, error))),
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
                    Err(error) => return Ok(guarded_call.finish_result(error_result(&tool_name, request_id, error))),
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
                    Err(error) => return Ok(guarded_call.finish_result(error_result(&tool_name, request_id, error))),
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
                    Err(error) => return Ok(guarded_call.finish_result(error_result(&tool_name, request_id, error))),
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

        let result = result.unwrap_or_else(|error| error_result(&tool_name, request_id, error));

        Ok(guarded_call.finish_result(result))
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

fn error_result(tool_name: &str, request_id: &str, error: ToolExecutionError) -> CallToolResult {
    let content = ToolErrorContent {
        schema_version: crate::model::contract::SCHEMA_VERSION,
        request_id,
        tool: tool_name,
        code: error.code(),
        retryable: error.retryable(),
        message: error.to_string(),
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
    }

    #[async_trait::async_trait]
    impl ReadOnlyQuery for FakeAdapter {
        async fn cluster_overview(
            &self,
            args: cluster_tools::ClusterOverviewArgs,
        ) -> Result<QueryResult<cluster_tools::ClusterOverviewOutput>, ToolExecutionError> {
            if self.fail {
                return Err(ToolExecutionError::backend(
                    "nameserver unavailable secret_key=super-secret",
                ));
            }
            Ok(QueryResult::bypass(cluster_tools::ClusterOverviewOutput {
                cluster: args.cluster,
                namesrv_addr: "127.0.0.1:9876".to_string(),
                brokers: vec![broker_summary()],
                topic_count: 2,
                consumer_group_count: 1,
                generated_at: "1".to_string(),
            }))
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
        let result = ToolExecutor::new(FakeAdapter { fail: false }, guard.clone())
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
    async fn backend_error_is_returned_as_tool_error() {
        let guard = test_guard("diagnose");
        let result = ToolExecutor::new(FakeAdapter { fail: true }, guard.clone())
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
        assert_eq!(error["code"], "backend_error");
        assert_eq!(error["retryable"], true);
        assert_eq!(error["request_id"], "test-request");
        assert!(error["message"].as_str().unwrap().contains("nameserver unavailable"));
        assert!(!content_text(&result).contains("super-secret"));
        let records = guard.audit_log().records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, AuditStatus::Failure);
    }

    #[tokio::test]
    async fn invalid_arguments_are_actionable_tool_errors() {
        let result = ToolExecutor::new(FakeAdapter { fail: false }, test_guard("diagnose"))
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
        assert!(content_text(&result).contains("permission denied"));
        let records = guard.audit_log().records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, AuditStatus::Failure);
    }

    #[cfg(feature = "change-planning")]
    #[tokio::test]
    async fn runtime_policy_denies_change_planning_by_default() {
        let guard = test_guard_with_policy("operator", false);
        let result = ToolExecutor::new(FakeAdapter { fail: false }, guard.clone())
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
        let result = ToolExecutor::new(FakeAdapter { fail: false }, guard.clone())
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
