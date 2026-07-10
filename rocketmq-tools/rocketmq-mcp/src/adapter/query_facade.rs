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

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::adapter::admin_session::AdminCoreSessionFactory;
use crate::adapter::admin_session::AdminSession;
use crate::adapter::admin_session::AdminSessionFactory;
use crate::adapter::admin_session::ResolvedCluster;
use crate::adapter::admin_session::SessionConsumerLag;
use crate::adapter::admin_session::SessionTopicRoute;
use crate::config::McpConfig;
use crate::infrastructure::cache::CacheMetricsSnapshot;
use crate::infrastructure::cache::QueryCache;
use crate::model::contract::observed_at;
use crate::model::contract::paginate;
use crate::model::contract::PageRequest;
use crate::model::contract::QueryResult;
use crate::model::contract::DEFAULT_PAGE_LIMIT;
use crate::model::contract::SCHEMA_VERSION;
use crate::model::diagnosis::DiagnosisReport;
use crate::service::diagnosis_service;
use crate::tools::broker_tools::DescribeBrokerArgs;
use crate::tools::broker_tools::DescribeBrokerOutput;
use crate::tools::cluster_tools::ClusterOverviewArgs;
use crate::tools::cluster_tools::ClusterOverviewOutput;
use crate::tools::consumer_tools::ListConsumerGroupsArgs;
use crate::tools::consumer_tools::ListConsumerGroupsOutput;
use crate::tools::consumer_tools::QueryConsumerLagArgs;
use crate::tools::consumer_tools::QueryConsumerLagOutput;
use crate::tools::diagnosis_tools::DiagnoseConsumerLagArgs;
use crate::tools::executor::ToolExecutionError;
use crate::tools::topic_tools::DescribeTopicArgs;
use crate::tools::topic_tools::DescribeTopicOutput;
use crate::tools::topic_tools::ListTopicsArgs;
use crate::tools::topic_tools::ListTopicsOutput;
use crate::tools::topic_tools::QueryTopicRouteArgs;
use crate::tools::topic_tools::QueryTopicRouteOutput;

#[derive(Debug, Clone)]
pub(crate) struct WorkflowControl {
    timeout: Duration,
    cancellation: CancellationToken,
}

impl WorkflowControl {
    pub(crate) fn new(timeout: Duration, cancellation: CancellationToken) -> Self {
        Self { timeout, cancellation }
    }
}

impl Default for WorkflowControl {
    fn default() -> Self {
        Self::new(Duration::from_secs(30), CancellationToken::new())
    }
}

type WorkflowFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ToolExecutionError>> + Send + 'a>>;

#[async_trait::async_trait]
pub(crate) trait ReadOnlyQuery: Clone + Send + Sync + 'static {
    async fn cluster_overview(
        &self,
        args: ClusterOverviewArgs,
    ) -> Result<QueryResult<ClusterOverviewOutput>, ToolExecutionError>;

    async fn list_topics(&self, args: ListTopicsArgs) -> Result<QueryResult<ListTopicsOutput>, ToolExecutionError>;

    async fn describe_topic(
        &self,
        args: DescribeTopicArgs,
    ) -> Result<QueryResult<DescribeTopicOutput>, ToolExecutionError>;

    async fn query_topic_route(
        &self,
        args: QueryTopicRouteArgs,
    ) -> Result<QueryResult<QueryTopicRouteOutput>, ToolExecutionError>;

    async fn list_consumer_groups(
        &self,
        args: ListConsumerGroupsArgs,
    ) -> Result<QueryResult<ListConsumerGroupsOutput>, ToolExecutionError>;

    async fn query_consumer_lag(
        &self,
        args: QueryConsumerLagArgs,
    ) -> Result<QueryResult<QueryConsumerLagOutput>, ToolExecutionError>;

    async fn describe_broker(
        &self,
        args: DescribeBrokerArgs,
    ) -> Result<QueryResult<DescribeBrokerOutput>, ToolExecutionError>;

    async fn diagnose_consumer_lag(
        &self,
        args: DiagnoseConsumerLagArgs,
    ) -> Result<QueryResult<DiagnosisReport>, ToolExecutionError>;
}

#[derive(Clone)]
pub(crate) struct QueryFacade<F> {
    config: McpConfig,
    factory: F,
    control: WorkflowControl,
    cache: QueryCache,
    visibility_class: String,
}

impl<F> fmt::Debug for QueryFacade<F>
where
    F: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryFacade")
            .field("config", &self.config)
            .field("factory", &self.factory)
            .field("control", &self.control)
            .field("visibility_class", &self.visibility_class)
            .finish_non_exhaustive()
    }
}

impl<F> QueryFacade<F>
where
    F: AdminSessionFactory,
{
    pub(crate) fn with_factory(config: McpConfig, factory: F) -> Self {
        Self::with_factory_and_control(config, factory, WorkflowControl::default())
    }

    pub(crate) fn with_factory_and_control(config: McpConfig, factory: F, control: WorkflowControl) -> Self {
        Self {
            cache: QueryCache::new(config.cache.enabled, config.cache.max_entries),
            config,
            factory,
            control,
            visibility_class: "local".to_string(),
        }
    }

    pub(crate) fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.control.cancellation = cancellation;
        self
    }

    pub(crate) fn with_visibility_class(mut self, visibility_class: impl Into<String>) -> Self {
        self.visibility_class = visibility_class.into();
        self
    }

    pub(crate) fn cache_metrics(&self) -> CacheMetricsSnapshot {
        self.cache.metrics()
    }

    pub(crate) async fn invalidate_cache(&self) -> usize {
        self.cache.clear().await
    }

    pub(crate) async fn cluster_overview(
        &self,
        args: ClusterOverviewArgs,
    ) -> Result<QueryResult<ClusterOverviewOutput>, ToolExecutionError> {
        let cluster = self.resolve_cluster(Some(&args.cluster))?;
        let key = self.cache_key("cluster_overview", &cluster.name, "");
        let ttl = Duration::from_millis(self.config.cache.cluster_overview_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                || async {
                    self.run_workflow(cluster, |session, cluster| {
                        Box::pin(async move {
                            let brokers = session.broker_rows().await?;
                            let topics = session.topic_entries().await?;
                            let consumer_groups = session.consumer_groups().await?;
                            Ok(ClusterOverviewOutput {
                                cluster: cluster.name.clone(),
                                namesrv_addr: cluster.namesrv_addr.clone(),
                                brokers,
                                topic_count: topics.len(),
                                consumer_group_count: consumer_groups.len(),
                                generated_at: observed_at(),
                            })
                        })
                    })
                    .await
                },
            )
            .await
    }

    pub(crate) async fn list_topics(
        &self,
        args: ListTopicsArgs,
    ) -> Result<QueryResult<ListTopicsOutput>, ToolExecutionError> {
        let cluster = self.resolve_cluster(args.cluster.as_deref())?;
        let key = self.cache_key(
            "list_topics",
            &cluster.name,
            &list_key(args.filter.as_deref(), &args.page),
        );
        let ttl = Duration::from_millis(self.config.cache.topic_list_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                || async {
                    self.run_workflow(cluster, move |session, cluster| {
                        Box::pin(async move {
                            let mut topics = session.topic_entries().await?;
                            topics.sort_by(|left, right| left.topic.cmp(&right.topic));
                            if let Some(filter) = normalized_filter(args.filter.as_deref()) {
                                topics.retain(|entry| entry.topic.to_ascii_lowercase().contains(&filter));
                            }
                            let page = paginate(topics, &args.page)
                                .map_err(|error| ToolExecutionError::InvalidArguments(error.to_string()))?;
                            Ok(ListTopicsOutput {
                                cluster: cluster.name.clone(),
                                namesrv_addr: cluster.namesrv_addr.clone(),
                                page,
                                generated_at: observed_at(),
                            })
                        })
                    })
                    .await
                },
            )
            .await
    }

    pub(crate) async fn describe_topic(
        &self,
        mut args: DescribeTopicArgs,
    ) -> Result<QueryResult<DescribeTopicOutput>, ToolExecutionError> {
        args.topic = normalized_identifier("topic", &args.topic)?;
        let cluster = self.resolve_cluster(Some(&args.cluster))?;
        let key = self.cache_key(
            "describe_topic",
            &cluster.name,
            &format!("topic={}|{}", args.topic, page_key(&args.page)),
        );
        let ttl = Duration::from_millis(self.config.cache.topic_list_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                || async {
                    self.run_workflow(cluster, move |session, cluster| {
                        Box::pin(async move {
                            let route = session.topic_route(&args.topic).await?;
                            let route = topic_route_output(cluster, &args.topic, route, &args.page)?;
                            Ok(describe_topic_output(&route))
                        })
                    })
                    .await
                },
            )
            .await
    }

    pub(crate) async fn query_topic_route(
        &self,
        mut args: QueryTopicRouteArgs,
    ) -> Result<QueryResult<QueryTopicRouteOutput>, ToolExecutionError> {
        args.topic = normalized_identifier("topic", &args.topic)?;
        let cluster = self.resolve_cluster(Some(&args.cluster))?;
        let key = self.cache_key(
            "topic_route",
            &cluster.name,
            &format!("topic={}|{}", args.topic, page_key(&args.page)),
        );
        let ttl = Duration::from_millis(self.config.cache.topic_list_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                || async {
                    self.run_workflow(cluster, move |session, cluster| {
                        Box::pin(async move {
                            let route = session.topic_route(&args.topic).await?;
                            topic_route_output(cluster, &args.topic, route, &args.page)
                        })
                    })
                    .await
                },
            )
            .await
    }

    pub(crate) async fn list_consumer_groups(
        &self,
        args: ListConsumerGroupsArgs,
    ) -> Result<QueryResult<ListConsumerGroupsOutput>, ToolExecutionError> {
        let cluster = self.resolve_cluster(args.cluster.as_deref())?;
        let key = self.cache_key(
            "list_consumer_groups",
            &cluster.name,
            &list_key(args.filter.as_deref(), &args.page),
        );
        let ttl = Duration::from_millis(self.config.cache.consumer_lag_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                || async {
                    self.run_workflow(cluster, move |session, cluster| {
                        Box::pin(async move {
                            let mut groups = session.consumer_groups().await?;
                            groups.sort_by(|left, right| left.group.cmp(&right.group));
                            if let Some(filter) = normalized_filter(args.filter.as_deref()) {
                                groups.retain(|entry| entry.group.to_ascii_lowercase().contains(&filter));
                            }
                            let page = paginate(groups, &args.page)
                                .map_err(|error| ToolExecutionError::InvalidArguments(error.to_string()))?;
                            Ok(ListConsumerGroupsOutput {
                                cluster: cluster.name.clone(),
                                namesrv_addr: cluster.namesrv_addr.clone(),
                                page,
                                generated_at: observed_at(),
                            })
                        })
                    })
                    .await
                },
            )
            .await
    }

    pub(crate) async fn query_consumer_lag(
        &self,
        mut args: QueryConsumerLagArgs,
    ) -> Result<QueryResult<QueryConsumerLagOutput>, ToolExecutionError> {
        args.topic = normalized_identifier("topic", &args.topic)?;
        args.consumer_group = normalized_identifier("consumer_group", &args.consumer_group)?;
        let cluster = self.resolve_cluster(Some(&args.cluster))?;
        let key = self.cache_key(
            "consumer_lag",
            &cluster.name,
            &format!(
                "topic={}|group={}|{}",
                args.topic,
                args.consumer_group,
                page_key(&args.page)
            ),
        );
        let ttl = Duration::from_millis(self.config.cache.consumer_lag_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                || async {
                    self.run_workflow(cluster, move |session, cluster| {
                        Box::pin(async move {
                            let lag = session.consumer_lag(&args.topic, &args.consumer_group).await?;
                            consumer_lag_output(cluster, args.topic, args.consumer_group, &args.page, lag)
                        })
                    })
                    .await
                },
            )
            .await
    }

    pub(crate) async fn describe_broker(
        &self,
        mut args: DescribeBrokerArgs,
    ) -> Result<QueryResult<DescribeBrokerOutput>, ToolExecutionError> {
        args.broker_name = normalized_identifier("broker_name", &args.broker_name)?;
        let cluster = self.resolve_cluster(Some(&args.cluster))?;
        let key = self.cache_key(
            "describe_broker",
            &cluster.name,
            &format!("broker={}", args.broker_name),
        );
        let ttl = Duration::from_millis(self.config.cache.broker_metrics_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                || async {
                    self.run_workflow(cluster, move |session, cluster| {
                        Box::pin(describe_broker_in_session(session, cluster, args.broker_name))
                    })
                    .await
                },
            )
            .await
    }

    pub(crate) async fn diagnose_consumer_lag(
        &self,
        mut args: DiagnoseConsumerLagArgs,
    ) -> Result<QueryResult<DiagnosisReport>, ToolExecutionError> {
        args.topic = normalized_identifier("topic", &args.topic)?;
        args.consumer_group = normalized_identifier("consumer_group", &args.consumer_group)?;
        let cluster = self.resolve_cluster(Some(&args.cluster))?;
        let key = self.cache_key(
            "diagnose_consumer_lag",
            &cluster.name,
            &format!(
                "topic={}|group={}|threshold={:?}|time_range={:?}",
                args.topic, args.consumer_group, args.lag_threshold, args.time_range
            ),
        );
        let ttl = Duration::from_millis(self.config.cache.consumer_lag_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                || async {
                    self.run_workflow(cluster, move |session, cluster| {
                        Box::pin(async move {
                            let lag_result =
                                session
                                    .consumer_lag(&args.topic, &args.consumer_group)
                                    .await
                                    .and_then(|lag| {
                                        consumer_lag_output(
                                            cluster,
                                            args.topic.clone(),
                                            args.consumer_group.clone(),
                                            &PageRequest::default(),
                                            lag,
                                        )
                                    });
                            let (topic_result, route_result) = match session.topic_route(&args.topic).await {
                                Ok(route) => {
                                    let route =
                                        topic_route_output(cluster, &args.topic, route, &PageRequest::default())?;
                                    (Ok(describe_topic_output(&route)), Ok(route))
                                }
                                Err(error) => {
                                    let message = error.to_string();
                                    (Err(ToolExecutionError::backend(&message)), Err(error))
                                }
                            };
                            let broker_result = match top_lag_broker(lag_result.as_ref().ok()) {
                                Some(broker_name) => {
                                    Some(describe_broker_in_session(session, cluster, broker_name).await)
                                }
                                None => None,
                            };

                            Ok(diagnosis_service::build_consumer_lag_report(
                                args,
                                lag_result,
                                topic_result,
                                route_result,
                                broker_result,
                            ))
                        })
                    })
                    .await
                },
            )
            .await
    }

    async fn run_workflow<T, O>(&self, cluster: ResolvedCluster, operation: O) -> Result<T, ToolExecutionError>
    where
        T: Send,
        O: for<'a> FnOnce(&'a mut F::Session, &'a ResolvedCluster) -> WorkflowFuture<'a, T>,
    {
        let deadline = tokio::time::Instant::now() + self.control.timeout;
        let mut session = tokio::select! {
            _ = self.control.cancellation.cancelled() => return Err(ToolExecutionError::Cancelled),
            result = tokio::time::timeout_at(deadline, self.factory.start(cluster.clone())) => match result {
                Ok(result) => result?,
                Err(_) => {
                    return Err(ToolExecutionError::TimedOut {
                        timeout_ms: self.control.timeout.as_millis().try_into().unwrap_or(u64::MAX),
                    });
                }
            },
        };
        let result = {
            let operation = operation(&mut session, &cluster);
            tokio::select! {
                _ = self.control.cancellation.cancelled() => Err(ToolExecutionError::Cancelled),
                result = tokio::time::timeout_at(deadline, operation) => match result {
                    Ok(result) => result,
                    Err(_) => Err(ToolExecutionError::TimedOut {
                        timeout_ms: self.control.timeout.as_millis().try_into().unwrap_or(u64::MAX),
                    }),
                },
            }
        };
        let shutdown = session.shutdown().await;
        match (result, shutdown) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(output), Ok(())) => Ok(output),
        }
    }

    fn resolve_cluster(&self, cluster: Option<&str>) -> Result<ResolvedCluster, ToolExecutionError> {
        let cluster = cluster.map(str::trim).filter(|cluster| !cluster.is_empty());
        let config = match cluster {
            Some(name) => self
                .config
                .clusters
                .iter()
                .find(|candidate| candidate.name == name)
                .ok_or_else(|| ToolExecutionError::InvalidArguments(format!("unknown cluster: {name}")))?,
            None => self
                .config
                .clusters
                .iter()
                .find(|candidate| candidate.default.unwrap_or(false))
                .or_else(|| (self.config.clusters.len() == 1).then(|| &self.config.clusters[0]))
                .ok_or_else(|| {
                    ToolExecutionError::InvalidArguments(
                        "cluster is required because no unique default cluster is configured".to_string(),
                    )
                })?,
        };

        Ok(ResolvedCluster {
            name: config.name.clone(),
            namesrv_addr: config.namesrv_addr.clone(),
        })
    }

    fn cache_key(&self, kind: &str, cluster: &str, parameters: &str) -> String {
        format!(
            "{SCHEMA_VERSION}|{}|{kind}|cluster={}|{parameters}",
            self.visibility_class,
            cluster.trim()
        )
    }
}

impl QueryFacade<AdminCoreSessionFactory> {
    pub(crate) fn new(config: McpConfig) -> Self {
        Self::with_factory(config, AdminCoreSessionFactory)
    }
}

#[async_trait::async_trait]
impl<F> ReadOnlyQuery for QueryFacade<F>
where
    F: AdminSessionFactory,
{
    async fn cluster_overview(
        &self,
        args: ClusterOverviewArgs,
    ) -> Result<QueryResult<ClusterOverviewOutput>, ToolExecutionError> {
        QueryFacade::cluster_overview(self, args).await
    }

    async fn list_topics(&self, args: ListTopicsArgs) -> Result<QueryResult<ListTopicsOutput>, ToolExecutionError> {
        QueryFacade::list_topics(self, args).await
    }

    async fn describe_topic(
        &self,
        args: DescribeTopicArgs,
    ) -> Result<QueryResult<DescribeTopicOutput>, ToolExecutionError> {
        QueryFacade::describe_topic(self, args).await
    }

    async fn query_topic_route(
        &self,
        args: QueryTopicRouteArgs,
    ) -> Result<QueryResult<QueryTopicRouteOutput>, ToolExecutionError> {
        QueryFacade::query_topic_route(self, args).await
    }

    async fn list_consumer_groups(
        &self,
        args: ListConsumerGroupsArgs,
    ) -> Result<QueryResult<ListConsumerGroupsOutput>, ToolExecutionError> {
        QueryFacade::list_consumer_groups(self, args).await
    }

    async fn query_consumer_lag(
        &self,
        args: QueryConsumerLagArgs,
    ) -> Result<QueryResult<QueryConsumerLagOutput>, ToolExecutionError> {
        QueryFacade::query_consumer_lag(self, args).await
    }

    async fn describe_broker(
        &self,
        args: DescribeBrokerArgs,
    ) -> Result<QueryResult<DescribeBrokerOutput>, ToolExecutionError> {
        QueryFacade::describe_broker(self, args).await
    }

    async fn diagnose_consumer_lag(
        &self,
        args: DiagnoseConsumerLagArgs,
    ) -> Result<QueryResult<DiagnosisReport>, ToolExecutionError> {
        QueryFacade::diagnose_consumer_lag(self, args).await
    }
}

fn consumer_lag_output(
    cluster: &ResolvedCluster,
    topic: String,
    consumer_group: String,
    page_request: &PageRequest,
    lag: SessionConsumerLag,
) -> Result<QueryConsumerLagOutput, ToolExecutionError> {
    let max_queue_lag = lag.queues.iter().map(|queue| queue.lag).max().unwrap_or_default();
    let page =
        paginate(lag.queues, page_request).map_err(|error| ToolExecutionError::InvalidArguments(error.to_string()))?;
    Ok(QueryConsumerLagOutput {
        cluster: cluster.name.clone(),
        namesrv_addr: cluster.namesrv_addr.clone(),
        topic,
        consumer_group,
        total_lag: lag.total_lag,
        max_queue_lag,
        consume_tps: lag.consume_tps,
        inflight_total: lag.inflight_total,
        page,
        generated_at: observed_at(),
    })
}

fn topic_route_output(
    cluster: &ResolvedCluster,
    topic: &str,
    route: SessionTopicRoute,
    page_request: &PageRequest,
) -> Result<QueryTopicRouteOutput, ToolExecutionError> {
    let read_queue_count = route.queues.iter().map(|queue| queue.read_queue_nums).sum();
    let write_queue_count = route.queues.iter().map(|queue| queue.write_queue_nums).sum();
    let page = paginate(route.queues, page_request)
        .map_err(|error| ToolExecutionError::InvalidArguments(error.to_string()))?;
    Ok(QueryTopicRouteOutput {
        cluster: cluster.name.clone(),
        namesrv_addr: cluster.namesrv_addr.clone(),
        topic: topic.to_string(),
        brokers: route.brokers,
        read_queue_count,
        write_queue_count,
        page,
        generated_at: observed_at(),
    })
}

fn describe_topic_output(route: &QueryTopicRouteOutput) -> DescribeTopicOutput {
    let mut broker_names = route
        .brokers
        .iter()
        .map(|broker| broker.broker_name.clone())
        .collect::<Vec<_>>();
    broker_names.sort();
    broker_names.dedup();
    DescribeTopicOutput {
        cluster: route.cluster.clone(),
        namesrv_addr: route.namesrv_addr.clone(),
        topic: route.topic.clone(),
        broker_names,
        read_queue_count: route.read_queue_count,
        write_queue_count: route.write_queue_count,
        brokers: route.brokers.clone(),
        page: route.page.clone(),
        generated_at: route.generated_at.clone(),
    }
}

async fn describe_broker_in_session<S>(
    session: &mut S,
    cluster: &ResolvedCluster,
    broker_name: String,
) -> Result<DescribeBrokerOutput, ToolExecutionError>
where
    S: AdminSession,
{
    let brokers = session
        .broker_rows()
        .await?
        .into_iter()
        .filter(|broker| broker.broker_name == broker_name)
        .collect::<Vec<_>>();
    if brokers.is_empty() {
        session.probe_broker_runtime().await?;
        return Err(ToolExecutionError::InvalidArguments(format!(
            "broker not found in cluster {}: {broker_name}",
            cluster.name
        )));
    }
    Ok(DescribeBrokerOutput {
        cluster: cluster.name.clone(),
        namesrv_addr: cluster.namesrv_addr.clone(),
        broker_name,
        brokers,
        generated_at: observed_at(),
    })
}

fn top_lag_broker(lag: Option<&QueryConsumerLagOutput>) -> Option<String> {
    lag.and_then(|lag| {
        lag.page
            .items
            .iter()
            .max_by_key(|queue| queue.lag)
            .map(|queue| queue.broker_name.clone())
    })
}

fn normalized_filter(filter: Option<&str>) -> Option<String> {
    filter
        .map(str::trim)
        .filter(|filter| !filter.is_empty())
        .map(str::to_ascii_lowercase)
}

fn normalized_identifier(field: &str, value: &str) -> Result<String, ToolExecutionError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ToolExecutionError::InvalidArguments(format!(
            "{field} must not be empty"
        )));
    }
    Ok(value.to_string())
}

fn list_key(filter: Option<&str>, page: &PageRequest) -> String {
    format!(
        "filter={}|{}",
        normalized_filter(filter).unwrap_or_default(),
        page_key(page)
    )
}

fn page_key(page: &PageRequest) -> String {
    format!(
        "limit={}|cursor={}",
        page.limit.unwrap_or(DEFAULT_PAGE_LIMIT),
        page.cursor.as_deref().unwrap_or_default()
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio_util::sync::CancellationToken;

    use crate::config::McpConfig;
    use crate::model::contract::CacheStatus;
    use crate::resources;
    use crate::tools::cluster_tools::BrokerSummary;
    use crate::tools::cluster_tools::ClusterOverviewArgs;
    use crate::tools::consumer_tools::ConsumerGroupSummary;
    use crate::tools::consumer_tools::QueueLag;
    use crate::tools::diagnosis_tools::DiagnoseConsumerLagArgs;
    use crate::tools::topic_tools::TopicListEntry;
    use crate::tools::topic_tools::TopicRouteBroker;
    use crate::tools::topic_tools::TopicRouteQueue;

    use super::*;

    #[derive(Debug, Default)]
    struct LifecycleCounters {
        starts: AtomicUsize,
        shutdowns: AtomicUsize,
        broker_queries: AtomicUsize,
        topic_queries: AtomicUsize,
        consumer_group_queries: AtomicUsize,
        route_queries: AtomicUsize,
        consumer_lag_queries: AtomicUsize,
        runtime_probes: AtomicUsize,
    }

    #[derive(Clone, Default)]
    struct FakeSessionFactory {
        counters: Arc<LifecycleCounters>,
        selected_broker_missing: bool,
        hang_broker_query: bool,
        hang_topic_query: bool,
        delay_topic_query: bool,
        fail_topic_query: bool,
    }

    #[async_trait::async_trait]
    impl AdminSessionFactory for FakeSessionFactory {
        type Session = FakeSession;

        async fn start(&self, cluster: ResolvedCluster) -> Result<Self::Session, ToolExecutionError> {
            self.counters.starts.fetch_add(1, Ordering::SeqCst);
            Ok(FakeSession {
                cluster,
                counters: self.counters.clone(),
                selected_broker_missing: self.selected_broker_missing,
                hang_broker_query: self.hang_broker_query,
                hang_topic_query: self.hang_topic_query,
                delay_topic_query: self.delay_topic_query,
                fail_topic_query: self.fail_topic_query,
            })
        }
    }

    struct FakeSession {
        cluster: ResolvedCluster,
        counters: Arc<LifecycleCounters>,
        selected_broker_missing: bool,
        hang_broker_query: bool,
        hang_topic_query: bool,
        delay_topic_query: bool,
        fail_topic_query: bool,
    }

    #[async_trait::async_trait]
    impl AdminSession for FakeSession {
        async fn broker_rows(&mut self) -> Result<Vec<BrokerSummary>, ToolExecutionError> {
            self.counters.broker_queries.fetch_add(1, Ordering::SeqCst);
            if self.hang_broker_query {
                std::future::pending::<()>().await;
            }
            let broker_name = if self.selected_broker_missing {
                "broker-b"
            } else {
                "broker-a"
            };
            Ok(vec![broker_summary(&self.cluster.name, broker_name)])
        }

        async fn topic_entries(&mut self) -> Result<Vec<TopicListEntry>, ToolExecutionError> {
            self.counters.topic_queries.fetch_add(1, Ordering::SeqCst);
            if self.hang_topic_query {
                std::future::pending::<()>().await;
            }
            if self.delay_topic_query {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            if self.fail_topic_query {
                return Err(ToolExecutionError::backend("topic query failed"));
            }
            Ok(vec![topic_entry("orders"), topic_entry("payments")])
        }

        async fn topic_route(&mut self, _topic: &str) -> Result<SessionTopicRoute, ToolExecutionError> {
            self.counters.route_queries.fetch_add(1, Ordering::SeqCst);
            Ok(SessionTopicRoute {
                brokers: vec![route_broker(&self.cluster.name, "broker-a")],
                queues: vec![route_queue("broker-a")],
            })
        }

        async fn consumer_groups(&mut self) -> Result<Vec<ConsumerGroupSummary>, ToolExecutionError> {
            self.counters.consumer_group_queries.fetch_add(1, Ordering::SeqCst);
            Ok(vec![consumer_group("order-service")])
        }

        async fn consumer_lag(
            &mut self,
            _topic: &str,
            _consumer_group: &str,
        ) -> Result<SessionConsumerLag, ToolExecutionError> {
            self.counters.consumer_lag_queries.fetch_add(1, Ordering::SeqCst);
            Ok(SessionConsumerLag {
                queues: vec![queue_lag("broker-a")],
                total_lag: 10_000,
                consume_tps: 0.5,
                inflight_total: 5,
            })
        }

        async fn probe_broker_runtime(&mut self) -> Result<(), ToolExecutionError> {
            self.counters.runtime_probes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn shutdown(self) -> Result<(), ToolExecutionError> {
            self.counters.shutdowns.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn query_facade_cluster_overview_starts_and_shuts_down_one_admin_session() {
        let factory = FakeSessionFactory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);

        let result = facade
            .cluster_overview(ClusterOverviewArgs {
                cluster: "local-dev".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(result.topic_count, 2);
        assert_eq!(result.consumer_group_count, 1);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(counters.broker_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.topic_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.consumer_group_queries.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn query_facade_high_lag_diagnosis_reuses_one_session_and_one_route_query() {
        let factory = FakeSessionFactory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);

        let report = facade
            .diagnose_consumer_lag(DiagnoseConsumerLagArgs {
                cluster: "local-dev".to_string(),
                topic: "orders".to_string(),
                consumer_group: "order-service".to_string(),
                time_range: None,
                lag_threshold: Some(1_000),
            })
            .await
            .unwrap();

        assert_eq!(report.evidence_version, "rocketmq-mcp.evidence.consumer-lag.v1");
        assert_eq!(report.rules_version, "rocketmq-mcp.rules.consumer-lag.v1");
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(counters.consumer_lag_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.route_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.broker_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.runtime_probes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn query_facade_missing_selected_broker_fallback_reuses_the_workflow_session() {
        let factory = FakeSessionFactory {
            selected_broker_missing: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);

        let report = facade.diagnose_consumer_lag(diagnosis_request()).await.unwrap();

        assert!(report
            .evidences
            .iter()
            .any(|evidence| evidence.id == "broker_description"
                && evidence.status == crate::model::diagnosis::EvidenceStatus::Error));
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(counters.route_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.broker_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.runtime_probes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn query_facade_timeout_shuts_down_the_started_session_once() {
        let factory = FakeSessionFactory {
            hang_broker_query: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let control = WorkflowControl::new(Duration::from_millis(10), CancellationToken::new());
        let facade = QueryFacade::with_factory_and_control(example_config(), factory, control);

        let error = facade
            .cluster_overview(ClusterOverviewArgs {
                cluster: "local-dev".to_string(),
            })
            .await
            .unwrap_err();

        assert!(matches!(error, ToolExecutionError::TimedOut { .. }));
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn query_facade_cancellation_shuts_down_the_started_session_once() {
        let factory = FakeSessionFactory {
            hang_broker_query: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let cancellation = CancellationToken::new();
        let control = WorkflowControl::new(Duration::from_secs(1), cancellation.clone());
        let facade = QueryFacade::with_factory_and_control(example_config(), factory, control);
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            cancellation.cancel();
        });

        let error = facade
            .cluster_overview(ClusterOverviewArgs {
                cluster: "local-dev".to_string(),
            })
            .await
            .unwrap_err();

        assert!(matches!(error, ToolExecutionError::Cancelled));
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn query_facade_backend_failure_shuts_down_the_started_session_once() {
        let factory = FakeSessionFactory {
            fail_topic_query: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);

        let error = facade
            .list_topics(ListTopicsArgs {
                cluster: Some("local-dev".to_string()),
                filter: None,
                page: PageRequest::default(),
            })
            .await
            .unwrap_err();

        assert!(matches!(error, ToolExecutionError::Backend(_)));
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(counters.topic_queries.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn production_query_facade_uses_the_admin_core_session_factory() {
        let _: QueryFacade<AdminCoreSessionFactory> = QueryFacade::new(example_config());
    }

    #[tokio::test]
    async fn query_facade_resource_read_uses_one_session_and_live_query_data() {
        let factory = FakeSessionFactory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);

        let result = resources::reader::read_resource(&facade, "rocketmq://clusters/local-dev/topics")
            .await
            .unwrap();
        let payload = match &result.contents[0] {
            rmcp::model::ResourceContents::TextResourceContents { text, .. } => {
                serde_json::from_str::<serde_json::Value>(text).unwrap()
            }
            _ => panic!("resource should contain JSON text"),
        };

        assert_eq!(payload["source"], "live");
        assert_eq!(payload["partial"], false);
        assert_eq!(payload["topics"]["total_count"], 2);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(counters.topic_queries.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn query_facade_reuses_cached_results_across_tool_and_resource_queries() {
        let factory = FakeSessionFactory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let request = ListTopicsArgs {
            cluster: Some("local-dev".to_string()),
            filter: None,
            page: PageRequest::default(),
        };

        let tool_result = facade.list_topics(request).await.unwrap();
        let resource_result = resources::reader::read_resource(&facade, "rocketmq://clusters/local-dev/topics")
            .await
            .unwrap();
        let payload = match &resource_result.contents[0] {
            rmcp::model::ResourceContents::TextResourceContents { text, .. } => {
                serde_json::from_str::<serde_json::Value>(text).unwrap()
            }
            _ => panic!("resource should contain JSON text"),
        };

        assert_eq!(tool_result.cache_status, CacheStatus::Miss);
        assert_eq!(payload["cache_status"], "hit");
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(counters.topic_queries.load(Ordering::SeqCst), 1);
        assert_eq!(
            facade.cache_metrics(),
            CacheMetricsSnapshot {
                hits: 1,
                misses: 1,
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn query_facade_singleflight_coalesces_concurrent_identical_misses() {
        let factory = FakeSessionFactory {
            delay_topic_query: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let facade = facade.clone();
            tasks.spawn(async move {
                facade
                    .list_topics(ListTopicsArgs {
                        cluster: Some("local-dev".to_string()),
                        filter: None,
                        page: PageRequest::default(),
                    })
                    .await
                    .unwrap()
                    .cache_status
            });
        }

        let mut statuses = Vec::new();
        while let Some(status) = tasks.join_next().await {
            statuses.push(status.unwrap());
        }

        assert_eq!(
            statuses.iter().filter(|status| **status == CacheStatus::Miss).count(),
            1
        );
        assert_eq!(statuses.iter().filter(|status| **status == CacheStatus::Hit).count(), 7);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(counters.topic_queries.load(Ordering::SeqCst), 1);
        assert_eq!(facade.cache_metrics().coalesced_waiters, 7);
    }

    #[tokio::test]
    async fn query_facade_singleflight_waiter_observes_its_cancellation() {
        let factory = FakeSessionFactory {
            hang_topic_query: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let leader_cancellation = CancellationToken::new();
        let control = WorkflowControl::new(Duration::from_secs(1), leader_cancellation.clone());
        let facade = QueryFacade::with_factory_and_control(example_config(), factory, control);
        let request = || ListTopicsArgs {
            cluster: Some("local-dev".to_string()),
            filter: None,
            page: PageRequest::default(),
        };
        let leader_facade = facade.clone();
        let leader = tokio::spawn(async move { leader_facade.list_topics(request()).await });
        while counters.topic_queries.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }

        let waiter_cancellation = CancellationToken::new();
        let waiter_facade = facade.clone().with_cancellation(waiter_cancellation.clone());
        let waiter = tokio::spawn(async move { waiter_facade.list_topics(request()).await });
        tokio::task::yield_now().await;
        waiter_cancellation.cancel();
        let waiter_error = tokio::time::timeout(Duration::from_millis(100), waiter)
            .await
            .expect("cancelled waiter must not wait for the leader")
            .unwrap()
            .unwrap_err();

        assert!(matches!(waiter_error, ToolExecutionError::Cancelled));
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        leader_cancellation.cancel();
        assert!(matches!(leader.await.unwrap(), Err(ToolExecutionError::Cancelled)));
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn query_facade_cache_isolates_visibility_classes() {
        let factory = FakeSessionFactory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let reader = facade.clone().with_visibility_class("reader");
        let topology_reader = facade.with_visibility_class("topology-reader");
        let request = || ListTopicsArgs {
            cluster: Some("local-dev".to_string()),
            filter: None,
            page: PageRequest::default(),
        };

        let first = reader.list_topics(request()).await.unwrap();
        let second = topology_reader.list_topics(request()).await.unwrap();

        assert_eq!(first.cache_status, CacheStatus::Miss);
        assert_eq!(second.cache_status, CacheStatus::Miss);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 2);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 2);
        assert_eq!(counters.topic_queries.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn query_facade_normalizes_identifiers_before_caching_and_querying() {
        let factory = FakeSessionFactory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let request = |topic: &str| QueryTopicRouteArgs {
            cluster: "local-dev".to_string(),
            topic: topic.to_string(),
            page: PageRequest::default(),
        };

        let first = facade.query_topic_route(request(" orders ")).await.unwrap();
        let second = facade.query_topic_route(request("orders")).await.unwrap();

        assert_eq!(first.topic, "orders");
        assert_eq!(first.cache_status, CacheStatus::Miss);
        assert_eq!(second.cache_status, CacheStatus::Hit);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.route_queries.load(Ordering::SeqCst), 1);
    }

    fn example_config() -> McpConfig {
        McpConfig::load(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("conf")
                .join("mcp.example.toml"),
        )
        .unwrap()
    }

    fn diagnosis_request() -> DiagnoseConsumerLagArgs {
        DiagnoseConsumerLagArgs {
            cluster: "local-dev".to_string(),
            topic: "orders".to_string(),
            consumer_group: "order-service".to_string(),
            time_range: None,
            lag_threshold: Some(1_000),
        }
    }

    fn broker_summary(cluster: &str, broker_name: &str) -> BrokerSummary {
        BrokerSummary {
            cluster: cluster.to_string(),
            broker_name: broker_name.to_string(),
            broker_id: 0,
            broker_addr: "127.0.0.1:10911".to_string(),
            version: "V5_3_0".to_string(),
            in_tps: "1.0".to_string(),
            out_tps: "1.0".to_string(),
            timer_progress: "0".to_string(),
            page_cache_lock_time_millis: "0".to_string(),
            hour: "0".to_string(),
            space: "1%".to_string(),
            broker_active: true,
        }
    }

    fn topic_entry(topic: &str) -> TopicListEntry {
        TopicListEntry {
            topic: topic.to_string(),
            cluster: Some("local-dev".to_string()),
            consumer_group: None,
        }
    }

    fn consumer_group(group: &str) -> ConsumerGroupSummary {
        ConsumerGroupSummary {
            group: group.to_string(),
            version: 1,
            client_count: 1,
            consume_type: "CONSUME_PASSIVELY".to_string(),
            message_model: "CLUSTERING".to_string(),
            consume_tps: 1.0,
            diff_total: 0,
        }
    }

    fn queue_lag(broker_name: &str) -> QueueLag {
        QueueLag {
            topic: "orders".to_string(),
            broker_name: broker_name.to_string(),
            queue_id: 0,
            broker_offset: 10,
            consumer_offset: 0,
            lag: 10,
            inflight: 0,
            last_observed_at: None,
            client_ip: None,
        }
    }

    fn route_broker(cluster: &str, broker_name: &str) -> TopicRouteBroker {
        TopicRouteBroker {
            cluster: cluster.to_string(),
            broker_name: broker_name.to_string(),
            broker_addrs: Default::default(),
            zone_name: None,
            enable_acting_master: false,
        }
    }

    fn route_queue(broker_name: &str) -> TopicRouteQueue {
        TopicRouteQueue {
            broker_name: broker_name.to_string(),
            read_queue_nums: 4,
            write_queue_nums: 4,
            perm: 6,
            topic_sys_flag: 0,
        }
    }
}
