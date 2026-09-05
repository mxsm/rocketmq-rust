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

use rocketmq_admin_core::core::broker::BrokerRuntimeTargetStatus;
use tokio_util::sync::CancellationToken;

use crate::adapter::admin_session::AdminCoreSessionFactory;
use crate::adapter::admin_session::AdminSession;
use crate::adapter::admin_session::AdminSessionFactory;
use crate::adapter::admin_session::ResolvedCluster;
use crate::adapter::admin_session::SessionConnections;
use crate::adapter::admin_session::SessionConsumerLag;
use crate::adapter::admin_session::SessionTopicRoute;
use crate::adapter::identifier_alias::IdentifierAliasError;
use crate::adapter::identifier_alias::IdentifierAliaser;
use crate::config::McpConfig;
use crate::guard::context::VisibilityClass;
use crate::infrastructure::cache::CacheMetricsSnapshot;
use crate::infrastructure::cache::QueryCache;
use crate::infrastructure::snapshot::RetainedSize;
use crate::infrastructure::snapshot::SnapshotKind;
use crate::infrastructure::snapshot::SnapshotRequest;
use crate::infrastructure::snapshot::SnapshotSelectionMode;
use crate::infrastructure::snapshot::SnapshotStore;
use crate::infrastructure::snapshot::SnapshotView;
use crate::infrastructure::snapshot::SnapshotWeight;
use crate::model::contract::observed_at;
use crate::model::contract::observed_at_from_millis;
use crate::model::contract::paginate;
use crate::model::contract::Page;
use crate::model::contract::PageRequest;
use crate::model::contract::QueryCompleteness;
use crate::model::contract::QueryPayload;
use crate::model::contract::QueryResult;
use crate::model::contract::QuerySource;
use crate::model::contract::SourceFailure;
use crate::model::contract::SourceFailureCode;
use crate::model::contract::SCHEMA_VERSION;
use crate::model::diagnosis::DiagnosisReport;
use crate::service::diagnosis_collector::ConsumerLagEvidence;
use crate::service::diagnosis_rules;
use crate::tools::broker_tools::BrokerDiagnosticsArgs;
use crate::tools::broker_tools::BrokerDiagnosticsOutput;
use crate::tools::broker_tools::DescribeBrokerArgs;
use crate::tools::broker_tools::DescribeBrokerOutput;
use crate::tools::cluster_tools::ClusterOverviewArgs;
use crate::tools::cluster_tools::ClusterOverviewOutput;
use crate::tools::config_tools::BrokerConfigSummaryArgs;
use crate::tools::config_tools::BrokerConfigSummaryOutput;
use crate::tools::config_tools::BrokerLogFilterStateArgs;
use crate::tools::config_tools::BrokerLogFilterStateOutput;
use crate::tools::config_tools::ConsumerGroupConfigStateArgs;
use crate::tools::config_tools::ConsumerGroupConfigStateOutput;
use crate::tools::config_tools::TopicConfigStateArgs;
use crate::tools::config_tools::TopicConfigStateOutput;
use crate::tools::connection_tools::ConnectionRow;
use crate::tools::connection_tools::ListConsumerConnectionsArgs;
use crate::tools::connection_tools::ListConsumerConnectionsOutput;
use crate::tools::connection_tools::ListProducerConnectionsArgs;
use crate::tools::connection_tools::ListProducerConnectionsOutput;
use crate::tools::consumer_tools::ListConsumerGroupsArgs;
use crate::tools::consumer_tools::ListConsumerGroupsOutput;
use crate::tools::consumer_tools::QueryConsumerLagArgs;
use crate::tools::consumer_tools::QueryConsumerLagOutput;
use crate::tools::diagnosis_tools::DiagnoseConsumerLagArgs;
use crate::tools::executor::ToolExecutionError;
use crate::tools::message_tools::MessageMetadataArgs;
use crate::tools::message_tools::MessageMetadataOutput;
use crate::tools::proxy_tools::ProxyDrainStateArgs;
use crate::tools::proxy_tools::ProxyDrainStateOutput;
use crate::tools::topic_tools::DescribeTopicArgs;
use crate::tools::topic_tools::DescribeTopicOutput;
use crate::tools::topic_tools::ListTopicsArgs;
use crate::tools::topic_tools::ListTopicsOutput;
use crate::tools::topic_tools::QueryTopicRouteArgs;
use crate::tools::topic_tools::QueryTopicRouteOutput;

mod consumer_observation;
mod infrastructure_observation;
mod topic_observation;

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

#[derive(Debug, Clone)]
struct ConnectionSnapshot {
    rows: Vec<ConnectionRow>,
    queried_broker_count: usize,
}

impl RetainedSize for ConnectionRow {
    fn retained_heap_size(&self) -> usize {
        self.broker_name
            .capacity()
            .saturating_add(self.client_alias.capacity())
            .saturating_add(self.language.capacity())
            .saturating_add(self.last_update_at.as_ref().map_or(0, |timestamp| timestamp.capacity()))
    }
}

impl RetainedSize for ConnectionSnapshot {
    fn retained_heap_size(&self) -> usize {
        self.rows.retained_heap_size()
    }
}

pub(crate) trait ReadOnlyQuery: Clone + Send + Sync + 'static {
    fn cluster_overview(
        &self,
        args: ClusterOverviewArgs,
    ) -> impl Future<Output = Result<QueryResult<ClusterOverviewOutput>, ToolExecutionError>> + Send;

    fn list_topics(
        &self,
        args: ListTopicsArgs,
    ) -> impl Future<Output = Result<QueryResult<ListTopicsOutput>, ToolExecutionError>> + Send;

    fn describe_topic(
        &self,
        args: DescribeTopicArgs,
    ) -> impl Future<Output = Result<QueryResult<DescribeTopicOutput>, ToolExecutionError>> + Send;

    fn query_topic_route(
        &self,
        args: QueryTopicRouteArgs,
    ) -> impl Future<Output = Result<QueryResult<QueryTopicRouteOutput>, ToolExecutionError>> + Send;

    fn topic_stats(
        &self,
        _args: crate::tools::topic_tools::GetTopicStatsArgs,
    ) -> impl Future<Output = Result<QueryResult<crate::tools::topic_tools::GetTopicStatsOutput>, ToolExecutionError>> + Send
    {
        async {
            Err(ToolExecutionError::Backend(
                "Topic statistics are unavailable".to_string(),
            ))
        }
    }

    fn topic_config(
        &self,
        _args: crate::tools::config_tools::GetTopicConfigArgs,
    ) -> impl Future<Output = Result<QueryResult<crate::tools::config_tools::GetTopicConfigOutput>, ToolExecutionError>> + Send
    {
        async {
            Err(ToolExecutionError::Backend(
                "Topic configuration is unavailable".to_string(),
            ))
        }
    }

    fn list_consumer_groups(
        &self,
        args: ListConsumerGroupsArgs,
    ) -> impl Future<Output = Result<QueryResult<ListConsumerGroupsOutput>, ToolExecutionError>> + Send;

    fn describe_consumer_group(
        &self,
        cluster: String,
        group: String,
    ) -> impl Future<Output = Result<QueryResult<crate::tools::consumer_tools::ConsumerGroupSummary>, ToolExecutionError>>
           + Send {
        async move {
            let result = self
                .list_consumer_groups(ListConsumerGroupsArgs {
                    cluster: Some(cluster.clone()),
                    filter: Some(group.clone()),
                    page: PageRequest {
                        limit: Some(crate::model::contract::MAX_PAGE_LIMIT),
                        cursor: None,
                    },
                })
                .await?;
            let summary = result
                .data
                .page
                .items
                .iter()
                .find(|summary| summary.group == group)
                .cloned()
                .ok_or_else(|| {
                    ToolExecutionError::InvalidArguments(format!(
                        "consumer group not found in cluster {cluster}: {group}"
                    ))
                })?;
            Ok(QueryResult::from_payload(
                QueryPayload::new(summary, result.partial, result.warnings, result.source_failures),
                result.observed_at,
                result.freshness_ms,
                result.cache_status,
            ))
        }
    }

    fn query_consumer_lag(
        &self,
        args: QueryConsumerLagArgs,
    ) -> impl Future<Output = Result<QueryResult<QueryConsumerLagOutput>, ToolExecutionError>> + Send;

    fn consumer_group_details(
        &self,
        _args: crate::tools::consumer_tools::GetConsumerGroupDetailsArgs,
    ) -> impl Future<
        Output = Result<QueryResult<crate::tools::consumer_tools::GetConsumerGroupDetailsOutput>, ToolExecutionError>,
    > + Send {
        async {
            Err(ToolExecutionError::Backend(
                "consumer group details are unavailable".to_string(),
            ))
        }
    }

    fn consumer_progress(
        &self,
        _args: crate::tools::consumer_tools::GetConsumerProgressArgs,
    ) -> impl Future<
        Output = Result<QueryResult<crate::tools::consumer_tools::GetConsumerProgressOutput>, ToolExecutionError>,
    > + Send {
        async {
            Err(ToolExecutionError::Backend(
                "consumer progress is unavailable".to_string(),
            ))
        }
    }

    fn describe_broker(
        &self,
        args: DescribeBrokerArgs,
    ) -> impl Future<Output = Result<QueryResult<DescribeBrokerOutput>, ToolExecutionError>> + Send;

    fn broker_diagnostics(
        &self,
        _args: BrokerDiagnosticsArgs,
    ) -> impl Future<Output = Result<QueryResult<BrokerDiagnosticsOutput>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "exact Broker diagnostics are unavailable".to_string(),
            ))
        }
    }

    fn broker_config_summary(
        &self,
        _args: BrokerConfigSummaryArgs,
    ) -> impl Future<Output = Result<QueryResult<BrokerConfigSummaryOutput>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "exact Broker configuration is unavailable".to_string(),
            ))
        }
    }

    fn broker_log_filter_state(
        &self,
        _args: BrokerLogFilterStateArgs,
    ) -> impl Future<Output = Result<QueryResult<BrokerLogFilterStateOutput>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "exact Broker log-filter state is unavailable".to_string(),
            ))
        }
    }

    fn proxy_drain_state(
        &self,
        _args: ProxyDrainStateArgs,
    ) -> impl Future<Output = Result<QueryResult<ProxyDrainStateOutput>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "Proxy drain state is unavailable".to_string(),
            ))
        }
    }

    fn list_consumer_connections(
        &self,
        _args: ListConsumerConnectionsArgs,
    ) -> impl Future<Output = Result<QueryResult<ListConsumerConnectionsOutput>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "consumer connection observations are unavailable".to_string(),
            ))
        }
    }

    fn list_producer_connections(
        &self,
        _args: ListProducerConnectionsArgs,
    ) -> impl Future<Output = Result<QueryResult<ListProducerConnectionsOutput>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "producer connection observations are unavailable".to_string(),
            ))
        }
    }

    fn message_metadata(
        &self,
        _args: MessageMetadataArgs,
    ) -> impl Future<Output = Result<QueryResult<MessageMetadataOutput>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "message metadata is unavailable".to_string(),
            ))
        }
    }

    fn topic_config_state(
        &self,
        _args: TopicConfigStateArgs,
    ) -> impl Future<Output = Result<QueryResult<TopicConfigStateOutput>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "Topic configuration state is unavailable".to_string(),
            ))
        }
    }

    fn consumer_group_config_state(
        &self,
        _args: ConsumerGroupConfigStateArgs,
    ) -> impl Future<Output = Result<QueryResult<ConsumerGroupConfigStateOutput>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "Consumer Group configuration state is unavailable".to_string(),
            ))
        }
    }

    fn ha_status(
        &self,
        _args: crate::tools::infrastructure_tools::GetHaStatusArgs,
    ) -> impl Future<
        Output = Result<QueryResult<crate::tools::infrastructure_tools::GetHaStatusOutput>, ToolExecutionError>,
    > + Send {
        async {
            Err(ToolExecutionError::Backend(
                "HA observations are unavailable".to_string(),
            ))
        }
    }

    fn controller_metadata(
        &self,
        _args: crate::tools::infrastructure_tools::GetControllerMetadataArgs,
    ) -> impl Future<
        Output = Result<
            QueryResult<crate::tools::infrastructure_tools::GetControllerMetadataOutput>,
            ToolExecutionError,
        >,
    > + Send {
        async {
            Err(ToolExecutionError::Backend(
                "Controller metadata is unavailable".to_string(),
            ))
        }
    }

    fn nameserver_config_summary(
        &self,
        _args: crate::tools::infrastructure_tools::GetNameserverConfigSummaryArgs,
    ) -> impl Future<
        Output = Result<
            QueryResult<crate::tools::infrastructure_tools::GetNameserverConfigSummaryOutput>,
            ToolExecutionError,
        >,
    > + Send {
        async {
            Err(ToolExecutionError::Backend(
                "NameServer configuration is unavailable".to_string(),
            ))
        }
    }

    fn diagnose_consumer_lag(
        &self,
        args: DiagnoseConsumerLagArgs,
    ) -> impl Future<Output = Result<QueryResult<DiagnosisReport>, ToolExecutionError>> + Send;
}

#[derive(Clone)]
pub(crate) struct QueryFacade<F> {
    config: McpConfig,
    factory: F,
    control: WorkflowControl,
    cache: QueryCache,
    snapshots: SnapshotStore,
    aliases: IdentifierAliaser,
    visibility_class: VisibilityClass,
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
            snapshots: SnapshotStore::new(config.cache.cursor_snapshot_max_entries),
            aliases: IdentifierAliaser::default(),
            config,
            factory,
            control,
            visibility_class: VisibilityClass::default(),
        }
    }

    pub(crate) fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.control.cancellation = cancellation;
        self
    }

    pub(crate) fn with_visibility_class(mut self, visibility_class: VisibilityClass) -> Self {
        self.visibility_class = visibility_class;
        self
    }

    #[cfg(test)]
    pub(crate) fn visibility_class(&self) -> VisibilityClass {
        self.visibility_class
    }

    pub(crate) fn cache_metrics(&self) -> CacheMetricsSnapshot {
        self.cache.metrics().merge(self.snapshots.metrics())
    }

    pub(crate) async fn invalidate_cache(&self) -> usize {
        self.cache.clear().await.saturating_add(self.snapshots.clear().await)
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
                    let page = PageRequest::default();
                    let topic_snapshot = self.topic_inventory_snapshot(cluster.clone(), None, &page).await?;
                    let consumer_snapshot = self
                        .consumer_group_inventory_snapshot(cluster.clone(), None, false, &page)
                        .await?;
                    self.run_workflow(cluster, move |session, cluster| {
                        Box::pin(async move {
                            let brokers = session.broker_rows().await?;
                            let mut completeness = brokers.completeness();
                            completeness.merge(topic_snapshot.payload.completeness());
                            completeness.merge(consumer_snapshot.payload.completeness());
                            Ok(completeness.wrap(ClusterOverviewOutput {
                                cluster: cluster.name.clone(),
                                namesrv_addr: cluster.namesrv_addr.clone(),
                                brokers: brokers.data,
                                topic_count: topic_snapshot.payload.data.len(),
                                consumer_group_count: consumer_snapshot.payload.data.len(),
                                generated_at: observed_at(),
                            }))
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
        let snapshot = self
            .topic_inventory_snapshot(cluster.clone(), args.filter.as_deref(), &args.page)
            .await?;
        let entries = snapshot
            .payload
            .data
            .iter()
            .cloned()
            .map(|topic| crate::tools::topic_tools::TopicListEntry {
                topic,
                cluster: Some(cluster.rocketmq_cluster_name.clone()),
                consumer_group: None,
            })
            .collect::<Vec<_>>();
        let page = self.snapshots.page(&snapshot, &entries)?;
        Ok(query_result_from_snapshot(
            &snapshot,
            snapshot.payload.completeness().wrap(ListTopicsOutput {
                cluster: cluster.name,
                namesrv_addr: cluster.namesrv_addr,
                page,
                generated_at: observed_at(),
            }),
        ))
    }

    pub(crate) async fn describe_topic(
        &self,
        mut args: DescribeTopicArgs,
    ) -> Result<QueryResult<DescribeTopicOutput>, ToolExecutionError> {
        args.topic = normalized_identifier("topic", &args.topic)?;
        let cluster = self.resolve_cluster(Some(&args.cluster))?;
        let snapshot = self
            .topic_route_snapshot(cluster.clone(), args.topic.clone(), &args.page)
            .await?;
        let route = topic_route_output_from_snapshot(&self.snapshots, &snapshot, &cluster, &args.topic)?;
        Ok(query_result_from_snapshot(
            &snapshot,
            snapshot.payload.completeness().wrap(describe_topic_output(&route)),
        ))
    }

    pub(crate) async fn query_topic_route(
        &self,
        mut args: QueryTopicRouteArgs,
    ) -> Result<QueryResult<QueryTopicRouteOutput>, ToolExecutionError> {
        args.topic = normalized_identifier("topic", &args.topic)?;
        let cluster = self.resolve_cluster(Some(&args.cluster))?;
        let snapshot = self
            .topic_route_snapshot(cluster.clone(), args.topic.clone(), &args.page)
            .await?;
        let output = topic_route_output_from_snapshot(&self.snapshots, &snapshot, &cluster, &args.topic)?;
        Ok(query_result_from_snapshot(
            &snapshot,
            snapshot.payload.completeness().wrap(output),
        ))
    }

    pub(crate) async fn list_consumer_groups(
        &self,
        args: ListConsumerGroupsArgs,
    ) -> Result<QueryResult<ListConsumerGroupsOutput>, ToolExecutionError> {
        let cluster = self.resolve_cluster(args.cluster.as_deref())?;
        let snapshot = self
            .consumer_group_inventory_snapshot(cluster.clone(), args.filter.as_deref(), false, &args.page)
            .await?;
        let names_page = self.snapshots.page(&snapshot, &snapshot.payload.data)?;
        let selected_names = names_page.items.clone();
        if selected_names.is_empty() {
            return Ok(query_result_from_snapshot(
                &snapshot,
                snapshot.payload.completeness().wrap(ListConsumerGroupsOutput {
                    cluster: cluster.name,
                    namesrv_addr: cluster.namesrv_addr,
                    page: Page {
                        items: Vec::new(),
                        count: 0,
                        total_count: names_page.total_count,
                        has_more: names_page.has_more,
                        next_cursor: names_page.next_cursor,
                    },
                    generated_at: observed_at(),
                }),
            ));
        }
        let key = self.cache_key(
            "consumer_group_page_enrichment",
            &cluster.name,
            &format!(
                "snapshot={}|cursor={}",
                snapshot.identity(),
                args.page.cursor.as_deref().unwrap_or("first")
            ),
        );
        let ttl = Duration::from_millis(self.config.cache.consumer_lag_ttl_ms);
        let enrichment_snapshot = snapshot.clone();
        let enriched = self
            .cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                move || async move {
                    self.run_workflow(cluster.clone(), move |session, cluster| {
                        Box::pin(async move {
                            let groups = session.consumer_groups_exact(&selected_names).await?;
                            let mut completeness = enrichment_snapshot.payload.completeness();
                            completeness.merge(groups.completeness());
                            let mut groups = groups.data;
                            groups.sort_by(|left, right| left.group.cmp(&right.group));
                            Ok(completeness.wrap(ListConsumerGroupsOutput {
                                cluster: cluster.name.clone(),
                                namesrv_addr: cluster.namesrv_addr.clone(),
                                page: Page {
                                    count: groups.len(),
                                    items: groups,
                                    total_count: names_page.total_count,
                                    has_more: names_page.has_more,
                                    next_cursor: names_page.next_cursor,
                                },
                                generated_at: observed_at(),
                            }))
                        })
                    })
                    .await
                },
            )
            .await?;
        Ok(QueryResult::from_payload(
            QueryPayload::new(
                enriched.data,
                enriched.partial,
                enriched.warnings,
                enriched.source_failures,
            ),
            snapshot.observed_at,
            snapshot.freshness_ms,
            enriched.cache_status,
        ))
    }

    pub(crate) async fn query_consumer_lag(
        &self,
        mut args: QueryConsumerLagArgs,
    ) -> Result<QueryResult<QueryConsumerLagOutput>, ToolExecutionError> {
        args.topic = normalized_identifier("topic", &args.topic)?;
        args.consumer_group = normalized_identifier("consumer_group", &args.consumer_group)?;
        let cluster = self.resolve_cluster(Some(&args.cluster))?;
        let snapshot = self
            .consumer_lag_snapshot(
                cluster.clone(),
                args.topic.clone(),
                args.consumer_group.clone(),
                &args.page,
            )
            .await?;
        let output =
            consumer_lag_output_from_snapshot(&self.snapshots, &snapshot, &cluster, args.topic, args.consumer_group)?;
        Ok(query_result_from_snapshot(
            &snapshot,
            snapshot.payload.completeness().wrap(output),
        ))
    }

    pub(crate) async fn describe_consumer_group(
        &self,
        cluster_name: String,
        group: String,
    ) -> Result<QueryResult<crate::tools::consumer_tools::ConsumerGroupSummary>, ToolExecutionError> {
        let group = normalized_identifier("consumer_group", &group)?;
        let cluster = self.resolve_cluster(Some(&cluster_name))?;
        let page = PageRequest {
            limit: Some(1),
            cursor: None,
        };
        let snapshot = self
            .consumer_group_inventory_snapshot(cluster.clone(), Some(&group), true, &page)
            .await?;
        if snapshot.payload.data.is_empty() {
            return Err(ToolExecutionError::InvalidArguments(format!(
                "consumer group not found in cluster {}: {group}",
                cluster.name
            )));
        }
        let selected = snapshot.payload.data.clone();
        let key = self.cache_key(
            "consumer_group_exact_enrichment",
            &cluster.name,
            &format!("snapshot={}", snapshot.identity()),
        );
        let ttl = Duration::from_millis(self.config.cache.consumer_lag_ttl_ms);
        let inventory_completeness = snapshot.payload.completeness();
        let enriched = self
            .cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                || async {
                    self.run_workflow(cluster, move |session, _| {
                        Box::pin(async move {
                            let groups = session.consumer_groups_exact(&selected).await?;
                            let mut completeness = inventory_completeness;
                            completeness.merge(groups.completeness());
                            let summary = groups
                                .data
                                .into_iter()
                                .find(|summary| summary.group == group)
                                .ok_or_else(|| {
                                    ToolExecutionError::Backend(
                                        "selected consumer group enrichment was unavailable".to_string(),
                                    )
                                })?;
                            Ok(completeness.wrap(summary))
                        })
                    })
                    .await
                },
            )
            .await?;
        Ok(QueryResult::from_payload(
            QueryPayload::new(
                enriched.data,
                enriched.partial,
                enriched.warnings,
                enriched.source_failures,
            ),
            snapshot.observed_at,
            snapshot.freshness_ms,
            enriched.cache_status,
        ))
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

    pub(crate) async fn broker_diagnostics(
        &self,
        mut args: BrokerDiagnosticsArgs,
    ) -> Result<QueryResult<BrokerDiagnosticsOutput>, ToolExecutionError> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        args.broker_name = normalized_logical_identifier("broker_name", &args.broker_name)?;
        let cluster = self.resolve_required_cluster(&args.cluster)?;
        let key = self.cache_key(
            "broker_diagnostics",
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
                    self.run_workflow(cluster, move |session, _| {
                        Box::pin(async move { session.broker_diagnostics(&args.broker_name).await })
                    })
                    .await
                },
            )
            .await
    }

    pub(crate) async fn broker_config_summary(
        &self,
        mut args: BrokerConfigSummaryArgs,
    ) -> Result<QueryResult<BrokerConfigSummaryOutput>, ToolExecutionError> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        args.broker_name = normalized_logical_identifier("broker_name", &args.broker_name)?;
        let cluster = self.resolve_required_cluster(&args.cluster)?;
        let key = self.cache_key(
            "broker_config_summary",
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
                    self.run_workflow(cluster, move |session, _| {
                        Box::pin(async move { session.broker_config_summary(&args.broker_name).await })
                    })
                    .await
                },
            )
            .await
    }

    pub(crate) async fn broker_log_filter_state(
        &self,
        mut args: BrokerLogFilterStateArgs,
    ) -> Result<QueryResult<BrokerLogFilterStateOutput>, ToolExecutionError> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        args.broker_name = normalized_logical_identifier("broker_name", &args.broker_name)?;
        args.logger = normalized_broker_logger(&args.logger)?;
        let cluster = self.resolve_required_cluster(&args.cluster)?;
        let key = self.cache_key(
            "broker_log_filter_state",
            &cluster.name,
            &format!("broker={}|logger={}", args.broker_name, args.logger),
        );
        let ttl = Duration::from_millis(self.config.cache.broker_metrics_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                || async {
                    self.run_workflow(cluster, move |session, _| {
                        Box::pin(async move { session.broker_log_filter_state(&args.broker_name, &args.logger).await })
                    })
                    .await
                },
            )
            .await
    }

    pub(crate) async fn proxy_drain_state(
        &self,
        mut args: ProxyDrainStateArgs,
    ) -> Result<QueryResult<ProxyDrainStateOutput>, ToolExecutionError> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        args.proxy_name = normalized_logical_identifier("proxy_name", &args.proxy_name)?;
        let cluster = self.resolve_required_cluster(&args.cluster)?;
        let proxy_endpoint = self
            .resolve_proxy_endpoint(&cluster.name, &args.proxy_name)?
            .to_string();
        let key = self.cache_key(
            "proxy_drain_state",
            &cluster.name,
            &format!("proxy={}", args.proxy_name),
        );
        let ttl = Duration::from_millis(self.config.cache.broker_metrics_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                || async {
                    self.run_workflow(cluster, move |session, _| {
                        Box::pin(async move { session.proxy_drain_state(&args.proxy_name, &proxy_endpoint).await })
                    })
                    .await
                },
            )
            .await
    }

    pub(crate) async fn list_consumer_connections(
        &self,
        mut args: ListConsumerConnectionsArgs,
    ) -> Result<QueryResult<ListConsumerConnectionsOutput>, ToolExecutionError> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        args.consumer_group = normalized_identifier("consumer_group", &args.consumer_group)?;
        let cluster = self.resolve_required_cluster(&args.cluster)?;
        let snapshot = self
            .connection_snapshot(
                SnapshotKind::ConsumerConnections,
                cluster.clone(),
                format!("consumer_group={}", args.consumer_group),
                args.page.clone(),
                |session| {
                    let group = args.consumer_group.clone();
                    Box::pin(async move { session.consumer_connections(&group).await })
                },
            )
            .await?;
        let page = self.snapshots.page(&snapshot, &snapshot.payload.data.rows)?;
        Ok(query_result_from_snapshot(
            &snapshot,
            snapshot.payload.completeness().wrap(ListConsumerConnectionsOutput {
                cluster: cluster.name,
                consumer_group: args.consumer_group,
                queried_broker_count: snapshot.payload.data.queried_broker_count,
                page,
                generated_at: observed_at(),
            }),
        ))
    }

    pub(crate) async fn list_producer_connections(
        &self,
        mut args: ListProducerConnectionsArgs,
    ) -> Result<QueryResult<ListProducerConnectionsOutput>, ToolExecutionError> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        args.topic = normalized_identifier("topic", &args.topic)?;
        args.producer_group = normalized_identifier("producer_group", &args.producer_group)?;
        let cluster = self.resolve_required_cluster(&args.cluster)?;
        let selector = format!("topic={}|producer_group={}", args.topic, args.producer_group);
        let snapshot = self
            .connection_snapshot(
                SnapshotKind::ProducerConnections,
                cluster.clone(),
                selector,
                args.page.clone(),
                |session| {
                    let topic = args.topic.clone();
                    let producer_group = args.producer_group.clone();
                    Box::pin(async move { session.producer_connections(&topic, &producer_group).await })
                },
            )
            .await?;
        let page = self.snapshots.page(&snapshot, &snapshot.payload.data.rows)?;
        Ok(query_result_from_snapshot(
            &snapshot,
            snapshot.payload.completeness().wrap(ListProducerConnectionsOutput {
                cluster: cluster.name,
                topic: args.topic,
                producer_group: args.producer_group,
                queried_broker_count: snapshot.payload.data.queried_broker_count,
                page,
                generated_at: observed_at(),
            }),
        ))
    }

    pub(crate) async fn message_metadata(
        &self,
        mut args: MessageMetadataArgs,
    ) -> Result<QueryResult<MessageMetadataOutput>, ToolExecutionError> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        args.message_id = normalized_identifier("message_id", &args.message_id)?;
        let cluster = self.resolve_required_cluster(&args.cluster)?;
        let lookup_alias = self.aliases.message_alias(&args.message_id).map_err(alias_error)?;
        let key = self.cache_key("message_metadata", &cluster.name, &format!("message={lookup_alias}"));
        let ttl = Duration::from_millis(self.config.cache.broker_metrics_ttl_ms);
        let aliases = self.aliases.clone();
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                || async {
                    self.run_workflow(cluster, move |session, cluster| {
                        Box::pin(async move {
                            let metadata = session.message_metadata(&args.message_id).await?;
                            let message_alias = aliases.message_alias(&metadata.message_id).map_err(alias_error)?;
                            let unique_message_alias = metadata
                                .unique_message_id
                                .as_deref()
                                .map(|message_id| aliases.unique_message_alias(message_id))
                                .transpose()
                                .map_err(alias_error)?;
                            Ok(QueryPayload::complete(MessageMetadataOutput {
                                cluster: cluster.name.clone(),
                                message_alias,
                                unique_message_alias,
                                topic: metadata.topic,
                                born_at: observed_at_from_millis(metadata.born_timestamp),
                                stored_at: observed_at_from_millis(metadata.store_timestamp),
                                queue_id: metadata.queue_id,
                                queue_offset: metadata.queue_offset,
                                store_size: metadata.store_size,
                                reconsume_times: metadata.reconsume_times,
                                sys_flag: metadata.sys_flag,
                                flag: metadata.flag,
                                prepared_transaction_offset: metadata.prepared_transaction_offset,
                            }))
                        })
                    })
                    .await
                },
            )
            .await
    }

    pub(crate) async fn topic_config_state(
        &self,
        mut args: TopicConfigStateArgs,
    ) -> Result<QueryResult<TopicConfigStateOutput>, ToolExecutionError> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        args.topic = normalized_identifier("topic", &args.topic)?;
        args.broker_names = normalized_broker_names(args.broker_names)?;
        let cluster = self.resolve_required_cluster(&args.cluster)?;
        let key = self.cache_key(
            "topic_config_state",
            &cluster.name,
            &format!("topic={}|brokers={}", args.topic, args.broker_names.join(",")),
        );
        let ttl = Duration::from_millis(self.config.cache.broker_metrics_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                || async {
                    self.run_workflow(cluster, move |session, _| {
                        Box::pin(async move { session.topic_config_state(&args.topic, &args.broker_names).await })
                    })
                    .await
                },
            )
            .await
    }

    pub(crate) async fn consumer_group_config_state(
        &self,
        mut args: ConsumerGroupConfigStateArgs,
    ) -> Result<QueryResult<ConsumerGroupConfigStateOutput>, ToolExecutionError> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        args.group = normalized_identifier("group", &args.group)?;
        args.broker_names = normalized_broker_names(args.broker_names)?;
        let cluster = self.resolve_required_cluster(&args.cluster)?;
        let key = self.cache_key(
            "consumer_group_config_state",
            &cluster.name,
            &format!("group={}|brokers={}", args.group, args.broker_names.join(",")),
        );
        let ttl = Duration::from_millis(self.config.cache.broker_metrics_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                || async {
                    self.run_workflow(cluster, move |session, _| {
                        Box::pin(async move {
                            session
                                .consumer_group_config_state(&args.group, &args.broker_names)
                                .await
                        })
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
            &format!("topic={}|group={}", args.topic, args.consumer_group),
        );
        let ttl = Duration::from_millis(self.config.cache.consumer_lag_ttl_ms);
        let policy = diagnosis_rules::ConsumerLagPolicy::from(&self.config.diagnosis);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                move || async {
                    self.run_workflow(cluster, move |session, cluster| {
                        Box::pin(async move {
                            let mut completeness = QueryCompleteness::default();
                            let lag_result = match session.consumer_lag(&args.topic, &args.consumer_group).await {
                                Ok(lag) => {
                                    completeness.merge(lag.completeness());
                                    consumer_lag_output(
                                        cluster,
                                        args.topic.clone(),
                                        args.consumer_group.clone(),
                                        &PageRequest::default(),
                                        lag.data,
                                    )
                                }
                                Err(error) => {
                                    completeness.merge(completeness_for_error(
                                        QuerySource::ConsumerStatistics,
                                        &args.consumer_group,
                                        &error,
                                    ));
                                    Err(error)
                                }
                            };
                            let (topic_result, route_result) = match session.topic_route(&args.topic).await {
                                Ok(route) => {
                                    let route =
                                        topic_route_output(cluster, &args.topic, route, &PageRequest::default())?;
                                    (Ok(describe_topic_output(&route)), Ok(route))
                                }
                                Err(error) => {
                                    completeness.merge(completeness_for_error(
                                        QuerySource::TopicRoute,
                                        &args.topic,
                                        &error,
                                    ));
                                    (
                                        Err(ToolExecutionError::Backend(
                                            "topic route source is unavailable".to_string(),
                                        )),
                                        Err(error),
                                    )
                                }
                            };
                            let broker_result = match top_lag_broker(lag_result.as_ref().ok()) {
                                Some(broker_name) => {
                                    match describe_broker_in_session(session, cluster, broker_name.clone()).await {
                                        Ok(broker) => {
                                            completeness.merge(broker.completeness());
                                            Some(Ok(broker.data))
                                        }
                                        Err(error) => {
                                            completeness.merge(completeness_for_error(
                                                QuerySource::BrokerRuntime,
                                                &broker_name,
                                                &error,
                                            ));
                                            Some(Err(error))
                                        }
                                    }
                                }
                                None => None,
                            };

                            let report = diagnosis_rules::evaluate(
                                &args,
                                ConsumerLagEvidence {
                                    lag: lag_result,
                                    topic: topic_result,
                                    route: route_result,
                                    broker: broker_result,
                                },
                                &policy,
                            );
                            if report.partial {
                                completeness.partial = true;
                                completeness.warnings.push("diagnosis_evidence_incomplete".to_string());
                            }
                            Ok(completeness.wrap(report))
                        })
                    })
                    .await
                },
            )
            .await
    }

    async fn topic_inventory_snapshot(
        &self,
        cluster: ResolvedCluster,
        filter: Option<&str>,
        page: &PageRequest,
    ) -> Result<SnapshotView<Vec<String>>, ToolExecutionError> {
        let filter = normalized_filter(filter).unwrap_or_default();
        let request = SnapshotRequest::try_new(
            SnapshotKind::TopicInventory,
            cluster.name.clone(),
            filter.clone(),
            page,
            self.visibility_class.as_str(),
        )?;
        self.snapshots
            .get_or_load(
                request,
                page.cursor.as_deref(),
                self.cursor_snapshot_ttl(),
                self.snapshot_response_ttl(self.config.cache.topic_list_ttl_ms),
                |topics: &Vec<String>| SnapshotWeight::inventory(topics.len()),
                &self.control.cancellation,
                || {
                    self.run_workflow(cluster, move |session, _| {
                        Box::pin(async move {
                            let mut topics = sorted_unique_topic_names(session.topic_inventory().await?);
                            if !filter.is_empty() {
                                topics.retain(|topic| topic.to_ascii_lowercase().contains(&filter));
                            }
                            Ok(QueryPayload::complete(topics))
                        })
                    })
                },
            )
            .await
    }

    async fn consumer_group_inventory_snapshot(
        &self,
        cluster: ResolvedCluster,
        filter: Option<&str>,
        exact: bool,
        page: &PageRequest,
    ) -> Result<SnapshotView<Vec<String>>, ToolExecutionError> {
        let filter = if exact {
            filter.map(str::trim).unwrap_or_default().to_string()
        } else {
            normalized_filter(filter).unwrap_or_default()
        };
        let selection_mode = if exact {
            SnapshotSelectionMode::ExactIdentifier
        } else {
            SnapshotSelectionMode::LiteralFilter
        };
        let request = SnapshotRequest::try_new_with_selection(
            SnapshotKind::ConsumerGroupInventory,
            cluster.name.clone(),
            filter.clone(),
            selection_mode,
            page,
            self.visibility_class.as_str(),
        )?;
        self.snapshots
            .get_or_load(
                request,
                page.cursor.as_deref(),
                self.cursor_snapshot_ttl(),
                self.snapshot_response_ttl(self.config.cache.consumer_lag_ttl_ms),
                |groups: &Vec<String>| SnapshotWeight::inventory(groups.len()),
                &self.control.cancellation,
                || {
                    self.run_workflow(cluster, move |session, _| {
                        Box::pin(async move {
                            let groups = session.consumer_group_inventory().await?;
                            Ok(groups.map(|mut groups| {
                                groups.sort();
                                groups.dedup();
                                if !filter.is_empty() {
                                    if exact {
                                        groups.retain(|group| group == &filter);
                                    } else {
                                        groups.retain(|group| group.to_ascii_lowercase().contains(&filter));
                                    }
                                }
                                groups
                            }))
                        })
                    })
                },
            )
            .await
    }

    async fn topic_route_snapshot(
        &self,
        cluster: ResolvedCluster,
        topic: String,
        page: &PageRequest,
    ) -> Result<SnapshotView<SessionTopicRoute>, ToolExecutionError> {
        let request = SnapshotRequest::try_new(
            SnapshotKind::TopicRoute,
            cluster.name.clone(),
            format!("topic={topic}"),
            page,
            self.visibility_class.as_str(),
        )?;
        self.snapshots
            .get_or_load(
                request,
                page.cursor.as_deref(),
                self.cursor_snapshot_ttl(),
                self.snapshot_response_ttl(self.config.cache.topic_list_ttl_ms),
                |route: &SessionTopicRoute| SnapshotWeight::detail(route.queues.len()),
                &self.control.cancellation,
                || {
                    self.run_workflow(cluster, move |session, _| {
                        Box::pin(async move { session.topic_route(&topic).await.map(QueryPayload::complete) })
                    })
                },
            )
            .await
    }

    async fn consumer_lag_snapshot(
        &self,
        cluster: ResolvedCluster,
        topic: String,
        consumer_group: String,
        page: &PageRequest,
    ) -> Result<SnapshotView<SessionConsumerLag>, ToolExecutionError> {
        let request = SnapshotRequest::try_new(
            SnapshotKind::ConsumerLag,
            cluster.name.clone(),
            format!("topic={topic}|group={consumer_group}"),
            page,
            self.visibility_class.as_str(),
        )?;
        self.snapshots
            .get_or_load(
                request,
                page.cursor.as_deref(),
                self.cursor_snapshot_ttl(),
                self.snapshot_response_ttl(self.config.cache.consumer_lag_ttl_ms),
                |lag: &SessionConsumerLag| SnapshotWeight::detail(lag.queues.len()),
                &self.control.cancellation,
                || {
                    self.run_workflow(cluster, move |session, _| {
                        Box::pin(async move { session.consumer_lag(&topic, &consumer_group).await })
                    })
                },
            )
            .await
    }

    async fn connection_snapshot<O>(
        &self,
        kind: SnapshotKind,
        cluster: ResolvedCluster,
        selector: String,
        page: PageRequest,
        operation: O,
    ) -> Result<SnapshotView<ConnectionSnapshot>, ToolExecutionError>
    where
        O: for<'a> FnOnce(&'a mut F::Session) -> WorkflowFuture<'a, QueryPayload<SessionConnections>>,
    {
        let request = SnapshotRequest::try_new_with_selection(
            kind,
            cluster.name.clone(),
            selector,
            SnapshotSelectionMode::ExactIdentifier,
            &page,
            self.visibility_class.as_str(),
        )?;
        let aliases = self.aliases.clone();
        self.snapshots
            .get_or_load(
                request,
                page.cursor.as_deref(),
                self.cursor_snapshot_ttl(),
                self.snapshot_response_ttl(self.config.cache.consumer_lag_ttl_ms),
                |connections: &ConnectionSnapshot| SnapshotWeight::detail(connections.rows.len()),
                &self.control.cancellation,
                || {
                    self.run_workflow(cluster, move |session, _| {
                        let observations = operation(session);
                        Box::pin(async move {
                            let observations = observations.await?;
                            project_connections(observations, &aliases)
                        })
                    })
                },
            )
            .await
    }

    fn cursor_snapshot_ttl(&self) -> Duration {
        Duration::from_millis(self.config.cache.cursor_snapshot_ttl_ms)
    }

    fn snapshot_response_ttl(&self, ttl_ms: u64) -> Option<Duration> {
        (self.config.cache.enabled && self.config.cache.max_entries > 0 && ttl_ms > 0)
            .then(|| Duration::from_millis(ttl_ms))
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
            rocketmq_cluster_name: config.physical_cluster_name().to_string(),
            namesrv_addr: config.namesrv_addr.clone(),
            credentials: config
                .resolve_admin_credentials()
                .map_err(|error| ToolExecutionError::Backend(error.to_string()))?,
            controller_targets: config
                .controllers
                .iter()
                .map(|controller| {
                    rocketmq_admin_core::read_client_adapter::ControllerObservationTarget::new(
                        controller.name.clone(),
                        controller.endpoint.clone(),
                    )
                })
                .collect(),
        })
    }

    fn resolve_required_cluster(&self, cluster: &str) -> Result<ResolvedCluster, ToolExecutionError> {
        if cluster.trim().is_empty() {
            return Err(ToolExecutionError::InvalidArguments(
                "cluster must not be empty".to_string(),
            ));
        }
        self.resolve_cluster(Some(cluster))
    }

    fn resolve_proxy_endpoint<'a>(
        &'a self,
        cluster_name: &str,
        proxy_name: &str,
    ) -> Result<&'a str, ToolExecutionError> {
        self.config
            .clusters
            .iter()
            .find(|cluster| cluster.name == cluster_name)
            .and_then(|cluster| cluster.proxy_endpoint(proxy_name))
            .ok_or_else(|| {
                ToolExecutionError::InvalidArguments(format!("proxy not found in cluster {cluster_name}: {proxy_name}"))
            })
    }

    fn cache_key(&self, kind: &str, cluster: &str, parameters: &str) -> String {
        format!(
            "{SCHEMA_VERSION}|{}|{kind}|cluster={}|{parameters}",
            self.visibility_class.as_str(),
            cluster.trim()
        )
    }
}

impl QueryFacade<AdminCoreSessionFactory> {
    pub(crate) fn new(
        config: McpConfig,
        client_runtime: std::sync::Arc<rocketmq_admin_core::read_client_adapter::ClientRuntime>,
    ) -> Self {
        Self::with_factory(config, AdminCoreSessionFactory::new(client_runtime))
    }
}

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

    async fn topic_stats(
        &self,
        args: crate::tools::topic_tools::GetTopicStatsArgs,
    ) -> Result<QueryResult<crate::tools::topic_tools::GetTopicStatsOutput>, ToolExecutionError> {
        QueryFacade::topic_stats(self, args).await
    }

    async fn topic_config(
        &self,
        args: crate::tools::config_tools::GetTopicConfigArgs,
    ) -> Result<QueryResult<crate::tools::config_tools::GetTopicConfigOutput>, ToolExecutionError> {
        QueryFacade::topic_config(self, args).await
    }

    async fn list_consumer_groups(
        &self,
        args: ListConsumerGroupsArgs,
    ) -> Result<QueryResult<ListConsumerGroupsOutput>, ToolExecutionError> {
        QueryFacade::list_consumer_groups(self, args).await
    }

    async fn describe_consumer_group(
        &self,
        cluster: String,
        group: String,
    ) -> Result<QueryResult<crate::tools::consumer_tools::ConsumerGroupSummary>, ToolExecutionError> {
        QueryFacade::describe_consumer_group(self, cluster, group).await
    }

    async fn query_consumer_lag(
        &self,
        args: QueryConsumerLagArgs,
    ) -> Result<QueryResult<QueryConsumerLagOutput>, ToolExecutionError> {
        QueryFacade::query_consumer_lag(self, args).await
    }

    async fn consumer_group_details(
        &self,
        args: crate::tools::consumer_tools::GetConsumerGroupDetailsArgs,
    ) -> Result<QueryResult<crate::tools::consumer_tools::GetConsumerGroupDetailsOutput>, ToolExecutionError> {
        QueryFacade::consumer_group_details(self, args).await
    }

    async fn consumer_progress(
        &self,
        args: crate::tools::consumer_tools::GetConsumerProgressArgs,
    ) -> Result<QueryResult<crate::tools::consumer_tools::GetConsumerProgressOutput>, ToolExecutionError> {
        QueryFacade::consumer_progress(self, args).await
    }

    async fn describe_broker(
        &self,
        args: DescribeBrokerArgs,
    ) -> Result<QueryResult<DescribeBrokerOutput>, ToolExecutionError> {
        QueryFacade::describe_broker(self, args).await
    }

    async fn broker_diagnostics(
        &self,
        args: BrokerDiagnosticsArgs,
    ) -> Result<QueryResult<BrokerDiagnosticsOutput>, ToolExecutionError> {
        QueryFacade::broker_diagnostics(self, args).await
    }

    async fn broker_config_summary(
        &self,
        args: BrokerConfigSummaryArgs,
    ) -> Result<QueryResult<BrokerConfigSummaryOutput>, ToolExecutionError> {
        QueryFacade::broker_config_summary(self, args).await
    }

    async fn broker_log_filter_state(
        &self,
        args: BrokerLogFilterStateArgs,
    ) -> Result<QueryResult<BrokerLogFilterStateOutput>, ToolExecutionError> {
        QueryFacade::broker_log_filter_state(self, args).await
    }

    async fn proxy_drain_state(
        &self,
        args: ProxyDrainStateArgs,
    ) -> Result<QueryResult<ProxyDrainStateOutput>, ToolExecutionError> {
        QueryFacade::proxy_drain_state(self, args).await
    }

    async fn list_consumer_connections(
        &self,
        args: ListConsumerConnectionsArgs,
    ) -> Result<QueryResult<ListConsumerConnectionsOutput>, ToolExecutionError> {
        QueryFacade::list_consumer_connections(self, args).await
    }

    async fn list_producer_connections(
        &self,
        args: ListProducerConnectionsArgs,
    ) -> Result<QueryResult<ListProducerConnectionsOutput>, ToolExecutionError> {
        QueryFacade::list_producer_connections(self, args).await
    }

    async fn message_metadata(
        &self,
        args: MessageMetadataArgs,
    ) -> Result<QueryResult<MessageMetadataOutput>, ToolExecutionError> {
        QueryFacade::message_metadata(self, args).await
    }

    async fn topic_config_state(
        &self,
        args: TopicConfigStateArgs,
    ) -> Result<QueryResult<TopicConfigStateOutput>, ToolExecutionError> {
        QueryFacade::topic_config_state(self, args).await
    }

    async fn consumer_group_config_state(
        &self,
        args: ConsumerGroupConfigStateArgs,
    ) -> Result<QueryResult<ConsumerGroupConfigStateOutput>, ToolExecutionError> {
        QueryFacade::consumer_group_config_state(self, args).await
    }

    async fn ha_status(
        &self,
        args: crate::tools::infrastructure_tools::GetHaStatusArgs,
    ) -> Result<QueryResult<crate::tools::infrastructure_tools::GetHaStatusOutput>, ToolExecutionError> {
        QueryFacade::ha_status(self, args).await
    }

    async fn controller_metadata(
        &self,
        args: crate::tools::infrastructure_tools::GetControllerMetadataArgs,
    ) -> Result<QueryResult<crate::tools::infrastructure_tools::GetControllerMetadataOutput>, ToolExecutionError> {
        QueryFacade::controller_metadata(self, args).await
    }

    async fn nameserver_config_summary(
        &self,
        args: crate::tools::infrastructure_tools::GetNameserverConfigSummaryArgs,
    ) -> Result<QueryResult<crate::tools::infrastructure_tools::GetNameserverConfigSummaryOutput>, ToolExecutionError>
    {
        QueryFacade::nameserver_config_summary(self, args).await
    }

    async fn diagnose_consumer_lag(
        &self,
        args: DiagnoseConsumerLagArgs,
    ) -> Result<QueryResult<DiagnosisReport>, ToolExecutionError> {
        QueryFacade::diagnose_consumer_lag(self, args).await
    }
}

fn query_result_from_snapshot<S, T>(snapshot: &SnapshotView<S>, payload: QueryPayload<T>) -> QueryResult<T> {
    QueryResult::from_payload(
        payload,
        snapshot.observed_at.clone(),
        snapshot.freshness_ms,
        snapshot.cache_status,
    )
}

fn consumer_lag_output_from_snapshot(
    store: &SnapshotStore,
    snapshot: &SnapshotView<SessionConsumerLag>,
    cluster: &ResolvedCluster,
    topic: String,
    consumer_group: String,
) -> Result<QueryConsumerLagOutput, ToolExecutionError> {
    let lag = &snapshot.payload.data;
    let max_queue_lag = lag.queues.iter().map(|queue| queue.lag).max().unwrap_or_default();
    let page = store.page(snapshot, &lag.queues)?;
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

fn topic_route_output_from_snapshot(
    store: &SnapshotStore,
    snapshot: &SnapshotView<SessionTopicRoute>,
    cluster: &ResolvedCluster,
    topic: &str,
) -> Result<QueryTopicRouteOutput, ToolExecutionError> {
    let route = &snapshot.payload.data;
    let read_queue_count = route.queues.iter().map(|queue| queue.read_queue_nums).sum();
    let write_queue_count = route.queues.iter().map(|queue| queue.write_queue_nums).sum();
    let page = store.page(snapshot, &route.queues)?;
    Ok(QueryTopicRouteOutput {
        cluster: cluster.name.clone(),
        namesrv_addr: cluster.namesrv_addr.clone(),
        topic: topic.to_string(),
        brokers: route.brokers.clone(),
        read_queue_count,
        write_queue_count,
        page,
        generated_at: observed_at(),
    })
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
) -> Result<QueryPayload<DescribeBrokerOutput>, ToolExecutionError>
where
    S: AdminSession,
{
    let broker_payload = session.broker_rows().await?;
    let completeness = broker_payload.completeness();
    let brokers = broker_payload
        .data
        .into_iter()
        .filter(|broker| broker.broker_name == broker_name)
        .collect::<Vec<_>>();
    if brokers.is_empty() {
        match session.probe_broker_runtime_target(&broker_name).await? {
            BrokerRuntimeTargetStatus::Available | BrokerRuntimeTargetStatus::SourceUnavailable => {
                return Err(ToolExecutionError::Backend(
                    "selected broker source is unavailable".to_string(),
                ));
            }
            BrokerRuntimeTargetStatus::NotFound => {}
        }
        return Err(ToolExecutionError::InvalidArguments(format!(
            "broker not found in cluster {}: {broker_name}",
            cluster.name
        )));
    }
    Ok(completeness.wrap(DescribeBrokerOutput {
        cluster: cluster.name.clone(),
        namesrv_addr: cluster.namesrv_addr.clone(),
        broker_name,
        brokers,
        generated_at: observed_at(),
    }))
}

fn completeness_for_error(source: QuerySource, logical_target: &str, error: &ToolExecutionError) -> QueryCompleteness {
    let (code, retryable) = match error {
        ToolExecutionError::TimedOut { .. } => (SourceFailureCode::Timeout, true),
        ToolExecutionError::RateLimited(_) => (SourceFailureCode::RateLimited, true),
        ToolExecutionError::PermissionDenied(_)
        | ToolExecutionError::UnauthorizedScope(_)
        | ToolExecutionError::TenantMismatch(_)
        | ToolExecutionError::ClusterNotAllowed(_) => (SourceFailureCode::PermissionDenied, false),
        ToolExecutionError::InvalidArguments(_) => (SourceFailureCode::NotFound, false),
        ToolExecutionError::Backend(_) | ToolExecutionError::Cancelled => (SourceFailureCode::SourceUnavailable, true),
        ToolExecutionError::OutputTooLarge { .. }
        | ToolExecutionError::ChangePlanningDisabled(_)
        | ToolExecutionError::Internal(_) => (SourceFailureCode::InvalidResponse, false),
    };
    QueryCompleteness {
        partial: true,
        warnings: vec!["source_failures_present".to_string()],
        source_failures: vec![SourceFailure::new(source, code, retryable, logical_target)],
    }
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

fn sorted_unique_topic_names(mut topics: Vec<String>) -> Vec<String> {
    topics.sort();
    topics.dedup();
    topics
}

fn normalized_identifier(field: &str, value: &str) -> Result<String, ToolExecutionError> {
    const MAX_IDENTIFIER_BYTES: usize = 255;
    let value = value.trim();
    if value.is_empty() {
        return Err(ToolExecutionError::InvalidArguments(format!(
            "{field} must not be empty"
        )));
    }
    if value.len() > MAX_IDENTIFIER_BYTES {
        return Err(ToolExecutionError::InvalidArguments(format!(
            "{field} must not exceed {MAX_IDENTIFIER_BYTES} bytes"
        )));
    }
    Ok(value.to_string())
}

fn normalized_logical_identifier(field: &str, value: &str) -> Result<String, ToolExecutionError> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > 100
        || value.parse::<std::net::IpAddr>().is_ok()
        || value.parse::<std::net::SocketAddr>().is_ok()
        || value.contains([':', '/', '\\', '@', '=', '&', '?'])
        || value.chars().any(char::is_control)
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'))
    {
        Err(ToolExecutionError::InvalidArguments(format!(
            "{field} must be a logical identifier of at most 100 bytes"
        )))
    } else {
        Ok(value.to_string())
    }
}

fn normalized_broker_names(mut broker_names: Vec<String>) -> Result<Vec<String>, ToolExecutionError> {
    const MAX_BROKER_NAMES: usize = 64;
    if broker_names.is_empty() || broker_names.len() > MAX_BROKER_NAMES {
        return Err(ToolExecutionError::InvalidArguments(format!(
            "broker_names must contain between 1 and {MAX_BROKER_NAMES} logical Brokers"
        )));
    }
    broker_names = broker_names
        .into_iter()
        .map(|broker_name| normalized_logical_identifier("broker_names", &broker_name))
        .collect::<Result<Vec<_>, _>>()?;
    broker_names.sort();
    broker_names.dedup();
    Ok(broker_names)
}

fn project_connections(
    observations: QueryPayload<SessionConnections>,
    aliases: &IdentifierAliaser,
) -> Result<QueryPayload<ConnectionSnapshot>, ToolExecutionError> {
    let mut completeness = observations.completeness();
    if observations.data.truncated {
        completeness.partial = true;
        completeness.warnings.push("connection_rows_truncated".to_string());
    }
    let queried_broker_count = observations.data.queried_broker_count;
    let mut rows = observations
        .data
        .rows
        .into_iter()
        .map(|row| {
            Ok(ConnectionRow {
                broker_name: row.broker_name,
                client_alias: aliases
                    .client_alias(&row.client_id, &row.client_addr)
                    .map_err(alias_error)?,
                language: row.language,
                version: row.version,
                last_update_at: row.last_update_timestamp.and_then(observed_at_from_millis),
            })
        })
        .collect::<Result<Vec<_>, ToolExecutionError>>()?;
    rows.sort_by(|left, right| {
        left.broker_name
            .cmp(&right.broker_name)
            .then(left.client_alias.cmp(&right.client_alias))
            .then(left.language.cmp(&right.language))
            .then(left.version.cmp(&right.version))
            .then(left.last_update_at.cmp(&right.last_update_at))
    });
    rows.dedup();
    Ok(completeness.wrap(ConnectionSnapshot {
        rows,
        queried_broker_count,
    }))
}

fn alias_error(error: IdentifierAliasError) -> ToolExecutionError {
    ToolExecutionError::Internal(error.to_string())
}

fn normalized_broker_logger(logger: &str) -> Result<String, ToolExecutionError> {
    if logger != logger.trim()
        || logger.is_empty()
        || logger.len() > 128
        || !logger
            .strip_prefix("rocketmq_broker::")
            .is_some_and(valid_rust_module_path)
    {
        Err(ToolExecutionError::InvalidArguments(
            "logger must be an allowlisted rocketmq_broker:: target of at most 128 bytes".to_string(),
        ))
    } else {
        Ok(logger.to_string())
    }
}

fn valid_rust_module_path(path: &str) -> bool {
    !path.is_empty()
        && path.split("::").all(|segment| {
            let mut characters = segment.chars();
            characters
                .next()
                .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
                && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
        })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::Barrier;
    use tokio_util::sync::CancellationToken;

    use crate::adapter::admin_session::SessionConnectionRow;
    use crate::adapter::admin_session::SessionMessageMetadata;
    use crate::config::McpConfig;
    use crate::model::contract::CacheStatus;
    use crate::resources;
    use crate::tools::cluster_tools::BrokerSummary;
    use crate::tools::cluster_tools::ClusterOverviewArgs;
    use crate::tools::consumer_tools::ConsumerGroupSummary;
    use crate::tools::consumer_tools::QueueLag;
    use crate::tools::diagnosis_tools::DiagnoseConsumerLagArgs;
    use crate::tools::topic_tools::TopicRouteBroker;
    use crate::tools::topic_tools::TopicRouteQueue;

    use super::*;

    #[derive(Debug, Default)]
    struct LifecycleCounters {
        starts: AtomicUsize,
        shutdowns: AtomicUsize,
        broker_queries: AtomicUsize,
        broker_diagnostics_queries: AtomicUsize,
        broker_config_queries: AtomicUsize,
        broker_log_filter_queries: AtomicUsize,
        proxy_drain_queries: AtomicUsize,
        proxy_endpoint_mismatches: AtomicUsize,
        consumer_connection_queries: AtomicUsize,
        producer_connection_queries: AtomicUsize,
        message_metadata_queries: AtomicUsize,
        topic_config_state_queries: AtomicUsize,
        consumer_group_config_state_queries: AtomicUsize,
        topic_inventory_queries: AtomicUsize,
        consumer_group_queries: AtomicUsize,
        consumer_group_inventory_queries: AtomicUsize,
        consumer_group_enrichment_queries: AtomicUsize,
        consumer_group_enriched_targets: AtomicUsize,
        route_queries: AtomicUsize,
        consumer_lag_queries: AtomicUsize,
        runtime_probes: AtomicUsize,
    }

    #[derive(Debug)]
    struct TopicInventoryGate {
        armed: AtomicBool,
        entered: AtomicUsize,
        release: Barrier,
    }

    impl TopicInventoryGate {
        fn new(expected_loaders: usize) -> Self {
            Self {
                armed: AtomicBool::new(false),
                entered: AtomicUsize::new(0),
                release: Barrier::new(expected_loaders + 1),
            }
        }

        fn arm(&self) {
            self.entered.store(0, Ordering::SeqCst);
            self.armed.store(true, Ordering::SeqCst);
        }

        async fn wait_if_armed(&self) {
            if self.armed.load(Ordering::SeqCst) {
                self.entered.fetch_add(1, Ordering::SeqCst);
                self.release.wait().await;
            }
        }

        async fn release(&self) {
            self.armed.store(false, Ordering::SeqCst);
            self.release.wait().await;
        }
    }

    async fn wait_for_atomic_count(counter: &AtomicUsize, expected: usize) {
        for _ in 0..10_000 {
            if counter.load(Ordering::SeqCst) == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(counter.load(Ordering::SeqCst), expected);
    }

    async fn assert_new_tool_cancelled<T, Call, CallFuture, Count>(call: Call, count: Count)
    where
        T: Send + 'static,
        Call: FnOnce(QueryFacade<FakeSessionFactory>) -> CallFuture,
        CallFuture: Future<Output = Result<T, ToolExecutionError>> + Send + 'static,
        Count: Fn(&LifecycleCounters) -> usize,
    {
        let factory = FakeSessionFactory {
            hang_broker_query: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let cancellation = CancellationToken::new();
        let facade = QueryFacade::with_factory_and_control(
            example_config(),
            factory,
            WorkflowControl::new(Duration::from_secs(30), cancellation.clone()),
        );
        let task = tokio::spawn(call(facade));
        for _ in 0..10_000 {
            if count(&counters) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(count(&counters), 1);
        cancellation.cancel();
        assert!(matches!(task.await.unwrap(), Err(ToolExecutionError::Cancelled)));
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    }

    #[derive(Clone, Default)]
    struct FakeSessionFactory {
        counters: Arc<LifecycleCounters>,
        selected_broker_missing: bool,
        failed_selected_broker: bool,
        overflow_failed_selected_broker: bool,
        partial_sources: bool,
        fail_exact_read: bool,
        hang_broker_query: bool,
        hang_topic_inventory: bool,
        topic_inventory_gate: Option<Arc<TopicInventoryGate>>,
        fail_topic_inventory: bool,
        many_groups: bool,
        case_colliding_groups: bool,
        empty_groups: bool,
        mutating_group_inventory: bool,
        many_route_rows: bool,
        many_lag_rows: bool,
        fail_group_enrichment: bool,
        truncated_connections: bool,
        yield_snapshot_queries: bool,
    }

    impl AdminSessionFactory for FakeSessionFactory {
        type Session = FakeSession;

        async fn start(&self, cluster: ResolvedCluster) -> Result<Self::Session, ToolExecutionError> {
            self.counters.starts.fetch_add(1, Ordering::SeqCst);
            Ok(FakeSession {
                cluster,
                counters: self.counters.clone(),
                selected_broker_missing: self.selected_broker_missing,
                failed_selected_broker: self.failed_selected_broker,
                overflow_failed_selected_broker: self.overflow_failed_selected_broker,
                partial_sources: self.partial_sources,
                fail_exact_read: self.fail_exact_read,
                hang_broker_query: self.hang_broker_query,
                hang_topic_inventory: self.hang_topic_inventory,
                topic_inventory_gate: self.topic_inventory_gate.clone(),
                fail_topic_inventory: self.fail_topic_inventory,
                many_groups: self.many_groups,
                case_colliding_groups: self.case_colliding_groups,
                empty_groups: self.empty_groups,
                mutating_group_inventory: self.mutating_group_inventory,
                many_route_rows: self.many_route_rows,
                many_lag_rows: self.many_lag_rows,
                fail_group_enrichment: self.fail_group_enrichment,
                truncated_connections: self.truncated_connections,
                yield_snapshot_queries: self.yield_snapshot_queries,
            })
        }
    }

    struct FakeSession {
        cluster: ResolvedCluster,
        counters: Arc<LifecycleCounters>,
        selected_broker_missing: bool,
        failed_selected_broker: bool,
        overflow_failed_selected_broker: bool,
        partial_sources: bool,
        fail_exact_read: bool,
        hang_broker_query: bool,
        hang_topic_inventory: bool,
        topic_inventory_gate: Option<Arc<TopicInventoryGate>>,
        fail_topic_inventory: bool,
        many_groups: bool,
        case_colliding_groups: bool,
        empty_groups: bool,
        mutating_group_inventory: bool,
        many_route_rows: bool,
        many_lag_rows: bool,
        fail_group_enrichment: bool,
        truncated_connections: bool,
        yield_snapshot_queries: bool,
    }

    impl AdminSession for FakeSession {
        async fn broker_rows(&mut self) -> Result<QueryPayload<Vec<BrokerSummary>>, ToolExecutionError> {
            self.counters.broker_queries.fetch_add(1, Ordering::SeqCst);
            if self.hang_broker_query {
                std::future::pending::<()>().await;
            }
            if self.failed_selected_broker {
                return Ok(partial_payload(Vec::new(), QuerySource::BrokerRuntime, "broker-a"));
            }
            if self.overflow_failed_selected_broker {
                let failures = (0..crate::model::contract::MAX_SOURCE_FAILURES)
                    .map(|index| {
                        SourceFailure::new(
                            QuerySource::BrokerRuntime,
                            SourceFailureCode::SourceUnavailable,
                            true,
                            format!("broker-{index:02}"),
                        )
                    })
                    .chain(std::iter::once(SourceFailure::new(
                        QuerySource::BrokerRuntime,
                        SourceFailureCode::SourceUnavailable,
                        true,
                        "broker-z",
                    )))
                    .collect();
                return Ok(QueryPayload::new(Vec::new(), true, Vec::new(), failures));
            }
            let broker_name = if self.selected_broker_missing {
                "broker-b"
            } else {
                "broker-a"
            };
            let brokers = vec![broker_summary(&self.cluster.name, broker_name)];
            Ok(if self.partial_sources {
                partial_payload(brokers, QuerySource::BrokerRuntime, "broker-b")
            } else {
                QueryPayload::complete(brokers)
            })
        }

        async fn topic_inventory(&mut self) -> Result<Vec<String>, ToolExecutionError> {
            self.counters.topic_inventory_queries.fetch_add(1, Ordering::SeqCst);
            if self.hang_topic_inventory {
                std::future::pending::<()>().await;
            }
            if let Some(gate) = &self.topic_inventory_gate {
                gate.wait_if_armed().await;
            }
            if self.fail_topic_inventory {
                return Err(ToolExecutionError::backend("topic query failed"));
            }
            Ok(vec!["payments".to_string(), "orders".to_string(), "orders".to_string()])
        }

        async fn topic_route(&mut self, _topic: &str) -> Result<SessionTopicRoute, ToolExecutionError> {
            self.counters.route_queries.fetch_add(1, Ordering::SeqCst);
            if self.yield_snapshot_queries {
                tokio::task::yield_now().await;
            }
            let queues = if self.many_route_rows {
                (0..5).map(|index| route_queue(&format!("broker-{index}"))).collect()
            } else {
                vec![route_queue("broker-a")]
            };
            Ok(SessionTopicRoute {
                brokers: vec![route_broker(&self.cluster.name, "broker-a")],
                queues,
            })
        }

        async fn consumer_groups(&mut self) -> Result<QueryPayload<Vec<ConsumerGroupSummary>>, ToolExecutionError> {
            self.counters.consumer_group_queries.fetch_add(1, Ordering::SeqCst);
            let groups = vec![consumer_group("order-service")];
            Ok(if self.partial_sources {
                partial_payload(groups, QuerySource::ConsumerConnection, "broker-b")
            } else {
                QueryPayload::complete(groups)
            })
        }

        async fn consumer_group_inventory(&mut self) -> Result<QueryPayload<Vec<String>>, ToolExecutionError> {
            let query_index = self
                .counters
                .consumer_group_inventory_queries
                .fetch_add(1, Ordering::SeqCst);
            if self.yield_snapshot_queries {
                tokio::task::yield_now().await;
            }
            let groups = if self.empty_groups {
                Vec::new()
            } else if self.case_colliding_groups {
                vec!["OrderGroup".to_string(), "ordergroup".to_string()]
            } else if self.mutating_group_inventory && query_index > 0 {
                vec![
                    "group-inserted".to_string(),
                    "group-4".to_string(),
                    "group-0".to_string(),
                ]
            } else if self.many_groups || self.mutating_group_inventory {
                (0..5).map(|index| format!("group-{index}")).collect()
            } else {
                vec!["order-service".to_string()]
            };
            Ok(QueryPayload::complete(groups))
        }

        async fn consumer_groups_exact(
            &mut self,
            groups: &[String],
        ) -> Result<QueryPayload<Vec<ConsumerGroupSummary>>, ToolExecutionError> {
            self.counters
                .consumer_group_enrichment_queries
                .fetch_add(1, Ordering::SeqCst);
            self.counters
                .consumer_group_enriched_targets
                .fetch_add(groups.len(), Ordering::SeqCst);
            if self.yield_snapshot_queries {
                tokio::task::yield_now().await;
            }
            if self.fail_group_enrichment {
                return Err(ToolExecutionError::backend("all group enrichment sources failed"));
            }
            let groups = groups.iter().map(|group| consumer_group(group)).collect::<Vec<_>>();
            Ok(if self.partial_sources {
                partial_payload(groups, QuerySource::ConsumerConnection, "broker-b")
            } else {
                QueryPayload::complete(groups)
            })
        }

        async fn consumer_lag(
            &mut self,
            _topic: &str,
            _consumer_group: &str,
        ) -> Result<QueryPayload<SessionConsumerLag>, ToolExecutionError> {
            self.counters.consumer_lag_queries.fetch_add(1, Ordering::SeqCst);
            if self.yield_snapshot_queries {
                tokio::task::yield_now().await;
            }
            let queues = if self.many_lag_rows {
                (0..5)
                    .map(|index| {
                        let mut row = queue_lag("broker-a");
                        row.queue_id = index;
                        row
                    })
                    .collect()
            } else {
                vec![queue_lag("broker-a")]
            };
            let lag = SessionConsumerLag {
                queues,
                total_lag: 10_000,
                consume_tps: 0.5,
                inflight_total: 5,
            };
            Ok(if self.partial_sources {
                partial_payload(lag, QuerySource::ConsumerStatistics, "broker-b")
            } else {
                QueryPayload::complete(lag)
            })
        }

        async fn probe_broker_runtime_target(
            &mut self,
            _broker_name: &str,
        ) -> Result<BrokerRuntimeTargetStatus, ToolExecutionError> {
            self.counters.runtime_probes.fetch_add(1, Ordering::SeqCst);
            if self.failed_selected_broker || self.overflow_failed_selected_broker {
                Ok(BrokerRuntimeTargetStatus::SourceUnavailable)
            } else if self.selected_broker_missing {
                Ok(BrokerRuntimeTargetStatus::NotFound)
            } else {
                Ok(BrokerRuntimeTargetStatus::Available)
            }
        }

        async fn broker_diagnostics(
            &mut self,
            broker_name: &str,
        ) -> Result<QueryPayload<BrokerDiagnosticsOutput>, ToolExecutionError> {
            self.counters.broker_diagnostics_queries.fetch_add(1, Ordering::SeqCst);
            if self.hang_broker_query {
                std::future::pending::<()>().await;
            }
            if self.fail_exact_read {
                return Err(ToolExecutionError::backend("exact read failed"));
            }
            let output = BrokerDiagnosticsOutput {
                cluster: self.cluster.name.clone(),
                broker_name: broker_name.to_string(),
                diagnostics_schema_version: "1".to_string(),
                observed_at_millis: 1,
                brokers: Vec::new(),
                unavailable_brokers: usize::from(self.partial_sources),
            };
            Ok(if self.partial_sources {
                partial_payload(output, QuerySource::BrokerRuntime, broker_name)
            } else {
                QueryPayload::complete(output)
            })
        }

        async fn broker_config_summary(
            &mut self,
            broker_name: &str,
        ) -> Result<QueryPayload<BrokerConfigSummaryOutput>, ToolExecutionError> {
            self.counters.broker_config_queries.fetch_add(1, Ordering::SeqCst);
            if self.fail_exact_read {
                return Err(ToolExecutionError::backend("exact read failed"));
            }
            let output = BrokerConfigSummaryOutput {
                cluster: self.cluster.name.clone(),
                broker_name: broker_name.to_string(),
                brokers: Vec::new(),
            };
            Ok(if self.partial_sources {
                partial_payload(output, QuerySource::BrokerConfig, broker_name)
            } else {
                QueryPayload::complete(output)
            })
        }

        async fn broker_log_filter_state(
            &mut self,
            broker_name: &str,
            logger: &str,
        ) -> Result<QueryPayload<BrokerLogFilterStateOutput>, ToolExecutionError> {
            self.counters.broker_log_filter_queries.fetch_add(1, Ordering::SeqCst);
            if self.fail_exact_read {
                return Err(ToolExecutionError::backend("exact read failed"));
            }
            let output = BrokerLogFilterStateOutput {
                cluster: self.cluster.name.clone(),
                broker_name: broker_name.to_string(),
                logger: logger.to_string(),
                brokers: Vec::new(),
            };
            Ok(if self.partial_sources {
                partial_payload(output, QuerySource::BrokerLogFilter, broker_name)
            } else {
                QueryPayload::complete(output)
            })
        }

        async fn proxy_drain_state(
            &mut self,
            proxy_name: &str,
            proxy_endpoint: &str,
        ) -> Result<QueryPayload<ProxyDrainStateOutput>, ToolExecutionError> {
            self.counters.proxy_drain_queries.fetch_add(1, Ordering::SeqCst);
            if self.fail_exact_read {
                return Err(ToolExecutionError::backend("exact read failed"));
            }
            let expected_endpoint = if self.cluster.name == "secondary" {
                "proxy-secondary.internal:8081"
            } else {
                "127.0.0.1:8081"
            };
            if proxy_endpoint != expected_endpoint {
                self.counters.proxy_endpoint_mismatches.fetch_add(1, Ordering::SeqCst);
            }
            let output = ProxyDrainStateOutput {
                cluster: self.cluster.name.clone(),
                proxy_name: proxy_name.to_string(),
                state_schema_version: "1".to_string(),
                phase: crate::tools::proxy_tools::ProxyDrainPhase::Accepting,
                operation_id: None,
                admission_open: true,
                routing_open: true,
                readiness_published: true,
                zero_pending: true,
                pending: crate::tools::proxy_tools::ProxyDrainPending {
                    active_connections: 0,
                    sessions: 0,
                    receipt_handles: 0,
                    prepared_transactions: 0,
                    telemetry_links: 0,
                    remoting_channels: 0,
                    telemetry_commands: 0,
                    rpc_in_flight: 0,
                },
            };
            Ok(QueryPayload::complete(output))
        }

        async fn consumer_connections(
            &mut self,
            _consumer_group: &str,
        ) -> Result<QueryPayload<SessionConnections>, ToolExecutionError> {
            self.counters.consumer_connection_queries.fetch_add(1, Ordering::SeqCst);
            if self.hang_broker_query {
                std::future::pending::<()>().await;
            }
            if self.fail_exact_read {
                return Err(ToolExecutionError::backend("raw-client-secret backend failure"));
            }
            let result = SessionConnections {
                rows: connection_rows(),
                queried_broker_count: 2,
                truncated: self.truncated_connections,
            };
            Ok(if self.partial_sources {
                partial_payload(result, QuerySource::ConsumerConnection, "broker-b")
            } else {
                QueryPayload::complete(result)
            })
        }

        async fn producer_connections(
            &mut self,
            _topic: &str,
            _producer_group: &str,
        ) -> Result<QueryPayload<SessionConnections>, ToolExecutionError> {
            self.counters.producer_connection_queries.fetch_add(1, Ordering::SeqCst);
            if self.hang_broker_query {
                std::future::pending::<()>().await;
            }
            if self.fail_exact_read {
                return Err(ToolExecutionError::backend("raw-producer-secret backend failure"));
            }
            let result = SessionConnections {
                rows: connection_rows(),
                queried_broker_count: 2,
                truncated: self.truncated_connections,
            };
            Ok(if self.partial_sources {
                partial_payload(result, QuerySource::ProducerConnection, "broker-b")
            } else {
                QueryPayload::complete(result)
            })
        }

        async fn message_metadata(&mut self, _message_id: &str) -> Result<SessionMessageMetadata, ToolExecutionError> {
            self.counters.message_metadata_queries.fetch_add(1, Ordering::SeqCst);
            if self.hang_broker_query {
                std::future::pending::<()>().await;
            }
            if self.fail_exact_read {
                return Err(ToolExecutionError::backend("raw-message-secret backend failure"));
            }
            Ok(SessionMessageMetadata {
                message_id: "RAW-OFFSET-MESSAGE-ID".to_string(),
                unique_message_id: Some("RAW-UNIQUE-MESSAGE-ID".to_string()),
                topic: "orders".to_string(),
                born_timestamp: 1_000,
                store_timestamp: 2_000,
                queue_id: 1,
                queue_offset: 2,
                store_size: 3,
                reconsume_times: 4,
                sys_flag: 5,
                flag: 6,
                prepared_transaction_offset: 7,
            })
        }

        async fn topic_config_state(
            &mut self,
            topic: &str,
            _broker_names: &[String],
        ) -> Result<QueryPayload<TopicConfigStateOutput>, ToolExecutionError> {
            self.counters.topic_config_state_queries.fetch_add(1, Ordering::SeqCst);
            if self.hang_broker_query {
                std::future::pending::<()>().await;
            }
            if self.fail_exact_read {
                return Err(ToolExecutionError::backend("raw-address backend failure"));
            }
            let output = TopicConfigStateOutput {
                cluster: self.cluster.name.clone(),
                topic: topic.to_string(),
                brokers: vec![crate::tools::config_tools::TopicConfigStateRow {
                    broker_name: "broker-a".to_string(),
                    version: 9,
                    read_queue_nums: 8,
                    write_queue_nums: 8,
                    order: false,
                }],
            };
            Ok(if self.partial_sources {
                partial_payload(output, QuerySource::TopicConfig, "broker-b")
            } else {
                QueryPayload::complete(output)
            })
        }

        async fn consumer_group_config_state(
            &mut self,
            group: &str,
            _broker_names: &[String],
        ) -> Result<QueryPayload<ConsumerGroupConfigStateOutput>, ToolExecutionError> {
            self.counters
                .consumer_group_config_state_queries
                .fetch_add(1, Ordering::SeqCst);
            if self.hang_broker_query {
                std::future::pending::<()>().await;
            }
            if self.fail_exact_read {
                return Err(ToolExecutionError::backend("raw-address backend failure"));
            }
            let output = ConsumerGroupConfigStateOutput {
                cluster: self.cluster.name.clone(),
                group: group.to_string(),
                brokers: vec![crate::tools::config_tools::ConsumerGroupConfigStateRow {
                    broker_name: "broker-a".to_string(),
                    version: 10,
                    retry_max_times: 16,
                    retry_queue_nums: 1,
                    consume_timeout_minutes: 15,
                    consume_enable: true,
                    consume_from_min_enable: false,
                    consume_broadcast_enable: false,
                    consume_message_orderly: false,
                    broker_id: 0,
                    which_broker_when_consume_slowly: 1,
                    notify_consumer_ids_changed_enable: true,
                    group_sys_flag: 0,
                }],
            };
            Ok(if self.partial_sources {
                partial_payload(output, QuerySource::ConsumerGroupConfig, "broker-b")
            } else {
                QueryPayload::complete(output)
            })
        }

        async fn shutdown(self) -> Result<(), ToolExecutionError> {
            self.counters.shutdowns.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn connection_rows() -> Vec<SessionConnectionRow> {
        vec![
            SessionConnectionRow {
                broker_name: "broker-b".to_string(),
                client_id: "raw-client-b".to_string(),
                client_addr: "10.0.0.2:2000".to_string(),
                language: "JAVA".to_string(),
                version: 2,
                last_update_timestamp: Some(2_000),
            },
            SessionConnectionRow {
                broker_name: "broker-a".to_string(),
                client_id: "raw-client-a".to_string(),
                client_addr: "10.0.0.1:1000".to_string(),
                language: "RUST".to_string(),
                version: 1,
                last_update_timestamp: Some(1_000),
            },
        ]
    }

    fn partial_payload<T>(data: T, source: QuerySource, logical_target: &str) -> QueryPayload<T> {
        QueryPayload::new(
            data,
            true,
            Vec::new(),
            vec![SourceFailure::new(
                source,
                SourceFailureCode::SourceUnavailable,
                true,
                logical_target,
            )],
        )
    }

    #[tokio::test]
    async fn new_read_tools_cache_safe_projections_and_single_snapshot_pages() {
        let factory = FakeSessionFactory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);

        let consumer_args = |cursor| ListConsumerConnectionsArgs {
            cluster: "local-dev".to_string(),
            consumer_group: "group-a".to_string(),
            page: PageRequest { limit: Some(1), cursor },
        };
        let first_consumer = facade.list_consumer_connections(consumer_args(None)).await.unwrap();
        let first_alias = first_consumer.data.page.items[0].client_alias.clone();
        let second_consumer = facade
            .list_consumer_connections(consumer_args(first_consumer.data.page.next_cursor.clone()))
            .await
            .unwrap();
        let replay_consumer = facade.list_consumer_connections(consumer_args(None)).await.unwrap();
        assert_ne!(first_alias, second_consumer.data.page.items[0].client_alias);
        assert_eq!(first_alias, replay_consumer.data.page.items[0].client_alias);
        assert_eq!(counters.consumer_connection_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
        let consumer_json = serde_json::to_string(&first_consumer.data).unwrap();
        assert!(!consumer_json.contains("raw-client"));
        assert!(!consumer_json.contains("10.0.0."));

        let producer_args = |cursor| ListProducerConnectionsArgs {
            cluster: "local-dev".to_string(),
            topic: "orders".to_string(),
            producer_group: "producer-a".to_string(),
            page: PageRequest { limit: Some(1), cursor },
        };
        let first_producer = facade.list_producer_connections(producer_args(None)).await.unwrap();
        facade
            .list_producer_connections(producer_args(first_producer.data.page.next_cursor.clone()))
            .await
            .unwrap();
        assert_eq!(counters.producer_connection_queries.load(Ordering::SeqCst), 1);
        assert!(!serde_json::to_string(&first_producer.data).unwrap().contains("10.0.0."));

        let raw_lookup = "RAW-LOOKUP-MESSAGE-ID";
        let message_args = || MessageMetadataArgs {
            cluster: "local-dev".to_string(),
            message_id: raw_lookup.to_string(),
        };
        let message = facade.message_metadata(message_args()).await.unwrap();
        let message_replay = facade.message_metadata(message_args()).await.unwrap();
        assert_eq!(message.cache_status, CacheStatus::Miss);
        assert_eq!(message_replay.cache_status, CacheStatus::Hit);
        assert_eq!(message.data.message_alias, message_replay.data.message_alias);
        let message_json = serde_json::to_string(&message.data).unwrap();
        for raw in [raw_lookup, "RAW-OFFSET-MESSAGE-ID", "RAW-UNIQUE-MESSAGE-ID"] {
            assert!(!message_json.contains(raw));
        }
        for forbidden in ["body", "properties", "born_host", "store_host", "endpoint"] {
            assert!(!message_json.contains(forbidden));
        }
        assert_eq!(counters.message_metadata_queries.load(Ordering::SeqCst), 1);

        let topic_state = facade
            .topic_config_state(TopicConfigStateArgs {
                cluster: "local-dev".to_string(),
                topic: "orders".to_string(),
                broker_names: vec!["broker-b".to_string(), "broker-a".to_string(), "broker-b".to_string()],
            })
            .await
            .unwrap();
        let topic_state_replay = facade
            .topic_config_state(TopicConfigStateArgs {
                cluster: "local-dev".to_string(),
                topic: "orders".to_string(),
                broker_names: vec!["broker-a".to_string(), "broker-b".to_string()],
            })
            .await
            .unwrap();
        assert_eq!(topic_state.data.brokers[0].version, 9);
        assert_eq!(topic_state_replay.cache_status, CacheStatus::Hit);
        assert_eq!(counters.topic_config_state_queries.load(Ordering::SeqCst), 1);

        let group_state = facade
            .consumer_group_config_state(ConsumerGroupConfigStateArgs {
                cluster: "local-dev".to_string(),
                group: "group-a".to_string(),
                broker_names: vec!["broker-b".to_string(), "broker-a".to_string(), "broker-a".to_string()],
            })
            .await
            .unwrap();
        assert_eq!(group_state.data.brokers[0].retry_max_times, 16);
        assert_eq!(counters.consumer_group_config_state_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 5);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn connection_cursors_bind_exact_selector_limit_and_visibility_without_requery() {
        let factory = FakeSessionFactory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let first = facade
            .list_producer_connections(ListProducerConnectionsArgs {
                cluster: "local-dev".to_string(),
                topic: "orders".to_string(),
                producer_group: "producer-a".to_string(),
                page: PageRequest {
                    limit: Some(1),
                    cursor: None,
                },
            })
            .await
            .unwrap();
        let cursor = first.data.page.next_cursor.unwrap();
        for (topic, producer_group, limit) in [
            ("payments", "producer-a", 1),
            ("orders", "producer-b", 1),
            ("orders", "producer-a", 2),
        ] {
            let result = facade
                .list_producer_connections(ListProducerConnectionsArgs {
                    cluster: "local-dev".to_string(),
                    topic: topic.to_string(),
                    producer_group: producer_group.to_string(),
                    page: PageRequest {
                        limit: Some(limit),
                        cursor: Some(cursor.clone()),
                    },
                })
                .await;
            assert!(matches!(result, Err(ToolExecutionError::InvalidArguments(_))));
        }
        let sensitive = facade.with_visibility_class(VisibilityClass::Sensitive);
        let cross_visibility = sensitive
            .list_producer_connections(ListProducerConnectionsArgs {
                cluster: "local-dev".to_string(),
                topic: "orders".to_string(),
                producer_group: "producer-a".to_string(),
                page: PageRequest {
                    limit: Some(1),
                    cursor: Some(cursor),
                },
            })
            .await;
        assert!(matches!(cross_visibility, Err(ToolExecutionError::InvalidArguments(_))));
        assert_eq!(counters.producer_connection_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn new_read_tools_preserve_partial_truncated_and_cache_evidence() {
        let factory = FakeSessionFactory {
            partial_sources: true,
            truncated_connections: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let consumer = facade
            .list_consumer_connections(ListConsumerConnectionsArgs {
                cluster: "local-dev".to_string(),
                consumer_group: "group-a".to_string(),
                page: PageRequest::default(),
            })
            .await
            .unwrap();
        assert!(consumer.partial);
        assert!(consumer.warnings.contains(&"connection_rows_truncated".to_string()));
        assert_eq!(consumer.source_failures[0].source, QuerySource::ConsumerConnection);

        let producer_args = || ListProducerConnectionsArgs {
            cluster: "local-dev".to_string(),
            topic: "orders".to_string(),
            producer_group: "producer-a".to_string(),
            page: PageRequest::default(),
        };
        let producer = facade.list_producer_connections(producer_args()).await.unwrap();
        let producer_replay = facade.list_producer_connections(producer_args()).await.unwrap();
        assert!(producer.partial);
        assert_eq!(producer.warnings, producer_replay.warnings);
        assert_eq!(producer.source_failures, producer_replay.source_failures);

        let topic = facade
            .topic_config_state(TopicConfigStateArgs {
                cluster: "local-dev".to_string(),
                topic: "orders".to_string(),
                broker_names: vec!["broker-a".to_string(), "broker-b".to_string()],
            })
            .await
            .unwrap();
        assert!(topic.partial);
        assert_eq!(topic.source_failures[0].source, QuerySource::TopicConfig);
        let group = facade
            .consumer_group_config_state(ConsumerGroupConfigStateArgs {
                cluster: "local-dev".to_string(),
                group: "group-a".to_string(),
                broker_names: vec!["broker-a".to_string(), "broker-b".to_string()],
            })
            .await
            .unwrap();
        assert!(group.partial);
        assert_eq!(group.source_failures[0].source, QuerySource::ConsumerGroupConfig);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn every_new_read_tool_total_backend_failure_shuts_down() {
        let factory = FakeSessionFactory {
            fail_exact_read: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let failures = [
            facade
                .list_consumer_connections(ListConsumerConnectionsArgs {
                    cluster: "local-dev".to_string(),
                    consumer_group: "group-a".to_string(),
                    page: PageRequest::default(),
                })
                .await
                .map(|_| ()),
            facade
                .list_producer_connections(ListProducerConnectionsArgs {
                    cluster: "local-dev".to_string(),
                    topic: "orders".to_string(),
                    producer_group: "producer-a".to_string(),
                    page: PageRequest::default(),
                })
                .await
                .map(|_| ()),
            facade
                .message_metadata(MessageMetadataArgs {
                    cluster: "local-dev".to_string(),
                    message_id: "RAW-MESSAGE-ID".to_string(),
                })
                .await
                .map(|_| ()),
            facade
                .topic_config_state(TopicConfigStateArgs {
                    cluster: "local-dev".to_string(),
                    topic: "orders".to_string(),
                    broker_names: vec!["broker-a".to_string()],
                })
                .await
                .map(|_| ()),
            facade
                .consumer_group_config_state(ConsumerGroupConfigStateArgs {
                    cluster: "local-dev".to_string(),
                    group: "group-a".to_string(),
                    broker_names: vec!["broker-a".to_string()],
                })
                .await
                .map(|_| ()),
        ];
        assert!(failures
            .iter()
            .all(|result| matches!(result, Err(ToolExecutionError::Backend(_)))));
        assert_eq!(counters.starts.load(Ordering::SeqCst), 5);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn every_new_read_tool_timeout_shuts_down_without_sleep() {
        let factory = FakeSessionFactory {
            hang_broker_query: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory_and_control(
            example_config(),
            factory,
            WorkflowControl::new(Duration::from_millis(10), CancellationToken::new()),
        );
        let failures = [
            facade
                .list_consumer_connections(ListConsumerConnectionsArgs {
                    cluster: "local-dev".to_string(),
                    consumer_group: "group-a".to_string(),
                    page: PageRequest::default(),
                })
                .await
                .map(|_| ()),
            facade
                .list_producer_connections(ListProducerConnectionsArgs {
                    cluster: "local-dev".to_string(),
                    topic: "orders".to_string(),
                    producer_group: "producer-a".to_string(),
                    page: PageRequest::default(),
                })
                .await
                .map(|_| ()),
            facade
                .message_metadata(MessageMetadataArgs {
                    cluster: "local-dev".to_string(),
                    message_id: "RAW-MESSAGE-ID".to_string(),
                })
                .await
                .map(|_| ()),
            facade
                .topic_config_state(TopicConfigStateArgs {
                    cluster: "local-dev".to_string(),
                    topic: "orders".to_string(),
                    broker_names: vec!["broker-a".to_string()],
                })
                .await
                .map(|_| ()),
            facade
                .consumer_group_config_state(ConsumerGroupConfigStateArgs {
                    cluster: "local-dev".to_string(),
                    group: "group-a".to_string(),
                    broker_names: vec!["broker-a".to_string()],
                })
                .await
                .map(|_| ()),
        ];
        assert!(failures
            .iter()
            .all(|result| matches!(result, Err(ToolExecutionError::TimedOut { timeout_ms: 10 }))));
        assert_eq!(counters.starts.load(Ordering::SeqCst), 5);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn every_new_read_tool_cancellation_shuts_down_without_sleep() {
        assert_new_tool_cancelled(
            |facade| async move {
                facade
                    .list_consumer_connections(ListConsumerConnectionsArgs {
                        cluster: "local-dev".to_string(),
                        consumer_group: "group-a".to_string(),
                        page: PageRequest::default(),
                    })
                    .await
            },
            |counters| counters.consumer_connection_queries.load(Ordering::SeqCst),
        )
        .await;
        assert_new_tool_cancelled(
            |facade| async move {
                facade
                    .list_producer_connections(ListProducerConnectionsArgs {
                        cluster: "local-dev".to_string(),
                        topic: "orders".to_string(),
                        producer_group: "producer-a".to_string(),
                        page: PageRequest::default(),
                    })
                    .await
            },
            |counters| counters.producer_connection_queries.load(Ordering::SeqCst),
        )
        .await;
        assert_new_tool_cancelled(
            |facade| async move {
                facade
                    .message_metadata(MessageMetadataArgs {
                        cluster: "local-dev".to_string(),
                        message_id: "RAW-MESSAGE-ID".to_string(),
                    })
                    .await
            },
            |counters| counters.message_metadata_queries.load(Ordering::SeqCst),
        )
        .await;
        assert_new_tool_cancelled(
            |facade| async move {
                facade
                    .topic_config_state(TopicConfigStateArgs {
                        cluster: "local-dev".to_string(),
                        topic: "orders".to_string(),
                        broker_names: vec!["broker-a".to_string()],
                    })
                    .await
            },
            |counters| counters.topic_config_state_queries.load(Ordering::SeqCst),
        )
        .await;
        assert_new_tool_cancelled(
            |facade| async move {
                facade
                    .consumer_group_config_state(ConsumerGroupConfigStateArgs {
                        cluster: "local-dev".to_string(),
                        group: "group-a".to_string(),
                        broker_names: vec!["broker-a".to_string()],
                    })
                    .await
            },
            |counters| counters.consumer_group_config_state_queries.load(Ordering::SeqCst),
        )
        .await;
    }

    #[test]
    fn read_only_query_futures_preserve_the_send_contract() {
        fn assert_send<T: Send>(_: T) {}

        fn assert_query_futures_are_send<Q: ReadOnlyQuery>(
            query: &Q,
            cluster_overview: ClusterOverviewArgs,
            list_topics: ListTopicsArgs,
            describe_topic: DescribeTopicArgs,
            topic_route: QueryTopicRouteArgs,
            consumer_groups: ListConsumerGroupsArgs,
            consumer_lag: QueryConsumerLagArgs,
            broker: DescribeBrokerArgs,
            exact_reads: (
                BrokerDiagnosticsArgs,
                BrokerConfigSummaryArgs,
                BrokerLogFilterStateArgs,
                ProxyDrainStateArgs,
            ),
            diagnosis: DiagnoseConsumerLagArgs,
        ) {
            let (broker_diagnostics, broker_config, broker_log_filter, proxy_drain) = exact_reads;
            assert_send(query.cluster_overview(cluster_overview));
            assert_send(query.list_topics(list_topics));
            assert_send(query.describe_topic(describe_topic));
            assert_send(query.query_topic_route(topic_route));
            assert_send(query.list_consumer_groups(consumer_groups));
            assert_send(query.query_consumer_lag(consumer_lag));
            assert_send(query.describe_broker(broker));
            assert_send(query.broker_diagnostics(broker_diagnostics));
            assert_send(query.broker_config_summary(broker_config));
            assert_send(query.broker_log_filter_state(broker_log_filter));
            assert_send(query.proxy_drain_state(proxy_drain));
            assert_send(query.diagnose_consumer_lag(diagnosis));
        }

        let facade = QueryFacade::with_factory(example_config(), FakeSessionFactory::default());
        let page = PageRequest::default();
        assert_query_futures_are_send(
            &facade,
            ClusterOverviewArgs {
                cluster: "local-dev".to_string(),
            },
            ListTopicsArgs::default(),
            DescribeTopicArgs {
                cluster: "local-dev".to_string(),
                topic: "orders".to_string(),
                page: page.clone(),
            },
            QueryTopicRouteArgs {
                cluster: "local-dev".to_string(),
                topic: "orders".to_string(),
                page: page.clone(),
            },
            ListConsumerGroupsArgs::default(),
            QueryConsumerLagArgs {
                cluster: "local-dev".to_string(),
                topic: "orders".to_string(),
                consumer_group: "order-service".to_string(),
                page,
            },
            DescribeBrokerArgs {
                cluster: "local-dev".to_string(),
                broker_name: "broker-a".to_string(),
            },
            (
                BrokerDiagnosticsArgs {
                    cluster: "local-dev".to_string(),
                    broker_name: "broker-a".to_string(),
                },
                BrokerConfigSummaryArgs {
                    cluster: "local-dev".to_string(),
                    broker_name: "broker-a".to_string(),
                },
                BrokerLogFilterStateArgs {
                    cluster: "local-dev".to_string(),
                    broker_name: "broker-a".to_string(),
                    logger: "rocketmq_broker::processor".to_string(),
                },
                ProxyDrainStateArgs {
                    cluster: "local-dev".to_string(),
                    proxy_name: "proxy-local".to_string(),
                },
            ),
            DiagnoseConsumerLagArgs {
                cluster: "local-dev".to_string(),
                topic: "orders".to_string(),
                consumer_group: "order-service".to_string(),
            },
        );
    }

    #[tokio::test]
    async fn exact_read_tools_cache_complete_and_partial_evidence_without_requerying() {
        let factory = FakeSessionFactory {
            partial_sources: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);

        let diagnostics = || BrokerDiagnosticsArgs {
            cluster: "local-dev".to_string(),
            broker_name: "broker-a".to_string(),
        };
        let first = facade.broker_diagnostics(diagnostics()).await.unwrap();
        let replay = facade.broker_diagnostics(diagnostics()).await.unwrap();
        assert_eq!(first.cache_status, CacheStatus::Miss);
        assert_eq!(replay.cache_status, CacheStatus::Hit);
        assert_eq!(first.partial, replay.partial);
        assert_eq!(first.warnings, replay.warnings);
        assert_eq!(first.source_failures, replay.source_failures);

        let config = || BrokerConfigSummaryArgs {
            cluster: "local-dev".to_string(),
            broker_name: "broker-a".to_string(),
        };
        let first = facade.broker_config_summary(config()).await.unwrap();
        let replay = facade.broker_config_summary(config()).await.unwrap();
        assert_eq!(first.cache_status, CacheStatus::Miss);
        assert_eq!(replay.cache_status, CacheStatus::Hit);
        assert_eq!(first.partial, replay.partial);
        assert_eq!(first.warnings, replay.warnings);
        assert_eq!(first.source_failures, replay.source_failures);

        let log_filter = || BrokerLogFilterStateArgs {
            cluster: "local-dev".to_string(),
            broker_name: "broker-a".to_string(),
            logger: "rocketmq_broker::processor".to_string(),
        };
        let first = facade.broker_log_filter_state(log_filter()).await.unwrap();
        let replay = facade.broker_log_filter_state(log_filter()).await.unwrap();
        assert_eq!(first.cache_status, CacheStatus::Miss);
        assert_eq!(replay.cache_status, CacheStatus::Hit);
        assert_eq!(first.partial, replay.partial);
        assert_eq!(first.warnings, replay.warnings);
        assert_eq!(first.source_failures, replay.source_failures);

        let proxy = || ProxyDrainStateArgs {
            cluster: "local-dev".to_string(),
            proxy_name: "proxy-local".to_string(),
        };
        let first = facade.proxy_drain_state(proxy()).await.unwrap();
        let replay = facade.proxy_drain_state(proxy()).await.unwrap();
        assert_eq!(first.cache_status, CacheStatus::Miss);
        assert_eq!(replay.cache_status, CacheStatus::Hit);
        assert!(!first.partial);
        assert!(first.source_failures.is_empty());

        assert_eq!(counters.broker_diagnostics_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.broker_config_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.broker_log_filter_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.proxy_drain_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 4);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn exact_read_tool_cache_isolates_visibility_classes() {
        let factory = FakeSessionFactory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let standard = facade.clone().with_visibility_class(VisibilityClass::Standard);
        let sensitive = facade.with_visibility_class(VisibilityClass::Sensitive);
        let args = || BrokerDiagnosticsArgs {
            cluster: "local-dev".to_string(),
            broker_name: "broker-a".to_string(),
        };

        assert_eq!(
            standard.broker_diagnostics(args()).await.unwrap().cache_status,
            CacheStatus::Miss
        );
        assert_eq!(
            sensitive.broker_diagnostics(args()).await.unwrap().cache_status,
            CacheStatus::Miss
        );
        assert_eq!(counters.broker_diagnostics_queries.load(Ordering::SeqCst), 2);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn exact_read_tool_cancellation_shuts_down_the_started_session() {
        let factory = FakeSessionFactory {
            hang_broker_query: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let cancellation = CancellationToken::new();
        let control = WorkflowControl::new(Duration::from_secs(30), cancellation.clone());
        let facade = QueryFacade::with_factory_and_control(example_config(), factory, control);
        let task = tokio::spawn(async move {
            facade
                .broker_diagnostics(BrokerDiagnosticsArgs {
                    cluster: "local-dev".to_string(),
                    broker_name: "broker-a".to_string(),
                })
                .await
        });
        wait_for_atomic_count(&counters.broker_diagnostics_queries, 1).await;
        cancellation.cancel();

        assert!(matches!(task.await.unwrap(), Err(ToolExecutionError::Cancelled)));
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn exact_read_tool_timeout_shuts_down_the_started_session() {
        let factory = FakeSessionFactory {
            hang_broker_query: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let control = WorkflowControl::new(Duration::from_millis(10), CancellationToken::new());
        let facade = QueryFacade::with_factory_and_control(example_config(), factory, control);
        let result = facade
            .broker_diagnostics(BrokerDiagnosticsArgs {
                cluster: "local-dev".to_string(),
                broker_name: "broker-a".to_string(),
            })
            .await;

        assert!(matches!(result, Err(ToolExecutionError::TimedOut { timeout_ms: 10 })));
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn exact_read_backend_failures_shutdown_every_started_session() {
        let factory = FakeSessionFactory {
            fail_exact_read: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);

        assert!(matches!(
            facade
                .broker_diagnostics(BrokerDiagnosticsArgs {
                    cluster: "local-dev".to_string(),
                    broker_name: "broker-a".to_string(),
                })
                .await,
            Err(ToolExecutionError::Backend(_))
        ));
        assert!(matches!(
            facade
                .broker_config_summary(BrokerConfigSummaryArgs {
                    cluster: "local-dev".to_string(),
                    broker_name: "broker-a".to_string(),
                })
                .await,
            Err(ToolExecutionError::Backend(_))
        ));
        assert!(matches!(
            facade
                .broker_log_filter_state(BrokerLogFilterStateArgs {
                    cluster: "local-dev".to_string(),
                    broker_name: "broker-a".to_string(),
                    logger: "rocketmq_broker::processor".to_string(),
                })
                .await,
            Err(ToolExecutionError::Backend(_))
        ));
        assert!(matches!(
            facade
                .proxy_drain_state(ProxyDrainStateArgs {
                    cluster: "local-dev".to_string(),
                    proxy_name: "proxy-local".to_string(),
                })
                .await,
            Err(ToolExecutionError::Backend(_))
        ));
        assert_eq!(counters.starts.load(Ordering::SeqCst), 4);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn exact_read_tools_require_explicit_cluster_and_strict_logger() {
        let factory = FakeSessionFactory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);

        assert!(matches!(
            facade
                .broker_diagnostics(BrokerDiagnosticsArgs {
                    cluster: "   ".to_string(),
                    broker_name: "broker-a".to_string(),
                })
                .await,
            Err(ToolExecutionError::InvalidArguments(_))
        ));
        assert!(matches!(
            facade
                .broker_config_summary(BrokerConfigSummaryArgs {
                    cluster: "".to_string(),
                    broker_name: "broker-a".to_string(),
                })
                .await,
            Err(ToolExecutionError::InvalidArguments(_))
        ));
        assert!(matches!(
            facade
                .broker_log_filter_state(BrokerLogFilterStateArgs {
                    cluster: "local-dev".to_string(),
                    broker_name: "broker-a".to_string(),
                    logger: "rocketmq_broker::processor target".to_string(),
                })
                .await,
            Err(ToolExecutionError::InvalidArguments(_))
        ));
        assert!(matches!(
            facade
                .proxy_drain_state(ProxyDrainStateArgs {
                    cluster: "\t".to_string(),
                    proxy_name: "proxy-local".to_string(),
                })
                .await,
            Err(ToolExecutionError::InvalidArguments(_))
        ));
        let endpoint_cluster = "10.0.0.8:9876";
        let error = facade
            .broker_diagnostics(BrokerDiagnosticsArgs {
                cluster: endpoint_cluster.to_string(),
                broker_name: "broker-a".to_string(),
            })
            .await
            .unwrap_err();
        assert!(matches!(error, ToolExecutionError::InvalidArguments(_)));
        assert!(!error.to_string().contains(endpoint_cluster));
        assert_eq!(counters.starts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn proxy_alias_resolution_is_cluster_local_and_never_uses_an_unknown_alias() {
        let factory = FakeSessionFactory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config_with_secondary_proxy(), factory);

        let local = facade
            .proxy_drain_state(ProxyDrainStateArgs {
                cluster: "local-dev".to_string(),
                proxy_name: "proxy-local".to_string(),
            })
            .await
            .unwrap();
        let secondary = facade
            .proxy_drain_state(ProxyDrainStateArgs {
                cluster: "secondary".to_string(),
                proxy_name: "proxy-local".to_string(),
            })
            .await
            .unwrap();
        let starts = counters.starts.load(Ordering::SeqCst);
        let missing = facade
            .proxy_drain_state(ProxyDrainStateArgs {
                cluster: "local-dev".to_string(),
                proxy_name: "proxy-missing".to_string(),
            })
            .await
            .unwrap_err();

        assert_eq!(local.data.cluster, "local-dev");
        assert_eq!(secondary.data.cluster, "secondary");
        assert!(matches!(missing, ToolExecutionError::InvalidArguments(_)));
        assert_eq!(counters.starts.load(Ordering::SeqCst), starts);
        assert_eq!(counters.proxy_endpoint_mismatches.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn query_facade_cluster_overview_counts_unique_inventory_with_one_source_call() {
        let factory = FakeSessionFactory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config_with_physical_cluster(), factory);

        let result = facade
            .cluster_overview(ClusterOverviewArgs {
                cluster: "local-dev".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(result.topic_count, 2);
        assert_eq!(result.consumer_group_count, 1);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 3);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 3);
        assert_eq!(counters.broker_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.consumer_group_inventory_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.consumer_group_enrichment_queries.load(Ordering::SeqCst), 0);
        assert_eq!(counters.consumer_group_queries.load(Ordering::SeqCst), 0);
        assert_eq!(counters.route_queries.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn query_facade_composes_partial_evidence_for_overview_lists_lag_broker_and_diagnosis() {
        let facade = QueryFacade::with_factory(
            example_config(),
            FakeSessionFactory {
                partial_sources: true,
                ..Default::default()
            },
        );

        let overview = facade
            .cluster_overview(ClusterOverviewArgs {
                cluster: "local-dev".to_string(),
            })
            .await
            .unwrap();
        assert!(overview.partial);
        assert_eq!(overview.source_failures.len(), 1);

        let groups = facade
            .list_consumer_groups(ListConsumerGroupsArgs {
                cluster: Some("local-dev".to_string()),
                filter: None,
                page: PageRequest::default(),
            })
            .await
            .unwrap();
        assert!(groups.partial);
        assert_eq!(groups.source_failures[0].source, QuerySource::ConsumerConnection);

        let lag = facade
            .query_consumer_lag(QueryConsumerLagArgs {
                cluster: "local-dev".to_string(),
                topic: "orders".to_string(),
                consumer_group: "order-service".to_string(),
                page: PageRequest::default(),
            })
            .await
            .unwrap();
        assert!(lag.partial);
        assert_eq!(lag.source_failures[0].source, QuerySource::ConsumerStatistics);

        let broker = facade
            .describe_broker(DescribeBrokerArgs {
                cluster: "local-dev".to_string(),
                broker_name: "broker-a".to_string(),
            })
            .await
            .unwrap();
        assert!(broker.partial);
        assert_eq!(broker.source_failures[0].logical_target, "broker-b");

        let diagnosis = facade.diagnose_consumer_lag(diagnosis_request()).await.unwrap();
        assert!(diagnosis.partial);
        assert!(diagnosis
            .warnings
            .iter()
            .any(|warning| warning == "source_failures_present"));
        assert!(diagnosis.source_failures.len() >= 2);
    }

    #[tokio::test]
    async fn failed_selected_broker_is_not_reported_as_not_found() {
        let facade = QueryFacade::with_factory(
            example_config(),
            FakeSessionFactory {
                failed_selected_broker: true,
                ..Default::default()
            },
        );

        let error = facade
            .describe_broker(DescribeBrokerArgs {
                cluster: "local-dev".to_string(),
                broker_name: "broker-a".to_string(),
            })
            .await
            .unwrap_err();

        assert!(matches!(error, ToolExecutionError::Backend(_)));
        assert!(!error.to_string().contains("not found"));
    }

    #[tokio::test]
    async fn selected_broker_beyond_public_failure_cap_is_unavailable_for_tool_and_resource() {
        let facade = QueryFacade::with_factory(
            example_config(),
            FakeSessionFactory {
                overflow_failed_selected_broker: true,
                ..Default::default()
            },
        );

        let public = facade
            .cluster_overview(ClusterOverviewArgs {
                cluster: "local-dev".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(
            public.source_failures.len(),
            crate::model::contract::MAX_SOURCE_FAILURES
        );
        assert!(public
            .warnings
            .iter()
            .any(|warning| warning == "source_failures_truncated"));
        assert!(!public
            .source_failures
            .iter()
            .any(|failure| failure.logical_target == "broker-z"));

        let tool_error = ReadOnlyQuery::describe_broker(
            &facade,
            DescribeBrokerArgs {
                cluster: "local-dev".to_string(),
                broker_name: "broker-z".to_string(),
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(tool_error, ToolExecutionError::Backend(_)));
        assert!(!tool_error.to_string().contains("not found"));

        let resource_error =
            resources::reader::read_resource(&facade, "rocketmq://clusters/local-dev/brokers/broker-z")
                .await
                .unwrap_err();
        assert_ne!(resource_error.code, rmcp::model::ErrorCode::RESOURCE_NOT_FOUND);
        assert_eq!(resource_error.data.unwrap()["code"], "source_unavailable");
    }

    #[tokio::test]
    async fn query_facade_list_topics_sorts_deduplicates_and_preserves_null_consumer_group() {
        let factory = FakeSessionFactory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config_with_physical_cluster(), factory);

        let result = facade
            .list_topics(ListTopicsArgs {
                cluster: Some("local-dev".to_string()),
                filter: None,
                page: PageRequest::default(),
            })
            .await
            .unwrap();

        assert_eq!(
            result
                .page
                .items
                .iter()
                .map(|entry| entry.topic.as_str())
                .collect::<Vec<_>>(),
            ["orders", "payments"]
        );
        assert!(result
            .page
            .items
            .iter()
            .all(|entry| entry.cluster.as_deref() == Some("DefaultCluster")));
        assert!(result.page.items.iter().all(|entry| entry.consumer_group.is_none()));
        assert_eq!(
            serde_json::to_value(&result.page.items[0]).unwrap()["consumer_group"],
            serde_json::Value::Null
        );
        assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.route_queries.load(Ordering::SeqCst), 0);
        assert_eq!(counters.consumer_group_queries.load(Ordering::SeqCst), 0);
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
            })
            .await
            .unwrap();

        assert_eq!(report.evidence_version, "rocketmq-mcp.evidence.consumer-lag.v2");
        assert_eq!(report.rules_version, "rocketmq-mcp.rules.consumer-lag.v2");
        assert_eq!(report.policy_profile, "production-default");
        assert!(!report.partial);
        assert!(report.evidence_snapshot.is_some());
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
                && evidence.status == crate::model::diagnosis::EvidenceStatus::Unavailable));
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(counters.route_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.broker_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.runtime_probes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn query_facade_timeout_shuts_down_every_started_session_once() {
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
        let starts = counters.starts.load(Ordering::SeqCst);
        assert!(starts > 0);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), starts);
    }

    #[tokio::test]
    async fn query_facade_cancellation_shuts_down_every_started_session_once() {
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
        let starts = counters.starts.load(Ordering::SeqCst);
        assert!(starts > 0);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), starts);
    }

    #[tokio::test]
    async fn query_facade_backend_failure_shuts_down_the_started_session_once() {
        let factory = FakeSessionFactory {
            fail_topic_inventory: true,
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
        assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn production_query_facade_uses_the_admin_core_session_factory() {
        static OWNER: std::sync::LazyLock<rocketmq_runtime::RuntimeOwner> = std::sync::LazyLock::new(|| {
            rocketmq_runtime::RuntimeOwner::plan(rocketmq_runtime::RuntimeConfig {
                thread_name: "rocketmq-mcp-query-test".to_string(),
                ..Default::default()
            })
            .expect("runtime configuration is valid")
            .build()
            .expect("MCP query test runtime should start")
        });
        let client_runtime = rocketmq_admin_core::read_client_adapter::ClientRuntime::try_new(
            OWNER.root_context().component("client"),
            rocketmq_admin_core::read_client_adapter::ClientRuntimeConfig::default(),
            rocketmq_observability::TelemetryHandle::noop(),
        )
        .expect("MCP query test client runtime should start");
        let _: QueryFacade<AdminCoreSessionFactory> = QueryFacade::new(example_config(), client_runtime);
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
        assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn query_facade_reuses_cached_results_across_tool_and_resource_queries() {
        let factory = FakeSessionFactory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let tool_query = facade.clone().with_visibility_class(VisibilityClass::Sensitive);
        let resource_query = facade.clone().with_visibility_class(VisibilityClass::Sensitive);
        let request = ListTopicsArgs {
            cluster: Some("local-dev".to_string()),
            filter: None,
            page: PageRequest::default(),
        };

        let tool_result = tool_query.list_topics(request).await.unwrap();
        let resource_result = resources::reader::read_resource(&resource_query, "rocketmq://clusters/local-dev/topics")
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
        assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 1);
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
        let gate = Arc::new(TopicInventoryGate::new(1));
        gate.arm();
        let factory = FakeSessionFactory {
            topic_inventory_gate: Some(gate.clone()),
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let start = Arc::new(Barrier::new(9));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let facade = facade.clone();
            let start = start.clone();
            tasks.spawn(async move {
                start.wait().await;
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

        start.wait().await;
        wait_for_atomic_count(&gate.entered, 1).await;
        for _ in 0..10_000 {
            if facade.cache_metrics().coalesced_waiters == 7 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(gate.entered.load(Ordering::SeqCst), 1);
        assert_eq!(facade.cache_metrics().coalesced_waiters, 7);
        gate.release().await;

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
        assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 1);
        assert_eq!(facade.cache_metrics().coalesced_waiters, 7);
    }

    #[tokio::test]
    async fn query_facade_singleflight_waiter_observes_its_cancellation() {
        let factory = FakeSessionFactory {
            hang_topic_inventory: true,
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
        while counters.topic_inventory_queries.load(Ordering::SeqCst) == 0 {
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
        let reader = facade.clone().with_visibility_class(VisibilityClass::Standard);
        let topology_reader = facade.with_visibility_class(VisibilityClass::Sensitive);
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
        assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn query_facade_same_class_shares_ordinary_cache_and_snapshot_state() {
        let factory = FakeSessionFactory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let first = facade.clone().with_visibility_class(VisibilityClass::Standard);
        let second = facade.with_visibility_class(VisibilityClass::Standard);

        let topic_args = || ListTopicsArgs {
            cluster: Some("local-dev".to_string()),
            filter: None,
            page: PageRequest::default(),
        };
        assert_eq!(
            first.list_topics(topic_args()).await.unwrap().cache_status,
            CacheStatus::Miss
        );
        assert_eq!(
            second.list_topics(topic_args()).await.unwrap().cache_status,
            CacheStatus::Hit
        );

        let broker_args = || DescribeBrokerArgs {
            cluster: "local-dev".to_string(),
            broker_name: "broker-a".to_string(),
        };
        assert_eq!(
            first.describe_broker(broker_args()).await.unwrap().cache_status,
            CacheStatus::Miss
        );
        assert_eq!(
            second.describe_broker(broker_args()).await.unwrap().cache_status,
            CacheStatus::Hit
        );
        assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.broker_queries.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn query_facade_different_classes_isolate_ordinary_cache_and_snapshot_state() {
        let factory = FakeSessionFactory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let standard = facade.clone().with_visibility_class(VisibilityClass::Standard);
        let sensitive = facade.with_visibility_class(VisibilityClass::Sensitive);

        let topic_args = || ListTopicsArgs {
            cluster: Some("local-dev".to_string()),
            filter: None,
            page: PageRequest::default(),
        };
        assert_eq!(
            standard.list_topics(topic_args()).await.unwrap().cache_status,
            CacheStatus::Miss
        );
        assert_eq!(
            sensitive.list_topics(topic_args()).await.unwrap().cache_status,
            CacheStatus::Miss
        );

        let broker_args = || DescribeBrokerArgs {
            cluster: "local-dev".to_string(),
            broker_name: "broker-a".to_string(),
        };
        assert_eq!(
            standard.describe_broker(broker_args()).await.unwrap().cache_status,
            CacheStatus::Miss
        );
        assert_eq!(
            sensitive.describe_broker(broker_args()).await.unwrap().cache_status,
            CacheStatus::Miss
        );
        assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 2);
        assert_eq!(counters.broker_queries.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cross_class_cursor_replay_fails_without_an_upstream_reload() {
        let factory = FakeSessionFactory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let standard = facade.clone().with_visibility_class(VisibilityClass::Standard);
        let sensitive = facade.with_visibility_class(VisibilityClass::Sensitive);
        let first = standard
            .list_topics(ListTopicsArgs {
                cluster: Some("local-dev".to_string()),
                filter: None,
                page: PageRequest {
                    limit: Some(1),
                    cursor: None,
                },
            })
            .await
            .unwrap();

        let error = sensitive
            .list_topics(ListTopicsArgs {
                cluster: Some("local-dev".to_string()),
                filter: None,
                page: PageRequest {
                    limit: Some(1),
                    cursor: first.page.next_cursor.clone(),
                },
            })
            .await
            .unwrap_err();

        assert!(matches!(error, ToolExecutionError::InvalidArguments(_)));
        assert!(error.to_string().contains("requested query context"));
        assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn singleflight_coalesces_within_class_but_not_across_classes() {
        let gate = Arc::new(TopicInventoryGate::new(2));
        gate.arm();
        let factory = FakeSessionFactory {
            topic_inventory_gate: Some(gate.clone()),
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let start = Arc::new(Barrier::new(5));
        let mut tasks = tokio::task::JoinSet::new();
        for visibility in [
            VisibilityClass::Standard,
            VisibilityClass::Standard,
            VisibilityClass::Sensitive,
            VisibilityClass::Sensitive,
        ] {
            let request_facade = facade.clone().with_visibility_class(visibility);
            let start = start.clone();
            tasks.spawn(async move {
                start.wait().await;
                request_facade
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

        start.wait().await;
        wait_for_atomic_count(&gate.entered, 2).await;
        for _ in 0..10_000 {
            if facade.cache_metrics().coalesced_waiters == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(facade.cache_metrics().coalesced_waiters, 2);
        gate.release().await;

        let mut statuses = Vec::new();
        while let Some(status) = tasks.join_next().await {
            statuses.push(status.unwrap());
        }
        assert_eq!(
            statuses.iter().filter(|status| **status == CacheStatus::Miss).count(),
            2
        );
        assert_eq!(statuses.iter().filter(|status| **status == CacheStatus::Hit).count(), 2);
        assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 2);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 2);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 2);
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

    #[tokio::test]
    async fn consumer_group_walk_is_stable_when_upstream_reorders_removes_and_inserts() {
        let factory = FakeSessionFactory {
            mutating_group_inventory: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let mut cursor = None;
        let mut second_cursor = None;
        let mut observed = Vec::new();
        loop {
            let result = facade
                .list_consumer_groups(ListConsumerGroupsArgs {
                    cluster: Some("local-dev".to_string()),
                    filter: None,
                    page: PageRequest {
                        limit: Some(2),
                        cursor: cursor.clone(),
                    },
                })
                .await
                .unwrap();
            observed.extend(result.page.items.iter().map(|group| group.group.clone()));
            if second_cursor.is_none() {
                second_cursor = result.page.next_cursor.clone();
            }
            cursor = result.page.next_cursor.clone();
            if cursor.is_none() {
                break;
            }
        }

        assert_eq!(observed, ["group-0", "group-1", "group-2", "group-3", "group-4"]);
        assert_eq!(counters.consumer_group_inventory_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.consumer_group_enrichment_queries.load(Ordering::SeqCst), 3);
        assert_eq!(counters.consumer_group_enriched_targets.load(Ordering::SeqCst), 5);
        let sessions_before_replay = counters.starts.load(Ordering::SeqCst);
        let replay = facade
            .list_consumer_groups(ListConsumerGroupsArgs {
                cluster: Some("local-dev".to_string()),
                filter: None,
                page: PageRequest {
                    limit: Some(2),
                    cursor: second_cursor,
                },
            })
            .await
            .unwrap();
        assert_eq!(replay.cache_status, CacheStatus::Hit);
        assert_eq!(counters.starts.load(Ordering::SeqCst), sessions_before_replay);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), sessions_before_replay);
    }

    #[tokio::test]
    async fn empty_consumer_group_inventory_needs_no_enrichment_session() {
        let factory = FakeSessionFactory {
            empty_groups: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let result = facade
            .list_consumer_groups(ListConsumerGroupsArgs::default())
            .await
            .unwrap();

        assert!(result.page.items.is_empty());
        assert_eq!(result.page.total_count, 0);
        assert!(!result.page.has_more);
        assert_eq!(counters.consumer_group_inventory_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.consumer_group_enrichment_queries.load(Ordering::SeqCst), 0);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn total_selected_page_enrichment_failure_is_not_cached_and_shuts_down() {
        let factory = FakeSessionFactory {
            fail_group_enrichment: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let error = facade
            .list_consumer_groups(ListConsumerGroupsArgs::default())
            .await
            .unwrap_err();
        assert!(matches!(error, ToolExecutionError::Backend(_)));
        assert_eq!(counters.consumer_group_inventory_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.consumer_group_enrichment_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 2);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn route_snapshot_is_shared_by_tool_description_and_query_resource_pages() {
        let factory = FakeSessionFactory {
            many_route_rows: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let first = facade
            .query_topic_route(QueryTopicRouteArgs {
                cluster: "local-dev".to_string(),
                topic: "orders".to_string(),
                page: PageRequest {
                    limit: Some(2),
                    cursor: None,
                },
            })
            .await
            .unwrap();
        let cursor = first.page.next_cursor.clone().unwrap();
        let described = facade
            .describe_topic(DescribeTopicArgs {
                cluster: "local-dev".to_string(),
                topic: "orders".to_string(),
                page: PageRequest {
                    limit: Some(2),
                    cursor: Some(cursor.clone()),
                },
            })
            .await
            .unwrap();
        assert_eq!(described.page.items.len(), 2);
        let uri = format!("rocketmq://clusters/local-dev/topics/orders/route?limit=2&cursor={cursor}");
        resources::reader::read_resource(&facade, &uri).await.unwrap();

        assert_eq!(counters.route_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn consumer_lag_resource_page_reuses_tool_detail_snapshot_without_a_session() {
        let factory = FakeSessionFactory {
            many_lag_rows: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let first = facade
            .query_consumer_lag(QueryConsumerLagArgs {
                cluster: "local-dev".to_string(),
                topic: "orders".to_string(),
                consumer_group: "order-service".to_string(),
                page: PageRequest {
                    limit: Some(2),
                    cursor: None,
                },
            })
            .await
            .unwrap();
        let cursor = first.page.next_cursor.clone().unwrap();
        let uri = format!(
            "rocketmq://clusters/local-dev/consumer-groups/order-service/lag?topic=orders&limit=2&cursor={cursor}"
        );
        resources::reader::read_resource(&facade, &uri).await.unwrap();

        assert_eq!(counters.consumer_lag_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn exact_consumer_group_resource_enriches_only_the_requested_group() {
        let factory = FakeSessionFactory {
            many_groups: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let result = resources::reader::read_resource(&facade, "rocketmq://clusters/local-dev/consumer-groups/group-4")
            .await
            .unwrap();
        let payload = match &result.contents[0] {
            rmcp::model::ResourceContents::TextResourceContents { text, .. } => {
                serde_json::from_str::<serde_json::Value>(text).unwrap()
            }
            _ => panic!("resource should contain JSON text"),
        };
        assert_eq!(payload["consumer_group"]["group"], "group-4");
        assert_eq!(counters.consumer_group_inventory_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.consumer_group_enrichment_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.consumer_group_enriched_targets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn literal_filter_then_exact_resource_use_distinct_structured_snapshot_selections() {
        let factory = FakeSessionFactory {
            many_groups: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);

        let listed = facade
            .list_consumer_groups(ListConsumerGroupsArgs {
                cluster: Some("local-dev".to_string()),
                filter: Some("exact=group-4".to_string()),
                page: PageRequest {
                    limit: Some(1),
                    cursor: None,
                },
            })
            .await
            .unwrap();
        assert!(listed.page.items.is_empty());
        let same_text_filter = facade
            .list_consumer_groups(ListConsumerGroupsArgs {
                cluster: Some("local-dev".to_string()),
                filter: Some("group-4".to_string()),
                page: PageRequest {
                    limit: Some(1),
                    cursor: None,
                },
            })
            .await
            .unwrap();
        assert_eq!(same_text_filter.page.items[0].group, "group-4");

        let resource =
            resources::reader::read_resource(&facade, "rocketmq://clusters/local-dev/consumer-groups/group-4")
                .await
                .unwrap();
        let payload = match &resource.contents[0] {
            rmcp::model::ResourceContents::TextResourceContents { text, .. } => {
                serde_json::from_str::<serde_json::Value>(text).unwrap()
            }
            _ => panic!("resource should contain JSON text"),
        };
        assert_eq!(payload["consumer_group"]["group"], "group-4");
        assert_eq!(counters.consumer_group_inventory_queries.load(Ordering::SeqCst), 3);
        assert_eq!(counters.consumer_group_enrichment_queries.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn exact_resource_then_literal_filter_use_distinct_structured_snapshot_selections() {
        let factory = FakeSessionFactory {
            many_groups: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);

        resources::reader::read_resource(&facade, "rocketmq://clusters/local-dev/consumer-groups/group-4")
            .await
            .unwrap();
        let same_text_filter = facade
            .list_consumer_groups(ListConsumerGroupsArgs {
                cluster: Some("local-dev".to_string()),
                filter: Some("group-4".to_string()),
                page: PageRequest {
                    limit: Some(1),
                    cursor: None,
                },
            })
            .await
            .unwrap();
        assert_eq!(same_text_filter.page.items[0].group, "group-4");
        let listed = facade
            .list_consumer_groups(ListConsumerGroupsArgs {
                cluster: Some("local-dev".to_string()),
                filter: Some("exact=group-4".to_string()),
                page: PageRequest {
                    limit: Some(1),
                    cursor: None,
                },
            })
            .await
            .unwrap();

        assert!(listed.page.items.is_empty());
        assert_eq!(counters.consumer_group_inventory_queries.load(Ordering::SeqCst), 3);
        assert_eq!(counters.consumer_group_enrichment_queries.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn case_colliding_group_list_then_exact_resources_preserve_identity() {
        let factory = FakeSessionFactory {
            case_colliding_groups: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);

        let listed = facade
            .list_consumer_groups(ListConsumerGroupsArgs {
                cluster: Some("local-dev".to_string()),
                filter: Some("ordergroup".to_string()),
                page: PageRequest {
                    limit: Some(10),
                    cursor: None,
                },
            })
            .await
            .unwrap();
        assert_eq!(
            listed
                .page
                .items
                .iter()
                .map(|item| item.group.as_str())
                .collect::<Vec<_>>(),
            ["OrderGroup", "ordergroup"]
        );
        for group in ["OrderGroup", "ordergroup"] {
            let resource = resources::reader::read_resource(
                &facade,
                &format!("rocketmq://clusters/local-dev/consumer-groups/{group}"),
            )
            .await
            .unwrap();
            assert_eq!(resource_json(resource)["consumer_group"]["group"], group);
        }
        let error =
            resources::reader::read_resource(&facade, "rocketmq://clusters/local-dev/consumer-groups/ORDERGROUP")
                .await
                .unwrap_err();
        assert!(error.to_string().contains("not found"));
        assert!(!error.to_string().contains("unavailable"));
        assert_eq!(counters.consumer_group_inventory_queries.load(Ordering::SeqCst), 4);
        assert_eq!(counters.consumer_group_enrichment_queries.load(Ordering::SeqCst), 3);
        assert_eq!(counters.consumer_group_enriched_targets.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn case_colliding_exact_resources_then_list_preserve_identity() {
        let factory = FakeSessionFactory {
            case_colliding_groups: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);

        for group in ["OrderGroup", "ordergroup"] {
            let resource = resources::reader::read_resource(
                &facade,
                &format!("rocketmq://clusters/local-dev/consumer-groups/{group}"),
            )
            .await
            .unwrap();
            assert_eq!(resource_json(resource)["consumer_group"]["group"], group);
        }
        let error =
            resources::reader::read_resource(&facade, "rocketmq://clusters/local-dev/consumer-groups/ORDERGROUP")
                .await
                .unwrap_err();
        assert!(error.to_string().contains("not found"));
        assert!(!error.to_string().contains("unavailable"));
        let listed = facade
            .list_consumer_groups(ListConsumerGroupsArgs {
                cluster: Some("local-dev".to_string()),
                filter: Some("ordergroup".to_string()),
                page: PageRequest {
                    limit: Some(10),
                    cursor: None,
                },
            })
            .await
            .unwrap();
        assert_eq!(
            listed
                .page
                .items
                .iter()
                .map(|item| item.group.as_str())
                .collect::<Vec<_>>(),
            ["OrderGroup", "ordergroup"]
        );
        assert_eq!(counters.consumer_group_inventory_queries.load(Ordering::SeqCst), 4);
        assert_eq!(counters.consumer_group_enrichment_queries.load(Ordering::SeqCst), 3);
        assert_eq!(counters.consumer_group_enriched_targets.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn cursor_mismatch_fails_without_rebuilding_or_starting_a_session() {
        let factory = FakeSessionFactory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let first = facade
            .list_topics(ListTopicsArgs {
                cluster: Some("local-dev".to_string()),
                filter: None,
                page: PageRequest {
                    limit: Some(1),
                    cursor: None,
                },
            })
            .await
            .unwrap();
        let starts = counters.starts.load(Ordering::SeqCst);
        let error = facade
            .list_topics(ListTopicsArgs {
                cluster: Some("local-dev".to_string()),
                filter: Some("orders".to_string()),
                page: PageRequest {
                    limit: Some(1),
                    cursor: first.page.next_cursor.clone(),
                },
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("context"));
        assert_eq!(counters.starts.load(Ordering::SeqCst), starts);
    }

    #[tokio::test]
    async fn topic_snapshot_bypass_reloads_first_pages_without_losing_cursor_replay() {
        let mut config = example_config();
        config.cache.enabled = false;
        let gate = Arc::new(TopicInventoryGate::new(2));
        let factory = FakeSessionFactory {
            topic_inventory_gate: Some(gate.clone()),
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(config, factory);
        let args = || ListTopicsArgs {
            cluster: Some("local-dev".to_string()),
            filter: None,
            page: PageRequest {
                limit: Some(1),
                cursor: None,
            },
        };

        assert_eq!(
            facade.list_topics(args()).await.unwrap().cache_status,
            CacheStatus::Bypass
        );
        assert_eq!(
            facade.list_topics(args()).await.unwrap().cache_status,
            CacheStatus::Bypass
        );
        gate.arm();
        let start = Arc::new(Barrier::new(3));
        let left_start = start.clone();
        let left_facade = facade.clone();
        let left = tokio::spawn(async move {
            left_start.wait().await;
            left_facade.list_topics(args()).await
        });
        let right_start = start.clone();
        let right_facade = facade.clone();
        let right = tokio::spawn(async move {
            right_start.wait().await;
            right_facade.list_topics(args()).await
        });
        start.wait().await;
        wait_for_atomic_count(&gate.entered, 2).await;
        assert_eq!(facade.cache_metrics().coalesced_waiters, 0);
        gate.release().await;
        let left = left.await.unwrap();
        let right = right.await.unwrap();
        assert_eq!(left.unwrap().cache_status, CacheStatus::Bypass);
        assert_eq!(right.unwrap().cache_status, CacheStatus::Bypass);
        let first = facade.list_topics(args()).await.unwrap();
        let continuation = ListTopicsArgs {
            page: PageRequest {
                limit: Some(1),
                cursor: first.page.next_cursor.clone(),
            },
            ..args()
        };
        assert_eq!(
            facade.list_topics(continuation.clone()).await.unwrap().cache_status,
            CacheStatus::Hit
        );
        assert_eq!(
            facade.list_topics(continuation).await.unwrap().cache_status,
            CacheStatus::Hit
        );
        assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 5);
        assert_eq!(facade.cache_metrics().coalesced_waiters, 0);
    }

    #[tokio::test]
    async fn route_snapshot_zero_ttl_reloads_first_pages_without_losing_cursor_replay() {
        let mut config = example_config();
        config.cache.topic_list_ttl_ms = 0;
        let factory = FakeSessionFactory {
            many_route_rows: true,
            yield_snapshot_queries: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(config, factory);
        let args = || QueryTopicRouteArgs {
            cluster: "local-dev".to_string(),
            topic: "orders".to_string(),
            page: PageRequest {
                limit: Some(2),
                cursor: None,
            },
        };

        assert_eq!(
            facade.query_topic_route(args()).await.unwrap().cache_status,
            CacheStatus::Bypass
        );
        assert_eq!(
            facade.query_topic_route(args()).await.unwrap().cache_status,
            CacheStatus::Bypass
        );
        let (left, right) = tokio::join!(facade.query_topic_route(args()), facade.query_topic_route(args()));
        assert_eq!(left.unwrap().cache_status, CacheStatus::Bypass);
        assert_eq!(right.unwrap().cache_status, CacheStatus::Bypass);
        let first = facade.query_topic_route(args()).await.unwrap();
        let continuation = QueryTopicRouteArgs {
            page: PageRequest {
                limit: Some(2),
                cursor: first.page.next_cursor.clone(),
            },
            ..args()
        };
        assert_eq!(
            facade
                .query_topic_route(continuation.clone())
                .await
                .unwrap()
                .cache_status,
            CacheStatus::Hit
        );
        assert_eq!(
            facade.query_topic_route(continuation).await.unwrap().cache_status,
            CacheStatus::Hit
        );
        assert_eq!(counters.route_queries.load(Ordering::SeqCst), 5);
        assert_eq!(facade.cache_metrics().coalesced_waiters, 0);
    }

    #[tokio::test]
    async fn lag_snapshot_zero_ttl_reloads_first_pages_without_losing_cursor_replay() {
        let mut config = example_config();
        config.cache.consumer_lag_ttl_ms = 0;
        let factory = FakeSessionFactory {
            many_lag_rows: true,
            yield_snapshot_queries: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(config, factory);
        let args = || QueryConsumerLagArgs {
            cluster: "local-dev".to_string(),
            topic: "orders".to_string(),
            consumer_group: "order-service".to_string(),
            page: PageRequest {
                limit: Some(2),
                cursor: None,
            },
        };

        assert_eq!(
            facade.query_consumer_lag(args()).await.unwrap().cache_status,
            CacheStatus::Bypass
        );
        assert_eq!(
            facade.query_consumer_lag(args()).await.unwrap().cache_status,
            CacheStatus::Bypass
        );
        let (left, right) = tokio::join!(facade.query_consumer_lag(args()), facade.query_consumer_lag(args()));
        assert_eq!(left.unwrap().cache_status, CacheStatus::Bypass);
        assert_eq!(right.unwrap().cache_status, CacheStatus::Bypass);
        let first = facade.query_consumer_lag(args()).await.unwrap();
        let continuation = QueryConsumerLagArgs {
            page: PageRequest {
                limit: Some(2),
                cursor: first.page.next_cursor.clone(),
            },
            ..args()
        };
        assert_eq!(
            facade
                .query_consumer_lag(continuation.clone())
                .await
                .unwrap()
                .cache_status,
            CacheStatus::Hit
        );
        assert_eq!(
            facade.query_consumer_lag(continuation).await.unwrap().cache_status,
            CacheStatus::Hit
        );
        assert_eq!(counters.consumer_lag_queries.load(Ordering::SeqCst), 5);
        assert_eq!(facade.cache_metrics().coalesced_waiters, 0);
    }

    #[tokio::test]
    async fn group_snapshot_bypass_reloads_first_pages_but_reuses_cursor_inventory() {
        let mut config = example_config();
        config.cache.enabled = false;
        let factory = FakeSessionFactory {
            many_groups: true,
            yield_snapshot_queries: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(config, factory);
        let args = || ListConsumerGroupsArgs {
            cluster: Some("local-dev".to_string()),
            filter: None,
            page: PageRequest {
                limit: Some(2),
                cursor: None,
            },
        };

        assert_eq!(
            facade.list_consumer_groups(args()).await.unwrap().cache_status,
            CacheStatus::Bypass
        );
        assert_eq!(
            facade.list_consumer_groups(args()).await.unwrap().cache_status,
            CacheStatus::Bypass
        );
        let (left, right) = tokio::join!(facade.list_consumer_groups(args()), facade.list_consumer_groups(args()));
        assert_eq!(left.unwrap().cache_status, CacheStatus::Bypass);
        assert_eq!(right.unwrap().cache_status, CacheStatus::Bypass);
        let first = facade.list_consumer_groups(args()).await.unwrap();
        let continuation = ListConsumerGroupsArgs {
            page: PageRequest {
                limit: Some(2),
                cursor: first.page.next_cursor.clone(),
            },
            ..args()
        };
        assert_eq!(
            facade
                .list_consumer_groups(continuation.clone())
                .await
                .unwrap()
                .cache_status,
            CacheStatus::Bypass
        );
        assert_eq!(
            facade.list_consumer_groups(continuation).await.unwrap().cache_status,
            CacheStatus::Bypass
        );
        assert_eq!(counters.consumer_group_inventory_queries.load(Ordering::SeqCst), 5);
        assert_eq!(counters.consumer_group_enrichment_queries.load(Ordering::SeqCst), 7);
        assert_eq!(facade.cache_metrics().coalesced_waiters, 0);
    }

    #[tokio::test]
    async fn topic_route_and_lag_full_page_walks_use_one_base_rpc_each() {
        let factory = FakeSessionFactory {
            many_route_rows: true,
            many_lag_rows: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);

        let mut cursor = None;
        let mut topics = Vec::new();
        loop {
            let result = facade
                .list_topics(ListTopicsArgs {
                    cluster: Some("local-dev".to_string()),
                    filter: None,
                    page: PageRequest { limit: Some(1), cursor },
                })
                .await
                .unwrap();
            topics.extend(result.page.items.iter().map(|topic| topic.topic.clone()));
            cursor = result.page.next_cursor.clone();
            if cursor.is_none() {
                break;
            }
        }

        let mut cursor = None;
        let mut route_rows = 0;
        loop {
            let result = facade
                .query_topic_route(QueryTopicRouteArgs {
                    cluster: "local-dev".to_string(),
                    topic: "orders".to_string(),
                    page: PageRequest { limit: Some(2), cursor },
                })
                .await
                .unwrap();
            route_rows += result.page.items.len();
            cursor = result.page.next_cursor.clone();
            if cursor.is_none() {
                break;
            }
        }

        let mut cursor = None;
        let mut lag_rows = 0;
        loop {
            let result = facade
                .query_consumer_lag(QueryConsumerLagArgs {
                    cluster: "local-dev".to_string(),
                    topic: "orders".to_string(),
                    consumer_group: "order-service".to_string(),
                    page: PageRequest { limit: Some(2), cursor },
                })
                .await
                .unwrap();
            lag_rows += result.page.items.len();
            cursor = result.page.next_cursor.clone();
            if cursor.is_none() {
                break;
            }
        }

        assert_eq!(topics, ["orders", "payments"]);
        assert_eq!(route_rows, 5);
        assert_eq!(lag_rows, 5);
        assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.route_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.consumer_lag_queries.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn consumer_group_pages_share_inventory_and_enrichment_in_both_surface_orders() {
        let factory = FakeSessionFactory {
            many_groups: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        facade
            .list_consumer_groups(ListConsumerGroupsArgs {
                cluster: Some("local-dev".to_string()),
                filter: None,
                page: PageRequest {
                    limit: Some(2),
                    cursor: None,
                },
            })
            .await
            .unwrap();
        resources::reader::read_resource(&facade, "rocketmq://clusters/local-dev/consumer-groups?limit=2")
            .await
            .unwrap();
        assert_eq!(counters.consumer_group_inventory_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.consumer_group_enrichment_queries.load(Ordering::SeqCst), 1);

        let factory = FakeSessionFactory {
            many_groups: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        resources::reader::read_resource(&facade, "rocketmq://clusters/local-dev/consumer-groups?limit=2")
            .await
            .unwrap();
        facade
            .list_consumer_groups(ListConsumerGroupsArgs {
                cluster: Some("local-dev".to_string()),
                filter: None,
                page: PageRequest {
                    limit: Some(2),
                    cursor: None,
                },
            })
            .await
            .unwrap();
        assert_eq!(counters.consumer_group_inventory_queries.load(Ordering::SeqCst), 1);
        assert_eq!(counters.consumer_group_enrichment_queries.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn detail_snapshot_preserves_observation_and_full_evidence_across_surfaces() {
        let factory = FakeSessionFactory {
            many_lag_rows: true,
            partial_sources: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(example_config(), factory);
        let first = facade
            .query_consumer_lag(QueryConsumerLagArgs {
                cluster: "local-dev".to_string(),
                topic: "orders".to_string(),
                consumer_group: "order-service".to_string(),
                page: PageRequest {
                    limit: Some(2),
                    cursor: None,
                },
            })
            .await
            .unwrap();
        let cursor = first.page.next_cursor.clone().unwrap();
        let uri = format!(
            "rocketmq://clusters/local-dev/consumer-groups/order-service/lag?topic=orders&limit=2&cursor={cursor}"
        );
        let resource = resource_json(resources::reader::read_resource(&facade, &uri).await.unwrap());
        assert_eq!(resource["observed_at"], first.observed_at);
        assert_eq!(resource["partial"], first.partial);
        assert_eq!(resource["warnings"], serde_json::json!(first.warnings));
        assert_eq!(resource["source_failures"], serde_json::json!(first.source_failures));

        let replay = facade
            .query_consumer_lag(QueryConsumerLagArgs {
                cluster: "local-dev".to_string(),
                topic: "orders".to_string(),
                consumer_group: "order-service".to_string(),
                page: PageRequest {
                    limit: Some(2),
                    cursor: Some(cursor),
                },
            })
            .await
            .unwrap();
        assert_eq!(replay.observed_at, first.observed_at);
        assert_eq!(replay.partial, first.partial);
        assert_eq!(replay.warnings, first.warnings);
        assert_eq!(replay.source_failures, first.source_failures);
        assert_eq!(replay.cache_status, CacheStatus::Hit);
        assert_eq!(counters.consumer_lag_queries.load(Ordering::SeqCst), 1);
    }

    fn example_config() -> McpConfig {
        McpConfig::load(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("conf")
                .join("mcp.example.toml"),
        )
        .unwrap()
    }

    fn resource_json(result: rmcp::model::ReadResourceResult) -> serde_json::Value {
        match &result.contents[0] {
            rmcp::model::ResourceContents::TextResourceContents { text, .. } => serde_json::from_str(text).unwrap(),
            _ => panic!("resource should contain JSON text"),
        }
    }

    fn example_config_with_physical_cluster() -> McpConfig {
        let mut config = example_config();
        config.clusters[0].rocketmq_cluster_name = Some("DefaultCluster".to_string());
        config
    }

    fn example_config_with_secondary_proxy() -> McpConfig {
        let mut config = example_config();
        let mut secondary = config.clusters[0].clone();
        secondary.name = "secondary".to_string();
        secondary.namesrv_addr = "secondary-namesrv.internal:9876".to_string();
        secondary.default = Some(false);
        secondary.proxies[0].endpoint = "proxy-secondary.internal:8081".to_string();
        config.clusters.push(secondary);
        config
    }

    fn diagnosis_request() -> DiagnoseConsumerLagArgs {
        DiagnoseConsumerLagArgs {
            cluster: "local-dev".to_string(),
            topic: "orders".to_string(),
            consumer_group: "order-service".to_string(),
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
