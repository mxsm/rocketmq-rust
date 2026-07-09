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

use std::collections::BTreeMap;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_admin_core::core::broker::BrokerRuntimeStatsQueryRequest;
use rocketmq_admin_core::core::broker::BrokerService;
use rocketmq_admin_core::core::cluster::ClusterBaseInfoRow;
use rocketmq_admin_core::core::cluster::ClusterListQueryRequest;
use rocketmq_admin_core::core::cluster::ClusterService;
use rocketmq_admin_core::core::consumer::ConsumerProgressRequest;
use rocketmq_admin_core::core::consumer::ConsumerProgressResult;
use rocketmq_admin_core::core::consumer::ConsumerService;
use rocketmq_admin_core::core::topic::TopicListQueryRequest;
use rocketmq_admin_core::core::topic::TopicRouteQueryRequest;
use rocketmq_admin_core::core::topic::TopicService;
use rocketmq_admin_core::core::RocketMQError;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;

use crate::config::ClusterConfig;
use crate::config::McpConfig;
use crate::tools::broker_tools;
use crate::tools::cluster_tools;
use crate::tools::consumer_tools;
use crate::tools::executor::ToolExecutionError;
use crate::tools::topic_tools;

pub(crate) trait ReadOnlyAdminAdapter: Clone + Send + Sync + 'static {
    async fn cluster_overview(
        &self,
        args: cluster_tools::ClusterOverviewArgs,
    ) -> Result<cluster_tools::ClusterOverviewOutput, ToolExecutionError>;

    async fn list_topics(
        &self,
        args: topic_tools::ListTopicsArgs,
    ) -> Result<topic_tools::ListTopicsOutput, ToolExecutionError>;

    async fn describe_topic(
        &self,
        args: topic_tools::DescribeTopicArgs,
    ) -> Result<topic_tools::DescribeTopicOutput, ToolExecutionError>;

    async fn query_topic_route(
        &self,
        args: topic_tools::QueryTopicRouteArgs,
    ) -> Result<topic_tools::QueryTopicRouteOutput, ToolExecutionError>;

    async fn list_consumer_groups(
        &self,
        args: consumer_tools::ListConsumerGroupsArgs,
    ) -> Result<consumer_tools::ListConsumerGroupsOutput, ToolExecutionError>;

    async fn query_consumer_lag(
        &self,
        args: consumer_tools::QueryConsumerLagArgs,
    ) -> Result<consumer_tools::QueryConsumerLagOutput, ToolExecutionError>;

    async fn describe_broker(
        &self,
        args: broker_tools::DescribeBrokerArgs,
    ) -> Result<broker_tools::DescribeBrokerOutput, ToolExecutionError>;
}

#[derive(Debug, Clone)]
pub(crate) struct AdminCoreAdapter {
    config: McpConfig,
}

impl AdminCoreAdapter {
    pub(crate) fn new(config: McpConfig) -> Self {
        Self { config }
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
            None => default_cluster(&self.config.clusters)?,
        };

        Ok(ResolvedCluster {
            name: config.name.clone(),
            namesrv_addr: config.namesrv_addr.clone(),
        })
    }
}

impl ReadOnlyAdminAdapter for AdminCoreAdapter {
    async fn cluster_overview(
        &self,
        args: cluster_tools::ClusterOverviewArgs,
    ) -> Result<cluster_tools::ClusterOverviewOutput, ToolExecutionError> {
        let cluster = self.resolve_cluster(Some(&args.cluster))?;
        let cluster_result = ClusterService::query_cluster_list_by_request_with_rpc_hook(
            ClusterListQueryRequest::new(false, Some(cluster.name.clone()))
                .with_optional_namesrv_addr(Some(cluster.namesrv_addr.clone())),
            None,
        )
        .await
        .map_err(admin_error)?;
        let topics = self
            .list_topics(topic_tools::ListTopicsArgs {
                cluster: Some(cluster.name.clone()),
            })
            .await?;
        let consumer_groups = self
            .list_consumer_groups(consumer_tools::ListConsumerGroupsArgs {
                cluster: Some(cluster.name.clone()),
            })
            .await?;

        Ok(cluster_tools::ClusterOverviewOutput {
            cluster: cluster.name,
            namesrv_addr: cluster.namesrv_addr,
            brokers: cluster_result.base_rows.iter().map(map_broker_summary).collect(),
            topic_count: topics.topic_count,
            consumer_group_count: consumer_groups.consumer_group_count,
            generated_at: generated_at(),
        })
    }

    async fn list_topics(
        &self,
        args: topic_tools::ListTopicsArgs,
    ) -> Result<topic_tools::ListTopicsOutput, ToolExecutionError> {
        let cluster = self.resolve_cluster(args.cluster.as_deref())?;
        let result = TopicService::query_topic_list(
            TopicListQueryRequest::new()
                .with_optional_namesrv_addr(Some(cluster.namesrv_addr.clone()))
                .with_optional_cluster_name(Some(cluster.name.clone())),
        )
        .await
        .map_err(admin_error)?;
        let mut topics = result
            .topics
            .iter()
            .map(|item| topic_tools::TopicListEntry {
                topic: item.topic.to_string(),
                cluster: item.cluster.as_ref().map(ToString::to_string),
                consumer_group: item.consumer_group.as_ref().map(ToString::to_string),
            })
            .collect::<Vec<_>>();
        topics.sort_by(|left, right| left.topic.cmp(&right.topic));

        Ok(topic_tools::ListTopicsOutput {
            cluster: cluster.name,
            namesrv_addr: cluster.namesrv_addr,
            topic_count: topics.len(),
            topics,
            generated_at: generated_at(),
        })
    }

    async fn describe_topic(
        &self,
        args: topic_tools::DescribeTopicArgs,
    ) -> Result<topic_tools::DescribeTopicOutput, ToolExecutionError> {
        let route = self
            .query_topic_route(topic_tools::QueryTopicRouteArgs {
                cluster: args.cluster,
                topic: args.topic,
            })
            .await?;
        let mut broker_names = route
            .brokers
            .iter()
            .map(|broker| broker.broker_name.clone())
            .collect::<Vec<_>>();
        broker_names.sort();
        broker_names.dedup();

        Ok(topic_tools::DescribeTopicOutput {
            cluster: route.cluster,
            namesrv_addr: route.namesrv_addr,
            topic: route.topic,
            read_queue_count: route.queues.iter().map(|queue| queue.read_queue_nums).sum(),
            write_queue_count: route.queues.iter().map(|queue| queue.write_queue_nums).sum(),
            broker_names,
            brokers: route.brokers,
            queues: route.queues,
            generated_at: route.generated_at,
        })
    }

    async fn query_topic_route(
        &self,
        args: topic_tools::QueryTopicRouteArgs,
    ) -> Result<topic_tools::QueryTopicRouteOutput, ToolExecutionError> {
        let cluster = self.resolve_cluster(Some(&args.cluster))?;
        let route = TopicService::query_topic_route(
            TopicRouteQueryRequest::try_new(args.topic.clone())
                .map_err(admin_error)?
                .with_optional_namesrv_addr(Some(cluster.namesrv_addr.clone())),
        )
        .await
        .map_err(admin_error)?
        .ok_or_else(|| ToolExecutionError::Backend(format!("topic route not found: {}", args.topic)))?;

        Ok(map_topic_route(cluster, args.topic, route))
    }

    async fn list_consumer_groups(
        &self,
        args: consumer_tools::ListConsumerGroupsArgs,
    ) -> Result<consumer_tools::ListConsumerGroupsOutput, ToolExecutionError> {
        let cluster = self.resolve_cluster(args.cluster.as_deref())?;
        let result = ConsumerService::query_consumer_progress_by_request_with_rpc_hook(
            ConsumerProgressRequest::try_new(
                None,
                None,
                false,
                Some(cluster.name.clone()),
                Some(cluster.namesrv_addr.clone()),
            )
            .map_err(admin_error)?,
            None,
        )
        .await
        .map_err(admin_error)?;
        let groups = match result {
            ConsumerProgressResult::All(groups) => groups
                .into_iter()
                .map(|group| consumer_tools::ConsumerGroupSummary {
                    group: group.group,
                    version: group.version,
                    client_count: group.count,
                    consume_type: format!("{:?}", group.consume_type),
                    message_model: format!("{:?}", group.message_model),
                    consume_tps: group.consume_tps,
                    diff_total: group.diff_total,
                })
                .collect::<Vec<_>>(),
            ConsumerProgressResult::Group(_) => Vec::new(),
        };

        Ok(consumer_tools::ListConsumerGroupsOutput {
            cluster: cluster.name,
            namesrv_addr: cluster.namesrv_addr,
            consumer_group_count: groups.len(),
            groups,
            generated_at: generated_at(),
        })
    }

    async fn query_consumer_lag(
        &self,
        args: consumer_tools::QueryConsumerLagArgs,
    ) -> Result<consumer_tools::QueryConsumerLagOutput, ToolExecutionError> {
        let cluster = self.resolve_cluster(Some(&args.cluster))?;
        let result = ConsumerService::query_consumer_progress_by_request_with_rpc_hook(
            ConsumerProgressRequest::try_new(
                Some(args.consumer_group.clone()),
                Some(args.topic.clone()),
                true,
                Some(cluster.name.clone()),
                Some(cluster.namesrv_addr.clone()),
            )
            .map_err(admin_error)?,
            None,
        )
        .await
        .map_err(admin_error)?;

        let ConsumerProgressResult::Group(progress) = result else {
            return Err(ToolExecutionError::Backend(
                "consumer progress query did not return group result".to_string(),
            ));
        };

        let queues = progress
            .rows
            .iter()
            .map(|row| consumer_tools::QueueLag {
                topic: row.topic.to_string(),
                broker_name: row.broker_name.to_string(),
                queue_id: row.queue_id,
                broker_offset: row.broker_offset,
                consumer_offset: row.consumer_offset,
                lag: row.diff,
                inflight: row.inflight,
                last_timestamp: row.last_timestamp,
                client_ip: row.client_ip.clone(),
            })
            .collect::<Vec<_>>();
        let max_queue_lag = queues.iter().map(|queue| queue.lag).max().unwrap_or_default();

        Ok(consumer_tools::QueryConsumerLagOutput {
            cluster: cluster.name,
            namesrv_addr: cluster.namesrv_addr,
            topic: args.topic,
            consumer_group: args.consumer_group,
            total_lag: progress.diff_total,
            max_queue_lag,
            queue_count: queues.len(),
            consume_tps: progress.consume_tps,
            inflight_total: progress.inflight_total,
            queues,
            generated_at: generated_at(),
        })
    }

    async fn describe_broker(
        &self,
        args: broker_tools::DescribeBrokerArgs,
    ) -> Result<broker_tools::DescribeBrokerOutput, ToolExecutionError> {
        let cluster = self.resolve_cluster(Some(&args.cluster))?;
        let cluster_result = ClusterService::query_cluster_list_by_request_with_rpc_hook(
            ClusterListQueryRequest::new(false, Some(cluster.name.clone()))
                .with_optional_namesrv_addr(Some(cluster.namesrv_addr.clone())),
            None,
        )
        .await
        .map_err(admin_error)?;
        let brokers = cluster_result
            .base_rows
            .iter()
            .filter(|row| row.broker_name == args.broker_name)
            .map(map_broker_summary)
            .collect::<Vec<_>>();

        if brokers.is_empty() {
            let _ = BrokerService::query_broker_runtime_stats_by_request_with_rpc_hook(
                BrokerRuntimeStatsQueryRequest::try_new(None, Some(cluster.name.clone())).map_err(admin_error)?,
                None,
            )
            .await;
            return Err(ToolExecutionError::Backend(format!(
                "broker not found in cluster {}: {}",
                cluster.name, args.broker_name
            )));
        }

        Ok(broker_tools::DescribeBrokerOutput {
            cluster: cluster.name,
            namesrv_addr: cluster.namesrv_addr,
            broker_name: args.broker_name,
            brokers,
            generated_at: generated_at(),
        })
    }
}

#[derive(Debug, Clone)]
struct ResolvedCluster {
    name: String,
    namesrv_addr: String,
}

fn default_cluster(clusters: &[ClusterConfig]) -> Result<&ClusterConfig, ToolExecutionError> {
    clusters
        .iter()
        .find(|cluster| cluster.default.unwrap_or(false))
        .or_else(|| (clusters.len() == 1).then(|| &clusters[0]))
        .ok_or_else(|| ToolExecutionError::InvalidArguments("cluster must be provided".to_string()))
}

fn admin_error(error: RocketMQError) -> ToolExecutionError {
    ToolExecutionError::backend(error)
}

fn generated_at() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().to_string())
        .unwrap_or_else(|_| "0".to_string())
}

fn map_broker_summary(row: &ClusterBaseInfoRow) -> cluster_tools::BrokerSummary {
    cluster_tools::BrokerSummary {
        cluster: row.cluster_name.clone(),
        broker_name: row.broker_name.clone(),
        broker_id: row.broker_id,
        broker_addr: row.broker_addr.to_string(),
        version: row.version.clone(),
        in_tps: row.in_tps.clone(),
        out_tps: row.out_tps.clone(),
        timer_progress: row.timer_progress.clone(),
        page_cache_lock_time_millis: row.page_cache_lock_time_millis.clone(),
        hour: row.hour.clone(),
        space: row.space.clone(),
        broker_active: row.broker_active,
    }
}

fn map_topic_route(
    cluster: ResolvedCluster,
    topic: String,
    route: TopicRouteData,
) -> topic_tools::QueryTopicRouteOutput {
    let mut brokers = route
        .broker_datas
        .iter()
        .map(|broker| {
            let broker_addrs = broker
                .broker_addrs()
                .iter()
                .map(|(broker_id, broker_addr)| (broker_id.to_string(), broker_addr.to_string()))
                .collect::<BTreeMap<_, _>>();
            topic_tools::TopicRouteBroker {
                cluster: broker.cluster().to_string(),
                broker_name: broker.broker_name().to_string(),
                broker_addrs,
                zone_name: broker.zone_name().map(ToString::to_string),
                enable_acting_master: broker.enable_acting_master(),
            }
        })
        .collect::<Vec<_>>();
    brokers.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));

    let mut queues = route
        .queue_datas
        .iter()
        .map(|queue| topic_tools::TopicRouteQueue {
            broker_name: queue.broker_name().to_string(),
            read_queue_nums: queue.read_queue_nums(),
            write_queue_nums: queue.write_queue_nums(),
            perm: queue.perm(),
            topic_sys_flag: queue.topic_sys_flag(),
        })
        .collect::<Vec<_>>();
    queues.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));

    topic_tools::QueryTopicRouteOutput {
        cluster: cluster.name,
        namesrv_addr: cluster.namesrv_addr,
        topic,
        brokers,
        queues,
        generated_at: generated_at(),
    }
}
