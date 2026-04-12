// Copyright 2023 The RocketMQ Rust Authors
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

//! Topic operations - Core business logic
//!
//! This module contains reusable topic management operations that can be
//! used by CLI, API, or any other interface.

use std::collections::HashMap;
use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::config::TopicConfig as RocketMQTopicConfig;
use rocketmq_common::common::mix_all::DLQ_GROUP_TOPIC_PREFIX;
use rocketmq_common::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;

use super::types::AllocateMqQueryRequest;
use super::types::AllocatedMqQueryResult;
use super::types::DeleteTopicRequest;
use super::types::DeleteTopicResult;
use super::types::OrderConfMethod;
use super::types::OrderConfRequest;
use super::types::OrderConfResult;
use super::types::TopicClusterList;
use super::types::TopicClusterQueryRequest;
use super::types::TopicListItem;
use super::types::TopicListQueryRequest;
use super::types::TopicListResult;
use super::types::TopicRouteQueryRequest;
use super::types::TopicStatusQueryRequest;
use super::types::UpdateTopicListRequest;
use super::types::UpdateTopicListResult;
use super::types::UpdateTopicPermRequest;
use super::types::UpdateTopicPermResult;
use super::types::UpdateTopicRequest;
use super::types::UpdateTopicResult;
use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::resolver::BrokerAddressResolver;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

/// Topic operations service
pub struct TopicService;

/// Alias for TopicService for compatibility
pub type TopicOperations = TopicService;

impl TopicService {
    /// Get cluster list for a given topic
    ///
    /// # Arguments
    /// * `admin` - Admin client instance
    /// * `topic` - Topic name
    ///
    /// # Returns
    /// Set of cluster names containing the topic
    pub async fn get_topic_cluster_list(
        admin: &mut DefaultMQAdminExt,
        topic: impl Into<String>,
    ) -> RocketMQResult<TopicClusterList> {
        let topic = topic.into();
        let clusters = admin
            .get_topic_cluster_list(topic.clone())
            .await
            .map_err(|_| ToolsError::topic_not_found(topic.clone()))?;

        Ok(TopicClusterList { clusters })
    }

    /// Query topic clusters through a complete core request lifecycle.
    ///
    /// The caller supplies presentation-independent request data. The service
    /// owns the admin client lifecycle and returns a DTO that UI layers render.
    pub async fn query_topic_clusters(request: TopicClusterQueryRequest) -> RocketMQResult<TopicClusterList> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = Self::get_topic_cluster_list(&mut admin, request.topic().clone()).await;
        admin.shutdown().await;
        result
    }

    /// Query all topics, optionally filtered by cluster, through a complete core request lifecycle.
    pub async fn query_topic_list(request: TopicListQueryRequest) -> RocketMQResult<TopicListResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = Self::query_topic_list_with_admin(&mut admin, &request).await;
        admin.shutdown().await;
        result
    }

    /// Query topic route through a complete core request lifecycle.
    pub async fn query_topic_route(
        request: TopicRouteQueryRequest,
    ) -> RocketMQResult<Option<rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData>> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = Self::get_topic_route(&mut admin, request.topic().clone()).await;
        admin.shutdown().await;
        result
    }

    /// Query topic status through a complete core request lifecycle.
    pub async fn query_topic_status(
        request: TopicStatusQueryRequest,
    ) -> RocketMQResult<rocketmq_remoting::protocol::admin::topic_stats_table::TopicStatsTable> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let topic = request.topic().clone();
        let result = if let Some(cluster) = request.cluster_name() {
            let topic_route_data = admin.examine_topic_route_info(cluster.clone()).await?;
            let mut topic_stats_table = rocketmq_remoting::protocol::admin::topic_stats_table::TopicStatsTable::new();
            if let Some(route_data) = &topic_route_data {
                let mut total_offset_table = HashMap::new();
                for broker_data in &route_data.broker_datas {
                    let addr = broker_data.select_broker_addr();
                    let offset_table = admin
                        .examine_topic_stats(topic.clone(), addr)
                        .await?
                        .into_offset_table();
                    total_offset_table.extend(offset_table);
                }
                topic_stats_table.set_offset_table(total_offset_table);
            }
            Ok(topic_stats_table)
        } else {
            admin.examine_topic_stats(topic, None).await
        };
        admin.shutdown().await;
        result
    }

    /// Delete a topic through a complete core request lifecycle.
    pub async fn delete_topic_by_request(request: DeleteTopicRequest) -> RocketMQResult<DeleteTopicResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = Self::delete_topic(&mut admin, request.topic().clone(), request.cluster_name().clone()).await;
        admin.shutdown().await;
        result.map(|_| DeleteTopicResult {
            topic: request.topic().clone(),
            cluster_name: request.cluster_name().clone(),
        })
    }

    /// Apply order configuration through a complete core request lifecycle.
    pub async fn apply_order_conf(request: OrderConfRequest) -> RocketMQResult<OrderConfResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = match request.method() {
            OrderConfMethod::Put => {
                let order_conf = CheetahString::from(request.order_conf().unwrap_or_default());
                Self::create_or_update_order_conf(&mut admin, request.topic().clone(), order_conf.clone())
                    .await
                    .map(|_| OrderConfResult {
                        topic: request.topic().clone(),
                        method: request.method(),
                        order_conf: Some(order_conf),
                    })
            }
            OrderConfMethod::Get => Self::get_order_conf(&mut admin, request.topic().clone())
                .await
                .map(|order_conf| OrderConfResult {
                    topic: request.topic().clone(),
                    method: request.method(),
                    order_conf: Some(order_conf),
                }),
            OrderConfMethod::Delete => Self::delete_order_conf(&mut admin, request.topic().clone())
                .await
                .map(|_| OrderConfResult {
                    topic: request.topic().clone(),
                    method: request.method(),
                    order_conf: None,
                }),
        };
        admin.shutdown().await;
        result
    }

    /// Query message queue allocation through a complete core request lifecycle.
    pub async fn query_allocated_mq_by_request(
        request: AllocateMqQueryRequest,
    ) -> RocketMQResult<AllocatedMqQueryResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = Self::query_allocated_mq(&mut admin, request.topic().clone(), request.ip_list().clone()).await;
        admin.shutdown().await;
        result
    }

    /// Create or update a topic through a complete core request lifecycle.
    pub async fn create_or_update_topic_by_request(request: UpdateTopicRequest) -> RocketMQResult<UpdateTopicResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let config = request.config().clone();
        let target = request.target().clone();
        let result = Self::create_or_update_topic(&mut admin, config.clone(), target.clone())
            .await
            .map(|_| UpdateTopicResult {
                order_warning: config.order,
                config,
                target,
            });
        admin.shutdown().await;
        result
    }

    /// Apply a batch of topic configs through a complete core request lifecycle.
    pub async fn update_topic_config_list_by_request(
        request: UpdateTopicListRequest,
    ) -> RocketMQResult<UpdateTopicListResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let target = request.target().clone();
        let topic_configs = request.topic_configs().to_vec();
        let result = Self::update_topic_config_list(&mut admin, target, topic_configs).await;
        admin.shutdown().await;
        result
    }

    /// Update topic permission through a complete core request lifecycle.
    pub async fn update_topic_perm_by_request(
        request: UpdateTopicPermRequest,
    ) -> RocketMQResult<UpdateTopicPermResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = Self::update_topic_perm(
            &mut admin,
            request.topic().clone(),
            request.perm(),
            request.target().clone(),
        )
        .await
        .map(|_| UpdateTopicPermResult {
            topic: request.topic().clone(),
            target: request.target().clone(),
            perm: request.perm(),
        });
        admin.shutdown().await;
        result
    }

    /// Get topic route information
    ///
    /// # Arguments
    /// * `admin` - Admin client instance
    /// * `topic` - Topic name
    ///
    /// # Returns
    /// Topic route data including broker and queue information
    pub async fn get_topic_route(
        admin: &mut DefaultMQAdminExt,
        topic: impl Into<CheetahString>,
    ) -> RocketMQResult<Option<rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData>> {
        let topic = topic.into();
        Ok(admin
            .examine_topic_route_info(topic.clone())
            .await
            .map_err(|_| ToolsError::topic_not_found(topic.to_string()))?)
    }

    /// Delete a topic from cluster
    ///
    /// # Arguments
    /// * `admin` - Admin client instance
    /// * `topic` - Topic name
    /// * `cluster_name` - Cluster name (optional)
    ///
    /// # Returns
    /// Result indicating success or failure
    pub async fn delete_topic(
        admin: &mut DefaultMQAdminExt,
        topic: impl Into<CheetahString>,
        cluster_name: impl Into<CheetahString>,
    ) -> RocketMQResult<()> {
        let topic = topic.into();
        let cluster = cluster_name.into();

        Ok(admin.delete_topic(topic.clone(), cluster.clone()).await.map_err(|e| {
            ToolsError::internal(format!(
                "Failed to delete topic '{topic}' from cluster '{cluster}': {e}"
            ))
        })?)
    }

    /// Create or update a topic configuration
    ///
    /// # Arguments
    /// * `admin` - Admin client instance
    /// * `config` - Topic configuration
    /// * `target` - Target broker address or cluster name
    ///
    /// # Returns
    /// Result indicating success or failure
    pub async fn create_or_update_topic(
        admin: &mut DefaultMQAdminExt,
        config: super::types::TopicConfig,
        target: super::types::TopicTarget,
    ) -> RocketMQResult<()> {
        use rocketmq_common::common::TopicFilterType;

        // Convert to internal TopicConfig
        let internal_config = RocketMQTopicConfig {
            topic_name: Some(config.topic_name.clone()),
            read_queue_nums: config.read_queue_nums as u32,
            write_queue_nums: config.write_queue_nums as u32,
            perm: config.perm as u32,
            topic_filter_type: config
                .topic_filter_type
                .map(|s| TopicFilterType::from(s.as_str()))
                .unwrap_or_default(),
            topic_sys_flag: config.topic_sys_flag.unwrap_or(0) as u32,
            order: config.order,
            attributes: std::collections::HashMap::new(),
        };

        match target {
            super::types::TopicTarget::Broker(addr) => admin
                .create_and_update_topic_config(addr, internal_config)
                .await
                .map_err(|e| ToolsError::internal(format!("Failed to create/update topic: {e}")).into()),
            super::types::TopicTarget::Cluster(cluster_name) => {
                // Get all master brokers in cluster
                let cluster_info = admin
                    .examine_broker_cluster_info()
                    .await
                    .map_err(|e| ToolsError::internal(format!("Failed to get cluster info: {e}")))?;

                // Find master brokers in the cluster
                let master_addrs =
                    BrokerAddressResolver::fetch_master_addr_by_cluster_name(&cluster_info, &cluster_name)?;

                if master_addrs.is_empty() {
                    return Err(ToolsError::ClusterNotFound {
                        cluster: cluster_name.to_string(),
                    }
                    .into());
                }

                // Create topic on all master brokers
                for addr in master_addrs {
                    admin
                        .create_and_update_topic_config(addr, internal_config.clone())
                        .await
                        .map_err(|e| ToolsError::internal(format!("Failed to create/update topic: {e}")))?;
                }

                Ok(())
            }
        }
    }

    /// Apply a batch of topic configs to one broker or every master broker in a cluster.
    pub async fn update_topic_config_list(
        admin: &mut DefaultMQAdminExt,
        target: super::types::TopicTarget,
        topic_configs: Vec<RocketMQTopicConfig>,
    ) -> RocketMQResult<UpdateTopicListResult> {
        if topic_configs.is_empty() {
            return Err(ToolsError::validation_error("topicConfigs", "topicConfigs must not be empty").into());
        }

        let broker_addrs = match &target {
            super::types::TopicTarget::Broker(broker_addr) => vec![broker_addr.clone()],
            super::types::TopicTarget::Cluster(cluster_name) => {
                let cluster_info = admin
                    .examine_broker_cluster_info()
                    .await
                    .map_err(|e| ToolsError::internal(format!("Failed to get cluster info: {e}")))?;
                let master_addrs =
                    BrokerAddressResolver::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name)?;
                if master_addrs.is_empty() {
                    return Err(ToolsError::ClusterNotFound {
                        cluster: cluster_name.to_string(),
                    }
                    .into());
                }
                master_addrs
            }
        };

        for broker_addr in &broker_addrs {
            admin
                .create_and_update_topic_config_list(broker_addr.clone(), topic_configs.clone())
                .await
                .map_err(|e| {
                    ToolsError::internal(format!(
                        "Failed to submit topic config list to broker '{broker_addr}': {e}"
                    ))
                })?;
        }

        Ok(UpdateTopicListResult { target, broker_addrs })
    }

    /// Batch get cluster lists for multiple topics
    ///
    /// # Arguments
    /// * `admin` - Admin client instance
    /// * `topics` - List of topic names
    ///
    /// # Returns
    /// Map of topic name to cluster list
    pub async fn batch_get_topic_clusters(
        admin: &mut DefaultMQAdminExt,
        topics: Vec<String>,
    ) -> RocketMQResult<std::collections::HashMap<String, HashSet<CheetahString>>> {
        use futures::future::join_all;

        let futures = topics.iter().map(|topic| admin.get_topic_cluster_list(topic.clone()));

        let results = join_all(futures).await;

        let map = topics
            .into_iter()
            .zip(results)
            .filter_map(|(topic, result)| match result {
                Ok(clusters) => Some((topic, clusters)),
                Err(e) => {
                    tracing::warn!("Failed to get clusters for topic {topic}: {e}");
                    None
                }
            })
            .collect();

        Ok(map)
    }

    /// List all topics
    ///
    /// # Arguments
    /// * `admin` - Admin client instance
    ///
    /// # Returns
    /// Set of all topic names
    pub async fn list_all_topics(admin: &mut DefaultMQAdminExt) -> RocketMQResult<HashSet<CheetahString>> {
        let topic_list = admin
            .fetch_all_topic_list()
            .await
            .map_err(|e| ToolsError::internal(format!("Failed to fetch topic list: {e}")))?;

        Ok(topic_list.topic_list.into_iter().collect())
    }

    async fn query_topic_list_with_admin(
        admin: &mut DefaultMQAdminExt,
        request: &TopicListQueryRequest,
    ) -> RocketMQResult<TopicListResult> {
        let topic_list = admin
            .fetch_all_topic_list()
            .await
            .map_err(|e| ToolsError::internal(format!("Failed to fetch topic list: {e}")))?;

        let Some(cluster_name) = request.cluster_name() else {
            return Ok(TopicListResult {
                topics: topic_list
                    .topic_list
                    .into_iter()
                    .map(|topic| TopicListItem {
                        topic,
                        cluster: None,
                        consumer_group: None,
                    })
                    .collect(),
            });
        };

        let cluster_info = admin
            .examine_broker_cluster_info()
            .await
            .map_err(|e| ToolsError::internal(format!("Failed to get cluster info: {e}")))?;

        let mut topics = Vec::new();
        for topic in topic_list.topic_list {
            if topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) || topic.starts_with(DLQ_GROUP_TOPIC_PREFIX) {
                continue;
            }

            let route = match admin.examine_topic_route_info(topic.clone()).await? {
                Some(route) => route,
                None => continue,
            };
            if !Self::topic_route_belongs_to_cluster(&route, &cluster_info, cluster_name) {
                continue;
            }

            let group_list = admin.query_topic_consume_by_who(topic.clone()).await?;
            if group_list.get_group_list().is_empty() {
                topics.push(TopicListItem {
                    topic,
                    cluster: Some(cluster_name.clone()),
                    consumer_group: None,
                });
            } else {
                topics.extend(group_list.get_group_list().iter().map(|group| TopicListItem {
                    topic: topic.clone(),
                    cluster: Some(cluster_name.clone()),
                    consumer_group: Some(group.clone()),
                }));
            }
        }

        Ok(TopicListResult { topics })
    }

    fn topic_route_belongs_to_cluster(
        route: &rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData,
        cluster_info: &rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo,
        cluster_name: &CheetahString,
    ) -> bool {
        let Some(cluster_table) = cluster_info.cluster_addr_table.as_ref() else {
            return false;
        };
        let Some(cluster_brokers) = cluster_table.get(cluster_name) else {
            return false;
        };

        route
            .broker_datas
            .iter()
            .any(|broker| cluster_brokers.contains(broker.broker_name()))
    }

    /// Get topic statistics
    ///
    /// # Arguments
    /// * `admin` - Admin client instance
    /// * `topic` - Topic name
    /// * `broker_addr` - Optional broker address
    ///
    /// # Returns
    /// Topic statistics table
    pub async fn get_topic_stats(
        admin: &mut DefaultMQAdminExt,
        topic: impl Into<CheetahString>,
        broker_addr: Option<CheetahString>,
    ) -> RocketMQResult<rocketmq_remoting::protocol::admin::topic_stats_table::TopicStatsTable> {
        admin
            .examine_topic_stats(topic.into(), broker_addr)
            .await
            .map_err(|e| ToolsError::internal(format!("Failed to get topic stats: {e}")).into())
    }

    /// Update topic permission
    ///
    /// # Arguments
    /// * `admin` - Admin client instance
    /// * `topic` - Topic name
    /// * `perm` - New permission value (2=W, 4=R, 6=RW)
    /// * `target` - Target broker or cluster
    ///
    /// # Returns
    /// Result indicating success or failure
    pub async fn update_topic_perm(
        admin: &mut DefaultMQAdminExt,
        topic: impl Into<CheetahString>,
        perm: i32,
        target: super::types::TopicTarget,
    ) -> RocketMQResult<()> {
        use rocketmq_common::common::config::TopicConfig as RocketMQTopicConfig;

        let topic = topic.into();

        match target {
            super::types::TopicTarget::Broker(broker_addr) => {
                // Get existing config
                let topic_config = admin
                    .examine_topic_config(broker_addr.clone(), topic.clone())
                    .await
                    .map_err(|e| ToolsError::internal(format!("Failed to get topic config: {e}")))?;

                // Update permission
                let updated_config = RocketMQTopicConfig {
                    topic_name: Some(topic.clone()),
                    read_queue_nums: topic_config.read_queue_nums,
                    write_queue_nums: topic_config.write_queue_nums,
                    perm: perm as u32,
                    topic_filter_type: topic_config.topic_filter_type,
                    topic_sys_flag: topic_config.topic_sys_flag,
                    order: topic_config.order,
                    attributes: std::collections::HashMap::new(),
                };

                admin
                    .create_and_update_topic_config(broker_addr, updated_config)
                    .await
                    .map_err(|e| ToolsError::internal(format!("Failed to update topic permission: {e}")))?;

                Ok(())
            }
            super::types::TopicTarget::Cluster(cluster_name) => {
                // Get cluster info
                let cluster_info = admin
                    .examine_broker_cluster_info()
                    .await
                    .map_err(|e| ToolsError::internal(format!("Failed to get cluster info: {e}")))?;

                // Find master brokers
                let master_addrs =
                    BrokerAddressResolver::fetch_master_addr_by_cluster_name(&cluster_info, &cluster_name)?;

                if master_addrs.is_empty() {
                    return Err(ToolsError::ClusterNotFound {
                        cluster: cluster_name.to_string(),
                    }
                    .into());
                }

                // Update on all master brokers
                for broker_addr in master_addrs {
                    let topic_config = admin
                        .examine_topic_config(broker_addr.clone(), topic.clone())
                        .await
                        .map_err(|e| ToolsError::internal(format!("Failed to get topic config: {e}")))?;

                    let updated_config = RocketMQTopicConfig {
                        topic_name: Some(topic.clone()),
                        read_queue_nums: topic_config.read_queue_nums,
                        write_queue_nums: topic_config.write_queue_nums,
                        perm: perm as u32,
                        topic_filter_type: topic_config.topic_filter_type,
                        topic_sys_flag: topic_config.topic_sys_flag,
                        order: topic_config.order,
                        attributes: std::collections::HashMap::new(),
                    };

                    admin
                        .create_and_update_topic_config(broker_addr, updated_config)
                        .await
                        .map_err(|e| ToolsError::internal(format!("Failed to update topic permission: {e}")))?;
                }

                Ok(())
            }
        }
    }

    /// Query message queues allocated for a topic on specific IPs
    ///
    /// # Arguments
    /// * `admin` - Admin client instance
    /// * `topic` - Topic name
    /// * `ip_list` - Comma-separated IP addresses
    ///
    /// # Returns
    /// Allocation summary for CLI/TUI rendering
    pub async fn query_allocated_mq(
        admin: &mut DefaultMQAdminExt,
        topic: impl Into<CheetahString>,
        ip_list: impl Into<CheetahString>,
    ) -> RocketMQResult<AllocatedMqQueryResult> {
        let topic = topic.into();
        let ip_list = ip_list.into();

        let requested_ips = ip_list
            .split(',')
            .map(str::trim)
            .filter(|ip| !ip.is_empty())
            .map(CheetahString::from)
            .collect::<Vec<_>>();
        let route_opt = admin.examine_topic_route_info(topic.clone()).await?;

        Ok(match route_opt {
            Some(route) => AllocatedMqQueryResult {
                topic,
                requested_ips,
                route_found: true,
                total_queues: route.queue_datas.len(),
                broker_names: route
                    .broker_datas
                    .iter()
                    .map(|broker| broker.broker_name().clone())
                    .collect(),
            },
            None => AllocatedMqQueryResult {
                topic,
                requested_ips,
                route_found: false,
                total_queues: 0,
                broker_names: Vec::new(),
            },
        })
    }

    /// Create or update order configuration
    ///
    /// # Arguments
    /// * `admin` - Admin client instance
    /// * `topic` - Topic name
    /// * `order_conf` - Order configuration (e.g., "broker-a:4;broker-b:4")
    ///
    /// # Returns
    /// Result indicating success or failure
    pub async fn create_or_update_order_conf(
        admin: &mut DefaultMQAdminExt,
        topic: impl Into<CheetahString>,
        order_conf: impl Into<CheetahString>,
    ) -> RocketMQResult<()> {
        const NAMESPACE: &str = "ORDER_TOPIC_CONFIG";
        admin
            .create_and_update_kv_config(
                CheetahString::from_static_str(NAMESPACE),
                topic.into(),
                order_conf.into(),
            )
            .await
            .map_err(|e| ToolsError::internal(format!("Failed to update order config: {e}")).into())
    }

    /// Get order configuration
    ///
    /// # Arguments
    /// * `admin` - Admin client instance
    /// * `topic` - Topic name
    ///
    /// # Returns
    /// Order configuration string
    pub async fn get_order_conf(
        admin: &mut DefaultMQAdminExt,
        topic: impl Into<CheetahString>,
    ) -> RocketMQResult<CheetahString> {
        const NAMESPACE: &str = "ORDER_TOPIC_CONFIG";
        admin
            .get_kv_config(CheetahString::from_static_str(NAMESPACE), topic.into())
            .await
            .map_err(|e| ToolsError::internal(format!("Failed to get order config: {e}")).into())
    }

    /// Delete order configuration
    ///
    /// # Arguments
    /// * `admin` - Admin client instance
    /// * `topic` - Topic name
    ///
    /// # Returns
    /// Result indicating success or failure
    pub async fn delete_order_conf(
        admin: &mut DefaultMQAdminExt,
        topic: impl Into<CheetahString>,
    ) -> RocketMQResult<()> {
        const NAMESPACE: &str = "ORDER_TOPIC_CONFIG";
        admin
            .delete_kv_config(CheetahString::from_static_str(NAMESPACE), topic.into())
            .await
            .map_err(|e| ToolsError::internal(format!("Failed to delete order config: {e}")).into())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_topic_config_creation() {
        // Test topic config structure
        let _config = super::super::types::TopicConfig {
            topic_name: "test_topic".into(),
            read_queue_nums: 8,
            write_queue_nums: 8,
            perm: 6,
            topic_filter_type: None,
            topic_sys_flag: None,
            order: false,
        };
    }
}
