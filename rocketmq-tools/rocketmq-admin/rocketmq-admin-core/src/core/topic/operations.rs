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

use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;

use super::types::TopicClusterList;
use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
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
        use rocketmq_common::common::config::TopicConfig as RocketMQTopicConfig;
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
                let master_addrs = crate::commands::command_util::CommandUtil::fetch_master_addr_by_cluster_name(
                    &cluster_info,
                    &cluster_name,
                )?;

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
                let master_addrs = crate::commands::command_util::CommandUtil::fetch_master_addr_by_cluster_name(
                    &cluster_info,
                    &cluster_name,
                )?;

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
    /// Result indicating success or failure
    pub async fn query_allocated_mq(
        admin: &mut DefaultMQAdminExt,
        topic: impl Into<CheetahString>,
        ip_list: impl Into<CheetahString>,
    ) -> RocketMQResult<()> {
        let topic = topic.into();
        let ip_list = ip_list.into();

        // Parse IP list
        let ips: Vec<_> = ip_list.split(',').map(|s| s.trim()).collect();

        // Get topic route
        let route_opt = admin.examine_topic_route_info(topic.clone()).await?;

        if let Some(route) = route_opt {
            // Build a simple allocation overview
            println!("Topic: {topic}");
            println!("IP List: {}", ips.join(", "));
            println!("\nMessage Queue Allocation:");
            println!("Total Queues: {}", route.queue_datas.len());
            println!("\nBrokers:");
            for broker in &route.broker_datas {
                println!("  - {}", broker.broker_name());
            }
        } else {
            println!("No route information found for topic: {topic}");
        }

        Ok(())
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
