/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Topic operations - Core business logic
//!
//! This module contains reusable topic management operations that can be
//! used by CLI, API, or any other interface.

use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;

use super::types::TopicClusterList;
use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::RocketMQError;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

/// Topic operations service
pub struct TopicService;

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
            .map_err(|_| RocketMQError::Tools(ToolsError::topic_not_found(topic.clone())))?;

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
    ) -> RocketMQResult<Option<rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData>>
    {
        let topic = topic.into();
        admin
            .examine_topic_route_info(topic.clone())
            .await
            .map_err(|_| RocketMQError::Tools(ToolsError::topic_not_found(topic.to_string())))
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

        admin
            .delete_topic(topic.clone(), cluster.clone())
            .await
            .map_err(|e| {
                RocketMQError::Tools(ToolsError::internal(format!(
                    "Failed to delete topic '{topic}' from cluster '{cluster}': {e}"
                )))
            })
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

        let futures = topics
            .iter()
            .map(|topic| admin.get_topic_cluster_list(topic.clone()));

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
