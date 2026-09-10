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
use rocketmq_client_rust::{RouteAdmin as _, TopicAdmin as _};
use rocketmq_model::common::config::TopicConfig as RocketMQTopicConfig;
use rocketmq_model::common::mix_all::DLQ_GROUP_TOPIC_PREFIX;
use rocketmq_model::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_protocol::protocol::route_facade::BrokerDataExt;

use super::types::AllocateMqQueryRequest;
use super::types::AllocatedMqQueryResult;
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
use crate::client_adapter::services::admin::AdminBuilder;
use crate::client_adapter::services::resolver::BrokerAddressResolver;
use rocketmq_client_rust::DefaultMQAdminExt;
use rocketmq_error::Result as CanonicalResult;

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
    ) -> CanonicalResult<TopicClusterList> {
        let topic = topic.into();
        let clusters = admin
            .get_topic_cluster_list(topic.clone())
            .await
            .map_err(|_| crate::client_adapter::services::errors::topic_route_not_found(topic.clone()))?;

        Ok(TopicClusterList { clusters })
    }

    /// Query topic clusters through a complete core request lifecycle.
    ///
    /// The caller supplies presentation-independent request data. The service
    /// owns the admin client lifecycle and returns a DTO that UI layers render.
    pub async fn query_topic_clusters(request: TopicClusterQueryRequest) -> CanonicalResult<TopicClusterList> {
        let mut admin = request
            .admin_builder()
            .build_and_start()
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)?;
        let result = Self::get_topic_cluster_list(&mut admin, request.topic().clone()).await;
        admin.shutdown().await;
        result
    }

    /// Query the clusters of a topic using the caller-owned runtime and optional credentials.
    pub async fn query_topic_clusters_by_request_with_credentials(
        request: TopicClusterQueryRequest,
        credentials: Option<crate::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_client_rust::ClientRuntime>,
    ) -> CanonicalResult<TopicClusterList> {
        let mut admin = admin_builder_with_credentials(request.admin_builder(), credentials, client_runtime)
            .build_and_start()
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)?;
        let result = Self::get_topic_cluster_list(&mut admin, request.topic().clone()).await;
        admin.shutdown().await;
        result
    }

    /// Query all topics, optionally filtered by cluster, through a complete core request lifecycle.
    pub async fn query_topic_list(request: TopicListQueryRequest) -> CanonicalResult<TopicListResult> {
        let mut admin = request
            .admin_builder()
            .build_and_start()
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)?;
        let result = Self::query_topic_list_with_admin(&mut admin, &request).await;
        admin.shutdown().await;
        result
    }

    /// Query all topics using the caller-owned runtime and optional credentials.
    ///
    /// This application-facing variant keeps runtime ownership explicit and applies
    /// the same authentication hook as the other CLI-backed topic operations.
    pub async fn query_topic_list_by_request_with_credentials(
        request: TopicListQueryRequest,
        credentials: Option<crate::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_client_rust::ClientRuntime>,
    ) -> CanonicalResult<TopicListResult> {
        let mut admin = admin_builder_with_credentials(request.admin_builder(), credentials, client_runtime)
            .build_and_start()
            .await?;
        let result = Self::query_topic_list_with_admin(&mut admin, &request).await;
        admin.shutdown().await;
        result
    }

    /// Query topic route through a complete core request lifecycle.
    pub async fn query_topic_route(
        request: TopicRouteQueryRequest,
    ) -> CanonicalResult<Option<rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData>> {
        let mut admin = request
            .admin_builder()
            .build_and_start()
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)?;
        let result = Self::get_topic_route(&mut admin, request.topic().clone()).await;
        admin.shutdown().await;
        result
    }

    /// Query a topic route using the caller-owned runtime and optional credentials.
    ///
    /// This application-facing variant keeps runtime ownership explicit and applies
    /// the same authentication hook as the other CLI-backed topic operations.
    pub async fn query_topic_route_by_request_with_credentials(
        request: TopicRouteQueryRequest,
        credentials: Option<crate::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_client_rust::ClientRuntime>,
    ) -> CanonicalResult<Option<rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData>> {
        let mut admin = admin_builder_with_credentials(request.admin_builder(), credentials, client_runtime)
            .build_and_start()
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)?;
        let result = Self::get_topic_route(&mut admin, request.topic().clone()).await;
        admin.shutdown().await;
        result
    }

    /// Query topic status through a complete core request lifecycle.
    pub async fn query_topic_status(
        request: TopicStatusQueryRequest,
    ) -> CanonicalResult<rocketmq_protocol::protocol::admin::topic_stats_table::TopicStatsTable> {
        let mut admin = request
            .admin_builder()
            .build_and_start()
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)?;
        let result = Self::query_topic_status_with_admin(&mut admin, &request).await;
        admin.shutdown().await;
        result
    }

    /// Query topic status using the caller-owned runtime and optional credentials.
    pub async fn query_topic_status_by_request_with_credentials(
        request: TopicStatusQueryRequest,
        credentials: Option<crate::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_client_rust::ClientRuntime>,
    ) -> CanonicalResult<rocketmq_protocol::protocol::admin::topic_stats_table::TopicStatsTable> {
        let mut admin = admin_builder_with_credentials(request.admin_builder(), credentials, client_runtime)
            .build_and_start()
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)?;
        let result = Self::query_topic_status_with_admin(&mut admin, &request).await;
        admin.shutdown().await;
        result
    }

    async fn query_topic_status_with_admin(
        admin: &mut DefaultMQAdminExt,
        request: &TopicStatusQueryRequest,
    ) -> CanonicalResult<rocketmq_protocol::protocol::admin::topic_stats_table::TopicStatsTable> {
        let topic = request.topic().clone();
        if let Some(cluster) = request.cluster_name() {
            let topic_route_data = admin
                .examine_topic_route_info(cluster.clone())
                .await
                .map_err(crate::IntoCanonicalError::into_canonical_error)?;
            let mut topic_stats_table = rocketmq_protocol::protocol::admin::topic_stats_table::TopicStatsTable::new();
            if let Some(route_data) = &topic_route_data {
                let mut total_offset_table = HashMap::new();
                let mut topic_put_tps = 0.0;
                for broker_data in &route_data.broker_datas {
                    let addr = broker_data.select_broker_addr();
                    let stats = admin
                        .examine_topic_stats(topic.clone(), addr)
                        .await
                        .map_err(crate::IntoCanonicalError::into_canonical_error)?;
                    topic_put_tps += stats.get_topic_put_tps();
                    total_offset_table.extend(stats.into_offset_table());
                }
                topic_stats_table.set_offset_table(total_offset_table);
                topic_stats_table.set_topic_put_tps(topic_put_tps);
            }
            Ok(topic_stats_table)
        } else {
            admin
                .examine_topic_stats(topic, None)
                .await
                .map_err(crate::IntoCanonicalError::into_canonical_error)
        }
    }

    /// Apply order configuration through a complete core request lifecycle.
    pub async fn apply_order_conf(request: OrderConfRequest) -> CanonicalResult<OrderConfResult> {
        let mut admin = request
            .admin_builder()
            .build_and_start()
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)?;
        let result = Self::apply_order_conf_with_admin(&mut admin, &request).await;
        admin.shutdown().await;
        result
    }

    /// Apply order configuration using the caller-owned runtime and optional credentials.
    pub async fn apply_order_conf_by_request_with_credentials(
        request: OrderConfRequest,
        credentials: Option<crate::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_client_rust::ClientRuntime>,
    ) -> CanonicalResult<OrderConfResult> {
        let mut admin = admin_builder_with_credentials(request.admin_builder(), credentials, client_runtime)
            .build_and_start()
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)?;
        let result = Self::apply_order_conf_with_admin(&mut admin, &request).await;
        admin.shutdown().await;
        result
    }

    async fn apply_order_conf_with_admin(
        admin: &mut DefaultMQAdminExt,
        request: &OrderConfRequest,
    ) -> CanonicalResult<OrderConfResult> {
        match request.method() {
            OrderConfMethod::Put => {
                let order_conf = CheetahString::from(request.order_conf().unwrap_or_default());
                Self::create_or_update_order_conf(admin, request.topic().clone(), order_conf.clone())
                    .await
                    .map(|_| OrderConfResult {
                        topic: request.topic().clone(),
                        method: request.method(),
                        order_conf: Some(order_conf),
                    })
            }
            OrderConfMethod::Get => Self::get_order_conf(admin, request.topic().clone())
                .await
                .map(|order_conf| OrderConfResult {
                    topic: request.topic().clone(),
                    method: request.method(),
                    order_conf: Some(order_conf),
                }),
            OrderConfMethod::Delete => {
                Self::delete_order_conf(admin, request.topic().clone())
                    .await
                    .map(|_| OrderConfResult {
                        topic: request.topic().clone(),
                        method: request.method(),
                        order_conf: None,
                    })
            }
        }
    }

    /// Query message queue allocation through a complete core request lifecycle.
    pub async fn query_allocated_mq_by_request(
        request: AllocateMqQueryRequest,
    ) -> CanonicalResult<AllocatedMqQueryResult> {
        let mut admin = request
            .admin_builder()
            .build_and_start()
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)?;
        let result = Self::query_allocated_mq(&mut admin, request.topic().clone(), request.ip_list().clone()).await;
        admin.shutdown().await;
        result
    }

    /// Query message queue allocation using the caller-owned runtime and optional credentials.
    pub async fn query_allocated_mq_by_request_with_credentials(
        request: AllocateMqQueryRequest,
        credentials: Option<crate::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_client_rust::ClientRuntime>,
    ) -> CanonicalResult<AllocatedMqQueryResult> {
        let mut admin = admin_builder_with_credentials(request.admin_builder(), credentials, client_runtime)
            .build_and_start()
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)?;
        let result = Self::query_allocated_mq(&mut admin, request.topic().clone(), request.ip_list().clone()).await;
        admin.shutdown().await;
        result
    }

    /// Create or update a topic through a complete core request lifecycle.
    pub async fn create_or_update_topic_by_request(request: UpdateTopicRequest) -> CanonicalResult<UpdateTopicResult> {
        let mut admin = request
            .admin_builder()
            .build_and_start()
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)?;
        let config = request.config().clone();
        let target = request.target().clone();
        let result = Self::create_or_update_topic(&mut admin, config.clone(), target.clone())
            .await
            .map(|_| UpdateTopicResult {
                order_conf_updated: config.order,
                config,
                target,
            });
        admin.shutdown().await;
        result
    }

    /// Create or update a topic using the caller-owned client runtime and optional credentials.
    ///
    /// This is the application-facing lifecycle variant. It preserves the legacy request API while
    /// allowing CLI and service hosts to keep runtime ownership explicit and attach the configured
    /// admin authentication hook.
    pub async fn create_or_update_topic_by_request_with_credentials(
        request: UpdateTopicRequest,
        credentials: Option<crate::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_client_rust::ClientRuntime>,
    ) -> CanonicalResult<UpdateTopicResult> {
        let mut admin = admin_builder_with_credentials(request.admin_builder(), credentials, client_runtime)
            .build_and_start()
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)?;
        let config = request.config().clone();
        let target = request.target().clone();
        let result = Self::create_or_update_topic(&mut admin, config.clone(), target.clone())
            .await
            .map(|_| UpdateTopicResult {
                order_conf_updated: config.order,
                config,
                target,
            });
        admin.shutdown().await;
        result
    }

    /// Apply a batch of topic configs through a complete core request lifecycle.
    pub async fn update_topic_config_list_by_request(
        request: UpdateTopicListRequest,
    ) -> CanonicalResult<UpdateTopicListResult> {
        let mut admin = request
            .admin_builder()
            .build_and_start()
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)?;
        let target = request.target().clone();
        let topic_configs = request.topic_configs().to_vec();
        let result = Self::update_topic_config_list(&mut admin, target, topic_configs).await;
        admin.shutdown().await;
        result
    }

    /// Apply a batch of topic configs using the caller-owned runtime and optional credentials.
    pub async fn update_topic_config_list_by_request_with_credentials(
        request: UpdateTopicListRequest,
        credentials: Option<crate::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_client_rust::ClientRuntime>,
    ) -> CanonicalResult<UpdateTopicListResult> {
        let mut admin = admin_builder_with_credentials(request.admin_builder(), credentials, client_runtime)
            .build_and_start()
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)?;
        let target = request.target().clone();
        let topic_configs = request.topic_configs().to_vec();
        let result = Self::update_topic_config_list(&mut admin, target, topic_configs).await;
        admin.shutdown().await;
        result
    }

    /// Update topic permission through a complete core request lifecycle.
    pub async fn update_topic_perm_by_request(
        request: UpdateTopicPermRequest,
    ) -> CanonicalResult<UpdateTopicPermResult> {
        let mut admin = request
            .admin_builder()
            .build_and_start()
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)?;
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

    /// Update topic permission using the caller-owned runtime and optional credentials.
    pub async fn update_topic_perm_by_request_with_credentials(
        request: UpdateTopicPermRequest,
        credentials: Option<crate::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_client_rust::ClientRuntime>,
    ) -> CanonicalResult<UpdateTopicPermResult> {
        let mut admin = admin_builder_with_credentials(request.admin_builder(), credentials, client_runtime)
            .build_and_start()
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)?;
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
    ) -> CanonicalResult<Option<rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData>> {
        let topic = topic.into();
        admin
            .examine_topic_route_info(topic.clone())
            .await
            .map_err(|_| crate::client_adapter::services::errors::topic_route_not_found(topic.to_string()))
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
    ) -> CanonicalResult<()> {
        let topic = topic.into();
        let cluster = cluster_name.into();

        admin
            .delete_topic(topic.clone(), cluster.clone())
            .await
            .map_err(crate::client_adapter::services::errors::internal_by)
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
    ) -> CanonicalResult<()> {
        use rocketmq_model::common::TopicFilterType;

        let topic_name = config.topic_name.clone();
        let write_queue_nums = config.write_queue_nums as u32;
        let order = config.order;

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
            attributes: config
                .attributes
                .into_iter()
                .map(|(key, value)| (CheetahString::from(key), CheetahString::from(value)))
                .collect(),
        };

        let (target_addrs, target_broker_names, cluster_wide) = match target {
            super::types::TopicTarget::Broker(addr) => {
                let broker_names = if order {
                    let cluster_info = admin
                        .examine_broker_cluster_info()
                        .await
                        .map_err(crate::client_adapter::services::errors::internal_by)?;
                    HashSet::from([BrokerAddressResolver::fetch_broker_name_by_addr(
                        &cluster_info,
                        addr.as_str(),
                    )?])
                } else {
                    HashSet::new()
                };
                (vec![addr], broker_names, false)
            }
            super::types::TopicTarget::Cluster(cluster_name) => {
                let cluster_info = admin
                    .examine_broker_cluster_info()
                    .await
                    .map_err(crate::IntoCanonicalError::into_canonical_error)?;

                let master_addrs =
                    BrokerAddressResolver::fetch_master_addr_by_cluster_name(&cluster_info, &cluster_name)?;

                if master_addrs.is_empty() {
                    return Err(crate::client_adapter::services::errors::cluster_not_found(
                        cluster_name.as_str(),
                    ));
                }

                let broker_names =
                    BrokerAddressResolver::fetch_broker_name_by_cluster_name(&cluster_info, cluster_name.as_str())?
                        .into_iter()
                        .collect();
                (master_addrs, broker_names, true)
            }
        };

        for addr in target_addrs {
            admin
                .create_and_update_topic_config(addr, internal_config.clone())
                .await
                .map_err(crate::client_adapter::services::errors::internal_by)?;
        }

        if order {
            let order_conf = build_order_conf(&target_broker_names, write_queue_nums);
            admin
                .create_or_update_order_conf(topic_name, order_conf.into(), cluster_wide)
                .await
                .map_err(crate::client_adapter::services::errors::internal_by)?;
        }

        Ok(())
    }

    /// Apply a batch of topic configs to one broker or every master broker in a cluster.
    pub async fn update_topic_config_list(
        admin: &mut DefaultMQAdminExt,
        target: super::types::TopicTarget,
        topic_configs: Vec<RocketMQTopicConfig>,
    ) -> CanonicalResult<UpdateTopicListResult> {
        if topic_configs.is_empty() {
            return Err(crate::client_adapter::services::errors::admin_validation_failed(
                "topicConfigs",
                "topicConfigs must not be empty",
            ));
        }

        let broker_addrs = match &target {
            super::types::TopicTarget::Broker(broker_addr) => vec![broker_addr.clone()],
            super::types::TopicTarget::Cluster(cluster_name) => {
                let cluster_info = admin
                    .examine_broker_cluster_info()
                    .await
                    .map_err(crate::IntoCanonicalError::into_canonical_error)?;
                let master_addrs =
                    BrokerAddressResolver::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name)?;
                if master_addrs.is_empty() {
                    return Err(crate::client_adapter::services::errors::cluster_not_found(
                        cluster_name.as_str(),
                    ));
                }
                master_addrs
            }
        };

        for broker_addr in &broker_addrs {
            admin
                .create_and_update_topic_config_list(broker_addr.clone(), topic_configs.clone())
                .await
                .map_err(crate::client_adapter::services::errors::internal_by)?;
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
    ) -> CanonicalResult<std::collections::HashMap<String, HashSet<CheetahString>>> {
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

    /// Query topics using an already started admin client.
    ///
    /// The caller owns the admin client lifecycle, allowing multiple read-only
    /// operations to share one workflow-scoped session.
    pub(crate) async fn query_topic_list_with_admin(
        admin: &mut DefaultMQAdminExt,
        request: &TopicListQueryRequest,
    ) -> CanonicalResult<TopicListResult> {
        let topic_list = admin
            .fetch_all_topic_list()
            .await
            .map_err(crate::client_adapter::services::errors::internal_by)?;

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
            .map_err(crate::client_adapter::services::errors::internal_by)?;

        let mut topics = Vec::new();
        for topic in topic_list.topic_list {
            if topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) || topic.starts_with(DLQ_GROUP_TOPIC_PREFIX) {
                continue;
            }

            let route = match admin
                .examine_topic_route_info(topic.clone())
                .await
                .map_err(crate::IntoCanonicalError::into_canonical_error)?
            {
                Some(route) => route,
                None => continue,
            };
            if !Self::topic_route_belongs_to_cluster(&route, &cluster_info, cluster_name) {
                continue;
            }

            let group_list = admin
                .query_topic_consume_by_who(topic.clone())
                .await
                .map_err(crate::IntoCanonicalError::into_canonical_error)?;
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
        route: &rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData,
        cluster_info: &rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo,
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
    ) -> CanonicalResult<rocketmq_protocol::protocol::admin::topic_stats_table::TopicStatsTable> {
        admin
            .examine_topic_stats(topic.into(), broker_addr)
            .await
            .map_err(crate::client_adapter::services::errors::internal_by)
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
    ) -> CanonicalResult<()> {
        use rocketmq_model::common::config::TopicConfig as RocketMQTopicConfig;

        let topic = topic.into();

        match target {
            super::types::TopicTarget::Broker(broker_addr) => {
                // Get existing config
                let topic_config = admin
                    .examine_topic_config(broker_addr.clone(), topic.clone())
                    .await
                    .map_err(crate::client_adapter::services::errors::internal_by)?;

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
                    .map_err(crate::client_adapter::services::errors::internal_by)?;

                Ok(())
            }
            super::types::TopicTarget::Cluster(cluster_name) => {
                // Get cluster info
                let cluster_info = admin
                    .examine_broker_cluster_info()
                    .await
                    .map_err(crate::client_adapter::services::errors::internal_by)?;

                // Find master brokers
                let master_addrs =
                    BrokerAddressResolver::fetch_master_addr_by_cluster_name(&cluster_info, &cluster_name)?;

                if master_addrs.is_empty() {
                    return Err(crate::client_adapter::services::errors::cluster_not_found(cluster_name));
                }

                // Update on all master brokers
                for broker_addr in master_addrs {
                    let topic_config = admin
                        .examine_topic_config(broker_addr.clone(), topic.clone())
                        .await
                        .map_err(crate::client_adapter::services::errors::internal_by)?;

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
                        .map_err(crate::client_adapter::services::errors::internal_by)?;
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
    ) -> CanonicalResult<AllocatedMqQueryResult> {
        let topic = topic.into();
        let ip_list = ip_list.into();

        let requested_ips = ip_list
            .split_char(',')
            .map(str::trim)
            .filter(|ip| !ip.is_empty())
            .map(CheetahString::from)
            .collect::<Vec<_>>();
        let route_opt = admin
            .examine_topic_route_info(topic.clone())
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)?;

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
    ) -> CanonicalResult<()> {
        const NAMESPACE: &str = "ORDER_TOPIC_CONFIG";
        admin
            .create_and_update_kv_config(
                CheetahString::from_static_str(NAMESPACE),
                topic.into(),
                order_conf.into(),
            )
            .await
            .map_err(crate::client_adapter::services::errors::internal_by)
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
    ) -> CanonicalResult<CheetahString> {
        const NAMESPACE: &str = "ORDER_TOPIC_CONFIG";
        admin
            .get_kv_config(CheetahString::from_static_str(NAMESPACE), topic.into())
            .await
            .map_err(crate::client_adapter::services::errors::internal_by)
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
    ) -> CanonicalResult<()> {
        const NAMESPACE: &str = "ORDER_TOPIC_CONFIG";
        admin
            .delete_kv_config(CheetahString::from_static_str(NAMESPACE), topic.into())
            .await
            .map_err(crate::client_adapter::services::errors::internal_by)
    }
}

fn build_order_conf(broker_names: &HashSet<String>, write_queue_nums: u32) -> String {
    let mut broker_names = broker_names.iter().collect::<Vec<_>>();
    broker_names.sort();
    broker_names
        .into_iter()
        .map(|broker_name| format!("{broker_name}:{write_queue_nums}"))
        .collect::<Vec<_>>()
        .join(";")
}

fn admin_builder_with_credentials(
    builder: AdminBuilder,
    credentials: Option<crate::core::security::AdminCredentials>,
    client_runtime: std::sync::Arc<rocketmq_client_rust::ClientRuntime>,
) -> AdminBuilder {
    let builder = builder.client_runtime(client_runtime);
    match credentials {
        Some(hook) => builder.credentials(hook),
        None => builder,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use rocketmq_client_rust::ClientRuntime;
    use rocketmq_client_rust::ClientRuntimeConfig;
    use rocketmq_client_rust::TelemetryHandle;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;

    use super::admin_builder_with_credentials;
    use super::build_order_conf;
    use crate::client_adapter::services::admin::AdminBuilder;
    use crate::core::security::AdminCredentials;

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
            attributes: std::collections::HashMap::new(),
        };
    }

    #[test]
    fn topic_admin_builder_injects_runtime_and_credentials() {
        let runtime_owner = RuntimeOwner::plan(RuntimeConfig {
            thread_name: "topic-service-unit-test".to_string(),
            ..Default::default()
        })
        .expect("runtime configuration is valid")
        .build()
        .unwrap();
        let client_runtime = ClientRuntime::try_new(
            runtime_owner.root_context().component("topic-service-client"),
            ClientRuntimeConfig::default(),
            TelemetryHandle::noop(),
        )
        .unwrap();
        let credentials = AdminCredentials::try_new("access-key", "secret-key", None).unwrap();

        let builder =
            admin_builder_with_credentials(AdminBuilder::new(), Some(credentials), Arc::clone(&client_runtime));
        let debug = format!("{builder:?}");

        assert!(debug.contains("client_runtime: true"));
        assert!(debug.contains("rpc_hook: true"));
    }

    #[test]
    fn order_conf_is_deterministic_for_every_target_broker() {
        let broker_names = HashSet::from(["broker-b".to_string(), "broker-a".to_string(), "broker-c".to_string()]);

        assert_eq!(build_order_conf(&broker_names, 8), "broker-a:8;broker-b:8;broker-c:8");
    }
}
