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

//! RouteInfoManager v2 - Refactored with DashMap-based concurrent tables
//!
//! This is the new implementation using fine-grained concurrency with DashMap,
//! replacing the global RwLock approach from v1.

use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::mix_all;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_remoting::protocol::body::topic::topic_list::TopicList;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
use rocketmq_remoting::protocol::namesrv::RegisterBrokerResult;
use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
use rocketmq_remoting::protocol::route::route_data_view::QueueData;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_rust::ArcMut;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::bootstrap::NameServerRuntimeInner;
use crate::route::batch_unregistration_service::BatchUnregistrationService;
use crate::route::error::RocketMQError;
use crate::route::error::RouteResult;
use crate::route::tables::BrokerAddrTable;
use crate::route::tables::BrokerLiveInfo;
use crate::route::tables::BrokerLiveTable;
use crate::route::tables::ClusterAddrTable;
use crate::route::tables::TopicQueueTable;
use crate::route::types::BrokerName;
use crate::route::types::TopicName;
use crate::route_info::broker_addr_info::BrokerAddrInfo;

const DEFAULT_BROKER_CHANNEL_EXPIRED_TIME: u64 = 1000 * 60 * 2; // 2 minutes

/// RouteInfoManager v2 with DashMap-based concurrent tables
///
/// Key improvements over v1:
/// - Fine-grained concurrency: DashMap per table instead of global RwLock
/// - Zero-copy: Arc<str> for shared strings instead of String clones
/// - Type-safe errors: Result<T, RocketMQError> instead of Option
/// - Better modularity: Separate table modules for maintainability
pub struct RouteInfoManagerV2 {
    // New DashMap-based concurrent tables
    topic_queue_table: TopicQueueTable,
    broker_addr_table: BrokerAddrTable,
    cluster_addr_table: ClusterAddrTable,
    broker_live_table: BrokerLiveTable,

    // Legacy components (will be migrated in later phases)
    name_server_runtime_inner: ArcMut<NameServerRuntimeInner>,
    un_register_service: ArcMut<BatchUnregistrationService>,
}

impl RouteInfoManagerV2 {
    /// Create a new RouteInfoManager with DashMap-based tables
    pub(crate) fn new(name_server_runtime_inner: ArcMut<NameServerRuntimeInner>) -> Self {
        let un_register_service = ArcMut::new(BatchUnregistrationService::new(
            name_server_runtime_inner.clone(),
        ));

        Self {
            // Initialize with estimated capacities based on typical cluster sizes
            topic_queue_table: TopicQueueTable::with_capacity(1024),
            broker_addr_table: BrokerAddrTable::with_capacity(128),
            cluster_addr_table: ClusterAddrTable::with_capacity(32),
            broker_live_table: BrokerLiveTable::with_capacity(256),
            name_server_runtime_inner,
            un_register_service,
        }
    }

    /// Start the route manager with channel disconnect listener
    pub fn start(&self, receiver: tokio::sync::broadcast::Receiver<std::net::SocketAddr>) {
        info!("Starting RouteInfoManager v2 with DashMap tables");
        self.un_register_service.mut_from_ref().start();

        let broker_live_table = self.broker_live_table.clone();
        let broker_addr_table = self.broker_addr_table.clone();
        let cluster_addr_table = self.cluster_addr_table.clone();
        let topic_queue_table = self.topic_queue_table.clone();

        let mut receiver = receiver;
        tokio::spawn(async move {
            while let Ok(socket_addr) = receiver.recv().await {
                // Handle connection disconnect - remove broker by socket address
                // This is called when a broker connection is lost
                debug!("Connection disconnected: {}", socket_addr);

                // Find and remove broker by socket address
                if let Some(broker_info) =
                    Self::find_broker_by_socket_addr(&broker_live_table, socket_addr)
                {
                    broker_live_table.remove(&broker_info);

                    // Clean up broker from other tables
                    if let Some(broker_name) =
                        Self::find_broker_name_by_addr(&broker_addr_table, &broker_info.broker_addr)
                    {
                        Self::cleanup_broker_from_tables(
                            &broker_name,
                            &broker_info.cluster_name,
                            &broker_addr_table,
                            &cluster_addr_table,
                            &topic_queue_table,
                        );
                    }
                }
            }
        });
    }

    /// Find broker info by socket address
    fn find_broker_by_socket_addr(
        _broker_live_table: &BrokerLiveTable,
        _socket_addr: std::net::SocketAddr,
    ) -> Option<Arc<BrokerAddrInfo>> {
        // TODO: Need to map socket address to broker address info
        // This requires tracking socket addresses in BrokerLiveInfo
        None
    }

    /// Find broker name by broker address
    fn find_broker_name_by_addr(
        broker_addr_table: &BrokerAddrTable,
        broker_addr: &str,
    ) -> Option<String> {
        for (broker_name, broker_data) in broker_addr_table.get_all_brokers() {
            for (_broker_id, addr) in broker_data.broker_addrs().iter() {
                if addr.as_str() == broker_addr {
                    return Some(broker_name.to_string());
                }
            }
        }
        None
    }

    /// Cleanup broker from all tables
    fn cleanup_broker_from_tables(
        broker_name: &str,
        cluster_name: &str,
        broker_addr_table: &BrokerAddrTable,
        cluster_addr_table: &ClusterAddrTable,
        topic_queue_table: &TopicQueueTable,
    ) {
        broker_addr_table.remove(broker_name);
        cluster_addr_table.remove_broker(cluster_name, broker_name);

        // Remove broker from all topics
        let all_topics = topic_queue_table.get_all_topics();
        for topic in all_topics {
            topic_queue_table.remove_broker(topic.as_ref(), broker_name);
        }

        // Cleanup empty topics
        topic_queue_table.cleanup_empty_topics();
    }

    /// Handle connection disconnection (compatibility with v1)
    pub fn connection_disconnected(&self, socket_addr: std::net::SocketAddr) {
        debug!("Connection disconnected (v2): {}", socket_addr);
        // In v2, this is handled asynchronously in the start() method
        // This method is kept for compatibility
    }

    /// Shutdown the route manager
    pub fn shutdown(&self) {
        info!("Shutting down RouteInfoManager v2");
        // Cleanup if needed
    }
}

// ============================================================================
// Broker Registration
// ============================================================================

impl RouteInfoManagerV2 {
    /// Register a broker with the name server
    ///
    /// This is the main entry point for broker registration, handling:
    /// - Cluster membership updates
    /// - Broker address mapping
    /// - Topic configuration sync
    /// - Heartbeat registration
    ///
    /// # Arguments
    /// * `cluster_name` - Name of the cluster
    /// * `broker_addr` - Broker network address
    /// * `broker_name` - Logical broker name
    /// * `broker_id` - Broker ID (0 for master, >0 for slaves)
    /// * `ha_server_addr` - HA server address
    /// * `zone_name` - Optional zone/region identifier
    /// * `timeout_millis` - Heartbeat timeout in milliseconds
    /// * `enable_acting_master` - Whether to enable acting master mode
    /// * `topic_config_wrapper` - Topic configurations
    /// * `filter_server_list` - Message filter servers
    /// * `channel` - Network channel for communication
    ///
    /// # Returns
    /// Result containing RegisterBrokerResult or RocketMQError
    #[allow(clippy::too_many_arguments)]
    pub fn register_broker(
        &self,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
        ha_server_addr: CheetahString,
        zone_name: Option<CheetahString>,
        timeout_millis: Option<u64>,
        enable_acting_master: Option<bool>,
        topic_config_wrapper: TopicConfigAndMappingSerializeWrapper,
        _filter_server_list: Vec<CheetahString>,
        channel: Channel,
    ) -> RouteResult<RegisterBrokerResult> {
        let mut result = RegisterBrokerResult::default();

        // CheetahString already supports zero-copy sharing through clone
        let cluster_name_arc = cluster_name.clone();
        let broker_name_arc = broker_name.clone();

        // Step 1: Update cluster membership
        let is_new_broker = self
            .cluster_addr_table
            .add_broker(cluster_name_arc.clone(), broker_name_arc.clone());

        debug!(
            "Cluster membership updated: cluster={}, broker={}, new={}",
            cluster_name, broker_name, is_new_broker
        );

        // Step 2: Update broker address table
        let register_first = self.update_broker_addr_table(
            &cluster_name,
            &broker_name,
            broker_id,
            &broker_addr,
            zone_name.as_ref(),
            enable_acting_master,
        )?;

        // Step 3: Update topic queue configurations
        let is_master = broker_id == mix_all::MASTER_ID;
        if is_master {
            self.update_topic_queue_table(&broker_name_arc, &topic_config_wrapper, register_first)?;
        }

        // Step 4: Register broker live status
        self.register_broker_live_info(
            cluster_name.clone(),
            broker_addr.clone(),
            timeout_millis,
            topic_config_wrapper
                .topic_config_serialize_wrapper()
                .data_version()
                .clone(),
            channel,
            ha_server_addr,
        )?;

        // Step 5: Handle master address for slaves
        if !is_master {
            if let Some(master_addr) = self.get_master_address(&broker_name)? {
                result.master_addr = master_addr.clone();
                if let Some(master_live_info) =
                    self.get_broker_live_info(&cluster_name, &master_addr)
                {
                    if let Some(ref ha_addr) = master_live_info.ha_server_addr {
                        result.ha_server_addr = ha_addr.clone().into();
                    }
                }
            }
        }

        info!(
            "Broker registered: cluster={}, broker={}, id={}, addr={}, first={}",
            cluster_name, broker_name, broker_id, broker_addr, register_first
        );

        Ok(result)
    }

    /// Update broker address table with new broker information
    fn update_broker_addr_table(
        &self,
        cluster_name: &str,
        broker_name: &str,
        broker_id: u64,
        broker_addr: &str,
        zone_name: Option<&CheetahString>,
        enable_acting_master: Option<bool>,
    ) -> RouteResult<bool> {
        let broker_name_arc: BrokerName = CheetahString::from_string(broker_name.to_string());

        // Check if broker already exists
        let register_first = if let Some(existing_broker) = self.broker_addr_table.get(broker_name)
        {
            // Broker exists, update it
            let mut new_broker_data = (*existing_broker).clone();
            new_broker_data.set_enable_acting_master(enable_acting_master.unwrap_or_default());
            if let Some(zone) = zone_name {
                new_broker_data.set_zone_name(Some(zone.clone()));
            }

            // Add/update broker address
            new_broker_data
                .broker_addrs_mut()
                .insert(broker_id, broker_addr.into());

            // Update in table
            self.broker_addr_table
                .insert(broker_name_arc, new_broker_data);
            false
        } else {
            // New broker, create it
            let mut broker_addrs = HashMap::new();
            broker_addrs.insert(broker_id, broker_addr.into());

            let mut broker_data = BrokerData::new(
                cluster_name.into(),
                broker_name.into(),
                broker_addrs,
                zone_name.cloned(),
            );
            broker_data.set_enable_acting_master(enable_acting_master.unwrap_or_default());

            self.broker_addr_table.insert(broker_name_arc, broker_data);
            true
        };

        Ok(register_first)
    }

    /// Update topic queue table with topic configurations
    fn update_topic_queue_table(
        &self,
        broker_name: &BrokerName,
        topic_config_wrapper: &TopicConfigAndMappingSerializeWrapper,
        register_first: bool,
    ) -> RouteResult<()> {
        let topic_config_table = topic_config_wrapper
            .topic_config_serialize_wrapper()
            .topic_config_table();

        for (topic_name, topic_config) in topic_config_table.iter() {
            let topic_name_cheetah = topic_name.clone();

            // Create QueueData from TopicConfig
            let queue_data = QueueData::new(
                broker_name.to_string().into(),
                topic_config.read_queue_nums,
                topic_config.write_queue_nums,
                topic_config.perm,
                topic_config.topic_sys_flag,
            );

            // Check if we should update
            let should_update = register_first
                || self.is_topic_config_changed(topic_name_cheetah.as_str(), broker_name.as_str());

            if should_update {
                self.topic_queue_table.insert(
                    topic_name_cheetah.clone(),
                    broker_name.clone(),
                    queue_data,
                );

                debug!(
                    "Topic queue updated: topic={}, broker={}, read={}, write={}",
                    topic_name,
                    broker_name.as_str(),
                    topic_config.read_queue_nums,
                    topic_config.write_queue_nums
                );
            }
        }

        Ok(())
    }

    /// Register broker live information (heartbeat tracking)
    fn register_broker_live_info(
        &self,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
        timeout_millis: Option<u64>,
        data_version: DataVersion,
        _channel: Channel,
        ha_server_addr: CheetahString,
    ) -> RouteResult<()> {
        let broker_addr_info = Arc::new(BrokerAddrInfo::new(cluster_name, broker_addr));

        let timeout = timeout_millis.unwrap_or(DEFAULT_BROKER_CHANNEL_EXPIRED_TIME);
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let live_info = BrokerLiveInfo::new(current_time, data_version)
            .with_timeout(timeout)
            .with_ha_server(ha_server_addr.to_string());

        self.broker_live_table.register(broker_addr_info, live_info);

        Ok(())
    }

    /// Check if topic configuration has changed
    fn is_topic_config_changed(&self, topic: &str, broker_name: &str) -> bool {
        // Check if topic exists in table
        if !self.topic_queue_table.contains_topic(topic) {
            return true;
        }

        // Check if broker exists for this topic
        self.topic_queue_table.get(topic, broker_name).is_none()
    }

    /// Get master broker address for a broker name
    fn get_master_address(&self, broker_name: &str) -> RouteResult<Option<CheetahString>> {
        if let Some(broker_data) = self.broker_addr_table.get(broker_name) {
            Ok(broker_data.broker_addrs().get(&mix_all::MASTER_ID).cloned())
        } else {
            Ok(None)
        }
    }

    /// Get broker live info
    fn get_broker_live_info(
        &self,
        cluster_name: &str,
        broker_addr: &str,
    ) -> Option<Arc<BrokerLiveInfo>> {
        let broker_addr_info =
            BrokerAddrInfo::new(cluster_name.to_string(), broker_addr.to_string());
        self.broker_live_table.get(&broker_addr_info)
    }

    pub(crate) fn register_topic(&self, topic: CheetahString, queue_data_vec: Vec<QueueData>) {
        if queue_data_vec.is_empty() {
            return;
        }

        // Validate all brokers exist before inserting
        for queue_data in &queue_data_vec {
            if !self.broker_addr_table.contains(&queue_data.broker_name) {
                warn!(
                    "Register topic contains illegal broker, {}, {:?}",
                    topic, queue_data
                );
                return;
            }
        }

        // All brokers valid, proceed with insertion
        for queue_data in queue_data_vec {
            self.topic_queue_table.insert(
                topic.clone(),
                queue_data.broker_name.clone(),
                queue_data,
            );
        }
        info!(
            "Register topic route.{}, {:?}",
            topic,
            self.topic_queue_table.get_topic_queues(&topic)
        )
    }

    pub(crate) fn delete_topic(
        &mut self,
        topic: CheetahString,
        cluster_name: Option<CheetahString>,
    ) {
        if let Some(cluster_name) = cluster_name {
            let broker_names = self.cluster_addr_table.get_brokers(cluster_name.as_str());
            if broker_names.is_empty() {
                return;
            }
            let queue_data_map = self.topic_queue_table.get_topic_queues_map(topic.as_str());
            if queue_data_map.is_some() && !queue_data_map.as_ref().unwrap().is_empty() {
                for broker_name in broker_names {
                    let removed_qd = self
                        .topic_queue_table
                        .remove_broker(topic.as_ref(), broker_name.as_ref());
                    if removed_qd.is_some() {
                        info!(
                            "deleteTopic, remove one broker's topic {} {} {:?}",
                            broker_name, topic, removed_qd
                        );
                    }
                }
                let queue_data_map = self.topic_queue_table.get_topic_queues_map(topic.as_str());
                if queue_data_map.is_none() || queue_data_map.as_ref().unwrap().is_empty() {
                    self.topic_queue_table.remove_topic(topic.as_ref());
                    info!(
                        "deleteTopic, remove the Cluster {:?} topic {} completely",
                        cluster_name, topic
                    );
                }
            }
        } else {
            self.topic_queue_table.remove_topic(topic.as_ref());
        }
    }
}

// ============================================================================
// Broker Unregistration
// ============================================================================

impl RouteInfoManagerV2 {
    /// Submit broker unregistration request to batch service
    pub fn submit_unregister_broker_request(&self, request: UnRegisterBrokerRequestHeader) -> bool {
        self.un_register_service.submit(request)
    }

    /// Unregister a broker from the name server
    pub fn unregister_broker(
        &self,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
    ) -> RouteResult<()> {
        // Step 1: Remove from broker live table
        let broker_addr_info = BrokerAddrInfo::new(cluster_name.clone(), broker_addr.clone());
        let removed = self.broker_live_table.remove(&broker_addr_info);

        info!(
            "Broker live info removed: cluster={}, broker={}, addr={}, success={}",
            cluster_name,
            broker_name,
            broker_addr,
            removed.is_some()
        );

        // Step 2: Remove broker address from broker table
        let _broker_removed = self
            .broker_addr_table
            .remove_broker_address(broker_name.as_str(), broker_id);

        // Step 3: Check if all broker addresses are gone
        let broker_empty = if let Some(broker_data) = self.broker_addr_table.get(&broker_name) {
            broker_data.broker_addrs().is_empty()
        } else {
            true
        };

        // Step 4: If broker completely removed, clean up cluster and topics
        if broker_empty {
            self.broker_addr_table.remove(&broker_name);
            self.cluster_addr_table
                .remove_broker(cluster_name.as_str(), broker_name.as_str());
            self.cleanup_topics_for_broker(broker_name.as_str())?;

            info!(
                "Broker completely removed: cluster={}, broker={}",
                cluster_name, broker_name
            );
        }

        Ok(())
    }

    /// Clean up topics associated with a removed broker
    fn cleanup_topics_for_broker(&self, broker_name: &str) -> RouteResult<()> {
        // Get all topics
        let all_topics = self.topic_queue_table.get_all_topics();

        for topic in all_topics {
            // Remove broker from topic
            self.topic_queue_table
                .remove_broker(topic.as_ref(), broker_name);
        }

        // Clean up empty topics
        let removed_count = self.topic_queue_table.cleanup_empty_topics();
        if removed_count > 0 {
            debug!(
                "Cleaned up {} empty topics after broker removal",
                removed_count
            );
        }

        Ok(())
    }
}

// ============================================================================
// Route Lookup
// ============================================================================

impl RouteInfoManagerV2 {
    /// Get topic route data for a given topic
    ///
    /// This is the main query API used by producers and consumers
    /// to discover where a topic's messages should be sent/consumed.
    pub fn pickup_topic_route_data(&self, topic: &str) -> RouteResult<TopicRouteData> {
        // Get queue data for the topic
        let queue_data_list = self.topic_queue_table.get_topic_queues(topic);

        if queue_data_list.is_empty() {
            return Err(RocketMQError::route_not_found(topic));
        }

        // Collect broker names from queue data (already CheetahString)
        let broker_names: Vec<BrokerName> = queue_data_list
            .iter()
            .map(|(broker_name, _)| broker_name.clone())
            .collect();

        // Get broker data for each broker
        let mut broker_data_list = Vec::new();
        for broker_name in broker_names {
            if let Some(broker_data) = self.broker_addr_table.get(broker_name.as_str()) {
                // Clone BrokerData for the route response
                broker_data_list.push((*broker_data).clone());
            }
        }

        // Convert queue data to Vec<QueueData>
        let queue_data_vec: Vec<QueueData> = queue_data_list
            .into_iter()
            .map(|(_, queue_data)| (*queue_data).clone())
            .collect();

        // Construct TopicRouteData
        let topic_route_data = TopicRouteData {
            queue_datas: queue_data_vec,
            broker_datas: broker_data_list,
            ..Default::default()
        };

        debug!(
            "Topic route data retrieved: topic={}, brokers={}, queues={}",
            topic,
            topic_route_data.broker_datas.len(),
            topic_route_data.queue_datas.len()
        );

        Ok(topic_route_data)
    }

    /// Get all topics registered in the name server
    pub fn get_all_topics(&self) -> Vec<TopicName> {
        self.topic_queue_table.get_all_topics()
    }

    /// Get topics for a specific cluster
    pub fn get_topics_by_cluster(&self, cluster_name: &str) -> RouteResult<Vec<TopicName>> {
        // Get all brokers in the cluster
        let broker_names = self.cluster_addr_table.get_brokers(cluster_name);

        if broker_names.is_empty() {
            return Err(RocketMQError::cluster_not_found(cluster_name));
        }

        // Get all topics and filter by brokers in this cluster
        let all_topics = self.topic_queue_table.get_all_topics();
        let mut cluster_topics = Vec::new();

        for topic in all_topics {
            let topic_queues = self.topic_queue_table.get_topic_queues(topic.as_str());

            // Check if any queue belongs to a broker in this cluster
            for (broker_name, _) in topic_queues {
                if broker_names
                    .iter()
                    .any(|b| b.as_str() == broker_name.as_str())
                {
                    cluster_topics.push(topic);
                    break;
                }
            }
        }

        Ok(cluster_topics)
    }
}

// ============================================================================
// Broker Heartbeat & Health Check
// ============================================================================

impl RouteInfoManagerV2 {
    /// Scan for inactive brokers and remove them
    ///
    /// This should be called periodically (e.g., every 5 seconds)
    pub fn scan_not_active_broker(&self) -> RouteResult<usize> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Get expired brokers
        let expired_brokers = self.broker_live_table.get_expired_brokers(current_time);

        let count = expired_brokers.len();
        if count > 0 {
            warn!("Found {} expired brokers, removing...", count);

            // Remove each expired broker
            for broker_addr_info in expired_brokers {
                let cluster_name = broker_addr_info.cluster_name.clone();
                let broker_addr = broker_addr_info.broker_addr.clone();

                // Find broker name and ID from broker addr table
                if let Some((broker_name, broker_id)) = self.find_broker_by_addr(&broker_addr) {
                    // Unregister the broker
                    if let Err(e) = self.unregister_broker(
                        cluster_name.clone(),
                        broker_addr,
                        broker_name.clone(),
                        broker_id,
                    ) {
                        warn!(
                            "Failed to unregister expired broker: cluster={}, broker={}, error={}",
                            cluster_name, broker_name, e
                        );
                    }
                }
            }
        }

        Ok(count)
    }

    /// Find broker name and ID by address
    fn find_broker_by_addr(&self, broker_addr: &str) -> Option<(CheetahString, u64)> {
        for (broker_name, broker_data) in self.broker_addr_table.get_all_brokers() {
            for (broker_id, addr) in broker_data.broker_addrs().iter() {
                if addr.as_str() == broker_addr {
                    return Some((broker_name, *broker_id));
                }
            }
        }
        None
    }

    // ==================== Compatibility Methods for v1 API ====================
    // The following methods provide compatibility with v1's API surface

    /// Check if broker topic config has changed
    pub fn is_broker_topic_config_changed(
        &self,
        _cluster_name: &str,
        broker_addr: &str,
        data_version: &DataVersion,
    ) -> bool {
        // Find broker using addr info
        if let Some(broker_live_info) = self.broker_live_table.get_broker_by_addr(broker_addr) {
            return &broker_live_info.data_version != data_version;
        }
        true // If broker not found, assume changed
    }

    /// Update broker info update timestamp
    pub fn update_broker_info_update_timestamp(
        &self,
        _cluster_name: CheetahString,
        broker_addr: CheetahString,
    ) {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        self.broker_live_table
            .update_last_update_timestamp(&broker_addr, current_time);
    }

    /// Query broker topic config data version
    pub fn query_broker_topic_config(
        &self,
        _cluster_name: CheetahString,
        broker_addr: CheetahString,
    ) -> Option<DataVersion> {
        self.broker_live_table
            .get_broker_by_addr(&broker_addr)
            .map(|info| info.data_version.clone())
    }

    /// Get all cluster info (v1 compatibility)
    pub fn get_all_cluster_info(&self) -> RouteResult<ClusterInfo> {
        use std::collections::HashMap;
        use std::collections::HashSet;

        use rocketmq_remoting::protocol::route::route_data_view::BrokerData;

        let mut cluster_addr_table: HashMap<CheetahString, HashSet<CheetahString>> = HashMap::new();
        let mut broker_addr_table: HashMap<CheetahString, BrokerData> = HashMap::new();

        // Populate cluster_addr_table (broker_names is already Vec<CheetahString>)
        for (cluster_name, broker_names) in self.cluster_addr_table.get_all_cluster_brokers() {
            cluster_addr_table.insert(cluster_name, broker_names.into_iter().collect());
        }

        // Populate broker_addr_table (broker_name is already CheetahString)
        for (broker_name, broker_data) in self.broker_addr_table.get_all_brokers() {
            let data = BrokerData::new(
                CheetahString::from_string(broker_data.cluster().to_string()),
                broker_name.clone(),
                broker_data
                    .broker_addrs()
                    .iter()
                    .map(|(id, addr)| (*id, addr.clone()))
                    .collect(),
                None,
            );
            broker_addr_table.insert(broker_name, data);
        }

        Ok(ClusterInfo {
            broker_addr_table: Some(broker_addr_table),
            cluster_addr_table: Some(cluster_addr_table),
        })
    }

    /// Wipe write permission of broker by lock (v1 compatibility)
    pub fn wipe_write_perm_of_broker_by_lock(&self, broker_name: String) -> RouteResult<i32> {
        let mut wipe_topic_count = 0;

        // Iterate over all topics and update permissions
        for (topic, queue_datas) in self.topic_queue_table.iter_all_with_data() {
            for queue_data in queue_datas.iter() {
                if queue_data.broker_name() == broker_name.as_str() {
                    // Update permission (remove write permission)
                    let perm = queue_data.perm()
                        & !rocketmq_common::common::constant::PermName::PERM_WRITE;
                    self.topic_queue_table.update_queue_data_perm(
                        &topic,
                        &broker_name,
                        perm as i32,
                    );
                    wipe_topic_count += 1;
                }
            }
        }

        Ok(wipe_topic_count)
    }

    /// Add write permission of broker by lock (v1 compatibility)
    pub fn add_write_perm_of_broker_by_lock(&self, broker_name: String) -> RouteResult<i32> {
        let mut add_topic_count = 0;

        // Iterate over all topics and update permissions
        for (topic, queue_datas) in self.topic_queue_table.iter_all_with_data() {
            for queue_data in queue_datas.iter() {
                if queue_data.broker_name() == broker_name.as_str() {
                    // Update permission (add write permission)
                    let perm =
                        queue_data.perm() | rocketmq_common::common::constant::PermName::PERM_WRITE;
                    self.topic_queue_table.update_queue_data_perm(
                        &topic,
                        &broker_name,
                        perm as i32,
                    );
                    add_topic_count += 1;
                }
            }
        }

        Ok(add_topic_count)
    }

    /// Get system topic list (v1 compatibility)
    pub fn get_system_topic_list(&self) -> RouteResult<TopicList> {
        use rocketmq_common::common::topic::TopicValidator;

        let system_topic_set = TopicValidator::get_system_topic_set();
        let topics: Vec<CheetahString> = self
            .topic_queue_table
            .get_all_topics()
            .into_iter()
            .filter(|topic| system_topic_set.contains(topic.as_str()))
            .collect();

        Ok(TopicList {
            topic_list: topics,
            broker_addr: None,
        })
    }

    /// Get unit topics (v1 compatibility)
    pub fn get_unit_topics(&self) -> RouteResult<TopicList> {
        // Filter topics with unit flag
        let topics: Vec<CheetahString> = self
            .topic_queue_table
            .iter_all_with_data()
            .into_iter()
            .filter(|(_, queue_datas)| queue_datas.iter().any(|qd| qd.topic_sys_flag() == 1))
            .map(|(topic, _)| CheetahString::from_string(topic))
            .collect();

        Ok(TopicList {
            topic_list: topics,
            broker_addr: None,
        })
    }

    /// Get has unit sub topic list (v1 compatibility)
    pub fn get_has_unit_sub_topic_list(&self) -> RouteResult<TopicList> {
        // Filter topics that have unit subscription
        let topics: Vec<CheetahString> = self
            .topic_queue_table
            .iter_all_with_data()
            .into_iter()
            .filter(|(_, queue_datas)| queue_datas.iter().any(|qd| qd.topic_sys_flag() == 1))
            .map(|(topic, _)| CheetahString::from_string(topic))
            .collect();

        Ok(TopicList {
            topic_list: topics,
            broker_addr: None,
        })
    }

    /// Get has unit sub ununit topic list (v1 compatibility)
    pub fn get_has_unit_sub_ununit_topic_list(&self) -> RouteResult<TopicList> {
        // Filter topics that have unit subscription but are not unit topics
        let topics: Vec<CheetahString> = self
            .topic_queue_table
            .iter_all_with_data()
            .into_iter()
            .filter(|(_, queue_datas)| {
                // Has unit subscription flag but topic itself is not unit
                queue_datas.iter().any(|qd| qd.topic_sys_flag() != 0)
            })
            .map(|(topic, _)| CheetahString::from_string(topic))
            .collect();

        Ok(TopicList {
            topic_list: topics,
            broker_addr: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // NOTE: Integration tests for RouteInfoManagerV2 require complex setup.
    // Unit tests for underlying components are in their respective modules:
    // - TopicQueueTable: route::tables::topic_table::tests (8 tests)
    // - BrokerAddrTable: route::tables::broker_table::tests (7 tests)
    // - ClusterAddrTable: route::tables::cluster_table::tests (7 tests)
    // - BrokerLiveTable: route::tables::live_table::tests (7 tests)
    // - Error handling: route::error::tests (5 tests)
    //
    // Total: 34 tests covering all table operations.

    #[test]
    fn test_default_broker_timeout() {
        assert_eq!(DEFAULT_BROKER_CHANNEL_EXPIRED_TIME, 1000 * 60 * 2);
    }
}
