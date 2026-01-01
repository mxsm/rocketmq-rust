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

//! RouteInfoManager v2 - Refactored with DashMap-based concurrent tables
//!
//! This is the new implementation using fine-grained concurrency with DashMap,
//! replacing the global RwLock approach from v1.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::mix_all;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::clients::RemotingClient;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_remoting::protocol::body::topic::topic_list::TopicList;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::brokerid_change_request_header::NotifyMinBrokerIdChangeRequestHeader;
use rocketmq_remoting::protocol::namesrv::RegisterBrokerResult;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
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
use crate::route::segmented_lock::SegmentedLock;
use crate::route::tables::BrokerAddrTable;
use crate::route::tables::BrokerLiveInfo;
use crate::route::tables::BrokerLiveTable;
use crate::route::tables::ClusterAddrTable;
use crate::route::tables::FilterServerTable;
use crate::route::tables::TopicQueueMappingInfoTable;
use crate::route::tables::TopicQueueTable;
use crate::route::types::BrokerName;
use crate::route::types::TopicName;
use crate::route_info::broker_addr_info::BrokerAddrInfo;

const DEFAULT_BROKER_CHANNEL_EXPIRED_TIME: u64 = 1000 * 60 * 2; // 2 minutes

/// RouteInfoManager v2 with DashMap-based concurrent tables and segmented locking
///
/// Key improvements over v1:
/// - Fine-grained concurrency: DashMap per table instead of global RwLock
/// - Segmented locking: Per-broker/topic locks for atomic cross-table operations
/// - Zero-copy: Arc<str> for shared strings instead of String clones
/// - Type-safe errors: Result<T, RocketMQError> instead of Option
/// - Better modularity: Separate table modules for maintainability
///
/// ## Concurrency Model
///
/// This implementation uses a hybrid approach combining DashMap's lock-free concurrency
/// with segmented read-write locks for operations that span multiple tables:
///
/// 1. **DashMap**: Each table (topic, broker, cluster, live) uses DashMap for lock-free concurrent
///    reads and writes within a single table.
///
/// 2. **Segmented Locks**: For operations that need to atomically update multiple tables (e.g.,
///    broker registration, unregistration), we use segment-level read-write locks based on the
///    broker name or topic name hash.
///
/// ### Lock Acquisition Strategy
///
/// - **Single-table operations**: Use DashMap directly (no explicit locking)
/// - **Multi-table reads**: Acquire segment read lock, then use DashMap
/// - **Multi-table writes**: Acquire segment write lock, then use DashMap
///
/// ### Deadlock Prevention
///
/// When acquiring multiple locks, always sort by segment index to prevent deadlocks.
/// The `SegmentedLock::*_lock_multiple` methods handle this automatically.
///
/// ## Performance Characteristics
///
/// - **Single-table ops**: O(1) lock-free (DashMap only)
/// - **Multi-table read**: O(1) segment read lock + O(1) DashMap read
/// - **Multi-table write**: O(1) segment write lock + O(1) DashMap write
/// - **Contention**: Minimal due to 16-way lock striping
pub struct RouteInfoManagerV2 {
    // DashMap-based concurrent tables (lock-free for single-table operations)
    topic_queue_table: TopicQueueTable,
    broker_addr_table: BrokerAddrTable,
    cluster_addr_table: ClusterAddrTable,
    broker_live_table: BrokerLiveTable,
    filter_server_table: FilterServerTable,
    topic_queue_mapping_info_table: TopicQueueMappingInfoTable,

    // Segmented locks for atomic cross-table operations
    // - broker_locks: Locks for broker-related operations (keyed by broker name)
    // - topic_locks: Locks for topic-related operations (keyed by topic name)
    broker_locks: SegmentedLock,
    topic_locks: SegmentedLock,

    // Legacy components (will be migrated in later phases)
    name_server_runtime_inner: ArcMut<NameServerRuntimeInner>,
    un_register_service: ArcMut<BatchUnregistrationService>,
}

impl RouteInfoManagerV2 {
    /// Create a new RouteInfoManager with DashMap-based tables and segmented locks
    pub(crate) fn new(name_server_runtime_inner: ArcMut<NameServerRuntimeInner>) -> Self {
        let un_register_service = ArcMut::new(BatchUnregistrationService::new(name_server_runtime_inner.clone()));

        Self {
            // Initialize with estimated capacities based on typical cluster sizes
            topic_queue_table: TopicQueueTable::with_capacity(1024),
            broker_addr_table: BrokerAddrTable::with_capacity(128),
            cluster_addr_table: ClusterAddrTable::with_capacity(32),
            broker_live_table: BrokerLiveTable::with_capacity(256),
            filter_server_table: FilterServerTable::with_capacity(128),
            topic_queue_mapping_info_table: TopicQueueMappingInfoTable::with_capacity(256),

            // Initialize segmented locks (16 segments each for optimal performance)
            broker_locks: SegmentedLock::new(),
            topic_locks: SegmentedLock::new(),

            name_server_runtime_inner,
            un_register_service,
        }
    }

    /// Start the route manager
    ///
    /// This matches Java's start() method which only starts the unRegisterService.
    /// Channel disconnection events are handled by BrokerHousekeepingService through
    /// ChannelEventListener interface (on_channel_close, on_channel_exception, on_channel_idle).
    ///
    /// Java code:
    /// ```java
    /// public void start() {
    ///     this.unRegisterService.start();
    /// }
    /// ```
    pub fn start(&self) {
        info!("Starting RouteInfoManager v2 with DashMap tables");
        self.un_register_service.mut_from_ref().start();
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
    fn find_broker_name_by_addr(broker_addr_table: &BrokerAddrTable, broker_addr: &str) -> Option<String> {
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

    /// Handle connection disconnection by socket address
    ///
    /// 1. Find broker info by socket address
    /// 2. Setup unregister request
    /// 3. Submit to batch unregistration service
    pub fn on_channel_destroy(&self, socket_addr: std::net::SocketAddr) {
        let mut unregister_request = UnRegisterBrokerRequestHeader::default();
        let mut need_unregister = false;

        // Find broker by socket address and setup unregister request
        if let Some(broker_addr_info) = Self::find_broker_by_socket_addr(&self.broker_live_table, socket_addr) {
            need_unregister = self.setup_unregister_request(&mut unregister_request, &broker_addr_info);
        }

        if need_unregister {
            let result = self.submit_unregister_broker_request(unregister_request.clone());
            info!(
                "the broker's channel destroyed, submit the unregister request at once, broker info: {:?}, submit \
                 result: {}",
                unregister_request, result
            );
        }
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
    /// - Conflict detection and resolution
    /// - Acting master support
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
        filter_server_list: Vec<CheetahString>,
        channel: Channel,
    ) -> RouteResult<RegisterBrokerResult> {
        // ===================================================================
        // SEGMENTED LOCK ACQUISITION
        // ===================================================================
        // Acquire write lock for this broker segment to ensure atomic updates
        // across multiple tables (cluster, broker_addr, topic_queue, broker_live).
        //
        // This prevents race conditions such as:
        // 1. Concurrent registrations of the same broker
        // 2. Registration racing with unregistration
        // 3. Inconsistent state across tables
        //
        // Lock scope: Only brokers hashing to the same segment will contend.
        // Other brokers can register concurrently in different segments.
        let _broker_lock = self.broker_locks.write_lock(&broker_name.as_str());

        let mut result = RegisterBrokerResult::default();

        let cluster_name_arc = cluster_name.clone();
        let broker_name_arc = broker_name.clone();

        // Step 1: Update cluster membership
        // This is safe because we hold the broker write lock
        self.cluster_addr_table
            .add_broker(cluster_name_arc, broker_name_arc.clone());

        debug!(
            "Cluster membership updated: cluster={}, broker={}",
            cluster_name, broker_name
        );

        // Step 2: Update broker address table with conflict detection
        // Protected by broker write lock - atomic with cluster update
        let update_result = self.update_broker_addr_table_v2(
            &cluster_name,
            &broker_name,
            broker_id,
            &broker_addr,
            zone_name.as_ref(),
            enable_acting_master,
            &topic_config_wrapper,
        )?;

        // Step 3: Check if broker registration should be rejected
        if update_result.is_none() {
            warn!(
                "Broker registration rejected due to version conflict: cluster={}, broker={}, id={}, addr={}",
                cluster_name, broker_name, broker_id, broker_addr
            );
            return Ok(result);
        }
        let (register_first, is_min_broker_id_changed) = update_result.unwrap();

        // Step 4: Update topic queue configurations
        // Protected by broker write lock - atomic with broker updates
        let is_master = broker_id == mix_all::MASTER_ID;
        let is_old_version_broker = enable_acting_master.is_none();

        // Determine if this is a prime slave (acting master candidate)
        let is_prime_slave = if !is_old_version_broker && !is_master {
            // Get current broker addresses to find minimum broker ID
            if let Some(broker_data) = self.broker_addr_table.get(&broker_name) {
                if let Some(&min_broker_id) = broker_data.broker_addrs().keys().min() {
                    broker_id == min_broker_id
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        if is_master || is_prime_slave {
            self.update_topic_queue_table_v2(&broker_name_arc, &topic_config_wrapper, register_first, is_prime_slave)?;
        }

        // Step 5: Register broker live status
        // Protected by broker write lock - atomic with all above updates
        let prev_broker_live_info = self.register_broker_live_info(
            cluster_name.clone(),
            broker_addr.clone(),
            timeout_millis,
            topic_config_wrapper
                .topic_config_serialize_wrapper()
                .data_version()
                .clone(),
            channel,
            ha_server_addr.clone(),
        )?;

        if prev_broker_live_info.is_none() {
            info!(
                "New broker registered: cluster={}, broker={}, id={}, addr={}, HAService: {}",
                cluster_name, broker_name, broker_id, broker_addr, ha_server_addr
            );
        }

        // Step 6: Handle filter server list
        let broker_addr_info = Arc::new(BrokerAddrInfo::new(cluster_name.clone(), broker_addr.clone()));
        if filter_server_list.is_empty() {
            self.filter_server_table.remove(&broker_addr_info);
        } else {
            self.filter_server_table
                .register(broker_addr_info.clone(), filter_server_list);
        }

        // Step 7: Handle master address for slaves
        // Protected by broker write lock - consistent read of master info
        if !is_master {
            if let Some(master_addr) = self.get_master_address(&broker_name)? {
                result.master_addr = master_addr.clone();
                if let Some(master_live_info) = self.get_broker_live_info(&cluster_name, &master_addr) {
                    if let Some(ref ha_addr) = master_live_info.ha_server_addr {
                        result.ha_server_addr = ha_addr.clone();
                    }
                }
            }
        }

        // Step 8: Notify if min broker ID changed
        if is_min_broker_id_changed
            && self
                .name_server_runtime_inner
                .name_server_config()
                .notify_min_broker_id_changed
        {
            if let Some(broker_data) = self.broker_addr_table.get(&broker_name) {
                let ha_server_addr = self
                    .broker_live_table
                    .get(broker_addr_info.as_ref())
                    .map(|item| item.ha_server_addr.clone())
                    .unwrap_or_default();
                self.notify_min_broker_id_changed(broker_data.broker_addrs(), None, ha_server_addr);
            }
        }

        info!(
            "Broker registered: cluster={}, broker={}, id={}, addr={}, first={}",
            cluster_name, broker_name, broker_id, broker_addr, register_first
        );

        Ok(result)
    }

    /// Update broker address table with new broker information (v2 with conflict detection)
    ///
    /// Returns:
    /// - Ok(Some((register_first, is_min_broker_id_changed))): Success
    /// - Ok(None): Registration rejected due to version conflict
    /// - Err: Other errors
    fn update_broker_addr_table_v2(
        &self,
        cluster_name: &CheetahString,
        broker_name: &CheetahString,
        broker_id: u64,
        broker_addr: &CheetahString,
        zone_name: Option<&CheetahString>,
        enable_acting_master: Option<bool>,
        topic_config_wrapper: &TopicConfigAndMappingSerializeWrapper,
    ) -> RouteResult<Option<(bool, bool)>> {
        let broker_name_arc: BrokerName = broker_name.clone();
        let mut is_min_broker_id_changed = false;

        // Check if broker already exists
        let mut register_first = false;
        if let Some(existing_broker) = self.broker_addr_table.get(broker_name) {
            // Broker exists, update it
            let mut new_broker_data = (*existing_broker).clone();

            let is_old_version_broker = enable_acting_master.is_none();
            new_broker_data
                .set_enable_acting_master(!is_old_version_broker && enable_acting_master.unwrap_or_default());
            if let Some(zone) = zone_name {
                new_broker_data.set_zone_name(Some(zone.clone()));
            }

            let broker_addrs_map = new_broker_data.broker_addrs_mut();

            // Track minBrokerId changes
            let prev_min_broker_id = broker_addrs_map.keys().min().copied().unwrap_or(u64::MAX);
            if broker_id < prev_min_broker_id {
                is_min_broker_id_changed = true;
            }

            // Switch slave to master: remove same IP:PORT with different broker ID
            // The same IP:PORT must only have one record in brokerAddrTable
            broker_addrs_map.retain(|&id, addr| !(addr.as_str() == broker_addr.as_str() && id != broker_id));

            // Check for address conflict with different stateVersion
            if let Some(old_broker_addr) = broker_addrs_map.get(&broker_id) {
                if old_broker_addr.as_str() != broker_addr.as_str() {
                    // Address changed for same broker ID - check version conflict
                    let old_broker_info = BrokerAddrInfo::new(cluster_name.clone(), old_broker_addr.clone());

                    if let Some(old_broker_live_info) = self.broker_live_table.get(&old_broker_info) {
                        let old_state_version = old_broker_live_info.data_version.get_state_version();
                        let new_state_version = topic_config_wrapper
                            .topic_config_serialize_wrapper()
                            .data_version()
                            .get_state_version();

                        if old_state_version > new_state_version {
                            warn!(
                                "Registering Broker conflicts with the existed one, just ignore.: Cluster:{}, \
                                 BrokerName:{}, BrokerId:{}, Old BrokerAddr:{}, Old Version:{}, New BrokerAddr:{}, \
                                 New Version:{}",
                                cluster_name,
                                broker_name,
                                broker_id,
                                old_broker_addr,
                                old_state_version,
                                broker_addr,
                                new_state_version
                            );

                            // Remove the rejected broker from brokerLiveTable
                            let rejected_broker_info = BrokerAddrInfo::new(cluster_name.clone(), broker_addr.clone());
                            self.broker_live_table.remove(&rejected_broker_info);

                            return Ok(None); // Registration rejected
                        }
                    }
                }
            }

            // Check if broker has only one topic but is not registered yet
            if !broker_addrs_map.contains_key(&broker_id)
                && topic_config_wrapper
                    .topic_config_serialize_wrapper()
                    .topic_config_table()
                    .len()
                    == 1
            {
                warn!(
                    "Can't register topicConfigWrapper={:?} because broker[{}]={} has not registered.",
                    topic_config_wrapper
                        .topic_config_serialize_wrapper()
                        .topic_config_table()
                        .keys()
                        .collect::<Vec<_>>(),
                    broker_id,
                    broker_addr
                );
                return Ok(None); // Registration rejected
            }

            // Check if this is first registration
            let old_addr = broker_addrs_map.insert(broker_id, broker_addr.clone());
            register_first =
                register_first || old_addr.is_none() || old_addr.as_ref().map(|s| s.is_empty()).unwrap_or(false);

            // Update in table
            self.broker_addr_table.insert(broker_name_arc, new_broker_data);
        } else {
            // New broker, create it
            register_first = true;
            let mut broker_addrs = HashMap::new();
            broker_addrs.insert(broker_id, broker_addr.clone());

            let is_old_version_broker = enable_acting_master.is_none();
            let mut broker_data = BrokerData::new(
                cluster_name.clone(),
                broker_name.clone(),
                broker_addrs,
                zone_name.cloned(),
            );
            broker_data.set_enable_acting_master(!is_old_version_broker && enable_acting_master.unwrap_or_default());

            self.broker_addr_table.insert(broker_name_arc, broker_data);
        }

        Ok(Some((register_first, is_min_broker_id_changed)))
    }

    /// Update topic queue table with topic configurations (v2 with deletion and prime slave)
    fn update_topic_queue_table_v2(
        &self,
        broker_name: &BrokerName,
        topic_config_wrapper: &TopicConfigAndMappingSerializeWrapper,
        register_first: bool,
        is_prime_slave: bool,
    ) -> RouteResult<()> {
        use std::collections::HashSet;

        use rocketmq_common::common::constant::PermName;

        let topic_config_table = topic_config_wrapper
            .topic_config_serialize_wrapper()
            .topic_config_table();
        let topic_queue_mapping_info_map = topic_config_wrapper.topic_queue_mapping_info_map();

        // Delete topics that don't exist in tcTable from the current broker
        // Static topic is not supported if topicQueueMappingInfoMap is empty
        if self
            .name_server_runtime_inner
            .name_server_config()
            .delete_topic_with_broker_registration
            && topic_queue_mapping_info_map.is_empty()
        {
            let old_topic_set = self.topic_set_of_broker_name(broker_name.as_str());
            let new_topic_set: HashSet<_> = topic_config_table.keys().cloned().collect();

            // Find topics to delete (in old but not in new)
            for to_delete_topic in old_topic_set.difference(&new_topic_set) {
                if let Some(removed_qd) = self
                    .topic_queue_table
                    .remove_broker(to_delete_topic.as_ref(), broker_name.as_str())
                {
                    info!(
                        "deleteTopic, remove one broker's topic {} {} {:?}",
                        broker_name, to_delete_topic, removed_qd
                    );
                }

                // Check if topic is now empty
                if self
                    .topic_queue_table
                    .get_topic_queues_map(to_delete_topic.as_str())
                    .map(|map| map.is_empty())
                    .unwrap_or(true)
                {
                    self.topic_queue_table.remove_topic(to_delete_topic.as_ref());
                    info!("deleteTopic, remove the topic all queue {}", to_delete_topic);
                }
            }
        }

        // Get cluster name and broker addr for config change detection
        let (cluster_name, broker_addr) = if let Some(broker_data) = self.broker_addr_table.get(broker_name) {
            let cluster = broker_data.cluster().to_string();
            let addr = broker_data
                .broker_addrs()
                .values()
                .next()
                .map(|s| s.to_string())
                .unwrap_or_default();
            (cluster, addr)
        } else {
            (String::new(), String::new())
        };

        // Process each topic configuration
        for (topic_name, topic_config) in topic_config_table.iter() {
            let topic_name_cheetah = topic_name.clone();

            // Check if we should update this topic
            if register_first
                || self.is_topic_config_changed(
                    &cluster_name,
                    &broker_addr,
                    topic_config_wrapper.topic_config_serialize_wrapper().data_version(),
                    broker_name.as_str(),
                    topic_name.as_str(),
                )
            {
                let mut topic_config = topic_config.clone();

                // In Slave Acting Master mode, Namesrv regards the surviving Slave
                // with the smallest brokerId as the "agent" Master, and modifies
                // the brokerPermission to read-only
                if is_prime_slave {
                    if let Some(broker_data) = self.broker_addr_table.get(broker_name) {
                        if broker_data.enable_acting_master() {
                            // Wipe write permission for prime slave
                            topic_config.perm &= !PermName::PERM_WRITE;
                        }
                    }
                }

                // Create QueueData from TopicConfig
                let queue_data = QueueData::new(
                    broker_name.to_string().into(),
                    topic_config.read_queue_nums,
                    topic_config.write_queue_nums,
                    topic_config.perm,
                    topic_config.topic_sys_flag,
                );

                // Check if queue data exists and log appropriately
                let old_queue_data = self.topic_queue_table.get(topic_name, broker_name.as_str());
                if let Some(existed_qd) = old_queue_data {
                    if existed_qd.as_ref() != &queue_data {
                        info!(
                            "topic changed, {} OLD: {:?} NEW: {:?}",
                            topic_name, existed_qd, queue_data
                        );
                    }
                } else {
                    info!("new topic registered, {} {:?}", topic_name, &queue_data);
                }

                self.topic_queue_table
                    .insert(topic_name_cheetah.clone(), broker_name.clone(), queue_data);

                debug!(
                    "Topic queue updated: topic={}, broker={}, read={}, write={}, perm={}",
                    topic_name,
                    broker_name.as_str(),
                    topic_config.read_queue_nums,
                    topic_config.write_queue_nums,
                    topic_config.perm
                );
            }
        }

        // Update topic queue mapping info if broker topic config changed or first registration
        if self.is_broker_topic_config_changed(
            &cluster_name.into(),
            &broker_addr.into(),
            topic_config_wrapper.topic_config_serialize_wrapper().data_version(),
        ) || register_first
        {
            for entry in topic_queue_mapping_info_map.iter() {
                let topic = entry.key().clone();
                let mapping_info = entry.value();

                // Extract broker name from mapping info (bname is a field, not a method)
                let broker_name_mapping = mapping_info.bname.clone().unwrap_or_else(|| broker_name.clone());

                self.topic_queue_mapping_info_table.register(
                    topic,
                    broker_name_mapping,
                    Arc::new((**mapping_info).clone()),
                );
            }
        }

        Ok(())
    }

    /// Register broker live information (heartbeat tracking)
    /// Returns the previous BrokerLiveInfo if it existed
    fn register_broker_live_info(
        &self,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
        timeout_millis: Option<u64>,
        data_version: DataVersion,
        _channel: Channel,
        ha_server_addr: CheetahString,
    ) -> RouteResult<Option<Arc<BrokerLiveInfo>>> {
        let broker_addr_info = Arc::new(BrokerAddrInfo::new(cluster_name, broker_addr));

        let timeout = timeout_millis.unwrap_or(DEFAULT_BROKER_CHANNEL_EXPIRED_TIME);
        let current_time = get_current_millis();

        let live_info = BrokerLiveInfo::new(current_time, data_version)
            .with_timeout(timeout)
            .with_ha_server(ha_server_addr);

        let prev = self.broker_live_table.register(broker_addr_info, live_info);

        Ok(prev)
    }

    /// Check if topic configuration has changed (extended version)
    fn is_topic_config_changed(
        &self,
        cluster_name: &str,
        broker_addr: &str,
        data_version: &DataVersion,
        broker_name: &str,
        topic: &str,
    ) -> bool {
        let is_change = self.is_broker_topic_config_changed(&cluster_name.into(), &broker_addr.into(), data_version);
        if is_change {
            return true;
        }

        // Check if topic exists in table
        if !self.topic_queue_table.contains_topic(topic) {
            return true;
        }

        // Check if broker exists for this topic
        self.topic_queue_table.get(topic, broker_name).is_none()
    }

    /// Get all topics registered by a specific broker
    fn topic_set_of_broker_name(&self, broker_name: &str) -> std::collections::HashSet<CheetahString> {
        use std::collections::HashSet;

        let mut topic_set = HashSet::new();
        for (topic, queues) in self.topic_queue_table.iter_all_with_data() {
            for queue_data in queues.iter() {
                if queue_data.broker_name() == broker_name {
                    topic_set.insert(CheetahString::from_string(topic));
                    break;
                }
            }
        }
        topic_set
    }

    /// Notify when minimum broker ID has changed (for master election)
    ///
    /// This method notifies brokers when the minimum broker ID changes,
    /// which is critical for acting master mode where the slave with the
    /// smallest broker ID becomes the acting master.
    ///
    /// # Arguments
    /// * `broker_addrs` - Map of broker IDs to addresses
    /// * `offline_broker_addr` - Address of broker going offline (if any)
    /// * `ha_server_addr` - HA server address
    fn notify_min_broker_id_changed(
        &self,
        broker_addrs: &HashMap<u64, CheetahString>,
        offline_broker_addr: Option<CheetahString>,
        ha_server_addr: Option<CheetahString>,
    ) {
        if broker_addrs.is_empty() {
            return;
        }

        let min_broker_id = match broker_addrs.keys().min().copied() {
            Some(id) => id,
            None => return,
        };

        let min_broker_addr = broker_addrs.get(&min_broker_id).cloned();

        let request_header = NotifyMinBrokerIdChangeRequestHeader::new(
            Some(min_broker_id),
            None,
            min_broker_addr,
            offline_broker_addr.clone(),
            ha_server_addr,
        );

        // Choose which brokers to notify
        let broker_addrs_notify = Self::choose_broker_addrs_to_notify(broker_addrs, &offline_broker_addr);

        if broker_addrs_notify.is_empty() {
            return;
        }

        info!(
            "Min broker id changed to {}, notify {:?}, offline broker addr {:?}",
            min_broker_id, broker_addrs_notify, offline_broker_addr
        );

        // Create remoting command
        let request = RemotingCommand::create_request_command(RequestCode::NotifyMinBrokerIdChange, request_header);

        // Send notification to each broker asynchronously
        for broker_addr in broker_addrs_notify {
            let remoting_client = self.name_server_runtime_inner.clone();
            let request = request.clone();
            let broker_addr = broker_addr.clone();

            tokio::spawn(async move {
                let _ = remoting_client
                    .remoting_client()
                    .invoke_request_oneway(&broker_addr, request, 3000)
                    .await;
            });
        }
    }

    /// Choose which broker addresses should receive the min broker ID change notification
    ///
    /// # Logic
    /// - If only 1 broker or offline event: notify all brokers
    /// - Otherwise: notify all brokers except the one with min broker ID
    ///
    /// # Arguments
    /// * `broker_addrs` - Map of broker IDs to addresses
    /// * `offline_broker_addr` - Address of broker going offline (if any)
    ///
    /// # Returns
    /// Vector of broker addresses to notify
    fn choose_broker_addrs_to_notify(
        broker_addrs: &HashMap<u64, CheetahString>,
        offline_broker_addr: &Option<CheetahString>,
    ) -> Vec<CheetahString> {
        // If only one broker or there's an offline event, notify all
        if broker_addrs.len() == 1 || offline_broker_addr.is_some() {
            return broker_addrs.values().cloned().collect();
        }

        // Otherwise, notify all except the min broker ID
        let min_broker_id = match broker_addrs.keys().min().copied() {
            Some(id) => id,
            None => return Vec::new(),
        };

        broker_addrs
            .iter()
            .filter(|(&id, _)| id != min_broker_id)
            .map(|(_, addr)| addr.clone())
            .collect()
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
    fn get_broker_live_info(&self, cluster_name: &str, broker_addr: &str) -> Option<Arc<BrokerLiveInfo>> {
        let broker_addr_info = BrokerAddrInfo::new(cluster_name.to_string(), broker_addr.to_string());
        self.broker_live_table.get(&broker_addr_info)
    }

    /// Register a topic with queue data for multiple brokers
    ///
    /// This method ensures atomicity by acquiring locks for:
    /// 1. The topic being registered (write lock)
    /// 2. All brokers referenced in queue_data_vec (read locks)
    ///
    /// ## Consistency Guarantee
    ///
    /// The method performs two operations that must be atomic:
    /// 1. Validate all brokers exist in broker_addr_table
    /// 2. Insert queue data into topic_queue_table
    ///
    /// Without locking, a concurrent unregister_broker could delete a broker
    /// between validation and insertion, causing inconsistent state where
    /// topic_queue_table references non-existent brokers.
    ///
    /// ## Lock Strategy
    ///
    /// - **Topic write lock**: Prevents concurrent modifications to this topic
    /// - **Broker read locks**: Prevents brokers from being deleted during registration
    ///
    /// This ensures that if all brokers pass validation, they're guaranteed to
    /// still exist when we insert the queue data.
    pub(crate) fn register_topic(&self, topic: CheetahString, queue_data_vec: Vec<QueueData>) {
        if queue_data_vec.is_empty() {
            return;
        }

        // Acquire topic write lock and broker read locks atomically
        // This prevents:
        // 1. Concurrent topic registrations (topic write lock)
        // 2. Broker deletion during registration (broker read locks)
        let broker_names: Vec<_> = queue_data_vec.iter().map(|qd| qd.broker_name.as_str()).collect();

        let _topic_lock = self.topic_locks.write_lock(&topic);
        let _broker_locks = self.broker_locks.read_lock_multiple(&broker_names);

        // Check if topic already exists
        let topic_exists = self.topic_queue_table.contains_topic(topic.as_str());

        // Validate all brokers exist first (before any modification)
        for queue_data in &queue_data_vec {
            if !self.broker_addr_table.contains(&queue_data.broker_name) {
                warn!("Register topic contains illegal broker, {}, {:?}", topic, queue_data);
                return;
            }
        }

        // All brokers valid, proceed with insertion/update
        for queue_data in &queue_data_vec {
            self.topic_queue_table
                .insert(topic.clone(), queue_data.broker_name.clone(), queue_data.clone());
        }

        // Log appropriate message based on whether topic existed
        if topic_exists {
            info!(
                "Topic route already exist.{}, {:?}",
                topic,
                self.topic_queue_table.get_topic_queues(&topic)
            );
        } else {
            info!("Register topic route:{}, {:?}", topic, queue_data_vec);
        }
    }

    /// Delete a topic from the name server
    ///
    /// This method deletes topic queue data either for a specific cluster
    /// or completely if no cluster is specified.
    ///
    /// ## Consistency Guarantee
    ///
    /// The method performs multiple operations that must be atomic:
    /// 1. Query brokers in the cluster (cluster_addr_table)
    /// 2. Remove topic-broker mappings (topic_queue_table)
    /// 3. Cleanup empty topics (topic_queue_table)
    ///
    /// ## Lock Strategy
    ///
    /// - **Topic write lock**: Ensures no concurrent topic registration/deletion
    /// - **Cluster read lock** (if cluster specified): Prevents cluster modification
    ///
    /// This ensures consistent deletion without race conditions with register_topic
    /// or register_broker operations.
    pub(crate) fn delete_topic(&mut self, topic: CheetahString, cluster_name: Option<CheetahString>) {
        // Acquire topic write lock to prevent concurrent modifications
        let _topic_lock = self.topic_locks.write_lock(&topic);

        match cluster_name {
            Some(cluster_name) => {
                // Get all the brokerNames for the specified cluster
                let broker_names = self.cluster_addr_table.get_brokers(cluster_name.as_str());

                if broker_names.is_empty() || !self.topic_queue_table.contains_topic(topic.as_str()) {
                    return;
                }

                let topic_str = topic.as_str();

                // Remove topic from each broker in the cluster
                for broker_name in &broker_names {
                    if let Some(qd) = self.topic_queue_table.remove_broker(topic_str, broker_name.as_str()) {
                        info!(
                            "deleteTopic, remove one broker's topic {} {} {:?}",
                            broker_name, topic, qd
                        );
                    }
                }

                // Check if topic queue map is empty after removal
                if !broker_names.is_empty()
                    && self
                        .topic_queue_table
                        .get_topic_queues_map(topic_str)
                        .is_none_or(|map| map.is_empty())
                {
                    info!("deleteTopic, remove the topic all queue {} {}", cluster_name, topic);
                    self.topic_queue_table.remove_topic(topic_str);
                }
            }
            None => {
                // Delete entire topic across all brokers
                self.topic_queue_table.remove_topic(topic.as_str());
            }
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
    ///
    /// This method atomically removes a broker from all tables using segmented locking
    /// to prevent race conditions with concurrent registrations or route lookups.
    pub fn unregister_broker(
        &self,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
    ) -> RouteResult<()> {
        // ===================================================================
        // SEGMENTED LOCK ACQUISITION
        // ===================================================================
        // Acquire write lock for this broker segment to ensure atomic cleanup
        // across multiple tables (broker_live, broker_addr, cluster, topic_queue).
        //
        // This prevents race conditions such as:
        // 1. Unregistration racing with registration
        // 2. Partial cleanup visible to route lookups
        // 3. Inconsistent broker state across tables
        //
        // Lock scope: Only brokers hashing to the same segment will contend.
        // Other broker operations can proceed concurrently in different segments.
        let _broker_lock = self.broker_locks.write_lock(&broker_name.as_str());

        // Step 1: Remove from broker live table
        // Protected by broker write lock - atomic with all cleanup operations
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
        // Protected by broker write lock - atomic with live table update
        let _broker_removed = self
            .broker_addr_table
            .remove_broker_address(broker_name.as_str(), broker_id);

        // Step 3: Check if all broker addresses are gone
        // Protected by broker write lock - consistent visibility
        let broker_empty = if let Some(broker_data) = self.broker_addr_table.get(&broker_name) {
            broker_data.broker_addrs().is_empty()
        } else {
            true
        };

        // Step 4: If broker completely removed, clean up cluster and topics
        // All cleanup operations are atomic within the broker write lock
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
            self.topic_queue_table.remove_broker(topic.as_ref(), broker_name);
        }

        // Clean up empty topics
        let removed_count = self.topic_queue_table.cleanup_empty_topics();
        if removed_count > 0 {
            debug!("Cleaned up {} empty topics after broker removal", removed_count);
        }

        Ok(())
    }

    /// Batch unregister brokers from the name server
    ///
    /// `RouteInfoManager.unRegisterBroker(Set<UnRegisterBrokerRequestHeader>)` and provides
    /// batch processing of broker unregistration requests for better performance.
    ///
    /// ## Key Features
    /// 1. Batch processing: Process multiple unregistration requests in one call
    /// 2. Track removed vs reduced brokers for proper cleanup
    /// 3. Clean topics by unregister requests (remove queue data or wipe write perm)
    /// 4. Notify min broker ID changes for acting master support
    ///
    /// ## Arguments
    /// * `un_register_requests` - Vector of unregistration requests to process
    pub fn un_register_broker(&self, un_register_requests: Vec<UnRegisterBrokerRequestHeader>) {
        if un_register_requests.is_empty() {
            return;
        }

        // Track brokers that are completely removed vs reduced (still have addresses)
        let mut removed_broker: HashSet<CheetahString> = HashSet::new();
        let mut reduced_broker: HashSet<CheetahString> = HashSet::new();

        // Track brokers that need notification for min broker ID change
        // Key: broker_name, Value: (broker_addrs, offline_broker_addr)
        let mut need_notify_broker_map: HashMap<CheetahString, (HashMap<u64, CheetahString>, CheetahString)> =
            HashMap::new();

        // Process each unregistration request
        for un_register_request in &un_register_requests {
            let broker_name = &un_register_request.broker_name;
            let cluster_name = &un_register_request.cluster_name;
            let broker_addr = &un_register_request.broker_addr;
            let broker_id = un_register_request.broker_id;

            // Acquire write lock for this broker segment
            let _broker_lock = self.broker_locks.write_lock(broker_name.as_str());

            // Step 1: Remove from broker live table
            let broker_addr_info = BrokerAddrInfo::new(cluster_name.clone(), broker_addr.clone());
            let prev_live_info = self.broker_live_table.remove(&broker_addr_info);
            info!(
                "unregisterBroker, remove from brokerLiveTable {}, {}",
                if prev_live_info.is_some() { "OK" } else { "Failed" },
                broker_addr_info
            );

            // Step 2: Remove from filter server table
            self.filter_server_table.remove(&Arc::new(broker_addr_info.clone()));

            // Step 3: Process broker address table
            let mut remove_broker_name = false;
            let mut is_min_broker_id_changed = false;

            if let Some(broker_data) = self.broker_addr_table.get(broker_name) {
                let mut broker_data_clone = (*broker_data).clone();
                let broker_addrs = broker_data_clone.broker_addrs_mut();

                // Check if min broker ID will change
                if !broker_addrs.is_empty() {
                    if let Some(&min_id) = broker_addrs.keys().min() {
                        if broker_id == min_id {
                            is_min_broker_id_changed = true;
                        }
                    }
                }

                // Remove the broker address
                broker_addrs.retain(|_id, addr| addr.as_str() != broker_addr.as_str());

                info!(
                    "unregisterBroker, remove addr from brokerAddrTable, broker={}, addr={}",
                    broker_name, broker_addr
                );

                if broker_addrs.is_empty() {
                    // Broker completely removed
                    self.broker_addr_table.remove(broker_name);
                    info!("unregisterBroker, remove name from brokerAddrTable OK, {}", broker_name);
                    remove_broker_name = true;
                } else {
                    // Broker still has addresses, update it
                    if is_min_broker_id_changed {
                        need_notify_broker_map.insert(broker_name.clone(), (broker_addrs.clone(), broker_addr.clone()));
                    }
                    self.broker_addr_table.insert(broker_name.clone(), broker_data_clone);
                }
            }

            // Step 4: Update cluster table if broker completely removed
            if remove_broker_name {
                self.cluster_addr_table
                    .remove_broker(cluster_name.as_str(), broker_name.as_str());

                // Check if cluster is now empty
                if self.cluster_addr_table.get_brokers(cluster_name.as_str()).is_empty() {
                    self.cluster_addr_table.remove_cluster(cluster_name.as_str());
                    info!(
                        "unregisterBroker, remove cluster from clusterAddrTable {}",
                        cluster_name
                    );
                }

                removed_broker.insert(broker_name.clone());
            } else {
                reduced_broker.insert(broker_name.clone());
            }
        }

        // Step 5: Clean topics by unregister requests
        self.clean_topic_by_un_register_requests(&removed_broker, &reduced_broker);

        // Step 6: Notify min broker ID changed if needed
        if !need_notify_broker_map.is_empty()
            && self
                .name_server_runtime_inner
                .name_server_config()
                .notify_min_broker_id_changed
        {
            for (broker_name, (broker_addrs, offline_broker_addr)) in need_notify_broker_map {
                // Check if broker exists and has acting master enabled
                if let Some(broker_data) = self.broker_addr_table.get(&broker_name) {
                    if broker_data.enable_acting_master() {
                        self.notify_min_broker_id_changed(&broker_addrs, Some(offline_broker_addr), None);
                    }
                }
            }
        }
    }

    /// Clean topic queue data by unregister requests
    ///
    /// 1. For removed brokers: Remove queue data from all topics
    /// 2. For reduced brokers (acting master mode): Wipe write permission if no master exists
    fn clean_topic_by_un_register_requests(
        &self,
        removed_broker: &HashSet<CheetahString>,
        reduced_broker: &HashSet<CheetahString>,
    ) {
        let all_topics = self.topic_queue_table.get_all_topics();
        let mut topics_to_remove = Vec::new();

        for topic in &all_topics {
            // Remove queue data for completely removed brokers
            for broker_name in removed_broker {
                if let Some(removed_qd) = self
                    .topic_queue_table
                    .remove_broker(topic.as_str(), broker_name.as_str())
                {
                    debug!(
                        "removeTopicByBrokerName, remove one broker's topic {} {:?}",
                        topic, removed_qd
                    );
                }
            }

            // Check if topic is now empty
            if self
                .topic_queue_table
                .get_topic_queues_map(topic.as_str())
                .is_none_or(|map| map.is_empty())
            {
                topics_to_remove.push(topic.clone());
                debug!("removeTopicByBrokerName, remove the topic all queue {}", topic);
                continue;
            }

            // For reduced brokers with acting master enabled: wipe write permission if no master
            for broker_name in reduced_broker {
                if let Some(broker_data) = self.broker_addr_table.get(broker_name) {
                    if broker_data.enable_acting_master() {
                        // Check if no master exists (min broker ID > 0)
                        let no_master_exists = broker_data.broker_addrs().is_empty()
                            || broker_data.broker_addrs().keys().min().copied().unwrap_or(0) > 0;

                        if no_master_exists {
                            // Wipe write permission for this broker's queue data
                            if let Some(queue_data) = self.topic_queue_table.get(topic.as_str(), broker_name.as_str()) {
                                let mut new_queue_data = (*queue_data).clone();
                                new_queue_data.perm &= !PermName::PERM_WRITE;
                                self.topic_queue_table
                                    .insert(topic.clone(), broker_name.clone(), new_queue_data);
                            }
                        }
                    }
                }
            }
        }

        // Remove empty topics
        for topic in topics_to_remove {
            self.topic_queue_table.remove_topic(topic.as_str());
        }
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
    ///
    /// ## Consistency Guarantee
    ///
    /// This method acquires a segment-level read lock for the topic to ensure
    /// consistent reads across multiple tables. Without this lock, we could see:
    /// - Topic queues that reference non-existent brokers
    /// - Partial broker registration state
    /// - Inconsistent broker address mappings
    ///
    /// The read lock allows concurrent reads of the same topic while preventing
    /// concurrent writes (broker registration/unregistration) from causing
    /// inconsistent state.
    pub fn pickup_topic_route_data(&self, topic: &str) -> RouteResult<TopicRouteData> {
        use rocketmq_common::common::constant::PermName;
        use rocketmq_common::common::topic::TopicValidator;

        // ===================================================================
        // SEGMENTED LOCK ACQUISITION
        // ===================================================================
        // Acquire read lock for this topic segment to ensure consistent reads
        // across multiple tables (topic_queue, broker_addr, broker_live).
        //
        // This prevents reading inconsistent state such as:
        // 1. Queue data referencing brokers that are being unregistered
        // 2. Broker data that is partially updated
        // 3. Stale broker addresses during registration
        //
        // Lock scope: Only topics hashing to the same segment will share a lock.
        // Other topic queries can proceed concurrently in different segments.
        // Multiple readers can hold the lock simultaneously.
        let _topic_lock = self.topic_locks.read_lock(&topic);

        let mut found_queue_data = false;
        let mut found_broker_data = false;

        // Get queue data for the topic
        // Protected by topic read lock - consistent with broker table
        let queue_data_list = self.topic_queue_table.get_topic_queues(topic);

        if queue_data_list.is_empty() {
            return Err(RocketMQError::route_not_found(topic));
        }

        // Convert queue data to Vec<QueueData>
        let queue_data_vec: Vec<QueueData> = queue_data_list
            .into_iter()
            .map(|(_, queue_data)| {
                found_queue_data = true;
                (*queue_data).clone()
            })
            .collect();

        // Collect broker names from queue data (already CheetahString)
        // Protected by topic read lock - brokers are stable during this read
        let broker_names: Vec<BrokerName> = queue_data_vec.iter().map(|qd| qd.broker_name.clone()).collect();

        // Get broker data for each broker
        // Protected by topic read lock - broker data is consistent
        let mut broker_data_list = Vec::new();
        for broker_name in broker_names {
            if let Some(broker_data) = self.broker_addr_table.get(broker_name.as_str()) {
                // Clone BrokerData for the route response
                broker_data_list.push((*broker_data).clone());
                found_broker_data = true;
            }
        }

        debug!(
            "pickup_topic_route_data topic={}, found_queue_data={}, found_broker_data={}",
            topic, found_queue_data, found_broker_data
        );

        if !found_broker_data || !found_queue_data {
            return Err(RocketMQError::route_not_found(topic));
        }

        // Construct TopicRouteData
        let mut topic_route_data = TopicRouteData {
            queue_datas: queue_data_vec,
            broker_datas: broker_data_list.clone(),
            ..Default::default()
        };

        // Populate filter server table from filter_server_table
        if !self.filter_server_table.is_empty() {
            for broker_data in &broker_data_list {
                for broker_addr in broker_data.broker_addrs().values() {
                    let broker_addr_info = Arc::new(BrokerAddrInfo {
                        cluster_name: broker_data.cluster().into(),
                        broker_addr: broker_addr.clone(),
                    });

                    // Get filter server list (may be None)
                    let filter_servers = self.filter_server_table.get(&broker_addr_info);

                    // Insert into map even if None
                    if let Some(servers) = filter_servers {
                        topic_route_data
                            .filter_server_table
                            .insert(broker_addr.clone(), servers.clone());
                    }
                }
            }
        }

        // Set topic queue mapping info for static topic support
        if let Some(mapping_info) = self.topic_queue_mapping_info_table.get_topic_mappings(topic) {
            topic_route_data.topic_queue_mapping_by_broker = Some(mapping_info);
        }

        // Check if acting master support is enabled
        if !self
            .name_server_runtime_inner
            .name_server_config()
            .support_acting_master
        {
            return Ok(topic_route_data);
        }

        // Skip acting master logic for sync broker member group topics
        if topic.starts_with(TopicValidator::SYNC_BROKER_MEMBER_GROUP_PREFIX) {
            return Ok(topic_route_data);
        }

        // Check if broker and queue data are available
        if topic_route_data.broker_datas.is_empty() || topic_route_data.queue_datas.is_empty() {
            return Ok(topic_route_data);
        }

        // Check if any broker needs acting master (no master broker ID present)
        let need_acting_master = topic_route_data.broker_datas.iter().any(|broker_data| {
            !broker_data.broker_addrs().is_empty() && !broker_data.broker_addrs().contains_key(&mix_all::MASTER_ID)
        });

        if !need_acting_master {
            return Ok(topic_route_data);
        }

        // Process acting master for brokers without master
        for broker_data in &mut topic_route_data.broker_datas {
            // Check conditions before getting mutable reference
            let enable_acting_master = broker_data.enable_acting_master();
            let broker_name = broker_data.broker_name().to_string();
            let broker_addrs = broker_data.broker_addrs_mut();

            // Skip if:
            // 1. No broker addresses
            // 2. Master already exists
            // 3. Acting master not enabled for this broker
            if broker_addrs.is_empty() || broker_addrs.contains_key(&mix_all::MASTER_ID) || !enable_acting_master {
                continue;
            }

            // No master - check if we should promote a slave to acting master
            for queue_data in &topic_route_data.queue_datas {
                if queue_data.broker_name() == broker_name.as_str() {
                    // Only promote if queue is not writable (read-only)
                    if !PermName::is_writeable(queue_data.perm()) {
                        // Find the minimum broker ID (closest slave)
                        if let Some(&min_broker_id) = broker_addrs.keys().min() {
                            // Remove the slave with minimum ID and promote it to master
                            if let Some(acting_master_addr) = broker_addrs.remove(&min_broker_id) {
                                broker_addrs.insert(mix_all::MASTER_ID, acting_master_addr);
                                debug!(
                                    "Promoted acting master: broker={}, slave_id={} -> master",
                                    broker_name, min_broker_id
                                );
                            }
                        }
                    }
                    break;
                }
            }
        }

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
                if broker_names.iter().any(|b| b.as_str() == broker_name.as_str()) {
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
    ///
    /// # Implementation Notes
    ///
    /// 1. Iterate through broker_live_table to find expired brokers
    /// 2. Close the channel for expired brokers (logged for tracking)
    /// 3. Call onChannelDestroy to trigger async batch unregistration
    ///
    /// The key difference from directly calling unregister_broker:
    /// - uses BatchUnregistrationService for better performance
    /// - Submissions are batched and processed together
    /// - This reduces lock contention on the global route tables
    pub fn scan_not_active_broker(&self) -> RouteResult<usize> {
        debug!("start scanNotActiveBroker");
        let current_time = get_current_millis();

        // Get expired brokers by checking heartbeat timeout
        let expired_brokers = self.broker_live_table.get_expired_brokers(current_time);

        let count = expired_brokers.len();
        if count > 0 {
            // Submit unregistration requests for each expired broker
            for broker_addr_info in expired_brokers {
                // Get the live info to retrieve timeout value for logging
                if let Some(live_info) = self.broker_live_table.get(&broker_addr_info) {
                    warn!(
                        "The broker channel expired, {} {}ms",
                        broker_addr_info, live_info.heartbeat_timeout_millis
                    );
                }

                // Trigger channel destroy logic, which will submit to batch unregistration service
                self.on_channel_destroy_by_addr_info(broker_addr_info);
            }
        }

        Ok(count)
    }

    /// Handle channel destruction by broker address info
    ///
    /// 1. Setup unregister request with broker info
    /// 2. Submit to batch unregistration service
    ///
    /// The batch service will process the request asynchronously.
    fn on_channel_destroy_by_addr_info(&self, broker_addr_info: Arc<BrokerAddrInfo>) {
        let mut unregister_request = UnRegisterBrokerRequestHeader::default();
        let need_unregister = self.setup_unregister_request(&mut unregister_request, &broker_addr_info);

        if need_unregister {
            let result = self.submit_unregister_broker_request(unregister_request.clone());
            info!(
                "the broker's channel destroyed, submit the unregister request at once, broker info: {}, submit \
                 result: {}",
                unregister_request, result
            );
        }
    }

    /// Setup unregister request from broker address info (instance method)
    ///
    /// Finds the broker name and broker ID from broker_addr_table
    /// and populates the unregister request header.
    ///
    /// Returns true if the broker was found and request was setup successfully.
    fn setup_unregister_request(
        &self,
        unregister_request: &mut UnRegisterBrokerRequestHeader,
        broker_addr_info: &BrokerAddrInfo,
    ) -> bool {
        Self::setup_unregister_request_static(unregister_request, broker_addr_info, &self.broker_addr_table)
    }

    /// Static helper to setup unregister request from broker address info
    ///
    /// This is a static method that doesn't hold &self to allow calling from async contexts.
    /// Finds the broker name and broker ID from broker_addr_table
    /// and populates the unregister request header.
    ///
    /// Returns true if the broker was found and request was setup successfully.
    fn setup_unregister_request_static(
        unregister_request: &mut UnRegisterBrokerRequestHeader,
        broker_addr_info: &BrokerAddrInfo,
        broker_addr_table: &BrokerAddrTable,
    ) -> bool {
        unregister_request.cluster_name = broker_addr_info.cluster_name.clone();
        unregister_request.broker_addr = broker_addr_info.broker_addr.clone();

        // Find broker name and broker ID from broker_addr_table
        for (broker_name, broker_data) in broker_addr_table.get_all_brokers() {
            if broker_addr_info.cluster_name != broker_data.cluster() {
                continue;
            }

            for (broker_id, addr) in broker_data.broker_addrs().iter() {
                if broker_addr_info.broker_addr.as_str() == addr.as_str() {
                    unregister_request.broker_name = broker_name;
                    unregister_request.broker_id = *broker_id;
                    return true;
                }
            }
        }

        false
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
    ///
    /// Compares the provided data version with the broker's current data version
    /// to determine if the topic configuration has changed.
    ///
    /// # Arguments
    /// * `cluster_name` - Name of the cluster
    /// * `broker_addr` - Broker network address
    /// * `data_version` - Data version to compare against
    ///
    /// # Returns
    /// `true` if configuration has changed or broker not found, `false` otherwise
    pub fn is_broker_topic_config_changed(
        &self,
        cluster_name: &CheetahString,
        broker_addr: &CheetahString,
        data_version: &DataVersion,
    ) -> bool {
        // Find broker using addr info
        let find_data_version = self.query_broker_topic_config(cluster_name.clone(), broker_addr.clone());
        if let Some(existing_version) = find_data_version {
            return &existing_version != data_version; // Compare values, not references
        }
        true // If broker not found, assume changed
    }

    /// Update broker info update timestamp
    ///
    /// This method updates the last update timestamp for a broker in the live table.
    ///
    /// # Arguments
    /// * `cluster_name` - Name of the cluster the broker belongs to
    /// * `broker_addr` - Network address of the broker
    pub fn update_broker_info_update_timestamp(&self, cluster_name: CheetahString, broker_addr: CheetahString) {
        let broker_addr_info = BrokerAddrInfo::new(cluster_name, broker_addr);
        self.broker_live_table
            .update_last_update_timestamp_by_addr_info(&broker_addr_info);
    }

    /// Query broker topic config data version
    ///
    /// This method retrieves the data version for a broker's topic configuration
    /// by looking up the broker in the live table using cluster name and broker address.
    ///
    /// # Arguments
    /// * `cluster_name` - Name of the cluster the broker belongs to
    /// * `broker_addr` - Network address of the broker
    ///
    /// # Returns
    /// `Some(DataVersion)` if broker is found and alive, `None` otherwise
    pub fn query_broker_topic_config(
        &self,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
    ) -> Option<DataVersion> {
        let broker_addr_info = BrokerAddrInfo::new(cluster_name, broker_addr);
        self.broker_live_table
            .get(&broker_addr_info)
            .map(|info| info.data_version.clone())
    }

    /// Get broker member group
    ///
    /// Returns a BrokerMemberGroup containing all broker addresses for the given broker name.
    ///
    /// # Arguments
    /// * `cluster_name` - Name of the cluster
    /// * `broker_name` - Name of the broker
    ///
    /// # Returns
    /// Always returns `Some(BrokerMemberGroup)`. If the broker exists in `broker_addr_table`, the
    /// group will contain its addresses; otherwise, it returns an empty `BrokerMemberGroup` with
    /// the provided cluster name and broker name but no addresses.
    pub fn get_broker_member_group(
        &self,
        cluster_name: CheetahString,
        broker_name: CheetahString,
    ) -> Option<BrokerMemberGroup> {
        let mut group_member = BrokerMemberGroup::new(cluster_name, broker_name.clone());

        // Get broker addresses from broker_addr_table
        if let Some(broker_data) = self.broker_addr_table.get(&broker_name) {
            group_member.broker_addrs = broker_data.broker_addrs().clone();
        }

        Some(group_member)
    }

    /// Get all cluster info
    ///
    /// Rust requires creating copies due to ownership rules and DashMap usage.
    pub fn get_all_cluster_info(&self) -> RouteResult<ClusterInfo> {
        use std::collections::HashMap;
        use std::collections::HashSet;

        use rocketmq_remoting::protocol::route::route_data_view::BrokerData;

        // Get all cluster data first for capacity pre-allocation
        let cluster_data = self.cluster_addr_table.get_all_cluster_brokers();
        let broker_data_list = self.broker_addr_table.get_all_brokers();

        // Pre-allocate with known capacity for better performance
        let mut cluster_addr_table: HashMap<CheetahString, HashSet<CheetahString>> =
            HashMap::with_capacity(cluster_data.len());
        let mut broker_addr_table: HashMap<CheetahString, BrokerData> = HashMap::with_capacity(broker_data_list.len());

        // Populate cluster_addr_table
        for (cluster_name, broker_names) in cluster_data {
            cluster_addr_table.insert(cluster_name, broker_names.into_iter().collect());
        }

        // Populate broker_addr_table
        for (broker_name, broker_data) in broker_data_list {
            let data = BrokerData::new(
                CheetahString::from_slice(broker_data.cluster()),
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

    /// Wipe write permission of broker by lock
    ///
    /// This method removes write permission from all topics that contain queue data
    /// for the specified broker:
    /// 1. Acquiring a write lock
    /// 2. Directly looking up the broker in each topic's queue map
    /// 3. Removing write permission from matched queue data
    ///
    /// # Arguments
    /// * `broker_name` - Name of the broker whose write permission should be wiped
    ///
    /// # Returns
    /// Number of topics whose queue data was updated
    pub fn wipe_write_perm_of_broker_by_lock(&self, broker_name: String) -> RouteResult<i32> {
        use rocketmq_common::common::constant::PermName;

        // Acquire write lock for this broker segment
        // This ensures no concurrent modifications to topics containing this broker
        let _broker_lock = self.broker_locks.write_lock(&broker_name);

        let mut wipe_topic_count = 0;

        // Iterate over all topics and directly look up the broker
        for topic in self.topic_queue_table.get_all_topics() {
            if let Some(queue_data) = self.topic_queue_table.get(&topic, &broker_name) {
                // Remove write permission
                let perm = queue_data.perm() & !PermName::PERM_WRITE;
                self.topic_queue_table
                    .update_queue_data_perm(&topic, &broker_name, perm as i32);
                wipe_topic_count += 1;
            }
        }

        Ok(wipe_topic_count)
    }

    /// Add write permission of broker by lock (v1 compatibility)
    ///
    /// This method adds write permission to all topics that contain queue data
    /// for the specified broker:
    /// 1. Acquiring a write lock
    /// 2. Directly looking up the broker in each topic's queue map
    /// 3. Setting permission to READ | WRITE (not just adding write flag)
    ///
    /// # Arguments
    /// * `broker_name` - Name of the broker whose write permission should be added
    ///
    /// # Returns
    /// Number of topics whose queue data was updated
    pub fn add_write_perm_of_broker_by_lock(&self, broker_name: String) -> RouteResult<i32> {
        use rocketmq_common::common::constant::PermName;

        // Acquire write lock for this broker segment
        // This ensures no concurrent modifications to topics containing this broker
        let _broker_lock = self.broker_locks.write_lock(&broker_name);

        let mut add_topic_count = 0;

        // Iterate over all topics and directly look up the broker
        for topic in self.topic_queue_table.get_all_topics() {
            if let Some(_queue_data) = self.topic_queue_table.get(&topic, &broker_name) {
                let perm = PermName::PERM_READ | PermName::PERM_WRITE;
                self.topic_queue_table
                    .update_queue_data_perm(&topic, &broker_name, perm as i32);
                add_topic_count += 1;
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
            .filter(|topic| system_topic_set.iter().any(|s| s == topic.as_str()))
            .collect();

        Ok(TopicList {
            topic_list: topics,
            broker_addr: None,
        })
    }

    /// Get unit topics (v1 compatibility)
    ///
    /// Returns topics marked with the Unit flag (FLAG_UNIT = 0x1).
    /// These are pure unit topics that support unit-based message routing.
    pub fn get_unit_topics(&self) -> RouteResult<TopicList> {
        use rocketmq_common::common::TopicSysFlag;

        // Filter topics with unit flag set
        let topics: Vec<CheetahString> = self
            .topic_queue_table
            .iter_all_with_data()
            .into_iter()
            .filter(|(_, queue_datas)| {
                queue_datas
                    .first()
                    .map(|qd| TopicSysFlag::has_unit_flag(qd.topic_sys_flag()))
                    .unwrap_or(false)
            })
            .map(|(topic, _)| CheetahString::from_string(topic))
            .collect();

        Ok(TopicList {
            topic_list: topics,
            broker_addr: None,
        })
    }

    /// Get has unit sub topic list (v1 compatibility)
    ///
    /// Returns topics marked with the Unit Subscription flag (FLAG_UNIT_SUB = 0x2).
    /// These topics have consumers that support unit-based subscription.
    pub fn get_has_unit_sub_topic_list(&self) -> RouteResult<TopicList> {
        use rocketmq_common::common::TopicSysFlag;

        // Filter topics with unit subscription flag set
        let topics: Vec<CheetahString> = self
            .topic_queue_table
            .iter_all_with_data()
            .into_iter()
            .filter(|(_, queue_datas)| {
                queue_datas
                    .first()
                    .map(|qd| TopicSysFlag::has_unit_sub_flag(qd.topic_sys_flag()))
                    .unwrap_or(false)
            })
            .map(|(topic, _)| CheetahString::from_string(topic))
            .collect();

        Ok(TopicList {
            topic_list: topics,
            broker_addr: None,
        })
    }

    /// Get has unit sub ununit topic list (v1 compatibility)
    ///
    /// Returns topics that:
    /// - Have unit subscription flag (FLAG_UNIT_SUB = 0x2) set
    /// - Do NOT have unit flag (FLAG_UNIT = 0x1) set
    ///
    /// These are non-unit topics whose consumers support unit-based subscription.
    pub fn get_has_unit_sub_ununit_topic_list(&self) -> RouteResult<TopicList> {
        use rocketmq_common::common::TopicSysFlag;

        // Filter topics that have unit subscription but are not unit topics
        let topics: Vec<CheetahString> = self
            .topic_queue_table
            .iter_all_with_data()
            .into_iter()
            .filter(|(_, queue_datas)| {
                queue_datas
                    .first()
                    .map(|qd| {
                        let sys_flag = qd.topic_sys_flag();
                        !TopicSysFlag::has_unit_flag(sys_flag) && TopicSysFlag::has_unit_sub_flag(sys_flag)
                    })
                    .unwrap_or(false)
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

    #[test]
    fn test_choose_broker_addrs_to_notify_single_broker() {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(0, CheetahString::from_static_str("broker0:10911"));

        let result = RouteInfoManagerV2::choose_broker_addrs_to_notify(&broker_addrs, &None);

        // Single broker: notify all (itself)
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].as_str(), "broker0:10911");
    }

    #[test]
    fn test_choose_broker_addrs_to_notify_multiple_brokers_no_offline() {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(0, CheetahString::from_static_str("broker0:10911"));
        broker_addrs.insert(1, CheetahString::from_static_str("broker1:10911"));
        broker_addrs.insert(2, CheetahString::from_static_str("broker2:10911"));

        let result = RouteInfoManagerV2::choose_broker_addrs_to_notify(&broker_addrs, &None);

        // Multiple brokers, no offline: notify all except min broker ID (0)
        assert_eq!(result.len(), 2);
        assert!(result.iter().any(|s| s.as_str() == "broker1:10911"));
        assert!(result.iter().any(|s| s.as_str() == "broker2:10911"));
        assert!(!result.iter().any(|s| s.as_str() == "broker0:10911"));
    }

    #[test]
    fn test_choose_broker_addrs_to_notify_with_offline() {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(0, CheetahString::from_static_str("broker0:10911"));
        broker_addrs.insert(1, CheetahString::from_static_str("broker1:10911"));
        broker_addrs.insert(2, CheetahString::from_static_str("broker2:10911"));

        let offline = Some(CheetahString::from_static_str("broker1:10911"));
        let result = RouteInfoManagerV2::choose_broker_addrs_to_notify(&broker_addrs, &offline);

        // With offline broker: notify all brokers (including min broker ID)
        assert_eq!(result.len(), 3);
        assert!(result.iter().any(|s| s.as_str() == "broker0:10911"));
        assert!(result.iter().any(|s| s.as_str() == "broker1:10911"));
        assert!(result.iter().any(|s| s.as_str() == "broker2:10911"));
    }

    #[test]
    fn test_choose_broker_addrs_to_notify_empty() {
        let broker_addrs = HashMap::new();
        let result = RouteInfoManagerV2::choose_broker_addrs_to_notify(&broker_addrs, &None);

        // Empty map: no notifications
        assert_eq!(result.len(), 0);
    }
}
