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

//! Canonical RouteInfoManager with immutable route snapshot publication
//!
//! Mutable DashMap tables form the write model. A single mutation gate protects
//! cross-table updates, then complete per-topic snapshots are atomically published.

use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use futures::StreamExt;
use rocketmq_model::common::constant::PermName;
use rocketmq_model::common::mix_all;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_protocol::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::brokerid_change_request_header::NotifyMinBrokerIdChangeRequestHeader;
use rocketmq_protocol::protocol::namesrv::RegisterBrokerResult;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
use rocketmq_protocol::protocol::route::route_data_view::QueueData;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::DataVersion;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_transport::api::SessionId;
use rocketmq_transport::api::SessionRegistry;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::bootstrap::NameServerRuntimeHandle;
use crate::route::batch_unregistration_service::BatchUnregistrationService;
use crate::route::batch_unregistration_service::BrokerUnregistrationRequest;
use crate::route::error::RocketMQError;
use crate::route::error::RouteResult;
use crate::route::tables::BrokerAddrTable;
use crate::route::tables::BrokerLiveInfo;
use crate::route::tables::BrokerLiveTable;
use crate::route::tables::ClusterAddrTable;
use crate::route::tables::FilterServerTable;
use crate::route::tables::TopicQueueMappingInfoTable;
use crate::route::tables::TopicQueueTable;
use crate::route::topic_route_snapshot::RouteMutationCoordinator;
use crate::route::topic_route_snapshot::RouteMutationGuard;
use crate::route::topic_route_snapshot::TopicRouteView;
use crate::route::types::BrokerName;
use crate::route::types::BrokerSession;
use crate::route::types::RemotingConnectionId;
use crate::route::types::TopicName;
use crate::route_info::broker_addr_info::BrokerAddrInfo;

mod management;

const DEFAULT_BROKER_CHANNEL_EXPIRED_TIME: u64 = 1000 * 60 * 2; // 2 minutes

/// Canonical route manager with serialized mutations and immutable route snapshots
///
/// Key properties:
/// - Coherent reads: one atomic per-topic snapshot load
/// - Serialized writes: one mutation gate covers every route-visible source-table update
/// - One coordinator avoids redundant nested lock layers on write paths
/// - Compact strings: CheetahString avoids repeated temporary conversions
/// - Type-safe errors: Result<T, RocketMQError> instead of Option
/// - Better modularity: Separate table modules for maintainability
///
/// ## Concurrency Model
///
/// This implementation separates the mutable write model from the immutable read model:
///
/// 1. **Mutation gate**: All route-visible source-table mutations are serialized. This first
///    version intentionally accepts write contention to make cross-table publication provable.
///
/// 2. **Source tables**: DashMap tables retain the converged broker/topic/filter/mapping model
///    used by mutations and management queries.
///
/// 3. **Published snapshots**: Route lookups atomically load one complete topic snapshot and do
///    not revisit mutable source tables.
///
/// ### Lock Acquisition Strategy
///
/// - **Route-visible writes**: Acquire the single mutation gate.
/// - **Route reads**: Load one immutable snapshot without the mutation gate.
/// - **Management reads**: Hold the mutation gate while assembling source-table DTOs.
///
/// ### Deadlock Prevention
///
/// The mutation gate is the only coordinator for route-visible writes.
///
/// ## Performance Characteristics
///
/// - **Route read**: one ArcSwap load plus a response clone
/// - **Route write**: serialized source mutation plus rebuilding each affected topic
/// - **Tradeoff**: higher write contention and write amplification for coherent read semantics
pub struct RouteInfoManager {
    // Mutable source tables used by the write model and management queries.
    topic_queue_table: TopicQueueTable,
    broker_addr_table: BrokerAddrTable,
    cluster_addr_table: ClusterAddrTable,
    broker_live_table: BrokerLiveTable,
    filter_server_table: FilterServerTable,
    topic_queue_mapping_info_table: TopicQueueMappingInfoTable,
    route_mutations: RouteMutationCoordinator,

    // Runtime and lifecycle components
    name_server_runtime_inner: NameServerRuntimeHandle,
    session_registry: Arc<SessionRegistry>,
    un_register_service: Arc<BatchUnregistrationService>,
    metrics: rocketmq_observability::metrics::namesrv::NameServerMetrics,
    expiry_safety_scan_interval: u64,
    last_expiry_safety_scan: AtomicU64,
    min_broker_notify_concurrency: usize,
    min_broker_notify_sequence: AtomicU64,
    min_broker_notify_versions: Arc<dashmap::DashMap<CheetahString, u64>>,

    #[cfg(test)]
    before_topic_cleanup_hook: parking_lot::Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    route_freshness_lookups: AtomicU64,
}

impl RouteInfoManager {
    /// Create a route manager with mutable source tables and snapshot publication.
    pub(crate) fn new(
        name_server_runtime_inner: NameServerRuntimeHandle,
        session_registry: Arc<SessionRegistry>,
        queue_capacity: usize,
        unregister_batch_size: usize,
        unregister_batch_time: std::time::Duration,
        expiry_index_mode: crate::config::ExpiryIndexMode,
        expiry_safety_scan_interval: u64,
        min_broker_notify_concurrency: usize,
        metrics: rocketmq_observability::metrics::namesrv::NameServerMetrics,
    ) -> Self {
        let un_register_service = Arc::new(BatchUnregistrationService::new(
            name_server_runtime_inner.clone(),
            queue_capacity,
            unregister_batch_size,
            unregister_batch_time,
            metrics.clone(),
        ));

        Self {
            // Initialize with estimated capacities based on typical cluster sizes
            topic_queue_table: TopicQueueTable::with_capacity(1024),
            broker_addr_table: BrokerAddrTable::with_capacity(128),
            cluster_addr_table: ClusterAddrTable::with_capacity(32),
            broker_live_table: BrokerLiveTable::with_capacity_and_expiry_index(256, expiry_index_mode),
            filter_server_table: FilterServerTable::with_capacity(128),
            topic_queue_mapping_info_table: TopicQueueMappingInfoTable::with_capacity(256),
            route_mutations: RouteMutationCoordinator::with_metrics(metrics.clone()),

            name_server_runtime_inner,
            session_registry,
            un_register_service,
            metrics,
            expiry_safety_scan_interval,
            last_expiry_safety_scan: AtomicU64::new(0),
            min_broker_notify_concurrency,
            min_broker_notify_sequence: AtomicU64::new(0),
            min_broker_notify_versions: Arc::new(dashmap::DashMap::new()),

            #[cfg(test)]
            before_topic_cleanup_hook: parking_lot::Mutex::new(None),
            #[cfg(test)]
            route_freshness_lookups: AtomicU64::new(0),
        }
    }

    #[cfg(test)]
    fn set_before_topic_cleanup_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self.before_topic_cleanup_hook.lock() = Some(hook);
    }

    #[cfg(test)]
    fn run_before_topic_cleanup_hook(&self) {
        let hook = self.before_topic_cleanup_hook.lock().clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    /// Builds a complete route view from source tables while the mutation gate is held.
    fn build_topic_route_data(&self, topic: &str) -> Option<TopicRouteData> {
        let queue_datas = self
            .topic_queue_table
            .get_topic_queues(topic)
            .into_iter()
            .map(|(_, queue_data)| (*queue_data).clone())
            .collect::<Vec<_>>();
        if queue_datas.is_empty() {
            return None;
        }

        let broker_datas = queue_datas
            .iter()
            .map(|queue_data| {
                self.broker_addr_table
                    .get(queue_data.broker_name())
                    .map(|broker_data| (*broker_data).clone())
            })
            .collect::<Option<Vec<_>>>()?;

        let mut route_data = TopicRouteData {
            queue_datas,
            broker_datas,
            ..Default::default()
        };

        for broker_data in &route_data.broker_datas {
            for broker_addr in broker_data.broker_addrs().values() {
                let broker_addr_info = Arc::new(BrokerAddrInfo {
                    cluster_name: broker_data.cluster().into(),
                    broker_addr: broker_addr.clone(),
                });
                if let Some(filter_servers) = self.filter_server_table.get(&broker_addr_info) {
                    route_data
                        .filter_server_table
                        .insert(broker_addr.clone(), filter_servers);
                }
            }
        }

        route_data.topic_queue_mapping_by_broker = self.topic_queue_mapping_info_table.get_topic_mappings(topic);
        Some(route_data)
    }

    fn publish_topic_route_snapshots(&self, mutation: &RouteMutationGuard<'_>, affected_topics: HashSet<TopicName>) {
        for topic in affected_topics {
            let rebuild_started = std::time::Instant::now();
            let route_data = self.build_topic_route_data(topic.as_str());
            let present = route_data.is_some();
            mutation.publish(topic, route_data);
            mutation.record_snapshot_rebuild(rebuild_started.elapsed(), present);
        }
    }

    #[cfg(test)]
    fn topic_route_snapshot_version(&self, topic: &str) -> Option<u64> {
        self.route_mutations.load(topic).map(|snapshot| snapshot.version)
    }

    /// Start the route manager
    pub fn start(&self) {
        info!("Starting RouteInfoManager with DashMap tables");
        self.un_register_service.start();
    }

    /// Find broker info by stable transport session identity.
    #[inline]
    fn find_broker_by_session_id(
        broker_live_table: &BrokerLiveTable,
        session_id: SessionId,
    ) -> Option<Arc<BrokerAddrInfo>> {
        broker_live_table.get_broker_info_by_session_id(session_id)
    }

    /// Find broker info and live state by remote socket address.
    fn find_broker_by_remote_addr(
        broker_live_table: &BrokerLiveTable,
        socket_addr: SocketAddr,
    ) -> Option<(Arc<BrokerAddrInfo>, Arc<BrokerLiveInfo>)> {
        broker_live_table.get_broker_info_by_remote_addr(socket_addr)
    }

    /// Handle connection disconnection by socket address
    ///
    /// 1. Find broker info by socket address
    /// 2. Setup unregister request
    /// 3. Submit to batch unregistration service
    pub fn on_session_destroy(&self, session_id: SessionId) {
        let mut unregister_request = UnRegisterBrokerRequestHeader::default();
        let Some(broker_addr_info) = Self::find_broker_by_session_id(&self.broker_live_table, session_id) else {
            return;
        };
        let Some(live_info) = self.broker_live_table.get(&broker_addr_info) else {
            return;
        };
        let need_unregister =
            self.setup_unregister_request_from_live(&mut unregister_request, &broker_addr_info, &live_info);

        if need_unregister {
            let result = self.submit_unregister_broker_request_guarded(
                unregister_request.clone(),
                live_info.channel_id.clone(),
                live_info.generation(),
            );
            info!(
                "the broker's channel destroyed, submit the unregister request at once, broker info: {:?}, submit \
                 result: {}",
                unregister_request, result
            );
        }
    }

    /// Handle broker disconnection when only the remote socket address is available.
    pub fn connection_disconnected(&self, socket_addr: SocketAddr) {
        if let Some((broker_addr_info, live_info)) =
            Self::find_broker_by_remote_addr(&self.broker_live_table, socket_addr)
        {
            self.on_channel_destroy_by_addr_info_and_live(broker_addr_info, live_info);
        }
    }

    /// Shutdown the route manager
    pub async fn shutdown(&self) -> Option<rocketmq_runtime::ShutdownReport> {
        info!("Shutting down RouteInfoManager");
        self.un_register_service.shutdown().await
    }
}

// ============================================================================
// Broker Registration
// ============================================================================

impl RouteInfoManager {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn register_broker_session(
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
        broker_session: BrokerSession,
    ) -> RouteResult<RegisterBrokerResult> {
        let registration_span = rocketmq_observability::trace::namesrv::broker_registration_span();
        let _registration_guard = registration_span.enter();
        let route_mutation = self.route_mutations.begin_mutation();
        let old_topics = self.topic_set_of_broker_name(broker_name.as_str());
        let mut incoming_topics = topic_config_wrapper
            .topic_config_serialize_wrapper()
            .topic_config_table()
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        incoming_topics.extend(topic_config_wrapper.topic_queue_mapping_info_map().keys().cloned());
        let broker_addr_info = Arc::new(BrokerAddrInfo::new(cluster_name.clone(), broker_addr.clone()));
        let broker_topic_config_changed = self.is_broker_topic_config_changed(
            &cluster_name,
            &broker_addr,
            topic_config_wrapper.topic_config_serialize_wrapper().data_version(),
        );
        let mut result = RegisterBrokerResult::default();

        let cluster_name_arc = cluster_name.clone();
        let broker_name_arc = broker_name.clone();

        // Step 1: Validate and update the broker address table before mutating cluster membership.
        // Still unpublished under the mutation gate.
        let update_result = self.update_broker_addr_table(
            &cluster_name,
            &broker_name,
            broker_id,
            &broker_addr,
            zone_name.as_ref(),
            enable_acting_master,
            &topic_config_wrapper,
        )?;

        // Step 3: Check if broker registration should be rejected
        let Some((register_first, is_min_broker_id_changed, broker_topology_changed)) = update_result else {
            warn!(
                "Broker registration rejected due to version conflict: cluster={}, broker={}, id={}, addr={}",
                cluster_name, broker_name, broker_id, broker_addr
            );
            self.metrics.record_broker_registration(self.active_broker_count());
            self.metrics.record_registration_delta("rejected", 0);
            registration_span.record("result", "rejected");
            return Ok(result);
        };

        // Step 2: Update cluster membership only after registration validation succeeds.
        self.cluster_addr_table
            .add_broker(cluster_name_arc, broker_name_arc.clone());

        debug!(
            "Cluster membership updated: cluster={}, broker={}",
            cluster_name, broker_name
        );

        // Step 4: Update topic queue configurations
        // Still unpublished under the mutation gate.
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

        let mut dirty_topics = HashSet::new();
        if is_master || is_prime_slave {
            dirty_topics = self.update_topic_queue_table(
                &broker_name_arc,
                &cluster_name,
                &broker_addr,
                &topic_config_wrapper,
                register_first,
                is_prime_slave,
                broker_topic_config_changed,
            )?;
        }

        // Step 5: Register broker live status
        // Still unpublished under the mutation gate.
        let prev_broker_live_info = self.register_broker_live_info(
            cluster_name.clone(),
            broker_name.clone(),
            broker_id,
            broker_addr.clone(),
            timeout_millis,
            topic_config_wrapper
                .topic_config_serialize_wrapper()
                .data_version()
                .clone(),
            broker_session,
            ha_server_addr.clone(),
        )?;

        if prev_broker_live_info.is_none() {
            info!(
                "New broker registered: cluster={}, broker={}, id={}, addr={}, HAService: {}",
                cluster_name, broker_name, broker_id, broker_addr, ha_server_addr
            );
        }

        // Step 6: Handle filter server list
        let previous_filter_servers = self.filter_server_table.get(broker_addr_info.as_ref());
        let filter_servers_changed = match previous_filter_servers.as_ref() {
            Some(previous) => previous != &filter_server_list,
            None => !filter_server_list.is_empty(),
        };
        if filter_server_list.is_empty() {
            self.filter_server_table.remove(&broker_addr_info);
        } else {
            self.filter_server_table
                .register(broker_addr_info.clone(), filter_server_list);
        }

        let delta_enabled = self
            .name_server_runtime_inner
            .name_server_config()
            .enable_registration_delta;
        if !delta_enabled || broker_topology_changed || filter_servers_changed {
            dirty_topics.extend(old_topics);
            dirty_topics.extend(incoming_topics);
        }
        self.metrics.record_registration_delta(
            if dirty_topics.is_empty() { "no-op" } else { "changed" },
            dirty_topics.len(),
        );
        self.publish_topic_route_snapshots(&route_mutation, dirty_topics);

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
                self.notify_min_broker_id_changed(&broker_name, broker_data.broker_addrs(), None, ha_server_addr);
            }
        }

        debug!(
            "Broker registered: cluster={}, broker={}, id={}, addr={}, first={}",
            cluster_name, broker_name, broker_id, broker_addr, register_first
        );

        self.metrics.record_broker_registration(self.active_broker_count());
        registration_span.record("result", "success");
        Ok(result)
    }

    /// Update broker address table with conflict detection.
    ///
    /// Returns:
    /// - Ok(Some((register_first, is_min_broker_id_changed, topology_changed))): Success
    /// - Ok(None): Registration rejected due to version conflict
    /// - Err: Other errors
    fn update_broker_addr_table(
        &self,
        cluster_name: &CheetahString,
        broker_name: &CheetahString,
        broker_id: u64,
        broker_addr: &CheetahString,
        zone_name: Option<&CheetahString>,
        enable_acting_master: Option<bool>,
        topic_config_wrapper: &TopicConfigAndMappingSerializeWrapper,
    ) -> RouteResult<Option<(bool, bool, bool)>> {
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

            let topology_changed = existing_broker.cluster() != new_broker_data.cluster()
                || existing_broker.zone_name() != new_broker_data.zone_name()
                || existing_broker.enable_acting_master() != new_broker_data.enable_acting_master()
                || existing_broker.broker_addrs() != new_broker_data.broker_addrs();
            if topology_changed {
                self.broker_addr_table.insert(broker_name_arc, new_broker_data);
            }
            return Ok(Some((register_first, is_min_broker_id_changed, topology_changed)));
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

        Ok(Some((register_first, is_min_broker_id_changed, true)))
    }

    /// Update topic queue table with deletion and prime-slave handling.
    fn update_topic_queue_table(
        &self,
        broker_name: &BrokerName,
        cluster_name: &CheetahString,
        broker_addr: &CheetahString,
        topic_config_wrapper: &TopicConfigAndMappingSerializeWrapper,
        register_first: bool,
        is_prime_slave: bool,
        broker_topic_config_changed: bool,
    ) -> RouteResult<HashSet<TopicName>> {
        use std::collections::HashSet;

        use rocketmq_model::common::constant::PermName;

        let topic_config_table = topic_config_wrapper
            .topic_config_serialize_wrapper()
            .topic_config_table();
        let topic_queue_mapping_info_map = topic_config_wrapper.topic_queue_mapping_info_map();
        let mut dirty_topics = HashSet::new();

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
                    dirty_topics.insert(to_delete_topic.clone());
                }
                if self
                    .topic_queue_mapping_info_table
                    .remove_broker(to_delete_topic.as_ref(), broker_name.as_str())
                    .is_some()
                {
                    dirty_topics.insert(to_delete_topic.clone());
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

        // Process each topic configuration
        for (topic_name, topic_config) in topic_config_table.iter() {
            let topic_name_cheetah = topic_name.clone();

            // Check if we should update this topic
            if register_first
                || broker_topic_config_changed
                || self.is_topic_config_changed(
                    cluster_name,
                    broker_addr,
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
                if let Some(ref existed_qd) = old_queue_data {
                    if existed_qd.as_ref() != &queue_data {
                        info!(
                            "topic changed, {} OLD: {:?} NEW: {:?}",
                            topic_name, existed_qd, queue_data
                        );
                    }
                } else {
                    info!("new topic registered, {} {:?}", topic_name, &queue_data);
                }

                let queue_changed = old_queue_data.as_deref() != Some(&queue_data);
                if queue_changed {
                    self.topic_queue_table
                        .insert(topic_name_cheetah.clone(), broker_name.clone(), queue_data);
                    dirty_topics.insert(topic_name_cheetah.clone());
                }

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
        if broker_topic_config_changed || register_first {
            for (topic, mapping_info) in topic_queue_mapping_info_map {
                let topic = topic.clone();

                // Extract broker name from mapping info (bname is a field, not a method)
                let broker_name_mapping = mapping_info.bname.clone().unwrap_or_else(|| broker_name.clone());

                let mapping_changed = self
                    .topic_queue_mapping_info_table
                    .get(topic.as_str(), broker_name_mapping.as_str())
                    .as_deref()
                    != Some(mapping_info);
                if mapping_changed {
                    self.topic_queue_mapping_info_table.register(
                        topic.clone(),
                        broker_name_mapping,
                        Arc::new(mapping_info.clone()),
                    );
                    dirty_topics.insert(topic.clone());
                }
            }
        }

        Ok(dirty_topics)
    }

    /// Register broker live information (heartbeat tracking)
    /// Returns the previous BrokerLiveInfo if it existed
    fn register_broker_live_info(
        &self,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
        broker_addr: CheetahString,
        timeout_millis: Option<u64>,
        data_version: DataVersion,
        broker_session: BrokerSession,
        ha_server_addr: CheetahString,
    ) -> RouteResult<Option<Arc<BrokerLiveInfo>>> {
        let broker_addr_info = Arc::new(BrokerAddrInfo::new(cluster_name, broker_addr));

        let timeout = timeout_millis.unwrap_or(DEFAULT_BROKER_CHANNEL_EXPIRED_TIME);
        let current_time = current_millis();

        if let Some(existing) = self.broker_live_table.get(broker_addr_info.as_ref()) {
            let is_unchanged = existing.heartbeat_timeout_millis == timeout
                && existing.data_version == data_version
                && existing.ha_server_addr.as_ref() == Some(&ha_server_addr)
                && existing.remote_addr == broker_session.remote_addr
                && existing.channel_id == broker_session.channel_id
                && existing.session_id == Some(broker_session.id)
                && existing.broker_name.as_ref() == Some(&broker_name)
                && existing.broker_id == Some(broker_id);
            if is_unchanged {
                self.broker_live_table
                    .update_heartbeat(broker_addr_info.as_ref(), current_time);
                return Ok(Some(existing));
            }
        }

        let live_info = BrokerLiveInfo::new(
            current_time,
            data_version,
            broker_session.remote_addr,
            broker_session.channel_id.clone(),
        )
        .with_session(broker_session)
        .with_broker_identity(broker_name, broker_id)
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
        self.topic_queue_table
            .topics_for_broker(broker_name)
            .into_iter()
            .collect()
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
        broker_name: &CheetahString,
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
        let request = build_min_broker_id_change_request(
            &self.name_server_runtime_inner.remoting_command_factory(),
            request_header,
        );
        let notification_version = self.min_broker_notify_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        self.min_broker_notify_versions
            .insert(broker_name.clone(), notification_version);

        // Send notification to each broker asynchronously
        let Some(task_group) = self.name_server_runtime_inner.task_group() else {
            warn!("skip min broker id notification because NameServer task group is unavailable");
            return;
        };
        let remoting_client = self.name_server_runtime_inner.clone();
        let broker_name = broker_name.clone();
        let notification_versions = Arc::clone(&self.min_broker_notify_versions);
        let concurrency = self.min_broker_notify_concurrency;
        if let Err(error) = task_group.spawn_service("namesrv.notify-min-broker-id", async move {
            let Some(runtime) = remoting_client.upgrade() else {
                return;
            };
            let client = runtime.remoting_client().clone();
            let attempted = broker_addrs_notify.len();
            let results = futures::stream::iter(broker_addrs_notify)
                .map(|broker_addr| {
                    let client = client.clone();
                    let request = request.clone();
                    let broker_name = broker_name.clone();
                    let notification_versions = Arc::clone(&notification_versions);
                    async move {
                        let is_latest = notification_versions
                            .get(&broker_name)
                            .is_some_and(|version| *version == notification_version);
                        if !is_latest {
                            return None;
                        }
                        let succeeded = match client.invoke_request_oneway(&broker_addr, request, 3000).await {
                            Ok(()) => true,
                            Err(error) => {
                                warn!(
                                    remote_addr = %broker_addr,
                                    error_kind = ?error.kind(),
                                    "minimum broker id notification failed"
                                );
                                false
                            }
                        };
                        Some(succeeded)
                    }
                })
                .buffer_unordered(concurrency)
                .collect::<Vec<_>>()
                .await;
            let superseded = results.iter().any(Option::is_none);
            let succeeded = results.into_iter().filter(|result| *result == Some(true)).count();
            notification_versions.remove_if(&broker_name, |_, version| *version == notification_version);
            if !superseded && succeeded != attempted {
                warn!(
                    attempted,
                    succeeded, "minimum broker id notification broadcast was incomplete"
                );
            }
        }) {
            warn!("failed to spawn min broker id notification task: {error}");
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
    pub(crate) fn get_broker_live_info(&self, cluster_name: &str, broker_addr: &str) -> Option<Arc<BrokerLiveInfo>> {
        let broker_addr_info = BrokerAddrInfo::new(cluster_name.to_string(), broker_addr.to_string());
        self.broker_live_table.get(&broker_addr_info)
    }

    /// Register a topic with queue data for multiple brokers
    ///
    /// This method mutates and publishes under the global route mutation gate,
    /// and validates brokers within that transaction.
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
    /// - **Mutation gate**: Serializes every route-visible write and publication
    /// - **Mutation gate**: Keeps validation and publication in one generation
    ///
    /// This ensures that if all brokers pass validation, they're guaranteed to
    /// still exist when we insert the queue data.
    pub(crate) fn register_topic(&self, topic: CheetahString, queue_data_vec: Vec<QueueData>) {
        if queue_data_vec.is_empty() {
            return;
        }

        let route_mutation = self.route_mutations.begin_mutation();

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

        self.publish_topic_route_snapshots(&route_mutation, HashSet::from([topic]));
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
    /// The mutation gate serializes deletion, which is published atomically as a
    /// replacement snapshot or explicit absence.
    pub(crate) fn delete_topic(&self, topic: CheetahString, cluster_name: Option<CheetahString>) {
        let route_mutation = self.route_mutations.begin_mutation();

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
                    self.topic_queue_mapping_info_table
                        .remove_broker(topic_str, broker_name.as_str());
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
                self.topic_queue_mapping_info_table.remove_topic(topic.as_str());
            }
        }

        self.publish_topic_route_snapshots(&route_mutation, HashSet::from([topic]));
    }
}

// ============================================================================
// Broker Unregistration
// ============================================================================

impl RouteInfoManager {
    /// Submit broker unregistration request to batch service
    pub fn submit_unregister_broker_request(&self, request: UnRegisterBrokerRequestHeader) -> bool {
        self.un_register_service.submit(request)
    }

    fn submit_unregister_broker_request_guarded(
        &self,
        request: UnRegisterBrokerRequestHeader,
        expected_channel_id: RemotingConnectionId,
        expected_generation: crate::route::types::BrokerGeneration,
    ) -> bool {
        self.un_register_service
            .submit_channel_guarded(request, expected_channel_id, expected_generation)
    }

    /// Unregister a broker from the name server
    ///
    /// This method removes a broker under the route mutation gate and publishes every
    /// affected topic before releasing that gate.
    pub fn unregister_broker(
        &self,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
    ) -> RouteResult<()> {
        let route_mutation = self.route_mutations.begin_mutation();
        let affected_topics = self
            .topic_queue_table
            .topics_for_broker(broker_name.as_str())
            .into_iter()
            .collect();

        // Step 1: Remove from broker live table
        // Still unpublished under the mutation gate.
        let broker_addr_info = BrokerAddrInfo::new(cluster_name.clone(), broker_addr.clone());
        let removed = self.broker_live_table.remove(&broker_addr_info);
        self.filter_server_table.remove(&broker_addr_info);

        info!(
            "Broker live info removed: cluster={}, broker={}, addr={}, success={}",
            cluster_name,
            broker_name,
            broker_addr,
            removed.is_some()
        );

        // Step 2: Remove broker address from broker table
        // Still unpublished under the mutation gate.
        let _broker_removed = self
            .broker_addr_table
            .remove_broker_address(broker_name.as_str(), broker_id);

        // Step 3: Check if all broker addresses are gone
        // Source tables remain stable under the mutation gate.
        let broker_empty = if let Some(broker_data) = self.broker_addr_table.get(&broker_name) {
            broker_data.broker_addrs().is_empty()
        } else {
            true
        };

        // Step 4: If broker completely removed, clean up cluster and topics
        // Complete source-table cleanup before publishing affected topics.
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

        self.publish_topic_route_snapshots(&route_mutation, affected_topics);
        self.metrics.record_active_broker_count(self.active_broker_count());
        Ok(())
    }

    /// Clean up topics associated with a removed broker
    fn cleanup_topics_for_broker(&self, broker_name: &str) -> RouteResult<()> {
        for topic in self.topic_queue_table.topics_for_broker(broker_name) {
            // Remove broker from topic
            self.topic_queue_table.remove_broker(topic.as_ref(), broker_name);
            self.topic_queue_mapping_info_table
                .remove_broker(topic.as_ref(), broker_name);
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
        self.un_register_broker_requests(
            un_register_requests
                .into_iter()
                .map(BrokerUnregistrationRequest::explicit)
                .collect(),
        );
    }

    pub(crate) fn un_register_broker_requests(&self, requests: Vec<BrokerUnregistrationRequest>) {
        if requests.is_empty() {
            return;
        }

        let route_mutation = self.route_mutations.begin_mutation();
        let mut seen = HashSet::new();
        let mut un_register_requests = Vec::with_capacity(requests.len());
        for request in requests {
            let dedup_key = (
                request.header.cluster_name.clone(),
                request.header.broker_addr.clone(),
                request.expected_channel_id.clone(),
                request.expected_generation,
            );
            if !seen.insert(dedup_key) {
                continue;
            }
            if request.expected_channel_id.is_some() || request.expected_generation.is_some() {
                let broker_addr_info =
                    BrokerAddrInfo::new(request.header.cluster_name.clone(), request.header.broker_addr.clone());
                let matches_current_registration =
                    self.broker_live_table.get(&broker_addr_info).is_some_and(|live_info| {
                        request
                            .expected_channel_id
                            .as_ref()
                            .is_none_or(|expected| &live_info.channel_id == expected)
                            && request
                                .expected_generation
                                .is_none_or(|expected| live_info.generation() == expected)
                    });
                if !matches_current_registration {
                    debug!(
                        broker_addr = %request.header.broker_addr,
                        "ignore stale or duplicate channel-destroy event"
                    );
                    continue;
                }
            }
            un_register_requests.push(request.header);
        }
        if un_register_requests.is_empty() {
            return;
        }
        let affected_topics = un_register_requests
            .iter()
            .flat_map(|request| self.topic_queue_table.topics_for_broker(request.broker_name.as_str()))
            .collect();

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

        #[cfg(test)]
        self.run_before_topic_cleanup_hook();

        // Step 5: Clean topics by unregister requests
        self.clean_topic_by_un_register_requests(&removed_broker, &reduced_broker);
        self.publish_topic_route_snapshots(&route_mutation, affected_topics);

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
                        self.notify_min_broker_id_changed(&broker_name, &broker_addrs, Some(offline_broker_addr), None);
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
        for (topic, broker_name) in self.topic_queue_table.topic_broker_pairs_for_brokers(removed_broker) {
            if let Some(removed_qd) = self
                .topic_queue_table
                .remove_broker(topic.as_str(), broker_name.as_str())
            {
                debug!(
                    "removeTopicByBrokerName, remove one broker's topic {} {:?}",
                    topic, removed_qd
                );
            }
            self.topic_queue_mapping_info_table
                .remove_broker(topic.as_str(), broker_name.as_str());
        }

        let removed_topic_count = self.topic_queue_table.cleanup_empty_topics();
        if removed_topic_count > 0 {
            debug!(
                "removeTopicByBrokerName, remove {} topics with no remaining queue data",
                removed_topic_count
            );
        }

        let reduced_brokers_requiring_write_wipe = reduced_broker
            .iter()
            .filter_map(|broker_name| {
                let broker_data = self.broker_addr_table.get(broker_name.as_str())?;
                let no_master_exists = broker_data.broker_addrs().is_empty()
                    || broker_data.broker_addrs().keys().min().copied().unwrap_or(0) > 0;

                (broker_data.enable_acting_master() && no_master_exists).then(|| broker_name.clone())
            })
            .collect::<HashSet<_>>();

        for (topic, broker_name) in self
            .topic_queue_table
            .topic_broker_pairs_for_brokers(&reduced_brokers_requiring_write_wipe)
        {
            if let Some(queue_data) = self.topic_queue_table.get(topic.as_str(), broker_name.as_str()) {
                let perm = queue_data.perm() & !PermName::PERM_WRITE;
                self.topic_queue_table
                    .update_queue_data_perm(topic.as_str(), broker_name.as_str(), perm as i32);
            }
        }
        self.metrics.record_active_broker_count(self.active_broker_count());
    }
}

// ============================================================================
// Route Lookup
// ============================================================================

impl RouteInfoManager {
    /// Get topic route data for a given topic
    ///
    /// This is the main query API used by producers and consumers
    /// to discover where a topic's messages should be sent/consumed.
    ///
    /// ## Consistency Guarantee
    ///
    /// This method loads one immutable snapshot. Source tables are consulted only
    /// by the serialized write model when publishing a replacement snapshot.
    pub fn pickup_topic_route_data(&self, topic: &str) -> RouteResult<TopicRouteData> {
        self.load_topic_route_view(topic)
            .map(|view| view.route_data().as_ref().clone())
    }

    pub(crate) fn load_topic_route_view(&self, topic: &str) -> RouteResult<TopicRouteView> {
        use rocketmq_model::common::topic::TopicValidator;

        let snapshot = self
            .route_mutations
            .load(topic)
            .ok_or_else(|| RocketMQError::route_not_found(topic))?;
        debug!(
            "Loaded topic route snapshot: topic={}, version={}, published_at={}",
            topic, snapshot.version, snapshot.published_at
        );

        if !self
            .name_server_runtime_inner
            .name_server_config()
            .support_acting_master
            || topic.starts_with(TopicValidator::SYNC_BROKER_MEMBER_GROUP_PREFIX)
        {
            return Ok(snapshot.base_view());
        }

        Ok(snapshot.acting_master_view(build_acting_master_route_data))
    }
}

fn build_acting_master_route_data(route_data: &TopicRouteData) -> Option<TopicRouteData> {
    if route_data.broker_datas.is_empty() || route_data.queue_datas.is_empty() {
        return None;
    }

    let need_acting_master = route_data.broker_datas.iter().any(|broker_data| {
        !broker_data.broker_addrs().is_empty() && !broker_data.broker_addrs().contains_key(&mix_all::MASTER_ID)
    });

    if !need_acting_master {
        return None;
    }

    let mut acting_route_data = route_data.clone();
    let mut promoted = false;
    for broker_data in &mut acting_route_data.broker_datas {
        let enable_acting_master = broker_data.enable_acting_master();
        let broker_name = broker_data.broker_name().clone();
        let broker_addrs = broker_data.broker_addrs_mut();

        if broker_addrs.is_empty() || broker_addrs.contains_key(&mix_all::MASTER_ID) || !enable_acting_master {
            continue;
        }

        for queue_data in &acting_route_data.queue_datas {
            if queue_data.broker_name() == &broker_name {
                if !PermName::is_writeable(queue_data.perm()) {
                    if let Some(&min_broker_id) = broker_addrs.keys().min() {
                        if let Some(acting_master_addr) = broker_addrs.remove(&min_broker_id) {
                            broker_addrs.insert(mix_all::MASTER_ID, acting_master_addr);
                            promoted = true;
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

    promoted.then_some(acting_route_data)
}

impl RouteInfoManager {
    /// Get the number of brokers currently tracked as live.
    pub fn active_broker_count(&self) -> usize {
        self.broker_live_table.len()
    }

    pub(crate) fn record_registration_decode(
        &self,
        wire_bytes: usize,
        decoded_bytes: Option<usize>,
        elapsed: std::time::Duration,
    ) {
        self.metrics
            .record_registration_decode(wire_bytes, decoded_bytes, elapsed);
    }

    pub(crate) fn route_freshness_millis(&self, route: &TopicRouteData) -> Option<u64> {
        let now = current_millis();
        route
            .broker_datas
            .iter()
            .flat_map(|broker| {
                broker.broker_addrs().values().filter_map(|address| {
                    #[cfg(test)]
                    self.route_freshness_lookups.fetch_add(1, Ordering::Relaxed);
                    let key = BrokerAddrInfo::new(CheetahString::from(broker.cluster()), address.clone());
                    self.broker_live_table
                        .get(&key)
                        .map(|live| now.saturating_sub(live.last_update_timestamp()))
                })
            })
            .max()
    }

    #[cfg(test)]
    pub(crate) fn route_freshness_lookup_count(&self) -> u64 {
        self.route_freshness_lookups.load(Ordering::Relaxed)
    }
}

// ============================================================================
// Broker Heartbeat & Health Check
// ============================================================================

impl RouteInfoManager {
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
    /// - This coalesces source-table cleanup and snapshot publication
    pub fn scan_not_active_broker(&self) -> usize {
        let scan_started = std::time::Instant::now();
        debug!("start scanNotActiveBroker");
        let current_time = current_millis();

        let expiry_mode = self.broker_live_table.expiry_index_mode();
        let indexed_expired = self.broker_live_table.get_indexed_expired_brokers(current_time);
        let safety_scan_due = expiry_mode == crate::config::ExpiryIndexMode::Active
            && current_time.saturating_sub(self.last_expiry_safety_scan.load(Ordering::Acquire))
                >= self.expiry_safety_scan_interval
            && self
                .last_expiry_safety_scan
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |last| {
                    (current_time.saturating_sub(last) >= self.expiry_safety_scan_interval).then_some(current_time)
                })
                .is_ok();
        let full_scan_expired = match expiry_mode {
            crate::config::ExpiryIndexMode::Off | crate::config::ExpiryIndexMode::Shadow => {
                self.broker_live_table.get_expired_brokers(current_time)
            }
            crate::config::ExpiryIndexMode::Active if safety_scan_due => {
                self.broker_live_table.get_expired_brokers(current_time)
            }
            crate::config::ExpiryIndexMode::Active => Vec::new(),
        };
        if expiry_mode == crate::config::ExpiryIndexMode::Shadow {
            let full_set = full_scan_expired
                .iter()
                .map(|broker| broker.as_ref())
                .collect::<HashSet<_>>();
            let indexed_set = indexed_expired
                .iter()
                .map(|(broker, _)| broker.as_ref())
                .collect::<HashSet<_>>();
            if full_set != indexed_set {
                self.metrics.record_expiry_event(
                    rocketmq_observability::metrics::namesrv::NameServerExpiryEvent::IndexMismatch,
                );
                warn!(
                    full_scan_count = full_set.len(),
                    indexed_count = indexed_set.len(),
                    "broker expiry shadow index diverged from the authoritative full scan"
                );
            }
        }
        if safety_scan_due {
            self.metrics
                .record_expiry_event(rocketmq_observability::metrics::namesrv::NameServerExpiryEvent::SafetyReconcile);
        }
        let examined = if expiry_mode != crate::config::ExpiryIndexMode::Active || safety_scan_due {
            self.active_broker_count()
        } else {
            indexed_expired.len()
        };
        let mut expired_brokers = indexed_expired
            .into_iter()
            .map(|(broker, generation)| (broker, Some(generation)))
            .collect::<Vec<_>>();
        if expiry_mode != crate::config::ExpiryIndexMode::Active {
            expired_brokers = full_scan_expired.into_iter().map(|broker| (broker, None)).collect();
        } else if safety_scan_due {
            let mut indexed_keys = expired_brokers
                .iter()
                .map(|(broker, _)| broker.as_ref().clone())
                .collect::<HashSet<_>>();
            for broker in full_scan_expired {
                if indexed_keys.insert(broker.as_ref().clone()) {
                    expired_brokers.push((broker, None));
                }
            }
        }

        let count = expired_brokers.len();
        if count > 0 {
            // Submit unregistration requests for each expired broker
            for (broker_addr_info, expected_generation) in expired_brokers {
                if let Some(live_info) = self.broker_live_table.get(&broker_addr_info) {
                    if expected_generation.is_some_and(|generation| live_info.generation() != generation) {
                        continue;
                    }
                    warn!(
                        "The broker channel expired, {} {}ms",
                        broker_addr_info, live_info.heartbeat_timeout_millis
                    );

                    if let Some(session_id) = live_info.session_id {
                        self.session_registry.close_now(session_id);
                    }

                    self.on_channel_destroy_by_addr_info_and_live(broker_addr_info, live_info);
                    continue;
                }

                // Trigger channel destroy logic, which will submit to batch unregistration service
                self.on_channel_destroy_by_addr_info(broker_addr_info);
            }
        }

        self.metrics.record_active_broker_count(self.active_broker_count());
        self.metrics
            .record_expiry_scan(expiry_mode.as_str(), examined, count, scan_started.elapsed());
        count
    }

    /// Handle channel destruction by broker address info
    ///
    /// 1. Setup unregister request with broker info
    /// 2. Submit to batch unregistration service
    ///
    /// The batch service will process the request asynchronously.
    fn on_channel_destroy_by_addr_info(&self, broker_addr_info: Arc<BrokerAddrInfo>) {
        if let Some(live_info) = self.broker_live_table.get(&broker_addr_info) {
            self.on_channel_destroy_by_addr_info_and_live(broker_addr_info, live_info);
            return;
        }
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

    fn on_channel_destroy_by_addr_info_and_live(
        &self,
        broker_addr_info: Arc<BrokerAddrInfo>,
        live_info: Arc<BrokerLiveInfo>,
    ) {
        let mut unregister_request = UnRegisterBrokerRequestHeader::default();
        if !self.setup_unregister_request_from_live(&mut unregister_request, &broker_addr_info, &live_info) {
            return;
        }
        let result = self.submit_unregister_broker_request_guarded(
            unregister_request.clone(),
            live_info.channel_id.clone(),
            live_info.generation(),
        );
        info!(
            "the broker's channel destroyed, submit guarded unregister request, broker info: {}, submit result: {}",
            unregister_request, result
        );
    }

    fn setup_unregister_request_from_live(
        &self,
        unregister_request: &mut UnRegisterBrokerRequestHeader,
        broker_addr_info: &BrokerAddrInfo,
        live_info: &BrokerLiveInfo,
    ) -> bool {
        unregister_request.cluster_name = broker_addr_info.cluster_name.clone();
        unregister_request.broker_addr = broker_addr_info.broker_addr.clone();
        match (&live_info.broker_name, live_info.broker_id) {
            (Some(broker_name), Some(broker_id)) => {
                unregister_request.broker_name = broker_name.clone();
                unregister_request.broker_id = broker_id;
                true
            }
            _ => self.setup_unregister_request(unregister_request, broker_addr_info),
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

        if let Some((broker_name, broker_id)) = broker_addr_table.find_broker_by_addr_in_cluster(
            broker_addr_info.cluster_name.as_str(),
            broker_addr_info.broker_addr.as_str(),
        ) {
            unregister_request.broker_name = broker_name;
            unregister_request.broker_id = broker_id;
            return true;
        }

        false
    }

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

    /// Refresh a broker only when the heartbeat came from its current session.
    pub fn update_broker_info_update_timestamp_for_session(
        &self,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
        session_id: SessionId,
    ) -> bool {
        let broker_addr_info = BrokerAddrInfo::new(cluster_name, broker_addr);
        self.broker_live_table
            .update_heartbeat_for_session(&broker_addr_info, session_id, current_millis())
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

    /// Wipe write permission of broker by lock
    ///
    /// This method removes write permission from all topics that contain queue data
    /// for the specified broker:
    /// 1. Acquiring the route mutation gate
    /// 2. Directly looking up the broker in each topic's queue map
    /// 3. Removing write permission from matched queue data
    ///
    /// # Arguments
    /// * `broker_name` - Name of the broker whose write permission should be wiped
    ///
    /// # Returns
    /// Number of topics whose queue data was updated
    pub fn wipe_write_perm_of_broker_by_lock(&self, broker_name: String) -> i32 {
        use rocketmq_model::common::constant::PermName;

        let route_mutation = self.route_mutations.begin_mutation();

        let topic_queue_pairs = self.topic_queue_table.topic_queue_pairs_for_broker(&broker_name);
        let affected_topics = topic_queue_pairs.iter().map(|(topic, _)| topic.clone()).collect();

        for (topic, queue_data) in &topic_queue_pairs {
            let perm = queue_data.perm() & !PermName::PERM_WRITE;
            self.topic_queue_table
                .update_queue_data_perm(topic, &broker_name, perm as i32);
        }

        self.publish_topic_route_snapshots(&route_mutation, affected_topics);
        topic_queue_pairs.len() as i32
    }

    /// Add write permission of broker by lock.
    ///
    /// This method adds write permission to all topics that contain queue data
    /// for the specified broker:
    /// 1. Acquiring the route mutation gate
    /// 2. Directly looking up the broker in each topic's queue map
    /// 3. Setting permission to READ | WRITE (not just adding write flag)
    ///
    /// # Arguments
    /// * `broker_name` - Name of the broker whose write permission should be added
    ///
    /// # Returns
    /// Number of topics whose queue data was updated
    pub fn add_write_perm_of_broker_by_lock(&self, broker_name: String) -> i32 {
        use rocketmq_model::common::constant::PermName;

        let route_mutation = self.route_mutations.begin_mutation();

        let topic_queue_pairs = self.topic_queue_table.topic_queue_pairs_for_broker(&broker_name);
        let affected_topics = topic_queue_pairs.iter().map(|(topic, _)| topic.clone()).collect();

        for (topic, _) in &topic_queue_pairs {
            let perm = PermName::PERM_READ | PermName::PERM_WRITE;
            self.topic_queue_table
                .update_queue_data_perm(topic, &broker_name, perm as i32);
        }

        self.publish_topic_route_snapshots(&route_mutation, affected_topics);
        topic_queue_pairs.len() as i32
    }
}

fn build_min_broker_id_change_request(
    command_factory: &RemotingCommandFactory,
    request_header: NotifyMinBrokerIdChangeRequestHeader,
) -> RemotingCommand {
    command_factory.create_request_command(RequestCode::NotifyMinBrokerIdChange, request_header)
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::sync::OnceLock;

    use rocketmq_model::common::config::TopicConfig;
    use rocketmq_observability::TelemetryHandle;
    use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_info::TopicQueueMappingInfo;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_runtime::RuntimeOwner;
    use rocketmq_transport::test_support::session_id_for_test;
    use rocketmq_transport::test_support::LocalChannelHarness;

    use super::*;
    use crate::bootstrap::Builder;

    #[test]
    fn min_broker_notification_request_keeps_factory_defaults() {
        let factory = rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory::new(
            rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandDefaults::new(
                659,
                rocketmq_protocol::protocol::SerializeType::ROCKETMQ,
            ),
        );
        let header = NotifyMinBrokerIdChangeRequestHeader::new(Some(0), None, None, None, None);

        let request = build_min_broker_id_change_request(&factory, header);

        assert_eq!(request.version(), 659);
        assert_eq!(
            request.serialize_type(),
            rocketmq_protocol::protocol::SerializeType::ROCKETMQ
        );
    }

    fn test_route_manager() -> (crate::bootstrap::NameServerBootstrap, Arc<RouteInfoManager>) {
        test_route_manager_with_config(crate::NamesrvConfig::default())
    }

    fn test_route_manager_with_config(
        config: crate::NamesrvConfig,
    ) -> (crate::bootstrap::NameServerBootstrap, Arc<RouteInfoManager>) {
        static OWNER: OnceLock<RuntimeOwner> = OnceLock::new();
        let service_context = OWNER
            .get_or_init(|| {
                RuntimeOwner::new(RuntimeConfig::server_default("namesrv-route-snapshot-test"))
                    .expect("test runtime owner should build")
            })
            .root_context()
            .component("namesrv");
        let bootstrap = Builder::new(service_context, TelemetryHandle::noop())
            .set_name_server_config(config)
            .build();
        let manager = bootstrap.runtime_inner().route_info_manager();
        (bootstrap, manager)
    }

    fn registration_wrapper(topic: &CheetahString, version: i64, queues: u32) -> TopicConfigAndMappingSerializeWrapper {
        registration_wrapper_with_topics(version, &[(topic, queues)])
    }

    fn test_broker_session(remote_addr: SocketAddr) -> BrokerSession {
        static NEXT_SESSION_OWNER: AtomicU64 = AtomicU64::new(20_000);
        let owner_id = NEXT_SESSION_OWNER.fetch_add(1, Ordering::Relaxed);
        BrokerSession::for_test(
            session_id_for_test(owner_id),
            CheetahString::from_string(format!("transport-session-{owner_id}")),
            remote_addr,
        )
    }

    fn registration_wrapper_with_topics(
        version: i64,
        topics: &[(&CheetahString, u32)],
    ) -> TopicConfigAndMappingSerializeWrapper {
        let mut wrapper = TopicConfigAndMappingSerializeWrapper::default();
        wrapper.topic_config_serialize_wrapper.data_version = DataVersion::with_values(version, version, version);
        for (topic, queues) in topics {
            wrapper.topic_config_serialize_wrapper.topic_config_table.insert(
                (*topic).clone(),
                TopicConfig::with_perm((*topic).clone(), *queues, *queues, 6),
            );
        }
        wrapper
    }

    // NOTE: Integration tests for RouteInfoManager require complex setup.
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

    #[tokio::test]
    async fn unchanged_and_split_registrations_publish_only_missing_routes() {
        let runtime_context = RuntimeContext::from_current("namesrv-registration-delta-test");
        let config = crate::NamesrvConfig {
            enable_registration_delta: true,
            ..Default::default()
        };
        let bootstrap = Builder::new(runtime_context.service_context("namesrv"), TelemetryHandle::noop())
            .set_name_server_config(config)
            .build();
        let manager = bootstrap.runtime_inner().route_info_manager();
        let harness = LocalChannelHarness::new(
            runtime_context
                .service_context("namesrv-registration-channel")
                .task_group()
                .clone(),
        )
        .await
        .expect("test channel should build");
        let cluster_name = CheetahString::from_static_str("delta-cluster");
        let broker_name = CheetahString::from_static_str("delta-broker");
        let broker_addr = CheetahString::from_static_str("127.0.0.1:10911");
        let topic = CheetahString::from_static_str("delta-topic");
        let second_topic = CheetahString::from_static_str("delta-topic-two");
        let broker_addr_info = BrokerAddrInfo::new(cluster_name.clone(), broker_addr.clone());
        let broker_session = test_broker_session(harness.remote_address());

        manager
            .register_broker_session(
                cluster_name.clone(),
                broker_addr.clone(),
                broker_name.clone(),
                mix_all::MASTER_ID,
                broker_addr.clone(),
                None,
                Some(60_000),
                Some(false),
                registration_wrapper(&topic, 1, 4),
                Vec::new(),
                broker_session.clone(),
            )
            .expect("first registration should succeed");
        let first_version = manager
            .topic_route_snapshot_version(topic.as_str())
            .expect("first registration should publish the route");
        let first_live = manager
            .broker_live_table
            .get(&broker_addr_info)
            .expect("first registration should create live info");

        manager
            .register_broker_session(
                cluster_name.clone(),
                broker_addr.clone(),
                broker_name.clone(),
                mix_all::MASTER_ID,
                broker_addr.clone(),
                None,
                Some(60_000),
                Some(false),
                registration_wrapper(&topic, 1, 4),
                Vec::new(),
                broker_session.clone(),
            )
            .expect("unchanged registration should succeed");

        assert_eq!(
            manager.topic_route_snapshot_version(topic.as_str()),
            Some(first_version)
        );
        let second_live = manager
            .broker_live_table
            .get(&broker_addr_info)
            .expect("unchanged registration should retain live info");
        assert!(Arc::ptr_eq(&first_live, &second_live));
        assert_eq!(second_live.heartbeat_generation(), 1);

        manager
            .register_broker_session(
                cluster_name,
                broker_addr.clone(),
                broker_name,
                mix_all::MASTER_ID,
                broker_addr,
                None,
                Some(60_000),
                Some(false),
                registration_wrapper(&second_topic, 1, 8),
                Vec::new(),
                broker_session,
            )
            .expect("changed registration should succeed");
        assert_eq!(
            manager.topic_route_snapshot_version(topic.as_str()),
            Some(first_version)
        );
        assert!(manager
            .topic_route_snapshot_version(second_topic.as_str())
            .is_some_and(|version| version > first_version));

        harness.channel().connection_ref().close();
        drop(manager);
        drop(bootstrap);
        drop(harness);
        tokio::task::yield_now().await;
        let _ = runtime_context.shutdown_tasks(std::time::Duration::from_secs(1)).await;
    }

    #[test]
    fn route_views_share_base_data_and_preserve_the_owned_api() {
        let (_bootstrap, manager) = test_route_manager();
        let cluster_name = CheetahString::from_static_str("shared-view-cluster");
        let broker_name = CheetahString::from_static_str("shared-view-broker");
        let broker_addr = CheetahString::from_static_str("127.0.0.1:20911");
        let topic = CheetahString::from_static_str("shared-view-topic");

        manager
            .cluster_addr_table
            .add_broker(cluster_name.clone(), broker_name.clone());
        manager.broker_addr_table.insert(
            broker_name.clone(),
            BrokerData::new(
                cluster_name,
                broker_name.clone(),
                HashMap::from([(mix_all::MASTER_ID, broker_addr)]),
                None,
            ),
        );
        manager.register_topic(topic.clone(), vec![QueueData::new(broker_name, 4, 4, 6, 0)]);

        let first = manager
            .load_topic_route_view(topic.as_str())
            .expect("first shared view should exist");
        let second = manager
            .load_topic_route_view(topic.as_str())
            .expect("second shared view should exist");
        let owned = manager
            .pickup_topic_route_data(topic.as_str())
            .expect("the compatibility API should still return a route");

        assert_eq!(first.variant(), crate::route::topic_route_snapshot::RouteVariant::Base);
        assert_eq!(first.version(), second.version());
        assert!(Arc::ptr_eq(first.route_data(), second.route_data()));
        assert_eq!(first.route_data().as_ref(), &owned);
    }

    #[test]
    fn route_snapshot_hides_unregister_intermediate_state() {
        let (_bootstrap, manager) = test_route_manager();
        let cluster_name = CheetahString::from_static_str("snapshot-cluster");
        let broker_name = CheetahString::from_static_str("snapshot-broker");
        let broker_addr = CheetahString::from_static_str("127.0.0.1:10911");
        let topic = CheetahString::from_static_str("snapshot-topic");

        manager
            .cluster_addr_table
            .add_broker(cluster_name.clone(), broker_name.clone());
        manager.broker_addr_table.insert(
            broker_name.clone(),
            BrokerData::new(
                cluster_name.clone(),
                broker_name.clone(),
                HashMap::from([(mix_all::MASTER_ID, broker_addr.clone())]),
                None,
            ),
        );
        manager.register_topic(topic.clone(), vec![QueueData::new(broker_name.clone(), 4, 4, 6, 0)]);

        let first_version = manager
            .topic_route_snapshot_version(topic.as_str())
            .expect("registering the topic should publish a snapshot");
        let route_before_rejected_update = manager
            .pickup_topic_route_data(topic.as_str())
            .expect("registered topic should have a route");
        manager.register_topic(
            topic.clone(),
            vec![QueueData::new(
                CheetahString::from_static_str("missing-broker"),
                8,
                8,
                6,
                0,
            )],
        );
        assert_eq!(
            manager.topic_route_snapshot_version(topic.as_str()),
            Some(first_version)
        );
        assert_eq!(
            manager
                .pickup_topic_route_data(topic.as_str())
                .expect("rejected update must preserve the old snapshot"),
            route_before_rejected_update
        );

        assert_eq!(manager.wipe_write_perm_of_broker_by_lock(broker_name.to_string()), 1);
        let second_version = manager
            .topic_route_snapshot_version(topic.as_str())
            .expect("permission mutation should republish the topic");
        assert!(second_version > first_version);

        let initial_route = manager
            .pickup_topic_route_data(topic.as_str())
            .expect("registered topic should have a complete route");
        assert_eq!(initial_route.queue_datas.len(), 1);
        assert_eq!(initial_route.broker_datas.len(), 1);
        assert!(!PermName::is_writeable(initial_route.queue_datas[0].perm()));

        let mutation_paused = Arc::new(Barrier::new(2));
        let resume_mutation = Arc::new(Barrier::new(2));
        let management_cluster = cluster_name.clone();
        manager.set_before_topic_cleanup_hook({
            let mutation_paused = Arc::clone(&mutation_paused);
            let resume_mutation = Arc::clone(&resume_mutation);
            Arc::new(move || {
                mutation_paused.wait();
                resume_mutation.wait();
            })
        });

        let unregister_manager = Arc::clone(&manager);
        let unregister_thread = std::thread::spawn(move || {
            unregister_manager.un_register_broker(vec![UnRegisterBrokerRequestHeader {
                cluster_name,
                broker_addr,
                broker_name,
                broker_id: mix_all::MASTER_ID,
            }]);
        });

        mutation_paused.wait();
        let route_while_unregistration_is_paused = manager.pickup_topic_route_data(topic.as_str());
        let (management_result_tx, management_result_rx) = std::sync::mpsc::sync_channel(1);
        let management_manager = Arc::clone(&manager);
        let management_thread = std::thread::spawn(move || {
            management_result_tx
                .send(management_manager.get_topics_by_cluster(management_cluster.as_str()))
                .expect("management result receiver should remain available");
        });
        assert!(matches!(
            management_result_rx.recv_timeout(std::time::Duration::from_millis(25)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        resume_mutation.wait();
        unregister_thread.join().expect("unregister thread should not panic");
        let management_result = management_result_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("management read should complete after mutation publication");
        management_thread.join().expect("management thread should not panic");

        let route_while_unregistration_is_paused = route_while_unregistration_is_paused
            .expect("an unpublished unregister mutation must leave the complete old route visible");
        assert_eq!(route_while_unregistration_is_paused, initial_route);
        assert!(matches!(management_result, Err(RocketMQError::ClusterNotFound { .. })));
        assert!(manager.pickup_topic_route_data(topic.as_str()).is_err());
    }

    #[test]
    fn route_snapshot_republishes_filter_mapping_and_delete_changes() {
        let (_bootstrap, manager) = test_route_manager();
        let cluster_name = CheetahString::from_static_str("snapshot-metadata-cluster");
        let broker_name = CheetahString::from_static_str("snapshot-metadata-broker");
        let broker_addr = CheetahString::from_static_str("127.0.0.2:10911");
        let filter_server = CheetahString::from_static_str("127.0.0.2:12000");
        let topic = CheetahString::from_static_str("snapshot-metadata-topic");

        manager
            .cluster_addr_table
            .add_broker(cluster_name.clone(), broker_name.clone());
        manager.broker_addr_table.insert(
            broker_name.clone(),
            BrokerData::new(
                cluster_name.clone(),
                broker_name.clone(),
                HashMap::from([(mix_all::MASTER_ID, broker_addr.clone())]),
                None,
            ),
        );
        manager.register_topic(topic.clone(), vec![QueueData::new(broker_name.clone(), 4, 4, 6, 0)]);
        let initial_version = manager
            .topic_route_snapshot_version(topic.as_str())
            .expect("topic registration should publish a snapshot");

        {
            let mutation = manager.route_mutations.begin_mutation();
            manager.filter_server_table.register(
                Arc::new(BrokerAddrInfo::new(cluster_name, broker_addr.clone())),
                vec![filter_server.clone()],
            );
            manager.topic_queue_mapping_info_table.register(
                topic.clone(),
                broker_name.clone(),
                Arc::new(TopicQueueMappingInfo::new(topic.clone(), 4, broker_name.clone(), 1)),
            );
            manager.publish_topic_route_snapshots(&mutation, HashSet::from([topic.clone()]));
        }

        let metadata_version = manager
            .topic_route_snapshot_version(topic.as_str())
            .expect("metadata mutation should republish the snapshot");
        assert!(metadata_version > initial_version);
        let route = manager
            .pickup_topic_route_data(topic.as_str())
            .expect("metadata update should keep a complete route");
        assert_eq!(route.filter_server_table.get(&broker_addr), Some(&vec![filter_server]));
        assert!(route
            .topic_queue_mapping_by_broker
            .as_ref()
            .is_some_and(|mapping| mapping.contains_key(&broker_name)));

        manager.delete_topic(topic.clone(), None);
        assert!(manager.pickup_topic_route_data(topic.as_str()).is_err());
    }

    #[tokio::test]
    async fn mass_expiry_uses_known_identity() {
        let (_bootstrap, manager) = test_route_manager();
        let cluster_name = CheetahString::from_static_str("mass-expiry-cluster");
        const BROKER_COUNT: usize = 128;

        for index in 0..BROKER_COUNT {
            let broker_name = CheetahString::from_string(format!("mass-expiry-broker-{index}"));
            let broker_addr = CheetahString::from_string(format!("127.0.1.1:{}", 10_000 + index));
            manager
                .cluster_addr_table
                .add_broker(cluster_name.clone(), broker_name.clone());
            manager.broker_addr_table.insert(
                broker_name.clone(),
                BrokerData::new(
                    cluster_name.clone(),
                    broker_name.clone(),
                    HashMap::from([(mix_all::MASTER_ID, broker_addr.clone())]),
                    None,
                ),
            );
            let broker_addr_info = Arc::new(BrokerAddrInfo::new(cluster_name.clone(), broker_addr));
            manager.broker_live_table.register(
                broker_addr_info,
                BrokerLiveInfo::new(
                    0,
                    DataVersion::default(),
                    SocketAddr::from(([127, 0, 1, 1], (10_000 + index) as u16)),
                    CheetahString::from_string(format!("mass-expiry-channel-{index}")),
                )
                .with_timeout(1)
                .with_broker_identity(broker_name, mix_all::MASTER_ID),
            );
        }

        assert_eq!(manager.scan_not_active_broker(), BROKER_COUNT);
        assert_eq!(manager.un_register_service.queue_length(), BROKER_COUNT);
    }

    #[tokio::test]
    async fn active_expiry_index_finds_deadlines_without_safety_scan() {
        let config = crate::NamesrvConfig {
            expiry_index_mode: crate::config::ExpiryIndexMode::Active,
            ..Default::default()
        };
        let (_bootstrap, manager) = test_route_manager_with_config(config);
        let cluster_name = CheetahString::from_static_str("indexed-expiry-cluster");
        let broker_name = CheetahString::from_static_str("indexed-expiry-broker");
        let broker_addr = CheetahString::from_static_str("127.0.3.1:10911");
        manager
            .cluster_addr_table
            .add_broker(cluster_name.clone(), broker_name.clone());
        manager.broker_addr_table.insert(
            broker_name.clone(),
            BrokerData::new(
                cluster_name.clone(),
                broker_name.clone(),
                HashMap::from([(mix_all::MASTER_ID, broker_addr.clone())]),
                None,
            ),
        );
        manager.broker_live_table.register(
            Arc::new(BrokerAddrInfo::new(cluster_name, broker_addr)),
            BrokerLiveInfo::new(
                0,
                DataVersion::default(),
                SocketAddr::from(([127, 0, 3, 1], 10911)),
                CheetahString::from_static_str("indexed-expiry-channel"),
            )
            .with_timeout(1)
            .with_broker_identity(broker_name, mix_all::MASTER_ID),
        );
        manager
            .last_expiry_safety_scan
            .store(current_millis(), Ordering::Release);

        assert_eq!(manager.scan_not_active_broker(), 1);
        assert_eq!(manager.un_register_service.queue_length(), 1);
    }

    #[test]
    fn duplicate_channel_destroy_is_idempotent_and_stale_event_cannot_delete_new_registration() {
        let (_bootstrap, manager) = test_route_manager();
        let cluster_name = CheetahString::from_static_str("channel-fence-cluster");
        let broker_name = CheetahString::from_static_str("channel-fence-broker");
        let broker_addr = CheetahString::from_static_str("127.0.2.1:10911");
        let broker_addr_info = Arc::new(BrokerAddrInfo::new(cluster_name.clone(), broker_addr.clone()));
        let new_channel_id = CheetahString::from_static_str("new-channel");
        manager
            .cluster_addr_table
            .add_broker(cluster_name.clone(), broker_name.clone());
        manager.broker_addr_table.insert(
            broker_name.clone(),
            BrokerData::new(
                cluster_name.clone(),
                broker_name.clone(),
                HashMap::from([(mix_all::MASTER_ID, broker_addr.clone())]),
                None,
            ),
        );
        manager.broker_live_table.register(
            Arc::clone(&broker_addr_info),
            BrokerLiveInfo::new(
                current_millis(),
                DataVersion::default(),
                SocketAddr::from(([127, 0, 2, 1], 10911)),
                new_channel_id.clone(),
            )
            .with_broker_identity(broker_name.clone(), mix_all::MASTER_ID),
        );
        let header = UnRegisterBrokerRequestHeader {
            cluster_name: cluster_name.clone(),
            broker_addr: broker_addr.clone(),
            broker_name: broker_name.clone(),
            broker_id: mix_all::MASTER_ID,
        };
        let current_generation = manager
            .broker_live_table
            .get(&broker_addr_info)
            .expect("registered broker should be live")
            .generation();

        manager.un_register_broker_requests(vec![BrokerUnregistrationRequest::channel_guarded(
            header.clone(),
            CheetahString::from_static_str("old-channel"),
            current_generation,
        )]);
        assert!(manager.broker_live_table.contains(&broker_addr_info));
        assert!(manager.broker_addr_table.contains(&broker_name));

        assert!(manager
            .broker_live_table
            .update_heartbeat(&broker_addr_info, current_millis().saturating_add(1)));
        manager.un_register_broker_requests(vec![BrokerUnregistrationRequest::channel_guarded(
            header.clone(),
            new_channel_id.clone(),
            current_generation,
        )]);
        assert!(manager.broker_live_table.contains(&broker_addr_info));
        let refreshed_generation = manager
            .broker_live_table
            .get(&broker_addr_info)
            .expect("refreshed broker should still be live")
            .generation();

        manager.un_register_broker_requests(vec![
            BrokerUnregistrationRequest::channel_guarded(header.clone(), new_channel_id.clone(), refreshed_generation),
            BrokerUnregistrationRequest::channel_guarded(header, new_channel_id, refreshed_generation),
        ]);
        assert!(!manager.broker_live_table.contains(&broker_addr_info));
        assert!(!manager.broker_addr_table.contains(&broker_name));
    }

    #[test]
    fn test_choose_broker_addrs_to_notify_single_broker() {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(0, CheetahString::from_static_str("broker0:10911"));

        let result = RouteInfoManager::choose_broker_addrs_to_notify(&broker_addrs, &None);

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

        let result = RouteInfoManager::choose_broker_addrs_to_notify(&broker_addrs, &None);

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
        let result = RouteInfoManager::choose_broker_addrs_to_notify(&broker_addrs, &offline);

        // With offline broker: notify all brokers (including min broker ID)
        assert_eq!(result.len(), 3);
        assert!(result.iter().any(|s| s.as_str() == "broker0:10911"));
        assert!(result.iter().any(|s| s.as_str() == "broker1:10911"));
        assert!(result.iter().any(|s| s.as_str() == "broker2:10911"));
    }

    #[test]
    fn test_choose_broker_addrs_to_notify_empty() {
        let broker_addrs = HashMap::new();
        let result = RouteInfoManager::choose_broker_addrs_to_notify(&broker_addrs, &None);

        // Empty map: no notifications
        assert_eq!(result.len(), 0);
    }
}
