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

//! `RouteInfoManager` is a core component in Apache RocketMQ, primarily responsible for
//! managing and maintaining the routing information of the cluster. It ensures correct message
//! routing and load balancing within the system. Below are its main functions:
//!
//! ### 1. **Broker Information Management**
//! - Maintains information about all Brokers in the cluster, including addresses, names, and the
//!   clusters they belong to.
//! - Handles Broker registration, unregistration, and status updates.
//!
//! ### 2. **Topic Routing Information Management**
//! - Maintains the mapping relationship between Topics and Brokers to ensure messages are correctly
//!   routed to the target Brokers.
//! - Handles Topic creation, updates, and deletion operations.
//!
//! ### 3. **Consumer Group Information Management**
//! - Records the subscription relationships of consumer groups to ensure messages are correctly
//!   pushed to consumers.
//! - Handles consumer group registration, unregistration, and status updates.
//!
//! ### 4. **Routing Discovery Service**
//! - Provides the latest routing information to producers and consumers to ensure correct message
//!   routing and consumption.
//! - Clients periodically request the latest routing information.
//!
//! ### 5. **Broker Failure Handling**
//! - Detects Broker failures and updates routing information to avoid routing messages to
//!   unavailable Brokers.
//! - Supports automatic recovery and re-registration of Brokers.
//!
//! ### 6. **Cluster Scaling Support**
//! - Dynamically adjusts routing information to accommodate cluster scaling (expansion or
//!   reduction), ensuring high availability and load balancing.
//!
//! ### 7. **NameServer Data Maintenance**
//! - Stores and manages routing information in the NameServer to ensure data consistency and high
//!   availability.
//!
//! ### Summary
//! `RouteInfoManager` is a critical component in RocketMQ, responsible for managing routing
//! information and ensuring correct message routing and system stability.
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::common::TopicSysFlag;
use rocketmq_common::TimeUtils;
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
use rocketmq_remoting::protocol::static_topic::topic_queue_info::TopicQueueMappingInfo;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_rust::ArcMut;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::bootstrap::NameServerRuntimeInner;
use crate::route::batch_unregistration_service::BatchUnregistrationService;
use crate::route_info::broker_addr_info::BrokerAddrInfo;
use crate::route_info::broker_addr_info::BrokerLiveInfo;
use crate::route_info::broker_addr_info::BrokerStatusChangeInfo;

const DEFAULT_BROKER_CHANNEL_EXPIRED_TIME: i64 = 1000 * 60 * 2;

type TopicQueueTable = ArcMut<HashMap<CheetahString /* topic */, HashMap<CheetahString /* broker name */, QueueData>>>;
type BrokerAddrTable = ArcMut<HashMap<CheetahString /* brokerName */, BrokerData>>;
type ClusterAddrTable = ArcMut<HashMap<CheetahString /* clusterName */, HashSet<CheetahString /* brokerName */>>>;
type BrokerLiveTable = ArcMut<HashMap<BrokerAddrInfo /* brokerAddr */, BrokerLiveInfo>>;
type FilterServerTable = ArcMut<HashMap<BrokerAddrInfo /* brokerAddr */, Vec<CheetahString> /* Filter Server */>>;
type TopicQueueMappingInfoTable =
    ArcMut<HashMap<CheetahString /* topic */, HashMap<CheetahString /* brokerName */, TopicQueueMappingInfo>>>;

pub struct RouteInfoManager {
    pub(crate) topic_queue_table: TopicQueueTable,
    pub(crate) broker_addr_table: BrokerAddrTable,
    pub(crate) cluster_addr_table: ClusterAddrTable,
    pub(crate) broker_live_table: BrokerLiveTable,
    pub(crate) filter_server_table: FilterServerTable,
    pub(crate) topic_queue_mapping_info_table: TopicQueueMappingInfoTable,
    pub(crate) name_server_runtime_inner: ArcMut<NameServerRuntimeInner>,
    pub(crate) un_register_service: ArcMut<BatchUnregistrationService>,
    lock: Arc<parking_lot::RwLock<()>>,
}

#[allow(private_interfaces)]
impl RouteInfoManager {
    pub fn new(name_server_runtime_inner: ArcMut<NameServerRuntimeInner>) -> Self {
        let un_register_service = ArcMut::new(BatchUnregistrationService::new(name_server_runtime_inner.clone()));
        RouteInfoManager {
            topic_queue_table: ArcMut::new(HashMap::new()),
            broker_addr_table: ArcMut::new(HashMap::new()),
            cluster_addr_table: ArcMut::new(HashMap::new()),
            broker_live_table: ArcMut::new(HashMap::new()),
            filter_server_table: ArcMut::new(HashMap::new()),
            topic_queue_mapping_info_table: ArcMut::new(HashMap::new()),
            lock: Arc::new(Default::default()),
            name_server_runtime_inner,
            un_register_service,
        }
    }
}

//impl register broker
impl RouteInfoManager {
    pub fn submit_unregister_broker_request(&self, request: UnRegisterBrokerRequestHeader) -> bool {
        self.un_register_service.submit(request)
    }

    pub fn register_broker(
        &self,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
        ha_server_addr: CheetahString,
        zone_name: Option<CheetahString>,
        timeout_millis: Option<i64>,
        enable_acting_master: Option<bool>,
        topic_config_serialize_wrapper: TopicConfigAndMappingSerializeWrapper,
        filter_server_list: Vec<CheetahString>,
        channel: Channel,
    ) -> Option<RegisterBrokerResult> {
        let mut result = RegisterBrokerResult::default();
        let _write = self.lock.write();
        //init or update cluster information
        self.cluster_addr_table
            .mut_from_ref()
            .entry(cluster_name.clone())
            .or_default()
            .insert(broker_name.clone());

        let enable_acting_master_inner = enable_acting_master.unwrap_or_default();
        let mut register_first = if let Some(broker_data) = self.broker_addr_table.mut_from_ref().get_mut(&broker_name)
        {
            broker_data.set_enable_acting_master(enable_acting_master_inner);
            broker_data.set_zone_name(zone_name);
            false
        } else {
            let mut broker_data = BrokerData::new(cluster_name.clone(), broker_name.clone(), HashMap::new(), zone_name);
            broker_data.set_enable_acting_master(enable_acting_master_inner);
            self.broker_addr_table
                .mut_from_ref()
                .insert(broker_name.clone(), broker_data);
            true
        };
        let broker_data = self.broker_addr_table.mut_from_ref().get_mut(&broker_name).unwrap();
        let mut prev_min_broker_id = 0;
        if !broker_data.broker_addrs().is_empty() {
            prev_min_broker_id = broker_data.broker_addrs().keys().min().copied().unwrap();
        }
        let mut is_min_broker_id_changed = false;
        if broker_id < prev_min_broker_id {
            is_min_broker_id_changed = true;
        }

        //Switch slave to master: first remove <1, IP:PORT> in rocketmq-namesrv, then add <0,
        // IP:PORT> The same IP:PORT must only have one record in brokerAddrTable
        broker_data.remove_broker_by_addr(broker_id, &broker_addr);

        //If Local brokerId stateVersion bigger than the registering one
        if let Some(old_broker_addr) = broker_data.broker_addrs().get(&broker_id) {
            if old_broker_addr != &broker_addr {
                let addr_info_old = BrokerAddrInfo::new(cluster_name.clone(), old_broker_addr.to_string());
                if let Some(val) = self.broker_live_table.get(&addr_info_old) {
                    let old_state_version = val.data_version().state_version();
                    let new_state_version = topic_config_serialize_wrapper
                        .topic_config_serialize_wrapper
                        .data_version()
                        .state_version();
                    if old_state_version > new_state_version {
                        warn!(
                            "Registered Broker conflicts with the existed one, just ignore.:  Cluster:{}, \
                             BrokerName:{}, BrokerId:{},Old BrokerAddr:{}, Old Version:{}, New BrokerAddr:{}, New \
                             Version:{}.",
                            &cluster_name,
                            &broker_name,
                            broker_id,
                            old_broker_addr,
                            old_state_version,
                            &broker_addr,
                            new_state_version
                        );
                        self.broker_live_table
                            .mut_from_ref()
                            .remove(BrokerAddrInfo::new(cluster_name, broker_addr.clone()).as_ref());
                        return Some(result);
                    }
                }
            }
        }
        let size = topic_config_serialize_wrapper
            .topic_config_serialize_wrapper
            .topic_config_table()
            .len();
        if !broker_data.broker_addrs().contains_key(&broker_id) && size == 1 {
            warn!(
                "Can't register topicConfigWrapper={:?} because broker[{}]={} has not registered.",
                topic_config_serialize_wrapper
                    .topic_config_serialize_wrapper
                    .topic_config_table(),
                broker_id,
                broker_addr
            );
            return None;
        }

        let old_addr = broker_data.broker_addrs_mut().insert(broker_id, broker_addr.clone());

        register_first |= old_addr.is_none() || old_addr.as_ref().unwrap().is_empty();
        let is_master = mix_all::MASTER_ID == broker_id;

        let is_prime_slave = enable_acting_master.is_some()
            && !is_master
            && broker_id == broker_data.broker_addrs().keys().min().copied().unwrap();
        let broker_data = broker_data.clone();
        //handle master or prime slave topic config update
        if is_master || is_prime_slave {
            let tc_table = topic_config_serialize_wrapper
                .topic_config_serialize_wrapper
                .topic_config_table();
            let topic_queue_mapping_info_map = topic_config_serialize_wrapper.topic_queue_mapping_info_map();

            // Delete the topics that don't exist in tcTable from the current broker
            // Static topic is not supported currently
            if self
                .name_server_runtime_inner
                .name_server_config()
                .delete_topic_with_broker_registration
                && topic_queue_mapping_info_map.is_empty()
            {
                let old_topic_set = self.topic_set_of_broker_name(&broker_name);
                let new_topic_set = tc_table
                    .keys()
                    .map(|item| item.to_string())
                    .collect::<HashSet<String>>();
                let to_delete_topics = new_topic_set
                    .difference(&old_topic_set)
                    .map(|item| item.to_string())
                    .collect::<HashSet<String>>();
                for to_delete_topic in to_delete_topics {
                    let queue_data_map = self.topic_queue_table.mut_from_ref().get_mut(to_delete_topic.as_str());
                    if let Some(queue_data) = queue_data_map {
                        let removed_qd = queue_data.remove(&broker_name);
                        if let Some(ref removed_qd_inner) = removed_qd {
                            info!(
                                "broker[{}] delete topic[{}] queue[{:?}] because of master change",
                                broker_name, to_delete_topic, removed_qd_inner
                            );
                        }
                        if queue_data.is_empty() {
                            self.topic_queue_table.mut_from_ref().remove(to_delete_topic.as_str());
                        }
                    }
                }
            }
            let data_version = topic_config_serialize_wrapper
                .topic_config_serialize_wrapper
                .data_version();
            for topic_config in tc_table.values() {
                let mut config = topic_config.clone();
                if (register_first
                    || self.is_topic_config_changed(
                        &cluster_name,
                        &broker_addr,
                        data_version,
                        &broker_name,
                        topic_config.topic_name.as_ref().unwrap(),
                    ))
                    && is_prime_slave
                    && broker_data.enable_acting_master()
                {
                    config.perm &= !PermName::PERM_WRITE;
                }
                self.create_and_update_queue_data(&broker_name, config);
            }
            if self.is_broker_topic_config_changed(&cluster_name, &broker_addr, data_version) || register_first {
                for kv in topic_queue_mapping_info_map {
                    let topic = kv.key();
                    let vtq_info = kv.value();
                    if !self.topic_queue_mapping_info_table.contains_key(topic) {
                        self.topic_queue_mapping_info_table
                            .mut_from_ref()
                            .insert(topic.clone(), HashMap::new());
                    }
                    self.topic_queue_mapping_info_table
                        .mut_from_ref()
                        .get_mut(topic)
                        .unwrap()
                        .insert(vtq_info.bname.as_ref().unwrap().clone(), vtq_info.as_ref().clone());
                }
            }
        }

        let broker_addr_info = BrokerAddrInfo::new(cluster_name.clone(), broker_addr.clone());

        self.broker_live_table.mut_from_ref().insert(
            broker_addr_info.clone(),
            BrokerLiveInfo::new(
                get_current_millis() as i64,
                timeout_millis.unwrap_or(DEFAULT_BROKER_CHANNEL_EXPIRED_TIME),
                topic_config_serialize_wrapper
                    .topic_config_serialize_wrapper
                    .data_version()
                    .clone(),
                ha_server_addr,
                channel.remote_address(),
                channel.channel_id_owned(),
            ),
        );
        if filter_server_list.is_empty() {
            self.filter_server_table.mut_from_ref().remove(&broker_addr_info);
        } else {
            self.filter_server_table
                .mut_from_ref()
                .insert(broker_addr_info.clone(), filter_server_list);
        }

        if mix_all::MASTER_ID != broker_id {
            let master_address = broker_data.broker_addrs().get(&(mix_all::MASTER_ID));
            if let Some(master_addr) = master_address {
                let master_livie_info = self
                    .broker_live_table
                    .get(BrokerAddrInfo::new(cluster_name, master_addr.clone()).as_ref());
                if let Some(info) = master_livie_info {
                    result.ha_server_addr = info.ha_server_addr().clone();
                    result.master_addr = info.ha_server_addr().clone();
                }
            }
        }
        if is_min_broker_id_changed
            && self
                .name_server_runtime_inner
                .name_server_config()
                .notify_min_broker_id_changed
        {
            self.notify_min_broker_id_changed(
                broker_data.broker_addrs(),
                None,
                Some(
                    self.broker_live_table
                        .get(&broker_addr_info)
                        .unwrap()
                        .ha_server_addr()
                        .clone(),
                ),
            )
        }
        drop(_write);
        Some(result)
    }
}

impl RouteInfoManager {
    pub(crate) fn get_all_cluster_info(&self) -> ClusterInfo {
        ClusterInfo::new(
            Some(self.broker_addr_table.as_ref().clone()),
            Some(self.cluster_addr_table.as_ref().clone()),
        )
    }

    pub(crate) fn pickup_topic_route_data(&self, topic: &CheetahString) -> Option<TopicRouteData> {
        let mut topic_route_data = TopicRouteData {
            order_topic_conf: None,
            broker_datas: Vec::new(),
            queue_datas: Vec::new(),
            filter_server_table: HashMap::new(),
            topic_queue_mapping_by_broker: None,
        };

        let mut found_queue_data = false;
        let mut found_broker_data = false;

        // Acquire read lock
        let lock = self.lock.read();
        if let Some(queue_data_map) = self.topic_queue_table.get(topic) {
            topic_route_data
                .queue_datas
                .extend(queue_data_map.values().cloned().collect::<Vec<_>>());

            found_queue_data = true;

            for broker_name in queue_data_map.keys() {
                if let Some(broker_data) = self.broker_addr_table.get(broker_name) {
                    let broker_data_clone = broker_data.clone();
                    topic_route_data.broker_datas.push(broker_data_clone);
                    found_broker_data = true;

                    if !self.filter_server_table.is_empty() {
                        for broker_addr in broker_data.broker_addrs().values() {
                            let broker_addr_info = BrokerAddrInfo::new(broker_data.cluster(), broker_addr.clone());
                            if let Some(filter_server_list) = self.filter_server_table.get(&broker_addr_info) {
                                topic_route_data
                                    .filter_server_table
                                    .insert(broker_addr.clone(), filter_server_list.clone());
                            }
                        }
                    }
                }
            }
        }
        drop(lock);
        debug!("pickup_topic_route_data {:?} {:?}", topic, topic_route_data);

        if found_broker_data && found_queue_data {
            topic_route_data.topic_queue_mapping_by_broker = self.topic_queue_mapping_info_table.get(topic).cloned();

            if !self
                .name_server_runtime_inner
                .name_server_config()
                .support_acting_master
            {
                return Some(topic_route_data);
            }

            if topic.starts_with(TopicValidator::SYNC_BROKER_MEMBER_GROUP_PREFIX) {
                return Some(topic_route_data);
            }

            if topic_route_data.broker_datas.is_empty() || topic_route_data.queue_datas.is_empty() {
                return Some(topic_route_data);
            }

            let need_acting_master = topic_route_data.broker_datas.iter().any(|broker_data| {
                !broker_data.broker_addrs().is_empty()
                    && !broker_data.broker_addrs().contains_key(&(mix_all::MASTER_ID))
            });

            if !need_acting_master {
                return Some(topic_route_data);
            }

            for broker_data in &mut topic_route_data.broker_datas {
                if broker_data.broker_addrs().is_empty()
                    || broker_data.broker_addrs().contains_key(&(mix_all::MASTER_ID))
                    || !broker_data.enable_acting_master()
                {
                    continue;
                }

                // No master
                for queue_data in &topic_route_data.queue_datas {
                    if queue_data.broker_name() == broker_data.broker_name() {
                        if !PermName::is_writeable(queue_data.perm()) {
                            if let Some(min_broker_id) = broker_data.broker_addrs().keys().min() {
                                let min_broker_id = *min_broker_id;
                                if let Some(acting_master_addr) = broker_data.broker_addrs_mut().remove(&min_broker_id)
                                {
                                    broker_data
                                        .broker_addrs_mut()
                                        .insert(mix_all::MASTER_ID, acting_master_addr);
                                }
                            }
                        }
                        break;
                    }
                }
            }

            return Some(topic_route_data);
        }

        None
    }
}

impl RouteInfoManager {
    fn topic_set_of_broker_name(&self, broker_name: &str) -> HashSet<String> {
        let mut topic_of_broker = HashSet::new();
        for (key, value) in self.topic_queue_table.iter() {
            if value.contains_key(broker_name) {
                topic_of_broker.insert(key.to_string());
            }
        }
        topic_of_broker
    }

    pub(crate) fn is_topic_config_changed(
        &self,
        cluster_name: &CheetahString,
        broker_addr: &CheetahString,
        data_version: &DataVersion,
        broker_name: &CheetahString,
        topic: &str,
    ) -> bool {
        let is_change = self.is_broker_topic_config_changed(cluster_name, broker_addr, data_version);
        if is_change {
            return true;
        }
        let queue_data_map = self.topic_queue_table.get(topic);
        if let Some(queue_data) = queue_data_map {
            if queue_data.is_empty() {
                return true;
            }
            !queue_data.contains_key(broker_name)
        } else {
            true
        }
    }

    pub(crate) fn is_broker_topic_config_changed(
        &self,
        cluster_name: &CheetahString,
        broker_addr: &CheetahString,
        data_version: &DataVersion,
    ) -> bool {
        let option = self.query_broker_topic_config(cluster_name.clone(), broker_addr.clone());
        if let Some(pre) = option {
            pre != data_version
        } else {
            true
        }
    }

    pub(crate) fn query_broker_topic_config(
        &self,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
    ) -> Option<&DataVersion> {
        let info = BrokerAddrInfo::new(cluster_name, broker_addr);
        let pre = self.broker_live_table.get(info.as_ref());
        if let Some(live_info) = pre {
            return Some(live_info.data_version());
        }
        None
    }

    fn create_and_update_queue_data(&self, broker_name: &CheetahString, topic_config: TopicConfig) {
        let queue_data = QueueData::new(
            broker_name.clone(),
            topic_config.write_queue_nums,
            topic_config.read_queue_nums,
            topic_config.perm,
            topic_config.topic_sys_flag,
        );

        let queue_data_map = self
            .topic_queue_table
            .mut_from_ref()
            .get_mut(topic_config.topic_name.as_ref().unwrap().as_str());
        if let Some(queue_data_map_inner) = queue_data_map {
            let existed_qd = queue_data_map_inner.get(broker_name);

            if let Some(existed_qd) = existed_qd {
                if existed_qd != &queue_data {
                    info!(
                        "topic changed, {} OLD: {:?} NEW: {:?}",
                        topic_config.topic_name.as_ref().unwrap(),
                        existed_qd,
                        queue_data
                    );
                    queue_data_map_inner.insert(broker_name.clone(), queue_data);
                } else {
                    queue_data_map_inner.insert(broker_name.clone(), queue_data);
                }
            }
        } else {
            let mut queue_data_map_inner = HashMap::new();
            info!(
                "new topic registered, {} {:?}",
                topic_config.topic_name.as_ref().unwrap(),
                &queue_data
            );
            queue_data_map_inner.insert(broker_name.clone(), queue_data);
            self.topic_queue_table
                .mut_from_ref()
                .insert(topic_config.topic_name.as_ref().unwrap().clone(), queue_data_map_inner);
        }
    }

    fn notify_min_broker_id_changed(
        &self,
        broker_addr_map: &HashMap<u64, CheetahString>,
        offline_broker_addr: Option<CheetahString>,
        ha_broker_addr: Option<CheetahString>,
    ) {
        if broker_addr_map.is_empty() {
            return;
        }
        let min_broker_id = broker_addr_map.keys().min().copied().unwrap();
        // notify master
        let request_header = NotifyMinBrokerIdChangeRequestHeader::new(
            Some(min_broker_id),
            None,
            broker_addr_map.get(&min_broker_id).cloned(),
            offline_broker_addr.clone(),
            ha_broker_addr,
        );

        if let Some(broker_addrs_notify) = Self::choose_broker_addrs_to_notify(broker_addr_map, offline_broker_addr) {
            for broker_addr in broker_addrs_notify {
                let remoting_client = self.name_server_runtime_inner.clone();
                let requst_header = request_header.clone();
                let broker_addr = broker_addr.clone();
                tokio::spawn(async move {
                    let _ = remoting_client
                        .remoting_client()
                        .invoke_request_oneway(
                            &broker_addr,
                            RemotingCommand::create_request_command(
                                RequestCode::NotifyMinBrokerIdChange,
                                requst_header,
                            ),
                            3000,
                        )
                        .await;
                });
            }
        }
    }

    fn choose_broker_addrs_to_notify(
        broker_addr_map: &HashMap<u64, CheetahString>,
        offline_broker_addr: Option<CheetahString>,
    ) -> Option<Vec<&CheetahString>> {
        if broker_addr_map.len() == 1 || offline_broker_addr.is_some() {
            return Some(broker_addr_map.values().collect());
        }
        let min_broker_id = broker_addr_map.keys().min().copied().unwrap();
        let broker_addr_vec = broker_addr_map
            .iter()
            .filter(|(key, _value)| **key != min_broker_id)
            .map(|(_, value)| value)
            .collect();
        Some(broker_addr_vec)
    }

    pub(crate) fn update_broker_info_update_timestamp(
        &mut self,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
    ) {
        let broker_addr_info = BrokerAddrInfo::new(cluster_name, broker_addr);
        if let Some(value) = self.broker_live_table.get_mut(broker_addr_info.as_ref()) {
            value.last_update_timestamp = TimeUtils::get_current_millis() as i64;
        }
    }

    pub(crate) fn get_broker_member_group(
        &mut self,
        cluster_name: CheetahString,
        broker_name: CheetahString,
    ) -> Option<BrokerMemberGroup> {
        let mut group_member = BrokerMemberGroup::new(cluster_name, broker_name.clone());
        let lock_ = self.lock.read();
        if let Some(broker_data) = self.broker_addr_table.get(&broker_name) {
            group_member.broker_addrs = broker_data.broker_addrs().clone();
        }
        drop(lock_);
        Some(group_member)
    }

    #[inline]
    pub(crate) fn wipe_write_perm_of_broker_by_lock(&self, broker_name: &CheetahString) -> i32 {
        let lock = self.lock.write();
        let cnt = self.operate_write_perm_of_broker(broker_name, RequestCode::WipeWritePermOfBroker);
        drop(lock);
        cnt
    }

    #[inline]
    pub(crate) fn add_write_perm_of_broker_by_lock(&self, broker_name: &CheetahString) -> i32 {
        let lock = self.lock.write();
        let cnt = self.operate_write_perm_of_broker(broker_name, RequestCode::AddWritePermOfBroker);
        drop(lock);
        cnt
    }

    fn operate_write_perm_of_broker(&self, broker_name: &CheetahString, request_code: RequestCode) -> i32 {
        let mut topic_cnt = 0;
        for (_topic, qd_map) in self.topic_queue_table.mut_from_ref().iter_mut() {
            let qd = qd_map.get_mut(broker_name);
            if qd.is_none() {
                continue;
            }
            let qd = qd.unwrap();
            let mut perm = qd.perm;
            match request_code {
                RequestCode::WipeWritePermOfBroker => {
                    perm &= !PermName::PERM_WRITE;
                }
                RequestCode::AddWritePermOfBroker => {
                    perm = PermName::PERM_READ | PermName::PERM_WRITE;
                }
                _ => {}
            }
            qd.perm = perm;
            topic_cnt += 1;
        }
        topic_cnt
    }

    pub(crate) fn get_all_topic_list(&self) -> TopicList {
        let lock = self.lock.read();
        let topics = self.topic_queue_table.keys().cloned().collect::<Vec<CheetahString>>();
        drop(lock);
        TopicList {
            topic_list: topics,
            broker_addr: None,
        }
    }

    pub(crate) fn delete_topic(&mut self, topic: CheetahString, cluster_name: Option<CheetahString>) {
        let lock = self.lock.write();
        if cluster_name.as_ref().is_some_and(|inner| !inner.is_empty()) {
            let cluster_name_inner = cluster_name.unwrap();
            let broker_names = self.cluster_addr_table.get(&cluster_name_inner);
            if broker_names.is_none() || broker_names.unwrap().is_empty() {
                return;
            }
            let topic_queue_table_ = self.topic_queue_table.mut_from_ref();
            if let Some(queue_data_map) = topic_queue_table_.get_mut(&topic) {
                for broker_name in broker_names.unwrap() {
                    if let Some(remove_qd) = queue_data_map.remove(broker_name) {
                        info!(
                            "deleteTopic, remove one broker's topic {} {} {:?}",
                            broker_name, &topic, remove_qd
                        )
                    }
                }
                if queue_data_map.is_empty() {
                    topic_queue_table_.remove(&topic);
                }
            }
        } else {
            self.topic_queue_table.mut_from_ref().remove(&topic);
        }
        drop(lock)
    }

    pub(crate) fn register_topic(&self, topic: CheetahString, queue_data_vec: Vec<QueueData>) {
        if queue_data_vec.is_empty() {
            return;
        }
        let lock = self.lock.write();
        if !self.topic_queue_table.contains_key(&topic) {
            self.topic_queue_table
                .mut_from_ref()
                .insert(topic.clone(), HashMap::new());
        }
        let queue_data_map = self.topic_queue_table.mut_from_ref().get_mut(&topic).unwrap();
        let vec_length = queue_data_vec.len();
        for queue_data in queue_data_vec {
            if !self.broker_addr_table.contains_key(queue_data.broker_name()) {
                warn!("Register topic contains illegal broker, {}, {:?}", topic, queue_data);
                return;
            }
            queue_data_map.insert(queue_data.broker_name().clone(), queue_data);
        }
        drop(lock);

        if queue_data_map.len() > vec_length {
            info!("Topic route already exist.{}, {:?}", topic, queue_data_map)
        } else {
            info!("Register topic route:{}, {:?}", topic, queue_data_map)
        }
    }

    pub(crate) fn get_topics_by_cluster(&self, cluster: &CheetahString) -> TopicList {
        let mut topic_list = Vec::new();
        let lock = self.lock.read();
        if let Some(broker_name_set) = self.cluster_addr_table.get(cluster) {
            for broker_name in broker_name_set {
                for (topic, queue_data_map) in self.topic_queue_table.iter() {
                    if let Some(_queue_data) = queue_data_map.get(broker_name) {
                        topic_list.push(topic.clone());
                    }
                }
            }
        }
        drop(lock);
        TopicList {
            topic_list,
            broker_addr: None,
        }
    }

    pub(crate) fn get_system_topic_list(&self) -> TopicList {
        let mut topic_list = Vec::new();
        let mut broker_addr_out = None;
        for (cluster_name, broker_set) in self.cluster_addr_table.iter() {
            topic_list.push(cluster_name.clone());
            broker_set.iter().for_each(|broker_name| {
                topic_list.push(broker_name.clone());
            });
        }
        if !self.broker_addr_table.is_empty() {
            for broker_addr in self.broker_addr_table.values() {
                let broker_addrs = broker_addr.broker_addrs();
                if !broker_addrs.is_empty() {
                    broker_addr_out = Some(broker_addrs.values().next().unwrap().clone());
                    break;
                }
            }
        }
        TopicList {
            topic_list,
            broker_addr: broker_addr_out,
        }
    }

    pub(crate) fn get_unit_topics(&self) -> TopicList {
        let mut topic_list = Vec::new();
        for (topic, entry) in self.topic_queue_table.iter() {
            if !entry.is_empty() && TopicSysFlag::has_unit_flag(entry.values().next().unwrap().topic_sys_flag()) {
                topic_list.push(topic.clone());
            }
        }
        TopicList {
            topic_list,
            broker_addr: None,
        }
    }

    pub(crate) fn get_has_unit_sub_topic_list(&self) -> TopicList {
        let mut topic_list = Vec::new();
        for (topic, entry) in self.topic_queue_table.iter() {
            if !entry.is_empty() && TopicSysFlag::has_unit_sub_flag(entry.values().next().unwrap().topic_sys_flag()) {
                topic_list.push(topic.clone());
            }
        }
        TopicList {
            topic_list,
            broker_addr: None,
        }
    }

    pub(crate) fn get_has_unit_sub_un_unit_topic_list(&self) -> TopicList {
        let mut topic_list = Vec::new();
        for (topic, entry) in self.topic_queue_table.iter() {
            if !entry.is_empty()
                && !TopicSysFlag::has_unit_flag(entry.values().next().unwrap().topic_sys_flag())
                && TopicSysFlag::has_unit_sub_flag(entry.values().next().unwrap().topic_sys_flag())
            {
                topic_list.push(topic.clone());
            }
        }
        TopicList {
            topic_list,
            broker_addr: None,
        }
    }

    pub fn scan_not_active_broker(&mut self) {
        for (broker_addr_info, broker_live_info) in self.broker_live_table.clone().iter() {
            if broker_live_info.heartbeat_timeout_millis + broker_live_info.last_update_timestamp
                < TimeUtils::get_current_millis() as i64
            {
                self.on_connection_disconnected(broker_addr_info);
            }
        }
    }

    fn on_connection_disconnected(&mut self, broker_addr_info: &BrokerAddrInfo) {
        let mut request_header = UnRegisterBrokerRequestHeader::default();
        let need_un_register = self.setup_un_register_request(&mut request_header, broker_addr_info);
        if need_un_register {
            self.un_register_broker(vec![request_header]);
        }
    }

    fn setup_un_register_request(
        &self,
        un_register_request: &mut UnRegisterBrokerRequestHeader,
        broker_addr_info: &BrokerAddrInfo,
    ) -> bool {
        un_register_request
            .cluster_name
            .clone_from(&CheetahString::from_slice(broker_addr_info.cluster_name.as_str()));
        un_register_request
            .broker_addr
            .clone_from(&CheetahString::from_slice(broker_addr_info.broker_addr.as_str()));

        for (_broker_addr, broker_data) in self.broker_addr_table.iter() {
            if broker_addr_info.cluster_name != broker_data.cluster() {
                continue;
            }
            for (broker_id, ip) in broker_data.broker_addrs().iter() {
                if &broker_addr_info.broker_addr == ip {
                    un_register_request.broker_name = CheetahString::from_string(broker_data.broker_name().to_string());
                    un_register_request.broker_id = *broker_id;
                    return true;
                }
            }
        }
        false
    }

    pub(crate) fn un_register_broker(&mut self, un_register_requests: Vec<UnRegisterBrokerRequestHeader>) {
        let mut remove_broker = HashSet::new();
        let mut reduced_broker = HashSet::new();
        let mut need_notify_broker_map = HashMap::new();

        for un_register_request in &un_register_requests {
            let broker_name = &un_register_request.broker_name;
            let cluster_name = &un_register_request.cluster_name;
            let broker_addr = &un_register_request.broker_addr;

            let broker_addr_info = BrokerAddrInfo::new(cluster_name.clone(), broker_addr.clone());
            let pre = self.broker_live_table.remove(&broker_addr_info);
            info!(
                "unregisterBroker, remove from brokerLiveTable {}, {}",
                if pre.is_some() { "OK" } else { "Fail" },
                &broker_addr_info
            );

            self.filter_server_table.remove(&broker_addr_info);

            let mut remove_broker_name = false;
            let mut is_min_broker_id_changed = false;

            if let Some(broker_data) = self.broker_addr_table.get_mut(broker_name) {
                if !broker_data.broker_addrs().is_empty()
                    && un_register_request.broker_id == broker_data.broker_addrs().keys().min().copied().unwrap()
                {
                    is_min_broker_id_changed = true;
                }
                broker_data
                    .broker_addrs_mut()
                    .retain(|_broker_id, broker_addr_inner| broker_addr != broker_addr_inner);

                if broker_data.broker_addrs_mut().is_empty() {
                    self.broker_addr_table.remove(broker_name);
                    remove_broker_name = true;
                } else if is_min_broker_id_changed {
                    need_notify_broker_map.insert(
                        broker_name.clone(),
                        BrokerStatusChangeInfo {
                            broker_addrs: broker_data.broker_addrs().clone(),
                            offline_broker_addr: broker_addr.clone(),
                            ha_broker_addr: CheetahString::empty(),
                        },
                    );
                }
            }

            if remove_broker_name {
                let name_set = self.cluster_addr_table.get_mut(cluster_name.as_str());
                if let Some(name_set_inner) = name_set {
                    name_set_inner.remove(broker_name);
                    if name_set_inner.is_empty() {
                        self.cluster_addr_table.remove(cluster_name.as_str());
                    }
                }
                remove_broker.insert(broker_name);
            } else {
                reduced_broker.insert(broker_name);
            }
        }
        self.clean_topic_by_un_register_requests(remove_broker, reduced_broker);
        if !need_notify_broker_map.is_empty()
            && self
                .name_server_runtime_inner
                .name_server_config()
                .notify_min_broker_id_changed
        {
            for (broker_name, broker_status_change_info) in need_notify_broker_map {
                let broker_data = self.broker_addr_table.get(&broker_name);
                if let Some(broker_data) = broker_data {
                    if !broker_data.enable_acting_master() {
                        continue;
                    }
                    let broker_addrs = broker_status_change_info.broker_addrs;
                    let offline_broker_addr = broker_status_change_info.offline_broker_addr;
                    self.notify_min_broker_id_changed(&broker_addrs, Some(offline_broker_addr), None);
                }
            }
        }
    }

    fn clean_topic_by_un_register_requests(
        &mut self,
        removed_broker: HashSet<&CheetahString>,
        reduced_broker: HashSet<&CheetahString>,
    ) {
        let mut delete_topic = HashSet::new();
        for (topic, queue_data_map) in self.topic_queue_table.iter_mut() {
            for broker_name in removed_broker.iter() {
                if let Some(removed_qd) = queue_data_map.remove(*broker_name) {
                    info!(
                        "removeTopicByBrokerName, remove one broker's topic {} {:?}",
                        topic, removed_qd
                    );
                }
            }

            if queue_data_map.is_empty() {
                info!("removeTopicByBrokerName, remove the topic all queue {}", topic);
                delete_topic.insert(topic.clone());
            }

            for broker_name in &reduced_broker {
                if let Some(queue_data) = queue_data_map.get_mut(*broker_name) {
                    if self
                        .broker_addr_table
                        .get(*broker_name)
                        .is_some_and(|b| b.enable_acting_master())
                    {
                        // Master has been unregistered, wipe the write perm
                        let flag = {
                            let broker_data = self.broker_addr_table.get(*broker_name);
                            if let Some(broker_data_unwrap) = broker_data {
                                if broker_data_unwrap.broker_addrs().is_empty() {
                                    true
                                } else {
                                    broker_data_unwrap.broker_addrs().keys().copied().min().unwrap() > 0
                                }
                            } else {
                                true
                            }
                        };
                        //if self.is_no_master_exists(broker_name.as_str()) {
                        if flag {
                            // Update the queue data's permission, assuming PermName is an enum
                            // For simplicity, I'm using 0 as PERM_WRITE value, please replace it
                            // with the actual value
                            queue_data.perm &= !PermName::PERM_WRITE
                        }
                    }
                }
            }
        }
        for topic in delete_topic {
            self.topic_queue_table.remove(topic.as_str());
        }
    }

    fn is_no_master_exists(&self, broker_name: &str) -> bool {
        let broker_data = self.broker_addr_table.get(broker_name);
        if broker_data.is_none() {
            return true;
        }
        let broker_data_unwrap = broker_data.unwrap();
        if broker_data_unwrap.broker_addrs().is_empty() {
            return true;
        }
        broker_data_unwrap.broker_addrs().keys().cloned().min().unwrap() > 0
    }

    pub fn connection_disconnected(&mut self, socket_addr: SocketAddr) {
        let mut broker_addr_info = None;
        for (bai, bli) in self.broker_live_table.as_ref() {
            if bli.remote_addr == socket_addr {
                broker_addr_info = Some(bai.clone());
                break;
            }
        }
        if let Some(bai) = broker_addr_info {
            let mut request_header = UnRegisterBrokerRequestHeader::default();
            let need_un_register = self.setup_un_register_request(&mut request_header, &bai);
            if need_un_register {
                self.un_register_broker(vec![request_header]);
            }
        }
    }

    pub fn on_channel_destroy(&self, channel: &Channel) {
        let lock = self.lock.read();
        let mut un_register_request = UnRegisterBrokerRequestHeader::default();
        let mut broker_addr_found = None;
        let mut need_un_register = false;
        for (broker_addr_info, broker_live_info) in self.broker_live_table.as_ref() {
            if broker_live_info.channel_id.as_str() == channel.channel_id() {
                broker_addr_found = Some(broker_addr_info.clone());
                break;
            }
        }
        if let Some(broker_addr_info) = &mut broker_addr_found {
            need_un_register = self.setup_un_register_request(&mut un_register_request, broker_addr_info);
        }
        drop(lock);
        if need_un_register {
            let result = self.submit_unregister_broker_request(un_register_request.clone());
            info!(
                "the broker's channel destroyed, submit the unregister request at once, broker info: {}, submit \
                 result: {}",
                un_register_request, result
            );
        }
    }
}

// Non-instance method implementations
impl RouteInfoManager {
    //! start client connection disconnected listener
    pub fn start(&self) {
        /* let mut inner = self.name_server_runtime_inner.clone(); */
        self.un_register_service.mut_from_ref().start();
        /*let mut receiver = receiver;
        tokio::spawn(async move {
            while let Ok(socket_addr) = receiver.recv().await {
                inner
                    .route_info_manager_mut()
                    .connection_disconnected(socket_addr);
            }
        });*/
    }

    pub fn shutdown(&self) {
        self.un_register_service.shutdown();
    }
}
