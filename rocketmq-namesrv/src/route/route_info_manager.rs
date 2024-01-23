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

use std::{
    collections::{HashMap, HashSet},
    time::{Duration, SystemTime},
};

use rocketmq_common::{
    common::{
        config::TopicConfig, constant::PermName, mix_all, namesrv::namesrv_config::NamesrvConfig,
        topic::TopicValidator,
    },
    TimeUtils,
};
use rocketmq_remoting::{
    clients::RemoteClient,
    code::request_code::RequestCode,
    protocol::{
        body::{
            broker_body::{broker_member_group::BrokerMemberGroup, cluster_info::ClusterInfo},
            topic::topic_list::TopicList,
            topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper,
        },
        header::namesrv::brokerid_change_request_header::NotifyMinBrokerIdChangeRequestHeader,
        namesrv::RegisterBrokerResult,
        remoting_command::RemotingCommand,
        route::route_data_view::{BrokerData, QueueData, TopicRouteData},
        static_topic::topic_queue_info::TopicQueueMappingInfo,
        DataVersion,
    },
};
use tracing::{debug, info, warn};

use crate::route_info::broker_addr_info::{BrokerAddrInfo, BrokerLiveInfo};

const DEFAULT_BROKER_CHANNEL_EXPIRED_TIME: i64 = 1000 * 60 * 2;

type TopicQueueTable =
    HashMap<String /* topic */, HashMap<String /* broker name */, QueueData>>;
type BrokerAddrTable = HashMap<String /* brokerName */, BrokerData>;
type ClusterAddrTable = HashMap<String /* clusterName */, HashSet<String /* brokerName */>>;
type BrokerLiveTable = HashMap<BrokerAddrInfo /* brokerAddr */, BrokerLiveInfo>;
type FilterServerTable =
    HashMap<BrokerAddrInfo /* brokerAddr */, Vec<String> /* Filter Server */>;
type TopicQueueMappingInfoTable =
    HashMap<String /* topic */, HashMap<String /* brokerName */, TopicQueueMappingInfo>>;

#[derive(Debug, Clone, Default)]
pub struct RouteInfoManager {
    pub(crate) topic_queue_table: TopicQueueTable,
    pub(crate) broker_addr_table: BrokerAddrTable,
    pub(crate) cluster_addr_table: ClusterAddrTable,
    pub(crate) broker_live_table: BrokerLiveTable,
    pub(crate) filter_server_table: FilterServerTable,
    pub(crate) topic_queue_mapping_info_table: TopicQueueMappingInfoTable,
    pub(crate) namesrv_config: NamesrvConfig,
    pub(crate) remote_client: RemoteClient,
}

#[allow(private_interfaces)]
impl RouteInfoManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_config(namesrv_config: NamesrvConfig) -> Self {
        RouteInfoManager {
            topic_queue_table: HashMap::new(),
            broker_addr_table: HashMap::new(),
            cluster_addr_table: HashMap::new(),
            broker_live_table: HashMap::new(),
            filter_server_table: HashMap::new(),
            topic_queue_mapping_info_table: HashMap::new(),
            namesrv_config,
            remote_client: RemoteClient::new(),
        }
    }
}

//impl register broker
impl RouteInfoManager {
    pub fn register_broker(
        &mut self,
        cluster_name: String,
        broker_addr: String,
        broker_name: String,
        broker_id: i64,
        ha_server_addr: String,
        zone_name: Option<String>,
        _timeout_millis: Option<i64>,
        enable_acting_master: Option<bool>,
        topic_config_serialize_wrapper: TopicConfigAndMappingSerializeWrapper,
        filter_server_list: Vec<String>,
    ) -> Option<RegisterBrokerResult> {
        let mut result = RegisterBrokerResult::default();
        //init or update cluster information
        if !self.cluster_addr_table.contains_key(&cluster_name) {
            self.cluster_addr_table
                .insert(cluster_name.to_string(), HashSet::new());
        }
        self.cluster_addr_table
            .get_mut(&cluster_name)
            .unwrap()
            .insert(broker_name.clone());

        let enable_acting_master_inner = if let Some(value) = enable_acting_master {
            value
        } else {
            false
        };
        let mut register_first =
            if let Some(broker_data) = self.broker_addr_table.get_mut(&broker_name) {
                broker_data.set_enable_acting_master(enable_acting_master_inner);
                broker_data.set_zone_name(zone_name.clone());
                false
            } else {
                let mut broker_data = BrokerData::new(
                    cluster_name.clone(),
                    broker_name.clone(),
                    HashMap::new(),
                    zone_name,
                );
                broker_data.set_enable_acting_master(enable_acting_master_inner);
                self.broker_addr_table
                    .insert(broker_name.clone(), broker_data);
                true
            };
        let broker_data = self.broker_addr_table.get_mut(&broker_name).unwrap();
        let mut prev_min_broker_id = 0i64;
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
                let addr_info_old =
                    BrokerAddrInfo::new(cluster_name.clone(), old_broker_addr.to_string());
                if let Some(val) = self.broker_live_table.get(&addr_info_old) {
                    let old_state_version = val.data_version().state_version();
                    let new_state_version = topic_config_serialize_wrapper
                        .data_version()
                        .as_ref()
                        .unwrap()
                        .state_version();
                    if old_state_version > new_state_version {
                        warn!(
                            "Registered Broker conflicts with the existed one, just ignore.:  \
                             Cluster:{}, BrokerName:{}, BrokerId:{},Old BrokerAddr:{}, Old \
                             Version:{}, New BrokerAddr:{}, New Version:{}.",
                            &cluster_name,
                            &broker_name,
                            broker_id,
                            old_broker_addr,
                            old_state_version,
                            &broker_addr,
                            new_state_version
                        );
                        self.broker_live_table.remove(
                            BrokerAddrInfo::new(cluster_name.clone(), broker_addr.clone()).as_ref(),
                        );
                        return Some(result);
                    }
                }
            }
        }
        let size = if let Some(val) = topic_config_serialize_wrapper.topic_config_table() {
            val.len()
        } else {
            0
        };
        if !broker_data.broker_addrs().contains_key(&broker_id) && size == 1 {
            warn!(
                "Can't register topicConfigWrapper={:?} because broker[{}]={} has not registered.",
                topic_config_serialize_wrapper.topic_config_table(),
                broker_id,
                broker_addr
            );
            return None;
        }

        let old_addr = broker_data
            .broker_addrs_mut()
            .insert(broker_id, broker_addr.clone());

        register_first |= old_addr.is_none();
        let is_master = mix_all::MASTER_ID == broker_id as u64;

        let is_prime_slave = enable_acting_master.is_some()
            && !is_master
            && broker_id == broker_data.broker_addrs().keys().min().copied().unwrap();
        let broker_data = broker_data.clone();
        //handle master or prime slave topic config update
        if is_master || is_prime_slave {
            if let Some(tc_table) = topic_config_serialize_wrapper.topic_config_table() {
                let topic_queue_mapping_info_map =
                    topic_config_serialize_wrapper.topic_queue_mapping_info_map();

                // Delete the topics that don't exist in tcTable from the current broker
                // Static topic is not supported currently
                if self.namesrv_config.delete_topic_with_broker_registration
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
                        let queue_data_map = self.topic_queue_table.get_mut(&to_delete_topic);
                        if let Some(queue_data) = queue_data_map {
                            let removed_qd = queue_data.remove(&broker_name);
                            if let Some(ref removed_qd_inner) = removed_qd {
                                info!(
                                    "broker[{}] delete topic[{}] queue[{:?}] because of master \
                                     change",
                                    broker_name, to_delete_topic, removed_qd_inner
                                );
                            }
                            if queue_data.is_empty() {
                                self.topic_queue_table.remove(&to_delete_topic);
                            }
                        }
                    }
                }
                let data_version = topic_config_serialize_wrapper
                    .data_version()
                    .as_ref()
                    .unwrap();
                for topic_config in tc_table.values() {
                    let mut config = topic_config.clone();
                    if (register_first
                        || self.is_topic_config_changed(
                            &cluster_name,
                            &broker_addr,
                            data_version,
                            &broker_name,
                            &topic_config.topic_name,
                        ))
                        && is_prime_slave
                        && broker_data.enable_acting_master()
                    {
                        config.perm &= !(PermName::PERM_WRITE as u32);
                    }
                    self.create_and_update_queue_data(&broker_name, config);
                }
                if self.is_broker_topic_config_changed(&cluster_name, &broker_addr, data_version)
                    || register_first
                {
                    for (topic, vtq_info) in topic_queue_mapping_info_map {
                        if !self.topic_queue_mapping_info_table.contains_key(topic) {
                            self.topic_queue_mapping_info_table
                                .insert(topic.to_string(), HashMap::new());
                        }
                        self.topic_queue_mapping_info_table
                            .get_mut(topic)
                            .unwrap()
                            .insert(
                                vtq_info.bname.as_ref().unwrap().to_string(),
                                vtq_info.clone(),
                            );
                    }
                }
            }
        }

        let broker_addr_info = BrokerAddrInfo::new(cluster_name.clone(), broker_addr.clone());

        self.broker_live_table.insert(
            broker_addr_info.clone(),
            BrokerLiveInfo::new(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_millis() as i64,
                DEFAULT_BROKER_CHANNEL_EXPIRED_TIME,
                if let Some(data_version) = topic_config_serialize_wrapper.data_version() {
                    data_version.clone()
                } else {
                    DataVersion::default()
                },
                ha_server_addr.clone(),
            ),
        );
        if filter_server_list.is_empty() {
            self.filter_server_table.remove(&broker_addr_info);
        } else {
            self.filter_server_table
                .insert(broker_addr_info.clone(), filter_server_list);
        }

        if mix_all::MASTER_ID != broker_id as u64 {
            let master_address = broker_data.broker_addrs().get(&(mix_all::MASTER_ID as i64));
            if let Some(master_addr) = master_address {
                let master_livie_info = self
                    .broker_live_table
                    .get(BrokerAddrInfo::new(cluster_name.clone(), master_addr.clone()).as_ref());
                if let Some(info) = master_livie_info {
                    result.ha_server_addr = info.ha_server_addr().to_string();
                    result.master_addr = info.ha_server_addr().to_string();
                }
            }
        }
        if is_min_broker_id_changed && self.namesrv_config.notify_min_broker_id_changed {
            self.notify_min_broker_id_changed(
                broker_data.broker_addrs(),
                None,
                Some(
                    self.broker_live_table
                        .get(&broker_addr_info)
                        .unwrap()
                        .ha_server_addr()
                        .to_string(),
                ),
            )
        }
        Some(result)
    }
}

impl RouteInfoManager {
    pub(crate) fn get_all_cluster_info(&self) -> ClusterInfo {
        ClusterInfo::new(
            Some(self.broker_addr_table.clone()),
            Some(self.cluster_addr_table.clone()),
        )
    }

    pub(crate) fn pickup_topic_route_data(&self, topic: &str) -> Option<TopicRouteData> {
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

        if let Some(queue_data_map) = self.topic_queue_table.get(topic) {
            topic_route_data.queue_datas = queue_data_map.values().cloned().collect();
            found_queue_data = true;

            let broker_name_set: HashSet<&String> = queue_data_map.keys().collect();

            for broker_name in broker_name_set {
                if let Some(broker_data) = self.broker_addr_table.get(broker_name) {
                    let broker_data_clone = broker_data.clone();
                    topic_route_data.broker_datas.push(broker_data_clone);
                    found_broker_data = true;

                    if !self.filter_server_table.is_empty() {
                        for broker_addr in broker_data.broker_addrs().values() {
                            let broker_addr_info =
                                BrokerAddrInfo::new(broker_data.cluster(), broker_addr.clone());
                            if let Some(filter_server_list) =
                                self.filter_server_table.get(&broker_addr_info)
                            {
                                topic_route_data
                                    .filter_server_table
                                    .insert(broker_addr.clone(), filter_server_list.clone());
                            }
                        }
                    }
                }
            }
        }

        debug!("pickup_topic_route_data {:?} {:?}", topic, topic_route_data);

        if found_broker_data && found_queue_data {
            topic_route_data.topic_queue_mapping_by_broker = Some(
                self.topic_queue_mapping_info_table
                    .get(topic)
                    .cloned()
                    .unwrap_or_default(),
            );

            if !self.namesrv_config.support_acting_master {
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
                    && !broker_data
                        .broker_addrs()
                        .contains_key(&(mix_all::MASTER_ID as i64))
            });

            if !need_acting_master {
                return Some(topic_route_data);
            }

            for broker_data in &mut topic_route_data.broker_datas {
                if broker_data.broker_addrs().is_empty()
                    || broker_data
                        .broker_addrs()
                        .contains_key(&(mix_all::MASTER_ID as i64))
                    || !broker_data.enable_acting_master()
                {
                    continue;
                }

                // No master
                for queue_data in &topic_route_data.queue_datas {
                    if queue_data.broker_name() == broker_data.broker_name() {
                        if !PermName::is_writeable(queue_data.perm() as i8) {
                            if let Some(min_broker_id) =
                                broker_data.broker_addrs().keys().cloned().min()
                            {
                                if let Some(acting_master_addr) =
                                    broker_data.broker_addrs_mut().remove(&min_broker_id)
                                {
                                    broker_data
                                        .broker_addrs_mut()
                                        .insert(mix_all::MASTER_ID as i64, acting_master_addr);
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
    fn topic_set_of_broker_name(&mut self, broker_name: &str) -> HashSet<String> {
        let mut topic_of_broker = HashSet::new();
        for (key, value) in self.topic_queue_table.iter() {
            if value.contains_key(broker_name) {
                topic_of_broker.insert(key.to_string());
            }
        }
        topic_of_broker
    }

    pub(crate) fn is_topic_config_changed(
        &mut self,
        cluster_name: &str,
        broker_addr: &str,
        data_version: &DataVersion,
        broker_name: &str,
        topic: &str,
    ) -> bool {
        let is_change =
            self.is_broker_topic_config_changed(cluster_name, broker_addr, data_version);
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
        cluster_name: &str,
        broker_addr: &str,
        data_version: &DataVersion,
    ) -> bool {
        let option = self.query_broker_topic_config(cluster_name, broker_addr);
        if let Some(pre) = option {
            if !(pre == data_version) {
                return true;
            }
        }
        false
    }

    pub(crate) fn query_broker_topic_config(
        &self,
        cluster_name: &str,
        broker_addr: &str,
    ) -> Option<&DataVersion> {
        let info = BrokerAddrInfo::new(cluster_name.to_string(), broker_addr.to_string());
        let pre = self.broker_live_table.get(info.as_ref());
        if let Some(live_info) = pre {
            return Some(live_info.data_version());
        }
        None
    }

    fn create_and_update_queue_data(&mut self, broker_name: &str, topic_config: TopicConfig) {
        let queue_data = QueueData::new(
            broker_name.to_string(),
            topic_config.write_queue_nums,
            topic_config.read_queue_nums,
            topic_config.perm,
            topic_config.topic_sys_flag,
        );

        let queue_data_map = self.topic_queue_table.get_mut(&topic_config.topic_name);
        if let Some(queue_data_map_inner) = queue_data_map {
            let existed_qd = queue_data_map_inner.get(broker_name);
            if existed_qd.is_none() {
                queue_data_map_inner.insert(broker_name.to_string(), queue_data);
            } else {
                let unwrap = existed_qd.unwrap();
                if unwrap != &queue_data {
                    info!(
                        "topic changed, {} OLD: {:?} NEW: {:?}",
                        &topic_config.topic_name, unwrap, queue_data
                    );
                    queue_data_map_inner.insert(broker_name.to_string(), queue_data);
                }
            }
        } else {
            let mut queue_data_map_inner = HashMap::new();
            info!(
                "new topic registered, {} {:?}",
                &topic_config.topic_name, &queue_data
            );
            queue_data_map_inner.insert(broker_name.to_string(), queue_data);
            self.topic_queue_table
                .insert(topic_config.topic_name.clone(), queue_data_map_inner);
        }
    }

    fn notify_min_broker_id_changed(
        &mut self,
        broker_addr_map: &HashMap<i64, String>,
        offline_broker_addr: Option<String>,
        ha_broker_addr: Option<String>,
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

        if let Some(broker_addrs_notify) =
            self.choose_broker_addrs_to_notify(broker_addr_map, offline_broker_addr)
        {
            for broker_addr in broker_addrs_notify {
                let _ = self.remote_client.invoke_oneway(
                    broker_addr,
                    RemotingCommand::create_request_command(
                        RequestCode::NotifyMinBrokerIdChange,
                        request_header.clone(),
                    ),
                    Duration::from_millis(3000),
                );
            }
        }
    }

    fn choose_broker_addrs_to_notify(
        &mut self,
        broker_addr_map: &HashMap<i64, String>,
        offline_broker_addr: Option<String>,
    ) -> Option<Vec<String>> {
        if broker_addr_map.len() == 1 || offline_broker_addr.is_some() {
            return Some(broker_addr_map.values().cloned().collect());
        }
        let min_broker_id = broker_addr_map.keys().min().copied().unwrap();
        let broker_addr_vec = broker_addr_map
            .iter()
            .filter(|(key, _value)| **key != min_broker_id)
            .map(|(_, value)| value.clone())
            .collect();
        Some(broker_addr_vec)
    }

    pub(crate) fn update_broker_info_update_timestamp(
        &mut self,
        cluster_name: impl Into<String>,
        broker_addr: impl Into<String>,
    ) {
        let broker_addr_info = BrokerAddrInfo::new(cluster_name, broker_addr);
        if let Some(value) = self.broker_live_table.get_mut(broker_addr_info.as_ref()) {
            value.last_update_timestamp = TimeUtils::get_current_millis() as i64;
        }
    }

    pub(crate) fn get_broker_member_group(
        &mut self,
        cluster_name: &str,
        broker_name: &str,
    ) -> Option<BrokerMemberGroup> {
        if let Some(broker_data) = self.broker_addr_table.get(broker_name) {
            let map = broker_data.broker_addrs().clone();
            return Some(BrokerMemberGroup::new(
                Some(cluster_name.to_string()),
                Some(broker_name.to_string()),
                Some(map),
            ));
        }
        None
    }

    pub(crate) fn wipe_write_perm_of_broker_by_lock(&mut self, broker_name: &str) -> i32 {
        self.operate_write_perm_of_broker(broker_name, RequestCode::WipeWritePermOfBroker)
    }

    pub(crate) fn add_write_perm_of_broker_by_lock(&mut self, broker_name: &str) -> i32 {
        self.operate_write_perm_of_broker(broker_name, RequestCode::AddWritePermOfBroker)
    }

    fn operate_write_perm_of_broker(
        &mut self,
        broker_name: &str,
        request_code: RequestCode,
    ) -> i32 {
        let mut topic_cnt = 0;
        for (_topic, qd_map) in self.topic_queue_table.iter_mut() {
            let qd = qd_map.get_mut(broker_name).unwrap();
            let mut perm = qd.perm;
            match request_code {
                RequestCode::WipeWritePermOfBroker => {
                    perm &= !PermName::PERM_WRITE as u32;
                }
                RequestCode::AddWritePermOfBroker => {
                    perm = (PermName::PERM_READ | PermName::PERM_WRITE) as u32;
                }
                _ => {}
            }
            qd.perm = perm;
            topic_cnt += 1;
        }
        topic_cnt
    }

    pub(crate) fn get_all_topic_list(&self) -> TopicList {
        let topics = self
            .topic_queue_table
            .keys()
            .cloned()
            .collect::<Vec<String>>();

        TopicList {
            topic_list: topics,
            broker_addr: None,
        }
    }

    pub(crate) fn delete_topic(
        &mut self,
        topic: impl Into<String>,
        cluster_name: Option<impl Into<String>>,
    ) {
        let topic_inner = topic.into();
        if cluster_name.is_some() {
            let cluster_name_inner = cluster_name.map(|s| s.into()).unwrap();
            let broker_names = self.cluster_addr_table.get(cluster_name_inner.as_str());
            if broker_names.is_none() || broker_names.unwrap().is_empty() {
                return;
            }
            if let Some(queue_data_map) = self.topic_queue_table.get_mut(topic_inner.as_str()) {
                for broker_name in broker_names.unwrap() {
                    if let Some(remove_qd) = queue_data_map.remove(broker_name) {
                        info!(
                            "deleteTopic, remove one broker's topic {} {} {:?}",
                            broker_name, &topic_inner, remove_qd
                        )
                    }
                }
            }
        } else {
            self.topic_queue_table.remove(topic_inner.as_str());
        }
    }

    pub(crate) fn register_topic(
        &mut self,
        topic: impl Into<String>,
        queue_data_vec: Vec<QueueData>,
    ) {
        if queue_data_vec.is_empty() {
            return;
        }
        let topic_inner = topic.into();

        if self.topic_queue_table.get(&topic_inner).is_none() {
            self.topic_queue_table
                .insert(topic_inner.clone(), HashMap::new());
        }
        let queue_data_map = self.topic_queue_table.get_mut(&topic_inner).unwrap();
        let vec_length = queue_data_vec.len();
        for queue_data in queue_data_vec {
            if !self
                .broker_addr_table
                .contains_key(queue_data.broker_name())
            {
                warn!(
                    "Register topic contains illegal broker, {}, {:?}",
                    topic_inner, queue_data
                );
                return;
            }
            queue_data_map.insert(queue_data.broker_name().to_string(), queue_data);
        }

        if queue_data_map.len() > vec_length {
            info!(
                "Topic route already exist.{}, {:?}",
                &topic_inner, queue_data_map
            )
        } else {
            info!(
                "Register topic route:{}, {:?}",
                &topic_inner, queue_data_map
            )
        }
    }

    pub(crate) fn get_topics_by_cluster(&self, cluster: &str) -> TopicList {
        let mut topic_list = Vec::new();
        if let Some(broker_name_set) = self.cluster_addr_table.get(cluster) {
            for broker_name in broker_name_set {
                for (topic, queue_data_map) in self.topic_queue_table.iter() {
                    if let Some(_queue_data) = queue_data_map.get(broker_name) {
                        topic_list.push(topic.to_string());
                    }
                }
            }
        }
        TopicList {
            topic_list,
            broker_addr: None,
        }
    }

    pub(crate) fn get_system_topic_list(&self) -> TopicList {
        let mut topic_list = Vec::new();
        let mut broker_addr_out = String::new();
        for (cluster_name, broker_set) in self.cluster_addr_table.iter() {
            topic_list.push(cluster_name.clone());
            broker_set.iter().for_each(|broker_name| {
                topic_list.push(broker_name.clone());
            });
        }
        if !self.broker_addr_table.is_empty() {
            for broker_addr in self.broker_addr_table.values() {
                for ip in broker_addr.broker_addrs().values() {
                    broker_addr_out = ip.clone();
                }
            }
        }
        TopicList {
            topic_list,
            broker_addr: Some(broker_addr_out),
        }
    }
}
