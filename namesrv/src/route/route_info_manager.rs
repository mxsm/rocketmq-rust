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
    time::SystemTime,
};

use rocketmq_common::common::mix_all;
use rocketmq_remoting::protocol::{
    body::{
        broker_body::cluster_info::ClusterInfo,
        topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper,
    },
    namesrv::RegisterBrokerResult,
    route::route_data_view::{BrokerData, QueueData},
    static_topic::topic_queue_info::TopicQueueMappingInfo,
    DataVersion,
};

use crate::route_info::broker_addr_info::{BrokerAddrInfo, BrokerLiveInfo};

const DEFAULT_BROKER_CHANNEL_EXPIRED_TIME: i64 = 1000 * 60 * 2;

#[derive(Debug, Clone)]
pub(crate) struct RouteInfoManager {
    topic_queue_table: HashMap<String /* topic */, HashMap<String, QueueData>>,
    broker_addr_table: HashMap<String /* brokerName */, BrokerData>,
    cluster_addr_table: HashMap<String /* clusterName */, HashSet<String /* brokerName */>>,
    broker_live_table: HashMap<BrokerAddrInfo /* brokerAddr */, BrokerLiveInfo>,
    filter_server_table:
        HashMap<BrokerAddrInfo /* brokerAddr */, Vec<String> /* Filter Server */>,
    topic_queue_mapping_info_table:
        HashMap<String /* topic */, HashMap<String /* brokerName */, TopicQueueMappingInfo>>,
}

impl RouteInfoManager {
    pub fn new() -> Self {
        RouteInfoManager {
            topic_queue_table: HashMap::new(),
            broker_addr_table: HashMap::new(),
            cluster_addr_table: HashMap::new(),
            broker_live_table: HashMap::new(),
            filter_server_table: HashMap::new(),
            topic_queue_mapping_info_table: HashMap::new(),
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
        timeout_millis: Option<i64>,
        enable_acting_master: Option<bool>,
        topic_config_serialize_wrapper: TopicConfigAndMappingSerializeWrapper,
        filter_server_list: Vec<String>,
    ) -> Option<RegisterBrokerResult> {
        let mut result = RegisterBrokerResult::default();
        //update cluster broker addr table
        if !self.cluster_addr_table.contains_key(&cluster_name) {
            self.cluster_addr_table
                .insert(cluster_name.to_string(), HashSet::new());
        }
        self.cluster_addr_table
            .get_mut(&cluster_name)
            .unwrap()
            .insert(broker_name.clone());

        let is_old_version_broker = if let Some(value) = enable_acting_master {
            value
        } else {
            false
        };
        let mut register_first =
            if let Some(broker_data) = self.broker_addr_table.get_mut(&broker_name) {
                broker_data.set_enable_acting_master(is_old_version_broker);
                broker_data.set_zone_name(zone_name.clone());
                false
            } else {
                self.broker_addr_table.insert(
                    broker_name.clone(),
                    BrokerData::new(
                        cluster_name.clone(),
                        broker_name.clone(),
                        HashMap::new(),
                        zone_name,
                    ),
                );
                true
            };
        let broker_data_inner = self.broker_addr_table.get_mut(&broker_name).unwrap();
        let mut is_min_broker_id_changed = false;
        let mut prev_min_broker_id = 0i64;
        if !broker_data_inner.broker_addrs().is_empty() {
            prev_min_broker_id = *broker_data_inner.broker_addrs().keys().min().unwrap();
        }
        if broker_id < prev_min_broker_id {
            is_min_broker_id_changed = true;
        }

        broker_data_inner.remove_broker_by_addr(broker_id, &broker_addr);
        let old_broker_addr = broker_data_inner.broker_addrs().get(&broker_id);

        if let Some(old_broker_addr) = old_broker_addr {
            if old_broker_addr != &broker_addr {
                let addr_info =
                    BrokerAddrInfo::new(cluster_name.clone(), old_broker_addr.to_string());
                let old_broker_info = self.broker_live_table.get(&addr_info);
                if let Some(val) = old_broker_info {
                    let old_state_version = val.data_version().state_version();
                    let new_state_version = topic_config_serialize_wrapper
                        .data_version()
                        .as_ref()
                        .unwrap()
                        .state_version();
                    if old_state_version > new_state_version {
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
        if !broker_data_inner.broker_addrs().contains_key(&broker_id) && size == 1 {
            return None;
        }

        let old_addr = broker_data_inner
            .broker_addrs()
            .insert(broker_id, broker_addr.clone());

        register_first = register_first | old_addr.is_none();
        let is_master = mix_all::MASTER_ID == broker_id as u64;

        let is_prime_slave = !is_old_version_broker
            && !is_master
            && broker_id == *broker_data_inner.broker_addrs().keys().min().unwrap();
        if is_master || is_prime_slave {
            if let Some(v) = topic_config_serialize_wrapper.topic_config_table() {}
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
        }

        Some(result)
    }
}

impl RouteInfoManager {
    pub(crate) fn update_broker_info_update_timestamp(
        &mut self,
        cluster_name: String,
        broker_addr: String,
    ) {
    }

    pub(crate) fn get_all_cluster_info(&mut self) -> ClusterInfo {
        ClusterInfo::new(
            Some(self.broker_addr_table.clone()),
            Some(self.cluster_addr_table.clone()),
        )
    }
}
