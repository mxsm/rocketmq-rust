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

use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::static_topic::topic_queue_info::TopicQueueMappingInfo;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct BrokerData {
    cluster: String,
    #[serde(rename = "brokerName")]
    broker_name: String,
    #[serde(rename = "brokerAddrs")]
    broker_addrs: HashMap<i64 /* broker id */, String /* broker ip */>,
    #[serde(rename = "zoneName")]
    zone_name: Option<String>,
    #[serde(rename = "enableActingMaster")]
    enable_acting_master: bool,
}

impl BrokerData {
    pub fn new(
        cluster: String,
        broker_name: String,
        broker_addrs: HashMap<i64, String>,
        zone_name: Option<String>,
    ) -> BrokerData {
        BrokerData {
            cluster,
            broker_name,
            broker_addrs,
            zone_name,
            enable_acting_master: false,
        }
    }

    pub fn set_cluster(&mut self, cluster: String) {
        self.cluster = cluster;
    }

    pub fn set_broker_name(&mut self, broker_name: String) {
        self.broker_name = broker_name;
    }

    pub fn set_broker_addrs(&mut self, broker_addrs: HashMap<i64, String>) {
        self.broker_addrs = broker_addrs;
    }

    pub fn set_zone_name(&mut self, zone_name: Option<String>) {
        self.zone_name = zone_name;
    }

    pub fn set_enable_acting_master(&mut self, enable_acting_master: bool) {
        self.enable_acting_master = enable_acting_master;
    }

    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    pub fn broker_name_mut(&mut self) -> &str {
        &self.broker_name
    }

    pub fn broker_addrs(&self) -> &HashMap<i64, String> {
        &self.broker_addrs
    }

    pub fn broker_addrs_mut(&mut self) -> &mut HashMap<i64, String> {
        &mut self.broker_addrs
    }

    pub fn remove_broker_by_addr(&mut self, broker_id: i64, broker_addr: &str) {
        self.broker_addrs
            .retain(|key, value| value != broker_addr || *key == broker_id);
    }

    pub fn zone_name(&self) -> &Option<String> {
        &self.zone_name
    }

    pub fn enable_acting_master(&self) -> bool {
        self.enable_acting_master
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct QueueData {
    #[serde(rename = "brokerName")]
    broker_name: String,
    #[serde(rename = "readQueueNums")]
    read_queue_nums: u32,
    #[serde(rename = "writeQueueNums")]
    write_queue_nums: u32,
    pub perm: u32,
    #[serde(rename = "topicSysFlag")]
    topic_sys_flag: u32,
}

impl QueueData {
    pub fn new(
        broker_name: String,
        read_queue_nums: u32,
        write_queue_nums: u32,
        perm: u32,
        topic_sys_flag: u32,
    ) -> Self {
        Self {
            broker_name,
            read_queue_nums,
            write_queue_nums,
            perm,
            topic_sys_flag,
        }
    }

    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    pub fn read_queue_nums(&self) -> u32 {
        self.read_queue_nums
    }

    pub fn write_queue_nums(&self) -> u32 {
        self.write_queue_nums
    }

    pub fn perm(&self) -> u32 {
        self.perm
    }

    pub fn topic_sys_flag(&self) -> u32 {
        self.topic_sys_flag
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, Eq, PartialEq)]
pub struct TopicRouteData {
    #[serde(rename = "orderTopicConf")]
    pub order_topic_conf: Option<String>,
    #[serde(rename = "queueDatas")]
    pub queue_datas: Vec<QueueData>,
    #[serde(rename = "brokerDatas")]
    pub broker_datas: Vec<BrokerData>,
    #[serde(rename = "filterServerTable")]
    pub filter_server_table: HashMap<String, Vec<String>>,
    #[serde(rename = "TopicQueueMappingInfo")]
    pub topic_queue_mapping_by_broker: Option<HashMap<String, TopicQueueMappingInfo>>,
}

impl TopicRouteData {
    pub fn topic_route_data_changed(&self, old_data: Option<&TopicRouteData>) -> bool {
        if old_data.is_none() {
            return true;
        }
        /*let mut now = TopicRouteData::from_existing(self);
        let mut old = TopicRouteData::from_existing(old_data.unwrap());*/
        /*now.queue_datas.sort_by(|a, b| a.cmp(&b));
        now.broker_datas.sort_by(|a, b| a.cmp(&b));
        old.queue_datas.sort_by(|a, b| a.cmp(&b));
        old.broker_datas.sort_by(|a, b| a.cmp(&b));*/
        //now != old
        false
    }

    pub fn new() -> Self {
        TopicRouteData {
            order_topic_conf: None,
            queue_datas: Vec::new(),
            broker_datas: Vec::new(),
            filter_server_table: HashMap::new(),
            topic_queue_mapping_by_broker: None,
        }
    }

    fn from_existing(topic_route_data: &TopicRouteData) -> Self {
        TopicRouteData {
            order_topic_conf: topic_route_data.order_topic_conf.clone(),
            queue_datas: topic_route_data.queue_datas.clone(),
            broker_datas: topic_route_data.broker_datas.clone(),
            filter_server_table: topic_route_data.filter_server_table.clone(),
            topic_queue_mapping_by_broker: topic_route_data.topic_queue_mapping_by_broker.clone(),
        }
    }
}
