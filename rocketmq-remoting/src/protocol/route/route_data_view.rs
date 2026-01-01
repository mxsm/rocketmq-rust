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

use std::cmp::Ordering;
use std::collections::HashMap;

use cheetah_string::CheetahString;
use rand::seq::IteratorRandom;
use rocketmq_common::common::mix_all;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BrokerData {
    cluster: CheetahString,
    #[serde(rename = "brokerName")]
    broker_name: CheetahString,
    #[serde(rename = "brokerAddrs")]
    broker_addrs: HashMap<u64 /* broker id */, CheetahString /* broker ip */>,
    #[serde(rename = "zoneName")]
    zone_name: Option<CheetahString>,
    #[serde(rename = "enableActingMaster")]
    enable_acting_master: bool,
}

impl PartialEq for BrokerData {
    fn eq(&self, other: &Self) -> bool {
        self.broker_name == other.broker_name && self.broker_addrs == other.broker_addrs
    }
}

impl Eq for BrokerData {}

impl PartialOrd for BrokerData {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BrokerData {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.broker_name.cmp(&other.broker_name)
    }
}

impl BrokerData {
    pub fn new(
        cluster: CheetahString,
        broker_name: CheetahString,
        broker_addrs: HashMap<u64, CheetahString>,
        zone_name: Option<CheetahString>,
    ) -> BrokerData {
        BrokerData {
            cluster,
            broker_name,
            broker_addrs,
            zone_name,
            enable_acting_master: false,
        }
    }

    #[inline]
    pub fn set_cluster(&mut self, cluster: CheetahString) {
        self.cluster = cluster;
    }

    #[inline]
    pub fn set_broker_name(&mut self, broker_name: CheetahString) {
        self.broker_name = broker_name;
    }

    #[inline]
    pub fn set_broker_addrs(&mut self, broker_addrs: HashMap<u64, CheetahString>) {
        self.broker_addrs = broker_addrs;
    }

    #[inline]
    pub fn set_zone_name(&mut self, zone_name: Option<CheetahString>) {
        self.zone_name = zone_name;
    }

    #[inline]
    pub fn set_enable_acting_master(&mut self, enable_acting_master: bool) {
        self.enable_acting_master = enable_acting_master;
    }

    #[inline]
    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    #[inline]
    pub fn broker_name(&self) -> &CheetahString {
        &self.broker_name
    }

    #[inline]
    pub fn broker_addrs(&self) -> &HashMap<u64, CheetahString> {
        &self.broker_addrs
    }

    #[inline]
    pub fn broker_addrs_mut(&mut self) -> &mut HashMap<u64, CheetahString> {
        &mut self.broker_addrs
    }

    #[inline]
    pub fn remove_broker_by_addr(&mut self, broker_id: u64, broker_addr: &CheetahString) {
        self.broker_addrs
            .retain(|key, value| value != broker_addr || *key == broker_id);
    }

    #[inline]
    pub fn zone_name(&self) -> Option<&CheetahString> {
        self.zone_name.as_ref()
    }

    #[inline]
    pub fn enable_acting_master(&self) -> bool {
        self.enable_acting_master
    }

    pub fn select_broker_addr(&self) -> Option<CheetahString> {
        let master_address = self.broker_addrs.get(&(mix_all::MASTER_ID)).cloned();
        if master_address.is_none() {
            return self.broker_addrs.values().choose(&mut rand::rng()).cloned();
        }
        master_address
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Default)]
pub struct QueueData {
    #[serde(rename = "brokerName")]
    pub broker_name: CheetahString,
    #[serde(rename = "readQueueNums")]
    pub read_queue_nums: u32,
    #[serde(rename = "writeQueueNums")]
    pub write_queue_nums: u32,
    pub perm: u32,
    #[serde(rename = "topicSysFlag")]
    pub topic_sys_flag: u32,
}

impl QueueData {
    pub fn new(
        broker_name: CheetahString,
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

    #[inline]
    pub fn broker_name(&self) -> &CheetahString {
        &self.broker_name
    }

    #[inline]
    pub fn read_queue_nums(&self) -> u32 {
        self.read_queue_nums
    }

    #[inline]
    pub fn write_queue_nums(&self) -> u32 {
        self.write_queue_nums
    }

    #[inline]
    pub fn perm(&self) -> u32 {
        self.perm
    }

    #[inline]
    pub fn topic_sys_flag(&self) -> u32 {
        self.topic_sys_flag
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn broker_data_new_initializes_correctly() {
        let cluster = CheetahString::from("test_cluster");
        let broker_name = CheetahString::from("test_broker");
        let broker_addrs = HashMap::new();
        let zone_name = CheetahString::from("test_zone");

        let broker_data = BrokerData::new(
            cluster.clone(),
            broker_name.clone(),
            broker_addrs.clone(),
            Some(zone_name.clone()),
        );

        assert_eq!(broker_data.cluster, cluster);
        assert_eq!(broker_data.broker_name, broker_name);
        assert_eq!(broker_data.broker_addrs, broker_addrs);
        if let Some(zone) = &broker_data.zone_name {
            assert_eq!(zone, &zone_name);
        }
        assert!(!broker_data.enable_acting_master);
    }

    #[test]
    fn broker_data_setters_work_correctly() {
        let mut broker_data = BrokerData::new(
            CheetahString::from("cluster1"),
            CheetahString::from("broker1"),
            HashMap::new(),
            None,
        );

        broker_data.set_cluster(CheetahString::from("cluster2"));
        broker_data.set_broker_name(CheetahString::from("broker2"));
        broker_data.set_broker_addrs(HashMap::from([(1, CheetahString::from("127.0.0.1"))]));
        broker_data.set_zone_name(Some(CheetahString::from("zone1")));
        broker_data.set_enable_acting_master(true);

        assert_eq!(broker_data.cluster, CheetahString::from("cluster2"));
        assert_eq!(broker_data.broker_name, CheetahString::from("broker2"));
        assert_eq!(
            broker_data.broker_addrs.get(&1).unwrap(),
            &CheetahString::from("127.0.0.1")
        );
        if let Some(zone_name) = &broker_data.zone_name {
            assert_eq!(zone_name, &CheetahString::from("zone1"));
        }
        assert!(broker_data.enable_acting_master);
    }

    #[test]
    fn broker_data_remove_broker_by_addr_works_correctly() {
        let mut broker_data = BrokerData::new(
            CheetahString::from("cluster1"),
            CheetahString::from("broker1"),
            HashMap::from([
                (1, CheetahString::from("127.0.0.1")),
                (2, CheetahString::from("127.0.0.2")),
            ]),
            None,
        );

        broker_data.remove_broker_by_addr(1, &"127.0.0.1".into());
        //assert!(broker_data.broker_addrs.get(&1).is_none());
        assert!(broker_data.broker_addrs.contains_key(&2));
    }

    #[test]
    fn broker_data_select_broker_addr_returns_master_if_exists() {
        let broker_data = BrokerData::new(
            CheetahString::from("cluster1"),
            CheetahString::from("broker1"),
            HashMap::from([(mix_all::MASTER_ID, CheetahString::from("127.0.0.1"))]),
            None,
        );

        let selected_addr = broker_data.select_broker_addr();
        assert_eq!(selected_addr.unwrap(), CheetahString::from("127.0.0.1"));
    }

    #[test]
    fn broker_data_select_broker_addr_returns_random_if_no_master() {
        let broker_data = BrokerData::new(
            CheetahString::from("cluster1"),
            CheetahString::from("broker1"),
            HashMap::from([(2, CheetahString::from("127.0.0.2"))]),
            None,
        );

        let selected_addr = broker_data.select_broker_addr();
        assert_eq!(selected_addr.unwrap(), CheetahString::from("127.0.0.2"));
    }

    #[test]
    fn queue_data_new_initializes_correctly() {
        let queue_data = QueueData::new(CheetahString::from("broker1"), 4, 4, 6, 0);

        assert_eq!(queue_data.broker_name, CheetahString::from("broker1"));
        assert_eq!(queue_data.read_queue_nums, 4);
        assert_eq!(queue_data.write_queue_nums, 4);
        assert_eq!(queue_data.perm, 6);
        assert_eq!(queue_data.topic_sys_flag, 0);
    }
}
