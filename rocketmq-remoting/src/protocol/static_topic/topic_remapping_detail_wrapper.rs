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

use std::collections::HashMap;
use std::collections::HashSet;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::static_topic::topic_config_and_queue_mapping::TopicConfigAndQueueMapping;

pub const TYPE_CREATE_OR_UPDATE: &str = "CREATE_OR_UPDATE";
pub const TYPE_REMAPPING: &str = "REMAPPING";

pub const SUFFIX_BEFORE: &str = ".before";
pub const SUFFIX_AFTER: &str = ".after";

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicRemappingDetailWrapper {
    topic: CheetahString,
    method: CheetahString,
    epoch: u64,
    broker_config_map: HashMap<CheetahString, TopicConfigAndQueueMapping>,
    broker_to_map_in: HashSet<CheetahString>,
    broker_to_map_out: HashSet<CheetahString>,
}

impl TopicRemappingDetailWrapper {
    pub fn empty() -> Self {
        Self {
            topic: CheetahString::new(),
            method: CheetahString::new(),
            epoch: 0,
            broker_config_map: HashMap::new(),
            broker_to_map_in: HashSet::new(),
            broker_to_map_out: HashSet::new(),
        }
    }

    pub fn new(
        topic: CheetahString,
        method: CheetahString,
        epoch: u64,
        broker_config_map: HashMap<CheetahString, TopicConfigAndQueueMapping>,
        broker_to_map_in: HashSet<CheetahString>,
        broker_to_map_out: HashSet<CheetahString>,
    ) -> Self {
        Self {
            topic,
            method,
            epoch,
            broker_config_map,
            broker_to_map_in,
            broker_to_map_out,
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn method(&self) -> &str {
        &self.method
    }

    pub fn get_epoch(&self) -> u64 {
        self.epoch
    }

    pub fn broker_config_map(&self) -> &HashMap<CheetahString, TopicConfigAndQueueMapping> {
        &self.broker_config_map
    }

    pub fn broker_config_map_mut(&mut self) -> &mut HashMap<CheetahString, TopicConfigAndQueueMapping> {
        &mut self.broker_config_map
    }

    pub fn broker_to_map_in(&self) -> &HashSet<CheetahString> {
        &self.broker_to_map_in
    }

    pub fn broker_to_map_out(&self) -> &HashSet<CheetahString> {
        &self.broker_to_map_out
    }

    pub fn set_broker_config_map(&mut self, broker_config_map: HashMap<CheetahString, TopicConfigAndQueueMapping>) {
        self.broker_config_map = broker_config_map;
    }

    pub fn set_broker_to_map_in(&mut self, broker_to_map_in: HashSet<CheetahString>) {
        self.broker_to_map_in = broker_to_map_in;
    }

    pub fn set_broker_to_map_out(&mut self, broker_to_map_out: HashSet<CheetahString>) {
        self.broker_to_map_out = broker_to_map_out;
    }

    pub fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
    }

    pub fn set_method(&mut self, method: CheetahString) {
        self.method = method;
    }

    pub fn set_epoch(&mut self, epoch: u64) {
        self.epoch = epoch;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_remapping_detail_wrapper_empty() {
        let wrapper = TopicRemappingDetailWrapper::empty();
        assert!(wrapper.topic().is_empty());
        assert!(wrapper.method().is_empty());
        assert_eq!(wrapper.get_epoch(), 0);
        assert!(wrapper.broker_config_map().is_empty());
        assert!(wrapper.broker_to_map_in().is_empty());
        assert!(wrapper.broker_to_map_out().is_empty());
    }

    #[test]
    fn topic_remapping_detail_wrapper_new() {
        let topic = CheetahString::from("test_topic");
        let method = CheetahString::from(TYPE_REMAPPING);
        let epoch = 100u64;
        let mut broker_config_map = HashMap::new();
        broker_config_map.insert(CheetahString::from("broker_a"), TopicConfigAndQueueMapping::default());
        let mut broker_to_map_in = HashSet::new();
        broker_to_map_in.insert(CheetahString::from("broker_b"));
        let mut broker_to_map_out = HashSet::new();
        broker_to_map_out.insert(CheetahString::from("broker_c"));

        let wrapper = TopicRemappingDetailWrapper::new(
            topic.clone(),
            method.clone(),
            epoch,
            broker_config_map.clone(),
            broker_to_map_in.clone(),
            broker_to_map_out.clone(),
        );

        assert_eq!(wrapper.topic(), topic.as_str());
        assert_eq!(wrapper.method(), method.as_str());
        assert_eq!(wrapper.get_epoch(), epoch);
        assert_eq!(wrapper.broker_config_map(), &broker_config_map);
        assert_eq!(wrapper.broker_to_map_in(), &broker_to_map_in);
        assert_eq!(wrapper.broker_to_map_out(), &broker_to_map_out);
    }

    #[test]
    fn topic_remapping_detail_wrapper_setters_and_getters() {
        let mut wrapper = TopicRemappingDetailWrapper::empty();

        wrapper.set_topic(CheetahString::from("new_topic"));
        assert_eq!(wrapper.topic(), "new_topic");

        wrapper.set_method(CheetahString::from(TYPE_CREATE_OR_UPDATE));
        assert_eq!(wrapper.method(), TYPE_CREATE_OR_UPDATE);

        wrapper.set_epoch(500);
        assert_eq!(wrapper.get_epoch(), 500);

        let mut config_map = HashMap::new();
        config_map.insert(CheetahString::from("b1"), TopicConfigAndQueueMapping::default());
        wrapper.set_broker_config_map(config_map.clone());
        assert_eq!(wrapper.broker_config_map(), &config_map);

        let mut map_in = HashSet::new();
        map_in.insert(CheetahString::from("b2"));
        wrapper.set_broker_to_map_in(map_in.clone());
        assert_eq!(wrapper.broker_to_map_in(), &map_in);

        let mut map_out = HashSet::new();
        map_out.insert(CheetahString::from("b3"));
        wrapper.set_broker_to_map_out(map_out.clone());
        assert_eq!(wrapper.broker_to_map_out(), &map_out);

        wrapper
            .broker_config_map_mut()
            .insert(CheetahString::from("b4"), TopicConfigAndQueueMapping::default());
        assert!(wrapper.broker_config_map().contains_key(&CheetahString::from("b4")));
    }
}
