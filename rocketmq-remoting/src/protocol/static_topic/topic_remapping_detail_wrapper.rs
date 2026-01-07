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
