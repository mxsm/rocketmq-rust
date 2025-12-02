use std::collections::HashMap;
use std::collections::HashSet;

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::static_topic::topic_config_and_queue_mapping::TopicConfigAndQueueMapping;

pub const TYPE_CREATE_OR_UPDATE: &str = "CREATE_OR_UPDATE";
pub const TYPE_REMAPPING: &str = "REMAPPING";

pub const SUFFIX_BEFORE: &str = ".before";
pub const SUFFIX_AFTER: &str = ".after";
#[derive(Debug, Serialize, Deserialize)]
pub struct TopicRemappingDetailWrapper {
    topic: String,
    method: String,
    epoch: u64,
    broker_config_map: HashMap<String, TopicConfigAndQueueMapping>,
    broker_to_map_in: HashSet<String>,
    broker_to_map_out: HashSet<String>,
}
impl TopicRemappingDetailWrapper {
    pub fn empty() -> Self {
        Self {
            topic: String::new(),
            method: String::new(),
            epoch: 0,
            broker_config_map: HashMap::new(),
            broker_to_map_in: HashSet::new(),
            broker_to_map_out: HashSet::new(),
        }
    }

    pub fn new(
        topic: String,
        method: String,
        epoch: u64,
        broker_config_map: HashMap<String, TopicConfigAndQueueMapping>,
        broker_to_map_in: HashSet<String>,
        broker_to_map_out: HashSet<String>,
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

    pub fn broker_config_map(&self) -> &HashMap<String, TopicConfigAndQueueMapping> {
        &self.broker_config_map
    }

    pub fn broker_to_map_in(&self) -> &HashSet<String> {
        &self.broker_to_map_in
    }

    pub fn broker_to_map_out(&self) -> &HashSet<String> {
        &self.broker_to_map_out
    }

    pub fn set_broker_config_map(
        &mut self,
        broker_config_map: HashMap<String, TopicConfigAndQueueMapping>,
    ) {
        self.broker_config_map = broker_config_map;
    }

    pub fn set_broker_to_map_in(&mut self, broker_to_map_in: HashSet<String>) {
        self.broker_to_map_in = broker_to_map_in;
    }

    pub fn set_broker_to_map_out(&mut self, broker_to_map_out: HashSet<String>) {
        self.broker_to_map_out = broker_to_map_out;
    }

    pub fn set_topic(&mut self, topic: String) {
        self.topic = topic;
    }

    pub fn set_method(&mut self, method: String) {
        self.method = method;
    }

    pub fn set_epoch(&mut self, epoch: u64) {
        self.epoch = epoch;
    }
}
