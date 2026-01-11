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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::route::route_data_view::BrokerData;
use crate::protocol::route::route_data_view::QueueData;
use crate::protocol::static_topic::topic_queue_info::TopicQueueMappingInfo;

#[derive(Debug, Serialize, Deserialize, Clone, Default, Eq, PartialEq)]
pub struct TopicRouteData {
    #[serde(rename = "orderTopicConf")]
    pub order_topic_conf: Option<CheetahString>,
    #[serde(rename = "queueDatas")]
    pub queue_datas: Vec<QueueData>,
    #[serde(rename = "brokerDatas")]
    pub broker_datas: Vec<BrokerData>,
    #[serde(rename = "filterServerTable")]
    pub filter_server_table: HashMap<CheetahString, Vec<CheetahString>>,
    #[serde(rename = "topicQueueMappingInfo")]
    pub topic_queue_mapping_by_broker: Option<HashMap<CheetahString, TopicQueueMappingInfo>>,
}

impl TopicRouteData {
    pub fn topic_route_data_changed(&self, old_data: Option<&TopicRouteData>) -> bool {
        if old_data.is_none() {
            return true;
        }
        let mut now = TopicRouteData::from_existing(self);
        let mut old = TopicRouteData::from_existing(old_data.unwrap());
        now.queue_datas.sort();
        now.broker_datas.sort();
        old.queue_datas.sort();
        old.broker_datas.sort();
        now != old
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

    pub fn from_existing(topic_route_data: &TopicRouteData) -> Self {
        TopicRouteData {
            order_topic_conf: topic_route_data.order_topic_conf.clone(),
            queue_datas: topic_route_data.queue_datas.clone(),
            broker_datas: topic_route_data.broker_datas.clone(),
            filter_server_table: topic_route_data.filter_server_table.clone(),
            topic_queue_mapping_by_broker: topic_route_data.topic_queue_mapping_by_broker.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn topic_route_data_default_values() {
        let topic_route_data = TopicRouteData::default();
        assert!(topic_route_data.order_topic_conf.is_none());
        assert!(topic_route_data.queue_datas.is_empty());
        assert!(topic_route_data.broker_datas.is_empty());
        assert!(topic_route_data.filter_server_table.is_empty());
        assert!(topic_route_data.topic_queue_mapping_by_broker.is_none());
    }

    #[test]
    fn topic_route_data_with_values() {
        let mut filter_server_table = HashMap::new();
        filter_server_table.insert(CheetahString::from("key"), vec![CheetahString::from("value")]);
        let mut topic_queue_mapping_by_broker = HashMap::new();
        topic_queue_mapping_by_broker.insert(CheetahString::from("broker"), TopicQueueMappingInfo::default());
        let topic_route_data = TopicRouteData {
            order_topic_conf: Some(CheetahString::from("conf")),
            queue_datas: vec![QueueData::default()],
            broker_datas: vec![BrokerData::default()],
            filter_server_table,
            topic_queue_mapping_by_broker: Some(topic_queue_mapping_by_broker),
        };
        assert_eq!(topic_route_data.order_topic_conf, Some(CheetahString::from("conf")));
        assert_eq!(topic_route_data.queue_datas.len(), 1);
        assert_eq!(topic_route_data.broker_datas.len(), 1);
        assert_eq!(topic_route_data.filter_server_table.len(), 1);
        assert!(topic_route_data.topic_queue_mapping_by_broker.is_some());
    }

    #[test]
    fn serialize_topic_route_data() {
        let mut filter_server_table = HashMap::new();
        filter_server_table.insert(CheetahString::from("key"), vec![CheetahString::from("value")]);
        let mut topic_queue_mapping_by_broker = HashMap::new();
        topic_queue_mapping_by_broker.insert(CheetahString::from("broker"), TopicQueueMappingInfo::default());
        let topic_route_data = TopicRouteData {
            order_topic_conf: Some(CheetahString::from("conf")),
            queue_datas: vec![QueueData::default()],
            broker_datas: vec![BrokerData::default()],
            filter_server_table,
            topic_queue_mapping_by_broker: Some(topic_queue_mapping_by_broker),
        };
        let serialized = serde_json::to_string(&topic_route_data).unwrap();
        assert!(serialized.contains("\"orderTopicConf\":\"conf\""));
        assert!(serialized.contains("\"queueDatas\":["));
        assert!(serialized.contains("\"brokerDatas\":["));
        assert!(serialized.contains("\"filterServerTable\":{\"key\":[\"value\"]}"));
        assert!(serialized.contains("\"topicQueueMappingInfo\":{\"broker\":{"));
    }

    #[test]
    fn topic_route_data_changed_with_none_old_data() {
        let topic_route_data = TopicRouteData::default();
        assert!(topic_route_data.topic_route_data_changed(None));
    }

    #[test]
    fn topic_route_data_changed_with_different_data() {
        let topic_route_data = TopicRouteData::default();
        let old_data = TopicRouteData {
            order_topic_conf: Some(CheetahString::from("conf")),
            ..Default::default()
        };
        assert!(topic_route_data.topic_route_data_changed(Some(&old_data)));
    }

    #[test]
    fn topic_route_data_changed_with_same_data() {
        let topic_route_data = TopicRouteData::default();
        let old_data = TopicRouteData::default();
        assert!(!topic_route_data.topic_route_data_changed(Some(&old_data)));
    }
}
