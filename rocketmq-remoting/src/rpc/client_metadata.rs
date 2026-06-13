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
use std::sync::Arc;

use cheetah_string::CheetahString;
use parking_lot::RwLock;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::MASTER_ID;
use tracing::warn;

use crate::protocol::body::broker_body::cluster_info::ClusterInfo;
use crate::protocol::route::topic_route_data::TopicRouteData;
use crate::protocol::static_topic::topic_queue_mapping_info::TopicQueueMappingInfo;
use crate::protocol::static_topic::topic_queue_mapping_utils::TopicQueueMappingUtils;

pub struct ClientMetadata {
    topic_route_table: Arc<RwLock<HashMap<CheetahString /* Topic */, TopicRouteData>>>,
    topic_end_points_table:
        Arc<RwLock<HashMap<CheetahString /* Topic */, HashMap<MessageQueue, CheetahString /* brokerName */>>>>,
    broker_addr_table:
        Arc<RwLock<HashMap<CheetahString /* Broker Name */, HashMap<u64 /* brokerId */, CheetahString /* address */>>>>,
    broker_version_table:
        Arc<RwLock<HashMap<CheetahString /* Broker Name */, HashMap<CheetahString /* address */, i32>>>>,
}

impl Default for ClientMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientMetadata {
    pub fn new() -> Self {
        Self {
            topic_route_table: Arc::new(RwLock::new(HashMap::new())),
            topic_end_points_table: Arc::new(RwLock::new(HashMap::new())),
            broker_addr_table: Arc::new(RwLock::new(HashMap::new())),
            broker_version_table: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn fresh_topic_route(&self, topic: &CheetahString, topic_route_data: Option<TopicRouteData>) {
        if topic.is_empty() {
            return;
        }
        let Some(topic_route_data) = topic_route_data else {
            return;
        };
        let read_guard = self.topic_route_table.read();
        let old = read_guard.get(topic);
        if !topic_route_data.topic_route_data_changed(old) {
            return;
        }
        drop(read_guard);
        {
            let mut write_guard = self.broker_addr_table.write();
            for bd in topic_route_data.broker_datas.iter() {
                write_guard.insert(bd.broker_name().clone(), bd.broker_addrs().clone());
            }
        }

        {
            let mq_end_points = ClientMetadata::topic_route_data2endpoints_for_static_topic(topic, &topic_route_data);
            if let Some(mq_end_points) = mq_end_points.filter(|endpoints| !endpoints.is_empty()) {
                let mut write_guard = self.topic_end_points_table.write();
                write_guard.insert(topic.clone(), mq_end_points);
            }
        }
    }

    pub fn get_broker_name_from_message_queue(&self, mq: &MessageQueue) -> Option<CheetahString> {
        let read_guard = self.topic_end_points_table.read();
        match read_guard.get(mq.topic_str()).filter(|endpoints| !endpoints.is_empty()) {
            Some(topic_end_points) => topic_end_points.get(mq).cloned(),
            None => Some(mq.broker_name().clone()),
        }
    }

    pub fn refresh_cluster_info(&self, cluster_info: Option<&ClusterInfo>) {
        let Some(cluster_info) = cluster_info else {
            return;
        };
        let Some(broker_addr_table) = cluster_info.broker_addr_table.as_ref() else {
            return;
        };

        let mut write_guard = self.broker_addr_table.write();
        for (broker_name, broker_data) in broker_addr_table {
            let broker_addr = broker_data.broker_addrs();
            write_guard.insert(broker_name.clone(), broker_addr.clone());
        }
    }

    pub fn find_master_broker_addr(&self, broker_name: &str) -> Option<CheetahString> {
        let read_guard = self.broker_addr_table.read();
        read_guard
            .get(broker_name)
            .and_then(|broker_addrs| broker_addrs.get(&(MASTER_ID)).cloned())
    }

    pub fn topic_route_data2endpoints_for_static_topic(
        topic: &str,
        topic_route_data: &TopicRouteData,
    ) -> Option<HashMap<MessageQueue, CheetahString>> {
        let Some(topic_queue_mapping_by_broker) = topic_route_data.topic_queue_mapping_by_broker.as_ref() else {
            return Some(HashMap::new());
        };
        if topic_queue_mapping_by_broker.is_empty() {
            return Some(HashMap::new());
        }

        let mut mq_end_points_of_broker = HashMap::new();
        let mut mapping_infos_by_scope = HashMap::new();
        for (broker_name, info) in topic_queue_mapping_by_broker.iter() {
            if let Some(scope_inner) = info.scope.as_ref() {
                mapping_infos_by_scope
                    .entry(scope_inner.to_string())
                    .or_insert_with(HashMap::new)
                    .insert(broker_name.to_string(), info.clone());
            }
        }

        for (scope, topic_queue_mapping_info_map) in mapping_infos_by_scope {
            let mut mq_endpoints: HashMap<MessageQueue, TopicQueueMappingInfo> = HashMap::new();
            let mut mapping_infos: Vec<_> = topic_queue_mapping_info_map.iter().collect();
            mapping_infos.sort_by_key(|b| std::cmp::Reverse(b.1.epoch));

            let mut max_total_nums = 0;
            let max_total_num_of_epoch = -1;

            for (_, info) in mapping_infos {
                if info.epoch >= max_total_num_of_epoch && info.total_queues > max_total_nums {
                    max_total_nums = info.total_queues;
                }
                let Some(curr_id_map) = info.curr_id_map.as_ref() else {
                    warn!(
                        topic,
                        broker_name = %info.bname.as_deref().unwrap_or("<missing>"),
                        scope = %info.scope.as_deref().unwrap_or("<missing>"),
                        "skip static topic mapping info without currIdMap"
                    );
                    continue;
                };
                let Some(scope) = info.scope.as_ref() else {
                    continue;
                };
                for global_id in curr_id_map.keys() {
                    let mq = MessageQueue::from_parts(
                        topic,
                        TopicQueueMappingUtils::get_mock_broker_name(scope.as_str()),
                        *global_id,
                    );
                    if let Some(old_info) = mq_endpoints.get(&mq) {
                        if old_info.epoch <= info.epoch {
                            mq_endpoints.insert(mq, info.clone());
                        }
                    } else {
                        mq_endpoints.insert(mq, info.clone());
                    }
                }
            }

            for i in 0..max_total_nums {
                let mq = MessageQueue::from_parts(topic, TopicQueueMappingUtils::get_mock_broker_name(&scope), i);
                let broker_name = mq_endpoints
                    .get(&mq)
                    .and_then(|info| info.bname.clone())
                    .unwrap_or_else(|| {
                        CheetahString::from_static_str(mix_all::LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST)
                    });
                mq_end_points_of_broker.insert(mq, broker_name);
            }
        }

        Some(mq_end_points_of_broker)
    }

    pub fn broker_addr_table(&self) -> Arc<RwLock<HashMap<CheetahString, HashMap<u64, CheetahString>>>> {
        self.broker_addr_table.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn static_mapping_info(
        scope: Option<&str>,
        bname: Option<&str>,
        total_queues: i32,
        curr_ids: Option<&[i32]>,
    ) -> TopicQueueMappingInfo {
        TopicQueueMappingInfo {
            topic: Some(CheetahString::from_static_str("StaticTopic")),
            scope: scope.map(CheetahString::from),
            total_queues,
            bname: bname.map(CheetahString::from),
            epoch: 1,
            dirty: false,
            curr_id_map: curr_ids.map(|ids| ids.iter().map(|id| (*id, *id)).collect()),
        }
    }

    #[test]
    fn get_broker_name_from_message_queue_matches_java_fallbacks() {
        let metadata = ClientMetadata::new();
        let mq = MessageQueue::from_parts("TopicA", "BrokerA", 0);

        assert_eq!(
            metadata.get_broker_name_from_message_queue(&mq),
            Some(CheetahString::from_static_str("BrokerA"))
        );

        metadata
            .topic_end_points_table
            .write()
            .insert(CheetahString::from_static_str("TopicA"), HashMap::new());
        assert_eq!(
            metadata.get_broker_name_from_message_queue(&mq),
            Some(CheetahString::from_static_str("BrokerA"))
        );

        let other_mq = MessageQueue::from_parts("TopicA", "BrokerA", 1);
        let mut endpoints = HashMap::new();
        endpoints.insert(mq.clone(), CheetahString::from_static_str("MappedBroker"));
        metadata
            .topic_end_points_table
            .write()
            .insert(CheetahString::from_static_str("TopicA"), endpoints);

        assert_eq!(
            metadata.get_broker_name_from_message_queue(&mq),
            Some(CheetahString::from_static_str("MappedBroker"))
        );
        assert_eq!(metadata.get_broker_name_from_message_queue(&other_mq), None);
    }

    #[test]
    fn topic_route_data2endpoints_valid_mapping_matches_java_shape() {
        let mut mappings = HashMap::new();
        mappings.insert(
            CheetahString::from_static_str("DefaultBroker"),
            static_mapping_info(Some("scope"), Some("BrokerA"), 1, Some(&[0])),
        );
        let route = TopicRouteData {
            topic_queue_mapping_by_broker: Some(mappings),
            ..Default::default()
        };

        let endpoints = ClientMetadata::topic_route_data2endpoints_for_static_topic("StaticTopic", &route)
            .expect("static endpoints should be returned");

        let mq = MessageQueue::from_parts("StaticTopic", TopicQueueMappingUtils::get_mock_broker_name("scope"), 0);
        assert_eq!(endpoints.get(&mq), Some(&CheetahString::from_static_str("BrokerA")));
    }

    #[test]
    fn topic_route_data2endpoints_missing_curr_id_map_fills_not_exist_without_panic() {
        let mut mappings = HashMap::new();
        mappings.insert(
            CheetahString::from_static_str("DefaultBroker"),
            static_mapping_info(Some("scope"), Some("BrokerA"), 2, None),
        );
        let route = TopicRouteData {
            topic_queue_mapping_by_broker: Some(mappings),
            ..Default::default()
        };

        let endpoints = ClientMetadata::topic_route_data2endpoints_for_static_topic("StaticTopic", &route)
            .expect("static endpoints should be returned");

        assert_eq!(endpoints.len(), 2);
        for queue_id in 0..2 {
            let mq = MessageQueue::from_parts(
                "StaticTopic",
                TopicQueueMappingUtils::get_mock_broker_name("scope"),
                queue_id,
            );
            assert_eq!(
                endpoints.get(&mq),
                Some(&CheetahString::from_static_str(
                    mix_all::LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST
                ))
            );
        }
    }

    #[test]
    fn topic_route_data2endpoints_missing_bname_falls_back_without_panic() {
        let mut mappings = HashMap::new();
        mappings.insert(
            CheetahString::from_static_str("DefaultBroker"),
            static_mapping_info(Some("scope"), None, 1, Some(&[0])),
        );
        let route = TopicRouteData {
            topic_queue_mapping_by_broker: Some(mappings),
            ..Default::default()
        };

        let endpoints = ClientMetadata::topic_route_data2endpoints_for_static_topic("StaticTopic", &route)
            .expect("static endpoints should be returned");
        let mq = MessageQueue::from_parts("StaticTopic", TopicQueueMappingUtils::get_mock_broker_name("scope"), 0);

        assert_eq!(
            endpoints.get(&mq),
            Some(&CheetahString::from_static_str(
                mix_all::LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST
            ))
        );
    }

    #[test]
    fn fresh_topic_route_does_not_store_empty_static_endpoint_table() {
        let metadata = ClientMetadata::new();
        let topic = CheetahString::from_static_str("NormalTopic");
        metadata.fresh_topic_route(&topic, Some(TopicRouteData::default()));

        assert!(!metadata.topic_end_points_table.read().contains_key(&topic));
    }
}
