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

use crate::protocol::body::broker_body::cluster_info::ClusterInfo;
use crate::protocol::route::topic_route_data::TopicRouteData;
use crate::protocol::static_topic::topic_queue_info::TopicQueueMappingInfo;
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
        if topic.is_empty() || topic_route_data.is_none() {
            return;
        }
        let read_guard = self.topic_route_table.read();
        let old = read_guard.get(topic);
        if !topic_route_data.as_ref().unwrap().topic_route_data_changed(old) {
            return;
        }
        drop(read_guard);
        let topic_route_data = topic_route_data.unwrap();
        {
            let mut write_guard = self.broker_addr_table.write();
            for bd in topic_route_data.broker_datas.iter() {
                write_guard.insert(bd.broker_name().clone(), bd.broker_addrs().clone());
            }
        }

        {
            let mq_end_points = ClientMetadata::topic_route_data2endpoints_for_static_topic(topic, &topic_route_data);
            if let Some(mq_end_points) = mq_end_points {
                let mut write_guard = self.topic_end_points_table.write();
                write_guard.insert(topic.clone(), mq_end_points);
            }
        }
    }

    pub fn get_broker_name_from_message_queue(&self, mq: &MessageQueue) -> Option<CheetahString> {
        let read_guard = self.topic_end_points_table.read();
        let topic_end_points = read_guard.get(mq.get_topic());
        if topic_end_points.is_none() {
            return Some(mq.get_broker_name().clone());
        }
        let topic_end_points = topic_end_points.unwrap();
        let broker_name = topic_end_points.get(mq);
        broker_name.cloned()
    }

    pub fn refresh_cluster_info(&self, cluster_info: Option<&ClusterInfo>) {
        if cluster_info.is_none() {
            return;
        }
        let cluster_info = cluster_info.unwrap();
        if cluster_info.broker_addr_table.is_none() {
            return;
        }

        let mut write_guard = self.broker_addr_table.write();
        for (broker_name, broker_data) in cluster_info.broker_addr_table.as_ref().unwrap() {
            let broker_addr = broker_data.broker_addrs();
            write_guard.insert(broker_name.clone(), broker_addr.clone());
        }
    }

    pub fn find_master_broker_addr(&self, broker_name: &str) -> Option<CheetahString> {
        let read_guard = self.broker_addr_table.read();
        if !read_guard.contains_key(broker_name) {
            return None;
        }
        let broker_addr = read_guard.get(broker_name).unwrap().get(&(MASTER_ID));
        broker_addr.cloned()
    }

    pub fn topic_route_data2endpoints_for_static_topic(
        topic: &str,
        topic_route_data: &TopicRouteData,
    ) -> Option<HashMap<MessageQueue, CheetahString>> {
        if topic_route_data.topic_queue_mapping_by_broker.is_none()
            || topic_route_data
                .topic_queue_mapping_by_broker
                .as_ref()
                .unwrap()
                .is_empty()
        {
            return Some(HashMap::new());
        }

        let mut mq_end_points_of_broker = HashMap::new();
        let mut mapping_infos_by_scope = HashMap::new();
        for (broker_name, info) in topic_route_data.topic_queue_mapping_by_broker.as_ref().unwrap().iter() {
            let scope = info.scope.as_ref();
            if let Some(scope_inner) = scope {
                if !mapping_infos_by_scope.contains_key(scope_inner.as_str()) {
                    mapping_infos_by_scope.insert(scope_inner.to_string(), HashMap::new());
                }
                mapping_infos_by_scope
                    .get_mut(scope_inner.as_str())
                    .unwrap()
                    .insert(broker_name.to_string(), info.clone());
            }
        }

        for (scope, topic_queue_mapping_info_map) in mapping_infos_by_scope {
            let mut mq_endpoints: HashMap<MessageQueue, TopicQueueMappingInfo> = HashMap::new();
            let mut mapping_infos: Vec<_> = topic_queue_mapping_info_map.iter().collect();
            mapping_infos.sort_by(|a, b| b.1.epoch.cmp(&a.1.epoch));

            let mut max_total_nums = 0;
            let max_total_num_of_epoch = -1;

            for (_, info) in mapping_infos {
                if info.epoch >= max_total_num_of_epoch && info.total_queues > max_total_nums {
                    max_total_nums = info.total_queues;
                }
                for global_id in info.curr_id_map.as_ref().unwrap().keys() {
                    let mq = MessageQueue::from_parts(
                        topic,
                        TopicQueueMappingUtils::get_mock_broker_name(info.scope.as_ref().unwrap().as_str()),
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
                    .map(|info| info.bname.clone().unwrap())
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
