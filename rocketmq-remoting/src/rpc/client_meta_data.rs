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
use std::sync::Arc;

use parking_lot::RwLock;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::MASTER_ID;

use crate::protocol::body::broker_body::cluster_info::ClusterInfo;
use crate::protocol::route::route_data_view::TopicRouteData;
use crate::protocol::static_topic::topic_queue_mapping_utils::TopicQueueMappingUtils;

#[derive(Default)]
pub struct ClientMetadata {
    topic_route_table: RwLock<HashMap<String, TopicRouteData>>,
    topic_endpoints_table: RwLock<HashMap<String, RwLock<HashMap<MessageQueue, String>>>>,
    broker_addr_table: RwLock<HashMap<String, HashMap<i64, String>>>,
    broker_version_table: RwLock<HashMap<String, HashMap<String, i32>>>,
}

impl ClientMetadata {
    pub fn new() -> Self {
        ClientMetadata {
            topic_route_table: RwLock::new(HashMap::new()),
            topic_endpoints_table: RwLock::new(HashMap::new()),
            broker_addr_table: RwLock::new(HashMap::new()),
            broker_version_table: RwLock::new(HashMap::new()),
        }
    }

    pub fn fresh_topic_route(&self, topic: &str, topic_route_data: Option<TopicRouteData>) {
        if topic.is_empty() || topic_route_data.is_none() {
            return;
        }
        let topic_route_data = topic_route_data.unwrap();
        let mut topic_route_table = self.topic_route_table.write();
        let old = topic_route_table.get(topic);
        if !topic_route_data.topic_route_data_changed(old) {
            return;
        }

        for bd in topic_route_data.get_broker_datas() {
            let mut broker_addr_table = self.broker_addr_table.write();
            broker_addr_table.insert(bd.get_broker_name().clone(), bd.get_broker_addrs().clone());
        }

        let mq_endpoints =
            Self::topic_route_data_to_endpoints_for_static_topic(topic, &topic_route_data);
        if !mq_endpoints.is_empty() {
            let mut topic_endpoints_table = self.topic_endpoints_table.write();
            topic_endpoints_table.insert(topic.to_string(), RwLock::new(mq_endpoints));
        }
    }

    pub fn get_broker_name_from_message_queue(&self, mq: &MessageQueue) -> String {
        let topic_endpoints_table = self.topic_endpoints_table.read().unwrap();
        if let Some(endpoints) = topic_endpoints_table.get(&mq.get_topic()) {
            if !endpoints.read().unwrap().is_empty() {
                return endpoints
                    .read()
                    .unwrap()
                    .get(mq)
                    .unwrap_or(&mq.get_broker_name())
                    .clone();
            }
        }
        mq.get_broker_name().clone()
    }

    pub fn refresh_cluster_info(&self, cluster_info: ClusterInfo) {
        if cluster_info.is_none() || cluster_info.get_broker_addr_table().is_none() {
            return;
        }

        let mut broker_addr_table = self.broker_addr_table.write();
        for (key, broker_data) in cluster_info.get_broker_addr_table() {
            broker_addr_table.insert(key.clone(), broker_data.get_broker_addrs().clone());
        }
    }

    pub fn find_master_broker_addr(&self, broker_name: &str) -> Option<String> {
        let broker_addr_table = self.broker_addr_table.read().unwrap();
        broker_addr_table
            .get(broker_name)
            .and_then(|addrs| addrs.get(&MASTER_ID).cloned())
    }

    pub fn get_broker_addr_table(&self) -> Arc<RwLock<HashMap<String, HashMap<i64, String>>>> {
        Arc::new(self.broker_addr_table.clone())
    }

    pub fn topic_route_data_to_endpoints_for_static_topic(
        topic: &str,
        route: &TopicRouteData,
    ) -> HashMap<MessageQueue, String> {
        let mut mq_endpoints_of_broker = HashMap::new();

        if route.get_topic_queue_mapping_by_broker().is_none()
            || route
                .get_topic_queue_mapping_by_broker()
                .unwrap()
                .is_empty()
        {
            return mq_endpoints_of_broker;
        }

        let mut mapping_infos_by_scope = HashMap::new();
        for (key, info) in route.get_topic_queue_mapping_by_broker().unwrap() {
            if let Some(scope) = info.get_scope() {
                mapping_infos_by_scope
                    .entry(scope)
                    .or_insert_with(HashMap::new)
                    .insert(key.clone(), info.clone());
            }
        }

        for (scope, topic_queue_mapping_info_map) in mapping_infos_by_scope {
            let mut mq_endpoints = HashMap::new();
            let mut mapping_infos: Vec<_> = topic_queue_mapping_info_map.iter().collect();
            mapping_infos.sort_by(|a, b| b.1.get_epoch().cmp(&a.1.get_epoch()));

            let mut max_total_nums = 0;
            let mut max_total_num_of_epoch = -1;

            for (_, info) in mapping_infos {
                if info.get_epoch() >= max_total_num_of_epoch
                    && info.get_total_queues() > max_total_nums
                {
                    max_total_nums = info.get_total_queues();
                }
                for (global_id, _) in info.get_curr_id_map() {
                    let mq = MessageQueue::with_params(
                        topic,
                        &TopicQueueMappingUtils::get_mock_broker_name(&info.get_scope()),
                        *global_id,
                    );
                    if let Some(old_info) = mq_endpoints.get(&mq) {
                        if old_info.get_epoch() <= info.get_epoch() {
                            mq_endpoints.insert(mq, info.clone());
                        }
                    } else {
                        mq_endpoints.insert(mq, info.clone());
                    }
                }
            }

            for i in 0..max_total_nums {
                let mq = MessageQueue::with_params(
                    topic,
                    &TopicQueueMappingUtils::get_mock_broker_name(&scope),
                    i,
                );
                if !mq_endpoints.contains_key(&mq) {
                    mq_endpoints_of_broker.insert(
                        mq,
                        mix_all::LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST.to_string(),
                    );
                } else {
                    mq_endpoints_of_broker
                        .insert(mq, mq_endpoints.get(&mq).unwrap().get_bname().to_string());
                }
            }
        }
        mq_endpoints_of_broker
    }
}
