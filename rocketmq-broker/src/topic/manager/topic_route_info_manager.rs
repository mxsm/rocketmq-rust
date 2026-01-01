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
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_client_rust::factory::mq_client_instance::topic_route_data2topic_publish_info;
use rocketmq_client_rust::factory::mq_client_instance::topic_route_data2topic_subscribe_info;
use rocketmq_client_rust::producer::producer_impl::topic_publish_info::TopicPublishInfo;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_rust::ArcMut;
use rocketmq_rust::RocketMQTokioMutex;
use rocketmq_store::base::message_store::MessageStore;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;

const GET_TOPIC_ROUTE_TIMEOUT: u64 = 3000;
const LOCK_TIMEOUT_MILLIS: u64 = 3000;

#[derive(Clone)]
pub(crate) struct TopicRouteInfoManager<MS: MessageStore> {
    pub(crate) lock: Arc<RocketMQTokioMutex<()>>,
    pub(crate) topic_route_table: ArcMut<HashMap<CheetahString /* Topic */, TopicRouteData>>,
    pub(crate) broker_addr_table:
        ArcMut<HashMap<CheetahString /* Broker Name */, HashMap<u64 /* brokerId */, CheetahString /* address */>>>,
    pub(crate) topic_publish_info_table: ArcMut<HashMap<CheetahString /* topic */, TopicPublishInfo>>,
    pub(crate) topic_subscribe_info_table: ArcMut<HashMap<CheetahString /* topic */, HashSet<MessageQueue>>>,
    pub(crate) broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> TopicRouteInfoManager<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        TopicRouteInfoManager {
            lock: Arc::new(RocketMQTokioMutex::new(())),
            topic_route_table: ArcMut::new(HashMap::new()),
            broker_addr_table: ArcMut::new(HashMap::new()),
            topic_publish_info_table: ArcMut::new(HashMap::new()),
            topic_subscribe_info_table: ArcMut::new(HashMap::new()),
            broker_runtime_inner,
        }
    }

    pub fn start(&self) {
        let this = self.broker_runtime_inner.clone();
        let load_balance_poll_name_server_interval = self
            .broker_runtime_inner
            .broker_config()
            .load_balance_poll_name_server_interval;
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            loop {
                this.topic_route_info_manager()
                    .update_topic_route_info_from_name_server()
                    .await;
                tokio::time::sleep(tokio::time::Duration::from_millis(
                    load_balance_poll_name_server_interval,
                ))
                .await;
            }
        });
    }

    pub fn shutdown(&mut self) {
        warn!("TopicRouteInfoManager shutdown not implemented");
    }
    async fn update_topic_route_info_from_name_server(&self) {
        let topic_set_for_pop_assignment = self
            .topic_subscribe_info_table
            .keys()
            .cloned()
            .collect::<HashSet<CheetahString>>();
        let topic_set_for_escape_bridge = self
            .topic_route_table
            .keys()
            .cloned()
            .collect::<HashSet<CheetahString>>();
        let topics_all = topic_set_for_pop_assignment
            .union(&topic_set_for_escape_bridge)
            .cloned()
            .collect::<HashSet<CheetahString>>();
        for topic in topics_all {
            let is_need_update_subscribe_info = topic_set_for_pop_assignment.contains(&topic);
            let is_need_update_publish_info = topic_set_for_escape_bridge.contains(&topic);
            self.update_topic_route_info_from_name_server_ext(
                &topic,
                is_need_update_publish_info,
                is_need_update_subscribe_info,
            )
            .await;
        }
    }
    pub async fn update_topic_route_info_from_name_server_ext(
        &self,
        topic: &CheetahString,
        is_need_update_publish_info: bool,
        is_need_update_subscribe_info: bool,
    ) {
        if let Some(_lock) = self
            .lock
            .try_lock_timeout(Duration::from_millis(LOCK_TIMEOUT_MILLIS))
            .await
        {
            let topic_route_data = self
                .broker_runtime_inner
                .broker_outer_api()
                .get_topic_route_info_from_name_server(topic, GET_TOPIC_ROUTE_TIMEOUT, true)
                .await;
            if let Err(e) = topic_route_data {
                if !NamespaceUtil::is_retry_topic(topic) {
                    if let rocketmq_error::RocketMQError::BrokerOperationFailed { code, .. } = e {
                        if code == ResponseCode::TopicNotExist as i32 {
                            self.clean_none_route_topic(topic);
                            return;
                        }
                    }
                }

                warn!(
                    "TopicRouteInfoManager: updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer \
                     return null, Topic: {}.",
                    topic
                );
                return;
            }
            let mut topic_route_data = topic_route_data.unwrap();
            if is_need_update_subscribe_info {
                self.update_subscribe_info_table(topic.clone(), &topic_route_data);
            }

            if is_need_update_publish_info {
                self.update_topic_route_table(&mut topic_route_data, topic.clone());
            }
        } else {
            warn!("try to lock timeout");
        }
    }

    #[inline]
    fn clean_none_route_topic(&self, topic: &CheetahString) {
        self.topic_subscribe_info_table.mut_from_ref().remove(topic);
    }

    fn update_subscribe_info_table(&self, topic: CheetahString, topic_route_data: &TopicRouteData) -> bool {
        let mut tmp = TopicRouteData::from_existing(topic_route_data);
        tmp.topic_queue_mapping_by_broker = None;
        let new_subscribe_info = topic_route_data2topic_subscribe_info(topic.as_str(), &tmp);
        let old_subscribe_info = self
            .topic_subscribe_info_table
            .get(&topic)
            .cloned()
            .unwrap_or(HashSet::new());
        if new_subscribe_info == old_subscribe_info {
            return false;
        }
        info!(
            "the topic[{}] subscribe message queue changed, old[{:?}] ,new[{:?}]",
            topic, old_subscribe_info, new_subscribe_info,
        );
        self.topic_subscribe_info_table
            .mut_from_ref()
            .insert(topic, new_subscribe_info);
        true
    }

    fn update_topic_route_table(&self, topic_route_data: &mut TopicRouteData, topic: CheetahString) -> bool {
        let old = self.topic_route_table.get(&topic);
        let changed = topic_route_data.topic_route_data_changed(old);
        if !changed {
            if !self.is_need_update_topic_route_info(&topic) {
                return false;
            }
        } else {
            info!(
                "the topic[{}] route info changed, old[{:?}] ,new[{:?}]",
                topic, old, topic_route_data,
            )
        };
        for bd in &topic_route_data.broker_datas {
            self.broker_addr_table
                .mut_from_ref()
                .insert(bd.broker_name().clone(), bd.broker_addrs().clone());
        }
        let mut publish_info = topic_route_data2topic_publish_info(topic.as_str(), topic_route_data);
        publish_info.have_topic_router_info = true;
        self.update_topic_publish_info(&topic, publish_info);

        let clone_topic_route_data = TopicRouteData::from_existing(topic_route_data);
        info!(
            "topicRouteTable.put. Topic = {}, TopicRouteData[{:?}]",
            topic, clone_topic_route_data
        );
        self.topic_route_table
            .mut_from_ref()
            .insert(topic, clone_topic_route_data);
        true
    }

    #[inline]
    fn update_topic_publish_info(&self, topic: &CheetahString, info: TopicPublishInfo) {
        self.topic_publish_info_table.mut_from_ref().insert(topic.clone(), info);
    }

    #[inline]
    fn is_need_update_topic_route_info(&self, topic: &CheetahString) -> bool {
        let prev = self.topic_publish_info_table.get(topic);
        if let Some(prev) = prev {
            !prev.ok()
        } else {
            true
        }
    }

    pub async fn try_to_find_topic_publish_info(&self, topic: &CheetahString) -> Option<TopicPublishInfo> {
        let mut topic_publish_info = self.topic_publish_info_table.get(topic).cloned();
        if topic_publish_info.is_none() || !topic_publish_info.as_ref().unwrap().ok() {
            self.update_topic_route_info_from_name_server_ext(topic, true, false)
                .await;
            topic_publish_info = self.topic_publish_info_table.get(topic).cloned();
        }
        topic_publish_info
    }

    pub fn find_broker_address_in_publish(&self, broker_name: Option<&CheetahString>) -> Option<CheetahString> {
        let broker_name = broker_name?;
        let map = self.broker_addr_table.get(broker_name);
        if let Some(map) = map {
            if !map.is_empty() {
                return map.get(&mix_all::MASTER_ID).cloned();
            }
        }
        None
    }

    pub fn find_broker_address_in_subscribe(
        &self,
        broker_name: Option<&CheetahString>,
        broker_id: u64,
        only_this_broker: bool,
    ) -> Option<CheetahString> {
        let broker_name = broker_name?;
        let mut broker_addr: Option<CheetahString> = None;
        let mut found;

        if let Some(map) = self.broker_addr_table.get(broker_name) {
            if !map.is_empty() {
                broker_addr = map.get(&broker_id).cloned();
                let slave = broker_id != mix_all::MASTER_ID;
                found = broker_addr.is_some();

                if !found && slave {
                    broker_addr = map.get(&(broker_id + 1)).cloned();
                    found = broker_addr.is_some();
                }

                if !found && !only_this_broker {
                    if let Some((_, addr)) = map.iter().next() {
                        broker_addr = Some(addr.clone());
                    }
                }
            }
        }

        broker_addr
    }

    pub async fn get_topic_subscribe_info(&self, topic: &CheetahString) -> Option<HashSet<MessageQueue>> {
        let mut queues = self.topic_subscribe_info_table.get(topic).cloned();
        if queues.as_ref().is_none_or(|q| q.is_empty()) {
            self.update_topic_route_info_from_name_server_ext(topic, false, true)
                .await;
            queues = self.topic_subscribe_info_table.get(topic).cloned();
        }
        queues
    }
}
