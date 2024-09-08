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
use std::collections::HashSet;
use std::sync::Arc;

use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_common::ArcRefCellWrapper;
use rocketmq_common::WeakCellWrapper;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use tokio::sync::RwLock;
use tracing::error;
use tracing::warn;

use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::consumer_impl::re_balance::Rebalance;
use crate::factory::mq_client_instance::MQClientInstance;

const TIMEOUT_CHECK_TIMES: u32 = 3;
const QUERY_ASSIGNMENT_TIMEOUT: u32 = 3000;

pub(crate) struct RebalanceImpl<R> {
    pub(crate) process_queue_table: Arc<RwLock<HashMap<MessageQueue, ProcessQueue>>>,
    pub(crate) pop_process_queue_table: Arc<RwLock<HashMap<MessageQueue, PopProcessQueue>>>,
    pub(crate) topic_subscribe_info_table: Arc<RwLock<HashMap<String, HashSet<MessageQueue>>>>,
    pub(crate) subscription_inner: Arc<RwLock<HashMap<String, SubscriptionData>>>,
    pub(crate) consumer_group: Option<String>,
    pub(crate) message_model: Option<MessageModel>,
    pub(crate) allocate_message_queue_strategy: Option<Arc<dyn AllocateMessageQueueStrategy>>,
    pub(crate) mqclient_instance: Option<ArcRefCellWrapper<MQClientInstance>>,
    pub(crate) sub_rebalance_impl: Option<WeakCellWrapper<R>>,
}

impl<R> RebalanceImpl<R>
where
    R: Rebalance,
{
    pub fn new(
        consumer_group: Option<String>,
        message_model: Option<MessageModel>,
        allocate_message_queue_strategy: Option<Arc<dyn AllocateMessageQueueStrategy>>,
        mqclient_instance: Option<ArcRefCellWrapper<MQClientInstance>>,
    ) -> Self {
        RebalanceImpl {
            process_queue_table: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            pop_process_queue_table: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            topic_subscribe_info_table: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            subscription_inner: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            consumer_group,
            message_model,
            allocate_message_queue_strategy,
            mqclient_instance,
            sub_rebalance_impl: None,
        }
    }

    pub async fn put_subscription_data(&self, topic: &str, subscription_data: SubscriptionData) {
        let mut subscription_inner = self.subscription_inner.write().await;
        subscription_inner.insert(topic.to_string(), subscription_data);
    }

    pub async fn do_rebalance(&mut self, is_order: bool) -> bool {
        let mut balanced = true;
        let arc = self.subscription_inner.clone();
        let sub_table = arc.read().await;
        if !sub_table.is_empty() {
            for (topic, _) in sub_table.iter() {
                if !self.client_rebalance(topic) && self.try_query_assignment(topic).await {
                    if !self.get_rebalance_result_from_broker(topic, is_order).await {
                        balanced = false;
                    }
                } else if !self.rebalance_by_topic(topic, is_order).await {
                    balanced = false;
                }
            }
        }
        self.truncate_message_queue_not_my_topic().await;
        balanced
    }

    pub fn client_rebalance(&mut self, topic: &str) -> bool {
        true
    }

    async fn try_query_assignment(&self, topic: &str) -> bool {
        unimplemented!("try_query_assignment")
    }

    async fn truncate_message_queue_not_my_topic(&self) {
        unimplemented!("try_query_assignment")
    }

    async fn get_rebalance_result_from_broker(&self, topic: &str, is_order: bool) -> bool {
        unimplemented!("try_query_assignment")
    }

    async fn update_process_queue_table_in_rebalance(
        &self,
        topic: &str,
        mq_set: &HashSet<MessageQueue>,
        is_order: bool,
    ) -> bool {
        unimplemented!("try_query_assignment")
    }
    async fn rebalance_by_topic(&self, topic: &str, is_order: bool) -> bool {
        match self.message_model.unwrap() {
            MessageModel::Broadcasting => {
                unimplemented!("Broadcasting")
            }
            MessageModel::Clustering => {
                let topic_subscribe_info_table_inner = self.topic_subscribe_info_table.read().await;
                let mq_set = topic_subscribe_info_table_inner.get(topic);
                let ci_all = self
                    .mqclient_instance
                    .as_ref()
                    .unwrap()
                    .find_consumer_id_list(topic, self.consumer_group.as_ref().unwrap())
                    .await;
                if ci_all.is_none() && !topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
                    if let Some(sub_rebalance_impl) =
                        self.sub_rebalance_impl.as_ref().unwrap().upgrade()
                    {
                        sub_rebalance_impl.message_queue_changed(
                            topic,
                            &HashSet::new(),
                            &HashSet::new(),
                        );
                        warn!(
                            "doRebalance, {}, but the topic[{}] not exist.",
                            self.consumer_group.as_ref().unwrap(),
                            topic
                        );
                    }
                }
                if ci_all.is_none() {
                    warn!(
                        "doRebalance, {}, but the topic[{}] not exist.",
                        self.consumer_group.as_ref().unwrap(),
                        topic
                    );
                }
                if mq_set.is_some() && ci_all.is_some() {
                    let mq_set = mq_set.unwrap();
                    let mut mq_all = mq_set.iter().cloned().collect::<Vec<MessageQueue>>();
                    mq_all.sort();
                    let mut ci_all = ci_all.unwrap();
                    ci_all.sort();

                    let allocate_result = match self
                        .allocate_message_queue_strategy
                        .as_ref()
                        .unwrap()
                        .allocate(
                            self.consumer_group.as_ref().unwrap(),
                            self.mqclient_instance.as_ref().unwrap().client_id.as_ref(),
                            mq_all.as_slice(),
                            ci_all.as_slice(),
                        ) {
                        Ok(value) => value,
                        Err(e) => {
                            error!(
                                "allocate message queue exception. strategy name: {}, ex: {}",
                                self.allocate_message_queue_strategy
                                    .as_ref()
                                    .unwrap()
                                    .get_name(),
                                e.to_string()
                            );
                            return false;
                        }
                    };
                    let allocate_result_set = allocate_result
                        .into_iter()
                        .collect::<HashSet<MessageQueue>>();
                    let changed = self
                        .update_process_queue_table_in_rebalance(
                            topic,
                            &allocate_result_set,
                            is_order,
                        )
                        .await;
                    if changed {
                        // info
                        if let Some(sub_rebalance_impl) =
                            self.sub_rebalance_impl.as_ref().unwrap().upgrade()
                        {
                            sub_rebalance_impl.message_queue_changed(
                                topic,
                                mq_set,
                                &allocate_result_set,
                            );
                        }
                    }
                    return allocate_result_set.eq(&self.get_working_message_queue(topic));
                }
                true
            }
        }
    }

    pub fn get_working_message_queue(&self, topic: &str) -> HashSet<MessageQueue> {
        unimplemented!()
    }
}
