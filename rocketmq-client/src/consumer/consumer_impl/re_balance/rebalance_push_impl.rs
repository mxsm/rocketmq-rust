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
use std::collections::HashSet;
use std::sync::Arc;

use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::ArcRefCellWrapper;
use rocketmq_common::WeakCellWrapper;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;

use crate::base::client_config::ClientConfig;
use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::consumer::consumer_impl::re_balance::rebalance_impl::RebalanceImpl;
use crate::consumer::consumer_impl::re_balance::Rebalance;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::Result;

pub struct RebalancePushImpl {
    client_config: ClientConfig,
    consumer_config: ArcRefCellWrapper<ConsumerConfig>,
    rebalance_impl: RebalanceImpl<RebalancePushImpl>,
    default_mqpush_consumer_impl: Option<WeakCellWrapper<DefaultMQPushConsumerImpl>>,
}

impl RebalancePushImpl {
    pub fn new(
        client_config: ClientConfig,
        consumer_config: ArcRefCellWrapper<ConsumerConfig>,
    ) -> Self {
        RebalancePushImpl {
            client_config,
            consumer_config,
            rebalance_impl: RebalanceImpl::new(None, None, None, None),
            default_mqpush_consumer_impl: None,
        }
    }
}

impl RebalancePushImpl {
    pub fn set_default_mqpush_consumer_impl(
        &mut self,
        default_mqpush_consumer_impl: WeakCellWrapper<DefaultMQPushConsumerImpl>,
    ) {
        self.default_mqpush_consumer_impl = Some(default_mqpush_consumer_impl);
    }

    pub fn set_consumer_group(&mut self, consumer_group: String) {
        self.rebalance_impl.consumer_group = Some(consumer_group);
    }

    pub fn set_message_model(&mut self, message_model: MessageModel) {
        self.rebalance_impl.message_model = Some(message_model);
    }

    pub fn set_allocate_message_queue_strategy(
        &mut self,
        allocate_message_queue_strategy: Arc<dyn AllocateMessageQueueStrategy>,
    ) {
        self.rebalance_impl.allocate_message_queue_strategy = Some(allocate_message_queue_strategy);
    }

    pub fn set_mq_client_factory(&mut self, client_instance: ArcRefCellWrapper<MQClientInstance>) {
        self.rebalance_impl.client_instance = Some(client_instance);
    }

    pub async fn put_subscription_data(
        &mut self,
        topic: &str,
        subscription_data: SubscriptionData,
    ) {
        let mut subscription_inner = self.rebalance_impl.subscription_inner.write().await;
        subscription_inner.insert(topic.to_string(), subscription_data);
    }

    pub fn client_rebalance(&self, topic: &str) -> bool {
        self.consumer_config.client_rebalance
            || self.rebalance_impl.message_model.unwrap() == MessageModel::Broadcasting
            || if let Some(default_mqpush_consumer_impl) = self
                .default_mqpush_consumer_impl
                .as_ref()
                .unwrap()
                .upgrade()
            {
                default_mqpush_consumer_impl.is_consume_orderly()
            } else {
                false
            }
    }

    pub fn set_rebalance_impl(&mut self, rebalance_impl: WeakCellWrapper<RebalancePushImpl>) {
        self.rebalance_impl.sub_rebalance_impl = Some(rebalance_impl);
    }
}

impl Rebalance for RebalancePushImpl {
    fn message_queue_changed(
        &self,
        topic: &str,
        mq_all: &HashSet<MessageQueue>,
        mq_divided: &HashSet<MessageQueue>,
    ) {
        todo!()
    }

    fn remove_unnecessary_message_queue(
        &self,
        topic: &str,
        mq: MessageQueue,
        pq: ProcessQueue,
    ) -> bool {
        todo!()
    }

    fn remove_unnecessary_pop_message_queue(&self, mq: MessageQueue, pq: ProcessQueue) -> bool {
        todo!()
    }

    fn consume_type(&self) -> ConsumeType {
        todo!()
    }

    fn remove_dirty_offset(&self, mq: MessageQueue) {
        todo!()
    }

    fn compute_pull_from_where_with_exception(&self, mq: MessageQueue) -> Result<i64> {
        todo!()
    }

    fn get_consume_init_mode(&self) -> i32 {
        todo!()
    }

    fn dispatch_pull_request(&self, pull_request_list: Vec<PullRequest>, delay: i64) {
        todo!()
    }

    fn dispatch_pop_pull_request(&self, pull_request_list: Vec<PopRequest>, delay: i64) {
        todo!()
    }

    #[inline]
    fn create_process_queue(&self) -> ProcessQueue {
        ProcessQueue::new()
    }

    fn create_pop_process_queue(&self) -> PopProcessQueue {
        PopProcessQueue::new()
    }

    fn remove_process_queue(&self, mq: MessageQueue) {
        todo!()
    }

    fn unlock(&self, mq: MessageQueue, oneway: bool) {
        todo!()
    }

    fn lock_all(&self) {
        todo!()
    }

    fn unlock_all(&self, oneway: bool) {
        todo!()
    }

    async fn do_rebalance(&mut self, is_order: bool) -> bool {
        todo!()
    }
}
