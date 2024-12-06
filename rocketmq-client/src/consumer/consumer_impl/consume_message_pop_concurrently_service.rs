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
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_client_ext::MessageClientExt;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;

use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::consume_message_service::ConsumeMessageServiceTrait;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;
use crate::consumer::listener::message_listener_concurrently::ArcBoxMessageListenerConcurrently;

pub struct ConsumeMessagePopConcurrentlyService {
    pub(crate) default_mqpush_consumer_impl: Option<WeakArcMut<DefaultMQPushConsumerImpl>>,
    pub(crate) client_config: ArcMut<ClientConfig>,
    pub(crate) consumer_config: ArcMut<ConsumerConfig>,
    pub(crate) consumer_group: CheetahString,
    pub(crate) message_listener: ArcBoxMessageListenerConcurrently,
}

impl ConsumeMessagePopConcurrentlyService {
    pub fn new(
        client_config: ArcMut<ClientConfig>,
        consumer_config: ArcMut<ConsumerConfig>,
        consumer_group: CheetahString,
        message_listener: ArcBoxMessageListenerConcurrently,
    ) -> Self {
        Self {
            default_mqpush_consumer_impl: None,
            client_config,
            consumer_config,
            consumer_group,
            message_listener,
        }
    }
}

impl ConsumeMessageServiceTrait for ConsumeMessagePopConcurrentlyService {
    fn start(&mut self, this: WeakArcMut<Self>) {
        //todo!()
    }

    async fn shutdown(&mut self, await_terminate_millis: u64) {
        todo!()
    }

    fn update_core_pool_size(&self, core_pool_size: usize) {
        todo!()
    }

    fn inc_core_pool_size(&self) {
        todo!()
    }

    fn dec_core_pool_size(&self) {
        todo!()
    }

    fn get_core_pool_size(&self) -> usize {
        todo!()
    }

    async fn consume_message_directly(
        &self,
        msg: MessageExt,
        broker_name: Option<CheetahString>,
    ) -> ConsumeMessageDirectlyResult {
        todo!()
    }

    async fn submit_consume_request(
        &self,
        this: WeakArcMut<Self>,
        msgs: Vec<ArcMut<MessageClientExt>>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        dispatch_to_consume: bool,
    ) {
        todo!()
    }

    async fn submit_pop_consume_request(
        &self,
        msgs: Vec<MessageExt>,
        process_queue: &PopProcessQueue,
        message_queue: &MessageQueue,
    ) {
        todo!()
    }
}
