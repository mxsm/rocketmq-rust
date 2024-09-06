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

use rocketmq_common::ArcRefCellWrapper;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;

use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::factory::mq_client_instance::MQClientInstance;

#[derive(Clone)]
pub struct RebalancePushImpl;

impl RebalancePushImpl {
    pub fn set_consumer_group(&mut self, consumer_group: String) {
        todo!()
    }

    pub fn set_message_model(&mut self, message_model: MessageModel) {
        todo!()
    }

    pub fn set_allocate_message_queue_strategy(
        &mut self,
        allocate_message_queue_strategy: Option<
            Arc<Box<dyn AllocateMessageQueueStrategy + Sync + Send>>,
        >,
    ) {
        todo!()
    }

    pub fn set_mq_client_factory(
        &mut self,
        mq_client_factory: ArcRefCellWrapper<MQClientInstance>,
    ) {
        todo!()
    }

    pub async fn put_subscription_data(
        &mut self,
        topic: &str,
        subscription_data: SubscriptionData,
    ) {
        // TODO
        unimplemented!("put_subscription_data")
    }
}
