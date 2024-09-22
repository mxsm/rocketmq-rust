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

use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;

use crate::Result;
#[trait_variant::make(MQConsumerInner: Send)]
pub trait MQConsumerInnerLocal: MQConsumerInnerAny + Sync + 'static {
    fn group_name(&self) -> &str;

    fn message_model(&self) -> MessageModel;

    fn consume_type(&self) -> ConsumeType;

    fn consume_from_where(&self) -> ConsumeFromWhere;

    fn subscriptions(&self) -> HashSet<SubscriptionData>;

    fn do_rebalance(&self);

    async fn try_rebalance(&self) -> Result<bool>;

    async fn persist_consumer_offset(&self);

    async fn update_topic_subscribe_info(&mut self, topic: &str, info: &HashSet<MessageQueue>);

    async fn is_subscribe_topic_need_update(&self, topic: &str) -> bool;

    fn is_unit_mode(&self) -> bool;

    fn consumer_running_info(&self) -> ConsumerRunningInfo;
}

pub trait MQConsumerInnerAny: std::any::Any {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;

    fn as_any(&self) -> &dyn std::any::Any;
}

impl<T: MQConsumerInner> MQConsumerInnerAny for T {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
