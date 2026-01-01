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

use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_rust::ArcMut;

use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::pull_request::PullRequest;

/// The `MQConsumerInnerLocal` trait defines the core functionalities required for a local MQ
/// consumer. It extends the `MQConsumerInnerAny` trait and requires implementations to be `Sync`
/// and `'static`.

#[trait_variant::make(MQConsumerInner: Send)]
pub trait MQConsumerInnerLocal: MQConsumerInnerAny + Sync + 'static {
    /// Returns the group name of the consumer.
    fn group_name(&self) -> CheetahString;

    /// Returns the message model used by the consumer.
    fn message_model(&self) -> MessageModel;

    /// Returns the type of consumption (e.g., push or pull).
    fn consume_type(&self) -> ConsumeType;

    /// Returns the point from where the consumer should start consuming messages.
    fn consume_from_where(&self) -> ConsumeFromWhere;

    /// Returns the set of subscriptions for the consumer.
    fn subscriptions(&self) -> HashSet<SubscriptionData>;

    /// Performs the rebalancing of the consumer.
    fn do_rebalance(&self);

    /// Attempts to perform rebalancing asynchronously and returns a `Result` indicating success or
    /// failure.
    async fn try_rebalance(&self) -> rocketmq_error::RocketMQResult<bool>;

    /// Persists the consumer offset asynchronously.
    async fn persist_consumer_offset(&self);

    /// Updates the subscription information for a given topic asynchronously.
    ///
    /// # Arguments
    ///
    /// * `topic` - A string slice that holds the name of the topic.
    /// * `info` - A reference to a `HashSet` containing `MessageQueue` information.
    async fn update_topic_subscribe_info(&self, topic: CheetahString, info: &HashSet<MessageQueue>);

    /// Checks if the subscription information for a given topic needs to be updated asynchronously.
    ///
    /// # Arguments
    ///
    /// * `topic` - A string slice that holds the name of the topic.
    ///
    /// # Returns
    ///
    /// * `bool` - `true` if the subscription information needs to be updated, `false` otherwise.
    async fn is_subscribe_topic_need_update(&self, topic: &str) -> bool;

    /// Returns whether the consumer is in unit mode.
    fn is_unit_mode(&self) -> bool;

    /// Returns the running information of the consumer.
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

#[derive(Clone)]
pub struct MQConsumerInnerImpl {
    pub(crate) default_mqpush_consumer_impl: ArcMut<DefaultMQPushConsumerImpl>,
}

impl MQConsumerInnerImpl {
    pub(crate) async fn pop_message(&mut self, pop_request: PopRequest) {
        self.default_mqpush_consumer_impl.pop_message(pop_request).await;
    }

    pub(crate) async fn pull_message(&mut self, pull_request: PullRequest) {
        self.default_mqpush_consumer_impl.pull_message(pull_request).await;
    }

    pub(crate) async fn consume_message_directly(
        &self,
        msg: MessageExt,
        broker_name: Option<CheetahString>,
    ) -> Option<ConsumeMessageDirectlyResult> {
        self.default_mqpush_consumer_impl
            .consume_message_directly(msg, broker_name)
            .await
    }
}

impl MQConsumerInner for MQConsumerInnerImpl {
    #[inline]
    fn group_name(&self) -> CheetahString {
        MQConsumerInner::group_name(self.default_mqpush_consumer_impl.as_ref())
    }

    #[inline]
    fn message_model(&self) -> MessageModel {
        MQConsumerInner::message_model(self.default_mqpush_consumer_impl.as_ref())
    }

    #[inline]
    fn consume_type(&self) -> ConsumeType {
        MQConsumerInner::consume_type(self.default_mqpush_consumer_impl.as_ref())
    }

    #[inline]
    fn consume_from_where(&self) -> ConsumeFromWhere {
        MQConsumerInner::consume_from_where(self.default_mqpush_consumer_impl.as_ref())
    }

    #[inline]
    fn subscriptions(&self) -> HashSet<SubscriptionData> {
        MQConsumerInner::subscriptions(self.default_mqpush_consumer_impl.as_ref())
    }

    #[inline]
    fn do_rebalance(&self) {
        MQConsumerInner::do_rebalance(self.default_mqpush_consumer_impl.as_ref())
    }

    #[inline]
    async fn try_rebalance(&self) -> rocketmq_error::RocketMQResult<bool> {
        MQConsumerInner::try_rebalance(self.default_mqpush_consumer_impl.as_ref()).await
    }

    #[inline]
    async fn persist_consumer_offset(&self) {
        MQConsumerInner::persist_consumer_offset(self.default_mqpush_consumer_impl.as_ref()).await
    }

    #[inline]
    async fn update_topic_subscribe_info(&self, topic: CheetahString, info: &HashSet<MessageQueue>) {
        MQConsumerInner::update_topic_subscribe_info(self.default_mqpush_consumer_impl.mut_from_ref(), topic, info)
            .await
    }

    #[inline]
    async fn is_subscribe_topic_need_update(&self, topic: &str) -> bool {
        MQConsumerInner::is_subscribe_topic_need_update(self.default_mqpush_consumer_impl.as_ref(), topic).await
    }

    #[inline]
    fn is_unit_mode(&self) -> bool {
        MQConsumerInner::is_unit_mode(self.default_mqpush_consumer_impl.as_ref())
    }

    #[inline]
    fn consumer_running_info(&self) -> ConsumerRunningInfo {
        MQConsumerInner::consumer_running_info(self.default_mqpush_consumer_impl.as_ref())
    }
}
