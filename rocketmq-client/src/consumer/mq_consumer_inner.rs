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

use cheetah_string::CheetahString;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;

use crate::consumer::consumer_impl::default_lite_pull_consumer_impl::DefaultLitePullConsumerImpl;
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
    async fn do_rebalance(&self);

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

    /// Replaces the in-memory offsets for queues belonging to the given topic.
    async fn reset_offsets(&self, topic: &CheetahString, offsets: HashMap<MessageQueue, i64>);

    /// Returns a snapshot of the consumer offsets for the given topic.
    async fn consumer_status(&self, topic: &CheetahString) -> HashMap<MessageQueue, i64>;

    /// Returns the running information of the consumer.
    async fn consumer_running_info(&self) -> ConsumerRunningInfo;
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
    inner: MQConsumerInnerImplKind,
}

#[derive(Clone)]
enum MQConsumerInnerImplKind {
    Push(Arc<DefaultMQPushConsumerImpl>),
    LitePull(Arc<DefaultLitePullConsumerImpl>),
}

impl MQConsumerInnerImpl {
    pub(crate) fn from_push(default_mqpush_consumer_impl: Arc<DefaultMQPushConsumerImpl>) -> Self {
        Self {
            inner: MQConsumerInnerImplKind::Push(default_mqpush_consumer_impl),
        }
    }

    pub(crate) fn from_lite_pull(default_lite_pull_consumer_impl: Arc<DefaultLitePullConsumerImpl>) -> Self {
        Self {
            inner: MQConsumerInnerImplKind::LitePull(default_lite_pull_consumer_impl),
        }
    }

    pub(crate) fn is_push_consumer(&self) -> bool {
        matches!(self.inner, MQConsumerInnerImplKind::Push(_))
    }

    pub(crate) async fn pop_message(&mut self, pop_request: PopRequest) {
        match &mut self.inner {
            MQConsumerInnerImplKind::Push(consumer) => consumer.pop_message(pop_request).await,
            MQConsumerInnerImplKind::LitePull(consumer) => {
                tracing::warn!(
                    "Ignoring broker pop request for lite pull consumer group={}",
                    MQConsumerInner::group_name(consumer.as_ref())
                );
            }
        }
    }

    pub(crate) async fn pull_message(&mut self, pull_request: PullRequest) {
        match &mut self.inner {
            MQConsumerInnerImplKind::Push(consumer) => consumer.pull_message(pull_request).await,
            MQConsumerInnerImplKind::LitePull(consumer) => {
                tracing::warn!(
                    "Ignoring broker pull request for lite pull consumer group={}",
                    MQConsumerInner::group_name(consumer.as_ref())
                );
            }
        }
    }

    pub(crate) async fn consume_message_directly(
        &self,
        msg: MessageExt,
        broker_name: Option<CheetahString>,
    ) -> Option<ConsumeMessageDirectlyResult> {
        match &self.inner {
            MQConsumerInnerImplKind::Push(consumer) => consumer.consume_message_directly(msg, broker_name).await,
            MQConsumerInnerImplKind::LitePull(consumer) => {
                tracing::warn!(
                    "consumeMessageDirectly is not supported for lite pull consumer group={}",
                    MQConsumerInner::group_name(consumer.as_ref())
                );
                None
            }
        }
    }
}

impl MQConsumerInner for MQConsumerInnerImpl {
    #[inline]
    fn group_name(&self) -> CheetahString {
        match &self.inner {
            MQConsumerInnerImplKind::Push(consumer) => MQConsumerInner::group_name(consumer.as_ref()),
            MQConsumerInnerImplKind::LitePull(consumer) => MQConsumerInner::group_name(consumer.as_ref()),
        }
    }

    #[inline]
    fn message_model(&self) -> MessageModel {
        match &self.inner {
            MQConsumerInnerImplKind::Push(consumer) => MQConsumerInner::message_model(consumer.as_ref()),
            MQConsumerInnerImplKind::LitePull(consumer) => MQConsumerInner::message_model(consumer.as_ref()),
        }
    }

    #[inline]
    fn consume_type(&self) -> ConsumeType {
        match &self.inner {
            MQConsumerInnerImplKind::Push(consumer) => MQConsumerInner::consume_type(consumer.as_ref()),
            MQConsumerInnerImplKind::LitePull(consumer) => MQConsumerInner::consume_type(consumer.as_ref()),
        }
    }

    #[inline]
    fn consume_from_where(&self) -> ConsumeFromWhere {
        match &self.inner {
            MQConsumerInnerImplKind::Push(consumer) => MQConsumerInner::consume_from_where(consumer.as_ref()),
            MQConsumerInnerImplKind::LitePull(consumer) => MQConsumerInner::consume_from_where(consumer.as_ref()),
        }
    }

    #[inline]
    fn subscriptions(&self) -> HashSet<SubscriptionData> {
        match &self.inner {
            MQConsumerInnerImplKind::Push(consumer) => MQConsumerInner::subscriptions(consumer.as_ref()),
            MQConsumerInnerImplKind::LitePull(consumer) => MQConsumerInner::subscriptions(consumer.as_ref()),
        }
    }

    #[inline]
    async fn do_rebalance(&self) {
        match &self.inner {
            MQConsumerInnerImplKind::Push(consumer) => MQConsumerInner::do_rebalance(consumer.as_ref()).await,
            MQConsumerInnerImplKind::LitePull(consumer) => MQConsumerInner::do_rebalance(consumer.as_ref()).await,
        }
    }

    #[inline]
    async fn try_rebalance(&self) -> rocketmq_error::RocketMQResult<bool> {
        match &self.inner {
            MQConsumerInnerImplKind::Push(consumer) => MQConsumerInner::try_rebalance(consumer.as_ref()).await,
            MQConsumerInnerImplKind::LitePull(consumer) => MQConsumerInner::try_rebalance(consumer.as_ref()).await,
        }
    }

    #[inline]
    async fn persist_consumer_offset(&self) {
        match &self.inner {
            MQConsumerInnerImplKind::Push(consumer) => {
                MQConsumerInner::persist_consumer_offset(consumer.as_ref()).await
            }
            MQConsumerInnerImplKind::LitePull(consumer) => {
                MQConsumerInner::persist_consumer_offset(consumer.as_ref()).await
            }
        }
    }

    #[inline]
    async fn update_topic_subscribe_info(&self, topic: CheetahString, info: &HashSet<MessageQueue>) {
        match &self.inner {
            MQConsumerInnerImplKind::Push(consumer) => {
                MQConsumerInner::update_topic_subscribe_info(consumer.as_ref(), topic, info).await
            }
            MQConsumerInnerImplKind::LitePull(consumer) => {
                MQConsumerInner::update_topic_subscribe_info(consumer.as_ref(), topic, info).await
            }
        }
    }

    #[inline]
    async fn is_subscribe_topic_need_update(&self, topic: &str) -> bool {
        match &self.inner {
            MQConsumerInnerImplKind::Push(consumer) => {
                MQConsumerInner::is_subscribe_topic_need_update(consumer.as_ref(), topic).await
            }
            MQConsumerInnerImplKind::LitePull(consumer) => {
                MQConsumerInner::is_subscribe_topic_need_update(consumer.as_ref(), topic).await
            }
        }
    }

    #[inline]
    fn is_unit_mode(&self) -> bool {
        match &self.inner {
            MQConsumerInnerImplKind::Push(consumer) => MQConsumerInner::is_unit_mode(consumer.as_ref()),
            MQConsumerInnerImplKind::LitePull(consumer) => MQConsumerInner::is_unit_mode(consumer.as_ref()),
        }
    }

    #[inline]
    async fn reset_offsets(&self, topic: &CheetahString, offsets: HashMap<MessageQueue, i64>) {
        match &self.inner {
            MQConsumerInnerImplKind::Push(consumer) => {
                MQConsumerInner::reset_offsets(consumer.as_ref(), topic, offsets).await
            }
            MQConsumerInnerImplKind::LitePull(consumer) => {
                MQConsumerInner::reset_offsets(consumer.as_ref(), topic, offsets).await
            }
        }
    }

    #[inline]
    async fn consumer_status(&self, topic: &CheetahString) -> HashMap<MessageQueue, i64> {
        match &self.inner {
            MQConsumerInnerImplKind::Push(consumer) => MQConsumerInner::consumer_status(consumer.as_ref(), topic).await,
            MQConsumerInnerImplKind::LitePull(consumer) => {
                MQConsumerInner::consumer_status(consumer.as_ref(), topic).await
            }
        }
    }

    #[inline]
    async fn consumer_running_info(&self) -> ConsumerRunningInfo {
        match &self.inner {
            MQConsumerInnerImplKind::Push(consumer) => MQConsumerInner::consumer_running_info(consumer.as_ref()).await,
            MQConsumerInnerImplKind::LitePull(consumer) => {
                MQConsumerInner::consumer_running_info(consumer.as_ref()).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;

    use super::*;
    use crate::base::client_config::ClientConfig;
    use crate::consumer::consumer_impl::default_lite_pull_consumer_impl::LitePullConsumerConfig;

    #[test]
    fn lite_pull_wrapper_delegates_heartbeat_fields() {
        let consumer_group = CheetahString::from_static_str("lite_pull_wrapper_group");
        let consumer_config = Arc::new(LitePullConsumerConfig {
            consumer_group: consumer_group.clone(),
            ..Default::default()
        });
        let impl_ = Arc::new(DefaultLitePullConsumerImpl::new(
            Arc::new(ClientConfig::default()),
            consumer_config,
        ));
        let wrapper = MQConsumerInnerImpl::from_lite_pull(impl_);

        assert_eq!(MQConsumerInner::group_name(&wrapper), consumer_group);
        assert_eq!(MQConsumerInner::message_model(&wrapper), MessageModel::Clustering);
        assert_eq!(MQConsumerInner::consume_type(&wrapper), ConsumeType::ConsumeActively);
        assert_eq!(
            MQConsumerInner::consume_from_where(&wrapper),
            ConsumeFromWhere::ConsumeFromLastOffset
        );
        assert!(!MQConsumerInner::is_unit_mode(&wrapper));
        assert!(MQConsumerInner::subscriptions(&wrapper).is_empty());
    }
}
