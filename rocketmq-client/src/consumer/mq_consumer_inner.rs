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
use rocketmq_common::WeakCellWrapper;
use rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;

use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::Result;
#[trait_variant::make(MQConsumerInner: Send)]
pub trait MQConsumerInnerLocal: MQConsumerInnerAny + Sync + 'static {
    fn group_name(&self) -> String;

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

#[derive(Clone)]
pub(crate) struct MQConsumerInnerImpl {
    pub(crate) default_mqpush_consumer_impl: Option<WeakCellWrapper<DefaultMQPushConsumerImpl>>,
}

impl MQConsumerInnerImpl {
    pub(crate) async fn pop_message(&mut self, pop_request: PopRequest) {
        if let Some(ref default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl {
            if let Some(mut default_mqpush_consumer_impl) = default_mqpush_consumer_impl.upgrade() {
                default_mqpush_consumer_impl.pop_message(pop_request).await;
            }
        }
    }

    pub(crate) async fn pull_message(&mut self, pull_request: PullRequest) {
        if let Some(ref default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl {
            if let Some(mut default_mqpush_consumer_impl) = default_mqpush_consumer_impl.upgrade() {
                default_mqpush_consumer_impl
                    .pull_message(pull_request)
                    .await;
            }
        }
    }
}

impl MQConsumerInner for MQConsumerInnerImpl {
    fn group_name(&self) -> String {
        if let Some(ref default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl {
            if let Some(default_mqpush_consumer_impl) = default_mqpush_consumer_impl.upgrade() {
                return MQConsumerInner::group_name(default_mqpush_consumer_impl.as_ref());
            }
        }
        panic!("default_mqpush_consumer_impl is None");
    }

    fn message_model(&self) -> MessageModel {
        if let Some(ref default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl {
            if let Some(default_mqpush_consumer_impl) = default_mqpush_consumer_impl.upgrade() {
                return MQConsumerInner::message_model(default_mqpush_consumer_impl.as_ref());
            }
        }
        panic!("default_mqpush_consumer_impl is None");
    }

    fn consume_type(&self) -> ConsumeType {
        if let Some(ref default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl {
            if let Some(default_mqpush_consumer_impl) = default_mqpush_consumer_impl.upgrade() {
                return MQConsumerInner::consume_type(default_mqpush_consumer_impl.as_ref());
            }
        }
        panic!("default_mqpush_consumer_impl is None");
    }

    fn consume_from_where(&self) -> ConsumeFromWhere {
        if let Some(ref default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl {
            if let Some(default_mqpush_consumer_impl) = default_mqpush_consumer_impl.upgrade() {
                return MQConsumerInner::consume_from_where(default_mqpush_consumer_impl.as_ref());
            }
        }
        panic!("default_mqpush_consumer_impl is None");
    }

    fn subscriptions(&self) -> HashSet<SubscriptionData> {
        if let Some(ref default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl {
            if let Some(default_mqpush_consumer_impl) = default_mqpush_consumer_impl.upgrade() {
                return MQConsumerInner::subscriptions(default_mqpush_consumer_impl.as_ref());
            }
        }
        panic!("default_mqpush_consumer_impl is None");
    }

    fn do_rebalance(&self) {
        if let Some(ref default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl {
            if let Some(default_mqpush_consumer_impl) = default_mqpush_consumer_impl.upgrade() {
                return MQConsumerInner::do_rebalance(default_mqpush_consumer_impl.as_ref());
            }
        }
        panic!("default_mqpush_consumer_impl is None");
    }

    async fn try_rebalance(&self) -> Result<bool> {
        if let Some(ref default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl {
            if let Some(default_mqpush_consumer_impl) = default_mqpush_consumer_impl.upgrade() {
                return MQConsumerInner::try_rebalance(default_mqpush_consumer_impl.as_ref()).await;
            }
        }
        panic!("default_mqpush_consumer_impl is None");
    }

    async fn persist_consumer_offset(&self) {
        if let Some(ref default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl {
            if let Some(default_mqpush_consumer_impl) = default_mqpush_consumer_impl.upgrade() {
                return MQConsumerInner::persist_consumer_offset(
                    default_mqpush_consumer_impl.as_ref(),
                )
                .await;
            }
        }
        panic!("default_mqpush_consumer_impl is None");
    }

    async fn update_topic_subscribe_info(&mut self, topic: &str, info: &HashSet<MessageQueue>) {
        if let Some(ref default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl {
            if let Some(mut default_mqpush_consumer_impl) = default_mqpush_consumer_impl.upgrade() {
                return MQConsumerInner::update_topic_subscribe_info(
                    default_mqpush_consumer_impl.as_mut(),
                    topic,
                    info,
                )
                .await;
            }
        }
        panic!("default_mqpush_consumer_impl is None");
    }

    async fn is_subscribe_topic_need_update(&self, topic: &str) -> bool {
        if let Some(ref default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl {
            if let Some(default_mqpush_consumer_impl) = default_mqpush_consumer_impl.upgrade() {
                return MQConsumerInner::is_subscribe_topic_need_update(
                    default_mqpush_consumer_impl.as_ref(),
                    topic,
                )
                .await;
            }
        }
        panic!("default_mqpush_consumer_impl is None");
    }

    fn is_unit_mode(&self) -> bool {
        if let Some(ref default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl {
            if let Some(default_mqpush_consumer_impl) = default_mqpush_consumer_impl.upgrade() {
                return MQConsumerInner::is_unit_mode(default_mqpush_consumer_impl.as_ref());
            }
        }
        panic!("default_mqpush_consumer_impl is None");
    }

    fn consumer_running_info(&self) -> ConsumerRunningInfo {
        if let Some(ref default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl {
            if let Some(default_mqpush_consumer_impl) = default_mqpush_consumer_impl.upgrade() {
                return MQConsumerInner::consumer_running_info(
                    default_mqpush_consumer_impl.as_ref(),
                );
            }
        }
        panic!("default_mqpush_consumer_impl is None");
    }
}
