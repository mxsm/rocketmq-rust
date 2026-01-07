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
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_remoting::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
use rocketmq_rust::ArcMut;

use crate::producer::producer_impl::default_mq_producer_impl::DefaultMQProducerImpl;
use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;
use crate::producer::transaction_listener::TransactionListener;

pub trait MQProducerInner: Send + Sync + 'static {
    fn get_publish_topic_list(&self) -> HashSet<CheetahString>;

    fn is_publish_topic_need_update(&self, topic: &CheetahString) -> bool;

    fn get_check_listener(&self) -> Arc<Box<dyn TransactionListener>>;

    fn check_transaction_state(
        &self,
        broker_addr: &CheetahString,
        msg: MessageExt,
        check_request_header: CheckTransactionStateRequestHeader,
    );

    fn update_topic_publish_info(&mut self, topic: impl Into<CheetahString>, info: Option<TopicPublishInfo>);

    fn is_unit_mode(&self) -> bool;
}

#[derive(Clone)]
pub struct MQProducerInnerImpl {
    pub(crate) default_mqproducer_impl_inner: Option<ArcMut<DefaultMQProducerImpl>>,
}

impl MQProducerInnerImpl {
    pub fn get_publish_topic_list(&self) -> HashSet<CheetahString> {
        if let Some(default_mqproducer_impl_inner) = &self.default_mqproducer_impl_inner {
            return default_mqproducer_impl_inner.get_publish_topic_list();
        }
        HashSet::new()
    }

    pub fn is_publish_topic_need_update(&self, topic: &CheetahString) -> bool {
        if let Some(default_mqproducer_impl_inner) = &self.default_mqproducer_impl_inner {
            return default_mqproducer_impl_inner.is_publish_topic_need_update(topic);
        }
        false
    }

    pub fn get_check_listener(&self) -> Arc<Box<dyn TransactionListener>> {
        if let Some(default_mqproducer_impl_inner) = &self.default_mqproducer_impl_inner {
            return default_mqproducer_impl_inner.get_check_listener();
        }
        unreachable!("default_mqproducer_impl_inner is None")
    }

    pub fn check_transaction_state(
        &self,
        addr: &CheetahString,
        msg: MessageExt,
        check_request_header: CheckTransactionStateRequestHeader,
    ) {
        if let Some(default_mqproducer_impl_inner) = &self.default_mqproducer_impl_inner {
            default_mqproducer_impl_inner.check_transaction_state(addr, msg, check_request_header);
        }
    }

    pub fn update_topic_publish_info(&mut self, topic: impl Into<CheetahString>, info: Option<TopicPublishInfo>) {
        if let Some(default_mqproducer_impl_inner) = &mut self.default_mqproducer_impl_inner {
            default_mqproducer_impl_inner.update_topic_publish_info(topic.into(), info);
        }
    }

    pub fn is_unit_mode(&self) -> bool {
        if let Some(default_mqproducer_impl_inner) = &self.default_mqproducer_impl_inner {
            return default_mqproducer_impl_inner.is_unit_mode();
        }
        false
    }
}
