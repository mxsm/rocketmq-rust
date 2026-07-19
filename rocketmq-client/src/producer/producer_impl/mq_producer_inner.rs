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
use std::sync::Weak;

use crate::producer::producer_impl::default_mq_producer_impl::DefaultMQProducerImpl;
use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;
use crate::producer::transaction_listener::ArcTransactionListener;
use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_remoting::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;

pub trait MQProducerInner: Send + Sync + 'static {
    fn get_publish_topic_list(&self) -> HashSet<CheetahString>;

    fn is_publish_topic_need_update(&self, topic: &CheetahString) -> bool;

    fn get_check_listener(&self) -> Option<ArcTransactionListener>;

    fn check_transaction_state(
        &self,
        broker_addr: &CheetahString,
        msg: MessageExt,
        check_request_header: CheckTransactionStateRequestHeader,
    );

    fn update_topic_publish_info(&self, topic: impl Into<CheetahString>, info: Option<TopicPublishInfo>);

    fn is_unit_mode(&self) -> bool;
}

#[derive(Clone)]
pub struct MQProducerInnerImpl {
    producer: Weak<DefaultMQProducerImpl>,
}

impl MQProducerInnerImpl {
    pub(crate) fn new(producer: Weak<DefaultMQProducerImpl>) -> Self {
        Self { producer }
    }

    #[inline]
    fn producer(&self) -> Option<Arc<DefaultMQProducerImpl>> {
        self.producer.upgrade()
    }

    pub(crate) fn is_alive(&self) -> bool {
        self.producer().is_some()
    }

    pub(crate) fn points_to_same_producer(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.producer, &other.producer)
    }

    pub fn get_publish_topic_list(&self) -> HashSet<CheetahString> {
        if let Some(producer) = self.producer() {
            return producer.get_publish_topic_list();
        }
        HashSet::new()
    }

    pub fn is_publish_topic_need_update(&self, topic: &CheetahString) -> bool {
        if let Some(producer) = self.producer() {
            return producer.is_publish_topic_need_update(topic);
        }
        false
    }

    pub fn get_check_listener(&self) -> Option<ArcTransactionListener> {
        if let Some(producer) = self.producer() {
            return producer.get_check_listener();
        }
        None
    }

    pub fn check_transaction_state(
        &self,
        addr: &CheetahString,
        msg: MessageExt,
        check_request_header: CheckTransactionStateRequestHeader,
    ) {
        if let Some(producer) = self.producer() {
            producer.check_transaction_state(addr, msg, check_request_header);
        }
    }

    pub fn update_topic_publish_info(&self, topic: impl Into<CheetahString>, info: Option<TopicPublishInfo>) {
        if let Some(producer) = self.producer() {
            producer.update_topic_publish_info(topic.into(), info);
        }
    }

    pub fn is_unit_mode(&self) -> bool {
        if let Some(producer) = self.producer() {
            return producer.is_unit_mode();
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::client_config::ClientConfig;
    use crate::producer::default_mq_producer::ProducerConfig;

    #[test]
    fn dead_inner_returns_safe_defaults_instead_of_panicking() {
        let inner = MQProducerInnerImpl::new(Weak::new());

        assert!(!inner.is_alive());
        assert!(inner.get_publish_topic_list().is_empty());
        assert!(!inner.is_publish_topic_need_update(&CheetahString::from_static_str("TopicTest")));
        assert!(inner.get_check_listener().is_none());
        assert!(!inner.is_unit_mode());
        inner.update_topic_publish_info(CheetahString::from_static_str("TopicTest"), None);
    }

    #[test]
    fn weak_registry_entry_does_not_keep_producer_root_alive() {
        let producer = Arc::new(DefaultMQProducerImpl::new(
            ClientConfig::default(),
            ProducerConfig::default(),
            None,
        ));
        let inner = MQProducerInnerImpl::new(Arc::downgrade(&producer));

        assert!(inner.is_alive());
        drop(producer);

        assert!(!inner.is_alive());
        assert!(inner.get_publish_topic_list().is_empty());
    }

    #[test]
    fn weak_registry_entries_preserve_root_identity() {
        let producer = Arc::new(DefaultMQProducerImpl::new(
            ClientConfig::default(),
            ProducerConfig::default(),
            None,
        ));
        let same = MQProducerInnerImpl::new(Arc::downgrade(&producer));
        let same_clone = same.clone();
        let other_producer = Arc::new(DefaultMQProducerImpl::new(
            ClientConfig::default(),
            ProducerConfig::default(),
            None,
        ));
        let other = MQProducerInnerImpl::new(Arc::downgrade(&other_producer));

        assert!(same.points_to_same_producer(&same_clone));
        assert!(!same.points_to_same_producer(&other));
    }
}
