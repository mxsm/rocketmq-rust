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

use cheetah_string::CheetahString;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::mix_all;
use rocketmq_model::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_model::common::topic::TopicValidator;

pub struct TransactionalMessageUtil;

impl TransactionalMessageUtil {
    pub const REMOVE_TAG: &'static str = "d";
    pub const CHARSET: &'static str = "UTF-8";
    pub const OFFSET_SEPARATOR: &'static str = ",";
    pub const TRANSACTION_ID: &'static str = "__transactionId__";

    #[inline(always)]
    pub const fn build_op_topic() -> &'static str {
        TopicValidator::RMQ_SYS_TRANS_OP_HALF_TOPIC
    }
    #[inline(always)]
    pub const fn build_half_topic() -> &'static str {
        TopicValidator::RMQ_SYS_TRANS_HALF_TOPIC
    }
    #[inline(always)]
    pub const fn build_consumer_group() -> &'static str {
        mix_all::CID_SYS_RMQ_TRANS
    }

    pub fn build_transactional_message_from_half_message(msg_ext: &MessageExt) -> MessageExtBrokerInner {
        let mut msg_inner = MessageExtBrokerInner::default();
        msg_inner.message_ext_inner.set_msg_id(msg_ext.msg_id().clone());
        msg_inner.set_topic(
            msg_ext
                .property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC))
                .unwrap_or_default(),
        );
        if let Some(body) = msg_ext.get_body() {
            msg_inner.set_body(body.clone());
        }
        if let Some(real_queue_id_str) =
            msg_ext.property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_QUEUE_ID))
        {
            if let Ok(value) = real_queue_id_str.parse::<i32>() {
                msg_inner.message_ext_inner.set_queue_id(value);
            }
        }
        msg_inner.set_flag(msg_ext.get_flag());
        msg_inner.tags_code =
            MessageExtBrokerInner::tags_string_to_tags_code(msg_inner.tags().unwrap_or_default().as_str());
        msg_inner.message_ext_inner.set_born_timestamp(msg_ext.born_timestamp);
        msg_inner.message_ext_inner.set_born_host(msg_ext.born_host);

        if let Some(transaction_id) = msg_ext.property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
        )) {
            msg_inner.set_transaction_id(transaction_id);
        }

        // The half message's properties replace the map, as in Java, so no wait flag is set
        // earlier. The edits go to the map and the string is encoded once.
        let ext = &mut msg_inner.message_ext_inner;
        ext.set_properties(msg_ext.get_properties().clone());
        ext.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_TRANSACTION_PREPARED),
            CheetahString::from_static_str("true"),
        );
        ext.clear_property(MessageConst::PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        ext.clear_property(MessageConst::PROPERTY_REAL_QUEUE_ID);
        msg_inner.refresh_properties_string();

        let mut sys_flag = msg_ext.sys_flag();
        sys_flag |= MessageSysFlag::TRANSACTION_PREPARED_TYPE;
        msg_inner.message_ext_inner.set_sys_flag(sys_flag);

        msg_inner
    }

    pub fn get_immunity_time(check_immunity_time_str: &str, transaction_timeout: u64) -> u64 {
        let mut check_immunity_time = 0;

        if let Ok(parsed_time) = check_immunity_time_str.parse::<u64>() {
            check_immunity_time = parsed_time.saturating_mul(1000);
        }

        if check_immunity_time < transaction_timeout {
            check_immunity_time = transaction_timeout;
        }
        check_immunity_time
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_op_topic_returns_correct_topic() {
        assert_eq!(
            TransactionalMessageUtil::build_op_topic(),
            TopicValidator::RMQ_SYS_TRANS_OP_HALF_TOPIC
        );
    }

    #[test]
    fn build_half_topic_returns_correct_topic() {
        assert_eq!(
            TransactionalMessageUtil::build_half_topic(),
            TopicValidator::RMQ_SYS_TRANS_HALF_TOPIC
        );
    }

    #[test]
    fn build_consumer_group_returns_correct_group() {
        assert_eq!(
            TransactionalMessageUtil::build_consumer_group(),
            mix_all::CID_SYS_RMQ_TRANS
        );
    }

    #[test]
    fn build_transactional_message_from_half_message_with_valid_message() {
        let msg_ext = MessageExt::default();
        let msg_inner = TransactionalMessageUtil::build_transactional_message_from_half_message(&msg_ext);
        assert_eq!(msg_inner.message_ext_inner.msg_id(), msg_ext.msg_id());
        assert_eq!(
            msg_inner.get_topic(),
            &msg_ext
                .property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC))
                .unwrap_or_default()
        );
        assert_eq!(msg_inner.get_body(), msg_ext.get_body());
        assert_eq!(msg_inner.get_flag(), msg_ext.get_flag());
        assert_eq!(
            msg_inner.get_transaction_id(),
            msg_ext
                .property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX
                ))
                .as_ref()
        );
    }

    #[test]
    fn transactional_message_from_half_message_stores_the_edited_properties() {
        let mut msg_ext = MessageExt::default();
        for (name, value) in [
            (MessageConst::PROPERTY_TAGS, "TagA"),
            (MessageConst::PROPERTY_REAL_TOPIC, "orders"),
            (MessageConst::PROPERTY_REAL_QUEUE_ID, "3"),
            (MessageConst::PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET, "7"),
        ] {
            msg_ext.put_property(
                CheetahString::from_static_str(name),
                CheetahString::from_static_str(value),
            );
        }

        let msg_inner = TransactionalMessageUtil::build_transactional_message_from_half_message(&msg_ext);

        let stored = rocketmq_protocol::common::message::message_decoder::string_to_message_properties(Some(
            &msg_inner.properties_string,
        ));
        assert_eq!(&stored, msg_inner.get_properties());
        assert_eq!(stored[MessageConst::PROPERTY_TRANSACTION_PREPARED], "true");
        assert_eq!(stored[MessageConst::PROPERTY_TAGS], "TagA");
        assert!(!stored.contains_key(MessageConst::PROPERTY_REAL_QUEUE_ID));
        assert!(!stored.contains_key(MessageConst::PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET));
        assert_eq!(msg_inner.get_topic(), "orders");
        assert_eq!(msg_inner.message_ext_inner.queue_id, 3);
    }

    #[test]
    fn get_immunity_time_with_valid_time_string() {
        let immunity_time = TransactionalMessageUtil::get_immunity_time("10", 5000);
        assert_eq!(immunity_time, 10000);
    }

    #[test]
    fn get_immunity_time_with_invalid_time_string() {
        let immunity_time = TransactionalMessageUtil::get_immunity_time("invalid", 5000);
        assert_eq!(immunity_time, 5000);
    }

    #[test]
    fn get_immunity_time_with_time_less_than_transaction_timeout() {
        let immunity_time = TransactionalMessageUtil::get_immunity_time("3", 5000);
        assert_eq!(immunity_time, 5000);
    }

    #[test]
    fn get_immunity_time_saturates_on_overflowing_value() {
        let immunity_time = TransactionalMessageUtil::get_immunity_time("18446744073709551615", 5000);
        assert_eq!(immunity_time, u64::MAX);
    }
}
