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
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::MessageDecoder;

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
        msg_inner.set_wait_store_msg_ok(false);
        msg_inner.message_ext_inner.set_msg_id(msg_ext.msg_id().clone());
        msg_inner.set_topic(
            msg_ext
                .get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC))
                .unwrap_or_default(),
        );
        if let Some(body) = msg_ext.get_body() {
            msg_inner.set_body(body.clone());
        }
        if let Some(real_queue_id_str) = msg_ext.get_property(&CheetahString::from_static_str("REAL_QUEUE_ID")) {
            if let Ok(value) = real_queue_id_str.parse::<i32>() {
                msg_inner.message_ext_inner.set_queue_id(value);
            }
        }
        msg_inner.set_flag(msg_ext.get_flag());
        msg_inner.tags_code =
            MessageExtBrokerInner::tags_string_to_tags_code(msg_inner.get_tags().unwrap_or_default().as_str());
        msg_inner.message_ext_inner.set_born_timestamp(msg_ext.born_timestamp);
        msg_inner.message_ext_inner.set_born_host(msg_ext.born_host);

        if let Some(transaction_id) = msg_ext.get_property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
        )) {
            msg_inner.set_transaction_id(transaction_id);
        }

        MessageAccessor::set_properties(&mut msg_inner, msg_ext.get_properties().clone());
        MessageAccessor::put_property(
            &mut msg_inner,
            CheetahString::from_static_str(MessageConst::PROPERTY_TRANSACTION_PREPARED),
            CheetahString::from_static_str("true"),
        );
        MessageAccessor::clear_property(&mut msg_inner, MessageConst::PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        MessageAccessor::clear_property(&mut msg_inner, MessageConst::PROPERTY_REAL_QUEUE_ID);
        msg_inner.properties_string = MessageDecoder::message_properties_to_string(msg_inner.get_properties());

        let mut sys_flag = msg_ext.sys_flag();
        sys_flag |= MessageSysFlag::TRANSACTION_PREPARED_TYPE;
        msg_inner.message_ext_inner.set_sys_flag(sys_flag);

        msg_inner
    }

    pub fn get_immunity_time(check_immunity_time_str: &str, transaction_timeout: u64) -> u64 {
        let mut check_immunity_time = 0;

        if let Ok(parsed_time) = check_immunity_time_str.parse::<u64>() {
            check_immunity_time = parsed_time * 1000;
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
                .get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC))
                .unwrap_or_default()
        );
        assert_eq!(msg_inner.get_body(), msg_ext.get_body());
        assert_eq!(msg_inner.get_flag(), msg_ext.get_flag());
        assert_eq!(
            msg_inner.get_transaction_id(),
            msg_ext
                .get_property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX
                ))
                .as_ref()
        );
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
}
