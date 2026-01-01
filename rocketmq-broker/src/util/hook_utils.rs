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

use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::utils::queue_type_utils::QueueTypeUtils;
use rocketmq_common::MessageDecoder::message_properties_to_string;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::timer::timer_message_store;
use rocketmq_store::timer::timer_message_store::TimerMessageStore;
use tracing::error;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::out_api::broker_outer_api::BrokerOuterAPI;
use crate::schedule::schedule_message_service::delay_level_to_queue_id;

static PRINT_TIMES: AtomicI64 = AtomicI64::new(0);
const MAX_TOPIC_LENGTH: usize = 255;

pub struct HookUtils;

impl HookUtils {
    pub fn check_before_put_message(
        message_store: &impl MessageStore,
        message_store_config: &Arc<MessageStoreConfig>,
        msg: &mut dyn MessageTrait,
    ) -> Option<PutMessageResult> {
        if message_store.is_shutdown() {
            warn!("message store has shutdown, so putMessage is forbidden");
            return Some(PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable));
        }

        if message_store_config.duplication_enable && matches!(message_store_config.broker_role, BrokerRole::Slave) {
            let value = PRINT_TIMES.fetch_add(1, Ordering::Relaxed);
            if (value % 50000) == 0 {
                warn!("message store is in slave mode, so putMessage is forbidden ");
            }

            return Some(PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable));
        }

        if !message_store.get_running_flags().is_writeable() {
            let value = PRINT_TIMES.fetch_add(1, Ordering::SeqCst);
            if (value % 50000) == 0 {
                warn!(
                    "message store is not writeable, so putMessage is forbidden {}",
                    message_store.get_running_flags().get_flag_bits()
                );
            }

            return Some(PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable));
        } else {
            PRINT_TIMES.store(0, Ordering::SeqCst);
        }

        let topic_data = msg.get_topic().as_bytes();
        let retry_topic = msg.get_topic().starts_with(RETRY_GROUP_TOPIC_PREFIX);
        if !retry_topic && topic_data.len() > i8::MAX as usize {
            warn!(
                "putMessage message topic[{}] length too long {}, but it is not supported by broker",
                msg.get_topic(),
                topic_data.len()
            );
            return Some(PutMessageResult::new_default(PutMessageStatus::MessageIllegal));
        }
        if topic_data.len() > MAX_TOPIC_LENGTH {
            warn!(
                "putMessage message topic[{}] length too long {}, but it is not supported by broker",
                msg.get_topic(),
                topic_data.len()
            );
            return Some(PutMessageResult::new_default(PutMessageStatus::MessageIllegal));
        }

        if msg.get_body().is_none() {
            warn!(
                "putMessage message topic[{}], but message body is null",
                msg.get_topic()
            );
            return Some(PutMessageResult::new_default(PutMessageStatus::MessageIllegal));
        }
        if message_store.is_os_page_cache_busy() {
            return Some(PutMessageResult::new_default(PutMessageStatus::OsPageCacheBusy));
        }

        None
    }

    pub fn check_inner_batch(
        topic_config_table: &Arc<DashMap<CheetahString, ArcMut<TopicConfig>>>,
        msg: &MessageExt,
    ) -> Option<PutMessageResult> {
        if msg.properties().contains_key(MessageConst::PROPERTY_INNER_NUM)
            && !MessageSysFlag::check(msg.sys_flag(), MessageSysFlag::INNER_BATCH_FLAG)
        {
            warn!(
                "[BUG]The message had property {} but is not an inner batch",
                MessageConst::PROPERTY_INNER_NUM
            );
            return Some(PutMessageResult::new_default(PutMessageStatus::MessageIllegal));
        }

        if MessageSysFlag::check(msg.sys_flag(), MessageSysFlag::INNER_BATCH_FLAG) {
            let topic_config_ref = topic_config_table.get(msg.topic());
            let topic_config = topic_config_ref.as_deref();
            if !QueueTypeUtils::is_batch_cq_arc_mut(topic_config) {
                error!("[BUG]The message is an inner batch but cq type is not batch cq");
                return Some(PutMessageResult::new_default(PutMessageStatus::MessageIllegal));
            }
        }

        None
    }

    pub fn handle_schedule_message<MS: MessageStore>(
        broker_runtime_inner: &ArcMut<BrokerRuntimeInner<MS>>,
        msg: &mut MessageExtBrokerInner,
    ) -> Option<PutMessageResult> {
        let tran_type = MessageSysFlag::get_transaction_value(msg.sys_flag());
        if tran_type == MessageSysFlag::TRANSACTION_NOT_TYPE || tran_type == MessageSysFlag::TRANSACTION_COMMIT_TYPE {
            if !Self::is_rolled_timer_message(msg) && Self::check_if_timer_message(msg) {
                if !broker_runtime_inner.message_store_config().timer_wheel_enable {
                    // wheel timer is not enabled, reject the message
                    return Some(PutMessageResult::new_default(PutMessageStatus::WheelTimerNotEnable));
                }
                if let Some(transform_res) = Self::transform_timer_message(
                    broker_runtime_inner.timer_message_store_unchecked(),
                    broker_runtime_inner.message_store_config(),
                    msg,
                ) {
                    return Some(transform_res);
                }
            }
            // Delay Delivery
            if msg.message_ext_inner.message.get_delay_time_level() > 0 {
                Self::transform_delay_level_message(broker_runtime_inner, msg);
            }
        }
        None
    }

    fn is_rolled_timer_message(msg: &MessageExtBrokerInner) -> bool {
        timer_message_store::TIMER_TOPIC == msg.topic()
    }

    pub fn check_if_timer_message(msg: &mut MessageExtBrokerInner) -> bool {
        if msg.message_ext_inner.message.get_delay_time_level() > 0 {
            if msg
                .message_ext_inner
                .properties()
                .contains_key(MessageConst::PROPERTY_TIMER_DELIVER_MS)
            {
                msg.message_ext_inner
                    .message
                    .properties
                    .remove(MessageConst::PROPERTY_TIMER_DELIVER_MS);
            }
            if msg
                .message_ext_inner
                .properties()
                .contains_key(MessageConst::PROPERTY_TIMER_DELAY_SEC)
            {
                msg.message_ext_inner
                    .message
                    .properties
                    .remove(MessageConst::PROPERTY_TIMER_DELAY_SEC);
            }
            if msg
                .message_ext_inner
                .properties()
                .contains_key(MessageConst::PROPERTY_TIMER_DELAY_MS)
            {
                msg.message_ext_inner
                    .message
                    .properties
                    .remove(MessageConst::PROPERTY_TIMER_DELAY_MS);
            }
            return false;
        }
        if timer_message_store::TIMER_TOPIC == msg.topic()
            || msg
                .message_ext_inner
                .properties()
                .contains_key(MessageConst::PROPERTY_TIMER_OUT_MS)
        {
            return false;
        }
        msg.message_ext_inner
            .properties()
            .contains_key(MessageConst::PROPERTY_TIMER_DELIVER_MS)
            || msg
                .message_ext_inner
                .properties()
                .contains_key(MessageConst::PROPERTY_TIMER_DELAY_MS)
            || msg
                .message_ext_inner
                .properties()
                .contains_key(MessageConst::PROPERTY_TIMER_DELAY_SEC)
    }

    fn transform_timer_message(
        timer_message_store: &TimerMessageStore,
        message_store_config: &MessageStoreConfig,
        msg: &mut MessageExtBrokerInner,
    ) -> Option<PutMessageResult> {
        let delay_level = msg.message_ext_inner.message.get_delay_time_level();
        let deliver_ms = match msg.property(MessageConst::PROPERTY_TIMER_DELAY_SEC) {
            Some(delay_sec) => get_current_millis() + delay_sec.parse::<u64>().unwrap() * 1000,
            None => match msg.property(MessageConst::PROPERTY_TIMER_DELAY_MS) {
                Some(delay_ms) => get_current_millis() + delay_ms.parse::<u64>().unwrap(),
                None => match msg.property(MessageConst::PROPERTY_TIMER_DELIVER_MS) {
                    Some(deliver_ms) => deliver_ms.parse::<u64>().unwrap(),
                    None => return Some(PutMessageResult::new_default(PutMessageStatus::WheelTimerMsgIllegal)),
                },
            },
        };

        if deliver_ms > get_current_millis() {
            if delay_level <= 0 && deliver_ms - get_current_millis() > message_store_config.timer_max_delay_sec * 1000 {
                return Some(PutMessageResult::new_default(PutMessageStatus::WheelTimerMsgIllegal));
            }

            let timer_precision_ms = message_store_config.timer_precision_ms;
            let deliver_ms = if deliver_ms % timer_precision_ms == 0 {
                deliver_ms - timer_precision_ms
            } else {
                (deliver_ms / timer_precision_ms) * timer_precision_ms
            };

            if timer_message_store.is_reject(deliver_ms) {
                return Some(PutMessageResult::new_default(PutMessageStatus::WheelTimerFlowControl));
            }

            msg.message_ext_inner.message.properties.insert(
                CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_OUT_MS),
                CheetahString::from_string(deliver_ms.to_string()),
            );
            msg.message_ext_inner.message.properties.insert(
                CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC),
                CheetahString::from_slice(msg.topic()),
            );
            msg.message_ext_inner.message.properties.insert(
                CheetahString::from_static_str(MessageConst::PROPERTY_REAL_QUEUE_ID),
                CheetahString::from_string(msg.message_ext_inner.queue_id.to_string()),
            );
            msg.properties_string = message_properties_to_string(&msg.message_ext_inner.message.properties);
            msg.message_ext_inner.message.topic = CheetahString::from_static_str(timer_message_store::TIMER_TOPIC);
            msg.message_ext_inner.queue_id = 0;
        } else if msg
            .message_ext_inner
            .message
            .properties
            .contains_key(MessageConst::PROPERTY_TIMER_DEL_UNIQKEY)
        {
            return Some(PutMessageResult::new_default(PutMessageStatus::WheelTimerMsgIllegal));
        }
        None
    }

    /// Transforms a message with delay level into a scheduled message format.
    ///
    /// This function prepares a message for delayed processing by:
    /// 1. Capping the delay time level to the maximum supported level if necessary
    /// 2. Backing up the original topic and queue ID in message properties
    /// 3. Updating the message's topic to the system schedule topic
    /// 4. Setting the queue ID based on the delay time level
    ///
    /// # Arguments
    ///
    /// * `broker_runtime_inner` - Reference to the broker runtime that contains the schedule
    ///   message service
    /// * `msg` - Mutable reference to the message to be transformed
    ///
    /// # Type Parameters
    ///
    /// * `MS` - A type that implements the `MessageStore` trait
    pub fn transform_delay_level_message<MS: MessageStore>(
        broker_runtime_inner: &ArcMut<BrokerRuntimeInner<MS>>,
        msg: &mut MessageExtBrokerInner,
    ) {
        let schedule_message_service = broker_runtime_inner.schedule_message_service();
        if msg.message_ext_inner.message.get_delay_time_level() > schedule_message_service.get_max_delay_level() {
            msg.message_ext_inner
                .message
                .set_delay_time_level(schedule_message_service.get_max_delay_level());
        }

        // Backup real topic, queueId
        msg.message_ext_inner.message.properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC), // real topic: %RETRY% + consumerGroup
            CheetahString::from_string(msg.topic().to_string()),
        );
        msg.message_ext_inner.message.properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_REAL_QUEUE_ID),
            CheetahString::from_string(msg.message_ext_inner.queue_id.to_string()),
        );
        msg.properties_string = message_properties_to_string(&msg.message_ext_inner.message.properties);

        msg.message_ext_inner.message.topic = CheetahString::from_static_str(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC);
        msg.message_ext_inner.queue_id = delay_level_to_queue_id(msg.message_ext_inner.message.get_delay_time_level());
    }

    pub fn send_message_back(
        _outer_api: Arc<BrokerOuterAPI>,
        msg_list: &mut Vec<MessageExt>,
        _broker_name: &str,
        _broker_addr: &str,
    ) -> bool {
        for msg in msg_list.iter_mut() {
            msg.message.properties.insert(
                CheetahString::from_static_str(MessageConst::PROPERTY_WAIT_STORE_MSG_OK),
                CheetahString::from_string(false.to_string()),
            );
            /*            if let Err(e) = outer_api.send_message_to_specific_broker(
                broker_addr,
                broker_name,
                msg,
                "InnerSendMessageBackGroup",
                3000,
            ) {
                error!(
                    "send message back to broker {} addr {} failed: {:?}",
                    broker_name, broker_addr, e
                );
                return false;
            }*/
        }
        msg_list.clear();
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_common::common::message::message_ext::MessageExt;
    use rocketmq_store::base::message_status_enum::PutMessageStatus;

    use super::*;

    #[test]
    fn check_inner_batch_returns_message_illegal_when_inner_batch_flag_is_set_but_cq_type_is_not_batch_cq() {
        let topic_config_table = DashMap::new();
        topic_config_table.insert(
            CheetahString::from_static_str("test_topic"),
            ArcMut::new(TopicConfig::default()),
        );
        let topic_config_table = Arc::new(topic_config_table);
        let mut msg = MessageExt::default();
        msg.message.topic = "test_topic".into();
        msg.set_sys_flag(MessageSysFlag::INNER_BATCH_FLAG);

        let result = HookUtils::check_inner_batch(&topic_config_table, &msg);

        assert_eq!(result.unwrap().put_message_status(), PutMessageStatus::MessageIllegal);
    }
}
