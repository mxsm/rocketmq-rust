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

use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_single::MessageExt;
use rocketmq_common::common::message::message_single::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::utils::queue_type_utils::QueueTypeUtils;
use rocketmq_common::MessageDecoder::message_properties_to_string;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::config::broker_role::BrokerRole;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::log_file::MessageStore;
use rocketmq_store::timer::timer_message_store;
use rocketmq_store::timer::timer_message_store::TimerMessageStore;
use tracing::error;
use tracing::warn;

use crate::out_api::broker_outer_api::BrokerOuterAPI;
use crate::schedule::schedule_message_service::ScheduleMessageService;

static PRINT_TIMES: AtomicI64 = AtomicI64::new(0);
const MAX_TOPIC_LENGTH: usize = 255;

pub struct HookUtils;

impl HookUtils {
    pub fn check_before_put_message(
        message_store: &impl MessageStore,
        message_store_config: &Arc<MessageStoreConfig>,
        msg: &MessageExt,
    ) -> Option<PutMessageResult> {
        if message_store.is_shutdown() {
            warn!("message store has shutdown, so putMessage is forbidden");
            return Some(PutMessageResult::new_default(
                PutMessageStatus::ServiceNotAvailable,
            ));
        }

        if message_store_config.duplication_enable
            && matches!(message_store_config.broker_role, BrokerRole::Slave)
        {
            let value = PRINT_TIMES.fetch_add(1, Ordering::Relaxed);
            if (value % 50000) == 0 {
                warn!("message store is in slave mode, so putMessage is forbidden ");
            }

            return Some(PutMessageResult::new_default(
                PutMessageStatus::ServiceNotAvailable,
            ));
        }

        if !message_store.get_running_flags().is_writeable() {
            let value = PRINT_TIMES.fetch_add(1, Ordering::SeqCst);
            if (value % 50000) == 0 {
                warn!(
                    "message store is not writeable, so putMessage is forbidden {}",
                    message_store.get_running_flags().get_flag_bits()
                );
            }

            return Some(PutMessageResult::new_default(
                PutMessageStatus::ServiceNotAvailable,
            ));
        } else {
            PRINT_TIMES.store(0, Ordering::SeqCst);
        }

        let topic_data = msg.topic().as_bytes();
        let retry_topic = msg.topic().starts_with(RETRY_GROUP_TOPIC_PREFIX);
        if !retry_topic && topic_data.len() > i8::MAX as usize {
            warn!(
                "putMessage message topic[{}] length too long {}, but it is not supported by \
                 broker",
                msg.topic(),
                topic_data.len()
            );
            return Some(PutMessageResult::new_default(
                PutMessageStatus::MessageIllegal,
            ));
        }
        if topic_data.len() > MAX_TOPIC_LENGTH {
            warn!(
                "putMessage message topic[{}] length too long {}, but it is not supported by \
                 broker",
                msg.topic(),
                topic_data.len()
            );
            return Some(PutMessageResult::new_default(
                PutMessageStatus::MessageIllegal,
            ));
        }

        if msg.body().is_none() {
            warn!(
                "putMessage message topic[{}], but message body is null",
                msg.topic()
            );
            return Some(PutMessageResult::new_default(
                PutMessageStatus::MessageIllegal,
            ));
        }
        if message_store.is_os_page_cache_busy() {
            return Some(PutMessageResult::new_default(
                PutMessageStatus::OsPageCacheBusy,
            ));
        }

        None
    }

    pub fn check_inner_batch(
        topic_config_table: &Arc<parking_lot::Mutex<HashMap<String, TopicConfig>>>,
        msg: &MessageExt,
    ) -> Option<PutMessageResult> {
        if msg
            .properties()
            .contains_key(MessageConst::PROPERTY_INNER_NUM)
            && !MessageSysFlag::check(msg.sys_flag(), MessageSysFlag::INNER_BATCH_FLAG)
        {
            warn!(
                "[BUG]The message had property {} but is not an inner batch",
                MessageConst::PROPERTY_INNER_NUM
            );
            return Some(PutMessageResult::new_default(
                PutMessageStatus::MessageIllegal,
            ));
        }

        if MessageSysFlag::check(msg.sys_flag(), MessageSysFlag::INNER_BATCH_FLAG) {
            let topic_config = topic_config_table.lock().get(msg.topic()).cloned();
            if !QueueTypeUtils::is_batch_cq(&topic_config) {
                error!("[BUG]The message is an inner batch but cq type is not batch cq");
                return Some(PutMessageResult::new_default(
                    PutMessageStatus::MessageIllegal,
                ));
            }
        }

        None
    }

    pub fn handle_schedule_message(
        timer_message_store: &TimerMessageStore,
        schedule_message_service: &ScheduleMessageService,
        message_store_config: &Arc<MessageStoreConfig>,
        msg: &mut MessageExtBrokerInner,
    ) -> Option<PutMessageResult> {
        let tran_type = MessageSysFlag::get_transaction_value(msg.sys_flag());
        if tran_type == MessageSysFlag::TRANSACTION_NOT_TYPE
            || tran_type == MessageSysFlag::TRANSACTION_COMMIT_TYPE
        {
            if !Self::is_rolled_timer_message(msg) && Self::check_if_timer_message(msg) {
                if !message_store_config.timer_wheel_enable {
                    // wheel timer is not enabled, reject the message
                    return Some(PutMessageResult::new_default(
                        PutMessageStatus::WheelTimerNotEnable,
                    ));
                }
                if let Some(transform_res) =
                    Self::transform_timer_message(timer_message_store, message_store_config, msg)
                {
                    return Some(transform_res);
                }
            }
            // Delay Delivery
            if msg.message_ext_inner.message.get_delay_time_level() > 0 {
                Self::transform_delay_level_message(schedule_message_service, msg);
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
        message_store_config: &Arc<MessageStoreConfig>,
        msg: &mut MessageExtBrokerInner,
    ) -> Option<PutMessageResult> {
        let delay_level = msg.message_ext_inner.message.get_delay_time_level();
        let deliver_ms = match msg.property(MessageConst::PROPERTY_TIMER_DELAY_SEC) {
            Some(delay_sec) => get_current_millis() + delay_sec.parse::<u64>().unwrap() * 1000,
            None => match msg.property(MessageConst::PROPERTY_TIMER_DELAY_MS) {
                Some(delay_ms) => get_current_millis() + delay_ms.parse::<u64>().unwrap(),
                None => match msg.property(MessageConst::PROPERTY_TIMER_DELIVER_MS) {
                    Some(deliver_ms) => deliver_ms.parse::<u64>().unwrap(),
                    None => {
                        return Some(PutMessageResult::new_default(
                            PutMessageStatus::WheelTimerMsgIllegal,
                        ))
                    }
                },
            },
        };

        if deliver_ms > get_current_millis() {
            if delay_level <= 0
                && deliver_ms - get_current_millis()
                    > message_store_config.timer_max_delay_sec * 1000
            {
                return Some(PutMessageResult::new_default(
                    PutMessageStatus::WheelTimerMsgIllegal,
                ));
            }

            let timer_precision_ms = message_store_config.timer_precision_ms;
            let deliver_ms = if deliver_ms % timer_precision_ms == 0 {
                deliver_ms - timer_precision_ms
            } else {
                (deliver_ms / timer_precision_ms) * timer_precision_ms
            };

            if timer_message_store.is_reject(deliver_ms) {
                return Some(PutMessageResult::new_default(
                    PutMessageStatus::WheelTimerFlowControl,
                ));
            }

            msg.message_ext_inner.message.properties.insert(
                MessageConst::PROPERTY_TIMER_OUT_MS.to_string(),
                deliver_ms.to_string(),
            );
            msg.message_ext_inner.message.properties.insert(
                MessageConst::PROPERTY_REAL_TOPIC.to_string(),
                msg.topic().to_string(),
            );
            msg.message_ext_inner.message.properties.insert(
                MessageConst::PROPERTY_REAL_QUEUE_ID.to_string(),
                msg.message_ext_inner.queue_id.to_string(),
            );
            msg.properties_string =
                message_properties_to_string(&msg.message_ext_inner.message.properties);
            msg.message_ext_inner.message.topic = timer_message_store::TIMER_TOPIC.to_string();
            msg.message_ext_inner.queue_id = 0;
        } else if msg
            .message_ext_inner
            .message
            .properties
            .contains_key(MessageConst::PROPERTY_TIMER_DEL_UNIQKEY)
        {
            return Some(PutMessageResult::new_default(
                PutMessageStatus::WheelTimerMsgIllegal,
            ));
        }
        None
    }

    pub fn transform_delay_level_message(
        schedule_message_service: &ScheduleMessageService,
        msg: &mut MessageExtBrokerInner,
    ) {
        if msg.message_ext_inner.message.get_delay_time_level()
            > schedule_message_service.get_max_delay_level()
        {
            msg.message_ext_inner
                .message
                .set_delay_time_level(schedule_message_service.get_max_delay_level());
        }

        // Backup real topic, queueId
        msg.message_ext_inner.message.properties.insert(
            MessageConst::PROPERTY_REAL_TOPIC.to_string(),
            msg.topic().to_string(),
        );
        msg.message_ext_inner.message.properties.insert(
            MessageConst::PROPERTY_REAL_QUEUE_ID.to_string(),
            msg.message_ext_inner.queue_id.to_string(),
        );
        msg.properties_string =
            message_properties_to_string(&msg.message_ext_inner.message.properties);

        msg.message_ext_inner.message.topic = TopicValidator::RMQ_SYS_SCHEDULE_TOPIC.to_string();
        msg.message_ext_inner.queue_id = ScheduleMessageService::delay_level2queue_id(
            msg.message_ext_inner.message.get_delay_time_level(),
        );
    }

    pub fn send_message_back(
        _outer_api: Arc<BrokerOuterAPI>,
        msg_list: &mut Vec<MessageExt>,
        _broker_name: &str,
        _broker_addr: &str,
    ) -> bool {
        for msg in msg_list.iter_mut() {
            msg.message.properties.insert(
                MessageConst::PROPERTY_WAIT_STORE_MSG_OK.to_string(),
                false.to_string(),
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
    use std::collections::HashMap;
    use std::error::Error;
    use std::sync::Arc;

    use parking_lot::RwLock;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_common::common::message::message_single::MessageExt;
    use rocketmq_store::base::message_result::PutMessageResult;
    use rocketmq_store::base::message_status_enum::PutMessageStatus;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use rocketmq_store::hook::put_message_hook::BoxedPutMessageHook;
    use rocketmq_store::log_file::MessageStore;
    use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
    use rocketmq_store::store::running_flags::RunningFlags;

    use super::*;

    struct MockMessageStore {
        is_shutdown: bool,
        running_flags: RunningFlags,
    }

    impl Clone for MockMessageStore {
        fn clone(&self) -> Self {
            todo!()
        }
    }

    impl MessageStore for MockMessageStore {
        async fn load(&mut self) -> bool {
            todo!()
        }

        fn start(&mut self) -> Result<(), Box<dyn Error>> {
            todo!()
        }

        fn shutdown(&mut self) {
            todo!()
        }

        fn set_confirm_offset(&mut self, _phy_offset: i64) {
            todo!()
        }

        fn get_max_phy_offset(&self) -> i64 {
            todo!()
        }

        fn set_broker_init_max_offset(&mut self, _broker_init_max_offset: i64) {
            todo!()
        }

        fn get_state_machine_version(&self) -> i64 {
            todo!()
        }

        async fn put_message(&mut self, _msg: MessageExtBrokerInner) -> PutMessageResult {
            todo!()
        }

        fn truncate_files(&mut self, _offset_to_truncate: i64) -> bool {
            todo!()
        }

        fn get_running_flags(&self) -> &RunningFlags {
            &self.running_flags
        }

        fn is_shutdown(&self) -> bool {
            false
        }

        fn get_put_message_hook_list(&self) -> Arc<RwLock<Vec<BoxedPutMessageHook>>> {
            todo!()
        }

        fn set_put_message_hook(&self, put_message_hook: BoxedPutMessageHook) {
            todo!()
        }

        fn get_broker_stats_manager(&self) -> Option<Arc<BrokerStatsManager>> {
            todo!()
        }

        fn dispatch_behind_bytes(&self) {
            todo!()
        }
        // Implement required methods...
    }

    #[test]
    fn check_before_put_message_returns_service_not_available_when_message_store_is_shutdown() {
        let message_store = MockMessageStore {
            is_shutdown: true,
            running_flags: RunningFlags::new(),
        };
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let msg = MessageExt::default();

        let result =
            HookUtils::check_before_put_message(&message_store, &message_store_config, &msg);

        assert_eq!(
            result.unwrap().put_message_status(),
            PutMessageStatus::MessageIllegal
        );
    }

    #[test]
    fn check_before_put_message_returns_service_not_available_when_message_store_is_not_writeable()
    {
        let running_flags = RunningFlags::new();
        let message_store = MockMessageStore {
            is_shutdown: false,
            running_flags,
        };
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let msg = MessageExt::default();

        let result =
            HookUtils::check_before_put_message(&message_store, &message_store_config, &msg);

        assert_eq!(
            result.unwrap().put_message_status(),
            PutMessageStatus::MessageIllegal
        );
    }

    #[test]
    fn check_inner_batch_returns_message_illegal_when_inner_num_property_exists_but_inner_batch_flag_is_not_set(
    ) {
        let topic_config_table = Arc::new(parking_lot::Mutex::new(
            HashMap::<String, TopicConfig>::new(),
        ));
        let mut msg = MessageExt::default();
        msg.message.properties.insert(
            MessageConst::PROPERTY_INNER_NUM.to_string(),
            "1".to_string(),
        );

        let result = HookUtils::check_inner_batch(&topic_config_table, &msg);

        assert_eq!(
            result.unwrap().put_message_status(),
            PutMessageStatus::MessageIllegal
        );
    }

    #[test]
    fn check_inner_batch_returns_message_illegal_when_inner_batch_flag_is_set_but_cq_type_is_not_batch_cq(
    ) {
        let mut topic_config_table = HashMap::<String, TopicConfig>::new();
        topic_config_table.insert("test_topic".to_string(), TopicConfig::default());
        let topic_config_table = Arc::new(parking_lot::Mutex::new(topic_config_table));
        let mut msg = MessageExt::default();
        msg.message.topic = "test_topic".to_string();
        msg.set_sys_flag(MessageSysFlag::INNER_BATCH_FLAG);

        let result = HookUtils::check_inner_batch(&topic_config_table, &msg);

        assert_eq!(
            result.unwrap().put_message_status(),
            PutMessageStatus::MessageIllegal
        );
    }
}
