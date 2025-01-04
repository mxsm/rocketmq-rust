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
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::pop_ack_constants::PopAckConstants;
use rocketmq_common::common::TopicFilterType;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_status_enum::AppendMessageStatus;
use rocketmq_store::log_file::MessageStore;
use rocketmq_store::pop::pop_check_point::PopCheckPoint;

use crate::failover::escape_bridge::EscapeBridge;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

pub struct PopReviveService<MS> {
    queue_id: i32,
    revive_topic: CheetahString,
    current_revive_message_timestamp: i64,
    should_run_pop_revive: bool,
    inflight_revive_request_map: Arc<parking_lot::Mutex<BTreeMap<PopCheckPoint, (i64, bool)>>>,
    revive_offset: i64,
    ck_rewrite_intervals_in_seconds: [i32; 17],
    broker_config: Arc<BrokerConfig>,
    topic_config_manager: TopicConfigManager,
    consumer_offset_manager: Arc<ConsumerOffsetManager>,
    escape_bridge: ArcMut<EscapeBridge<MS>>,
}
impl<MS: MessageStore> PopReviveService<MS> {
    pub fn new(
        revive_topic: CheetahString,
        queue_id: i32,
        revive_offset: i64,
        broker_config: Arc<BrokerConfig>,
        topic_config_manager: TopicConfigManager,
        consumer_offset_manager: Arc<ConsumerOffsetManager>,
        escape_bridge: ArcMut<EscapeBridge<MS>>,
    ) -> Self {
        Self {
            queue_id,
            revive_topic,
            current_revive_message_timestamp: -1,
            should_run_pop_revive: false,
            inflight_revive_request_map: Arc::new(Default::default()),
            revive_offset,
            ck_rewrite_intervals_in_seconds: [
                10, 20, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200,
            ],
            broker_config,
            topic_config_manager,
            consumer_offset_manager,
            escape_bridge,
        }
    }

    async fn revive_retry(
        &mut self,
        pop_check_point: &PopCheckPoint,
        message_ext: &MessageExt,
    ) -> bool {
        let mut msg_inner = MessageExtBrokerInner::default();
        if pop_check_point
            .topic
            .starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX)
        {
            msg_inner.set_topic(CheetahString::from_string(
                KeyBuilder::build_pop_retry_topic(
                    pop_check_point.topic.as_str(),
                    pop_check_point.cid.as_str(),
                    self.broker_config.enable_retry_topic_v2,
                ),
            ));
        } else {
            msg_inner.set_topic(pop_check_point.topic.clone());
        }
        if let Some(bytes) = message_ext.body() {
            msg_inner.set_body(bytes);
        }
        msg_inner.message_ext_inner.queue_id = 0;
        if let Some(tags) = message_ext.get_tags() {
            msg_inner.set_tags(tags);
        } else {
            MessageAccessor::set_properties(&mut msg_inner, HashMap::new());
        }
        msg_inner.message_ext_inner.born_timestamp = message_ext.born_timestamp;
        msg_inner.set_flag(message_ext.get_flag());
        msg_inner.message_ext_inner.sys_flag = message_ext.sys_flag;
        msg_inner.message_ext_inner.born_host = message_ext.born_host;
        msg_inner.message_ext_inner.store_host = message_ext.store_host;
        msg_inner.message_ext_inner.reconsume_times = message_ext.reconsume_times + 1;
        MessageAccessor::set_properties(&mut msg_inner, message_ext.properties().clone());
        if message_ext.reconsume_times() == 0
            || msg_inner
                .get_property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_FIRST_POP_TIME,
                ))
                .is_none()
        {
            msg_inner.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_FIRST_POP_TIME),
                CheetahString::from(pop_check_point.pop_time.to_string()),
            );
        }
        msg_inner.properties_string =
            message_decoder::message_properties_to_string(msg_inner.get_properties());
        self.add_retry_topic_if_not_exist(msg_inner.get_topic(), &pop_check_point.cid);
        let put_message_result = self
            .escape_bridge
            .put_message_to_specific_queue(msg_inner)
            .await;
        if put_message_result.append_message_result().is_none()
            || put_message_result.append_message_result().unwrap().status
                != AppendMessageStatus::PutOk
        {
            return false;
        }
        true
    }

    pub fn add_retry_topic_if_not_exist(
        &mut self,
        topic: &CheetahString,
        consumer_group: &CheetahString,
    ) {
        if let Some(_topic_config) = self.topic_config_manager.select_topic_config(topic) {
            return;
        }
        let mut topic_config = TopicConfig::new(topic.clone());
        topic_config.read_queue_nums = PopAckConstants::RETRY_QUEUE_NUM as u32;
        topic_config.write_queue_nums = PopAckConstants::RETRY_QUEUE_NUM as u32;
        topic_config.topic_filter_type = TopicFilterType::SingleTag;
        topic_config.perm = 6;
        topic_config.topic_sys_flag = 0;
        self.topic_config_manager
            .update_topic_config(&mut topic_config);
        self.init_pop_retry_offset(topic, consumer_group);
    }

    fn init_pop_retry_offset(&mut self, topic: &CheetahString, consumer_group: &CheetahString) {
        let offset = self
            .consumer_offset_manager
            .query_offset(consumer_group, topic, 0);
        if offset < 0 {
            self.consumer_offset_manager.commit_offset(
                "initPopRetryOffset".into(),
                consumer_group,
                topic,
                0,
                0,
            );
        }
    }
}

struct ConsumeReviveObj {
    map: HashMap<CheetahString, PopCheckPoint>,
    sort_list: Option<Vec<PopCheckPoint>>,
    old_offset: i64,
    end_time: i64,
    new_offset: i64,
}

impl ConsumeReviveObj {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            sort_list: None,
            old_offset: 0,
            end_time: 0,
            new_offset: 0,
        }
    }

    fn gen_sort_list(&mut self) -> &Vec<PopCheckPoint> {
        if self.sort_list.is_none() {
            let mut list: Vec<PopCheckPoint> = self.map.values().cloned().collect();
            list.sort_by_key(|ck| ck.revive_offset);
            self.sort_list = Some(list);
        }
        self.sort_list.as_ref().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_store::pop::pop_check_point::PopCheckPoint;

    use super::*;

    #[test]
    fn new_initializes_correctly() {
        let obj = ConsumeReviveObj::new();
        assert!(obj.map.is_empty());
        assert!(obj.sort_list.is_none());
        assert_eq!(obj.old_offset, 0);
        assert_eq!(obj.end_time, 0);
        assert_eq!(obj.new_offset, 0);
    }

    #[test]
    fn gen_sort_list_creates_sorted_list() {
        let mut obj = ConsumeReviveObj::new();
        let ck1 = PopCheckPoint {
            revive_offset: 10,
            ..Default::default()
        };
        let ck2 = PopCheckPoint {
            revive_offset: 5,
            ..Default::default()
        };
        obj.map.insert(CheetahString::from("key1"), ck1.clone());
        obj.map.insert(CheetahString::from("key2"), ck2.clone());

        let sorted_list = obj.gen_sort_list();
        assert_eq!(sorted_list.len(), 2);
        assert_eq!(sorted_list[0].revive_offset, 5);
        assert_eq!(sorted_list[1].revive_offset, 10);
    }

    #[test]
    fn gen_sort_list_returns_existing_list_if_already_sorted() {
        let mut obj = ConsumeReviveObj::new();
        let ck1 = PopCheckPoint {
            revive_offset: 10,
            ..Default::default()
        };
        let ck2 = PopCheckPoint {
            revive_offset: 5,
            ..Default::default()
        };
        obj.map.insert(CheetahString::from("key1"), ck1.clone());
        obj.map.insert(CheetahString::from("key2"), ck2.clone());

        let _ = obj.gen_sort_list();
        let sorted_list = obj.gen_sort_list();
        assert_eq!(sorted_list.len(), 2);
        assert_eq!(sorted_list[0].revive_offset, 5);
        assert_eq!(sorted_list[1].revive_offset, 10);
    }
}
