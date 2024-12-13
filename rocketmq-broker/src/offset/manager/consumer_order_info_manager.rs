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
use std::ops::Deref;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config_manager::ConfigManager;
use serde::Deserialize;
use serde::Serialize;
use tracing::warn;

use crate::broker_path_config_helper::get_consumer_order_info_path;
use crate::offset::manager::consumer_order_info_lock_manager::ConsumerOrderInfoLockManager;

const TOPIC_GROUP_SEPARATOR: &str = "@";
const CLEAN_SPAN_FROM_LAST: u64 = 24 * 3600 * 1000;

#[derive(Default)]
pub(crate) struct ConsumerOrderInfoManager {
    pub(crate) broker_config: Arc<BrokerConfig>,
    pub(crate) consumer_order_info_wrapper: parking_lot::Mutex<ConsumerOrderInfoWrapper>,
    pub(crate) consumer_order_info_lock_manager: Option<ConsumerOrderInfoLockManager>,
}

//Fully implemented will be removed
#[allow(unused_variables)]
impl ConfigManager for ConsumerOrderInfoManager {
    fn decode0(&mut self, key: &[u8], body: &[u8]) {
        todo!()
    }

    fn stop(&mut self) -> bool {
        todo!()
    }

    fn config_file_path(&self) -> String {
        get_consumer_order_info_path(self.broker_config.store_path_root_dir.as_str())
    }

    fn encode(&mut self) -> String {
        todo!()
    }

    fn encode_pretty(&self, pretty_format: bool) -> String {
        "".to_string()
    }

    fn decode(&self, json_string: &str) {
        if json_string.is_empty() {
            return;
        }
        let wrapper =
            serde_json::from_str::<ConsumerOrderInfoWrapper>(json_string).unwrap_or_default();
        if !wrapper.table.is_empty() {
            self.consumer_order_info_wrapper
                .lock()
                .table
                .clone_from(&wrapper.table);
            if self.consumer_order_info_lock_manager.is_some() {
                self.consumer_order_info_lock_manager
                    .as_ref()
                    .unwrap()
                    .recover(self.consumer_order_info_wrapper.lock().deref());
            }
        }
    }
}

impl ConsumerOrderInfoManager {
    pub fn update_next_visible_time(
        &self,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: i32,
        queue_offset: u64,
        pop_time: u64,
        next_visible_time: u64,
    ) {
        let key = build_key(topic, group);
        let mut table = self.consumer_order_info_wrapper.lock();
        let qs = table.table.get_mut(&key);
        if qs.is_none() {
            warn!(
                "orderInfo of queueId is null. key: {}, queueOffset: {}, queueId: {}",
                key, queue_offset, queue_id
            );
            return;
        }
        let qs = qs.unwrap();
        let order_info = qs.get_mut(&queue_id);
        if order_info.is_none() {
            warn!(
                "orderInfo of queueId is null. key: {}, queueOffset: {}, queueId: {}",
                key, queue_offset, queue_id
            );
            return;
        }
        let order_info = order_info.unwrap();
        if pop_time != order_info.pop_time {
            warn!(
                "popTime is not equal to orderInfo saved. key: {}, queueOffset: {}, orderInfo: \
                 {}, popTime: {}",
                key, queue_offset, queue_id, pop_time,
            );
            return;
        }
        order_info
            .offset_next_visible_time
            .insert(queue_offset, next_visible_time);
        self.update_lock_free_timestamp(topic, group, queue_id, order_info);
    }

    fn update_lock_free_timestamp(
        &self,
        _topic: &CheetahString,
        _group: &CheetahString,
        _queue_id: i32,
        _order_info: &OrderInfo,
    ) {
        unimplemented!("")
    }
}

#[inline]
#[must_use]
fn build_key(topic: &CheetahString, group: &CheetahString) -> String {
    format!("{}{}{}", topic, TOPIC_GROUP_SEPARATOR, group)
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub(crate) struct ConsumerOrderInfoWrapper {
    table: HashMap<String /* topic@group */, HashMap<i32, OrderInfo>>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub(crate) struct OrderInfo {
    #[serde(rename = "popTime")]
    pop_time: u64,
    #[serde(rename = "i")]
    invisible_time: Option<u64>,
    #[serde(rename = "0")]
    offset_list: Vec<u64>,
    #[serde(rename = "ot")]
    offset_next_visible_time: HashMap<u64, u64>,
    #[serde(rename = "oc")]
    offset_consumed_count: HashMap<u64, i32>,
    #[serde(rename = "l")]
    last_consume_timestamp: u64,
    #[serde(rename = "cm")]
    commit_offset_bit: u64,
    #[serde(rename = "a")]
    attempt_id: String,
}
