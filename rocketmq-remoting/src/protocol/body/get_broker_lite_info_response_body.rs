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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

/// Response body containing broker lite information.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetBrokerLiteInfoResponseBody {
    store_type: Option<CheetahString>,
    #[serde(default)]
    max_lmq_num: i32,

    #[serde(default)]
    current_lmq_num: i32,

    #[serde(default)]
    lite_subscription_count: i32,

    #[serde(default)]
    order_info_count: i32,

    #[serde(default)]
    cq_table_size: i32,

    #[serde(default)]
    offset_table_size: i32,

    #[serde(default)]
    event_map_size: i32,

    #[serde(default)]
    topic_meta: HashMap<CheetahString, i32>,

    #[serde(default)]
    group_meta: HashMap<CheetahString, HashSet<CheetahString>>,
}

impl GetBrokerLiteInfoResponseBody {
    pub fn new() -> Self {
        GetBrokerLiteInfoResponseBody::default()
    }

    pub fn get_store_type(&self) -> Option<&CheetahString> {
        self.store_type.as_ref()
    }

    pub fn set_store_type(&mut self, store_type: CheetahString) {
        self.store_type = Some(store_type);
    }

    pub fn get_max_lmq_num(&self) -> i32 {
        self.max_lmq_num
    }

    pub fn set_max_lmq_num(&mut self, max_lmq_num: i32) {
        self.max_lmq_num = max_lmq_num;
    }

    pub fn get_current_lmq_num(&self) -> i32 {
        self.current_lmq_num
    }

    pub fn set_current_lmq_num(&mut self, current_lmq_num: i32) {
        self.current_lmq_num = current_lmq_num;
    }

    pub fn get_lite_subscription_count(&self) -> i32 {
        self.lite_subscription_count
    }

    pub fn set_lite_subscription_count(&mut self, lite_subscription_count: i32) {
        self.lite_subscription_count = lite_subscription_count;
    }

    pub fn get_order_info_count(&self) -> i32 {
        self.order_info_count
    }

    pub fn set_order_info_count(&mut self, order_info_count: i32) {
        self.order_info_count = order_info_count;
    }

    pub fn get_cq_table_size(&self) -> i32 {
        self.cq_table_size
    }

    pub fn set_cq_table_size(&mut self, cq_table_size: i32) {
        self.cq_table_size = cq_table_size;
    }

    pub fn get_offset_table_size(&self) -> i32 {
        self.offset_table_size
    }

    pub fn set_offset_table_size(&mut self, offset_table_size: i32) {
        self.offset_table_size = offset_table_size;
    }

    pub fn get_event_map_size(&self) -> i32 {
        self.event_map_size
    }

    pub fn set_event_map_size(&mut self, event_map_size: i32) {
        self.event_map_size = event_map_size;
    }

    pub fn get_topic_meta(&self) -> &HashMap<CheetahString, i32> {
        &self.topic_meta
    }

    pub fn get_topic_meta_mut(&mut self) -> &mut HashMap<CheetahString, i32> {
        &mut self.topic_meta
    }

    pub fn set_topic_meta(&mut self, topic_meta: HashMap<CheetahString, i32>) {
        self.topic_meta = topic_meta;
    }

    pub fn get_group_meta(&self) -> &HashMap<CheetahString, HashSet<CheetahString>> {
        &self.group_meta
    }

    pub fn get_group_meta_mut(&mut self) -> &mut HashMap<CheetahString, HashSet<CheetahString>> {
        &mut self.group_meta
    }

    pub fn set_group_meta(&mut self, group_meta: HashMap<CheetahString, HashSet<CheetahString>>) {
        self.group_meta = group_meta;
    }
}
