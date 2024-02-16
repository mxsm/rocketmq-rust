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
use crate::protocol::static_topic::{
    logic_queue_mapping_item::LogicQueueMappingItem,
    topic_queue_mapping_detail::TopicQueueMappingDetail,
};

pub struct TopicQueueMappingContext {
    pub topic: String,
    pub global_id: Option<i32>,
    pub mapping_detail: Option<TopicQueueMappingDetail>,
    pub mapping_item_list: Vec<LogicQueueMappingItem>,
    pub leader_item: Option<LogicQueueMappingItem>,
    pub current_item: Option<LogicQueueMappingItem>,
}

impl TopicQueueMappingContext {
    pub fn new(
        topic: impl Into<String>,
        global_id: Option<i32>,
        mapping_detail: Option<TopicQueueMappingDetail>,
        mapping_item_list: Vec<LogicQueueMappingItem>,
        leader_item: Option<LogicQueueMappingItem>,
    ) -> Self {
        Self {
            topic: topic.into(),
            global_id,
            mapping_detail,
            mapping_item_list,
            leader_item,
            current_item: None,
        }
    }
}
