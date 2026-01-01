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
use rocketmq_rust::ArcMut;

use crate::protocol::static_topic::logic_queue_mapping_item::LogicQueueMappingItem;
use crate::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;

#[derive(Default)]
pub struct TopicQueueMappingContext {
    pub topic: CheetahString,
    pub global_id: Option<i32>,
    pub mapping_detail: Option<ArcMut<TopicQueueMappingDetail>>,
    pub mapping_item_list: Vec<LogicQueueMappingItem>,
    pub leader_item: Option<LogicQueueMappingItem>,
    pub current_item: Option<LogicQueueMappingItem>,
}

impl TopicQueueMappingContext {
    pub fn new(
        topic: impl Into<CheetahString>,
        global_id: Option<i32>,
        mapping_detail: Option<ArcMut<TopicQueueMappingDetail>>,
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

impl TopicQueueMappingContext {
    pub fn is_leader(&self) -> bool {
        if let Some(leader_item) = &self.leader_item {
            match leader_item.bname {
                None => {
                    return false;
                }
                Some(ref broker_name) => match self.mapping_detail.as_ref() {
                    None => return false,
                    Some(inner) => match inner.topic_queue_mapping_info.bname {
                        None => return false,
                        Some(ref inner_bname) => {
                            return inner_bname == broker_name;
                        }
                    },
                },
            }
        }
        false
    }
}
