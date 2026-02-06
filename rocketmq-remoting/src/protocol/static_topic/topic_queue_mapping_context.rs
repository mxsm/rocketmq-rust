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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::static_topic::topic_queue_mapping_info::TopicQueueMappingInfo;

    #[test]
    fn topic_queue_mapping_context_default() {
        let context = TopicQueueMappingContext::default();
        assert_eq!(context.topic, "");
        assert!(context.global_id.is_none());
        assert!(context.mapping_detail.is_none());
        assert!(context.mapping_item_list.is_empty());
        assert!(context.leader_item.is_none());
        assert!(context.current_item.is_none());
    }

    #[test]
    fn topic_queue_mapping_context_new() {
        let topic = "test_topic";
        let global_id = Some(1);
        let mapping_detail = ArcMut::new(TopicQueueMappingDetail::default());
        let mapping_item_list = vec![LogicQueueMappingItem::default()];
        let leader_item = LogicQueueMappingItem::default();

        let context = TopicQueueMappingContext::new(
            topic,
            global_id,
            Some(mapping_detail.clone()),
            mapping_item_list.clone(),
            Some(leader_item.clone()),
        );

        assert_eq!(context.topic, CheetahString::from(topic));
        assert_eq!(context.global_id, global_id);
        assert_eq!(context.mapping_detail.unwrap(), mapping_detail);
        assert_eq!(context.mapping_item_list.len(), 1);
        assert_eq!(context.leader_item.unwrap(), leader_item);
        assert!(context.current_item.is_none());
    }

    #[test]
    fn topic_queue_mapping_context_is_leader() {
        let broker_name = CheetahString::from("broker_a");
        let leader_item = LogicQueueMappingItem {
            bname: Some(broker_name.clone()),
            ..Default::default()
        };
        let mapping_detail = TopicQueueMappingDetail {
            topic_queue_mapping_info: TopicQueueMappingInfo {
                bname: Some(broker_name.clone()),
                ..TopicQueueMappingInfo::default()
            },
            ..Default::default()
        };
        let mut context = TopicQueueMappingContext {
            leader_item: Some(leader_item.clone()),
            mapping_detail: Some(ArcMut::new(mapping_detail)),
            ..Default::default()
        };

        // Match: should be leader
        assert!(context.is_leader());

        // No mapping_detail: not leader
        context.mapping_detail = None;
        assert!(!context.is_leader());

        // Different bname in mapping_detail: not leader
        let diff_mapping_detail = TopicQueueMappingDetail {
            topic_queue_mapping_info: TopicQueueMappingInfo {
                bname: Some(CheetahString::from("broker_b")),
                ..TopicQueueMappingInfo::default()
            },
            ..Default::default()
        };
        context.mapping_detail = Some(ArcMut::new(diff_mapping_detail));
        assert!(!context.is_leader());

        // No bname in leader_item: not leader
        context.leader_item.as_mut().unwrap().bname = None;
        assert!(!context.is_leader());

        // No leader_item: not leader
        context.leader_item = None;
        assert!(!context.is_leader());
    }
}
