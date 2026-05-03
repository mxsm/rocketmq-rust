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

use rocketmq_rust::ArcMut;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::static_topic::i32_key_map_serde;
use crate::protocol::static_topic::logic_queue_mapping_item::LogicQueueMappingItem;
use crate::protocol::static_topic::topic_queue_mapping_info::TopicQueueMappingInfo;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct TopicQueueMappingDetail {
    #[serde(flatten)]
    pub topic_queue_mapping_info: TopicQueueMappingInfo,

    #[serde(rename = "hostedQueues")]
    #[serde(default, deserialize_with = "i32_key_map_serde::deserialize_optional_i32_key_map")]
    pub hosted_queues: Option<HashMap<i32 /* global id */, Vec<LogicQueueMappingItem>>>,
}

impl TopicQueueMappingDetail {
    pub fn get_mapping_info(
        mapping_detail: &TopicQueueMappingDetail,
        global_id: i32,
    ) -> Option<&Vec<LogicQueueMappingItem>> {
        mapping_detail.hosted_queues.as_ref()?.get(&global_id)
    }

    pub fn compute_max_offset_from_mapping(mapping_detail: &TopicQueueMappingDetail, global_id: Option<i32>) -> i64 {
        match Self::get_mapping_info(mapping_detail, global_id.unwrap()) {
            Some(mapping_items) => {
                if mapping_items.is_empty() {
                    return -1;
                }
                let item = mapping_items.last().unwrap();
                item.compute_max_static_queue_offset()
            }
            None => -1,
        }
    }
}

//impl static methods(Like java static method)
impl TopicQueueMappingDetail {
    pub fn build_id_map(mapping_detail: &TopicQueueMappingDetail) -> HashMap<i32, i32> {
        let mut curr_id_map = HashMap::new();
        let broker_name = mapping_detail.topic_queue_mapping_info.bname.as_ref();
        if let (Some(hosted_queues), Some(broker_name)) = (&mapping_detail.hosted_queues, broker_name) {
            for (global_id, items) in hosted_queues {
                if let Some(last_item) = items.last() {
                    if last_item.bname.as_ref() == Some(broker_name) {
                        curr_id_map.insert(*global_id, last_item.queue_id);
                    }
                }
            }
        }
        curr_id_map
    }

    pub fn clone_as_mapping_info(mapping_detail: &TopicQueueMappingDetail) -> TopicQueueMappingInfo {
        TopicQueueMappingInfo {
            topic: mapping_detail.topic_queue_mapping_info.topic.clone(),
            scope: mapping_detail.topic_queue_mapping_info.scope.clone(),
            total_queues: mapping_detail.topic_queue_mapping_info.total_queues,
            bname: mapping_detail.topic_queue_mapping_info.bname.clone(),
            epoch: mapping_detail.topic_queue_mapping_info.epoch,
            dirty: mapping_detail.topic_queue_mapping_info.dirty,
            curr_id_map: Some(Self::build_id_map(mapping_detail)),
        }
    }
    pub fn put_mapping_info(
        mut mapping_detail: ArcMut<TopicQueueMappingDetail>,
        global_id: i32,
        mapping_info: Vec<LogicQueueMappingItem>,
    ) {
        if mapping_info.is_empty() {
            return;
        }
        if let Some(q_map) = &mut mapping_detail.hosted_queues {
            q_map.insert(global_id, mapping_info);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_queue_mapping_detail_default() {
        let detail = TopicQueueMappingDetail::default();
        assert_eq!(detail.topic_queue_mapping_info, TopicQueueMappingInfo::default());
        assert!(detail.hosted_queues.is_none());
    }

    #[test]
    fn topic_queue_mapping_detail_serde() {
        let mut detail = TopicQueueMappingDetail::default();
        detail.topic_queue_mapping_info.topic = Some("test".into());
        let mut hosted_queues = HashMap::new();
        hosted_queues.insert(1, vec![LogicQueueMappingItem::default()]);
        detail.hosted_queues = Some(hosted_queues);

        let json = serde_json::to_string(&detail).unwrap();
        let expected = r#"{"topic":"test","scope":"__global__","totalQueues":0,"bname":null,"epoch":0,"dirty":false,"currIdMap":null,"hostedQueues":{"1":[{"gen":0,"queueId":0,"bname":null,"logicOffset":0,"startOffset":0,"endOffset":-1,"timeOfStart":-1,"timeOfEnd":-1}]}}"#;
        assert_eq!(json, expected);

        let deserialized: TopicQueueMappingDetail = serde_json::from_str(&json).unwrap();
        assert_eq!(detail, deserialized);
    }

    #[test]
    fn get_mapping_info() {
        let mut detail = TopicQueueMappingDetail::default();
        let items = vec![LogicQueueMappingItem::default()];
        let mut hosted_queues = HashMap::new();
        hosted_queues.insert(1, items.clone());
        detail.hosted_queues = Some(hosted_queues);

        assert_eq!(TopicQueueMappingDetail::get_mapping_info(&detail, 1).unwrap(), &items);
        assert!(TopicQueueMappingDetail::get_mapping_info(&detail, 2).is_none());
    }

    #[test]
    fn compute_max_offset_from_mapping() {
        let mut detail = TopicQueueMappingDetail::default();
        let mut items = vec![LogicQueueMappingItem::default()];
        items[0].logic_offset = 100;
        items[0].start_offset = 50;
        items[0].end_offset = 150;
        // max = logic_offset + end_offset - start_offset = 100 + 150 - 50 = 200

        let mut hosted_queues = HashMap::new();
        hosted_queues.insert(1, items);
        detail.hosted_queues = Some(hosted_queues);

        assert_eq!(
            TopicQueueMappingDetail::compute_max_offset_from_mapping(&detail, Some(1)),
            200
        );

        // Update end_offset to be invalid (less than start_offset)
        detail.hosted_queues.as_mut().unwrap().get_mut(&1).unwrap()[0].end_offset = 20;
        // max should be logic_offset = 100
        assert_eq!(
            TopicQueueMappingDetail::compute_max_offset_from_mapping(&detail, Some(1)),
            100
        );

        // non-existent global_id
        assert_eq!(
            TopicQueueMappingDetail::compute_max_offset_from_mapping(&detail, Some(2)),
            -1
        );
    }

    #[test]
    fn clone_as_mapping_info() {
        let detail = TopicQueueMappingDetail {
            topic_queue_mapping_info: TopicQueueMappingInfo {
                topic: Some("test_topic".into()),
                total_queues: 8,
                bname: Some("broker_a".into()),
                epoch: 12345,
                ..TopicQueueMappingInfo::default()
            },
            ..Default::default()
        };

        let info = TopicQueueMappingDetail::clone_as_mapping_info(&detail);
        assert_eq!(info.topic, detail.topic_queue_mapping_info.topic);
        assert_eq!(info.total_queues, detail.topic_queue_mapping_info.total_queues);
        assert_eq!(info.bname, detail.topic_queue_mapping_info.bname);
        assert_eq!(info.epoch, detail.topic_queue_mapping_info.epoch);
    }

    #[test]
    fn clone_as_mapping_info_builds_current_id_map_for_leader_items() {
        let broker_a = cheetah_string::CheetahString::from_static_str("broker_a");
        let broker_b = cheetah_string::CheetahString::from_static_str("broker_b");
        let mut hosted_queues = HashMap::new();
        hosted_queues.insert(
            0,
            vec![LogicQueueMappingItem {
                queue_id: 3,
                bname: Some(broker_a.clone()),
                ..Default::default()
            }],
        );
        hosted_queues.insert(
            1,
            vec![LogicQueueMappingItem {
                queue_id: 7,
                bname: Some(broker_b),
                ..Default::default()
            }],
        );
        let detail = TopicQueueMappingDetail {
            topic_queue_mapping_info: TopicQueueMappingInfo {
                topic: Some("test_topic".into()),
                total_queues: 2,
                bname: Some(broker_a),
                epoch: 10,
                ..TopicQueueMappingInfo::default()
            },
            hosted_queues: Some(hosted_queues),
        };

        let info = TopicQueueMappingDetail::clone_as_mapping_info(&detail);

        let curr_id_map = info.curr_id_map.unwrap();
        assert_eq!(curr_id_map.get(&0), Some(&3));
        assert!(!curr_id_map.contains_key(&1));
    }

    #[test]
    fn put_mapping_info() {
        let detail = ArcMut::new(TopicQueueMappingDetail::default());
        detail.mut_from_ref().hosted_queues = Some(HashMap::new());

        let items = vec![LogicQueueMappingItem::default()];
        TopicQueueMappingDetail::put_mapping_info(detail.clone(), 1, items.clone());

        assert_eq!(detail.hosted_queues.as_ref().unwrap().get(&1).unwrap(), &items);

        TopicQueueMappingDetail::put_mapping_info(detail.clone(), 2, vec![]);
        assert!(detail.hosted_queues.as_ref().unwrap().get(&2).is_none());
    }
}
