use std::collections::HashMap;

use rocketmq_rust::ArcMut;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::static_topic::logic_queue_mapping_item::LogicQueueMappingItem;
use crate::protocol::static_topic::topic_queue_info::TopicQueueMappingInfo;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct TopicQueueMappingDetail {
    #[serde(flatten)]
    pub topic_queue_mapping_info: TopicQueueMappingInfo,

    #[serde(rename = "hostedQueues")]
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
    pub fn clone_as_mapping_info(mapping_detail: &TopicQueueMappingDetail) -> TopicQueueMappingInfo {
        TopicQueueMappingInfo {
            topic: mapping_detail.topic_queue_mapping_info.topic.clone(),
            total_queues: mapping_detail.topic_queue_mapping_info.total_queues,
            bname: mapping_detail.topic_queue_mapping_info.bname.clone(),
            epoch: mapping_detail.topic_queue_mapping_info.epoch,
            ..TopicQueueMappingInfo::default()
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
