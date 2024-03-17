use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::protocol::static_topic::{
    logic_queue_mapping_item::LogicQueueMappingItem, topic_queue_info::TopicQueueMappingInfo,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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
    ) -> Option<Vec<LogicQueueMappingItem>> {
        if let Some(hosted_queues) = &mapping_detail.hosted_queues {
            if let Some(queue_items) = hosted_queues.get(&global_id) {
                return Some(queue_items.clone());
            }
        }
        None
    }
}

//impl static methods(Like java static method)
impl TopicQueueMappingDetail {
    pub fn clone_as_mapping_info(
        mapping_detail: &TopicQueueMappingDetail,
    ) -> TopicQueueMappingInfo {
        TopicQueueMappingInfo {
            topic: mapping_detail.topic_queue_mapping_info.topic.clone(),
            total_queues: mapping_detail.topic_queue_mapping_info.total_queues,
            bname: mapping_detail.topic_queue_mapping_info.bname.clone(),
            epoch: mapping_detail.topic_queue_mapping_info.epoch,
            ..TopicQueueMappingInfo::default()
        }
    }
}
