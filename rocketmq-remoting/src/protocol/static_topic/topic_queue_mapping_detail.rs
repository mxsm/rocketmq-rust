use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::protocol::static_topic::{
    logic_queue_mapping_item::LogicQueueMappingItem, topic_queue_info::TopicQueueMappingInfo,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TopicQueueMappingDetail {
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
