use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::protocol::static_topic::logic_queue_mapping_item::LogicQueueMappingItem;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicQueueMappingDetail {
    #[serde(rename = "hostedQueues")]
    hosted_queues: Option<HashMap<i32 /* global id */, Vec<LogicQueueMappingItem>>>,
}

impl Default for TopicQueueMappingDetail {
    fn default() -> Self {
        TopicQueueMappingDetail {
            hosted_queues: None,
        }
    }
}
