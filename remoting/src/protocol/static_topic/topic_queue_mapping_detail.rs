use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::protocol::static_topic::logic_queue_mapping_item::LogicQueueMappingItem;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TopicQueueMappingDetail {
    #[serde(rename = "hostedQueues")]
    hosted_queues: Option<HashMap<i32 /* global id */, Vec<LogicQueueMappingItem>>>,
}
