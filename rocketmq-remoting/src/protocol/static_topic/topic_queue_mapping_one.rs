use serde::Serialize;

use crate::protocol::static_topic::logic_queue_mapping_item::LogicQueueMappingItem;
use crate::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;

#[derive(Debug, Serialize, PartialEq, Clone)]
pub struct TopicQueueMappingOne {
    topic: String, // redundant field
    bname: String, //identify the hosted broker name
    global_id: i32,
    items: Vec<LogicQueueMappingItem>,
    mapping_detail: TopicQueueMappingDetail,
}
impl TopicQueueMappingOne {
    pub fn new(
        mapping_detail: TopicQueueMappingDetail,
        topic: String,
        bname: String,
        global_id: i32,
        items: Vec<LogicQueueMappingItem>,
    ) -> Self {
        Self {
            topic,
            bname,
            global_id,
            items,
            mapping_detail,
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn bname(&self) -> &str {
        &self.bname
    }

    pub fn global_id(&self) -> i32 {
        self.global_id
    }

    pub fn items(&self) -> &Vec<LogicQueueMappingItem> {
        &self.items
    }

    pub fn mapping_detail(&self) -> &TopicQueueMappingDetail {
        &self.mapping_detail
    }
}
