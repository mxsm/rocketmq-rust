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

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::static_topic::logic_queue_mapping_item::LogicQueueMappingItem;
use crate::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct TopicQueueMappingOne {
    topic: String, // redundant field
    bname: String, // identify the hosted broker name
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_queue_mapping_one_new_and_getters() {
        let mapping_detail = TopicQueueMappingDetail::default();
        let topic = "test_topic".to_string();
        let bname = "broker_a".to_string();
        let global_id = 1;
        let items = vec![LogicQueueMappingItem::default()];

        let mapping_one = TopicQueueMappingOne::new(
            mapping_detail.clone(),
            topic.clone(),
            bname.clone(),
            global_id,
            items.clone(),
        );

        assert_eq!(mapping_one.topic(), topic);
        assert_eq!(mapping_one.bname(), bname);
        assert_eq!(mapping_one.global_id(), global_id);
        assert_eq!(mapping_one.items(), &items);
        assert_eq!(mapping_one.mapping_detail(), &mapping_detail);
    }

    #[test]
    fn topic_queue_mapping_one_serde() {
        let mapping_detail = TopicQueueMappingDetail::default();
        let mapping_one = TopicQueueMappingOne::new(
            mapping_detail,
            "topic".to_string(),
            "broker".to_string(),
            1,
            vec![LogicQueueMappingItem::default()],
        );

        let json = serde_json::to_string(&mapping_one).unwrap();
        let expected = r#"{"topic":"topic","bname":"broker","global_id":1,"items":[{"gen":0,"queueId":0,"bname":null,"logicOffset":0,"startOffset":0,"endOffset":-1,"timeOfStart":-1,"timeOfEnd":-1}],"mapping_detail":{"topic":null,"scope":"__global__","totalQueues":0,"bname":null,"epoch":0,"dirty":false,"currIdMap":null,"hostedQueues":null}}"#;
        assert_eq!(json, expected);

        let deserialized = serde_json::from_str::<TopicQueueMappingOne>(&json).unwrap();
        assert_eq!(deserialized, mapping_one);
    }
}
