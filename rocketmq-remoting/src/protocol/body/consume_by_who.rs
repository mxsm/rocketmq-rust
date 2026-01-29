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

use std::collections::HashSet;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumeByWho {
    #[serde(default)]
    consumed_group: HashSet<CheetahString>,

    #[serde(default)]
    not_consumed_group: HashSet<CheetahString>,

    topic: Option<CheetahString>,

    #[serde(default)]
    queue_id: i32,

    #[serde(default)]
    offset: i64,
}

impl ConsumeByWho {
    pub fn new() -> Self {
        ConsumeByWho::default()
    }

    pub fn get_consumed_group(&self) -> &HashSet<CheetahString> {
        &self.consumed_group
    }

    pub fn set_consumed_group(&mut self, consumed_group: HashSet<CheetahString>) {
        self.consumed_group = consumed_group;
    }

    pub fn get_not_consumed_group(&self) -> &HashSet<CheetahString> {
        &self.not_consumed_group
    }

    pub fn set_not_consumed_group(&mut self, not_consumed_group: HashSet<CheetahString>) {
        self.not_consumed_group = not_consumed_group;
    }

    pub fn get_topic(&self) -> Option<&CheetahString> {
        self.topic.as_ref()
    }

    pub fn set_topic(&mut self, topic: CheetahString) {
        self.topic = Some(topic);
    }

    pub fn get_queue_id(&self) -> i32 {
        self.queue_id
    }

    pub fn set_queue_id(&mut self, queue_id: i32) {
        self.queue_id = queue_id;
    }

    pub fn get_offset(&self) -> i64 {
        self.offset
    }

    pub fn set_offset(&mut self, offset: i64) {
        self.offset = offset;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consume_by_who_default() {
        let body = ConsumeByWho::default();
        assert!(body.get_consumed_group().is_empty());
        assert!(body.get_not_consumed_group().is_empty());
        assert!(body.get_topic().is_none());
        assert_eq!(body.get_queue_id(), 0);
        assert_eq!(body.get_offset(), 0);
    }

    #[test]
    fn consume_by_who_getters_setters() {
        let mut body = ConsumeByWho::new();

        let mut consumed = HashSet::new();
        consumed.insert(CheetahString::from("g1"));
        body.set_consumed_group(consumed.clone());

        let mut not_consumed = HashSet::new();
        not_consumed.insert(CheetahString::from("g2"));
        body.set_not_consumed_group(not_consumed.clone());

        body.set_topic(CheetahString::from("topic1"));
        body.set_queue_id(1);
        body.set_offset(100);

        assert_eq!(body.get_consumed_group(), &consumed);
        assert_eq!(body.get_not_consumed_group(), &not_consumed);
        assert_eq!(body.get_topic().unwrap(), "topic1");
        assert_eq!(body.get_queue_id(), 1);
        assert_eq!(body.get_offset(), 100);
    }

    #[test]
    fn consume_by_who_serialization_and_deserialization() {
        let mut body = ConsumeByWho::new();
        body.set_consumed_group(HashSet::from(["g3".into()]));
        body.set_not_consumed_group(HashSet::from(["g4".into()]));
        body.set_topic(CheetahString::from("topic1"));
        body.set_offset(100);
        body.set_queue_id(1);

        let json = serde_json::to_string(&body).unwrap();
        assert!(json.contains("\"consumedGroup\":[\"g3\"]"));
        assert!(json.contains("\"notConsumedGroup\":[\"g4\"]"));
        assert!(json.contains("\"topic\":\"topic1\""));
        assert!(json.contains("\"offset\":100"));
        assert!(json.contains("\"queueId\":1"));

        let decoded: ConsumeByWho = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.get_consumed_group(), &HashSet::from(["g3".into()]));
        assert_eq!(decoded.get_not_consumed_group(), &HashSet::from(["g4".into()]));
        assert_eq!(decoded.get_topic().unwrap(), "topic1");
        assert_eq!(decoded.get_offset(), 100);
        assert_eq!(decoded.get_queue_id(), 1);
    }
}
