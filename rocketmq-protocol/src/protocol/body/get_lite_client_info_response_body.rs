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
pub struct GetLiteClientInfoResponseBody {
    #[serde(rename = "parentTopic")]
    parent_topic: Option<CheetahString>,

    #[serde(default)]
    group: CheetahString,

    #[serde(default)]
    client_id: CheetahString,

    #[serde(default)]
    last_access_time: u64,

    #[serde(default)]
    last_consume_time: u64,

    #[serde(default)]
    lite_topic_count: u32,

    #[serde(default)]
    lite_topic_set: HashSet<CheetahString>,
}

impl GetLiteClientInfoResponseBody {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn parent_topic(&self) -> Option<&CheetahString> {
        self.parent_topic.as_ref()
    }

    pub fn with_parent_topic(&mut self, parent_topic: CheetahString) -> &mut Self {
        self.parent_topic = Some(parent_topic);
        self
    }

    #[must_use]
    pub fn group(&self) -> &CheetahString {
        &self.group
    }

    pub fn with_group(&mut self, group: CheetahString) -> &mut Self {
        self.group = group;
        self
    }

    #[must_use]
    pub fn client_id(&self) -> &CheetahString {
        &self.client_id
    }

    pub fn with_client_id(&mut self, client_id: CheetahString) -> &mut Self {
        self.client_id = client_id;
        self
    }

    #[must_use]
    pub fn last_access_time(&self) -> u64 {
        self.last_access_time
    }

    pub fn with_last_access_time(&mut self, last_access_time: u64) -> &mut Self {
        self.last_access_time = last_access_time;
        self
    }

    #[must_use]
    pub fn last_consume_time(&self) -> u64 {
        self.last_consume_time
    }

    pub fn with_last_consume_time(&mut self, last_consume_time: u64) -> &mut Self {
        self.last_consume_time = last_consume_time;
        self
    }

    #[must_use]
    pub fn lite_topic_count(&self) -> u32 {
        self.lite_topic_count
    }

    pub fn with_lite_topic_count(&mut self, lite_topic_count: u32) -> &mut Self {
        self.lite_topic_count = lite_topic_count;
        self
    }

    #[must_use]
    pub fn lite_topic_set(&self) -> &HashSet<CheetahString> {
        &self.lite_topic_set
    }

    pub fn with_lite_topic_set(&mut self, lite_topic_set: HashSet<CheetahString>) -> &mut Self {
        self.lite_topic_set = lite_topic_set;
        self
    }

    pub fn add_lite_topic(&mut self, topic: CheetahString) -> &mut Self {
        self.lite_topic_set.insert(topic);
        self
    }

    pub fn remove_lite_topic(&mut self, topic: &CheetahString) -> bool {
        self.lite_topic_set.remove(topic)
    }

    #[must_use]
    pub fn contains_lite_topic(&self, topic: &CheetahString) -> bool {
        self.lite_topic_set.contains(topic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_lite_client_info_response_body_default() {
        let body = GetLiteClientInfoResponseBody::default();
        assert!(body.parent_topic().is_none());
        assert!(body.group().is_empty());
        assert!(body.client_id().is_empty());
        assert_eq!(body.last_access_time(), 0);
        assert_eq!(body.last_consume_time(), 0);
        assert_eq!(body.lite_topic_count(), 0);
        assert!(body.lite_topic_set().is_empty());
    }

    #[test]
    fn get_lite_client_info_response_body_getters_and_setters() {
        let mut body = GetLiteClientInfoResponseBody::new();
        body.with_parent_topic("parent".into())
            .with_group("group".into())
            .with_client_id("client".into())
            .with_last_access_time(100)
            .with_last_consume_time(200)
            .with_lite_topic_count(2)
            .with_lite_topic_set(HashSet::from(["topic1".into(), "topic2".into()]));

        assert_eq!(body.parent_topic().unwrap(), "parent");
        assert_eq!(body.group(), "group");
        assert_eq!(body.client_id(), "client");
        assert_eq!(body.last_access_time(), 100);
        assert_eq!(body.last_consume_time(), 200);
        assert_eq!(body.lite_topic_count(), 2);
        assert_eq!(
            body.lite_topic_set(),
            &HashSet::from(["topic1".into(), "topic2".into()])
        );
    }

    #[test]
    fn get_lite_client_info_response_body_topic_operations() {
        let mut body = GetLiteClientInfoResponseBody::new();
        body.add_lite_topic("topic1".into());
        assert!(body.contains_lite_topic(&"topic1".into()));

        body.remove_lite_topic(&"topic1".into());
        assert!(!body.contains_lite_topic(&"topic1".into()));
        assert!(body.lite_topic_set().is_empty());
    }

    #[test]
    fn get_lite_client_info_response_body_serialization_and_deserialization() {
        let mut body = GetLiteClientInfoResponseBody::new();
        body.with_parent_topic("parent".into())
            .with_group("group".into())
            .with_client_id("client".into())
            .with_last_access_time(100)
            .with_last_consume_time(200)
            .with_lite_topic_count(1)
            .with_lite_topic_set(HashSet::from(["topic3".into()]));

        let json = serde_json::to_string(&body).unwrap();
        let expected = r#"{"parentTopic":"parent","group":"group","clientId":"client","lastAccessTime":100,"lastConsumeTime":200,"liteTopicCount":1,"liteTopicSet":["topic3"]}"#;
        assert_eq!(json, expected);

        let decoded: GetLiteClientInfoResponseBody = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.parent_topic().unwrap(), "parent");
        assert_eq!(decoded.group(), "group");
        assert_eq!(decoded.client_id(), "client");
        assert_eq!(decoded.last_access_time(), 100);
        assert_eq!(decoded.last_consume_time(), 200);
        assert_eq!(decoded.lite_topic_count(), 1);
        assert_eq!(decoded.lite_topic_set(), &HashSet::from(["topic3".into()]));
    }
}
