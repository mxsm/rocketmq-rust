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
pub struct GetParentTopicInfoResponseBody {
    topic: Option<CheetahString>,

    #[serde(default)]
    ttl: i32,

    #[serde(default)]
    groups: HashSet<CheetahString>,

    #[serde(default)]
    lmq_num: i32,

    #[serde(default)]
    lite_topic_count: i32,
}

impl GetParentTopicInfoResponseBody {
    pub fn new() -> Self {
        GetParentTopicInfoResponseBody::default()
    }

    pub fn get_topic(&self) -> Option<&CheetahString> {
        self.topic.as_ref()
    }

    pub fn set_topic(&mut self, topic: CheetahString) {
        self.topic = Some(topic);
    }

    pub fn get_ttl(&self) -> i32 {
        self.ttl
    }

    pub fn set_ttl(&mut self, ttl: i32) {
        self.ttl = ttl;
    }

    pub fn get_groups(&self) -> &HashSet<CheetahString> {
        &self.groups
    }

    pub fn set_groups(&mut self, groups: HashSet<CheetahString>) {
        self.groups = groups;
    }

    pub fn get_lmq_num(&self) -> i32 {
        self.lmq_num
    }

    pub fn set_lmq_num(&mut self, lmq_num: i32) {
        self.lmq_num = lmq_num;
    }

    pub fn get_lite_topic_count(&self) -> i32 {
        self.lite_topic_count
    }

    pub fn set_lite_topic_count(&mut self, lite_topic_count: i32) {
        self.lite_topic_count = lite_topic_count;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_parent_topic_info_response_body_default() {
        let body = GetParentTopicInfoResponseBody::default();
        assert!(body.get_topic().is_none());
        assert_eq!(body.get_ttl(), 0);
        assert!(body.get_groups().is_empty());
        assert_eq!(body.get_lmq_num(), 0);
        assert_eq!(body.get_lite_topic_count(), 0);
    }

    #[test]
    fn get_parent_topic_info_response_body_getters_setters() {
        let mut body = GetParentTopicInfoResponseBody::new();
        body.set_topic(CheetahString::from("parent"));
        body.set_ttl(3600);
        let mut groups = HashSet::new();
        groups.insert(CheetahString::from("g1"));
        body.set_groups(groups.clone());
        body.set_lmq_num(10);
        body.set_lite_topic_count(5);

        assert_eq!(body.get_topic().unwrap(), "parent");
        assert_eq!(body.get_ttl(), 3600);
        assert_eq!(body.get_groups(), &groups);
        assert_eq!(body.get_lmq_num(), 10);
        assert_eq!(body.get_lite_topic_count(), 5);
    }

    #[test]
    fn get_parent_topic_info_response_body_serialization_and_deserialization() {
        let mut body = GetParentTopicInfoResponseBody::new();
        body.set_topic(CheetahString::from("parent"));
        body.set_ttl(3600);
        body.set_groups(HashSet::from([CheetahString::from("g1")]));
        body.set_lmq_num(10);
        body.set_lite_topic_count(5);

        let json = serde_json::to_string(&body).unwrap();
        assert!(json.contains("\"topic\":\"parent\""));
        assert!(json.contains("\"ttl\":3600"));
        assert!(json.contains("\"groups\":[\"g1\"]"));
        assert!(json.contains("\"lmqNum\":10"));
        assert!(json.contains("\"liteTopicCount\":5"));

        let decoded: GetParentTopicInfoResponseBody = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.get_topic().unwrap(), "parent");
        assert_eq!(decoded.get_ttl(), 3600);
        assert_eq!(decoded.get_groups(), &HashSet::from([CheetahString::from("g1")]));
        assert_eq!(decoded.get_lmq_num(), 10);
        assert_eq!(decoded.get_lite_topic_count(), 5);
    }
}
