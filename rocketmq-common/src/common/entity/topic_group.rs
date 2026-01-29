//  Copyright 2023 The RocketMQ Rust Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::hash::Hash;
use std::hash::Hasher;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default, Eq)]
pub struct TopicGroup {
    #[serde(default)]
    pub topic: CheetahString,

    #[serde(default)]
    pub group: CheetahString,
}

impl TopicGroup {
    pub fn from_parts(topic: CheetahString, group: CheetahString) -> Self {
        Self { topic, group }
    }

    #[must_use]
    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn with_topic(&mut self, topic: CheetahString) -> &mut Self {
        self.topic = topic;
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
}

impl PartialEq for TopicGroup {
    fn eq(&self, other: &Self) -> bool {
        self.topic == other.topic && self.group == other.group
    }
}

impl Hash for TopicGroup {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.topic.hash(state);
        self.group.hash(state);
    }
}

impl std::fmt::Display for TopicGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TopicGroup{{topic='{}', group='{}'}}", self.topic, self.group)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_group_default() {
        let tg = TopicGroup::default();
        assert!(tg.topic().is_empty());
        assert!(tg.group().is_empty());
    }

    #[test]
    fn topic_group_from_parts() {
        let tg = TopicGroup::from_parts("topic".into(), "group".into());
        assert_eq!(tg.topic(), "topic");
        assert_eq!(tg.group(), "group");
    }

    #[test]
    fn topic_group_with_methods() {
        let mut tg = TopicGroup::default();
        tg.with_topic("topic".into()).with_group("group".into());
        assert_eq!(tg.topic(), "topic");
        assert_eq!(tg.group(), "group");
    }

    #[test]
    fn topic_group_partial_eq_and_hash() {
        let tg1 = TopicGroup::from_parts("topic".into(), "group".into());
        let tg2 = TopicGroup::from_parts("topic".into(), "group".into());
        let tg3 = TopicGroup::from_parts("topic2".into(), "group".into());
        assert_eq!(tg1, tg2);
        assert_ne!(tg1, tg3);

        use std::collections::hash_map::DefaultHasher;
        let mut hasher1 = DefaultHasher::new();
        tg1.hash(&mut hasher1);
        let mut hasher2 = DefaultHasher::new();
        tg2.hash(&mut hasher2);
        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn topic_group_display() {
        let tg = TopicGroup::from_parts("topic".into(), "group".into());
        let display = format!("{}", tg);
        let expected = "TopicGroup{topic='topic', group='group'}";
        assert_eq!(display, expected);
    }

    #[test]
    fn topic_group_serde() {
        let tg = TopicGroup::from_parts("topic".into(), "group".into());
        let json = serde_json::to_string(&tg).unwrap();
        let expected = r#"{"topic":"topic","group":"group"}"#;
        assert_eq!(json, expected);
        let decoded: TopicGroup = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, tg);
    }
}
