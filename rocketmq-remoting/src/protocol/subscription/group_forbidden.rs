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

use std::fmt;

use cheetah_string::CheetahString;

#[derive(Debug, Eq)]
pub struct GroupForbidden {
    topic: CheetahString,
    group: CheetahString,
    readable: Option<bool>,
}

impl GroupForbidden {
    pub fn new(topic: CheetahString, group: CheetahString, readable: Option<bool>) -> Self {
        Self { topic, group, readable }
    }

    #[inline]
    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    #[inline]
    pub fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
    }

    #[inline]
    pub fn group(&self) -> &CheetahString {
        &self.group
    }

    #[inline]
    pub fn set_group(&mut self, group: CheetahString) {
        self.group = group;
    }

    #[inline]
    pub fn readable(&self) -> Option<bool> {
        self.readable
    }

    #[inline]
    pub fn set_readable(&mut self, readable: Option<bool>) {
        self.readable = readable;
    }
}

impl fmt::Display for GroupForbidden {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GroupForbidden [topic={}, group={}, readable={:?}]",
            self.topic, self.group, self.readable
        )
    }
}

impl std::hash::Hash for GroupForbidden {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.topic.hash(state);
        self.group.hash(state);
        self.readable.hash(state);
    }
}

impl PartialEq for GroupForbidden {
    fn eq(&self, other: &Self) -> bool {
        self.topic == other.topic && self.group == other.group && self.readable == other.readable
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn group_forbidden_new_creates_instance_with_correct_values() {
        let group_forbidden = GroupForbidden::new(
            CheetahString::from("testTopic"),
            CheetahString::from("testGroup"),
            Some(true),
        );
        assert_eq!(group_forbidden.topic(), &CheetahString::from("testTopic"));
        assert_eq!(group_forbidden.group(), "testGroup");
        assert_eq!(group_forbidden.readable(), Some(true));
    }

    #[test]
    fn group_forbidden_new_creates_instance_with_none_readable() {
        let group_forbidden =
            GroupForbidden::new(CheetahString::from("testTopic"), CheetahString::from("testGroup"), None);
        assert_eq!(group_forbidden.topic(), &CheetahString::from("testTopic"));
        assert_eq!(group_forbidden.group(), "testGroup");
        assert_eq!(group_forbidden.readable(), None);
    }

    #[test]
    fn group_forbidden_setters_update_values_correctly() {
        let mut group_forbidden = GroupForbidden::new(
            CheetahString::from("initialTopic"),
            CheetahString::from("initialGroup"),
            Some(false),
        );
        group_forbidden.set_topic(CheetahString::from("newTopic"));
        group_forbidden.set_group(CheetahString::from("newGroup"));
        group_forbidden.set_readable(Some(true));

        assert_eq!(group_forbidden.topic(), &CheetahString::from("newTopic"));
        assert_eq!(group_forbidden.group(), "newGroup");
        assert_eq!(group_forbidden.readable(), Some(true));
    }

    #[test]
    fn group_forbidden_display_formats_correctly() {
        let group_forbidden = GroupForbidden::new(
            CheetahString::from("testTopic"),
            CheetahString::from("testGroup"),
            Some(true),
        );
        let display = format!("{}", group_forbidden);
        assert_eq!(
            display,
            "GroupForbidden [topic=testTopic, group=testGroup, readable=Some(true)]"
        );
    }

    #[test]
    fn group_forbidden_equality_works_correctly() {
        let group_forbidden1 = GroupForbidden::new(
            CheetahString::from("testTopic"),
            CheetahString::from("testGroup"),
            Some(true),
        );
        let group_forbidden2 = GroupForbidden::new(
            CheetahString::from("testTopic"),
            CheetahString::from("testGroup"),
            Some(true),
        );
        let group_forbidden3 = GroupForbidden::new(
            CheetahString::from("differentTopic"),
            CheetahString::from("testGroup"),
            Some(true),
        );

        assert_eq!(group_forbidden1, group_forbidden2);
        assert_ne!(group_forbidden1, group_forbidden3);
    }

    #[test]
    fn group_forbidden_hash_works_correctly() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hash;
        use std::hash::Hasher;

        let group_forbidden = GroupForbidden::new(
            CheetahString::from("testTopic"),
            CheetahString::from("testGroup"),
            Some(true),
        );

        let mut hasher = DefaultHasher::new();
        group_forbidden.hash(&mut hasher);
        let hash1 = hasher.finish();

        let mut hasher = DefaultHasher::new();
        group_forbidden.hash(&mut hasher);
        let hash2 = hasher.finish();

        assert_eq!(hash1, hash2);
    }
}
