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
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Eq, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
    use std::collections::HashSet;

    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn methods_serde_and_display_preserve_group_forbidden() {
        let mut group_forbidden = GroupForbidden::new(
            CheetahString::from("initialTopic"),
            CheetahString::from("initialGroup"),
            Some(false),
        );
        group_forbidden.set_topic(CheetahString::from("testTopic"));
        group_forbidden.set_group(CheetahString::from("testGroup"));
        group_forbidden.set_readable(Some(true));

        assert_eq!(group_forbidden.topic(), &CheetahString::from("testTopic"));
        assert_eq!(group_forbidden.group(), "testGroup");
        assert_eq!(group_forbidden.readable(), Some(true));
        assert_eq!(
            group_forbidden.to_string(),
            "GroupForbidden [topic=testTopic, group=testGroup, readable=Some(true)]"
        );

        let json = serde_json::to_string(&group_forbidden).unwrap();
        assert_eq!(json, r#"{"topic":"testTopic","group":"testGroup","readable":true}"#);
        let decoded: GroupForbidden = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, group_forbidden);
    }

    #[test]
    fn equality_and_hash_use_all_fields() {
        let group_forbidden = GroupForbidden::new(
            CheetahString::from("testTopic"),
            CheetahString::from("testGroup"),
            Some(true),
        );
        let equal = GroupForbidden::new(
            CheetahString::from("testTopic"),
            CheetahString::from("testGroup"),
            Some(true),
        );
        assert_eq!(group_forbidden, equal);
        assert!(HashSet::from([group_forbidden.clone()]).contains(&equal));

        for different in [
            GroupForbidden::new("differentTopic".into(), "testGroup".into(), Some(true)),
            GroupForbidden::new("testTopic".into(), "differentGroup".into(), Some(true)),
            GroupForbidden::new("testTopic".into(), "testGroup".into(), None),
        ] {
            assert_ne!(group_forbidden, different);
        }
    }
}
