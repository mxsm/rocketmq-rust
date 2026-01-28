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

use std::collections::HashSet;
use std::fmt;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use super::LiteSubscriptionAction;
use super::OffsetOption;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LiteSubscriptionDTO {
    #[serde(default)]
    pub action: LiteSubscriptionAction,

    #[serde(default)]
    pub client_id: CheetahString,

    #[serde(default)]
    pub group: CheetahString,

    #[serde(default)]
    pub topic: CheetahString,

    #[serde(default)]
    pub lite_topic_set: HashSet<CheetahString>,

    #[serde(default)]
    pub offset_option: Option<OffsetOption>,

    #[serde(default)]
    pub version: i64,
}

impl LiteSubscriptionDTO {
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self {
            action: LiteSubscriptionAction::default(),
            client_id: CheetahString::new(),
            group: CheetahString::new(),
            topic: CheetahString::new(),
            lite_topic_set: HashSet::new(),
            offset_option: None,
            version: 0,
        }
    }

    #[must_use]
    #[inline]
    pub fn with_action(mut self, action: LiteSubscriptionAction) -> Self {
        self.action = action;
        self
    }

    #[must_use]
    #[inline]
    pub fn with_client_id(mut self, client_id: CheetahString) -> Self {
        self.client_id = client_id;
        self
    }

    #[must_use]
    #[inline]
    pub fn with_group(mut self, group: CheetahString) -> Self {
        self.group = group;
        self
    }

    #[must_use]
    #[inline]
    pub fn with_topic(mut self, topic: CheetahString) -> Self {
        self.topic = topic;
        self
    }

    #[must_use]
    #[inline]
    pub fn with_lite_topic_set(mut self, lite_topic_set: HashSet<CheetahString>) -> Self {
        self.lite_topic_set = lite_topic_set;
        self
    }

    #[must_use]
    #[inline]
    pub fn with_offset_option(mut self, offset_option: OffsetOption) -> Self {
        self.offset_option = Some(offset_option);
        self
    }

    #[must_use]
    #[inline]
    pub fn with_version(mut self, version: i64) -> Self {
        self.version = version;
        self
    }

    #[must_use]
    #[inline]
    pub const fn action(&self) -> LiteSubscriptionAction {
        self.action
    }

    #[inline]
    pub fn set_action(&mut self, action: LiteSubscriptionAction) {
        self.action = action;
    }

    #[must_use]
    #[inline]
    pub fn client_id(&self) -> &CheetahString {
        &self.client_id
    }

    #[inline]
    pub fn set_client_id(&mut self, client_id: CheetahString) {
        self.client_id = client_id;
    }

    #[must_use]
    #[inline]
    pub fn group(&self) -> &CheetahString {
        &self.group
    }

    #[inline]
    pub fn set_group(&mut self, group: CheetahString) {
        self.group = group;
    }

    #[must_use]
    #[inline]
    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    #[inline]
    pub fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
    }

    #[must_use]
    #[inline]
    pub fn lite_topic_set(&self) -> &HashSet<CheetahString> {
        &self.lite_topic_set
    }

    #[inline]
    pub fn set_lite_topic_set(&mut self, lite_topic_set: HashSet<CheetahString>) {
        self.lite_topic_set = lite_topic_set;
    }

    #[must_use]
    #[inline]
    pub fn offset_option(&self) -> Option<OffsetOption> {
        self.offset_option
    }

    #[inline]
    pub fn set_offset_option(&mut self, offset_option: OffsetOption) {
        self.offset_option = Some(offset_option);
    }

    #[must_use]
    #[inline]
    pub const fn version(&self) -> i64 {
        self.version
    }

    #[inline]
    pub fn set_version(&mut self, version: i64) {
        self.version = version;
    }
}

impl Default for LiteSubscriptionDTO {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for LiteSubscriptionDTO {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LiteSubscriptionDTO {{ action: {}, client_id: {}, group: {}, topic: {}, lite_topic_set: {:?}, \
             offset_option: {:?}, version: {} }}",
            self.action, self.client_id, self.group, self.topic, self.lite_topic_set, self.offset_option, self.version
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lite_subscription_dto_default() {
        let dto = LiteSubscriptionDTO::default();
        assert_eq!(dto.action(), LiteSubscriptionAction::default());
        assert!(dto.client_id().is_empty());
        assert!(dto.group().is_empty());
        assert!(dto.topic().is_empty());
        assert!(dto.lite_topic_set().is_empty());
        assert!(dto.offset_option().is_none());
        assert_eq!(dto.version(), 0);
    }

    #[test]
    fn lite_subscription_dto_with_methods() {
        let mut set = HashSet::new();
        set.insert("lite_topic".into());
        let offset = OffsetOption::offset(100);

        let dto = LiteSubscriptionDTO::new()
            .with_action(LiteSubscriptionAction::CompleteAdd)
            .with_client_id("client".into())
            .with_group("group".into())
            .with_topic("topic".into())
            .with_lite_topic_set(set.clone())
            .with_offset_option(offset)
            .with_version(1);

        assert_eq!(dto.action(), LiteSubscriptionAction::CompleteAdd);
        assert_eq!(dto.client_id(), "client");
        assert_eq!(dto.group(), "group");
        assert_eq!(dto.topic(), "topic");
        assert_eq!(dto.lite_topic_set(), &set);
        assert_eq!(dto.offset_option(), Some(offset));
        assert_eq!(dto.version(), 1);
    }

    #[test]
    fn lite_subscription_dto_setters() {
        let mut dto = LiteSubscriptionDTO::new();
        dto.set_action(LiteSubscriptionAction::CompleteRemove);
        dto.set_client_id("client2".into());
        dto.set_group("group2".into());
        dto.set_topic("topic2".into());

        let mut set = HashSet::new();
        set.insert("topic1".into());
        dto.set_lite_topic_set(set.clone());

        let offset = OffsetOption::offset(200);
        dto.set_offset_option(offset);
        dto.set_version(2);

        assert_eq!(dto.action(), LiteSubscriptionAction::CompleteRemove);
        assert_eq!(dto.client_id(), "client2");
        assert_eq!(dto.group(), "group2");
        assert_eq!(dto.topic(), "topic2");
        assert_eq!(dto.lite_topic_set(), &set);
        assert_eq!(dto.offset_option(), Some(offset));
        assert_eq!(dto.version(), 2);
    }

    #[test]
    fn lite_subscription_dto_display() {
        let dto = LiteSubscriptionDTO::new();
        let display = format!("{}", dto);
        let expected = "LiteSubscriptionDTO { action: PARTIAL_ADD, client_id: , group: , topic: , lite_topic_set: {}, \
                        offset_option: None, version: 0 }";
        assert_eq!(display, expected);
    }

    #[test]
    fn lite_subscription_dto_serde() {
        let dto = LiteSubscriptionDTO::new().with_topic("topic".into());
        let json = serde_json::to_string(&dto).unwrap();
        let expected = r#"{"action":"PARTIAL_ADD","clientId":"","group":"","topic":"topic","liteTopicSet":[],"offsetOption":null,"version":0}"#;
        assert_eq!(json, expected);
        let decoded: LiteSubscriptionDTO = serde_json::from_str(&json).unwrap();
        assert_eq!(dto, decoded);
    }
}
