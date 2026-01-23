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
