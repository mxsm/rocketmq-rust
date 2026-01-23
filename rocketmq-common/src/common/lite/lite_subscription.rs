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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiteSubscription {
    pub group: CheetahString,
    pub topic: CheetahString,
    pub lite_topic_set: HashSet<CheetahString>,
    pub update_time: i64,
}

impl LiteSubscription {
    #[must_use]
    #[inline]
    pub fn new(group: CheetahString, topic: CheetahString) -> Self {
        Self {
            group,
            topic,
            lite_topic_set: HashSet::new(),
            update_time: Self::current_time_millis(),
        }
    }

    #[must_use]
    #[inline]
    pub fn with_lite_topic_set(mut self, lite_topic_set: HashSet<CheetahString>) -> Self {
        self.lite_topic_set = lite_topic_set;
        self.update_time = Self::current_time_millis();
        self
    }

    #[must_use]
    #[inline]
    pub fn with_update_time(mut self, update_time: i64) -> Self {
        self.update_time = update_time;
        self
    }

    #[inline]
    pub fn add_lite_topic(&mut self, lite_topic: CheetahString) -> bool {
        self.refresh_update_time();
        self.lite_topic_set.insert(lite_topic)
    }

    #[inline]
    pub fn add_lite_topic_set(&mut self, set: &HashSet<CheetahString>) {
        self.refresh_update_time();
        self.lite_topic_set.extend(set.iter().cloned());
    }

    #[inline]
    pub fn remove_lite_topic(&mut self, lite_topic: &CheetahString) -> bool {
        self.refresh_update_time();
        self.lite_topic_set.remove(lite_topic)
    }

    #[inline]
    pub fn remove_lite_topic_set(&mut self, set: &HashSet<CheetahString>) {
        self.refresh_update_time();
        for topic in set {
            self.lite_topic_set.remove(topic);
        }
    }

    #[must_use]
    #[inline]
    pub const fn group(&self) -> &CheetahString {
        &self.group
    }

    #[inline]
    pub fn set_group(&mut self, group: CheetahString) {
        self.group = group;
    }

    #[must_use]
    #[inline]
    pub const fn topic(&self) -> &CheetahString {
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
        self.refresh_update_time();
    }

    #[must_use]
    #[inline]
    pub const fn update_time(&self) -> i64 {
        self.update_time
    }

    #[inline]
    pub fn set_update_time(&mut self, update_time: i64) {
        self.update_time = update_time;
    }

    #[inline]
    fn refresh_update_time(&mut self) {
        self.update_time = Self::current_time_millis();
    }

    #[inline]
    fn current_time_millis() -> i64 {
        use std::time::SystemTime;
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

impl fmt::Display for LiteSubscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LiteSubscription {{ group: {}, topic: {}, lite_topic_set: {:?}, update_time: {} }}",
            self.group, self.topic, self.lite_topic_set, self.update_time
        )
    }
}
