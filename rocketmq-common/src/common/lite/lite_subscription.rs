// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashSet;
use std::fmt;

use cheetah_string::CheetahString;
use rocketmq_protocol::common::lite::lite_subscription::LiteSubscription as CanonicalLiteSubscription;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiteSubscription {
    pub group: CheetahString,
    pub topic: CheetahString,
    pub lite_topic_set: HashSet<CheetahString>,
    pub update_time: i64,
    pub version: i64,
}

impl LiteSubscription {
    pub fn new(group: CheetahString, topic: CheetahString) -> Self {
        Self {
            group,
            topic,
            lite_topic_set: HashSet::new(),
            update_time: current_millis(),
            version: 0,
        }
    }

    pub fn with_lite_topic_set(mut self, lite_topic_set: HashSet<CheetahString>) -> Self {
        self.lite_topic_set = lite_topic_set;
        self.update_time = current_millis();
        self
    }

    pub fn with_update_time(mut self, update_time: i64) -> Self {
        self.update_time = update_time;
        self
    }

    pub fn with_version(mut self, version: i64) -> Self {
        self.version = version;
        self
    }

    pub fn add_lite_topic(&mut self, lite_topic: CheetahString) -> bool {
        self.update_time = current_millis();
        self.lite_topic_set.insert(lite_topic)
    }

    pub fn add_lite_topic_set(&mut self, set: &HashSet<CheetahString>) {
        self.update_time = current_millis();
        self.lite_topic_set.extend(set.iter().cloned());
    }

    pub fn remove_lite_topic(&mut self, lite_topic: &CheetahString) -> bool {
        self.update_time = current_millis();
        self.lite_topic_set.remove(lite_topic)
    }

    pub fn remove_lite_topic_set(&mut self, set: &HashSet<CheetahString>) {
        self.update_time = current_millis();
        for topic in set {
            self.lite_topic_set.remove(topic);
        }
    }

    pub const fn group(&self) -> &CheetahString {
        &self.group
    }

    pub fn set_group(&mut self, group: CheetahString) {
        self.group = group;
    }

    pub const fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
    }

    pub fn lite_topic_set(&self) -> &HashSet<CheetahString> {
        &self.lite_topic_set
    }

    pub fn set_lite_topic_set(&mut self, lite_topic_set: HashSet<CheetahString>) {
        self.lite_topic_set = lite_topic_set;
        self.update_time = current_millis();
    }

    pub const fn update_time(&self) -> i64 {
        self.update_time
    }

    pub fn set_update_time(&mut self, update_time: i64) {
        self.update_time = update_time;
    }

    pub const fn version(&self) -> i64 {
        self.version
    }

    pub fn set_version(&mut self, version: i64) {
        self.version = version;
    }
}

impl From<LiteSubscription> for CanonicalLiteSubscription {
    fn from(value: LiteSubscription) -> Self {
        Self {
            group: value.group,
            topic: value.topic,
            lite_topic_set: value.lite_topic_set,
            update_time: value.update_time,
            version: value.version,
        }
    }
}

impl From<CanonicalLiteSubscription> for LiteSubscription {
    fn from(value: CanonicalLiteSubscription) -> Self {
        Self {
            group: value.group,
            topic: value.topic,
            lite_topic_set: value.lite_topic_set,
            update_time: value.update_time,
            version: value.version,
        }
    }
}

impl fmt::Display for LiteSubscription {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "LiteSubscription {{ group: {}, topic: {}, lite_topic_set: {:?}, update_time: {}, version: {} }}",
            self.group, self.topic, self.lite_topic_set, self.update_time, self.version
        )
    }
}

fn current_millis() -> i64 {
    crate::utils::time_utils::current_millis() as i64
}
