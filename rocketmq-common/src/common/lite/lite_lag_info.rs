//  Copyright 2023 The RocketMQ Rust Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::fmt;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LiteLagInfo {
    #[serde(default)]
    pub lite_topic: CheetahString,

    #[serde(default)]
    pub lag_count: i64,

    #[serde(default = "default_earliest_unconsumed_timestamp")]
    pub earliest_unconsumed_timestamp: i64,
}

impl LiteLagInfo {
    #[must_use]
    #[inline]
    pub const fn new(lite_topic: CheetahString, lag_count: i64, earliest_unconsumed_timestamp: i64) -> Self {
        Self {
            lite_topic,
            lag_count,
            earliest_unconsumed_timestamp,
        }
    }

    #[must_use]
    #[inline]
    pub const fn lite_topic(&self) -> &CheetahString {
        &self.lite_topic
    }

    #[inline]
    pub fn set_lite_topic(&mut self, lite_topic: CheetahString) {
        self.lite_topic = lite_topic;
    }

    #[must_use]
    #[inline]
    pub const fn lag_count(&self) -> i64 {
        self.lag_count
    }

    #[inline]
    pub fn set_lag_count(&mut self, lag_count: i64) {
        self.lag_count = lag_count;
    }

    #[must_use]
    #[inline]
    pub const fn earliest_unconsumed_timestamp(&self) -> i64 {
        self.earliest_unconsumed_timestamp
    }

    #[inline]
    pub fn set_earliest_unconsumed_timestamp(&mut self, earliest_unconsumed_timestamp: i64) {
        self.earliest_unconsumed_timestamp = earliest_unconsumed_timestamp;
    }
}

fn default_earliest_unconsumed_timestamp() -> i64 {
    -1
}

impl fmt::Display for LiteLagInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LiteLagInfo {{ lite_topic: {}, lag_count: {}, earliest_unconsumed_timestamp: {} }}",
            self.lite_topic, self.lag_count, self.earliest_unconsumed_timestamp
        )
    }
}
