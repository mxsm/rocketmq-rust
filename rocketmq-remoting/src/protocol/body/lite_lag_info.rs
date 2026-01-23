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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LiteLagInfo {
    #[serde(default)]
    lite_topic: CheetahString,

    #[serde(default)]
    lag_count: i64,

    #[serde(default)]
    earliest_unconsumed_timestamp: i64,
}

impl LiteLagInfo {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn lite_topic(&self) -> &CheetahString {
        &self.lite_topic
    }

    pub fn with_lite_topic(&mut self, lite_topic: CheetahString) -> &mut Self {
        self.lite_topic = lite_topic;
        self
    }

    #[must_use]
    pub fn lag_count(&self) -> i64 {
        self.lag_count
    }

    pub fn with_lag_count(&mut self, lag_count: i64) -> &mut Self {
        self.lag_count = lag_count;
        self
    }

    #[must_use]
    pub fn earliest_unconsumed_timestamp(&self) -> i64 {
        self.earliest_unconsumed_timestamp
    }

    pub fn with_earliest_unconsumed_timestamp(&mut self, earliest_unconsumed_timestamp: i64) -> &mut Self {
        self.earliest_unconsumed_timestamp = earliest_unconsumed_timestamp;
        self
    }
}
