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

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::atomic::AtomicU64;

use cheetah_string::CheetahString;
use rocketmq_common::TimeUtils::get_current_millis;

use crate::protocol::DataVersion;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TimerMetricsSerializeWrapper {
    timing_count: HashMap<CheetahString, Metric>,
    data_version: DataVersion,
}

impl TimerMetricsSerializeWrapper {
    pub fn new() -> Self {
        TimerMetricsSerializeWrapper::default()
    }

    pub fn with_timing_count(mut self, timing_count: HashMap<CheetahString, Metric>) -> Self {
        self.timing_count = timing_count;
        self
    }

    pub fn with_data_version(mut self, data_version: DataVersion) -> Self {
        self.data_version = data_version;
        self
    }

    pub fn timing_count(&self) -> &HashMap<CheetahString, Metric> {
        &self.timing_count
    }

    pub fn data_version(&self) -> &DataVersion {
        &self.data_version
    }

    pub fn data_version_mut(&mut self) -> &mut DataVersion {
        &mut self.data_version
    }

    pub fn timing_count_mut(&mut self) -> &mut HashMap<CheetahString, Metric> {
        &mut self.timing_count
    }

    pub fn insert_metric(&mut self, key: CheetahString, metric: Metric) {
        self.timing_count.insert(key, metric);
    }

    pub fn get_metric(&self, key: &CheetahString) -> Option<&Metric> {
        self.timing_count.get(key)
    }

    pub fn get_metric_mut(&mut self, key: &CheetahString) -> Option<&mut Metric> {
        self.timing_count.get_mut(key)
    }
}

impl Default for TimerMetricsSerializeWrapper {
    fn default() -> Self {
        TimerMetricsSerializeWrapper {
            timing_count: HashMap::with_capacity(1024),
            data_version: DataVersion::default(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metric {
    pub count: AtomicU64,
    pub time_stamp: u64,
}

impl Default for Metric {
    fn default() -> Self {
        Metric {
            count: AtomicU64::new(0),
            time_stamp: get_current_millis(),
        }
    }
}

impl Display for Metric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Metric {{ count: {}, time_stamp: {} }}",
            self.count.load(std::sync::atomic::Ordering::Relaxed),
            self.time_stamp
        )
    }
}
