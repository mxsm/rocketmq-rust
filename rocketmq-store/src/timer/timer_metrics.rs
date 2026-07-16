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
use std::fs;
use std::path::Path;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::data_version_facade::DataVersionExt;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_store_local::timer::metrics::default_timer_dist;
use rocketmq_store_local::timer::metrics::TimerMetric;
use rocketmq_store_local::timer::metrics::TimerMetricsState;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TimerMetricsSerializeWrapper {
    timing_count: HashMap<String, TimerMetric>,
    #[serde(default)]
    timing_distribution: HashMap<i32, TimerMetric>,
    #[serde(default = "default_timer_dist")]
    timer_dist: Vec<i32>,
    data_version: DataVersion,
}

pub struct TimerMetrics {
    config_path: Mutex<Option<String>>,
    state: TimerMetricsState,
    data_version: Mutex<DataVersion>,
}

impl Default for TimerMetrics {
    fn default() -> Self {
        Self::new(None)
    }
}

impl ConfigManager for TimerMetrics {
    fn load(&self) -> bool {
        let Some(config_path) = self.config_path.lock().clone() else {
            return true;
        };

        match fs::read_to_string(config_path) {
            Ok(content) if !content.is_empty() => {
                self.decode(content.as_str());
                true
            }
            Ok(_) => true,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => true,
            Err(_) => false,
        }
    }

    fn persist(&self) {
        let Some(config_path) = self.config_path.lock().clone() else {
            return;
        };
        let Some(parent) = Path::new(config_path.as_str()).parent() else {
            return;
        };
        if fs::create_dir_all(parent).is_err() {
            return;
        }
        let _ = fs::write(config_path, self.encode_pretty(true));
    }

    fn config_file_path(&self) -> String {
        self.config_path.lock().clone().unwrap_or_default()
    }

    fn encode_pretty(&self, pretty_format: bool) -> String {
        let (timing_count, timing_distribution, timer_dist) = self.state.export();
        let wrapper = TimerMetricsSerializeWrapper {
            timing_count,
            timing_distribution,
            timer_dist,
            data_version: self.data_version.lock().clone(),
        };

        if pretty_format {
            serde_json::to_string_pretty(&wrapper).unwrap_or_default()
        } else {
            serde_json::to_string(&wrapper).unwrap_or_default()
        }
    }

    fn decode(&self, json_string: &str) {
        if let Ok(wrapper) = serde_json::from_str::<TimerMetricsSerializeWrapper>(json_string) {
            self.state
                .apply(wrapper.timing_count, wrapper.timing_distribution, wrapper.timer_dist);
            *self.data_version.lock() = wrapper.data_version;
        }
    }
}

impl TimerMetrics {
    pub fn new(config_path: Option<String>) -> Self {
        Self {
            config_path: Mutex::new(config_path),
            state: TimerMetricsState::default(),
            data_version: Mutex::new(rocketmq_remoting::protocol::data_version_facade::new_data_version()),
        }
    }

    pub fn set_config_path(&mut self, config_path: Option<String>) {
        *self.config_path.get_mut() = config_path;
    }

    pub fn data_version(&self) -> DataVersion {
        self.data_version.lock().clone()
    }

    pub fn to_wrapper(&self) -> TimerMetricsSerializeWrapper {
        let (timing_count, timing_distribution, timer_dist) = self.state.export();
        TimerMetricsSerializeWrapper {
            timing_count,
            timing_distribution,
            timer_dist,
            data_version: self.data_version(),
        }
    }

    pub fn apply_wrapper(&self, wrapper: TimerMetricsSerializeWrapper) {
        self.state
            .apply(wrapper.timing_count, wrapper.timing_distribution, wrapper.timer_dist);
        *self.data_version.lock() = wrapper.data_version;
    }

    pub fn get_timing_count(&self, key: &CheetahString) -> i64 {
        self.state.get_timing_count(key.as_str())
    }

    pub fn get_timing_count_snapshot(&self) -> HashMap<String, i64> {
        self.state.timing_count_snapshot()
    }

    pub fn replace_timing_count_snapshot(&self, timing_count_snapshot: HashMap<String, i64>) {
        self.state
            .replace_timing_count_snapshot(timing_count_snapshot, current_millis() as i64);
        self.data_version.lock().next_version();
    }

    pub fn add_timing_count(&self, key: &CheetahString, delta: i64) {
        self.state
            .add_timing_count(key.as_str(), delta, current_millis() as i64);
        self.data_version.lock().next_version();
    }

    pub fn get_timing_distribution_snapshot(&self) -> HashMap<i32, i64> {
        self.state.timing_distribution_snapshot()
    }

    pub fn replace_timing_distribution_snapshot(&self, timing_distribution_snapshot: HashMap<i32, i64>) {
        self.state
            .replace_timing_distribution_snapshot(timing_distribution_snapshot, current_millis() as i64);
        self.data_version.lock().next_version();
    }

    pub fn timer_dist_list(&self) -> Vec<i32> {
        self.state.timer_dist()
    }

    pub fn set_timer_dist_list(&self, timer_dist: Vec<i32>) {
        self.state.set_timer_dist(timer_dist);
        self.data_version.lock().next_version();
    }
}

impl TimerMetricsSerializeWrapper {
    pub fn data_version(&self) -> &DataVersion {
        &self.data_version
    }

    pub fn timing_count_snapshot(&self) -> HashMap<String, i64> {
        self.timing_count
            .iter()
            .filter_map(|(topic, metric)| (metric.count > 0).then_some((topic.clone(), metric.count)))
            .collect()
    }

    pub fn timing_distribution_snapshot(&self) -> HashMap<i32, i64> {
        self.timing_distribution
            .iter()
            .filter_map(|(period, metric)| (metric.count > 0).then_some((*period, metric.count)))
            .collect()
    }

    pub fn timer_dist(&self) -> &[i32] {
        &self.timer_dist
    }
}
