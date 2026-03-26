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
use parking_lot::RwLock;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::DataVersion;
use serde::Deserialize;
use serde::Serialize;

fn default_timer_dist() -> Vec<i32> {
    vec![5, 60, 300, 900, 3600, 14_400, 28_800, 86_400]
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Metric {
    count: i64,
    time_stamp: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct TimerMetricsSerializeWrapper {
    timing_count: HashMap<String, Metric>,
    #[serde(default)]
    timing_distribution: HashMap<i32, Metric>,
    #[serde(default = "default_timer_dist")]
    timer_dist: Vec<i32>,
    data_version: DataVersion,
}

pub struct TimerMetrics {
    config_path: Mutex<Option<String>>,
    timing_count: RwLock<HashMap<String, Metric>>,
    timing_distribution: RwLock<HashMap<i32, Metric>>,
    timer_dist: RwLock<Vec<i32>>,
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
        let wrapper = TimerMetricsSerializeWrapper {
            timing_count: self.timing_count.read().clone(),
            timing_distribution: self.timing_distribution.read().clone(),
            timer_dist: self.timer_dist.read().clone(),
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
            *self.timing_count.write() = wrapper.timing_count;
            *self.timing_distribution.write() = wrapper.timing_distribution;
            *self.timer_dist.write() = wrapper.timer_dist;
            *self.data_version.lock() = wrapper.data_version;
        }
    }
}

impl TimerMetrics {
    pub fn new(config_path: Option<String>) -> Self {
        Self {
            config_path: Mutex::new(config_path),
            timing_count: RwLock::new(HashMap::new()),
            timing_distribution: RwLock::new(HashMap::new()),
            timer_dist: RwLock::new(default_timer_dist()),
            data_version: Mutex::new(DataVersion::new()),
        }
    }

    pub fn set_config_path(&mut self, config_path: Option<String>) {
        *self.config_path.get_mut() = config_path;
    }

    pub fn get_timing_count(&self, key: &CheetahString) -> i64 {
        self.timing_count
            .read()
            .get(key.as_str())
            .map(|metric| metric.count)
            .unwrap_or_default()
    }

    pub fn get_timing_count_snapshot(&self) -> HashMap<String, i64> {
        self.timing_count
            .read()
            .iter()
            .filter_map(|(topic, metric)| (metric.count > 0).then_some((topic.clone(), metric.count)))
            .collect()
    }

    pub fn replace_timing_count_snapshot(&self, timing_count_snapshot: HashMap<String, i64>) {
        let mut timing_count = self.timing_count.write();
        timing_count.clear();
        for (topic, count) in timing_count_snapshot {
            if count <= 0 {
                continue;
            }
            timing_count.insert(
                topic,
                Metric {
                    count,
                    time_stamp: current_millis() as i64,
                },
            );
        }
        self.data_version.lock().next_version();
    }

    pub fn add_timing_count(&self, key: &CheetahString, delta: i64) {
        let mut timing_count = self.timing_count.write();
        let entry = timing_count.entry(key.to_string()).or_insert(Metric {
            count: 0,
            time_stamp: 0,
        });
        entry.count = (entry.count + delta).max(0);
        entry.time_stamp = current_millis() as i64;
        self.data_version.lock().next_version();
    }

    pub fn get_timing_distribution_snapshot(&self) -> HashMap<i32, i64> {
        self.timing_distribution
            .read()
            .iter()
            .filter_map(|(period, metric)| (metric.count > 0).then_some((*period, metric.count)))
            .collect()
    }

    pub fn replace_timing_distribution_snapshot(&self, timing_distribution_snapshot: HashMap<i32, i64>) {
        let mut timing_distribution = self.timing_distribution.write();
        timing_distribution.clear();
        for (period, count) in timing_distribution_snapshot {
            timing_distribution.insert(
                period,
                Metric {
                    count: count.max(0),
                    time_stamp: current_millis() as i64,
                },
            );
        }
        self.data_version.lock().next_version();
    }

    pub fn timer_dist_list(&self) -> Vec<i32> {
        self.timer_dist.read().clone()
    }

    pub fn set_timer_dist_list(&self, timer_dist: Vec<i32>) {
        *self.timer_dist.write() = timer_dist;
        self.data_version.lock().next_version();
    }
}
