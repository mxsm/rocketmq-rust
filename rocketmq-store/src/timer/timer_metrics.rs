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
use rocketmq_remoting::protocol::DataVersion;
use serde::Deserialize;
use serde::Serialize;

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
    data_version: DataVersion,
}

pub struct TimerMetrics {
    config_path: Mutex<Option<String>>,
    timing_count: RwLock<HashMap<String, Metric>>,
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
            *self.data_version.lock() = wrapper.data_version;
        }
    }
}

impl TimerMetrics {
    pub fn new(config_path: Option<String>) -> Self {
        Self {
            config_path: Mutex::new(config_path),
            timing_count: RwLock::new(HashMap::new()),
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
}
