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
use rocketmq_common::common::config_manager::ConfigManager;

pub struct TimerMetrics;

impl ConfigManager for TimerMetrics {
    fn config_file_path(&self) -> String {
        todo!()
    }

    fn encode_pretty(&self, pretty_format: bool) -> String {
        todo!()
    }

    fn decode(&self, json_string: &str) {
        todo!()
    }
}

impl TimerMetrics {
    pub fn get_timing_count(&self, key: &CheetahString) -> i64 {
        unimplemented!("get_timing_count is not implemented")
    }
}
