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

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BrokerStatsItem {
    sum: u64,
    tps: f64,
    avgpt: f64,
}

impl BrokerStatsItem {
    // Constructor
    pub fn new(sum: u64, tps: f64, avgpt: f64) -> Self {
        Self { sum, tps, avgpt }
    }

    // Getter and Setter for `sum`
    pub fn get_sum(&self) -> u64 {
        self.sum
    }

    pub fn set_sum(&mut self, sum: u64) {
        self.sum = sum;
    }

    // Getter and Setter for `tps`
    pub fn get_tps(&self) -> f64 {
        self.tps
    }

    pub fn set_tps(&mut self, tps: f64) {
        self.tps = tps;
    }

    // Getter and Setter for `avgpt`
    pub fn get_avgpt(&self) -> f64 {
        self.avgpt
    }

    pub fn set_avgpt(&mut self, avgpt: f64) {
        self.avgpt = avgpt;
    }
}
