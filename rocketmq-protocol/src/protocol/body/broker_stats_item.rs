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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_stats_item_default() {
        let item = BrokerStatsItem::default();
        assert_eq!(item.get_sum(), 0);
        assert_eq!(item.get_tps(), 0.0);
        assert_eq!(item.get_avgpt(), 0.0);
    }

    #[test]
    fn broker_stats_item_new() {
        let item = BrokerStatsItem::new(100, 10.5, 1.2);
        assert_eq!(item.get_sum(), 100);
        assert_eq!(item.get_tps(), 10.5);
        assert_eq!(item.get_avgpt(), 1.2);
    }

    #[test]
    fn broker_stats_item_setters() {
        let mut item = BrokerStatsItem::default();
        item.set_sum(200);
        item.set_tps(20.5);
        item.set_avgpt(2.2);
        assert_eq!(item.get_sum(), 200);
        assert_eq!(item.get_tps(), 20.5);
        assert_eq!(item.get_avgpt(), 2.2);
    }

    #[test]
    fn broker_stats_item_serde() {
        let item = BrokerStatsItem::new(100, 10.5, 1.2);
        let json = serde_json::to_string(&item).unwrap();
        let expected = r#"{"sum":100,"tps":10.5,"avgpt":1.2}"#;
        assert_eq!(json, expected);
        let deserialized: BrokerStatsItem = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.get_sum(), 100);
        assert_eq!(deserialized.get_tps(), 10.5);
        assert_eq!(deserialized.get_avgpt(), 1.2);
    }
}
