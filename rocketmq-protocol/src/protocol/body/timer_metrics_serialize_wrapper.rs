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
            time_stamp: 0,
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    fn metric(count: u64, time_stamp: u64) -> Metric {
        Metric {
            count: AtomicU64::new(count),
            time_stamp,
        }
    }

    #[test]
    fn wrapper_methods_cover_builders_and_mutable_accessors() {
        let default_metric = Metric::default();
        assert_eq!(default_metric.count.load(Ordering::Relaxed), 0);
        assert_eq!(default_metric.time_stamp, 0);

        let empty = TimerMetricsSerializeWrapper::new();
        assert!(empty.timing_count().is_empty());
        assert!(empty.timing_count().capacity() >= 1024);

        let key = CheetahString::from_static_str("metric-a");
        let mut data_version = DataVersion::default();
        data_version.set_state_version(7);
        let mut wrapper = TimerMetricsSerializeWrapper::new()
            .with_timing_count(HashMap::from([(key.clone(), metric(10, 1000))]))
            .with_data_version(data_version);

        assert!(wrapper.timing_count().capacity() >= 1);
        assert_eq!(wrapper.data_version().state_version(), 7);
        assert_eq!(
            wrapper
                .get_metric(&key)
                .expect("inserted metric")
                .count
                .load(Ordering::Relaxed),
            10
        );
        assert!(wrapper.get_metric(&CheetahString::from_static_str("missing")).is_none());

        wrapper
            .get_metric_mut(&key)
            .expect("mutable inserted metric")
            .time_stamp = 2000;
        wrapper.data_version_mut().set_state_version(8);
        wrapper
            .timing_count_mut()
            .insert(CheetahString::from_static_str("metric-b"), metric(20, 3000));
        wrapper.insert_metric(CheetahString::from_static_str("metric-c"), metric(30, 4000));

        assert_eq!(wrapper.get_metric(&key).expect("updated metric").time_stamp, 2000);
        assert_eq!(wrapper.data_version().state_version(), 8);
        assert_eq!(wrapper.timing_count().len(), 3);
    }

    #[test]
    fn metric_display_reports_the_current_values() {
        assert_eq!(
            metric(123, 9876543210).to_string(),
            "Metric { count: 123, time_stamp: 9876543210 }"
        );
    }

    #[test]
    fn serde_contract_preserves_metric_and_version_fields() {
        let key = CheetahString::from_static_str("metric-a");
        let mut data_version = DataVersion::default();
        data_version.set_state_version(7);
        data_version.set_timestamp(12345);
        data_version.set_counter(3);
        let wrapper = TimerMetricsSerializeWrapper::new()
            .with_timing_count(HashMap::from([(key.clone(), metric(42, 1000))]))
            .with_data_version(data_version);

        let value = serde_json::to_value(&wrapper).expect("serialize timer metrics");
        assert_eq!(value["timingCount"]["metric-a"]["count"], 42);
        assert_eq!(value["timingCount"]["metric-a"]["timeStamp"], 1000);
        assert_eq!(value["dataVersion"]["stateVersion"], 7);

        let decoded: TimerMetricsSerializeWrapper = serde_json::from_value(value).expect("deserialize timer metrics");
        let decoded_metric = decoded.get_metric(&key).expect("decoded metric");
        assert_eq!(decoded_metric.count.load(Ordering::Relaxed), 42);
        assert_eq!(decoded_metric.time_stamp, 1000);
        assert_eq!(decoded.data_version().state_version(), 7);
        assert_eq!(decoded.data_version().timestamp(), 12345);
        assert_eq!(decoded.data_version().counter(), 3);
    }
}
