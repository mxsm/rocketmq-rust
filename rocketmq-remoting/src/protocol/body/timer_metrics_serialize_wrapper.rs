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

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn test_metric_default_initialization() {
        let metric = Metric::default();
        assert_eq!(metric.count.load(Ordering::Relaxed), 0);
        assert!(metric.time_stamp > 0);
    }

    #[test]
    fn test_metric_custom_initialization() {
        let metric = Metric {
            count: AtomicU64::new(42),
            time_stamp: 1234567890,
        };
        assert_eq!(metric.count.load(Ordering::Relaxed), 42);
        assert_eq!(metric.time_stamp, 1234567890);
    }

    #[test]
    fn test_metric_with_zero_values() {
        let metric = Metric {
            count: AtomicU64::new(0),
            time_stamp: 0,
        };
        assert_eq!(metric.count.load(Ordering::Relaxed), 0);
        assert_eq!(metric.time_stamp, 0);
    }

    #[test]
    fn test_metric_with_large_values() {
        let large_count = u64::MAX;
        let large_timestamp = u64::MAX;

        let metric = Metric {
            count: AtomicU64::new(large_count),
            time_stamp: large_timestamp,
        };

        assert_eq!(metric.count.load(Ordering::Relaxed), large_count);
        assert_eq!(metric.time_stamp, large_timestamp);
    }

    #[test]
    fn test_metric_counter_operations() {
        let metric = Metric {
            count: AtomicU64::new(0),
            time_stamp: 1000,
        };

        metric.count.fetch_add(1, Ordering::Relaxed);
        assert_eq!(metric.count.load(Ordering::Relaxed), 1);

        metric.count.fetch_add(99, Ordering::Relaxed);
        assert_eq!(metric.count.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_metric_gauge_operations() {
        let metric = Metric {
            count: AtomicU64::new(50),
            time_stamp: 2000,
        };

        metric.count.store(75, Ordering::Relaxed);
        assert_eq!(metric.count.load(Ordering::Relaxed), 75);

        metric.count.store(25, Ordering::Relaxed);
        assert_eq!(metric.count.load(Ordering::Relaxed), 25);
    }

    #[test]
    fn test_metric_display_trait() {
        let metric = Metric {
            count: AtomicU64::new(123),
            time_stamp: 9876543210,
        };

        let display_string = format!("{}", metric);
        assert!(display_string.contains("123"));
        assert!(display_string.contains("9876543210"));
    }

    #[test]
    fn test_metric_serialization_to_json() {
        let metric = Metric {
            count: AtomicU64::new(100),
            time_stamp: 1234567890,
        };

        let json = serde_json::to_string(&metric).expect("Failed to serialize Metric");
        assert!(json.contains("\"count\":100"));
        assert!(json.contains("\"timeStamp\":1234567890"));
    }

    #[test]
    fn test_metric_deserialization_from_json() {
        let json = r#"{"count":250,"timeStamp":9876543210}"#;

        let metric: Metric = serde_json::from_str(json).expect("Failed to deserialize Metric");
        assert_eq!(metric.count.load(Ordering::Relaxed), 250);
        assert_eq!(metric.time_stamp, 9876543210);
    }

    #[test]
    fn test_metric_serialization_deserialization_roundtrip() {
        let original = Metric {
            count: AtomicU64::new(999),
            time_stamp: 1111111111,
        };

        let json = serde_json::to_string(&original).expect("Failed to serialize");
        let deserialized: Metric = serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(
            original.count.load(Ordering::Relaxed),
            deserialized.count.load(Ordering::Relaxed)
        );
        assert_eq!(original.time_stamp, deserialized.time_stamp);
    }

    #[test]
    fn test_timer_metrics_wrapper_default_initialization() {
        let wrapper = TimerMetricsSerializeWrapper::default();
        assert_eq!(wrapper.timing_count.len(), 0);
        assert!(wrapper.timing_count.capacity() >= 1024);
    }

    #[test]
    fn test_timer_metrics_wrapper_new() {
        let wrapper = TimerMetricsSerializeWrapper::new();
        assert_eq!(wrapper.timing_count.len(), 0);
    }

    #[test]
    fn test_timer_metrics_wrapper_builder_pattern() {
        let mut timing_count = HashMap::new();
        timing_count.insert(
            CheetahString::from_static_str("test_metric"),
            Metric {
                count: AtomicU64::new(42),
                time_stamp: 1000,
            },
        );

        let data_version = DataVersion::default();

        let wrapper = TimerMetricsSerializeWrapper::new()
            .with_timing_count(timing_count)
            .with_data_version(data_version);

        assert_eq!(wrapper.timing_count.len(), 1);
        assert!(wrapper
            .timing_count
            .contains_key(&CheetahString::from_static_str("test_metric")));
    }

    #[test]
    fn test_timer_metrics_wrapper_getter_methods() {
        let mut wrapper = TimerMetricsSerializeWrapper::new();
        let key = CheetahString::from_static_str("metric1");
        let metric = Metric {
            count: AtomicU64::new(10),
            time_stamp: 5000,
        };

        wrapper.insert_metric(key.clone(), metric);

        let timing_count = wrapper.timing_count();
        assert_eq!(timing_count.len(), 1);

        let _data_version = wrapper.data_version();

        let retrieved_metric = wrapper.get_metric(&key);
        assert!(retrieved_metric.is_some());
        assert_eq!(retrieved_metric.unwrap().count.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn test_timer_metrics_wrapper_mutable_getter_methods() {
        let mut wrapper = TimerMetricsSerializeWrapper::new();
        let key = CheetahString::from_static_str("metric2");
        let metric = Metric {
            count: AtomicU64::new(20),
            time_stamp: 6000,
        };

        wrapper.insert_metric(key.clone(), metric);

        let timing_count_mut = wrapper.timing_count_mut();
        assert_eq!(timing_count_mut.len(), 1);

        let _data_version_mut = wrapper.data_version_mut();

        let metric_mut = wrapper.get_metric_mut(&key);
        assert!(metric_mut.is_some());
        if let Some(m) = metric_mut {
            m.count.store(30, Ordering::Relaxed);
        }

        assert_eq!(wrapper.get_metric(&key).unwrap().count.load(Ordering::Relaxed), 30);
    }

    #[test]
    fn test_timer_metrics_wrapper_insert_and_get_metric() {
        let mut wrapper = TimerMetricsSerializeWrapper::new();

        let key1 = CheetahString::from_static_str("counter");
        let metric1 = Metric {
            count: AtomicU64::new(100),
            time_stamp: 7000,
        };

        let key2 = CheetahString::from_static_str("gauge");
        let metric2 = Metric {
            count: AtomicU64::new(200),
            time_stamp: 8000,
        };

        wrapper.insert_metric(key1.clone(), metric1);
        wrapper.insert_metric(key2.clone(), metric2);

        assert_eq!(wrapper.timing_count().len(), 2);

        let retrieved1 = wrapper.get_metric(&key1).unwrap();
        assert_eq!(retrieved1.count.load(Ordering::Relaxed), 100);

        let retrieved2 = wrapper.get_metric(&key2).unwrap();
        assert_eq!(retrieved2.count.load(Ordering::Relaxed), 200);
    }

    #[test]
    fn test_timer_metrics_wrapper_get_nonexistent_metric() {
        let wrapper = TimerMetricsSerializeWrapper::new();
        let key = CheetahString::from_static_str("nonexistent");

        let result = wrapper.get_metric(&key);
        assert!(result.is_none());
    }

    #[test]
    fn test_timer_metrics_wrapper_serialization_to_json() {
        let mut wrapper = TimerMetricsSerializeWrapper::new();

        let key = CheetahString::from_static_str("test");
        let metric = Metric {
            count: AtomicU64::new(50),
            time_stamp: 9000,
        };

        wrapper.insert_metric(key, metric);

        let json = serde_json::to_string(&wrapper).expect("Failed to serialize wrapper");
        assert!(json.contains("timingCount"));
        assert!(json.contains("dataVersion"));
    }

    #[test]
    fn test_timer_metrics_wrapper_deserialization_from_json() {
        let json = r#"{
            "timingCount": {
                "metric1": {
                    "count": 75,
                    "timeStamp": 10000
                }
            },
            "dataVersion": {
                "counter": 0,
                "stateVersion": 0,
                "timestamp": 0
            }
        }"#;

        let wrapper: TimerMetricsSerializeWrapper = serde_json::from_str(json).expect("Failed to deserialize wrapper");

        assert_eq!(wrapper.timing_count().len(), 1);
        let metric = wrapper.get_metric(&CheetahString::from_static_str("metric1")).unwrap();
        assert_eq!(metric.count.load(Ordering::Relaxed), 75);
        assert_eq!(metric.time_stamp, 10000);
    }

    #[test]
    fn test_timer_metrics_wrapper_serialization_deserialization_roundtrip() {
        let mut original = TimerMetricsSerializeWrapper::new();

        let key1 = CheetahString::from_static_str("counter1");
        let metric1 = Metric {
            count: AtomicU64::new(111),
            time_stamp: 11111,
        };

        let key2 = CheetahString::from_static_str("counter2");
        let metric2 = Metric {
            count: AtomicU64::new(222),
            time_stamp: 22222,
        };

        original.insert_metric(key1.clone(), metric1);
        original.insert_metric(key2.clone(), metric2);

        let json = serde_json::to_string(&original).expect("Failed to serialize");
        let deserialized: TimerMetricsSerializeWrapper = serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(original.timing_count().len(), deserialized.timing_count().len());

        let original_metric1 = original.get_metric(&key1).unwrap();
        let deserialized_metric1 = deserialized.get_metric(&key1).unwrap();
        assert_eq!(
            original_metric1.count.load(Ordering::Relaxed),
            deserialized_metric1.count.load(Ordering::Relaxed)
        );
        assert_eq!(original_metric1.time_stamp, deserialized_metric1.time_stamp);
    }

    #[test]
    fn test_timer_metrics_wrapper_with_empty_timing_count() {
        let wrapper = TimerMetricsSerializeWrapper::new();

        let json = serde_json::to_string(&wrapper).expect("Failed to serialize");
        let deserialized: TimerMetricsSerializeWrapper = serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(deserialized.timing_count().len(), 0);
    }

    #[test]
    fn test_timer_metrics_wrapper_with_multiple_metrics() {
        let mut wrapper = TimerMetricsSerializeWrapper::new();

        for i in 0..10 {
            let key = CheetahString::from(format!("metric_{}", i));
            let metric = Metric {
                count: AtomicU64::new(i * 10),
                time_stamp: i * 1000,
            };
            wrapper.insert_metric(key, metric);
        }

        assert_eq!(wrapper.timing_count().len(), 10);

        for i in 0..10 {
            let key = CheetahString::from(format!("metric_{}", i));
            let metric = wrapper.get_metric(&key).unwrap();
            assert_eq!(metric.count.load(Ordering::Relaxed), i * 10);
            assert_eq!(metric.time_stamp, i * 1000);
        }
    }

    #[test]
    fn test_metric_with_boundary_values() {
        let metric_min = Metric {
            count: AtomicU64::new(u64::MIN),
            time_stamp: u64::MIN,
        };
        assert_eq!(metric_min.count.load(Ordering::Relaxed), 0);
        assert_eq!(metric_min.time_stamp, 0);

        let metric_max = Metric {
            count: AtomicU64::new(u64::MAX),
            time_stamp: u64::MAX,
        };
        assert_eq!(metric_max.count.load(Ordering::Relaxed), u64::MAX);
        assert_eq!(metric_max.time_stamp, u64::MAX);
    }

    #[test]
    fn test_metric_atomic_operations_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let metric = Arc::new(Metric {
            count: AtomicU64::new(0),
            time_stamp: 1000,
        });

        let num_threads = 10;
        let increments_per_thread = 1000;
        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let m = Arc::clone(&metric);
                thread::spawn(move || {
                    for _ in 0..increments_per_thread {
                        m.count.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            metric.count.load(Ordering::Relaxed),
            num_threads * increments_per_thread
        );
    }
}
