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
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::RwLock;

use tracing::warn;

#[derive(Default)]
pub struct ColdDataCgCtrService {
    /// Tracks cold data read bytes per consumer group
    cg_cold_read_threshold: RwLock<HashMap<String, i64>>,
    /// Accumulated cold data bytes per consumer group
    cg_cold_acc: RwLock<HashMap<String, AtomicI64>>,
}

impl ColdDataCgCtrService {
    pub fn start(&mut self) {
        warn!("ColdDataCgCtrService started not implemented");
    }

    /// Check if a consumer group needs cold data flow control
    pub fn is_cg_need_cold_data_flow_ctr(&self, consumer_group: &str) -> bool {
        let threshold_map = self.cg_cold_read_threshold.read().unwrap();
        threshold_map.contains_key(consumer_group)
    }

    /// Accumulate cold data read bytes for a consumer group
    pub fn cold_acc(&self, consumer_group: &str, cold_data_sum: i64) {
        if cold_data_sum <= 0 {
            return;
        }

        let acc_map = self.cg_cold_acc.read().unwrap();
        if let Some(counter) = acc_map.get(consumer_group) {
            counter.fetch_add(cold_data_sum, Ordering::Relaxed);
        } else {
            drop(acc_map);
            let mut acc_map = self.cg_cold_acc.write().unwrap();
            acc_map
                .entry(consumer_group.to_string())
                .or_insert_with(|| AtomicI64::new(0))
                .fetch_add(cold_data_sum, Ordering::Relaxed);
        }
    }

    /// Add a consumer group to cold data flow control
    pub fn add_cg_cold_read_threshold(&self, consumer_group: &str, threshold: i64) {
        let mut threshold_map = self.cg_cold_read_threshold.write().unwrap();
        threshold_map.insert(consumer_group.to_string(), threshold);
    }

    /// Remove a consumer group from cold data flow control
    pub fn remove_cg_cold_read_threshold(&self, consumer_group: &str) {
        let mut threshold_map = self.cg_cold_read_threshold.write().unwrap();
        threshold_map.remove(consumer_group);
    }

    pub fn shutdown(&mut self) {
        warn!("ColdDataCgCtrService shutdown not implemented");
    }
}
