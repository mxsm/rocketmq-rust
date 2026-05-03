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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::RwLock;
use std::time::SystemTime;

use rocketmq_common::common::mix_all;
use serde::Serialize;
use tracing::warn;

const DEFAULT_CG_COLD_READ_THRESHOLD: i64 = 3 * 1024 * 1024;
const DEFAULT_GLOBAL_COLD_READ_THRESHOLD: i64 = 100 * 1024 * 1024;

struct ColdDataAccumulator {
    cold_acc: AtomicI64,
    create_time_mills: i64,
    last_cold_read_time_mills: AtomicI64,
}

impl ColdDataAccumulator {
    fn new(cold_data_sum: i64, now_millis: i64) -> Self {
        Self {
            cold_acc: AtomicI64::new(cold_data_sum),
            create_time_mills: now_millis,
            last_cold_read_time_mills: AtomicI64::new(now_millis),
        }
    }

    fn add(&self, cold_data_sum: i64, now_millis: i64) {
        self.cold_acc.fetch_add(cold_data_sum, Ordering::Relaxed);
        self.last_cold_read_time_mills.store(now_millis, Ordering::Relaxed);
    }

    fn cold_acc(&self) -> i64 {
        self.cold_acc.load(Ordering::Relaxed)
    }

    fn snapshot(&self) -> ColdDataAccumulatorSnapshot {
        ColdDataAccumulatorSnapshot {
            cold_acc: self.cold_acc(),
            create_time_mills: self.create_time_mills,
            last_cold_read_time_mills: self.last_cold_read_time_mills.load(Ordering::Relaxed),
        }
    }
}

#[derive(Serialize)]
struct ColdDataAccumulatorSnapshot {
    #[serde(rename = "coldAcc")]
    cold_acc: i64,
    #[serde(rename = "createTimeMills")]
    create_time_mills: i64,
    #[serde(rename = "lastColdReadTimeMills")]
    last_cold_read_time_mills: i64,
}

fn current_time_millis() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

pub struct ColdDataCgCtrService {
    cold_data_flow_control_enable: bool,
    /// Tracks cold data read bytes per consumer group
    cg_cold_read_threshold: RwLock<HashMap<String, i64>>,
    /// Accumulated cold data bytes per consumer group
    cg_cold_acc: RwLock<HashMap<String, ColdDataAccumulator>>,
    global_acc: AtomicI64,
}

impl ColdDataCgCtrService {
    pub fn new(cold_data_flow_control_enable: bool) -> Self {
        Self {
            cold_data_flow_control_enable,
            cg_cold_read_threshold: RwLock::new(HashMap::new()),
            cg_cold_acc: RwLock::new(HashMap::new()),
            global_acc: AtomicI64::new(0),
        }
    }

    pub fn start(&mut self) {
        warn!("ColdDataCgCtrService started not implemented");
    }

    /// Check if a consumer group needs cold data flow control
    pub fn is_cg_need_cold_data_flow_ctr(&self, consumer_group: &str) -> bool {
        if !self.cold_data_flow_control_enable || mix_all::is_sys_consumer_group_for_no_cold_read_limit(consumer_group)
        {
            return false;
        }

        let cold_acc = {
            let acc_map = self.cg_cold_acc.read().unwrap();
            let Some(counter) = acc_map.get(consumer_group) else {
                return false;
            };
            counter.cold_acc()
        };

        let threshold_map = self.cg_cold_read_threshold.read().unwrap();
        let threshold = threshold_map
            .get(consumer_group)
            .copied()
            .unwrap_or(DEFAULT_CG_COLD_READ_THRESHOLD);
        cold_acc >= threshold || self.global_acc.load(Ordering::Relaxed) >= DEFAULT_GLOBAL_COLD_READ_THRESHOLD
    }

    /// Accumulate cold data read bytes for a consumer group
    pub fn cold_acc(&self, consumer_group: &str, cold_data_sum: i64) {
        if cold_data_sum <= 0 {
            return;
        }

        self.global_acc.fetch_add(cold_data_sum, Ordering::Relaxed);
        let now_millis = current_time_millis();
        let acc_map = self.cg_cold_acc.read().unwrap();
        if let Some(counter) = acc_map.get(consumer_group) {
            counter.add(cold_data_sum, now_millis);
        } else {
            drop(acc_map);
            let mut acc_map = self.cg_cold_acc.write().unwrap();
            acc_map
                .entry(consumer_group.to_string())
                .and_modify(|counter| counter.add(cold_data_sum, now_millis))
                .or_insert_with(|| ColdDataAccumulator::new(cold_data_sum, now_millis));
        }
    }

    /// Add a consumer group to cold data flow control
    pub fn add_cg_cold_read_threshold(&self, consumer_group: &str, threshold: i64) {
        let mut threshold_map = self.cg_cold_read_threshold.write().unwrap();
        threshold_map.insert(consumer_group.to_string(), threshold);
    }

    pub fn add_or_update_group_config(&self, consumer_group: &str, threshold: i64) {
        self.add_cg_cold_read_threshold(consumer_group, threshold);
    }

    /// Remove a consumer group from cold data flow control
    pub fn remove_cg_cold_read_threshold(&self, consumer_group: &str) {
        let mut threshold_map = self.cg_cold_read_threshold.write().unwrap();
        threshold_map.remove(consumer_group);
    }

    pub fn remove_group_config(&self, consumer_group: &str) {
        self.remove_cg_cold_read_threshold(consumer_group);
    }

    pub fn get_cold_data_flow_ctr_info(&self) -> String {
        #[derive(Serialize)]
        struct ColdDataFlowCtrInfo {
            #[serde(rename = "runtimeTable")]
            runtime_table: BTreeMap<String, ColdDataAccumulatorSnapshot>,
            #[serde(rename = "configTable")]
            config_table: BTreeMap<String, i64>,
            #[serde(rename = "coldDataFlowControlEnable")]
            cold_data_flow_control_enable: bool,
            #[serde(rename = "cgColdReadThreshold")]
            cg_cold_read_threshold: i64,
            #[serde(rename = "globalColdReadThreshold")]
            global_cold_read_threshold: i64,
            #[serde(rename = "globalAcc")]
            global_acc: i64,
        }

        let config_table = self
            .cg_cold_read_threshold
            .read()
            .unwrap()
            .iter()
            .map(|(group, threshold)| (group.clone(), *threshold))
            .collect::<BTreeMap<_, _>>();
        let runtime_table = self
            .cg_cold_acc
            .read()
            .unwrap()
            .iter()
            .map(|(group, acc)| (group.clone(), acc.snapshot()))
            .collect::<BTreeMap<_, _>>();

        serde_json::to_string(&ColdDataFlowCtrInfo {
            runtime_table,
            config_table,
            cold_data_flow_control_enable: self.cold_data_flow_control_enable,
            cg_cold_read_threshold: DEFAULT_CG_COLD_READ_THRESHOLD,
            global_cold_read_threshold: DEFAULT_GLOBAL_COLD_READ_THRESHOLD,
            global_acc: self.global_acc.load(Ordering::Relaxed),
        })
        .expect("serialize cold data flow ctr info")
    }

    pub fn shutdown(&mut self) {
        warn!("ColdDataCgCtrService shutdown not implemented");
    }
}

impl Default for ColdDataCgCtrService {
    fn default() -> Self {
        Self::new(false)
    }
}

#[cfg(test)]
mod tests {
    use super::ColdDataCgCtrService;

    #[test]
    fn system_consumer_group_never_needs_cold_data_flow_control() {
        let service = ColdDataCgCtrService::new(true);
        service.add_or_update_group_config("CID_RMQ_SYS_GROUP_A", 128);
        service.cold_acc("CID_RMQ_SYS_GROUP_A", 128);

        assert!(!service.is_cg_need_cold_data_flow_ctr("CID_RMQ_SYS_GROUP_A"));
    }

    #[test]
    fn group_needs_cold_data_flow_control_only_after_threshold_is_reached() {
        let service = ColdDataCgCtrService::new(true);
        service.add_or_update_group_config("group-a", 128);

        assert!(!service.is_cg_need_cold_data_flow_ctr("group-a"));

        service.cold_acc("group-a", 127);
        assert!(!service.is_cg_need_cold_data_flow_ctr("group-a"));

        service.cold_acc("group-a", 1);
        assert!(service.is_cg_need_cold_data_flow_ctr("group-a"));
    }

    #[test]
    fn get_cold_data_flow_ctr_info_serializes_config_and_runtime_tables() {
        let service = ColdDataCgCtrService::new(true);
        service.add_or_update_group_config("group-a", 128);
        service.cold_acc("group-a", 32);

        let value: serde_json::Value =
            serde_json::from_str(&service.get_cold_data_flow_ctr_info()).expect("decode cold data info");

        assert_eq!(value["configTable"]["group-a"], 128);
        assert_eq!(value["runtimeTable"]["group-a"]["coldAcc"], 32);
        assert!(
            value["runtimeTable"]["group-a"]["createTimeMills"]
                .as_i64()
                .unwrap_or(0)
                > 0
        );
        assert!(
            value["runtimeTable"]["group-a"]["lastColdReadTimeMills"]
                .as_i64()
                .unwrap_or(0)
                > 0
        );
        assert_eq!(value["cgColdReadThreshold"], 3 * 1024 * 1024);
        assert_eq!(value["globalColdReadThreshold"], 100 * 1024 * 1024);
        assert_eq!(value["globalAcc"], 32);
        assert_eq!(value["coldDataFlowControlEnable"], true);
    }
}
