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

use rocketmq_common::common::mix_all;
use serde::Serialize;
use tracing::warn;

pub struct ColdDataCgCtrService {
    cold_data_flow_control_enable: bool,
    /// Tracks cold data read bytes per consumer group
    cg_cold_read_threshold: RwLock<HashMap<String, i64>>,
    /// Accumulated cold data bytes per consumer group
    cg_cold_acc: RwLock<HashMap<String, AtomicI64>>,
}

impl ColdDataCgCtrService {
    pub fn new(cold_data_flow_control_enable: bool) -> Self {
        Self {
            cold_data_flow_control_enable,
            cg_cold_read_threshold: RwLock::new(HashMap::new()),
            cg_cold_acc: RwLock::new(HashMap::new()),
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
            runtime_table: BTreeMap<String, i64>,
            #[serde(rename = "configTable")]
            config_table: BTreeMap<String, i64>,
            #[serde(rename = "coldDataFlowControlEnable")]
            cold_data_flow_control_enable: bool,
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
            .map(|(group, acc)| (group.clone(), acc.load(Ordering::Relaxed)))
            .collect::<BTreeMap<_, _>>();

        serde_json::to_string(&ColdDataFlowCtrInfo {
            runtime_table,
            config_table,
            cold_data_flow_control_enable: self.cold_data_flow_control_enable,
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

        assert!(!service.is_cg_need_cold_data_flow_ctr("CID_RMQ_SYS_GROUP_A"));
    }

    #[test]
    fn get_cold_data_flow_ctr_info_serializes_config_and_runtime_tables() {
        let service = ColdDataCgCtrService::new(true);
        service.add_or_update_group_config("group-a", 128);
        service.cold_acc("group-a", 32);

        let value: serde_json::Value =
            serde_json::from_str(&service.get_cold_data_flow_ctr_info()).expect("decode cold data info");

        assert_eq!(value["configTable"]["group-a"], 128);
        assert_eq!(value["runtimeTable"]["group-a"], 32);
        assert_eq!(value["coldDataFlowControlEnable"], true);
    }
}
