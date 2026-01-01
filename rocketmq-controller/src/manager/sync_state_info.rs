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

use std::collections::HashSet;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize)]
struct SyncStateInfoDef {
    cluster_name: String,
    broker_name: String,
    master_epoch: i32,
    sync_state_set_epoch: i32,
    sync_state_set: HashSet<u64>,
    master_broker_id: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct SyncStateInfo {
    cluster_name: CheetahString,
    broker_name: CheetahString,
    master_epoch: Arc<AtomicI32>,
    sync_state_set_epoch: Arc<AtomicI32>,
    sync_state_set: HashSet<u64>,
    master_broker_id: Option<u64>,
}

impl SyncStateInfo {
    pub fn new(cluster_name: impl Into<CheetahString>, broker_name: impl Into<CheetahString>) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_name: broker_name.into(),
            master_epoch: Arc::new(AtomicI32::new(0)),
            sync_state_set_epoch: Arc::new(AtomicI32::new(0)),
            sync_state_set: HashSet::new(),
            master_broker_id: None,
        }
    }

    pub fn update_master_info(&mut self, master_broker_id: u64) {
        self.master_broker_id = Some(master_broker_id);
        self.master_epoch.fetch_add(1, Ordering::SeqCst);
    }

    pub fn update_master_info_to_none(&mut self) {
        self.master_broker_id = None;
    }

    pub fn update_sync_state_set_info(&mut self, new_sync_state_set: &HashSet<u64>) {
        self.sync_state_set = new_sync_state_set.clone();
        self.sync_state_set_epoch.fetch_add(1, Ordering::SeqCst);
    }

    pub fn is_first_time_for_elect(&self) -> bool {
        self.master_epoch.load(Ordering::SeqCst) == 0
    }

    pub fn is_master_exist(&self) -> bool {
        self.master_broker_id.is_some()
    }

    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    pub fn sync_state_set(&self) -> HashSet<u64> {
        self.sync_state_set.clone()
    }

    pub fn sync_state_set_epoch(&self) -> i32 {
        self.sync_state_set_epoch.load(Ordering::SeqCst)
    }

    pub fn master_broker_id(&self) -> Option<u64> {
        self.master_broker_id
    }

    pub fn master_epoch(&self) -> i32 {
        self.master_epoch.load(Ordering::SeqCst)
    }

    pub fn remove_from_sync_state(&mut self, broker_id: u64) {
        self.sync_state_set.remove(&broker_id);
    }
}

impl Serialize for SyncStateInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let helper = SyncStateInfoDef {
            cluster_name: self.cluster_name.to_string(),
            broker_name: self.broker_name.to_string(),
            master_epoch: self.master_epoch.load(Ordering::SeqCst),
            sync_state_set_epoch: self.sync_state_set_epoch.load(Ordering::SeqCst),
            sync_state_set: self.sync_state_set.clone(),
            master_broker_id: self.master_broker_id,
        };
        helper.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SyncStateInfo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let helper = SyncStateInfoDef::deserialize(deserializer)?;
        Ok(SyncStateInfo {
            cluster_name: CheetahString::from_string(helper.cluster_name),
            broker_name: CheetahString::from_string(helper.broker_name),
            master_epoch: Arc::new(AtomicI32::new(helper.master_epoch)),
            sync_state_set_epoch: Arc::new(AtomicI32::new(helper.sync_state_set_epoch)),
            sync_state_set: helper.sync_state_set,
            master_broker_id: helper.master_broker_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_sync_state_info() {
        let cluster_name = "test_cluster";
        let broker_name = "test_broker";
        let info = SyncStateInfo::new(cluster_name, broker_name);

        assert_eq!(info.cluster_name(), cluster_name);
        assert_eq!(info.broker_name(), broker_name);
        assert_eq!(info.master_epoch(), 0);
        assert_eq!(info.sync_state_set_epoch(), 0);
        assert!(info.sync_state_set().is_empty());
        assert!(info.master_broker_id().is_none());
        assert!(info.is_first_time_for_elect());
    }

    #[test]
    fn test_update_master_info() {
        let mut info = SyncStateInfo::new("c", "b");
        assert!(!info.is_master_exist());

        info.update_master_info(100);

        assert_eq!(info.master_broker_id(), Some(100));
        assert_eq!(info.master_epoch(), 1);
        assert!(info.is_master_exist());
        assert!(!info.is_first_time_for_elect());
    }

    #[test]
    fn test_update_sync_state_set() {
        let mut info = SyncStateInfo::new("c", "b");
        let mut set = HashSet::new();
        set.insert(1);
        set.insert(2);

        info.update_sync_state_set_info(&set);

        assert_eq!(info.sync_state_set().len(), 2);
        assert!(info.sync_state_set().contains(&1));
        assert_eq!(info.sync_state_set_epoch(), 1);
    }

    #[test]
    fn test_remove_from_sync_state() {
        let mut info = SyncStateInfo::new("c", "b");
        let mut set = HashSet::new();
        set.insert(1);
        set.insert(2);
        info.update_sync_state_set_info(&set);

        info.remove_from_sync_state(1);

        assert_eq!(info.sync_state_set().len(), 1);
        assert!(!info.sync_state_set().contains(&1));
        assert!(info.sync_state_set().contains(&2));
    }
}
