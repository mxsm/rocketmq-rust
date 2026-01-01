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

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SyncStateSet {
    sync_state_set: Option<HashSet<i64>>,
    sync_state_set_epoch: i32,
}

impl SyncStateSet {
    pub fn new() -> SyncStateSet {
        SyncStateSet {
            sync_state_set: None,
            sync_state_set_epoch: 0,
        }
    }

    pub fn take_sync_state_set(&mut self) -> Option<HashSet<i64>> {
        self.sync_state_set.take()
    }

    pub fn with_values(sync_state_set: HashSet<i64>, sync_state_set_epoch: i32) -> SyncStateSet {
        SyncStateSet {
            sync_state_set: Some(sync_state_set),
            sync_state_set_epoch,
        }
    }

    pub fn set_sync_state_set(&mut self, sync_state_set: HashSet<i64>) {
        self.sync_state_set = Some(sync_state_set);
    }

    pub fn set_sync_state_set_epoch(&mut self, sync_state_set_epoch: i32) {
        self.sync_state_set_epoch = sync_state_set_epoch;
    }

    pub fn get_sync_state_set(&self) -> Option<&HashSet<i64>> {
        self.sync_state_set.as_ref()
    }

    pub fn get_sync_state_set_epoch(&self) -> i32 {
        self.sync_state_set_epoch
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_state_set_serializes_correctly() {
        let mut set = HashSet::new();
        set.insert(0);
        set.insert(1);

        let sync_state = SyncStateSet::with_values(set, 5);
        let json = serde_json::to_string(&sync_state).unwrap();

        assert!(json.contains("syncStateSet"));
        assert!(json.contains("syncStateSetEpoch"));
        assert!(json.contains("5"));
    }

    #[test]
    fn sync_state_set_deserializes_correctly() {
        let json = r#"{
            "syncStateSet": [0, 1, 2],
            "syncStateSetEpoch": 10
        }"#;

        let sync_state: SyncStateSet = serde_json::from_str(json).unwrap();
        assert!(sync_state.get_sync_state_set().is_some());
        assert_eq!(sync_state.get_sync_state_set().unwrap().len(), 3);
        assert!(sync_state.get_sync_state_set().unwrap().contains(&0));
        assert!(sync_state.get_sync_state_set().unwrap().contains(&1));
        assert!(sync_state.get_sync_state_set().unwrap().contains(&2));
        assert_eq!(sync_state.get_sync_state_set_epoch(), 10);
    }

    #[test]
    fn sync_state_set_default_and_new() {
        let sync_state = SyncStateSet::default();
        assert!(sync_state.get_sync_state_set().is_none());
        assert_eq!(sync_state.get_sync_state_set_epoch(), 0);

        let sync_state = SyncStateSet::new();
        assert!(sync_state.get_sync_state_set().is_none());
        assert_eq!(sync_state.get_sync_state_set_epoch(), 0);
    }

    #[test]
    fn sync_state_set_take_sync_state_set() {
        let mut set = HashSet::new();
        set.insert(1);
        set.insert(2);

        let mut sync_state = SyncStateSet::with_values(set, 5);

        let taken = sync_state.take_sync_state_set();
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().len(), 2);

        assert!(sync_state.get_sync_state_set().is_none());
    }

    #[test]
    fn sync_state_set_with_values() {
        let mut set = HashSet::new();
        set.insert(0);
        set.insert(1);
        set.insert(2);

        let sync_state = SyncStateSet::with_values(set, 5);
        assert!(sync_state.get_sync_state_set().is_some());
        assert_eq!(sync_state.get_sync_state_set().unwrap().len(), 3);
        assert_eq!(sync_state.get_sync_state_set_epoch(), 5);
    }

    #[test]
    fn sync_state_set_setters_and_getters() {
        let mut sync_state = SyncStateSet::new();

        let mut set = HashSet::new();
        set.insert(100);
        set.insert(200);

        sync_state.set_sync_state_set(set);
        sync_state.set_sync_state_set_epoch(10);

        assert!(sync_state.get_sync_state_set().is_some());
        assert_eq!(sync_state.get_sync_state_set().unwrap().len(), 2);
        assert!(sync_state.get_sync_state_set().unwrap().contains(&100));
        assert!(sync_state.get_sync_state_set().unwrap().contains(&200));
        assert_eq!(sync_state.get_sync_state_set_epoch(), 10);
    }
}
