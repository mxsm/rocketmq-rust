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

use std::fmt;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Deserialize, Serialize, Default, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EpochEntry {
    #[serde(default)]
    epoch: i32,
    #[serde(default)]
    start_offset: i64,
    #[serde(default = "EpochEntry::default_end_offset")]
    end_offset: i64,
}

impl EpochEntry {
    fn default_end_offset() -> i64 {
        i64::MAX
    }

    pub fn new(epoch: i32, start_offset: i64) -> Self {
        Self {
            epoch,
            start_offset,
            end_offset: i64::MAX,
        }
    }

    pub fn with_end_offset(epoch: i32, start_offset: i64, end_offset: i64) -> Self {
        Self {
            epoch,
            start_offset,
            end_offset,
        }
    }

    pub fn get_epoch(&self) -> i32 {
        self.epoch
    }

    pub fn set_epoch(&mut self, epoch: i32) {
        self.epoch = epoch;
    }

    pub fn get_start_offset(&self) -> i64 {
        self.start_offset
    }

    pub fn set_start_offset(&mut self, start_offset: i64) {
        self.start_offset = start_offset;
    }

    pub fn get_end_offset(&self) -> i64 {
        self.end_offset
    }

    pub fn set_end_offset(&mut self, end_offset: i64) {
        self.end_offset = end_offset;
    }
}

impl fmt::Display for EpochEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "EpochEntry{{epoch={}, startOffset={}, endOffset={}}}",
            self.epoch, self.start_offset, self.end_offset
        )
    }
}

#[derive(Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct EpochEntryCache {
    cluster_name: CheetahString,
    broker_name: CheetahString,
    broker_id: u64,
    epoch_list: Vec<EpochEntry>,
    max_offset: u64,
}

impl EpochEntryCache {
    pub fn new(
        cluster_name: impl Into<CheetahString>,
        broker_name: impl Into<CheetahString>,
        broker_id: u64,
        epoch_list: Vec<EpochEntry>,
        max_offset: u64,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_name: broker_name.into(),
            broker_id,
            epoch_list,
            max_offset,
        }
    }
    pub fn get_cluster_name(&self) -> &CheetahString {
        &self.cluster_name
    }
    pub fn set_cluster_name(&mut self, cluster_name: impl Into<CheetahString>) {
        self.cluster_name = cluster_name.into()
    }
    pub fn get_broker_name(&self) -> &CheetahString {
        &self.broker_name
    }
    pub fn set_broker_name(&mut self, broker_name: impl Into<CheetahString>) {
        self.broker_name = broker_name.into()
    }
    pub fn get_broker_id(&self) -> u64 {
        self.broker_id
    }
    pub fn get_epoch_list(&self) -> &Vec<EpochEntry> {
        &self.epoch_list
    }
    pub fn get_epoch_list_mut(&mut self) -> &mut Vec<EpochEntry> {
        &mut self.epoch_list
    }
    pub fn get_max_offset(&self) -> u64 {
        self.max_offset
    }
    pub fn set_max_offset(&mut self, max_offset: u64) {
        self.max_offset = max_offset;
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn new_creates_instance_of_epoch_entry_cache() {
        let epoch_entry_cache = EpochEntryCache::new("cluster1", "broker1", 1, vec![EpochEntry::new(1, 0)], 1);
        assert_eq!(epoch_entry_cache.get_cluster_name(), &CheetahString::from("cluster1"));
        assert_eq!(epoch_entry_cache.get_broker_id(), 1);
        assert_eq!(epoch_entry_cache.get_broker_name(), &CheetahString::from("broker1"));
    }
    #[test]
    fn set_broker_name_updates_broker_name() {
        let mut epoch_entry_cache = EpochEntryCache::new("cluster1", "broker1", 1, vec![EpochEntry::new(1, 0)], 1);
        epoch_entry_cache.set_broker_name("broker2");
        assert_eq!(epoch_entry_cache.get_broker_name(), &CheetahString::from("broker2"));
    }

    #[test]
    fn set_cluster_name_updates_cluster_name() {
        let mut epoch_entry_cache = EpochEntryCache::new("cluster1", "broker1", 1, vec![EpochEntry::new(1, 0)], 1);
        epoch_entry_cache.set_cluster_name("cluster2");
        assert_eq!(epoch_entry_cache.get_cluster_name(), &CheetahString::from("cluster2"));
    }

    #[test]
    fn epoch_entry_display() {
        let entry = EpochEntry::with_end_offset(1, 100, 200);
        assert_eq!(entry.to_string(), "EpochEntry{epoch=1, startOffset=100, endOffset=200}");
    }

    #[test]
    fn epoch_entry_cache_getters() {
        let entries = vec![EpochEntry::new(1, 0), EpochEntry::new(2, 100)];
        let cache = EpochEntryCache::new("cluster1", "broker1", 1, entries, 500);
        assert_eq!(cache.get_epoch_list().len(), 2);
        assert_eq!(cache.get_max_offset(), 500);
    }
}
