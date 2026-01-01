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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;
#[derive(Deserialize, Serialize, Default)]
pub struct EpochEntry;

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
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn new_creates_instance_of_epoch_entry_cache() {
        let epoch_entry_cache = EpochEntryCache::new("cluster1", "broker1", 1, vec![EpochEntry], 1);
        assert_eq!(epoch_entry_cache.get_cluster_name(), &CheetahString::from("cluster1"));
        assert_eq!(epoch_entry_cache.get_broker_id(), 1);
        assert_eq!(epoch_entry_cache.get_broker_name(), &CheetahString::from("broker1"));
    }
    #[test]
    fn set_broker_name_updates_broker_name() {
        let mut epoch_entry_cache = EpochEntryCache::new("cluster1", "broker1", 1, vec![EpochEntry], 1);
        epoch_entry_cache.set_broker_name("broker2");
        assert_eq!(epoch_entry_cache.get_broker_name(), &CheetahString::from("broker2"));
    }

    #[test]
    fn set_cluster_name_updates_cluster_name() {
        let mut epoch_entry_cache = EpochEntryCache::new("cluster1", "broker1", 1, vec![EpochEntry], 1);
        epoch_entry_cache.set_cluster_name("cluster2");
        assert_eq!(epoch_entry_cache.get_cluster_name(), &CheetahString::from("cluster2"));
    }
}
