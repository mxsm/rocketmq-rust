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
use std::fmt;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BrokerReplicasInfo {
    replicas_info_table: HashMap<CheetahString, ReplicasInfo>,
}

impl BrokerReplicasInfo {
    pub fn new() -> Self {
        Self {
            replicas_info_table: HashMap::new(),
        }
    }

    pub fn add_replica_info(&mut self, broker_name: CheetahString, replicas_info: ReplicasInfo) {
        self.replicas_info_table.insert(broker_name, replicas_info);
    }

    pub fn get_replicas_info_table(&self) -> &HashMap<CheetahString, ReplicasInfo> {
        &self.replicas_info_table
    }

    pub fn set_replicas_info_table(&mut self, replicas_info_table: HashMap<CheetahString, ReplicasInfo>) {
        self.replicas_info_table = replicas_info_table;
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReplicasInfo {
    master_broker_id: u64,
    master_address: CheetahString,
    master_epoch: i32,
    sync_state_set_epoch: i32,
    in_sync_replicas: Vec<ReplicaIdentity>,
    not_in_sync_replicas: Vec<ReplicaIdentity>,
}

impl ReplicasInfo {
    pub fn new(
        master_broker_id: u64,
        master_address: impl Into<CheetahString>,
        master_epoch: i32,
        sync_state_set_epoch: i32,
        in_sync_replicas: Vec<ReplicaIdentity>,
        not_in_sync_replicas: Vec<ReplicaIdentity>,
    ) -> Self {
        Self {
            master_broker_id,
            master_address: master_address.into(),
            master_epoch,
            sync_state_set_epoch,
            in_sync_replicas,
            not_in_sync_replicas,
        }
    }

    pub fn get_master_address(&self) -> &str {
        &self.master_address
    }

    pub fn set_master_address(&mut self, master_address: impl Into<CheetahString>) {
        self.master_address = master_address.into();
    }

    pub fn get_master_epoch(&self) -> i32 {
        self.master_epoch
    }

    pub fn set_master_epoch(&mut self, master_epoch: i32) {
        self.master_epoch = master_epoch;
    }

    pub fn get_sync_state_set_epoch(&self) -> i32 {
        self.sync_state_set_epoch
    }

    pub fn set_sync_state_set_epoch(&mut self, sync_state_set_epoch: i32) {
        self.sync_state_set_epoch = sync_state_set_epoch;
    }

    pub fn get_in_sync_replicas(&self) -> &Vec<ReplicaIdentity> {
        &self.in_sync_replicas
    }

    pub fn set_in_sync_replicas(&mut self, in_sync_replicas: Vec<ReplicaIdentity>) {
        self.in_sync_replicas = in_sync_replicas;
    }

    pub fn get_not_in_sync_replicas(&self) -> &Vec<ReplicaIdentity> {
        &self.not_in_sync_replicas
    }

    pub fn set_not_in_sync_replicas(&mut self, not_in_sync_replicas: Vec<ReplicaIdentity>) {
        self.not_in_sync_replicas = not_in_sync_replicas;
    }

    pub fn get_master_broker_id(&self) -> u64 {
        self.master_broker_id
    }

    pub fn set_master_broker_id(&mut self, master_broker_id: u64) {
        self.master_broker_id = master_broker_id;
    }

    pub fn is_exist_in_sync(&self, broker_name: &str, broker_id: u64, broker_address: &str) -> bool {
        self.in_sync_replicas.iter().any(|replica| {
            replica.broker_name == broker_name
                && replica.broker_id == broker_id
                && replica.broker_address == broker_address
        })
    }

    pub fn is_exist_in_not_sync(&self, broker_name: &str, broker_id: u64, broker_address: &str) -> bool {
        self.not_in_sync_replicas.iter().any(|replica| {
            replica.broker_name == broker_name
                && replica.broker_id == broker_id
                && replica.broker_address == broker_address
        })
    }

    pub fn is_exist_in_all_replicas(&self, broker_name: &str, broker_id: u64, broker_address: &str) -> bool {
        self.is_exist_in_sync(broker_name, broker_id, broker_address)
            || self.is_exist_in_not_sync(broker_name, broker_id, broker_address)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaIdentity {
    broker_name: CheetahString,
    broker_id: u64,
    broker_address: CheetahString,
    alive: bool,
}

impl ReplicaIdentity {
    pub fn new(
        broker_name: impl Into<CheetahString>,
        broker_id: u64,
        broker_address: impl Into<CheetahString>,
    ) -> Self {
        Self {
            broker_name: broker_name.into(),
            broker_id,
            broker_address: broker_address.into(),
            alive: false,
        }
    }

    pub fn new_with_alive(
        broker_name: impl Into<CheetahString>,
        broker_id: u64,
        broker_address: impl Into<CheetahString>,
        alive: bool,
    ) -> Self {
        Self {
            broker_name: broker_name.into(),
            broker_id,
            broker_address: broker_address.into(),
            alive,
        }
    }

    pub fn get_broker_name(&self) -> &CheetahString {
        &self.broker_name
    }

    pub fn set_broker_name(&mut self, broker_name: impl Into<CheetahString>) {
        self.broker_name = broker_name.into();
    }

    pub fn get_broker_address(&self) -> &CheetahString {
        &self.broker_address
    }

    pub fn set_broker_address(&mut self, broker_address: impl Into<CheetahString>) {
        self.broker_address = broker_address.into();
    }

    pub fn get_broker_id(&self) -> u64 {
        self.broker_id
    }

    pub fn set_broker_id(&mut self, broker_id: u64) {
        self.broker_id = broker_id;
    }

    pub fn get_alive(&self) -> bool {
        self.alive
    }

    pub fn set_alive(&mut self, alive: bool) {
        self.alive = alive;
    }
}

impl fmt::Display for ReplicaIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ReplicaIdentity{{ broker_name: '{}', broker_id: {}, broker_address: '{}', alive: {} }}",
            self.broker_name, self.broker_id, self.broker_address, self.alive
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replicas_info() -> ReplicasInfo {
        ReplicasInfo::new(
            1,
            "master-address",
            10,
            20,
            vec![ReplicaIdentity::new("broker-a", 1, "address-a")],
            vec![ReplicaIdentity::new("broker-b", 2, "address-b")],
        )
    }

    #[test]
    fn replica_identity_accessors_and_display_cover_all_fields() {
        let mut replica = ReplicaIdentity::new("broker-a", 1, "address-a");
        assert!(!replica.get_alive());

        replica.set_broker_name("broker-b");
        replica.set_broker_id(2);
        replica.set_broker_address("address-b");
        replica.set_alive(true);

        assert_eq!(replica.get_broker_name(), "broker-b");
        assert_eq!(replica.get_broker_id(), 2);
        assert_eq!(replica.get_broker_address(), "address-b");
        assert!(replica.get_alive());
        assert_eq!(
            replica.to_string(),
            "ReplicaIdentity{ broker_name: 'broker-b', broker_id: 2, broker_address: 'address-b', alive: true }"
        );
        assert!(ReplicaIdentity::new_with_alive("broker-c", 3, "address-c", true).get_alive());
    }

    #[test]
    fn replicas_info_accessors_update_all_fields() {
        let mut info = replicas_info();
        assert_eq!(info.get_master_broker_id(), 1);
        assert_eq!(info.get_master_address(), "master-address");
        assert_eq!(info.get_master_epoch(), 10);
        assert_eq!(info.get_sync_state_set_epoch(), 20);
        assert_eq!(info.get_in_sync_replicas().len(), 1);
        assert_eq!(info.get_not_in_sync_replicas().len(), 1);

        let in_sync = vec![ReplicaIdentity::new("broker-c", 3, "address-c")];
        let not_in_sync = vec![ReplicaIdentity::new("broker-d", 4, "address-d")];
        info.set_master_broker_id(3);
        info.set_master_address("new-master-address");
        info.set_master_epoch(11);
        info.set_sync_state_set_epoch(21);
        info.set_in_sync_replicas(in_sync.clone());
        info.set_not_in_sync_replicas(not_in_sync.clone());

        assert_eq!(info.get_master_broker_id(), 3);
        assert_eq!(info.get_master_address(), "new-master-address");
        assert_eq!(info.get_master_epoch(), 11);
        assert_eq!(info.get_sync_state_set_epoch(), 21);
        assert_eq!(info.get_in_sync_replicas(), &in_sync);
        assert_eq!(info.get_not_in_sync_replicas(), &not_in_sync);
    }

    #[test]
    fn membership_queries_require_the_complete_replica_identity() {
        let info = replicas_info();

        assert!(info.is_exist_in_sync("broker-a", 1, "address-a"));
        assert!(!info.is_exist_in_sync("other", 1, "address-a"));
        assert!(!info.is_exist_in_sync("broker-a", 2, "address-a"));
        assert!(!info.is_exist_in_sync("broker-a", 1, "other"));
        assert!(info.is_exist_in_not_sync("broker-b", 2, "address-b"));
        assert!(info.is_exist_in_all_replicas("broker-a", 1, "address-a"));
        assert!(info.is_exist_in_all_replicas("broker-b", 2, "address-b"));
        assert!(!info.is_exist_in_all_replicas("broker-c", 3, "address-c"));
    }

    #[test]
    fn broker_replicas_table_methods_add_and_replace_entries() {
        let mut table = BrokerReplicasInfo::new();
        assert!(table.get_replicas_info_table().is_empty());

        table.add_replica_info(CheetahString::from_static_str("broker-a"), replicas_info());
        assert!(table
            .get_replicas_info_table()
            .contains_key(&CheetahString::from_static_str("broker-a")));

        let replacement = HashMap::from([(CheetahString::from_static_str("broker-b"), replicas_info())]);
        table.set_replicas_info_table(replacement);
        assert_eq!(table.get_replicas_info_table().len(), 1);
        assert!(table
            .get_replicas_info_table()
            .contains_key(&CheetahString::from_static_str("broker-b")));
    }
}
