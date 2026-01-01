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

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
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
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn new_creates_instance_with_default_alive() {
        let replica = ReplicaIdentity::new("broker1", 1, "address1");
        assert_eq!(replica.get_broker_name(), &CheetahString::from("broker1"));
        assert_eq!(replica.get_broker_id(), 1);
        assert_eq!(replica.get_broker_address(), &CheetahString::from("address1"));
        assert!(!replica.get_alive());
    }

    #[test]
    fn new_with_alive_creates_instance_with_specified_alive() {
        let replica = ReplicaIdentity::new_with_alive("broker1", 1, "address1", true);
        assert_eq!(replica.get_broker_name(), &CheetahString::from("broker1"));
        assert_eq!(replica.get_broker_id(), 1);
        assert_eq!(replica.get_broker_address(), &CheetahString::from("address1"));
        assert!(replica.get_alive());
    }

    #[test]
    fn set_broker_name_updates_broker_name() {
        let mut replica = ReplicaIdentity::new("broker1", 1, "address1");
        replica.set_broker_name("broker2");
        assert_eq!(replica.get_broker_name(), &CheetahString::from("broker2"));
    }

    #[test]
    fn set_broker_address_updates_broker_address() {
        let mut replica = ReplicaIdentity::new("broker1", 1, "address1");
        replica.set_broker_address("address2");
        assert_eq!(replica.get_broker_address(), &CheetahString::from("address2"));
    }

    #[test]
    fn set_broker_id_updates_broker_id() {
        let mut replica = ReplicaIdentity::new("broker1", 1, "address1");
        replica.set_broker_id(2);
        assert_eq!(replica.get_broker_id(), 2);
    }

    #[test]
    fn set_alive_updates_alive_status() {
        let mut replica = ReplicaIdentity::new("broker1", 1, "address1");
        replica.set_alive(true);
        assert!(replica.get_alive());
    }

    #[test]
    fn display_formats_correctly() {
        let replica = ReplicaIdentity::new_with_alive("broker1", 1, "address1", true);
        let display = format!("{}", replica);
        assert_eq!(
            display,
            "ReplicaIdentity{ broker_name: 'broker1', broker_id: 1, broker_address: 'address1', alive: true }"
        );
    }

    #[test]
    fn new_creates_instance_with_all_fields() {
        let in_sync_replicas = vec![ReplicaIdentity::new("broker1", 1, "address1")];
        let not_in_sync_replicas = vec![ReplicaIdentity::new("broker2", 2, "address2")];
        let replicas_info = ReplicasInfo::new(
            1,
            "master_address",
            100,
            200,
            in_sync_replicas.clone(),
            not_in_sync_replicas.clone(),
        );
        assert_eq!(replicas_info.get_master_broker_id(), 1);
        assert_eq!(replicas_info.get_master_address(), "master_address");
        assert_eq!(replicas_info.get_master_epoch(), 100);
        assert_eq!(replicas_info.get_sync_state_set_epoch(), 200);
    }

    #[test]
    fn set_master_address_updates_master_address() {
        let mut replicas_info = ReplicasInfo::new(1, "master_address", 100, 200, vec![], vec![]);
        replicas_info.set_master_address("new_master_address");
        assert_eq!(replicas_info.get_master_address(), "new_master_address");
    }

    #[test]
    fn set_master_epoch_updates_master_epoch() {
        let mut replicas_info = ReplicasInfo::new(1, "master_address", 100, 200, vec![], vec![]);
        replicas_info.set_master_epoch(101);
        assert_eq!(replicas_info.get_master_epoch(), 101);
    }

    #[test]
    fn set_sync_state_set_epoch_updates_sync_state_set_epoch() {
        let mut replicas_info = ReplicasInfo::new(1, "master_address", 100, 200, vec![], vec![]);
        replicas_info.set_sync_state_set_epoch(201);
        assert_eq!(replicas_info.get_sync_state_set_epoch(), 201);
    }

    #[test]
    fn set_master_broker_id_updates_master_broker_id() {
        let mut replicas_info = ReplicasInfo::new(1, "master_address", 100, 200, vec![], vec![]);
        replicas_info.set_master_broker_id(2);
        assert_eq!(replicas_info.get_master_broker_id(), 2);
    }

    #[test]
    fn is_exist_in_sync_returns_true_for_existing_replica() {
        let in_sync_replicas = vec![ReplicaIdentity::new("broker1", 1, "address1")];
        let replicas_info = ReplicasInfo::new(1, "master_address", 100, 200, in_sync_replicas, vec![]);
        assert!(replicas_info.is_exist_in_sync("broker1", 1, "address1"));
    }

    #[test]
    fn is_exist_in_sync_returns_false_for_non_existing_replica() {
        let replicas_info = ReplicasInfo::new(1, "master_address", 100, 200, vec![], vec![]);
        assert!(!replicas_info.is_exist_in_sync("broker1", 1, "address1"));
    }

    #[test]
    fn is_exist_in_not_sync_returns_true_for_existing_replica() {
        let not_in_sync_replicas = vec![ReplicaIdentity::new("broker2", 2, "address2")];
        let replicas_info = ReplicasInfo::new(1, "master_address", 100, 200, vec![], not_in_sync_replicas);
        assert!(replicas_info.is_exist_in_not_sync("broker2", 2, "address2"));
    }

    #[test]
    fn is_exist_in_not_sync_returns_false_for_non_existing_replica() {
        let replicas_info = ReplicasInfo::new(1, "master_address", 100, 200, vec![], vec![]);
        assert!(!replicas_info.is_exist_in_not_sync("broker2", 2, "address2"));
    }

    #[test]
    fn is_exist_in_all_replicas_returns_true_for_existing_replica_in_sync() {
        let in_sync_replicas = vec![ReplicaIdentity::new("broker1", 1, "address1")];
        let replicas_info = ReplicasInfo::new(1, "master_address", 100, 200, in_sync_replicas, vec![]);
        assert!(replicas_info.is_exist_in_all_replicas("broker1", 1, "address1"));
    }

    #[test]
    fn is_exist_in_all_replicas_returns_true_for_existing_replica_in_not_sync() {
        let not_in_sync_replicas = vec![ReplicaIdentity::new("broker2", 2, "address2")];
        let replicas_info = ReplicasInfo::new(1, "master_address", 100, 200, vec![], not_in_sync_replicas);
        assert!(replicas_info.is_exist_in_all_replicas("broker2", 2, "address2"));
    }

    #[test]
    fn is_exist_in_all_replicas_returns_false_for_non_existing_replica() {
        let replicas_info = ReplicasInfo::new(1, "master_address", 100, 200, vec![], vec![]);
        assert!(!replicas_info.is_exist_in_all_replicas("broker1", 1, "address1"));
    }
}
