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

use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::epoch_entry_cache::EpochEntry;
use tracing::info;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerReplicaRole {
    Master,
    Slave,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleChangeOutcome {
    pub role: Option<BrokerReplicaRole>,
    pub sync_state_set_changed: bool,
    pub master_broker_id: Option<u64>,
    pub master_address: Option<CheetahString>,
    pub master_epoch: i32,
    pub sync_state_set_epoch: i32,
    pub sync_state_set: HashSet<i64>,
}

pub struct ReplicasManager {
    broker_controller_id: u64,
    broker_address: CheetahString,
    controller_addresses: Vec<CheetahString>,
    controller_leader_address: Option<CheetahString>,
    master_broker_id: Option<u64>,
    master_address: Option<CheetahString>,
    master_epoch: i32,
    sync_state_set_epoch: i32,
    sync_state_set: HashSet<i64>,
    started: bool,
}

impl ReplicasManager {
    pub fn new(config: &BrokerConfig, broker_address: CheetahString) -> Self {
        Self {
            broker_controller_id: config.broker_identity.broker_id,
            broker_address,
            controller_addresses: parse_controller_addresses(&config.controller_addr),
            controller_leader_address: None,
            master_broker_id: None,
            master_address: None,
            master_epoch: 0,
            sync_state_set_epoch: 0,
            sync_state_set: HashSet::new(),
            started: false,
        }
    }

    pub fn start(&mut self) {
        self.started = true;
        info!(
            "ReplicasManager started, broker_controller_id={}, controller_addresses={:?}",
            self.broker_controller_id, self.controller_addresses
        );
    }

    pub fn shutdown(&mut self) {
        self.started = false;
        info!(
            "ReplicasManager shutdown, broker_controller_id={}",
            self.broker_controller_id
        );
    }

    pub fn get_epoch_entries(&self) -> Vec<EpochEntry> {
        if self.master_epoch <= 0 {
            return Vec::new();
        }
        vec![EpochEntry::new(self.master_epoch, 0)]
    }

    pub fn broker_controller_id(&self) -> u64 {
        self.broker_controller_id
    }

    pub fn broker_address(&self) -> &CheetahString {
        &self.broker_address
    }

    pub fn controller_addresses(&self) -> &[CheetahString] {
        &self.controller_addresses
    }

    pub fn heartbeat_targets(&self) -> Vec<CheetahString> {
        let mut targets = Vec::new();
        if let Some(leader) = &self.controller_leader_address {
            targets.push(leader.clone());
        }
        for address in &self.controller_addresses {
            if !targets.contains(address) {
                targets.push(address.clone());
            }
        }
        targets
    }

    pub fn set_controller_leader_address(&mut self, controller_leader_address: CheetahString) {
        if controller_leader_address.is_empty() {
            return;
        }
        self.controller_leader_address = Some(controller_leader_address);
    }

    pub fn controller_leader_address(&self) -> Option<&CheetahString> {
        self.controller_leader_address.as_ref()
    }

    pub fn master_broker_id(&self) -> Option<u64> {
        self.master_broker_id
    }

    pub fn master_address(&self) -> Option<&CheetahString> {
        self.master_address.as_ref()
    }

    pub fn master_epoch(&self) -> i32 {
        self.master_epoch
    }

    pub fn sync_state_set_epoch(&self) -> i32 {
        self.sync_state_set_epoch
    }

    pub fn sync_state_set(&self) -> &HashSet<i64> {
        &self.sync_state_set
    }

    pub fn change_broker_role(
        &mut self,
        new_master_broker_id: Option<u64>,
        new_master_address: Option<CheetahString>,
        new_master_epoch: Option<i32>,
        sync_state_set_epoch: Option<i32>,
        sync_state_set: Option<&HashSet<i64>>,
    ) -> RocketMQResult<RoleChangeOutcome> {
        let Some(new_master_epoch) = new_master_epoch else {
            return Err(RocketMQError::illegal_argument(
                "notify broker role change missing master epoch",
            ));
        };

        if new_master_epoch < self.master_epoch {
            return Ok(self.outcome(None, false));
        }

        let next_sync_state_set_epoch = sync_state_set_epoch.unwrap_or(self.sync_state_set_epoch);
        let sync_state_set_changed = next_sync_state_set_epoch > self.sync_state_set_epoch;
        if sync_state_set_changed {
            self.sync_state_set_epoch = next_sync_state_set_epoch;
            self.sync_state_set = sync_state_set.cloned().unwrap_or_default();
        }

        if new_master_epoch == self.master_epoch {
            return Ok(self.outcome(None, sync_state_set_changed));
        }

        let Some(master_broker_id) = new_master_broker_id else {
            return Err(RocketMQError::illegal_argument(
                "notify broker role change missing master broker id",
            ));
        };

        self.master_epoch = new_master_epoch;
        self.master_broker_id = Some(master_broker_id);

        let role = if master_broker_id == self.broker_controller_id {
            self.master_address = Some(self.broker_address.clone());
            Some(BrokerReplicaRole::Master)
        } else {
            let Some(master_address) = new_master_address else {
                return Err(RocketMQError::illegal_argument(
                    "notify broker role change missing master address for slave transition",
                ));
            };
            self.master_address = Some(master_address);
            Some(BrokerReplicaRole::Slave)
        };

        info!(
            "Apply controller role change, broker_controller_id={}, new_role={:?}, master_broker_id={:?}, \
             master_address={:?}, master_epoch={}, sync_state_set_epoch={}",
            self.broker_controller_id,
            role,
            self.master_broker_id,
            self.master_address,
            self.master_epoch,
            self.sync_state_set_epoch
        );

        Ok(self.outcome(role, sync_state_set_changed))
    }

    fn outcome(&self, role: Option<BrokerReplicaRole>, sync_state_set_changed: bool) -> RoleChangeOutcome {
        RoleChangeOutcome {
            role,
            sync_state_set_changed,
            master_broker_id: self.master_broker_id,
            master_address: self.master_address.clone(),
            master_epoch: self.master_epoch,
            sync_state_set_epoch: self.sync_state_set_epoch,
            sync_state_set: self.sync_state_set.clone(),
        }
    }
}

fn parse_controller_addresses(controller_addr: &CheetahString) -> Vec<CheetahString> {
    if controller_addr.is_empty() {
        return Vec::new();
    }

    controller_addr
        .split(';')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(CheetahString::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use rocketmq_common::common::broker::broker_config::BrokerIdentity;

    use super::*;

    fn broker_config_with_controller_addr(controller_addr: &str, broker_id: u64) -> BrokerConfig {
        let mut config = BrokerConfig {
            controller_addr: controller_addr.into(),
            ..BrokerConfig::default()
        };
        config.broker_identity = BrokerIdentity {
            broker_id,
            ..config.broker_identity
        };
        config
    }

    #[test]
    fn new_manager_parses_controller_addresses() {
        let config = broker_config_with_controller_addr("127.0.0.1:9878;127.0.0.2:9878;", 2);
        let manager = ReplicasManager::new(&config, "127.0.0.1:10911".into());

        assert_eq!(manager.broker_controller_id(), 2);
        assert_eq!(
            manager.controller_addresses(),
            &[
                CheetahString::from("127.0.0.1:9878"),
                CheetahString::from("127.0.0.2:9878"),
            ]
        );
    }

    #[test]
    fn change_broker_role_promotes_local_broker_to_master() {
        let config = broker_config_with_controller_addr("127.0.0.1:9878", 2);
        let mut manager = ReplicasManager::new(&config, "127.0.0.1:10911".into());
        let sync_state_set = HashSet::from([2_i64, 3_i64]);

        let outcome = manager
            .change_broker_role(Some(2), None, Some(3), Some(4), Some(&sync_state_set))
            .expect("role change should succeed");

        assert_eq!(outcome.role, Some(BrokerReplicaRole::Master));
        assert_eq!(outcome.master_broker_id, Some(2));
        assert_eq!(outcome.master_address, Some(CheetahString::from("127.0.0.1:10911")));
        assert_eq!(outcome.master_epoch, 3);
        assert_eq!(outcome.sync_state_set_epoch, 4);
        assert_eq!(outcome.sync_state_set, sync_state_set);
    }

    #[test]
    fn change_broker_role_demotes_local_broker_to_slave() {
        let config = broker_config_with_controller_addr("127.0.0.1:9878", 2);
        let mut manager = ReplicasManager::new(&config, "127.0.0.1:10911".into());
        let sync_state_set = HashSet::from([1_i64, 2_i64]);

        let outcome = manager
            .change_broker_role(
                Some(1),
                Some("127.0.0.9:10911".into()),
                Some(5),
                Some(6),
                Some(&sync_state_set),
            )
            .expect("role change should succeed");

        assert_eq!(outcome.role, Some(BrokerReplicaRole::Slave));
        assert_eq!(outcome.master_broker_id, Some(1));
        assert_eq!(outcome.master_address, Some(CheetahString::from("127.0.0.9:10911")));
        assert_eq!(outcome.master_epoch, 5);
        assert_eq!(outcome.sync_state_set_epoch, 6);
        assert_eq!(outcome.sync_state_set, sync_state_set);
    }

    #[test]
    fn change_broker_role_ignores_stale_epoch() {
        let config = broker_config_with_controller_addr("127.0.0.1:9878", 2);
        let mut manager = ReplicasManager::new(&config, "127.0.0.1:10911".into());
        let sync_state_set = HashSet::from([2_i64]);

        manager
            .change_broker_role(Some(2), None, Some(3), Some(3), Some(&sync_state_set))
            .expect("initial role change should succeed");

        let stale = manager
            .change_broker_role(
                Some(1),
                Some("127.0.0.9:10911".into()),
                Some(2),
                Some(4),
                Some(&HashSet::from([1_i64])),
            )
            .expect("stale role change should be ignored");

        assert_eq!(stale.role, None);
        assert_eq!(stale.master_broker_id, Some(2));
        assert_eq!(stale.master_address, Some(CheetahString::from("127.0.0.1:10911")));
        assert_eq!(stale.master_epoch, 3);
    }

    #[test]
    fn heartbeat_targets_prefer_known_leader() {
        let config = broker_config_with_controller_addr("127.0.0.1:9878;127.0.0.2:9878", 2);
        let mut manager = ReplicasManager::new(&config, "127.0.0.1:10911".into());
        manager.set_controller_leader_address("127.0.0.2:9878".into());

        let targets = manager.heartbeat_targets();
        assert_eq!(
            targets,
            vec![
                CheetahString::from("127.0.0.2:9878"),
                CheetahString::from("127.0.0.1:9878"),
            ]
        );
    }
}
