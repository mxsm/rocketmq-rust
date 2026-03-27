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
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::epoch_entry_cache::EpochEntry;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use serde::Deserialize;
use serde::Serialize;
use tracing::info;
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerReplicaRole {
    Master,
    Slave,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterState {
    Initial,
    CreateTempMetadataFileDone,
    CreateMetadataFileDone,
    Registered,
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
    pub local_broker_id: u64,
    pub should_start_special_service: bool,
    pub should_register_to_namesrv: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControllerBrokerIdAction {
    UseCurrent(u64),
    ApplyBrokerId {
        broker_id: u64,
        register_check_code: CheetahString,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerRegisterFollowup {
    ApplyRoleChange,
    HeartbeatThenQueryReplicaInfo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerReplicaInfoFollowup {
    ApplyRoleChange,
    ElectMaster,
}

impl RoleChangeOutcome {
    pub fn target_broker_role(&self) -> Option<BrokerRole> {
        match self.role {
            Some(BrokerReplicaRole::Master) => Some(BrokerRole::SyncMaster),
            Some(BrokerReplicaRole::Slave) => Some(BrokerRole::Slave),
            None => None,
        }
    }

    pub fn should_clear_slave_master_address(&self) -> bool {
        matches!(self.role, Some(BrokerReplicaRole::Master))
    }

    pub fn slave_master_address(&self) -> Option<&CheetahString> {
        matches!(self.role, Some(BrokerReplicaRole::Slave))
            .then_some(self.master_address.as_ref())
            .flatten()
    }

    pub fn should_sync_master_online(&self) -> bool {
        self.slave_master_address().is_some()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct BrokerMetadataRecord {
    cluster_name: String,
    broker_name: String,
    broker_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TempBrokerMetadataRecord {
    cluster_name: String,
    broker_name: String,
    broker_id: u64,
    register_check_code: String,
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
    metadata_path: PathBuf,
    temp_metadata_path: PathBuf,
    metadata: Option<BrokerMetadataRecord>,
    temp_metadata: Option<TempBrokerMetadataRecord>,
    register_state: RegisterState,
    started: bool,
}

impl ReplicasManager {
    pub fn new(
        config: &BrokerConfig,
        message_store_config: &MessageStoreConfig,
        broker_address: CheetahString,
    ) -> Self {
        let metadata_path = metadata_path_from_config(message_store_config);
        let temp_metadata_path = temp_metadata_path_from_metadata_path(&metadata_path);
        let mut manager = Self {
            broker_controller_id: config.broker_identity.broker_id,
            broker_address,
            controller_addresses: parse_controller_addresses(&config.controller_addr),
            controller_leader_address: None,
            master_broker_id: None,
            master_address: None,
            master_epoch: 0,
            sync_state_set_epoch: 0,
            sync_state_set: HashSet::new(),
            metadata_path,
            temp_metadata_path,
            metadata: None,
            temp_metadata: None,
            register_state: RegisterState::Initial,
            started: false,
        };
        manager.recover_registration_state();
        manager
    }

    pub fn start(&mut self) {
        self.started = true;
        info!(
            "ReplicasManager started, broker_controller_id={}, register_state={:?}, controller_addresses={:?}",
            self.broker_controller_id, self.register_state, self.controller_addresses
        );
    }

    pub fn shutdown(&mut self) {
        self.started = false;
        info!(
            "ReplicasManager shutdown, broker_controller_id={}, register_state={:?}",
            self.broker_controller_id, self.register_state
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

    pub fn register_state(&self) -> RegisterState {
        self.register_state
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

    pub fn needs_broker_id_application(&self) -> bool {
        self.metadata.is_none()
    }

    pub fn pending_registration(&self) -> Option<(u64, CheetahString)> {
        self.temp_metadata.as_ref().map(|metadata| {
            (
                metadata.broker_id,
                CheetahString::from(metadata.register_check_code.clone()),
            )
        })
    }

    pub fn validate_registration_state(&self, config: &BrokerConfig) -> RocketMQResult<()> {
        validate_metadata_record(self.metadata.as_ref(), config, "broker metadata")?;
        validate_temp_metadata_record(self.temp_metadata.as_ref(), config)?;
        Ok(())
    }

    pub fn create_temp_metadata(&mut self, config: &BrokerConfig, broker_id: u64) -> RocketMQResult<()> {
        let temp_metadata = TempBrokerMetadataRecord {
            cluster_name: config.broker_identity.broker_cluster_name.to_string(),
            broker_name: config.broker_identity.broker_name.to_string(),
            broker_id,
            register_check_code: format!("{};{}", self.broker_address, current_millis()),
        };
        write_metadata_file(&self.temp_metadata_path, &temp_metadata)?;
        self.temp_metadata = Some(temp_metadata);
        self.broker_controller_id = broker_id;
        self.register_state = RegisterState::CreateTempMetadataFileDone;
        Ok(())
    }

    pub fn clear_temp_metadata(&mut self) -> RocketMQResult<()> {
        delete_metadata_file(&self.temp_metadata_path)?;
        self.temp_metadata = None;
        self.register_state = if self.metadata.is_some() {
            RegisterState::CreateMetadataFileDone
        } else {
            RegisterState::Initial
        };
        Ok(())
    }

    pub fn commit_temp_metadata(&mut self, config: &BrokerConfig) -> RocketMQResult<u64> {
        let Some(temp_metadata) = self.temp_metadata.as_ref() else {
            return Err(RocketMQError::illegal_argument(
                "commit_temp_metadata called without temp metadata",
            ));
        };

        let metadata = BrokerMetadataRecord {
            cluster_name: config.broker_identity.broker_cluster_name.to_string(),
            broker_name: config.broker_identity.broker_name.to_string(),
            broker_id: temp_metadata.broker_id,
        };
        write_metadata_file(&self.metadata_path, &metadata)?;
        self.metadata = Some(metadata);
        self.broker_controller_id = temp_metadata.broker_id;
        self.clear_temp_metadata()?;
        self.register_state = RegisterState::CreateMetadataFileDone;
        Ok(self.broker_controller_id)
    }

    pub fn mark_registered(&mut self) {
        self.register_state = RegisterState::Registered;
    }

    pub fn prepare_controller_broker_id_action(
        &mut self,
        config: &BrokerConfig,
        next_broker_id: Option<u64>,
    ) -> RocketMQResult<ControllerBrokerIdAction> {
        if !self.needs_broker_id_application() {
            return Ok(ControllerBrokerIdAction::UseCurrent(self.broker_controller_id));
        }

        if self.pending_registration().is_none() {
            let next_broker_id = next_broker_id.ok_or_else(|| {
                RocketMQError::illegal_argument(
                    "controller broker id preparation requires next broker id when no pending registration exists",
                )
            })?;
            self.create_temp_metadata(config, next_broker_id)?;
        }

        let (broker_id, register_check_code) = self.pending_registration().ok_or_else(|| {
            RocketMQError::illegal_argument("replicas manager did not retain pending controller registration metadata")
        })?;
        Ok(ControllerBrokerIdAction::ApplyBrokerId {
            broker_id,
            register_check_code,
        })
    }

    pub fn complete_controller_broker_id_application(&mut self, config: &BrokerConfig) -> RocketMQResult<u64> {
        if self.temp_metadata.is_some() {
            self.commit_temp_metadata(config)
        } else {
            Ok(self.broker_controller_id)
        }
    }

    pub fn register_followup(
        &self,
        master_broker_id: Option<u64>,
        master_epoch: Option<i32>,
    ) -> ControllerRegisterFollowup {
        if master_broker_id.is_some() && master_epoch.is_some() {
            ControllerRegisterFollowup::ApplyRoleChange
        } else {
            ControllerRegisterFollowup::HeartbeatThenQueryReplicaInfo
        }
    }

    pub fn replica_info_followup(
        &self,
        master_broker_id: Option<u64>,
        master_epoch: Option<i32>,
    ) -> ControllerReplicaInfoFollowup {
        if master_broker_id.is_some() && master_epoch.is_some() {
            ControllerReplicaInfoFollowup::ApplyRoleChange
        } else {
            ControllerReplicaInfoFollowup::ElectMaster
        }
    }

    pub fn change_broker_role(
        &mut self,
        controller_leader_address: Option<CheetahString>,
        new_master_broker_id: Option<u64>,
        new_master_address: Option<CheetahString>,
        new_master_epoch: Option<i32>,
        sync_state_set_epoch: Option<i32>,
        sync_state_set: Option<&HashSet<i64>>,
    ) -> RocketMQResult<RoleChangeOutcome> {
        if let Some(controller_leader_address) = controller_leader_address {
            self.set_controller_leader_address(controller_leader_address);
        }

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
        let should_start_special_service = self.master_broker_id == Some(self.broker_controller_id);
        RoleChangeOutcome {
            role,
            sync_state_set_changed,
            master_broker_id: self.master_broker_id,
            master_address: self.master_address.clone(),
            master_epoch: self.master_epoch,
            sync_state_set_epoch: self.sync_state_set_epoch,
            sync_state_set: self.sync_state_set.clone(),
            local_broker_id: if should_start_special_service {
                MASTER_ID
            } else {
                self.broker_controller_id
            },
            should_start_special_service,
            should_register_to_namesrv: role.is_some(),
        }
    }

    fn recover_registration_state(&mut self) {
        self.metadata = match read_metadata_file::<BrokerMetadataRecord>(&self.metadata_path) {
            Ok(metadata) => metadata,
            Err(error) => {
                warn!(
                    "Failed to read broker metadata from {}: {}",
                    self.metadata_path.display(),
                    error
                );
                None
            }
        };
        self.temp_metadata = match read_metadata_file::<TempBrokerMetadataRecord>(&self.temp_metadata_path) {
            Ok(metadata) => metadata,
            Err(error) => {
                warn!(
                    "Failed to read temp broker metadata from {}: {}",
                    self.temp_metadata_path.display(),
                    error
                );
                None
            }
        };

        if let Some(metadata) = self.metadata.as_ref() {
            self.broker_controller_id = metadata.broker_id;
            self.register_state = RegisterState::CreateMetadataFileDone;
            return;
        }
        if let Some(metadata) = self.temp_metadata.as_ref() {
            self.broker_controller_id = metadata.broker_id;
            self.register_state = RegisterState::CreateTempMetadataFileDone;
            return;
        }
        self.register_state = RegisterState::Initial;
    }
}

fn validate_metadata_record(
    metadata: Option<&BrokerMetadataRecord>,
    config: &BrokerConfig,
    label: &str,
) -> RocketMQResult<()> {
    if let Some(metadata) = metadata {
        if metadata.cluster_name != config.broker_identity.broker_cluster_name.as_str() {
            return Err(RocketMQError::illegal_argument(format!(
                "{} cluster mismatch: persisted={}, config={}",
                label, metadata.cluster_name, config.broker_identity.broker_cluster_name
            )));
        }
        if metadata.broker_name != config.broker_identity.broker_name.as_str() {
            return Err(RocketMQError::illegal_argument(format!(
                "{} broker name mismatch: persisted={}, config={}",
                label, metadata.broker_name, config.broker_identity.broker_name
            )));
        }
    }
    Ok(())
}

fn validate_temp_metadata_record(
    metadata: Option<&TempBrokerMetadataRecord>,
    config: &BrokerConfig,
) -> RocketMQResult<()> {
    if let Some(metadata) = metadata {
        if metadata.cluster_name != config.broker_identity.broker_cluster_name.as_str() {
            return Err(RocketMQError::illegal_argument(format!(
                "temp broker metadata cluster mismatch: persisted={}, config={}",
                metadata.cluster_name, config.broker_identity.broker_cluster_name
            )));
        }
        if metadata.broker_name != config.broker_identity.broker_name.as_str() {
            return Err(RocketMQError::illegal_argument(format!(
                "temp broker metadata broker name mismatch: persisted={}, config={}",
                metadata.broker_name, config.broker_identity.broker_name
            )));
        }
    }
    Ok(())
}

fn metadata_path_from_config(message_store_config: &MessageStoreConfig) -> PathBuf {
    if let Some(path) = message_store_config.store_path_broker_identity.as_ref() {
        return PathBuf::from(path.as_str());
    }
    PathBuf::from(message_store_config.store_path_root_dir.as_str()).join("brokerIdentity.json")
}

fn temp_metadata_path_from_metadata_path(metadata_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}-temp", metadata_path.display()))
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

fn read_metadata_file<T>(path: &Path) -> RocketMQResult<Option<T>>
where
    T: for<'de> Deserialize<'de>,
{
    match fs::read_to_string(path) {
        Ok(content) if content.trim().is_empty() => Ok(None),
        Ok(content) => serde_json::from_str(&content)
            .map(Some)
            .map_err(|error| RocketMQError::illegal_argument(format!("decode metadata failed: {}", error))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(RocketMQError::illegal_argument(format!(
            "read metadata file failed: {}",
            error
        ))),
    }
}

fn write_metadata_file<T>(path: &Path, value: &T) -> RocketMQResult<()>
where
    T: Serialize,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            RocketMQError::illegal_argument(format!(
                "create metadata directory failed for {}: {}",
                parent.display(),
                error
            ))
        })?;
    }
    let content = serde_json::to_string(value)
        .map_err(|error| RocketMQError::illegal_argument(format!("encode metadata failed: {}", error)))?;
    fs::write(path, content).map_err(|error| {
        RocketMQError::illegal_argument(format!("write metadata file failed for {}: {}", path.display(), error))
    })?;
    Ok(())
}

fn delete_metadata_file(path: &Path) -> RocketMQResult<()> {
    match fs::remove_file(path) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(RocketMQError::illegal_argument(format!(
            "delete metadata file failed for {}: {}",
            path.display(),
            error
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    fn broker_config_with_controller_addr(controller_addr: &str, broker_id: u64) -> BrokerConfig {
        let mut config = BrokerConfig {
            controller_addr: controller_addr.into(),
            ..BrokerConfig::default()
        };
        config.broker_identity.broker_id = broker_id;
        config
    }

    fn message_store_config_with_identity_path(suffix: &str) -> MessageStoreConfig {
        let mut config = MessageStoreConfig::default();
        let path = env::temp_dir().join(format!("rocketmq-rust-controller-mode-{}-{}", suffix, current_millis()));
        config.store_path_broker_identity = Some(path.to_string_lossy().into_owned().into());
        config
    }

    #[test]
    fn new_manager_parses_controller_addresses() {
        let config = broker_config_with_controller_addr("127.0.0.1:9878;127.0.0.2:9878;", 2);
        let message_store_config = message_store_config_with_identity_path("parse");
        let manager = ReplicasManager::new(&config, &message_store_config, "127.0.0.1:10911".into());

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
    fn create_and_commit_temp_metadata_round_trip() {
        let config = broker_config_with_controller_addr("127.0.0.1:9878", 7);
        let message_store_config = message_store_config_with_identity_path("metadata");
        let mut manager = ReplicasManager::new(&config, &message_store_config, "127.0.0.1:10911".into());

        manager
            .create_temp_metadata(&config, 9)
            .expect("temp metadata should be created");
        assert_eq!(manager.register_state(), RegisterState::CreateTempMetadataFileDone);
        assert_eq!(manager.pending_registration().map(|(broker_id, _)| broker_id), Some(9));

        manager
            .commit_temp_metadata(&config)
            .expect("metadata should be committed");
        assert_eq!(manager.register_state(), RegisterState::CreateMetadataFileDone);
        assert_eq!(manager.broker_controller_id(), 9);
        assert!(manager.pending_registration().is_none());

        let recovered = ReplicasManager::new(&config, &message_store_config, "127.0.0.1:10911".into());
        assert_eq!(recovered.broker_controller_id(), 9);
        assert_eq!(recovered.register_state(), RegisterState::CreateMetadataFileDone);
    }

    #[test]
    fn change_broker_role_promotes_local_broker_to_master() {
        let config = broker_config_with_controller_addr("127.0.0.1:9878", 2);
        let message_store_config = message_store_config_with_identity_path("master");
        let mut manager = ReplicasManager::new(&config, &message_store_config, "127.0.0.1:10911".into());
        let sync_state_set = HashSet::from([2_i64, 3_i64]);

        let outcome = manager
            .change_broker_role(
                Some("127.0.0.2:9878".into()),
                Some(2),
                None,
                Some(3),
                Some(4),
                Some(&sync_state_set),
            )
            .expect("role change should succeed");

        assert_eq!(outcome.role, Some(BrokerReplicaRole::Master));
        assert_eq!(outcome.master_broker_id, Some(2));
        assert_eq!(outcome.master_address, Some(CheetahString::from("127.0.0.1:10911")));
        assert_eq!(outcome.master_epoch, 3);
        assert_eq!(outcome.sync_state_set_epoch, 4);
        assert_eq!(outcome.sync_state_set, sync_state_set);
        assert_eq!(outcome.local_broker_id, MASTER_ID);
        assert!(outcome.should_start_special_service);
        assert!(outcome.should_register_to_namesrv);
        assert_eq!(outcome.target_broker_role(), Some(BrokerRole::SyncMaster));
        assert!(outcome.should_clear_slave_master_address());
        assert!(!outcome.should_sync_master_online());
        assert_eq!(
            manager.controller_leader_address(),
            Some(&CheetahString::from("127.0.0.2:9878"))
        );
    }

    #[test]
    fn change_broker_role_demotes_local_broker_to_slave() {
        let config = broker_config_with_controller_addr("127.0.0.1:9878", 2);
        let message_store_config = message_store_config_with_identity_path("slave");
        let mut manager = ReplicasManager::new(&config, &message_store_config, "127.0.0.1:10911".into());
        let sync_state_set = HashSet::from([1_i64, 2_i64]);

        let outcome = manager
            .change_broker_role(
                Some("127.0.0.3:9878".into()),
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
        assert_eq!(outcome.local_broker_id, 2);
        assert!(!outcome.should_start_special_service);
        assert!(outcome.should_register_to_namesrv);
        assert_eq!(outcome.target_broker_role(), Some(BrokerRole::Slave));
        assert_eq!(
            outcome.slave_master_address(),
            Some(&CheetahString::from("127.0.0.9:10911"))
        );
        assert!(outcome.should_sync_master_online());
        assert_eq!(
            manager.controller_leader_address(),
            Some(&CheetahString::from("127.0.0.3:9878"))
        );
    }

    #[test]
    fn change_broker_role_ignores_stale_epoch() {
        let config = broker_config_with_controller_addr("127.0.0.1:9878", 2);
        let message_store_config = message_store_config_with_identity_path("stale");
        let mut manager = ReplicasManager::new(&config, &message_store_config, "127.0.0.1:10911".into());
        let sync_state_set = HashSet::from([2_i64]);

        manager
            .change_broker_role(None, Some(2), None, Some(3), Some(3), Some(&sync_state_set))
            .expect("initial role change should succeed");

        let stale = manager
            .change_broker_role(
                Some("127.0.0.4:9878".into()),
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
        assert_eq!(stale.local_broker_id, MASTER_ID);
        assert!(stale.should_start_special_service);
        assert!(!stale.should_register_to_namesrv);
        assert_eq!(stale.target_broker_role(), None);
        assert!(!stale.should_clear_slave_master_address());
        assert!(!stale.should_sync_master_online());
        assert_eq!(
            manager.controller_leader_address(),
            Some(&CheetahString::from("127.0.0.4:9878"))
        );
    }

    #[test]
    fn heartbeat_targets_prefer_known_leader() {
        let config = broker_config_with_controller_addr("127.0.0.1:9878;127.0.0.2:9878", 2);
        let message_store_config = message_store_config_with_identity_path("heartbeat");
        let mut manager = ReplicasManager::new(&config, &message_store_config, "127.0.0.1:10911".into());
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

    #[test]
    fn prepare_controller_broker_id_action_reuses_pending_and_existing_metadata() {
        let config = broker_config_with_controller_addr("127.0.0.1:9878", 7);
        let message_store_config = message_store_config_with_identity_path("broker-id-action");
        let mut manager = ReplicasManager::new(&config, &message_store_config, "127.0.0.1:10911".into());

        let action = manager
            .prepare_controller_broker_id_action(&config, Some(11))
            .expect("prepare controller broker id action");
        let (pending_broker_id, pending_check_code) =
            manager.pending_registration().expect("pending controller registration");
        assert_eq!(
            action,
            ControllerBrokerIdAction::ApplyBrokerId {
                broker_id: 11,
                register_check_code: pending_check_code.clone(),
            }
        );
        assert_eq!(pending_broker_id, 11);

        let reused_action = manager
            .prepare_controller_broker_id_action(&config, None)
            .expect("reuse pending registration");
        assert_eq!(
            reused_action,
            ControllerBrokerIdAction::ApplyBrokerId {
                broker_id: 11,
                register_check_code: pending_check_code,
            }
        );

        manager
            .complete_controller_broker_id_application(&config)
            .expect("commit controller broker id");
        let existing_action = manager
            .prepare_controller_broker_id_action(&config, None)
            .expect("reuse committed broker id");
        assert_eq!(existing_action, ControllerBrokerIdAction::UseCurrent(11));
    }

    #[test]
    fn controller_bootstrap_followups_are_derived_by_replicas_manager() {
        let config = broker_config_with_controller_addr("127.0.0.1:9878", 7);
        let message_store_config = message_store_config_with_identity_path("bootstrap-followup");
        let manager = ReplicasManager::new(&config, &message_store_config, "127.0.0.1:10911".into());

        assert_eq!(
            manager.register_followup(None, None),
            ControllerRegisterFollowup::HeartbeatThenQueryReplicaInfo
        );
        assert_eq!(
            manager.register_followup(Some(7), Some(3)),
            ControllerRegisterFollowup::ApplyRoleChange
        );
        assert_eq!(
            manager.replica_info_followup(None, None),
            ControllerReplicaInfoFollowup::ElectMaster
        );
        assert_eq!(
            manager.replica_info_followup(Some(9), Some(4)),
            ControllerReplicaInfoFollowup::ApplyRoleChange
        );
    }
}
