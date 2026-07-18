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

//! # ReplicasInfoManager
//!
//! The manager that manages the replicas info for all brokers.
//! This can be thought of as the controller's memory state machine.
//!
//! ## Thread Safety
//!
//! This struct uses `DashMap` for concurrent access, making it thread-safe.
//! All methods can be called from multiple threads without external synchronization.
//!
//! ## Architecture
//!
//! The manager maintains two key data structures:
//! - `replica_info_table`: Maps broker name to `BrokerReplicaInfo` (broker topology)
//! - `sync_state_set_info_table`: Maps broker name to `SyncStateInfo` (ISR state)
//!
//! ## Event-Driven Design
//!
//! Upper layer components must call methods sequentially to update the state machine.
//! Methods return `ControllerResult` containing events that should be applied via `apply_event`.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::mix_all::FIRST_BROKER_CONTROLLER_ID;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_remoting::protocol::body::broker_replicas_info::BrokerReplicasInfo;
use rocketmq_remoting::protocol::body::broker_replicas_info::ReplicaIdentity;
use rocketmq_remoting::protocol::body::broker_replicas_info::ReplicasInfo;
use rocketmq_remoting::protocol::body::elect_master_response_body::ElectMasterResponseBody;
use rocketmq_remoting::protocol::body::sync_state_set_body::SyncStateSet;
use rocketmq_remoting::protocol::header::controller::alter_sync_state_set_response_header::AlterSyncStateSetResponseHeader;
use rocketmq_remoting::protocol::header::controller::apply_broker_id_response_header::ApplyBrokerIdResponseHeader;
use rocketmq_remoting::protocol::header::controller::get_next_broker_id_response_header::GetNextBrokerIdResponseHeader;
use rocketmq_remoting::protocol::header::controller::get_replica_info_response_header::GetReplicaInfoResponseHeader;
use rocketmq_remoting::protocol::header::controller::register_broker_to_controller_response_header::RegisterBrokerToControllerResponseHeader;
use rocketmq_remoting::protocol::header::elect_master_response_header::ElectMasterResponseHeader;
use rocketmq_remoting::protocol::RemotingSerializable;
use tracing::error;
use tracing::info;
use tracing::warn;

#[cfg(test)]
use crate::config::ControllerConfig;
use crate::config::ControllerConfigReader;
use crate::elect::elect_policy::ElectPolicy;
use crate::error::ControllerError;
use crate::error::Result;
use crate::event::alter_sync_state_set_event::AlterSyncStateSetEvent;
use crate::event::apply_broker_id_event::ApplyBrokerIdEvent;
use crate::event::clean_broker_data_event::CleanBrokerDataEvent;
use crate::event::controller_result::ControllerResult;
use crate::event::elect_master_event::ElectMasterEvent;
use crate::event::event_message::EventMessage;
use crate::event::event_type::EventType;
use crate::event::update_broker_address_event::UpdateBrokerAddressEvent;
use crate::helper::broker_valid_predicate::BrokerValidPredicate;
use crate::manager::broker_replica_info::BrokerReplicaInfo;
use crate::manager::sync_state_info::SyncStateInfo;
use crate::typ::BrokerIdentityInfoSnapshot;
use crate::typ::BrokerLiveInfoSnapshot;

/// Serialization structure for state machine persistence
#[derive(serde::Serialize, serde::Deserialize)]
struct SerializedState {
    replica_info_table: HashMap<String, BrokerReplicaInfo>,
    sync_state_set_info_table: HashMap<String, SyncStateInfo>,
    #[serde(default)]
    broker_live_table: Vec<(BrokerIdentityInfoSnapshot, BrokerLiveInfoSnapshot)>,
}

/// The manager that manages the replicas info for all brokers.
///
/// This class serves as the controller's memory state machine. If the upper layer
/// wants to update the state machine, it must sequentially call its methods.
///
/// # Thread Safety
///
/// All methods are thread-safe and can be called concurrently from multiple threads.
pub struct ReplicasInfoManager {
    /// Controller configuration
    config: ControllerConfigReader,

    /// Replica information table: broker_name -> BrokerReplicaInfo
    /// Thread-safe concurrent map
    replica_info_table: Arc<DashMap<CheetahString, BrokerReplicaInfo>>,

    /// Sync state set information table: broker_name -> SyncStateInfo
    /// Thread-safe concurrent map
    sync_state_set_info_table: Arc<DashMap<CheetahString, SyncStateInfo>>,

    /// Replicated broker liveness table, equivalent to Java RaftReplicasInfoManager
    /// brokerLiveTable.
    broker_live_table: Arc<DashMap<BrokerIdentityInfoSnapshot, BrokerLiveInfoSnapshot>>,
}

impl ReplicasInfoManager {
    /// Create a new ReplicasInfoManager with the given configuration
    pub fn new(config: ControllerConfigReader) -> Self {
        Self {
            config,
            replica_info_table: Arc::new(DashMap::new()),
            sync_state_set_info_table: Arc::new(DashMap::new()),
            broker_live_table: Arc::new(DashMap::new()),
        }
    }

    /// Alter the sync state set for a broker
    ///
    /// # Arguments
    ///
    /// * `broker_name` - The broker name
    /// * `master_broker_id` - The master broker id
    /// * `master_epoch` - The master epoch
    /// * `new_sync_state_set` - The new sync state set
    /// * `sync_state_set_epoch` - The sync state set epoch
    /// * `broker_alive_predicate` - Predicate to check if broker is alive
    ///
    /// # Returns
    ///
    /// A `ControllerResult` containing the response and events
    pub fn alter_sync_state_set(
        &self,
        broker_name: &str,
        master_broker_id: u64,
        master_epoch: i32,
        new_sync_state_set: HashSet<u64>,
        sync_state_set_epoch: i32,
        broker_alive_predicate: &dyn BrokerValidPredicate,
    ) -> ControllerResult<AlterSyncStateSetResponseHeader> {
        let mut result = ControllerResult::new(Some(AlterSyncStateSetResponseHeader::default()));

        // Check if broker exists
        if !self.is_contains_broker(broker_name) {
            result.set_code_and_remark(
                ResponseCode::ControllerAlterSyncStateSetFailed,
                "Broker metadata is not existed",
            );
            return result;
        }

        let sync_state_info = self
            .sync_state_set_info_table
            .get(broker_name)
            .expect("SyncStateInfo should exist");
        let broker_replica_info = self
            .replica_info_table
            .get(broker_name)
            .expect("BrokerReplicaInfo should exist");

        let old_sync_state_set = sync_state_info.sync_state_set();

        // Check whether the oldSyncStateSet is equal with newSyncStateSet
        if old_sync_state_set.len() == new_sync_state_set.len()
            && old_sync_state_set.iter().all(|id| new_sync_state_set.contains(id))
        {
            let err = "The newSyncStateSet is equal with oldSyncStateSet, no needed to update syncStateSet";
            warn!("{}", err);
            result.set_code_and_remark(ResponseCode::ControllerAlterSyncStateSetFailed, err);
            return result;
        }

        // Check master
        if sync_state_info.master_broker_id() != Some(master_broker_id) {
            let err = format!(
                "Rejecting alter syncStateSet request because the current leader is:{:?}, not {}",
                sync_state_info.master_broker_id(),
                master_broker_id
            );
            error!("{}", err);
            result.set_code_and_remark(ResponseCode::ControllerInvalidMaster, &err);
            return result;
        }

        // Check master epoch
        if master_epoch != sync_state_info.master_epoch() {
            let err = format!(
                "Rejecting alter syncStateSet request because the current master epoch is:{}, not {}",
                sync_state_info.master_epoch(),
                master_epoch
            );
            error!("{}", err);
            result.set_code_and_remark(ResponseCode::ControllerFencedMasterEpoch, &err);
            return result;
        }

        // Check syncStateSet epoch
        if sync_state_set_epoch != sync_state_info.sync_state_set_epoch() {
            let err = format!(
                "Rejecting alter syncStateSet request because the current syncStateSet epoch is:{}, not {}",
                sync_state_info.sync_state_set_epoch(),
                sync_state_set_epoch
            );
            error!("{}", err);
            result.set_code_and_remark(ResponseCode::ControllerFencedSyncStateSetEpoch, &err);
            return result;
        }

        // Check newSyncStateSet correctness
        for &replica in &new_sync_state_set {
            if !broker_replica_info.is_broker_exist(replica) {
                let err = format!(
                    "Rejecting alter syncStateSet request because the replicas {} don't exist",
                    replica
                );
                error!("{}", err);
                result.set_code_and_remark(ResponseCode::ControllerInvalidReplicas, &err);
                return result;
            }

            if !broker_alive_predicate.check(
                broker_replica_info.cluster_name(),
                broker_replica_info.broker_name(),
                Some(replica as i64),
            ) {
                let err = format!(
                    "Rejecting alter syncStateSet request because the replicas {} don't alive",
                    replica
                );
                error!("{}", err);
                result.set_code_and_remark(ResponseCode::ControllerBrokerNotAlive, &err);
                return result;
            }
        }

        if !new_sync_state_set.contains(&master_broker_id) {
            let err = format!(
                "Rejecting alter syncStateSet request because the newSyncStateSet don't contains origin leader {}",
                master_broker_id
            );
            error!("{}", err);
            result.set_code_and_remark(ResponseCode::ControllerAlterSyncStateSetFailed, &err);
            return result;
        }

        // Generate event
        let new_epoch = sync_state_info.sync_state_set_epoch() + 1;
        if let Some(response) = result.response_mut() {
            response.new_sync_state_set_epoch = new_epoch;
        }

        // Encode sync state set
        let sync_state_set_i64: HashSet<i64> = new_sync_state_set.iter().map(|&id| id as i64).collect();
        let sync_state_set_data = SyncStateSet::with_values(sync_state_set_i64, new_epoch);
        result.set_body(Bytes::from(sync_state_set_data.encode().unwrap()));

        let event = AlterSyncStateSetEvent::new(broker_name, new_sync_state_set);
        result.add_event(Arc::new(event));

        result
    }

    /// Elect a new master for a broker
    ///
    /// # Arguments
    ///
    /// * `broker_name` - The broker name
    /// * `broker_id` - The broker id requesting election (None for controller-triggered)
    /// * `designate_elect` - Whether this is a designated election
    /// * `elect_policy` - The election policy to use
    ///
    /// # Returns
    ///
    /// A `ControllerResult` containing the response and events
    pub fn elect_master(
        &self,
        broker_name: &str,
        broker_id: Option<u64>,
        designate_elect: bool,
        elect_policy: &dyn ElectPolicy,
    ) -> ControllerResult<ElectMasterResponseHeader> {
        let mut result = ControllerResult::new(Some(ElectMasterResponseHeader::default()));

        // Check if broker exists
        if !self.is_contains_broker(broker_name) {
            result.set_code_and_remark(
                ResponseCode::ControllerBrokerNeedToBeRegistered,
                "Broker hasn't been registered",
            );
            return result;
        }

        let sync_state_info = self
            .sync_state_set_info_table
            .get(broker_name)
            .expect("SyncStateInfo should exist");
        let broker_replica_info = self
            .replica_info_table
            .get(broker_name)
            .expect("BrokerReplicaInfo should exist");

        let sync_state_set = sync_state_info.sync_state_set();
        let old_master = sync_state_info.master_broker_id();

        let mut new_master: Option<u64> = None;

        // If this is the first time to elect a master
        if sync_state_info.is_first_time_for_elect() {
            if let Some(bid) = broker_id {
                new_master = Some(bid);
            }
        }

        // Elect by policy if not first time or new_master is None
        if new_master.is_none() {
            let all_replica_brokers = if self.config.snapshot().enable_elect_unclean_master {
                Some(broker_replica_info.get_all_broker())
            } else {
                None
            };

            let assigned_broker_id = if designate_elect { broker_id } else { None };

            // Convert HashSet<u64> to HashSet<i64> for elect_policy
            let sync_state_set_i64: HashSet<i64> = sync_state_set.iter().map(|&id| id as i64).collect();
            let all_replica_brokers_i64 = all_replica_brokers.map(|set: std::collections::HashSet<u64>| {
                set.iter()
                    .map(|&id| id as i64)
                    .collect::<std::collections::HashSet<i64>>()
            });

            new_master = elect_policy
                .elect(
                    broker_replica_info.cluster_name(),
                    broker_replica_info.broker_name(),
                    &sync_state_set_i64,
                    &all_replica_brokers_i64.unwrap_or_default(),
                    old_master.map(|id| id as i64),
                    assigned_broker_id.map(|id| id as i64),
                )
                .map(|id| id as u64);
        }

        // Check if old master is still valid
        if let (Some(new_m), Some(old_m)) = (new_master, old_master) {
            if new_m == old_m {
                let err = format!(
                    "The old master {} is still alive, not need to elect new master for broker {}",
                    old_m,
                    broker_replica_info.broker_name()
                );
                warn!("{}", err);

                if let Some(response) = result.response_mut() {
                    response.master_epoch = Some(sync_state_info.master_epoch());
                    response.sync_state_set_epoch = Some(sync_state_info.sync_state_set_epoch());
                    response.master_broker_id = Some(old_m as i64);
                    response.master_address = broker_replica_info.get_broker_address(old_m);
                }

                let sync_state_set_i64: std::collections::HashSet<i64> =
                    sync_state_set.iter().map(|&id| id as i64).collect();
                let body = ElectMasterResponseBody {
                    sync_state_set: sync_state_set_i64,
                    broker_member_group: None,
                };
                result.set_body(Bytes::from(body.encode().unwrap()));
                result.set_code_and_remark(ResponseCode::ControllerMasterStillExist, &err);
                return result;
            }
        }

        // A new master is elected
        if let Some(new_m) = new_master {
            let master_epoch = sync_state_info.master_epoch();
            let sync_state_set_epoch = sync_state_info.sync_state_set_epoch();
            let mut new_sync_state_set = HashSet::new();
            new_sync_state_set.insert(new_m);

            if let Some(response) = result.response_mut() {
                response.master_broker_id = Some(new_m as i64);
                response.master_address = broker_replica_info.get_broker_address(new_m);
                response.master_epoch = Some(master_epoch + 1);
                response.sync_state_set_epoch = Some(sync_state_set_epoch + 1);
            }

            let sync_state_set_i64: std::collections::HashSet<i64> =
                new_sync_state_set.iter().map(|&id| id as i64).collect();
            let body = ElectMasterResponseBody {
                sync_state_set: sync_state_set_i64,
                broker_member_group: Some(self.build_broker_member_group(&broker_replica_info)),
            };
            result.set_body(Bytes::from(body.encode().unwrap()));

            let event = ElectMasterEvent::with_new_master(broker_name, new_m);
            result.add_event(Arc::new(event));

            info!("Elect new master {} for broker {}", new_m, broker_name);
            return result;
        }

        // If elect failed and the electMaster is triggered by controller
        if broker_id.is_none() {
            let event = ElectMasterEvent::without_new_master(false, broker_name);
            result.add_event(Arc::new(event));
            result.set_code_and_remark(
                ResponseCode::ControllerMasterNotAvailable,
                "Old master has down and failed to elect a new broker master",
            );
        } else {
            result.set_code_and_remark(
                ResponseCode::ControllerElectMasterFailed,
                "Failed to elect a new master",
            );
        }

        warn!("Failed to elect a new master for broker {}", broker_name);
        result
    }

    /// Get the next broker id for a broker set
    pub fn get_next_broker_id(
        &self,
        cluster_name: &str,
        broker_name: &str,
    ) -> ControllerResult<GetNextBrokerIdResponseHeader> {
        let mut result = ControllerResult::new(Some(GetNextBrokerIdResponseHeader {
            cluster_name: Some(CheetahString::from_string(cluster_name.to_string())),
            broker_name: Some(CheetahString::from_string(broker_name.to_string())),
            next_broker_id: Some(FIRST_BROKER_CONTROLLER_ID),
        }));

        if let Some(broker_replica_info) = self.replica_info_table.get(broker_name) {
            if let Some(response) = result.response_mut() {
                response.next_broker_id = Some(broker_replica_info.get_next_assign_broker_id());
            }
        }

        result
    }

    /// Apply for a broker id
    pub fn apply_broker_id(
        &self,
        cluster_name: &str,
        broker_name: &str,
        broker_address: &str,
        applied_broker_id: u64,
        register_check_code: &str,
    ) -> ControllerResult<ApplyBrokerIdResponseHeader> {
        let mut result = ControllerResult::new(Some(ApplyBrokerIdResponseHeader {
            cluster_name: Some(CheetahString::from_string(cluster_name.to_string())),
            broker_name: Some(CheetahString::from_string(broker_name.to_string())),
        }));

        let event = ApplyBrokerIdEvent::new(
            cluster_name,
            broker_name,
            broker_address,
            applied_broker_id,
            register_check_code,
        );

        // Broker-set unregistered
        if let Some(broker_replica_info) = self.replica_info_table.get(broker_name) {
            // Broker-set registered
            if !broker_replica_info.is_broker_exist(applied_broker_id)
                || register_check_code
                    == broker_replica_info
                        .get_broker_register_check_code(applied_broker_id)
                        .unwrap_or_default()
                        .as_str()
            {
                result.add_event(Arc::new(event));
                return result;
            }

            result.set_code_and_remark(
                ResponseCode::ControllerBrokerIdInvalid,
                format!(
                    "Fail to apply brokerId: {} in broker-set: {}",
                    applied_broker_id, broker_name
                ),
            );
        } else {
            // First brokerId
            if applied_broker_id == FIRST_BROKER_CONTROLLER_ID {
                result.add_event(Arc::new(event));
            } else {
                result.set_code_and_remark(
                    ResponseCode::ControllerBrokerIdInvalid,
                    format!(
                        "Broker-set: {} hasn't been registered in controller, but broker try to apply brokerId: {}",
                        broker_name, applied_broker_id
                    ),
                );
            }
        }

        result
    }

    /// Register a broker to controller
    pub fn register_broker(
        &self,
        cluster_name: &str,
        broker_name: &str,
        broker_address: &str,
        broker_id: u64,
        alive_predicate: &dyn BrokerValidPredicate,
    ) -> ControllerResult<RegisterBrokerToControllerResponseHeader> {
        let mut result = ControllerResult::new(Some(RegisterBrokerToControllerResponseHeader {
            cluster_name: Some(CheetahString::from_string(cluster_name.to_string())),
            broker_name: Some(CheetahString::from_string(broker_name.to_string())),
            master_broker_id: None,
            master_address: None,
            master_epoch: None,
            sync_state_set_epoch: None,
        }));

        if !self.is_contains_broker(broker_name) {
            result.set_code_and_remark(
                ResponseCode::ControllerBrokerNeedToBeRegistered,
                format!("Broker-set: {} hasn't been registered in controller", broker_name),
            );
            return result;
        }

        let broker_replica_info = self
            .replica_info_table
            .get(broker_name)
            .expect("BrokerReplicaInfo should exist");
        let sync_state_info = self
            .sync_state_set_info_table
            .get(broker_name)
            .expect("SyncStateInfo should exist");

        if !broker_replica_info.is_broker_exist(broker_id) {
            result.set_code_and_remark(
                ResponseCode::ControllerBrokerNeedToBeRegistered,
                format!(
                    "BrokerId: {} hasn't been registered in broker-set: {}",
                    broker_id, broker_name
                ),
            );
            return result;
        }

        // If master still exists
        if let Some(master_id) = sync_state_info.master_broker_id() {
            if alive_predicate.check(cluster_name, broker_name, Some(master_id as i64)) {
                if let Some(response) = result.response_mut() {
                    response.master_broker_id = Some(master_id as i64);
                    response.master_address = broker_replica_info.get_broker_address(master_id);
                    response.master_epoch = Some(sync_state_info.master_epoch());
                    response.sync_state_set_epoch = Some(sync_state_info.sync_state_set_epoch());
                }
            }
        }

        let sync_state_set_i64: HashSet<i64> = sync_state_info.sync_state_set().iter().map(|&id| id as i64).collect();
        let sync_state_set_data = SyncStateSet::with_values(sync_state_set_i64, sync_state_info.sync_state_set_epoch());
        result.set_body(Bytes::from(sync_state_set_data.encode().unwrap()));

        // If this broker's address has been changed, we need to update it
        if let Some(current_addr) = broker_replica_info.get_broker_address(broker_id) {
            if current_addr.as_str() != broker_address {
                let event = UpdateBrokerAddressEvent::new(cluster_name, broker_name, broker_address, broker_id);
                result.add_event(Arc::new(event));
            }
        }

        result
    }

    /// Get replica info for a broker
    pub fn get_replica_info(&self, broker_name: &str) -> ControllerResult<GetReplicaInfoResponseHeader> {
        let mut result = ControllerResult::new(Some(GetReplicaInfoResponseHeader::default()));

        if self.is_contains_broker(broker_name) {
            let sync_state_info = self
                .sync_state_set_info_table
                .get(broker_name)
                .expect("SyncStateInfo should exist");
            let broker_replica_info = self
                .replica_info_table
                .get(broker_name)
                .expect("BrokerReplicaInfo should exist");

            if let Some(response) = result.response_mut() {
                let master_id = sync_state_info.master_broker_id();
                response.master_broker_id = master_id.map(|id| id as i64);
                response.master_address = master_id
                    .and_then(|id| broker_replica_info.get_broker_address(id))
                    .map(|s| s.to_string());
                response.master_epoch = Some(sync_state_info.master_epoch());
            }

            let sync_state_set_i64: HashSet<i64> =
                sync_state_info.sync_state_set().iter().map(|&id| id as i64).collect();
            let sync_state_set_data =
                SyncStateSet::with_values(sync_state_set_i64, sync_state_info.sync_state_set_epoch());
            result.set_body(Bytes::from(sync_state_set_data.encode().unwrap()));
        } else {
            result.set_code_and_remark(
                ResponseCode::ControllerBrokerMetadataNotExist,
                "Broker metadata is not existed",
            );
        }

        result
    }

    /// Get sync state data for multiple brokers
    pub fn get_sync_state_data(
        &self,
        broker_names: &[String],
        broker_alive_predicate: &dyn BrokerValidPredicate,
    ) -> ControllerResult<()> {
        let mut result = ControllerResult::new(None);
        let mut broker_replicas_info = BrokerReplicasInfo::new();

        for broker_name in broker_names {
            if !self.is_contains_broker(broker_name.as_str()) {
                continue;
            }

            let sync_state_info = self
                .sync_state_set_info_table
                .get(broker_name.as_str())
                .expect("SyncStateInfo should exist");
            let broker_replica_info = self
                .replica_info_table
                .get(broker_name.as_str())
                .expect("BrokerReplicaInfo should exist");

            let sync_state_set = sync_state_info.sync_state_set();
            let master_broker_id = sync_state_info.master_broker_id();

            let mut in_sync_replicas = Vec::new();
            let mut not_in_sync_replicas = Vec::new();

            for (&broker_id, broker_address) in &broker_replica_info.get_broker_id_table() {
                let is_alive = broker_alive_predicate.check(
                    broker_replica_info.cluster_name(),
                    broker_name,
                    Some(broker_id as i64),
                );

                let replica = ReplicaIdentity::new_with_alive(
                    broker_name.to_string(),
                    broker_id,
                    broker_address.to_string(),
                    is_alive,
                );

                if sync_state_set.contains(&broker_id) {
                    in_sync_replicas.push(replica);
                } else {
                    not_in_sync_replicas.push(replica);
                }
            }

            let master_address = master_broker_id
                .and_then(|id| broker_replica_info.get_broker_address(id))
                .unwrap_or_else(|| CheetahString::from_static_str(""));

            let replicas_info = ReplicasInfo::new(
                master_broker_id.unwrap_or(0),
                master_address,
                sync_state_info.master_epoch(),
                sync_state_info.sync_state_set_epoch(),
                in_sync_replicas,
                not_in_sync_replicas,
            );

            broker_replicas_info.add_replica_info(CheetahString::from_string(broker_name.to_string()), replicas_info);
        }

        result.set_body(Bytes::from(broker_replicas_info.encode().unwrap()));
        result
    }

    /// Get all broker IDs that belong to a broker set.
    pub fn broker_ids(&self, broker_name: &str) -> HashSet<u64> {
        self.replica_info_table
            .get(broker_name)
            .map(|broker_replica_info| broker_replica_info.get_all_broker())
            .unwrap_or_default()
    }

    /// Get the cluster name for a broker set.
    pub fn cluster_name(&self, broker_name: &str) -> Option<String> {
        self.replica_info_table
            .get(broker_name)
            .map(|broker_replica_info| broker_replica_info.cluster_name().to_string())
    }

    /// Clean broker data
    pub fn clean_broker_data(
        &self,
        cluster_name: &str,
        broker_name: &str,
        broker_controller_ids_to_clean: Option<&str>,
        clean_living_broker: bool,
        valid_predicate: &dyn BrokerValidPredicate,
    ) -> ControllerResult<()> {
        let mut result = ControllerResult::new(None);

        let mut broker_id_set: Option<HashSet<u64>> = None;

        if !clean_living_broker {
            // If SyncStateInfo.masterAddress is not empty, at least one broker is alive
            if let Some(sync_state_info) = self.sync_state_set_info_table.get(broker_name) {
                if broker_controller_ids_to_clean.is_none() && sync_state_info.master_broker_id().is_some() {
                    let remark = format!("Broker {} is still alive, clean up failure", broker_name);
                    result.set_code_and_remark(ResponseCode::ControllerInvalidCleanBrokerMetadata, &remark);
                    return result;
                }
            }

            if let Some(ids_str) = broker_controller_ids_to_clean {
                match self.parse_broker_ids(ids_str) {
                    Ok(ids) => {
                        // Check if any broker is still alive
                        for &broker_id in &ids {
                            if valid_predicate.check(cluster_name, broker_name, Some(broker_id as i64)) {
                                let remark = format!(
                                    "Broker [{}, {}] is still alive, clean up failure",
                                    broker_name, broker_id
                                );
                                result.set_code_and_remark(ResponseCode::ControllerInvalidCleanBrokerMetadata, &remark);
                                return result;
                            }
                        }
                        broker_id_set = Some(ids);
                    }
                    Err(e) => {
                        let remark = format!(
                            "Please set the option <brokerControllerIdsToClean> according to the format, error: {}",
                            e
                        );
                        result.set_code_and_remark(ResponseCode::ControllerInvalidCleanBrokerMetadata, &remark);
                        return result;
                    }
                }
            }
        }

        if self.is_contains_broker(broker_name) {
            let event = CleanBrokerDataEvent::new(broker_name, broker_id_set);
            result.add_event(Arc::new(event));
            return result;
        }

        result.set_code_and_remark(
            ResponseCode::ControllerInvalidCleanBrokerMetadata,
            format!("Broker {} is not existed, clean broker data failure.", broker_name),
        );
        result
    }

    /// Scan broker sets that need reelection
    pub fn scan_need_reelect_broker_sets(&self, valid_predicate: &dyn BrokerValidPredicate) -> Vec<String> {
        let mut need_reelect_broker_sets = Vec::new();

        for entry in self.sync_state_set_info_table.iter() {
            let broker_name = entry.key();
            let sync_state_info = entry.value();

            if let Some(master_broker_id) = sync_state_info.master_broker_id() {
                let cluster_name = sync_state_info.cluster_name();

                // Now master is inactive
                if !valid_predicate.check(cluster_name, broker_name, Some(master_broker_id as i64)) {
                    // Still at least one broker alive
                    if let Some(broker_replica_info) = self.replica_info_table.get(broker_name.as_str()) {
                        let alive = broker_replica_info
                            .get_all_broker()
                            .iter()
                            .any(|&id| valid_predicate.check(cluster_name, broker_name, Some(id as i64)));

                        if alive {
                            need_reelect_broker_sets.push(broker_name.to_string());
                        }
                    }
                }
            }
        }

        need_reelect_broker_sets
    }

    /// Apply a replicated broker heartbeat.
    ///
    /// This mirrors Java RaftReplicasInfoManager: timestamps, timeout and election priority
    /// are always refreshed, while epoch/offset state only moves forward.
    pub fn on_broker_heartbeat(
        &self,
        broker_identity: BrokerIdentityInfoSnapshot,
        broker_live_info: BrokerLiveInfoSnapshot,
    ) {
        let broker_identity = BrokerIdentityInfoSnapshot::new(
            broker_identity.cluster_name,
            broker_identity.broker_name,
            Some(broker_live_info.broker_id),
        );

        if let Some(mut prev) = self.broker_live_table.get_mut(&broker_identity) {
            prev.broker_addr = broker_live_info.broker_addr;
            prev.last_update_timestamp = broker_live_info.last_update_timestamp;
            prev.heartbeat_timeout_millis = broker_live_info.heartbeat_timeout_millis;
            prev.election_priority = broker_live_info.election_priority;

            if broker_live_info.epoch > prev.epoch
                || (broker_live_info.epoch == prev.epoch && broker_live_info.max_offset > prev.max_offset)
            {
                prev.epoch = broker_live_info.epoch;
                prev.max_offset = broker_live_info.max_offset;
                prev.confirm_offset = broker_live_info.confirm_offset;
            }
        } else {
            info!(
                "new broker live info replicated, {}, brokerId:{}",
                broker_identity, broker_live_info.broker_id
            );
            self.broker_live_table.insert(broker_identity, broker_live_info);
        }
    }

    /// Remove a broker from the replicated live table after a channel close event.
    pub fn on_broker_channel_close(
        &self,
        broker_identity: &BrokerIdentityInfoSnapshot,
    ) -> Option<BrokerIdentityInfoSnapshot> {
        self.broker_live_table
            .remove(broker_identity)
            .map(|(identity, _)| identity)
    }

    /// Remove expired brokers and return broker inactive events to notify.
    pub fn check_not_active_broker(&self, check_time_millis: u64) -> Vec<BrokerIdentityInfoSnapshot> {
        let mut inactive_brokers = Vec::new();

        for entry in self.broker_live_table.iter() {
            let live_info = entry.value();
            if !live_info.is_active_at(check_time_millis) {
                inactive_brokers.push(entry.key().clone());
            }
        }

        for broker_identity in &inactive_brokers {
            self.broker_live_table.remove(broker_identity);
            warn!(
                "The broker channel expired, brokerInfo {}, checkTime={}",
                broker_identity, check_time_millis
            );
        }

        let mut broker_sets_already_reported: HashSet<String> = inactive_brokers
            .iter()
            .map(|identity| identity.broker_name.clone())
            .collect();
        for broker_name in self.scan_need_reelect_broker_sets(self) {
            if !broker_sets_already_reported.insert(broker_name.clone()) {
                continue;
            }
            inactive_brokers.push(BrokerIdentityInfoSnapshot::new(
                self.cluster_name(&broker_name).unwrap_or_default(),
                broker_name,
                None,
            ));
        }

        inactive_brokers
    }

    pub fn is_broker_active_at(
        &self,
        cluster_name: &str,
        broker_name: &str,
        broker_id: i64,
        check_time_millis: u64,
    ) -> bool {
        if broker_id < 0 {
            return false;
        }

        let identity = BrokerIdentityInfoSnapshot::new(cluster_name, broker_name, Some(broker_id as u64));
        self.broker_live_table
            .get(&identity)
            .is_some_and(|live_info| live_info.is_active_at(check_time_millis))
    }

    pub fn is_broker_active(&self, cluster_name: &str, broker_name: &str, broker_id: i64) -> bool {
        self.is_broker_active_at(cluster_name, broker_name, broker_id, current_millis())
    }

    pub fn get_broker_live_info(
        &self,
        cluster_name: &str,
        broker_name: &str,
        broker_id: i64,
    ) -> Option<BrokerLiveInfoSnapshot> {
        if broker_id < 0 {
            return None;
        }

        let identity = BrokerIdentityInfoSnapshot::new(cluster_name, broker_name, Some(broker_id as u64));
        self.broker_live_table.get(&identity).map(|entry| entry.clone())
    }

    pub fn active_broker_ids(&self, cluster_name: &str, broker_name: &str) -> HashSet<u64> {
        self.broker_ids(broker_name)
            .into_iter()
            .filter(|broker_id| self.is_broker_active(cluster_name, broker_name, *broker_id as i64))
            .collect()
    }

    /// Apply events to memory state machine
    ///
    /// # Thread Safety
    ///
    /// This method is thread-safe and can be called concurrently.
    /// Applies an event through the legacy infallible facade.
    ///
    /// New internal callers should use [`Self::try_apply_event`] so an event-type/payload mismatch
    /// is not lost.
    pub fn apply_event(&self, event: &dyn EventMessage) {
        if let Err(error) = self.try_apply_event(event) {
            warn!(error = %error, "controller event rejected by legacy apply_event facade");
        }
    }

    /// Applies an event and reports an event-type/payload mismatch as a typed error.
    pub fn try_apply_event(&self, event: &dyn EventMessage) -> Result<()> {
        match event.get_event_type() {
            EventType::AlterSyncStateSet => {
                let event = event
                    .as_any()
                    .downcast_ref::<AlterSyncStateSetEvent>()
                    .ok_or_else(|| event_payload_mismatch(EventType::AlterSyncStateSet, "AlterSyncStateSetEvent"))?;
                self.handle_alter_sync_state_set(event);
            }
            EventType::ApplyBrokerId => {
                let event = event
                    .as_any()
                    .downcast_ref::<ApplyBrokerIdEvent>()
                    .ok_or_else(|| event_payload_mismatch(EventType::ApplyBrokerId, "ApplyBrokerIdEvent"))?;
                self.handle_apply_broker_id(event);
            }
            EventType::ElectMaster => {
                let event = event
                    .as_any()
                    .downcast_ref::<ElectMasterEvent>()
                    .ok_or_else(|| event_payload_mismatch(EventType::ElectMaster, "ElectMasterEvent"))?;
                self.handle_elect_master(event);
            }
            EventType::CleanBrokerData => {
                let event = event
                    .as_any()
                    .downcast_ref::<CleanBrokerDataEvent>()
                    .ok_or_else(|| event_payload_mismatch(EventType::CleanBrokerData, "CleanBrokerDataEvent"))?;
                self.handle_clean_broker_data(event);
            }
            EventType::UpdateBrokerAddress => {
                let event = event
                    .as_any()
                    .downcast_ref::<UpdateBrokerAddressEvent>()
                    .ok_or_else(|| {
                        event_payload_mismatch(EventType::UpdateBrokerAddress, "UpdateBrokerAddressEvent")
                    })?;
                self.handle_update_broker_address(event);
            }
            EventType::ReadEvent => {}
        }
        Ok(())
    }

    /// Serialize the state machine to bytes
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let state = SerializedState {
            replica_info_table: self
                .replica_info_table
                .iter()
                .map(|entry| (entry.key().to_string(), entry.value().clone()))
                .collect(),
            sync_state_set_info_table: self
                .sync_state_set_info_table
                .iter()
                .map(|entry| (entry.key().to_string(), entry.value().clone()))
                .collect(),
            broker_live_table: self
                .broker_live_table
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect(),
        };

        serde_json::to_vec(&state).map_err(|e| ControllerError::serialization_source("serialize sync state data", e))
    }

    /// Deserialize the state machine from bytes
    pub fn deserialize_from(&self, data: &[u8]) -> Result<()> {
        let state: SerializedState = serde_json::from_slice(data)
            .map_err(|e| ControllerError::serialization_source("deserialize sync state data", e))?;

        self.replica_info_table.clear();
        self.sync_state_set_info_table.clear();
        self.broker_live_table.clear();

        for (key, value) in state.replica_info_table {
            self.replica_info_table.insert(CheetahString::from_string(key), value);
        }

        for (key, value) in state.sync_state_set_info_table {
            self.sync_state_set_info_table
                .insert(CheetahString::from_string(key), value);
        }

        for (key, value) in state.broker_live_table {
            self.broker_live_table.insert(key, value);
        }

        Ok(())
    }

    // ==================== Private Helper Methods ====================

    /// Check if broker exists in both tables
    fn is_contains_broker(&self, broker_name: &str) -> bool {
        self.replica_info_table.contains_key(broker_name) && self.sync_state_set_info_table.contains_key(broker_name)
    }

    /// Build broker member group
    fn build_broker_member_group(&self, broker_replica_info: &BrokerReplicaInfo) -> BrokerMemberGroup {
        let broker_addrs: HashMap<u64, CheetahString> = broker_replica_info
            .get_broker_id_table()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect();

        BrokerMemberGroup {
            cluster: CheetahString::from_string(broker_replica_info.cluster_name().to_string()),
            broker_name: CheetahString::from_string(broker_replica_info.broker_name().to_string()),
            broker_addrs,
        }
    }

    /// Parse broker ids from string
    fn parse_broker_ids(&self, ids_str: &str) -> Result<HashSet<u64>> {
        ids_str
            .split(';')
            .map(|s| {
                s.parse::<u64>()
                    .map_err(|e| ControllerError::invalid_request_source("deserialize broker replica info", e))
            })
            .collect()
    }

    // ==================== Event Handlers ====================

    fn handle_alter_sync_state_set(&self, event: &AlterSyncStateSetEvent) {
        let broker_name = event.broker_name();
        if let Some(mut sync_state_info) = self.sync_state_set_info_table.get_mut(broker_name) {
            sync_state_info.update_sync_state_set_info(event.new_sync_state_set());
        }
    }

    fn handle_apply_broker_id(&self, event: &ApplyBrokerIdEvent) {
        let broker_name = event.broker_name();

        if let Some(broker_replica_info) = self.replica_info_table.get_mut(broker_name) {
            if !broker_replica_info.is_broker_exist(event.new_broker_id()) {
                broker_replica_info.add_broker(
                    event.new_broker_id(),
                    event.broker_address(),
                    event.register_check_code(),
                );
            }
        } else {
            // First time to register in this broker set
            let broker_replica_info = BrokerReplicaInfo::new(event.cluster_name(), event.broker_name());
            broker_replica_info.add_broker(
                event.new_broker_id(),
                event.broker_address(),
                event.register_check_code(),
            );
            self.replica_info_table.insert(
                CheetahString::from_string(event.broker_name().to_string()),
                broker_replica_info,
            );

            let sync_state_info = SyncStateInfo::new(event.cluster_name(), event.broker_name());
            self.sync_state_set_info_table.insert(
                CheetahString::from_string(event.broker_name().to_string()),
                sync_state_info,
            );
        }
    }

    fn handle_update_broker_address(&self, event: &UpdateBrokerAddressEvent) {
        let broker_name = event.broker_name();
        if let Some(broker_replica_info) = self.replica_info_table.get_mut(broker_name) {
            broker_replica_info.update_broker_address(event.broker_id(), event.broker_address());
        }
    }

    fn handle_elect_master(&self, event: &ElectMasterEvent) {
        let broker_name = event.broker_name();

        if !self.is_contains_broker(broker_name) {
            error!(
                "Receive an ElectMasterEvent which contains the un-registered broker, event = {:?}",
                event
            );
            return;
        }

        if let Some(mut sync_state_info) = self.sync_state_set_info_table.get_mut(broker_name) {
            if event.new_master_elected() {
                // Record new master
                if let Some(new_master) = event.new_master_broker_id() {
                    sync_state_info.update_master_info(new_master);

                    // Record new sync state set list
                    let mut new_sync_state_set = HashSet::new();
                    new_sync_state_set.insert(new_master);
                    sync_state_info.update_sync_state_set_info(&new_sync_state_set);
                }
            } else {
                // If new master was not elected, which means old master was shutdown
                // So we should delete old master, but retain sync state set list.
                sync_state_info.update_master_info_to_none();
            }
        }
    }

    fn handle_clean_broker_data(&self, event: &CleanBrokerDataEvent) {
        let broker_name = event.broker_name();

        if let Some(broker_id_set_to_clean) = event.broker_id_set_to_clean() {
            if broker_id_set_to_clean.is_empty() {
                self.replica_info_table.remove(broker_name);
                self.sync_state_set_info_table.remove(broker_name);
                return;
            }

            if !self.is_contains_broker(broker_name) {
                return;
            }

            if let Some(broker_replica_info) = self.replica_info_table.get_mut(broker_name) {
                for &broker_id in broker_id_set_to_clean {
                    broker_replica_info.remove_broker_id(broker_id);
                }

                if broker_replica_info.get_all_broker().is_empty() {
                    drop(broker_replica_info);
                    self.replica_info_table.remove(broker_name);
                }
            }

            if let Some(mut sync_state_info) = self.sync_state_set_info_table.get_mut(broker_name) {
                for &broker_id in broker_id_set_to_clean {
                    sync_state_info.remove_from_sync_state(broker_id);
                }

                if sync_state_info.sync_state_set().is_empty() {
                    drop(sync_state_info);
                    self.sync_state_set_info_table.remove(broker_name);
                }
            }
        } else {
            self.replica_info_table.remove(broker_name);
            self.sync_state_set_info_table.remove(broker_name);
        }
    }
}

fn compare_live_info(left: &BrokerLiveInfoSnapshot, right: &BrokerLiveInfoSnapshot) -> Ordering {
    match right.epoch.cmp(&left.epoch) {
        Ordering::Equal => match right.max_offset.cmp(&left.max_offset) {
            Ordering::Equal => left
                .election_priority
                .unwrap_or(i32::MAX)
                .cmp(&right.election_priority.unwrap_or(i32::MAX)),
            other => other,
        },
        other => other,
    }
}

impl BrokerValidPredicate for ReplicasInfoManager {
    fn check(&self, cluster_name: &str, broker_name: &str, broker_id: Option<i64>) -> bool {
        broker_id.is_some_and(|broker_id| self.is_broker_active(cluster_name, broker_name, broker_id))
    }
}

impl ReplicasInfoManager {
    fn try_elect_from_live_table(
        &self,
        cluster_name: &str,
        broker_name: &str,
        brokers: &HashSet<i64>,
        old_master: Option<i64>,
        prefer_broker_id: Option<i64>,
    ) -> Option<i64> {
        let valid_brokers: HashSet<i64> = brokers
            .iter()
            .copied()
            .filter(|broker_id| self.is_broker_active(cluster_name, broker_name, *broker_id))
            .collect();

        if valid_brokers.is_empty() {
            return None;
        }

        if let Some(old_master_id) = old_master {
            if valid_brokers.contains(&old_master_id)
                && (prefer_broker_id.is_none() || prefer_broker_id == Some(old_master_id))
            {
                return Some(old_master_id);
            }
        }

        if let Some(preferred_id) = prefer_broker_id {
            return valid_brokers.contains(&preferred_id).then_some(preferred_id);
        }

        let mut broker_infos: Vec<BrokerLiveInfoSnapshot> = valid_brokers
            .iter()
            .filter_map(|broker_id| self.get_broker_live_info(cluster_name, broker_name, *broker_id))
            .collect();
        broker_infos.sort_by(compare_live_info);

        broker_infos
            .first()
            .map(|broker| broker.broker_id as i64)
            .or_else(|| valid_brokers.iter().min().copied())
    }
}

fn event_payload_mismatch(event_type: EventType, expected: &'static str) -> ControllerError {
    ControllerError::InvalidRequest(format!(
        "event type {event_type} does not contain the expected {expected} payload"
    ))
}

impl ElectPolicy for ReplicasInfoManager {
    fn elect(
        &self,
        cluster_name: &str,
        broker_name: &str,
        sync_state_brokers: &HashSet<i64>,
        all_replica_brokers: &HashSet<i64>,
        old_master: Option<i64>,
        broker_id: Option<i64>,
    ) -> Option<i64> {
        self.try_elect_from_live_table(cluster_name, broker_name, sync_state_brokers, old_master, broker_id)
            .or_else(|| {
                self.try_elect_from_live_table(cluster_name, broker_name, all_replica_brokers, old_master, broker_id)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MismatchedEvent;

    impl EventMessage for MismatchedEvent {
        fn get_event_type(&self) -> EventType {
            EventType::ApplyBrokerId
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    #[test]
    fn test_replicas_info_manager_creation() {
        let config = ControllerConfigReader::new(ControllerConfig::new_node(1, "127.0.0.1:9876".parse().unwrap()));
        let manager = ReplicasInfoManager::new(config);
        assert_eq!(manager.replica_info_table.len(), 0);
        assert_eq!(manager.sync_state_set_info_table.len(), 0);

        let error = manager
            .try_apply_event(&MismatchedEvent)
            .expect_err("mismatched event payload must not be ignored");

        assert!(matches!(
            error,
            ControllerError::InvalidRequest(message)
                if message.contains("ApplyBrokerId") && message.contains("ApplyBrokerIdEvent")
        ));
    }
}
