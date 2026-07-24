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
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_common::FileUtils;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_runtime::MetadataDeadline;
use rocketmq_store::base::message_store::MessageStore;
use tracing::error;
use tracing::info;
use tracing::warn;

use super::BrokerControllerRuntime;
use crate::controller::replicas_manager::ControllerBrokerIdAction;
use crate::controller::replicas_manager::ControllerBrokerIdPersistencePlan;
use crate::controller::replicas_manager::ControllerRegisterFollowup;
use crate::controller::replicas_manager::ControllerReplicaInfoFollowup;
use crate::controller::replicas_manager::ControllerReplicaSyncFollowup;
use crate::controller::replicas_manager::ReplicasManager;

impl<MS: MessageStore> BrokerControllerRuntime<MS> {
    pub(crate) async fn run_heartbeat_cycle(&self) {
        if self.shutdown.load(Ordering::Acquire) {
            return;
        }
        let broker_config = self.config.broker_snapshot();
        if self.role_state.is_isolated() {
            if broker_config.enable_controller_mode {
                self.bootstrap_controller_mode().await;
            }
            info!("Skip send heartbeat for broker is isolated");
            return;
        }
        if broker_config.enable_controller_mode {
            self.refresh_controller_leader().await;
        }
        self.send_heartbeat().await;
    }

    pub(crate) async fn bootstrap_controller_mode(&self) {
        let Some(controller_leader) = self.discover_controller_leader().await else {
            warn!(
                "Skip controller mode bootstrap because controller leader is unavailable, broker={}",
                self.config.broker_snapshot().broker_identity.get_canonical_name()
            );
            return;
        };

        let broker_config = self.config.broker_snapshot();
        if let Some(Err(error)) = self.controller.with_replicas_mut(|replicas_manager| {
            replicas_manager.set_controller_leader_address(controller_leader.clone());
            replicas_manager.validate_registration_state(&broker_config)
        }) {
            warn!("Controller mode registration state is invalid: {}", error);
            return;
        }

        let controller_broker_id = match self.ensure_controller_broker_id(&controller_leader).await {
            Ok(controller_broker_id) => controller_broker_id,
            Err(error) => {
                warn!("Ensure controller broker id failed: {}", error);
                return;
            }
        };

        if self.replicas_snapshot().is_none() {
            warn!("ReplicasManager is not initialized for controller mode bootstrap");
            return;
        }

        let cluster_name = broker_config.broker_identity.broker_cluster_name.clone();
        let broker_name = broker_config.broker_identity.broker_name.clone();
        match self
            .broker_outer_api
            .register_broker_to_controller(
                cluster_name.clone(),
                broker_name.clone(),
                controller_broker_id as i64,
                self.broker_addr.clone(),
                &controller_leader,
            )
            .await
        {
            Ok((register_header, sync_state_set)) => {
                self.controller.with_replicas_mut(ReplicasManager::mark_registered);
                let sync_state_set = sync_state_set.unwrap_or_default();
                let register_followup = self
                    .replicas_snapshot()
                    .map(|replicas_manager| {
                        replicas_manager.register_followup(
                            register_header.master_broker_id.and_then(|id| u64::try_from(id).ok()),
                            register_header.master_epoch,
                        )
                    })
                    .unwrap_or(ControllerRegisterFollowup::HeartbeatThenQueryReplicaInfo);
                if register_followup == ControllerRegisterFollowup::ApplyRoleChange {
                    if let Err(error) = self
                        .apply_controller_role_change(
                            Some(controller_leader.clone()),
                            register_header.master_broker_id.and_then(|id| u64::try_from(id).ok()),
                            register_header.master_address,
                            register_header.master_epoch,
                            register_header.sync_state_set_epoch,
                            sync_state_set,
                        )
                        .await
                    {
                        warn!("Apply controller register result failed: {}", error);
                    }
                    return;
                }
            }
            Err(error) => {
                warn!("Register broker to controller failed: {}", error);
                return;
            }
        }

        if let Err(error) = self.send_heartbeat_to_controller_leader(&controller_leader).await {
            warn!("Send bootstrap heartbeat to controller failed: {}", error);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;

        if let Ok((replica_info_header, sync_state_set)) = self
            .broker_outer_api
            .get_replica_info(&controller_leader, broker_name.clone())
            .await
        {
            let replica_followup = self
                .replicas_snapshot()
                .map(|replicas_manager| {
                    replicas_manager.replica_info_followup(
                        replica_info_header
                            .master_broker_id
                            .and_then(|id| u64::try_from(id).ok()),
                        replica_info_header.master_epoch,
                    )
                })
                .unwrap_or(ControllerReplicaInfoFollowup::ElectMaster);
            if replica_followup == ControllerReplicaInfoFollowup::ApplyRoleChange
                && self
                    .apply_controller_replica_info(
                        controller_leader.clone(),
                        replica_info_header
                            .master_broker_id
                            .and_then(|id| u64::try_from(id).ok()),
                        replica_info_header.master_address.map(CheetahString::from_string),
                        replica_info_header.master_epoch,
                        Some(sync_state_set.get_sync_state_set_epoch()),
                        sync_state_set.get_sync_state_set().cloned().unwrap_or_default(),
                    )
                    .await
            {
                return;
            }
        }

        match self
            .broker_outer_api
            .broker_elect(
                &controller_leader,
                cluster_name,
                broker_name.clone(),
                controller_broker_id as i64,
            )
            .await
        {
            Ok((elect_header, sync_state_set)) => {
                if let Err(error) = self
                    .apply_controller_role_change(
                        Some(controller_leader),
                        elect_header.master_broker_id.and_then(|id| u64::try_from(id).ok()),
                        elect_header.master_address,
                        elect_header.master_epoch,
                        elect_header.sync_state_set_epoch,
                        sync_state_set,
                    )
                    .await
                {
                    warn!("Apply controller elect-master result failed: {}", error);
                }
            }
            Err(error) => {
                if let Ok((replica_info_header, sync_state_set)) = self
                    .broker_outer_api
                    .get_replica_info(&controller_leader, broker_name)
                    .await
                {
                    if self
                        .apply_controller_replica_info(
                            controller_leader,
                            replica_info_header
                                .master_broker_id
                                .and_then(|id| u64::try_from(id).ok()),
                            replica_info_header.master_address.map(CheetahString::from_string),
                            replica_info_header.master_epoch,
                            Some(sync_state_set.get_sync_state_set_epoch()),
                            sync_state_set.get_sync_state_set().cloned().unwrap_or_default(),
                        )
                        .await
                    {
                        return;
                    }
                }
                warn!("Elect master during controller mode bootstrap failed: {}", error);
            }
        }
    }

    pub(crate) async fn apply_controller_replica_info(
        &self,
        controller_leader: CheetahString,
        master_broker_id: Option<u64>,
        master_address: Option<CheetahString>,
        master_epoch: Option<i32>,
        sync_state_set_epoch: Option<i32>,
        sync_state_set: HashSet<i64>,
    ) -> bool {
        if master_broker_id.is_none() || master_epoch.is_none() {
            return false;
        }

        if let Err(error) = self
            .apply_controller_role_change(
                Some(controller_leader),
                master_broker_id,
                master_address,
                master_epoch,
                sync_state_set_epoch,
                sync_state_set,
            )
            .await
        {
            warn!("Apply controller replica info failed: {}", error);
            return false;
        }

        true
    }

    async fn persist_broker_id_snapshot(
        &self,
        resource: &'static str,
        target: PathBuf,
        content: Vec<u8>,
    ) -> RocketMQResult<()> {
        let metadata_io = self.metadata_io.as_ref().ok_or_else(|| {
            RocketMQError::illegal_argument("broker-id metadata actor is unavailable in the production control plane")
        })?;
        metadata_io
            .submit_next_durable(
                resource,
                target,
                content,
                MetadataDeadline::after(Duration::from_secs(10)),
            )
            .await
            .map_err(FileUtils::metadata_io_error)?;
        Ok(())
    }

    async fn remove_broker_id_temporary(&self, target: PathBuf) -> RocketMQResult<()> {
        if let Some(blocking) = self.blocking.as_ref() {
            return blocking
                .spawn_io("broker.identity.remove-pending", move || remove_file_if_exists(&target))
                .await
                .map_err(|error| RocketMQError::IO(std::io::Error::other(error)))?
                .map_err(RocketMQError::IO);
        }
        remove_file_if_exists(&target).map_err(RocketMQError::IO)
    }

    pub(crate) async fn ensure_controller_broker_id(
        &self,
        controller_leader: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<u64> {
        let _operation_guard = self.controller.lock_operation().await;
        let broker_config = self.config.broker_snapshot();
        let cluster_name = broker_config.broker_identity.broker_cluster_name.clone();
        let broker_name = broker_config.broker_identity.broker_name.clone();
        let next_broker_id = if self
            .replicas_snapshot()
            .is_some_and(|replicas_manager| replicas_manager.needs_broker_id_application())
            && self
                .replicas_snapshot()
                .is_some_and(|replicas_manager| replicas_manager.pending_registration().is_none())
        {
            let next_broker_id_response = self
                .broker_outer_api
                .get_next_broker_id(cluster_name.clone(), broker_name.clone(), controller_leader)
                .await?;
            Some(next_broker_id_response.next_broker_id.ok_or_else(|| {
                rocketmq_error::RocketMQError::illegal_argument(
                    "controller get_next_broker_id returned empty next_broker_id",
                )
            })?)
        } else {
            None
        };
        let (action, pending_temporary) = if self.metadata_io.is_some() {
            let plan = self
                .replicas_snapshot()
                .ok_or_else(|| {
                    RocketMQError::illegal_argument("replicas manager missing while planning controller broker id")
                })?
                .plan_controller_broker_id_persistence(&broker_config, next_broker_id)?;
            match plan {
                ControllerBrokerIdPersistencePlan::UseCurrent(broker_id) => return Ok(broker_id),
                ControllerBrokerIdPersistencePlan::ReusePending {
                    broker_id,
                    register_check_code,
                    temporary,
                } => (
                    ControllerBrokerIdAction::ApplyBrokerId {
                        broker_id,
                        register_check_code,
                    },
                    Some(temporary),
                ),
                ControllerBrokerIdPersistencePlan::PersistPending {
                    target,
                    content,
                    record,
                } => {
                    self.persist_broker_id_snapshot("broker.identity.pending", target.clone(), content)
                        .await?;
                    let action = self
                        .controller
                        .with_replicas_mut(|replicas_manager| replicas_manager.publish_pending_broker_id(record))
                        .ok_or_else(|| {
                            RocketMQError::illegal_argument(
                                "replicas manager missing while publishing durable controller broker id",
                            )
                        })??;
                    (action, Some(target))
                }
            }
        } else {
            let action = self
                .controller
                .with_replicas_mut(|replicas_manager| {
                    replicas_manager.prepare_controller_broker_id_action(&broker_config, next_broker_id)
                })
                .ok_or_else(|| {
                    RocketMQError::illegal_argument("replicas manager missing while preparing controller broker id")
                })??;
            (action, None)
        };

        let (broker_id, register_check_code) = match action {
            ControllerBrokerIdAction::UseCurrent(broker_id) => return Ok(broker_id),
            ControllerBrokerIdAction::ApplyBrokerId {
                broker_id,
                register_check_code,
            } => (broker_id, register_check_code),
        };

        if let Err(error) = self
            .broker_outer_api
            .apply_broker_id(
                cluster_name,
                broker_name,
                broker_id as i64,
                register_check_code,
                controller_leader,
            )
            .await
        {
            if let Some(temporary) = pending_temporary {
                match self.remove_broker_id_temporary(temporary).await {
                    Ok(()) => {
                        self.controller
                            .with_replicas_mut(ReplicasManager::clear_temp_metadata_state);
                    }
                    Err(cleanup_error) => {
                        warn!(%cleanup_error, "Failed to remove pending broker-id metadata after controller rejection");
                    }
                }
            } else {
                self.controller
                    .with_replicas_mut(|replicas_manager| replicas_manager.clear_temp_metadata());
            }
            return Err(error);
        }

        if self.metadata_io.is_some() {
            let snapshot = self
                .replicas_snapshot()
                .ok_or_else(|| {
                    RocketMQError::illegal_argument("replicas manager missing while planning broker id commit")
                })?
                .plan_controller_broker_id_commit(&broker_config)?;
            let Some(snapshot) = snapshot else {
                return Ok(broker_id);
            };
            self.persist_broker_id_snapshot("broker.identity.committed", snapshot.target, snapshot.content)
                .await?;
            self.remove_broker_id_temporary(snapshot.temporary).await?;
            return self
                .controller
                .with_replicas_mut(|replicas_manager| replicas_manager.publish_committed_broker_id(snapshot.record))
                .ok_or_else(|| {
                    RocketMQError::illegal_argument("replicas manager missing while publishing broker id commit")
                })?;
        }

        self.controller
            .with_replicas_mut(|replicas_manager| {
                replicas_manager.complete_controller_broker_id_application(&broker_config)
            })
            .ok_or_else(|| {
                RocketMQError::illegal_argument("replicas manager missing while committing controller broker id")
            })?
    }

    pub(crate) async fn discover_controller_leader(&self) -> Option<CheetahString> {
        let targets = self
            .replicas_snapshot()
            .map(|replicas_manager| replicas_manager.heartbeat_targets())
            .unwrap_or_default();
        let mut first_reachable = None;
        for address in targets {
            match self.broker_outer_api.get_controller_metadata(&address).await {
                Ok(metadata) => {
                    if let Some(controller_leader_address) = metadata.controller_leader_address {
                        return Some(controller_leader_address);
                    }
                    if metadata.is_leader == Some(true) {
                        return Some(address);
                    }
                    first_reachable.get_or_insert(address);
                }
                Err(error) => {
                    warn!("Discover controller leader failed via {}: {}", address, error);
                }
            }
        }
        first_reachable
    }

    pub(crate) async fn refresh_controller_leader(&self) -> Option<CheetahString> {
        let controller_leader = self.discover_controller_leader().await?;
        self.controller.with_replicas_mut(|replicas_manager| {
            replicas_manager.set_controller_leader_address(controller_leader.clone());
        });
        Some(controller_leader)
    }

    pub(crate) async fn sync_controller_replica_info(&self) {
        let broker_config = self.config.broker_snapshot();
        if !broker_config.enable_controller_mode || self.role_state.is_isolated() {
            return;
        }

        let controller_leader = if let Some(controller_leader) = self
            .replicas_snapshot()
            .and_then(|replicas_manager| replicas_manager.controller_leader_address().cloned())
        {
            controller_leader
        } else if let Some(controller_leader) = self.refresh_controller_leader().await {
            controller_leader
        } else {
            return;
        };

        let broker_name = broker_config.broker_identity.broker_name.clone();
        match self
            .fetch_controller_replica_info(&controller_leader, broker_name)
            .await
        {
            Ok((leader, response_header, sync_state_set_body)) => {
                let sync_state_set_epoch = sync_state_set_body.get_sync_state_set_epoch();
                let sync_state_set = sync_state_set_body.get_sync_state_set().cloned().unwrap_or_default();
                let sync_followup = self
                    .replicas_snapshot()
                    .map(|replicas_manager| {
                        replicas_manager.replica_sync_followup(
                            response_header.master_broker_id.and_then(|id| u64::try_from(id).ok()),
                            response_header.master_epoch,
                        )
                    })
                    .unwrap_or(ControllerReplicaSyncFollowup::Bootstrap);
                if sync_followup == ControllerReplicaSyncFollowup::ApplyRoleChange {
                    if let Err(error) = self
                        .apply_controller_role_change(
                            Some(leader),
                            response_header.master_broker_id.and_then(|id| u64::try_from(id).ok()),
                            response_header.master_address.map(CheetahString::from),
                            response_header.master_epoch,
                            Some(sync_state_set_epoch),
                            sync_state_set,
                        )
                        .await
                    {
                        warn!("Apply controller replica info failed: {}", error);
                    }
                } else {
                    self.bootstrap_controller_mode().await;
                }
            }
            Err(rocketmq_error::RocketMQError::BrokerOperationFailed { code, .. })
                if self
                    .replicas_snapshot()
                    .map(|replicas_manager| {
                        replicas_manager.replica_sync_error_followup(Some(code))
                            == ControllerReplicaSyncFollowup::Bootstrap
                    })
                    .unwrap_or(true) =>
            {
                self.bootstrap_controller_mode().await;
            }
            Err(error) => {
                warn!("Sync controller replica info failed: {}", error);
            }
        }
    }

    async fn fetch_controller_replica_info(
        &self,
        controller_leader: &CheetahString,
        broker_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<(
        CheetahString,
        rocketmq_remoting::protocol::header::controller::get_replica_info_response_header::GetReplicaInfoResponseHeader,
        rocketmq_remoting::protocol::body::sync_state_set_body::SyncStateSet,
    )> {
        match self
            .broker_outer_api
            .get_replica_info(controller_leader, broker_name.clone())
            .await
        {
            Ok(result) => Ok((controller_leader.clone(), result.0, result.1)),
            Err(error) => {
                let Some(refreshed_controller_leader) = self.refresh_controller_leader().await else {
                    return Err(error);
                };

                if refreshed_controller_leader == *controller_leader {
                    return Err(error);
                }

                self.broker_outer_api
                    .get_replica_info(&refreshed_controller_leader, broker_name)
                    .await
                    .map(|(response_header, sync_state_set)| {
                        (refreshed_controller_leader, response_header, sync_state_set)
                    })
            }
        }
    }

    pub(crate) async fn sync_broker_member_group(&self) {
        let broker_config = self.config.broker_snapshot();
        let broker_cluster_name = &broker_config.broker_identity.broker_cluster_name;
        let broker_name = &broker_config.broker_identity.broker_name;
        let broker_member_group = self
            .broker_outer_api
            .sync_broker_member_group(
                broker_cluster_name,
                broker_name,
                broker_config.compatible_with_old_name_srv,
            )
            .await;

        let broker_member_group = match broker_member_group {
            Ok(Some(broker_member_group)) if !broker_member_group.broker_addrs.is_empty() => broker_member_group,
            Ok(_) => {
                warn!(
                    "Couldn't find any broker member from namesrv in {}/{}",
                    broker_cluster_name, broker_name
                );
                return;
            }
            Err(error) => {
                error!("syncBrokerMemberGroup from namesrv failed, error={}", error);
                return;
            }
        };

        fn alive_broker_count(broker_addr_table: &HashMap<u64, CheetahString>, broker_id: u64) -> usize {
            broker_addr_table.len() + usize::from(!broker_addr_table.contains_key(&broker_id))
        }

        if let Err(error) = self.store.set_alive_replica_num_in_group(alive_broker_count(
            &broker_member_group.broker_addrs,
            broker_config.broker_identity.broker_id,
        ) as i32)
        {
            warn!(
                ?error,
                "Skip broker-member synchronization because Store is unavailable"
            );
            return;
        }
        if !self.role_state.is_isolated() {
            let min_broker_id = broker_member_group.minimum_broker_id();
            let min_broker_addr = broker_member_group.broker_addrs.get(&min_broker_id).cloned();
            self.update_min_broker(min_broker_id, min_broker_addr).await;
        }
    }

    pub(crate) async fn send_heartbeat(&self) {
        if self.shutdown.load(Ordering::Acquire) {
            return;
        }

        let Some(replicas_manager) = self.replicas_snapshot() else {
            return;
        };
        let heartbeat_state = replicas_manager.controller_heartbeat_state();
        let broker_config = self.config.broker_snapshot();
        let controller_targets = heartbeat_state.controller_targets;
        if controller_targets.is_empty() {
            warn!(
                "Skip controller heartbeat because no controller address is configured, broker={}",
                broker_config.broker_identity.get_canonical_name()
            );
            return;
        }

        if self.shutdown.load(Ordering::Acquire) {
            return;
        }

        let cluster_name = broker_config.broker_identity.broker_cluster_name.clone();
        let broker_name = broker_config.broker_identity.broker_name.clone();
        let send_heartbeat_timeout_millis = broker_config.send_heartbeat_timeout_millis;
        let controller_heartbeat_timeout_millis = broker_config.controller_heartbeat_timeout_mills;
        let broker_election_priority = broker_config.broker_election_priority;
        let broker_id = heartbeat_state.broker_id;
        let epoch = heartbeat_state.epoch;
        let (max_offset, confirm_offset) = self.store.controller_heartbeat_offsets();

        let futures = controller_targets.into_iter().map(|controller_address| {
            let cluster_name = cluster_name.clone();
            let broker_addr = self.broker_addr.clone();
            let broker_name = broker_name.clone();
            async move {
                if self.shutdown.load(Ordering::Acquire) {
                    return;
                }
                self.broker_outer_api
                    .send_heartbeat_to_controller(
                        controller_address,
                        cluster_name,
                        broker_addr,
                        broker_name,
                        broker_id,
                        send_heartbeat_timeout_millis,
                        epoch,
                        max_offset,
                        confirm_offset,
                        Some(controller_heartbeat_timeout_millis),
                        Some(broker_election_priority),
                    )
                    .await;
            }
        });
        futures::future::join_all(futures).await;
    }

    pub(crate) async fn send_heartbeat_to_controller_leader(
        &self,
        controller_leader: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let heartbeat_state = self
            .replicas_snapshot()
            .map(|replicas_manager| replicas_manager.controller_heartbeat_state())
            .ok_or_else(|| {
                rocketmq_error::RocketMQError::illegal_argument(
                    "replicas manager missing while sending controller leader heartbeat",
                )
            })?;
        let broker_config = self.config.broker_snapshot();
        let (max_offset, confirm_offset) = self.store.controller_heartbeat_offsets();

        self.broker_outer_api
            .send_heartbeat_to_controller_sync(
                controller_leader,
                broker_config.broker_identity.broker_cluster_name.clone(),
                self.broker_addr.clone(),
                broker_config.broker_identity.broker_name.clone(),
                heartbeat_state.broker_id,
                broker_config.send_heartbeat_timeout_millis,
                heartbeat_state.epoch,
                max_offset,
                confirm_offset,
                Some(broker_config.controller_heartbeat_timeout_mills),
                Some(broker_config.broker_election_priority),
            )
            .await
    }
}

fn remove_file_if_exists(target: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(target) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn controller_bootstrap_boundary_does_not_retain_the_broker_root() {
        let source = include_str!("bootstrap.rs");

        assert!(!source.contains(&["Arc", "Mut"].concat()));
        assert!(!source.contains(&["BrokerRuntime", "Inner"].concat()));
    }
}
