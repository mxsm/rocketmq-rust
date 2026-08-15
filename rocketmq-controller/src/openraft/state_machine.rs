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

//! Raft state machine implementation backed by `ReplicasInfoManager`.

use std::collections::HashSet;
use std::sync::Arc;

use arc_swap::ArcSwap;
use openraft::storage::RaftStateMachine;
use openraft::EntryPayload;
use openraft::OptionalSend;
use openraft::RaftSnapshotBuilder;
use rocketmq_model::utils::crc32_utils::crc32;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::controller::get_next_broker_id_response_header::GetNextBrokerIdResponseHeader;
use rocketmq_protocol::protocol::header::controller::get_replica_info_response_header::GetReplicaInfoResponseHeader;
use rocketmq_protocol::protocol::RemotingSerializable;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

use super::persistence::decode_v1;
use super::persistence::encode_v1;
use super::persistence::RaftRecordKey;
use super::persistence::RaftStateRepository;
use crate::config::ControllerConfigReader;
use crate::event::controller_result::ControllerResult;
use crate::manager::replicas_info_manager::ReplicasInfoManager;
use crate::openraft::SNAPSHOT_MAX_BYTES;
use crate::storage::SharedStorageBackend;
use crate::typ::ControllerRequest;
use crate::typ::ControllerResponse;
use crate::typ::ControllerResponseHeader;
use crate::typ::LogId;
use crate::typ::Snapshot;
use crate::typ::SnapshotMeta;
use crate::typ::StoredMembership;
use crate::typ::TypeConfig;

const SNAPSHOT_FORMAT_VERSION: u16 = 1;

fn storage_error(error: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(error.to_string())
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotData {
    pub replicas_info_manager_state: Vec<u8>,
    pub last_applied: Option<LogId>,
    pub last_membership: Option<StoredMembership>,
    pub snapshot_id: String,
    pub format_version: u16,
    pub checksum: u32,
}

impl SnapshotData {
    fn new(
        replicas_info_manager_state: Vec<u8>,
        last_applied: Option<LogId>,
        last_membership: StoredMembership,
    ) -> Result<Self, std::io::Error> {
        let mut data = Self {
            replicas_info_manager_state,
            last_applied,
            last_membership: Some(last_membership),
            snapshot_id: format!("snapshot-{}", last_applied.map_or(0, |log_id| log_id.index)),
            format_version: SNAPSHOT_FORMAT_VERSION,
            checksum: 0,
        };
        data.checksum = data.calculate_checksum()?;
        Ok(data)
    }

    fn validate(&self) -> Result<(), std::io::Error> {
        if self.format_version != SNAPSHOT_FORMAT_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unsupported controller snapshot format version {}", self.format_version),
            ));
        }
        if self.last_membership.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Controller snapshot is missing its membership state",
            ));
        }
        let actual_checksum = self.calculate_checksum()?;
        if actual_checksum != self.checksum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Controller snapshot checksum mismatch: expected {}, calculated {}",
                    self.checksum, actual_checksum
                ),
            ));
        }
        Ok(())
    }

    fn calculate_checksum(&self) -> Result<u32, std::io::Error> {
        encode_v1(
            RaftRecordKey::SnapshotData,
            &(
                self.format_version,
                &self.replicas_info_manager_state,
                self.last_applied,
                &self.last_membership,
                &self.snapshot_id,
            ),
        )
        .map(|bytes| crc32(&bytes))
    }
}

#[derive(Clone)]
struct CurrentSnapshot {
    meta: SnapshotMeta,
    data: Vec<u8>,
}

/// Read-only view of the locally applied controller state.
///
/// The view does not perform a Raft read barrier itself. Callers serving
/// business traffic must first complete
/// [`crate::openraft::RaftNodeManager::ensure_linearizable_read`]. It intentionally exposes no
/// state mutation API.
#[derive(Clone)]
pub struct StateMachineReadView {
    inner: Arc<ReplicasInfoManager>,
}

impl StateMachineReadView {
    /// Returns the next broker ID calculated from this immutable state revision.
    pub fn get_next_broker_id(
        &self,
        cluster_name: &str,
        broker_name: &str,
    ) -> ControllerResult<GetNextBrokerIdResponseHeader> {
        self.inner.get_next_broker_id(cluster_name, broker_name)
    }

    /// Returns replica metadata for a broker from this state revision.
    pub fn get_replica_info(&self, broker_name: &str) -> ControllerResult<GetReplicaInfoResponseHeader> {
        self.inner.get_replica_info(broker_name)
    }

    /// Returns all broker IDs known for a broker name.
    pub fn broker_ids(&self, broker_name: &str) -> HashSet<u64> {
        self.inner.broker_ids(broker_name)
    }

    /// Returns the cluster owning a broker name.
    pub fn cluster_name(&self, broker_name: &str) -> Option<String> {
        self.inner.cluster_name(broker_name)
    }

    /// Tests broker liveness at an explicit timestamp.
    pub fn is_broker_active_at(
        &self,
        cluster_name: &str,
        broker_name: &str,
        broker_id: i64,
        check_time_millis: u64,
    ) -> bool {
        self.inner
            .is_broker_active_at(cluster_name, broker_name, broker_id, check_time_millis)
    }
}

#[derive(Clone)]
pub struct StateMachine {
    config: ControllerConfigReader,
    replicas_info_manager: Arc<ArcSwap<ReplicasInfoManager>>,
    last_applied: Arc<RwLock<Option<LogId>>>,
    last_membership: Arc<RwLock<StoredMembership>>,
    current_snapshot: Arc<RwLock<Option<CurrentSnapshot>>>,
    repository: Option<RaftStateRepository>,
    /// Serializes durable state transitions and snapshot capture/installation.
    state_lock: Arc<Mutex<()>>,
}

impl StateMachine {
    pub fn new(config: ControllerConfigReader) -> Self {
        Self {
            replicas_info_manager: Arc::new(ArcSwap::from_pointee(ReplicasInfoManager::new(config.clone()))),
            config,
            last_applied: Arc::new(RwLock::new(None)),
            last_membership: Arc::new(RwLock::new(StoredMembership::default())),
            current_snapshot: Arc::new(RwLock::new(None)),
            repository: None,
            state_lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn open(config: ControllerConfigReader, backend: SharedStorageBackend) -> Result<Self, std::io::Error> {
        let repository = RaftStateRepository::new(backend);
        let loaded = repository.load().await?;
        let replicas_info_manager = Arc::new(ReplicasInfoManager::new(config.clone()));
        if let Some(state) = loaded.replicas_info_manager_state {
            replicas_info_manager.deserialize_from(&state).map_err(storage_error)?;
        }
        let state_machine = Self {
            replicas_info_manager: Arc::new(ArcSwap::from(replicas_info_manager)),
            config,
            last_applied: Arc::new(RwLock::new(loaded.last_applied)),
            last_membership: Arc::new(RwLock::new(loaded.last_membership)),
            current_snapshot: Arc::new(RwLock::new(None)),
            repository: Some(repository),
            state_lock: Arc::new(Mutex::new(())),
        };

        if let Some(snapshot) = loaded.current_snapshot {
            let data = snapshot.data;
            let meta = snapshot.meta;
            let snapshot_data = validate_snapshot_bytes(&data)?;
            let snapshot_membership = snapshot_data.last_membership.unwrap_or_default();
            if snapshot_data.last_applied != meta.last_log_id
                || snapshot_membership != meta.last_membership
                || snapshot_data.snapshot_id != meta.snapshot_id
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Persisted controller snapshot metadata does not match its checksummed payload",
                ));
            }
            *state_machine.current_snapshot.write().await = Some(CurrentSnapshot { meta, data });
        }

        Ok(state_machine)
    }

    pub(crate) fn replicas_info_manager(&self) -> Arc<ReplicasInfoManager> {
        self.replicas_info_manager.load_full()
    }

    /// Returns a read-only local state view without performing a Raft read barrier.
    #[must_use]
    pub fn read_view(&self) -> StateMachineReadView {
        StateMachineReadView {
            inner: self.replicas_info_manager.load_full(),
        }
    }

    /// Returns whether durable state contains an applied Raft log entry.
    ///
    /// Bootstrap uses this storage-backed value instead of the asynchronously published
    /// OpenRaft metrics so a restarting node cannot be mistaken for a new cluster.
    pub(crate) async fn has_persisted_applied_state(&self) -> bool {
        self.last_applied.read().await.is_some()
    }

    fn response_from_result<T, F>(
        replicas_info_manager: &ReplicasInfoManager,
        result: ControllerResult<T>,
        map_header: F,
    ) -> ControllerResponse
    where
        F: FnOnce(T) -> ControllerResponseHeader,
    {
        let (events, header, body, response_code, remark) = result.into_parts();
        for event in events {
            if let Err(error) = replicas_info_manager.try_apply_event(event.as_ref()) {
                return ControllerResponse::new(ResponseCode::SystemError.into(), Some(error.to_string()), None, None);
            }
        }

        ControllerResponse::new(
            response_code.into(),
            remark.map(|value| value.to_string()),
            header.map(map_header),
            body.map(|bytes| bytes.to_vec()),
        )
    }

    fn response_from_result_without_header(
        replicas_info_manager: &ReplicasInfoManager,
        result: ControllerResult<()>,
    ) -> ControllerResponse {
        let (events, _header, body, response_code, remark) = result.into_parts();
        for event in events {
            if let Err(error) = replicas_info_manager.try_apply_event(event.as_ref()) {
                return ControllerResponse::new(ResponseCode::SystemError.into(), Some(error.to_string()), None, None);
            }
        }

        ControllerResponse::new(
            response_code.into(),
            remark.map(|value| value.to_string()),
            None,
            body.map(|bytes| bytes.to_vec()),
        )
    }

    fn apply_request_to(
        replicas_info_manager: &ReplicasInfoManager,
        request: &ControllerRequest,
    ) -> ControllerResponse {
        match request {
            ControllerRequest::ApplyBrokerId {
                cluster_name,
                broker_name,
                broker_address,
                applied_broker_id,
                register_check_code,
            } => {
                let result = replicas_info_manager.apply_broker_id(
                    cluster_name,
                    broker_name,
                    broker_address,
                    *applied_broker_id,
                    register_check_code,
                );
                Self::response_from_result(replicas_info_manager, result, ControllerResponseHeader::ApplyBrokerId)
            }
            ControllerRequest::RegisterBroker {
                cluster_name,
                broker_name,
                broker_address,
                broker_id,
                alive_broker_ids: _,
            } => {
                let result = replicas_info_manager.register_broker(
                    cluster_name,
                    broker_name,
                    broker_address,
                    *broker_id,
                    replicas_info_manager,
                );
                Self::response_from_result(replicas_info_manager, result, ControllerResponseHeader::RegisterBroker)
            }
            ControllerRequest::AlterSyncStateSet {
                cluster_name: _cluster_name,
                broker_name,
                master_broker_id,
                master_epoch,
                new_sync_state_set,
                sync_state_set_epoch,
                alive_broker_ids: _,
            } => {
                let result = replicas_info_manager.alter_sync_state_set(
                    broker_name,
                    *master_broker_id,
                    *master_epoch,
                    new_sync_state_set.clone(),
                    *sync_state_set_epoch,
                    replicas_info_manager,
                );
                Self::response_from_result(
                    replicas_info_manager,
                    result,
                    ControllerResponseHeader::AlterSyncStateSet,
                )
            }
            ControllerRequest::ElectMaster {
                cluster_name: _cluster_name,
                broker_name,
                broker_id,
                designate_elect,
                alive_broker_ids: _,
                live_broker_infos: _,
            } => {
                let result = replicas_info_manager.elect_master(
                    broker_name,
                    *broker_id,
                    *designate_elect,
                    replicas_info_manager,
                );
                Self::response_from_result(replicas_info_manager, result, ControllerResponseHeader::ElectMaster)
            }
            ControllerRequest::CleanBrokerData {
                cluster_name,
                broker_name,
                broker_controller_ids_to_clean,
                clean_living_broker,
                alive_broker_ids: _,
            } => {
                let result = replicas_info_manager.clean_broker_data(
                    cluster_name,
                    broker_name,
                    broker_controller_ids_to_clean.as_deref(),
                    *clean_living_broker,
                    replicas_info_manager,
                );
                Self::response_from_result_without_header(replicas_info_manager, result)
            }
            ControllerRequest::BrokerHeartbeat {
                broker_identity,
                broker_live_info,
                lease_grant_allowed,
            } => {
                replicas_info_manager.on_broker_heartbeat(broker_identity.clone(), broker_live_info.clone());
                let lease_body = replicas_info_manager
                    .grant_write_lease(broker_identity, broker_live_info, *lease_grant_allowed)
                    .and_then(|grant| grant.encode().ok());
                ControllerResponse::new(
                    rocketmq_protocol::code::response_code::ResponseCode::Success.into(),
                    Some(if lease_body.is_some() {
                        "Heartbeat committed; write lease granted".to_string()
                    } else {
                        "Heartbeat committed; no write lease for this authority".to_string()
                    }),
                    None,
                    lease_body,
                )
            }
            ControllerRequest::BrokerChannelClose { broker_identity } => {
                replicas_info_manager.on_broker_channel_close(broker_identity);
                ControllerResponse::success()
            }
            ControllerRequest::CheckNotActiveBroker { check_time_millis } => {
                let inactive_brokers = replicas_info_manager.check_not_active_broker(*check_time_millis);
                let body = serde_json::to_vec(&inactive_brokers).ok();
                ControllerResponse::new(
                    rocketmq_protocol::code::response_code::ResponseCode::Success.into(),
                    None,
                    None,
                    body,
                )
            }
        }
    }

    #[cfg(test)]
    fn apply_request(&self, request: &ControllerRequest) -> ControllerResponse {
        Self::apply_request_to(self.replicas_info_manager.load().as_ref(), request)
    }

    async fn build_snapshot_data(&self) -> Result<SnapshotData, std::io::Error> {
        let replicas_info_manager_state = self
            .replicas_info_manager
            .load()
            .serialize()
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let last_applied = *self.last_applied.read().await;
        let last_membership = self.last_membership.read().await.clone();

        SnapshotData::new(replicas_info_manager_state, last_applied, last_membership)
    }

    async fn persist_state_values(
        &self,
        replicas_info_manager_state: Vec<u8>,
        last_applied: Option<LogId>,
        last_membership: &StoredMembership,
    ) -> Result<(), std::io::Error> {
        let Some(repository) = &self.repository else {
            return Ok(());
        };

        repository
            .persist_state(replicas_info_manager_state, last_applied, last_membership)
            .await
    }

    async fn persist_snapshot(&self, snapshot: &CurrentSnapshot) -> Result<(), std::io::Error> {
        let Some(repository) = &self.repository else {
            return Ok(());
        };

        repository.persist_snapshot(&snapshot.meta, &snapshot.data).await
    }

    async fn persist_snapshot_install(
        &self,
        data: &SnapshotData,
        current_snapshot: &CurrentSnapshot,
    ) -> Result<(), std::io::Error> {
        let Some(repository) = &self.repository else {
            return Ok(());
        };
        let last_membership = data.last_membership.clone().unwrap_or_default();
        repository
            .persist_snapshot_install(
                &data.replicas_info_manager_state,
                data.last_applied,
                &last_membership,
                &current_snapshot.meta,
                &current_snapshot.data,
            )
            .await
    }
}

fn validate_snapshot_bytes(bytes: &[u8]) -> Result<SnapshotData, std::io::Error> {
    if bytes.len() > SNAPSHOT_MAX_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Controller snapshot size {} exceeds the {} byte limit",
                bytes.len(),
                SNAPSHOT_MAX_BYTES
            ),
        ));
    }
    let data: SnapshotData = decode_v1(RaftRecordKey::SnapshotData, bytes)?;
    data.validate()?;
    Ok(data)
}

/// Validates a serialized controller snapshot without installing it.
///
/// The validator enforces the production size limit, schema, format version, required membership,
/// and checksum. It is suitable for offline inspection and fuzzing because it never mutates live
/// controller state.
///
/// # Errors
///
/// Returns [`std::io::ErrorKind::InvalidData`] when the payload violates any snapshot contract.
pub fn validate_snapshot_payload(bytes: &[u8]) -> Result<(), std::io::Error> {
    validate_snapshot_bytes(bytes).map(|_| ())
}

/// Integrity-checked identity used to bind a release manifest to its payload.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ValidatedSnapshotIdentity {
    pub(crate) snapshot_id: String,
    pub(crate) last_applied: Option<LogId>,
    pub(crate) voter_ids: Vec<u64>,
}

/// Validates a payload and returns only the non-sensitive Raft identity needed
/// by release checkpoint verification.
pub(crate) fn inspect_snapshot_payload(bytes: &[u8]) -> Result<ValidatedSnapshotIdentity, std::io::Error> {
    let data = validate_snapshot_bytes(bytes)?;
    let voter_ids = data.last_membership.unwrap_or_default().voter_ids().collect();
    Ok(ValidatedSnapshotIdentity {
        snapshot_id: data.snapshot_id,
        last_applied: data.last_applied,
        voter_ids,
    })
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot, std::io::Error> {
        let _state_guard = self.state_lock.lock().await;
        let data = self.build_snapshot_data().await?;
        let last_applied = data.last_applied;
        let last_membership = data.last_membership.clone().unwrap_or_default();
        let snapshot_id = data.snapshot_id.clone();
        let snapshot_data = encode_v1(RaftRecordKey::SnapshotData, &data)?;
        if snapshot_data.len() > SNAPSHOT_MAX_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Controller snapshot size {} exceeds the {} byte limit",
                    snapshot_data.len(),
                    SNAPSHOT_MAX_BYTES
                ),
            ));
        }
        let current_snapshot = CurrentSnapshot {
            meta: SnapshotMeta {
                last_log_id: last_applied,
                last_membership,
                snapshot_id,
            },
            data: snapshot_data.clone(),
        };
        self.persist_snapshot(&current_snapshot).await?;
        *self.current_snapshot.write().await = Some(current_snapshot.clone());

        Ok(Snapshot {
            meta: current_snapshot.meta,
            snapshot: std::io::Cursor::new(snapshot_data),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), std::io::Error> {
        let last_applied = *self.last_applied.read().await;
        let last_membership = self.last_membership.read().await.clone();
        Ok((last_applied, last_membership))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply<Strm>(&mut self, entries: Strm) -> Result<(), std::io::Error>
    where
        Strm: futures::Stream<Item = Result<openraft::storage::EntryResponder<TypeConfig>, std::io::Error>>
            + Unpin
            + OptionalSend,
    {
        use futures::StreamExt;

        let _state_guard = self.state_lock.lock().await;
        futures::pin_mut!(entries);
        let mut responses = Vec::new();
        let candidate_manager = ReplicasInfoManager::new(self.config.clone());
        candidate_manager
            .deserialize_from(&self.replicas_info_manager.load().serialize().map_err(storage_error)?)
            .map_err(storage_error)?;
        let mut candidate_last_applied = *self.last_applied.read().await;
        let mut candidate_last_membership = self.last_membership.read().await.clone();

        while let Some(entry_result) = entries.next().await {
            let (entry, responder) = entry_result?;
            let log_id = entry.log_id;

            candidate_last_applied = Some(log_id);

            let response = match entry.payload {
                EntryPayload::Blank => ControllerResponse::success(),
                EntryPayload::Normal(ref request) => Self::apply_request_to(&candidate_manager, request),
                EntryPayload::Membership(ref membership) => {
                    candidate_last_membership = StoredMembership::new(Some(log_id), membership.clone());
                    ControllerResponse::success()
                }
            };

            responses.push((responder, response));
        }

        if responses.is_empty() {
            return Ok(());
        }
        let candidate_state = candidate_manager.serialize().map_err(storage_error)?;
        self.persist_state_values(
            candidate_state.clone(),
            candidate_last_applied,
            &candidate_last_membership,
        )
        .await?;

        self.replicas_info_manager.store(Arc::new(candidate_manager));
        *self.last_applied.write().await = candidate_last_applied;
        *self.last_membership.write().await = candidate_last_membership;

        for (responder, response) in responses {
            if let Some(tx) = responder {
                tx.send(response);
            }
        }

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<std::io::Cursor<Vec<u8>>, std::io::Error> {
        Ok(std::io::Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: std::io::Cursor<Vec<u8>>,
    ) -> Result<(), std::io::Error> {
        let _state_guard = self.state_lock.lock().await;
        let snapshot_data = validate_snapshot_bytes(snapshot.get_ref())?;
        let snapshot_membership = snapshot_data.last_membership.clone().unwrap_or_default();
        if snapshot_data.last_applied != meta.last_log_id
            || snapshot_membership != meta.last_membership
            || snapshot_data.snapshot_id != meta.snapshot_id
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Controller snapshot metadata does not match its checksummed payload",
            ));
        }
        let candidate_manager = ReplicasInfoManager::new(self.config.clone());
        candidate_manager
            .deserialize_from(&snapshot_data.replicas_info_manager_state)
            .map_err(storage_error)?;
        let current_snapshot = CurrentSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };
        self.persist_snapshot_install(&snapshot_data, &current_snapshot).await?;
        self.replicas_info_manager.store(Arc::new(candidate_manager));
        *self.last_applied.write().await = snapshot_data.last_applied;
        *self.last_membership.write().await = snapshot_membership;
        *self.current_snapshot.write().await = Some(current_snapshot);
        tracing::info!("Installed snapshot at {:?}", meta.last_log_id);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, std::io::Error> {
        Ok(self.current_snapshot.read().await.clone().map(|snapshot| Snapshot {
            meta: snapshot.meta,
            snapshot: std::io::Cursor::new(snapshot.data),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ControllerConfig;
    use crate::typ::BrokerIdentityInfoSnapshot;
    use crate::typ::BrokerLiveInfoSnapshot;
    use rocketmq_protocol::code::response_code::ResponseCode;

    fn new_state_machine() -> StateMachine {
        let config = ControllerConfigReader::new(
            ControllerConfig::default().with_node_info(1, "127.0.0.1:39876".parse().expect("valid addr")),
        );
        StateMachine::new(config)
    }

    fn heartbeat_request(last_update_timestamp: u64, heartbeat_timeout_millis: u64) -> ControllerRequest {
        ControllerRequest::BrokerHeartbeat {
            broker_identity: BrokerIdentityInfoSnapshot::new("test-cluster", "broker-a", Some(1)),
            broker_live_info: BrokerLiveInfoSnapshot {
                cluster_name: "test-cluster".to_string(),
                broker_name: "broker-a".to_string(),
                broker_addr: "127.0.0.1:10911".to_string(),
                broker_id: 1,
                last_update_timestamp,
                heartbeat_timeout_millis,
                epoch: 1,
                max_offset: 100,
                confirm_offset: 80,
                election_priority: Some(1),
            },
            lease_grant_allowed: false,
        }
    }

    #[test]
    fn apply_broker_id_updates_replicas_info_manager() {
        let state_machine = new_state_machine();

        let response = state_machine.apply_request(&ControllerRequest::ApplyBrokerId {
            cluster_name: "test-cluster".to_string(),
            broker_name: "broker-a".to_string(),
            broker_address: "127.0.0.1:10911".to_string(),
            applied_broker_id: 1,
            register_check_code: "code-1".to_string(),
        });

        assert_eq!(response.response_code, ResponseCode::Success.to_i32());

        let next_broker_id = state_machine
            .replicas_info_manager()
            .get_next_broker_id("test-cluster", "broker-a")
            .response()
            .and_then(|header| header.next_broker_id)
            .expect("next broker id");
        assert_eq!(next_broker_id, 2);
    }

    #[tokio::test]
    async fn snapshot_round_trip_preserves_replicas_info_manager_state() {
        let mut state_machine = new_state_machine();
        state_machine.apply_request(&ControllerRequest::ApplyBrokerId {
            cluster_name: "test-cluster".to_string(),
            broker_name: "broker-a".to_string(),
            broker_address: "127.0.0.1:10911".to_string(),
            applied_broker_id: 1,
            register_check_code: "code-1".to_string(),
        });
        state_machine.apply_request(&heartbeat_request(1_000, 60_000));

        let snapshot = state_machine.build_snapshot().await.expect("snapshot");

        let mut restored = new_state_machine();
        restored
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install snapshot");

        let next_broker_id = restored
            .replicas_info_manager()
            .get_next_broker_id("test-cluster", "broker-a")
            .response()
            .and_then(|header| header.next_broker_id)
            .expect("next broker id");
        assert_eq!(next_broker_id, 2);
        assert!(restored
            .replicas_info_manager()
            .is_broker_active_at("test-cluster", "broker-a", 1, 2_000));
    }

    #[tokio::test]
    async fn reopened_state_reports_persisted_applied_log_before_raft_metrics_publish() {
        let context = rocketmq_runtime::RuntimeContext::from_current("controller-persisted-bootstrap-state-test");
        let backend = crate::storage::create_storage(
            crate::storage::StorageConfig::Memory,
            context.service_context("storage").storage_io().clone(),
        )
        .await
        .expect("create shared memory backend");
        let config = ControllerConfigReader::new(
            ControllerConfig::default().with_node_info(1, "127.0.0.1:39877".parse().expect("valid addr")),
        );
        let state_machine = StateMachine::open(config.clone(), backend.clone())
            .await
            .expect("open state machine");
        let last_applied = LogId {
            leader_id: crate::typ::Vote::new(7, 1).leader_id,
            index: 42,
        };
        let serialized_state = state_machine
            .replicas_info_manager()
            .serialize()
            .expect("serialize state machine");
        state_machine
            .persist_state_values(serialized_state, Some(last_applied), &StoredMembership::default())
            .await
            .expect("persist applied state");

        let reopened = StateMachine::open(config, backend).await.expect("reopen state machine");

        assert!(reopened.has_persisted_applied_state().await);
    }

    #[test]
    fn broker_heartbeat_updates_replicated_live_table() {
        let state_machine = new_state_machine();

        let response = state_machine.apply_request(&heartbeat_request(1_000, 3_000));

        assert_eq!(response.response_code, ResponseCode::Success.to_i32());
        assert!(state_machine
            .replicas_info_manager()
            .is_broker_active_at("test-cluster", "broker-a", 1, 3_999));
        assert!(!state_machine
            .replicas_info_manager()
            .is_broker_active_at("test-cluster", "broker-a", 1, 4_001));
    }

    #[test]
    fn snapshot_v1_bytes_are_deterministic_and_unchanged() {
        let snapshot = SnapshotData::new(vec![1, 2], None, StoredMembership::default()).expect("snapshot fixture");

        let bytes = encode_v1(RaftRecordKey::SnapshotData, &snapshot).expect("encode snapshot fixture");

        assert_eq!(
            String::from_utf8(bytes).expect("UTF-8 snapshot"),
            r#"{"replicas_info_manager_state":[1,2],"last_applied":null,"last_membership":{"log_id":null,"membership":{"configs":[],"nodes":{}}},"snapshot_id":"snapshot-0","format_version":1,"checksum":775474328}"#
        );
    }

    #[test]
    fn deterministic_snapshot_cases_preserve_checksum_and_payload() {
        const SEED: u64 = 0x524d_5143_4f4e_5452;
        let mut state = SEED;

        for case in 0..32 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let payload_len = (state & 0xff) as usize;
            let payload = (0..payload_len)
                .map(|index| state.rotate_left(index as u32) as u8)
                .collect::<Vec<_>>();
            let snapshot =
                SnapshotData::new(payload.clone(), None, StoredMembership::default()).expect("generated snapshot");
            let first = encode_v1(RaftRecordKey::SnapshotData, &snapshot)
                .unwrap_or_else(|error| panic!("seed={SEED:#018x} case={case} encode failed: {error}"));
            let second = encode_v1(RaftRecordKey::SnapshotData, &snapshot)
                .unwrap_or_else(|error| panic!("seed={SEED:#018x} case={case} re-encode failed: {error}"));
            assert_eq!(first, second, "seed={SEED:#018x} case={case}");

            let decoded = validate_snapshot_bytes(&first)
                .unwrap_or_else(|error| panic!("seed={SEED:#018x} case={case} validate failed: {error}"));
            assert_eq!(
                decoded.replicas_info_manager_state, payload,
                "seed={SEED:#018x} case={case}"
            );
            assert_eq!(decoded.last_applied, None, "seed={SEED:#018x} case={case}");
            assert_eq!(
                decoded.last_membership,
                Some(StoredMembership::default()),
                "seed={SEED:#018x} case={case}"
            );
            assert_eq!(decoded.checksum, snapshot.checksum, "seed={SEED:#018x} case={case}");

            let mut corrupted = decoded;
            corrupted.checksum ^= 1;
            let corrupted_bytes =
                encode_v1(RaftRecordKey::SnapshotData, &corrupted).expect("corrupted snapshot remains serializable");
            assert!(
                validate_snapshot_bytes(&corrupted_bytes).is_err(),
                "seed={SEED:#018x} case={case} accepted a checksum mismatch"
            );
        }
    }

    #[test]
    fn check_not_active_broker_removes_expired_live_info() {
        let state_machine = new_state_machine();
        state_machine.apply_request(&heartbeat_request(1_000, 3_000));

        let response = state_machine.apply_request(&ControllerRequest::CheckNotActiveBroker {
            check_time_millis: 4_001,
        });

        assert_eq!(response.response_code, ResponseCode::Success.to_i32());
        let inactive_brokers: Vec<BrokerIdentityInfoSnapshot> =
            serde_json::from_slice(response.body.as_deref().expect("inactive broker body")).expect("decode body");
        assert_eq!(
            inactive_brokers,
            vec![BrokerIdentityInfoSnapshot::new("test-cluster", "broker-a", Some(1))]
        );
        assert!(!state_machine
            .replicas_info_manager()
            .is_broker_active_at("test-cluster", "broker-a", 1, 4_001));
    }
}
