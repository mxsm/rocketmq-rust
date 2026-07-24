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
use rocketmq_common::utils::crc32_utils::crc32;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::header::controller::get_next_broker_id_response_header::GetNextBrokerIdResponseHeader;
use rocketmq_remoting::protocol::header::controller::get_replica_info_response_header::GetReplicaInfoResponseHeader;
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

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

const SNAPSHOT_META_KEY: &str = "openraft/state_machine/current_snapshot_meta";
const SNAPSHOT_DATA_KEY: &str = "openraft/state_machine/current_snapshot_data";
const REPLICAS_INFO_MANAGER_STATE_KEY: &str = "openraft/state_machine/replicas_info_manager";
const LAST_APPLIED_KEY: &str = "openraft/state_machine/last_applied";
const LAST_MEMBERSHIP_KEY: &str = "openraft/state_machine/last_membership";
const SNAPSHOT_FORMAT_VERSION: u16 = 1;

fn storage_error(error: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(error.to_string())
}

async fn load_json<T: DeserializeOwned>(
    backend: &SharedStorageBackend,
    key: &str,
) -> Result<Option<T>, std::io::Error> {
    let Some(bytes) = backend.get(key).await.map_err(storage_error)? else {
        return Ok(None);
    };

    serde_json::from_slice(&bytes).map(Some).map_err(storage_error)
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
        serde_json::to_vec(&(
            self.format_version,
            &self.replicas_info_manager_state,
            self.last_applied,
            &self.last_membership,
            &self.snapshot_id,
        ))
        .map(|bytes| crc32(&bytes))
        .map_err(storage_error)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PersistedSnapshotMeta {
    last_log_id: Option<LogId>,
    last_membership: StoredMembership,
    snapshot_id: String,
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
    backend: Option<SharedStorageBackend>,
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
            backend: None,
            state_lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn open(config: ControllerConfigReader, backend: SharedStorageBackend) -> Result<Self, std::io::Error> {
        let replicas_state = backend
            .get(REPLICAS_INFO_MANAGER_STATE_KEY)
            .await
            .map_err(storage_error)?;
        let last_applied_bytes = backend.get(LAST_APPLIED_KEY).await.map_err(storage_error)?;
        let last_membership_bytes = backend.get(LAST_MEMBERSHIP_KEY).await.map_err(storage_error)?;
        let (replicas_state, last_applied, last_membership) =
            match (replicas_state, last_applied_bytes, last_membership_bytes) {
                (None, None, None) => (None, None, StoredMembership::default()),
                (Some(state), Some(last_applied), Some(last_membership)) => (
                    Some(state),
                    serde_json::from_slice(&last_applied).map_err(storage_error)?,
                    serde_json::from_slice(&last_membership).map_err(storage_error)?,
                ),
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Controller state, last-applied index, and membership must be committed together",
                    ));
                }
            };
        let replicas_info_manager = Arc::new(ReplicasInfoManager::new(config.clone()));
        if let Some(state) = replicas_state {
            replicas_info_manager.deserialize_from(&state).map_err(storage_error)?;
        }
        let state_machine = Self {
            replicas_info_manager: Arc::new(ArcSwap::from(replicas_info_manager)),
            config,
            last_applied: Arc::new(RwLock::new(last_applied)),
            last_membership: Arc::new(RwLock::new(last_membership)),
            current_snapshot: Arc::new(RwLock::new(None)),
            backend: Some(backend.clone()),
            state_lock: Arc::new(Mutex::new(())),
        };

        match (
            load_json::<PersistedSnapshotMeta>(&backend, SNAPSHOT_META_KEY).await?,
            backend.get(SNAPSHOT_DATA_KEY).await.map_err(storage_error)?,
        ) {
            (Some(meta), Some(data)) => {
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
                *state_machine.current_snapshot.write().await = Some(CurrentSnapshot {
                    meta: SnapshotMeta {
                        last_log_id: meta.last_log_id,
                        last_membership: meta.last_membership,
                        snapshot_id: meta.snapshot_id,
                    },
                    data,
                });
            }
            (None, None) => {}
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Controller snapshot metadata and payload must be committed together",
                ));
            }
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
            } => {
                replicas_info_manager.on_broker_heartbeat(broker_identity.clone(), broker_live_info.clone());
                ControllerResponse::new(
                    rocketmq_remoting::code::response_code::ResponseCode::Success.into(),
                    Some("Heart beat success".to_string()),
                    None,
                    None,
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
                    rocketmq_remoting::code::response_code::ResponseCode::Success.into(),
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
        let Some(backend) = &self.backend else {
            return Ok(());
        };

        backend
            .write_batch(
                vec![
                    (REPLICAS_INFO_MANAGER_STATE_KEY.to_string(), replicas_info_manager_state),
                    (
                        LAST_APPLIED_KEY.to_string(),
                        serde_json::to_vec(&last_applied).map_err(storage_error)?,
                    ),
                    (
                        LAST_MEMBERSHIP_KEY.to_string(),
                        serde_json::to_vec(last_membership).map_err(storage_error)?,
                    ),
                ],
                Vec::new(),
            )
            .await
            .map_err(storage_error)?;
        backend.sync().await.map_err(storage_error)?;
        Ok(())
    }

    async fn persist_snapshot(&self, snapshot: &CurrentSnapshot) -> Result<(), std::io::Error> {
        let Some(backend) = &self.backend else {
            return Ok(());
        };

        let persisted_meta = PersistedSnapshotMeta {
            last_log_id: snapshot.meta.last_log_id,
            last_membership: snapshot.meta.last_membership.clone(),
            snapshot_id: snapshot.meta.snapshot_id.clone(),
        };
        backend
            .write_batch(
                vec![
                    (
                        SNAPSHOT_META_KEY.to_string(),
                        serde_json::to_vec(&persisted_meta).map_err(storage_error)?,
                    ),
                    (SNAPSHOT_DATA_KEY.to_string(), snapshot.data.clone()),
                ],
                Vec::new(),
            )
            .await
            .map_err(storage_error)?;
        backend.sync().await.map_err(storage_error)?;
        Ok(())
    }

    async fn persist_snapshot_install(
        &self,
        data: &SnapshotData,
        current_snapshot: &CurrentSnapshot,
    ) -> Result<(), std::io::Error> {
        let Some(backend) = &self.backend else {
            return Ok(());
        };
        let persisted_meta = PersistedSnapshotMeta {
            last_log_id: current_snapshot.meta.last_log_id,
            last_membership: current_snapshot.meta.last_membership.clone(),
            snapshot_id: current_snapshot.meta.snapshot_id.clone(),
        };
        let last_membership = data.last_membership.clone().unwrap_or_default();
        backend
            .write_batch(
                vec![
                    (
                        REPLICAS_INFO_MANAGER_STATE_KEY.to_string(),
                        data.replicas_info_manager_state.clone(),
                    ),
                    (
                        LAST_APPLIED_KEY.to_string(),
                        serde_json::to_vec(&data.last_applied).map_err(storage_error)?,
                    ),
                    (
                        LAST_MEMBERSHIP_KEY.to_string(),
                        serde_json::to_vec(&last_membership).map_err(storage_error)?,
                    ),
                    (
                        SNAPSHOT_META_KEY.to_string(),
                        serde_json::to_vec(&persisted_meta).map_err(storage_error)?,
                    ),
                    (SNAPSHOT_DATA_KEY.to_string(), current_snapshot.data.clone()),
                ],
                Vec::new(),
            )
            .await
            .map_err(storage_error)?;
        backend.sync().await.map_err(storage_error)
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
    let data: SnapshotData = serde_json::from_slice(bytes).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Failed to deserialize snapshot: {error}"),
        )
    })?;
    data.validate()?;
    Ok(data)
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot, std::io::Error> {
        let _state_guard = self.state_lock.lock().await;
        let data = self.build_snapshot_data().await?;
        let last_applied = data.last_applied;
        let last_membership = data.last_membership.clone().unwrap_or_default();
        let snapshot_id = data.snapshot_id.clone();
        let snapshot_data = serde_json::to_vec(&data)
            .map_err(|error| std::io::Error::other(format!("Failed to serialize snapshot: {}", error)))?;
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
    use rocketmq_remoting::code::response_code::ResponseCode;

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
