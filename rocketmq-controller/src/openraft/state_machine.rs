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

use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::EntryPayload;
use openraft::OptionalSend;
use openraft::RaftSnapshotBuilder;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_rust::ArcMut;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::config::ControllerConfig;
use crate::event::controller_result::ControllerResult;
use crate::manager::replicas_info_manager::ReplicasInfoManager;
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

async fn persist_json<T: Serialize>(
    backend: &SharedStorageBackend,
    key: &str,
    value: &T,
) -> Result<(), std::io::Error> {
    let bytes = serde_json::to_vec(value).map_err(storage_error)?;
    backend.put(key, &bytes).await.map_err(storage_error)?;
    Ok(())
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotData {
    pub replicas_info_manager_state: Vec<u8>,
    pub last_applied: Option<LogId>,
    pub last_membership: Option<StoredMembership>,
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

#[derive(Clone)]
pub struct StateMachine {
    replicas_info_manager: Arc<ReplicasInfoManager>,
    last_applied: Arc<RwLock<Option<LogId>>>,
    last_membership: Arc<RwLock<StoredMembership>>,
    current_snapshot: Arc<RwLock<Option<CurrentSnapshot>>>,
    backend: Option<SharedStorageBackend>,
}

impl StateMachine {
    pub fn new(config: ArcMut<ControllerConfig>) -> Self {
        Self {
            replicas_info_manager: Arc::new(ReplicasInfoManager::new(config)),
            last_applied: Arc::new(RwLock::new(None)),
            last_membership: Arc::new(RwLock::new(StoredMembership::default())),
            current_snapshot: Arc::new(RwLock::new(None)),
            backend: None,
        }
    }

    pub async fn open(config: ArcMut<ControllerConfig>, backend: SharedStorageBackend) -> Result<Self, std::io::Error> {
        let state_machine = Self {
            replicas_info_manager: Arc::new(ReplicasInfoManager::new(config)),
            last_applied: Arc::new(RwLock::new(load_json(&backend, LAST_APPLIED_KEY).await?)),
            last_membership: Arc::new(RwLock::new(
                load_json(&backend, LAST_MEMBERSHIP_KEY).await?.unwrap_or_default(),
            )),
            current_snapshot: Arc::new(RwLock::new(None)),
            backend: Some(backend.clone()),
        };

        if let Some(state) = backend
            .get(REPLICAS_INFO_MANAGER_STATE_KEY)
            .await
            .map_err(storage_error)?
        {
            state_machine
                .replicas_info_manager
                .deserialize_from(&state)
                .map_err(storage_error)?;
        }

        if let (Some(meta), Some(data)) = (
            load_json::<PersistedSnapshotMeta>(&backend, SNAPSHOT_META_KEY).await?,
            backend.get(SNAPSHOT_DATA_KEY).await.map_err(storage_error)?,
        ) {
            *state_machine.current_snapshot.write().await = Some(CurrentSnapshot {
                meta: SnapshotMeta {
                    last_log_id: meta.last_log_id,
                    last_membership: meta.last_membership,
                    snapshot_id: meta.snapshot_id,
                },
                data,
            });
        }

        Ok(state_machine)
    }

    pub fn replicas_info_manager(&self) -> Arc<ReplicasInfoManager> {
        self.replicas_info_manager.clone()
    }

    fn response_from_result<T, F>(&self, result: ControllerResult<T>, map_header: F) -> ControllerResponse
    where
        F: FnOnce(T) -> ControllerResponseHeader,
    {
        let (events, header, body, response_code, remark) = result.into_parts();
        for event in events {
            if let Err(error) = self.replicas_info_manager.try_apply_event(event.as_ref()) {
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

    fn response_from_result_without_header(&self, result: ControllerResult<()>) -> ControllerResponse {
        let (events, _header, body, response_code, remark) = result.into_parts();
        for event in events {
            if let Err(error) = self.replicas_info_manager.try_apply_event(event.as_ref()) {
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

    fn apply_request(&self, request: &ControllerRequest) -> ControllerResponse {
        match request {
            ControllerRequest::ApplyBrokerId {
                cluster_name,
                broker_name,
                broker_address,
                applied_broker_id,
                register_check_code,
            } => {
                let result = self.replicas_info_manager.apply_broker_id(
                    cluster_name,
                    broker_name,
                    broker_address,
                    *applied_broker_id,
                    register_check_code,
                );
                self.response_from_result(result, ControllerResponseHeader::ApplyBrokerId)
            }
            ControllerRequest::RegisterBroker {
                cluster_name,
                broker_name,
                broker_address,
                broker_id,
                alive_broker_ids: _,
            } => {
                let result = self.replicas_info_manager.register_broker(
                    cluster_name,
                    broker_name,
                    broker_address,
                    *broker_id,
                    self.replicas_info_manager.as_ref(),
                );
                self.response_from_result(result, ControllerResponseHeader::RegisterBroker)
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
                let result = self.replicas_info_manager.alter_sync_state_set(
                    broker_name,
                    *master_broker_id,
                    *master_epoch,
                    new_sync_state_set.clone(),
                    *sync_state_set_epoch,
                    self.replicas_info_manager.as_ref(),
                );
                self.response_from_result(result, ControllerResponseHeader::AlterSyncStateSet)
            }
            ControllerRequest::ElectMaster {
                cluster_name: _cluster_name,
                broker_name,
                broker_id,
                designate_elect,
                alive_broker_ids: _,
                live_broker_infos: _,
            } => {
                let result = self.replicas_info_manager.elect_master(
                    broker_name,
                    *broker_id,
                    *designate_elect,
                    self.replicas_info_manager.as_ref(),
                );
                self.response_from_result(result, ControllerResponseHeader::ElectMaster)
            }
            ControllerRequest::CleanBrokerData {
                cluster_name,
                broker_name,
                broker_controller_ids_to_clean,
                clean_living_broker,
                alive_broker_ids: _,
            } => {
                let result = self.replicas_info_manager.clean_broker_data(
                    cluster_name,
                    broker_name,
                    broker_controller_ids_to_clean.as_deref(),
                    *clean_living_broker,
                    self.replicas_info_manager.as_ref(),
                );
                self.response_from_result_without_header(result)
            }
            ControllerRequest::BrokerHeartbeat {
                broker_identity,
                broker_live_info,
            } => {
                self.replicas_info_manager
                    .on_broker_heartbeat(broker_identity.clone(), broker_live_info.clone());
                ControllerResponse::new(
                    rocketmq_remoting::code::response_code::ResponseCode::Success.into(),
                    Some("Heart beat success".to_string()),
                    None,
                    None,
                )
            }
            ControllerRequest::BrokerChannelClose { broker_identity } => {
                self.replicas_info_manager.on_broker_channel_close(broker_identity);
                ControllerResponse::success()
            }
            ControllerRequest::CheckNotActiveBroker { check_time_millis } => {
                let inactive_brokers = self.replicas_info_manager.check_not_active_broker(*check_time_millis);
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

    async fn build_snapshot_data(&self) -> Result<SnapshotData, std::io::Error> {
        let replicas_info_manager_state = self
            .replicas_info_manager
            .serialize()
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let last_applied = *self.last_applied.read().await;
        let last_membership = self.last_membership.read().await.clone();

        Ok(SnapshotData {
            replicas_info_manager_state,
            last_applied,
            last_membership: Some(last_membership),
        })
    }

    async fn install_snapshot_data(&self, data: SnapshotData) -> Result<(), std::io::Error> {
        self.replicas_info_manager
            .deserialize_from(&data.replicas_info_manager_state)
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        *self.last_applied.write().await = data.last_applied;
        *self.last_membership.write().await = data.last_membership.unwrap_or_default();
        self.persist_state().await?;
        Ok(())
    }

    async fn persist_state(&self) -> Result<(), std::io::Error> {
        let Some(backend) = &self.backend else {
            return Ok(());
        };

        let replicas_info_manager_state = self.replicas_info_manager.serialize().map_err(storage_error)?;
        let last_applied = *self.last_applied.read().await;
        let last_membership = self.last_membership.read().await.clone();

        backend
            .batch_put(vec![
                (REPLICAS_INFO_MANAGER_STATE_KEY.to_string(), replicas_info_manager_state),
                (
                    LAST_APPLIED_KEY.to_string(),
                    serde_json::to_vec(&last_applied).map_err(storage_error)?,
                ),
                (
                    LAST_MEMBERSHIP_KEY.to_string(),
                    serde_json::to_vec(&last_membership).map_err(storage_error)?,
                ),
            ])
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
        persist_json(backend, SNAPSHOT_META_KEY, &persisted_meta).await?;
        backend
            .put(SNAPSHOT_DATA_KEY, &snapshot.data)
            .await
            .map_err(storage_error)?;
        backend.sync().await.map_err(storage_error)?;
        Ok(())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot, std::io::Error> {
        let data = self.build_snapshot_data().await?;
        let last_applied = data.last_applied;
        let last_membership = data.last_membership.clone().unwrap_or_default();
        let snapshot_data = serde_json::to_vec(&data)
            .map_err(|error| std::io::Error::other(format!("Failed to serialize snapshot: {}", error)))?;
        let current_snapshot = CurrentSnapshot {
            meta: SnapshotMeta {
                last_log_id: last_applied,
                last_membership,
                snapshot_id: format!("snapshot-{}", last_applied.map_or(0, |log_id| log_id.index)),
            },
            data: snapshot_data.clone(),
        };
        *self.current_snapshot.write().await = Some(current_snapshot.clone());
        self.persist_snapshot(&current_snapshot).await?;

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

        futures::pin_mut!(entries);
        let mut responses = Vec::new();

        while let Some(entry_result) = entries.next().await {
            let (entry, responder) = entry_result?;
            let log_id = entry.log_id;

            *self.last_applied.write().await = Some(log_id);

            let response = match entry.payload {
                EntryPayload::Blank => ControllerResponse::success(),
                EntryPayload::Normal(ref request) => self.apply_request(request),
                EntryPayload::Membership(ref membership) => {
                    *self.last_membership.write().await = StoredMembership::new(Some(log_id), membership.clone());
                    ControllerResponse::success()
                }
            };

            responses.push((responder, response));
        }

        self.persist_state().await?;

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
        let snapshot_data: SnapshotData = serde_json::from_slice(snapshot.get_ref()).map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to deserialize snapshot: {}", error),
            )
        })?;

        self.install_snapshot_data(snapshot_data).await?;
        let current_snapshot = CurrentSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };
        *self.current_snapshot.write().await = Some(current_snapshot.clone());
        self.persist_snapshot(&current_snapshot).await?;
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
        let config =
            ArcMut::new(ControllerConfig::default().with_node_info(1, "127.0.0.1:39876".parse().expect("valid addr")));
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
