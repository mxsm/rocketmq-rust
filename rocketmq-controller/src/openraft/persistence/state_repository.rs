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

use crate::storage::SharedStorageBackend;
use crate::typ::LogId;
use crate::typ::SnapshotMeta;
use crate::typ::StoredMembership;

use super::backend_error;
use super::decode_v1;
use super::encode_v1;
use super::RaftRecordKey;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PersistedSnapshotMeta {
    last_log_id: Option<LogId>,
    last_membership: StoredMembership,
    snapshot_id: String,
}

pub(in crate::openraft) struct PersistedSnapshot {
    pub(in crate::openraft) meta: SnapshotMeta,
    pub(in crate::openraft) data: Vec<u8>,
}

pub(in crate::openraft) struct LoadedStateRepository {
    pub(in crate::openraft) replicas_info_manager_state: Option<Vec<u8>>,
    pub(in crate::openraft) last_applied: Option<LogId>,
    pub(in crate::openraft) last_membership: StoredMembership,
    pub(in crate::openraft) current_snapshot: Option<PersistedSnapshot>,
}

#[derive(Clone)]
pub(in crate::openraft) struct RaftStateRepository {
    backend: SharedStorageBackend,
}

impl RaftStateRepository {
    pub(in crate::openraft) fn new(backend: SharedStorageBackend) -> Self {
        Self { backend }
    }

    pub(in crate::openraft) async fn load(&self) -> Result<LoadedStateRepository, std::io::Error> {
        let replicas_state = self.read_bytes(RaftRecordKey::ReplicasInfoManagerState).await?;
        let last_applied = self.read_json::<Option<LogId>>(RaftRecordKey::LastApplied).await?;
        let last_membership = self
            .read_json::<StoredMembership>(RaftRecordKey::LastMembership)
            .await?;
        let (replicas_info_manager_state, last_applied, last_membership) =
            match (replicas_state, last_applied, last_membership) {
                (None, None, None) => (None, None, StoredMembership::default()),
                (Some(state), Some(last_applied), Some(last_membership)) => {
                    (Some(state), last_applied, last_membership)
                }
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Controller state, last-applied index, and membership must be committed together",
                    ));
                }
            };

        let snapshot_meta = self
            .read_json::<PersistedSnapshotMeta>(RaftRecordKey::SnapshotMeta)
            .await?;
        let snapshot_data = self.read_bytes(RaftRecordKey::SnapshotData).await?;
        let current_snapshot = match (snapshot_meta, snapshot_data) {
            (Some(meta), Some(data)) => Some(PersistedSnapshot {
                meta: SnapshotMeta {
                    last_log_id: meta.last_log_id,
                    last_membership: meta.last_membership,
                    snapshot_id: meta.snapshot_id,
                },
                data,
            }),
            (None, None) => None,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Controller snapshot metadata and payload must be committed together",
                ));
            }
        };

        Ok(LoadedStateRepository {
            replicas_info_manager_state,
            last_applied,
            last_membership,
            current_snapshot,
        })
    }

    pub(in crate::openraft) async fn persist_state(
        &self,
        replicas_info_manager_state: Vec<u8>,
        last_applied: Option<LogId>,
        last_membership: &StoredMembership,
    ) -> Result<(), std::io::Error> {
        let last_applied_key = RaftRecordKey::LastApplied;
        let membership_key = RaftRecordKey::LastMembership;
        self.write_batch_and_sync(
            vec![
                (
                    RaftRecordKey::ReplicasInfoManagerState.as_v1_key().into_owned(),
                    replicas_info_manager_state,
                ),
                (
                    last_applied_key.as_v1_key().into_owned(),
                    encode_v1(last_applied_key, &last_applied)?,
                ),
                (
                    membership_key.as_v1_key().into_owned(),
                    encode_v1(membership_key, last_membership)?,
                ),
            ],
            RaftRecordKey::ReplicasInfoManagerState,
        )
        .await
    }

    pub(in crate::openraft) async fn persist_snapshot(
        &self,
        meta: &SnapshotMeta,
        data: &[u8],
    ) -> Result<(), std::io::Error> {
        let meta_key = RaftRecordKey::SnapshotMeta;
        let persisted_meta = PersistedSnapshotMeta {
            last_log_id: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            snapshot_id: meta.snapshot_id.clone(),
        };
        self.write_batch_and_sync(
            vec![
                (meta_key.as_v1_key().into_owned(), encode_v1(meta_key, &persisted_meta)?),
                (RaftRecordKey::SnapshotData.as_v1_key().into_owned(), data.to_vec()),
            ],
            meta_key,
        )
        .await
    }

    pub(in crate::openraft) async fn persist_snapshot_install(
        &self,
        replicas_info_manager_state: &[u8],
        last_applied: Option<LogId>,
        last_membership: &StoredMembership,
        meta: &SnapshotMeta,
        data: &[u8],
    ) -> Result<(), std::io::Error> {
        let last_applied_key = RaftRecordKey::LastApplied;
        let membership_key = RaftRecordKey::LastMembership;
        let meta_key = RaftRecordKey::SnapshotMeta;
        let persisted_meta = PersistedSnapshotMeta {
            last_log_id: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            snapshot_id: meta.snapshot_id.clone(),
        };
        self.write_batch_and_sync(
            vec![
                (
                    RaftRecordKey::ReplicasInfoManagerState.as_v1_key().into_owned(),
                    replicas_info_manager_state.to_vec(),
                ),
                (
                    last_applied_key.as_v1_key().into_owned(),
                    encode_v1(last_applied_key, &last_applied)?,
                ),
                (
                    membership_key.as_v1_key().into_owned(),
                    encode_v1(membership_key, last_membership)?,
                ),
                (meta_key.as_v1_key().into_owned(), encode_v1(meta_key, &persisted_meta)?),
                (RaftRecordKey::SnapshotData.as_v1_key().into_owned(), data.to_vec()),
            ],
            RaftRecordKey::ReplicasInfoManagerState,
        )
        .await
    }

    async fn read_bytes(&self, key: RaftRecordKey) -> Result<Option<Vec<u8>>, std::io::Error> {
        self.backend
            .get(key.as_v1_key().as_ref())
            .await
            .map_err(|error| backend_error("read", key, error))
    }

    async fn read_json<T: serde::de::DeserializeOwned>(&self, key: RaftRecordKey) -> Result<Option<T>, std::io::Error> {
        self.read_bytes(key)
            .await?
            .map(|bytes| decode_v1(key, &bytes))
            .transpose()
    }

    async fn write_batch_and_sync(
        &self,
        puts: Vec<(String, Vec<u8>)>,
        key: RaftRecordKey,
    ) -> Result<(), std::io::Error> {
        self.backend
            .write_batch(puts, Vec::new())
            .await
            .map_err(|error| backend_error("write batch", key, error))?;
        self.backend
            .sync()
            .await
            .map_err(|error| backend_error("sync", key, error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_meta_v1_bytes_match_the_existing_record() {
        let meta = PersistedSnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
            snapshot_id: "snapshot-0".to_string(),
        };

        let bytes = encode_v1(RaftRecordKey::SnapshotMeta, &meta).expect("encode snapshot metadata");

        assert_eq!(
            String::from_utf8(bytes).expect("UTF-8 snapshot metadata"),
            r#"{"last_log_id":null,"last_membership":{"log_id":null,"membership":{"configs":[],"nodes":{}}},"snapshot_id":"snapshot-0"}"#
        );
    }
}
