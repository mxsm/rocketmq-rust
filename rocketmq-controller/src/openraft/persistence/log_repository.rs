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
use crate::typ::LogEntry;
use crate::typ::LogId;
use crate::typ::Vote;

use super::backend_error;
use super::decode_v1;
use super::encode_v1;
use super::RaftRecordKey;

pub(in crate::openraft) struct LoadedLogRepository {
    pub(in crate::openraft) vote: Option<Vote>,
    pub(in crate::openraft) committed: Option<LogId>,
    pub(in crate::openraft) last_purged: Option<LogId>,
    pub(in crate::openraft) entries: Vec<(u64, LogEntry)>,
}

#[derive(Clone)]
pub(in crate::openraft) struct RaftLogRepository {
    backend: SharedStorageBackend,
}

impl RaftLogRepository {
    pub(in crate::openraft) fn new(backend: SharedStorageBackend) -> Self {
        Self { backend }
    }

    pub(in crate::openraft) async fn load(&self) -> Result<LoadedLogRepository, std::io::Error> {
        let vote = self.load_json(RaftRecordKey::Vote).await?;
        let committed = self.load_json(RaftRecordKey::Committed).await?;
        let last_purged = self.load_json(RaftRecordKey::LastPurgedLog).await?;
        let mut keys = self
            .backend
            .list_keys(RaftRecordKey::log_prefix_v1())
            .await
            .map_err(|error| backend_error("list", RaftRecordKey::LogEntry(0), error))?;
        keys.sort_unstable();

        let mut entries = Vec::with_capacity(keys.len());
        for key in keys {
            let index = RaftRecordKey::parse_v1_log_key(&key)?;
            let record_key = RaftRecordKey::LogEntry(index);
            let Some(bytes) = self
                .backend
                .get(&key)
                .await
                .map_err(|error| backend_error("read", record_key, error))?
            else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "persisted Controller log-entry key has no value",
                ));
            };
            entries.push((index, decode_v1(record_key, &bytes)?));
        }

        Ok(LoadedLogRepository {
            vote,
            committed,
            last_purged,
            entries,
        })
    }

    pub(in crate::openraft) async fn save_vote(&self, vote: &Vote) -> Result<(), std::io::Error> {
        self.write_json(RaftRecordKey::Vote, vote).await
    }

    pub(in crate::openraft) async fn save_committed(&self, committed: &Option<LogId>) -> Result<(), std::io::Error> {
        self.write_json(RaftRecordKey::Committed, committed).await
    }

    pub(in crate::openraft) async fn append(&self, entries: &[LogEntry]) -> Result<(), std::io::Error> {
        let mut puts = Vec::with_capacity(entries.len());
        for entry in entries {
            let key = RaftRecordKey::LogEntry(entry.log_id.index);
            puts.push((key.as_v1_key().into_owned(), encode_v1(key, entry)?));
        }
        self.write_batch_and_sync(
            puts,
            Vec::new(),
            RaftRecordKey::LogEntry(entries.first().map_or(0, |entry| entry.log_id.index)),
        )
        .await
    }

    pub(in crate::openraft) async fn truncate(&self, indices: &[u64]) -> Result<(), std::io::Error> {
        let deletes = indices
            .iter()
            .map(|index| RaftRecordKey::LogEntry(*index).as_v1_key().into_owned())
            .collect();
        self.write_batch_and_sync(
            Vec::new(),
            deletes,
            RaftRecordKey::LogEntry(indices.first().copied().unwrap_or(0)),
        )
        .await
    }

    pub(in crate::openraft) async fn purge(&self, log_id: &LogId, indices: &[u64]) -> Result<(), std::io::Error> {
        let key = RaftRecordKey::LastPurgedLog;
        let deletes = indices
            .iter()
            .map(|index| RaftRecordKey::LogEntry(*index).as_v1_key().into_owned())
            .collect();
        self.write_batch_and_sync(
            vec![(key.as_v1_key().into_owned(), encode_v1(key, log_id)?)],
            deletes,
            key,
        )
        .await
    }

    async fn load_json<T: serde::de::DeserializeOwned>(&self, key: RaftRecordKey) -> Result<Option<T>, std::io::Error> {
        let Some(bytes) = self
            .backend
            .get(key.as_v1_key().as_ref())
            .await
            .map_err(|error| backend_error("read", key, error))?
        else {
            return Ok(None);
        };
        decode_v1(key, &bytes).map(Some)
    }

    async fn write_json<T: serde::Serialize>(&self, key: RaftRecordKey, value: &T) -> Result<(), std::io::Error> {
        self.write_batch_and_sync(
            vec![(key.as_v1_key().into_owned(), encode_v1(key, value)?)],
            Vec::new(),
            key,
        )
        .await
    }

    async fn write_batch_and_sync(
        &self,
        puts: Vec<(String, Vec<u8>)>,
        deletes: Vec<String>,
        key: RaftRecordKey,
    ) -> Result<(), std::io::Error> {
        if puts.is_empty() && deletes.is_empty() {
            return Ok(());
        }
        self.backend
            .write_batch(puts, deletes)
            .await
            .map_err(|error| backend_error("write batch", key, error))?;
        self.backend
            .sync()
            .await
            .map_err(|error| backend_error("sync", key, error))
    }
}
