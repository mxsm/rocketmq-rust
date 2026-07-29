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

//! Raft log storage implementation
//!
//! This module provides the log storage layer for OpenRaft, handling:
//! - Log entry persistence
//! - Vote information storage
//! - Log compaction and purging

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use dashmap::DashMap;
use openraft::storage::RaftLogStorage;
use openraft::LogState;
use openraft::OptionalSend;
use openraft::RaftLogReader;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

use super::persistence::RaftLogRepository;
use crate::storage::SharedStorageBackend;
use crate::typ::LogEntry;
use crate::typ::LogId;
use crate::typ::TypeConfig;
use crate::typ::Vote;

/// Durable Raft log view backed by the configured controller storage.
#[derive(Clone)]
pub struct LogStore {
    /// Log entries indexed by log index
    logs: Arc<DashMap<u64, LogEntry>>,
    /// Last purged log ID
    last_purged_log_id: Arc<RwLock<Option<LogId>>>,
    /// Committed log ID
    committed: Arc<RwLock<Option<LogId>>>,
    /// Current vote information
    vote: Arc<RwLock<Option<Vote>>>,
    repository: Option<RaftLogRepository>,
    /// Serializes vote, log, commit-index, truncate, and purge writes across all clones.
    write_lock: Arc<Mutex<()>>,
}

impl Default for LogStore {
    fn default() -> Self {
        Self::new()
    }
}

impl LogStore {
    /// Create a new log store
    pub fn new() -> Self {
        Self {
            logs: Arc::new(DashMap::new()),
            last_purged_log_id: Arc::new(RwLock::new(None)),
            committed: Arc::new(RwLock::new(None)),
            vote: Arc::new(RwLock::new(None)),
            repository: None,
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn open(backend: SharedStorageBackend) -> Result<Self, std::io::Error> {
        let repository = RaftLogRepository::new(backend);
        let loaded = repository.load().await?;
        let store = Self {
            logs: Arc::new(DashMap::new()),
            last_purged_log_id: Arc::new(RwLock::new(loaded.last_purged)),
            committed: Arc::new(RwLock::new(loaded.committed)),
            vote: Arc::new(RwLock::new(loaded.vote)),
            repository: Some(repository),
            write_lock: Arc::new(Mutex::new(())),
        };

        let mut previous_index = store.last_purged_log_id.read().await.map(|log_id| log_id.index);
        for (key_index, entry) in loaded.entries {
            if entry.log_id.index != key_index {
                return Err(invalid_log_data(format!(
                    "persisted log key index {key_index} does not match entry index {}",
                    entry.log_id.index
                )));
            }
            if let Some(previous_index) = previous_index {
                let expected = previous_index
                    .checked_add(1)
                    .ok_or_else(|| invalid_log_data("persisted Raft log index overflow"))?;
                if key_index != expected {
                    return Err(invalid_log_data(format!(
                        "persisted Raft log is not contiguous: expected {expected}, found {key_index}"
                    )));
                }
            } else if key_index != 0 {
                return Err(invalid_log_data(format!(
                    "persisted Raft log starts at {key_index} without a purge boundary"
                )));
            }
            if store.logs.insert(entry.log_id.index, entry).is_some() {
                return Err(invalid_log_data(format!(
                    "duplicate persisted Raft log index {key_index}"
                )));
            }
            previous_index = Some(key_index);
        }

        if let Some(committed) = *store.committed.read().await {
            let durable_last = store
                .last_log_id()
                .await
                .or(*store.last_purged_log_id.read().await)
                .ok_or_else(|| invalid_log_data("committed index exists without durable Raft logs"))?;
            if committed.index > durable_last.index {
                return Err(invalid_log_data(format!(
                    "committed index {} is ahead of durable log index {}",
                    committed.index, durable_last.index
                )));
            }
        }

        Ok(store)
    }

    /// Get the last log ID
    async fn last_log_id(&self) -> Option<LogId> {
        self.logs.iter().map(|entry| entry.value().log_id).max()
    }

    /// Get logs in the specified range
    fn get_log_entries<R: RangeBounds<u64>>(&self, range: R) -> Vec<LogEntry> {
        use std::ops::Bound;

        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };

        let mut entries = Vec::new();
        let mut index = start;
        loop {
            match range.end_bound() {
                Bound::Included(&n) if index > n => break,
                Bound::Excluded(&n) if index >= n => break,
                Bound::Unbounded => {}
                _ => {}
            }

            if let Some(entry) = self.logs.get(&index) {
                entries.push(entry.value().clone());
                index += 1;
            } else {
                break;
            }
        }
        entries
    }
}

fn invalid_log_data(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<LogEntry>, std::io::Error> {
        Ok(self.get_log_entries(range))
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, std::io::Error> {
        Ok(*self.vote.read().await)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, std::io::Error> {
        let last_purged = *self.last_purged_log_id.read().await;
        let last_log_id = self.last_log_id().await.or(last_purged);

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), std::io::Error> {
        let _write_guard = self.write_lock.lock().await;
        if let Some(repository) = &self.repository {
            repository.save_vote(vote).await?;
        }
        *self.vote.write().await = Some(*vote);
        Ok(())
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), std::io::Error> {
        let _write_guard = self.write_lock.lock().await;
        if let Some(committed) = committed {
            let durable_last = self
                .last_log_id()
                .await
                .or(*self.last_purged_log_id.read().await)
                .ok_or_else(|| invalid_log_data("cannot commit without a durable Raft log"))?;
            if committed.index > durable_last.index {
                return Err(invalid_log_data(format!(
                    "cannot commit index {} beyond durable log index {}",
                    committed.index, durable_last.index
                )));
            }
        }
        if let Some(repository) = &self.repository {
            repository.save_committed(&committed).await?;
        }
        *self.committed.write().await = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, std::io::Error> {
        Ok(*self.committed.read().await)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::IOFlushed<TypeConfig>,
    ) -> Result<(), std::io::Error>
    where
        I: IntoIterator<Item = LogEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let _write_guard = self.write_lock.lock().await;
        let entries = entries.into_iter().collect::<Vec<_>>();
        let mut expected_index = match self.last_log_id().await.or(*self.last_purged_log_id.read().await) {
            Some(log_id) => match log_id.index.checked_add(1) {
                Some(index) => index,
                None => {
                    let error = invalid_log_data("Raft append index overflow");
                    callback.io_completed(Err(std::io::Error::new(error.kind(), error.to_string())));
                    return Err(error);
                }
            },
            None => 0,
        };
        for entry in &entries {
            if entry.log_id.index != expected_index {
                callback.io_completed(Err(invalid_log_data(format!(
                    "Raft append is not contiguous: expected {expected_index}, found {}",
                    entry.log_id.index
                ))));
                return Err(invalid_log_data(format!(
                    "Raft append is not contiguous: expected {expected_index}, found {}",
                    entry.log_id.index
                )));
            }
            expected_index = match expected_index.checked_add(1) {
                Some(index) => index,
                None => {
                    let error = invalid_log_data("Raft append index overflow");
                    callback.io_completed(Err(std::io::Error::new(error.kind(), error.to_string())));
                    return Err(error);
                }
            };
        }
        if let Some(repository) = &self.repository {
            if let Err(error) = repository.append(&entries).await {
                callback.io_completed(Err(std::io::Error::new(error.kind(), error.to_string())));
                return Err(error);
            }
        }
        for entry in entries {
            self.logs.insert(entry.log_id.index, entry);
        }
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(&mut self, log_id: Option<LogId>) -> Result<(), std::io::Error> {
        let _write_guard = self.write_lock.lock().await;
        if let (Some(log_id), Some(last_purged)) = (log_id, *self.last_purged_log_id.read().await) {
            if log_id.index < last_purged.index {
                return Err(invalid_log_data(format!(
                    "cannot truncate after index {} below purge boundary {}",
                    log_id.index, last_purged.index
                )));
            }
        }
        // Remove all logs with index > log_id.index
        if let Some(log_id) = log_id {
            let keys_to_remove: Vec<u64> = self
                .logs
                .iter()
                .filter_map(|entry| {
                    if entry.key() > &log_id.index {
                        Some(*entry.key())
                    } else {
                        None
                    }
                })
                .collect();

            if let Some(repository) = &self.repository {
                repository.truncate(&keys_to_remove).await?;
            }

            for key in keys_to_remove {
                self.logs.remove(&key);
            }
        } else {
            // If log_id is None, remove all logs
            if let Some(repository) = &self.repository {
                let keys_to_remove: Vec<u64> = self.logs.iter().map(|entry| *entry.key()).collect();
                repository.truncate(&keys_to_remove).await?;
            }
            self.logs.clear();
        }

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), std::io::Error> {
        let _write_guard = self.write_lock.lock().await;
        if self
            .last_purged_log_id
            .read()
            .await
            .is_some_and(|last_purged| log_id.index < last_purged.index)
        {
            return Err(invalid_log_data("Raft purge boundary cannot move backwards"));
        }
        // Remove all logs with index <= log_id.index
        let keys_to_remove: Vec<u64> = self
            .logs
            .iter()
            .filter_map(|entry| {
                if entry.key() <= &log_id.index {
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect();

        if let Some(repository) = &self.repository {
            repository.purge(&log_id, &keys_to_remove).await?;
        }

        for key in keys_to_remove {
            self.logs.remove(&key);
        }

        *self.last_purged_log_id.write().await = Some(log_id);
        Ok(())
    }
}
