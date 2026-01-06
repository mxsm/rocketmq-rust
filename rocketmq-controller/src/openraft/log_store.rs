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
use tokio::sync::RwLock;

use crate::typ::LogEntry;
use crate::typ::LogId;
use crate::typ::TypeConfig;
use crate::typ::Vote;

/// In-memory log store for Raft
///
/// This implementation stores all log entries in memory using DashMap.
/// For production use, consider implementing persistent storage.
#[derive(Debug, Clone)]
pub struct LogStore {
    /// Log entries indexed by log index
    logs: Arc<DashMap<u64, LogEntry>>,
    /// Last purged log ID
    last_purged_log_id: Arc<RwLock<Option<LogId>>>,
    /// Committed log ID
    committed: Arc<RwLock<Option<LogId>>>,
    /// Current vote information
    vote: Arc<RwLock<Option<Vote>>>,
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
        }
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
        let last_log_id = self.last_log_id().await;

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), std::io::Error> {
        *self.vote.write().await = Some(*vote);
        Ok(())
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
        for entry in entries {
            let log_id = entry.log_id;
            self.logs.insert(log_id.index, entry);
        }
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(&mut self, log_id: Option<LogId>) -> Result<(), std::io::Error> {
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

            for key in keys_to_remove {
                self.logs.remove(&key);
            }
        } else {
            // If log_id is None, remove all logs
            self.logs.clear();
        }

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), std::io::Error> {
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

        for key in keys_to_remove {
            self.logs.remove(&key);
        }

        *self.last_purged_log_id.write().await = Some(log_id);
        Ok(())
    }
}
