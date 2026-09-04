// Copyright 2026 The RocketMQ Rust Authors
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

use rocketmq_store_api::StoreError;

use crate::config::RocksDbConfig;

/// Shared native-memory ownership for a group of RocksDB databases.
///
/// Every database and column family opened with this value shares one block
/// cache and one write-buffer manager. The write-buffer manager stalls writers
/// at its configured limit instead of allowing independent memtables to grow
/// without a process-level bound.
pub struct RocksDbResourceBudget {
    block_cache: ::rocksdb::Cache,
    write_buffer_manager: ::rocksdb::WriteBufferManager,
    block_cache_budget_bytes: usize,
    write_buffer_budget_bytes: usize,
}

impl RocksDbResourceBudget {
    /// Creates a shared RocksDB native-memory budget.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when either budget is zero.
    pub fn new(block_cache_budget_bytes: usize, write_buffer_budget_bytes: usize) -> Result<Self, StoreError> {
        if block_cache_budget_bytes == 0 {
            return Err(crate::error::request_invalid(rocketmq_store_api::StoreOperation::Load));
        }
        if write_buffer_budget_bytes == 0 {
            return Err(crate::error::request_invalid(rocketmq_store_api::StoreOperation::Load));
        }

        let block_cache = ::rocksdb::Cache::new_lru_cache(block_cache_budget_bytes);
        let write_buffer_manager =
            ::rocksdb::WriteBufferManager::new_write_buffer_manager(write_buffer_budget_bytes, true);
        Ok(Self {
            block_cache,
            write_buffer_manager,
            block_cache_budget_bytes,
            write_buffer_budget_bytes,
        })
    }

    pub(crate) fn from_config(config: &RocksDbConfig) -> Result<Self, StoreError> {
        Self::new(config.block_cache_budget_bytes, config.write_buffer_budget_bytes)
    }

    pub(crate) const fn block_cache(&self) -> &::rocksdb::Cache {
        &self.block_cache
    }

    pub(crate) const fn write_buffer_manager(&self) -> &::rocksdb::WriteBufferManager {
        &self.write_buffer_manager
    }

    /// Returns the configured shared block-cache limit.
    pub const fn block_cache_budget_bytes(&self) -> usize {
        self.block_cache_budget_bytes
    }

    /// Returns the configured shared memtable limit.
    pub const fn write_buffer_budget_bytes(&self) -> usize {
        self.write_buffer_budget_bytes
    }

    /// Returns current native block-cache usage.
    pub fn block_cache_usage_bytes(&self) -> usize {
        self.block_cache.get_usage()
    }

    /// Returns current native memtable usage tracked by RocksDB.
    pub fn write_buffer_usage_bytes(&self) -> usize {
        self.write_buffer_manager.get_usage()
    }
}
