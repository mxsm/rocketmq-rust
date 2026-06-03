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

use std::path::PathBuf;

use rocketmq_error::RocketMQError;

use crate::config::message_store_config::MessageStoreConfig;
use crate::rocksdb::options::RocksDbWriteProfile;
use crate::store_path_config_helper::get_store_path_consume_queue;

const ROCKSDB_MESSAGE_DIRECTORY: &str = "rocksdbstore";

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum RocksDbCompressionType {
    None,
    Snappy,
    #[default]
    Lz4,
    Zstd,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum RocksDbCompactionStyle {
    Level,
    #[default]
    Universal,
    Fifo,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RocksDbColumnFamilyConfig {
    pub name: String,
    pub write_buffer_size: usize,
    pub max_write_buffer_number: i32,
    pub block_cache_size: usize,
    pub block_size: usize,
    pub bloom_filter_bits: f64,
    pub compression_type: RocksDbCompressionType,
    pub bottommost_compression_type: RocksDbCompressionType,
    pub compaction_style: RocksDbCompactionStyle,
}

impl RocksDbColumnFamilyConfig {
    pub fn consume_queue_default() -> Self {
        Self {
            name: "default".to_string(),
            write_buffer_size: 128 * 1024 * 1024,
            max_write_buffer_number: 4,
            block_cache_size: 1024 * 1024 * 1024,
            block_size: 32 * 1024,
            bloom_filter_bits: 16.0,
            compression_type: RocksDbCompressionType::Lz4,
            bottommost_compression_type: RocksDbCompressionType::Lz4,
            compaction_style: RocksDbCompactionStyle::Universal,
        }
    }

    pub fn consume_queue_offset() -> Self {
        Self {
            name: "offset".to_string(),
            write_buffer_size: 64 * 1024 * 1024,
            max_write_buffer_number: 4,
            block_cache_size: 128 * 1024 * 1024,
            block_size: 32 * 1024,
            bloom_filter_bits: 16.0,
            compression_type: RocksDbCompressionType::None,
            bottommost_compression_type: RocksDbCompressionType::None,
            compaction_style: RocksDbCompactionStyle::Level,
        }
    }

    pub fn message_index_default() -> Self {
        Self {
            name: "default".to_string(),
            ..Self::consume_queue_default()
        }
    }

    pub fn message_timer() -> Self {
        Self {
            name: "timer".to_string(),
            write_buffer_size: 128 * 1024 * 1024,
            max_write_buffer_number: 4,
            block_cache_size: 512 * 1024 * 1024,
            block_size: 32 * 1024,
            bloom_filter_bits: 16.0,
            compression_type: RocksDbCompressionType::Lz4,
            bottommost_compression_type: RocksDbCompressionType::Lz4,
            compaction_style: RocksDbCompactionStyle::Universal,
        }
    }

    pub fn message_transaction() -> Self {
        Self {
            name: "trans".to_string(),
            write_buffer_size: 64 * 1024 * 1024,
            max_write_buffer_number: 4,
            block_cache_size: 256 * 1024 * 1024,
            block_size: 32 * 1024,
            bloom_filter_bits: 16.0,
            compression_type: RocksDbCompressionType::Lz4,
            bottommost_compression_type: RocksDbCompressionType::Lz4,
            compaction_style: RocksDbCompactionStyle::Level,
        }
    }

    pub fn validate(&self) -> Result<(), RocketMQError> {
        if self.name.is_empty() {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.column_family.name",
                value: self.name.clone(),
                reason: "column family name must not be empty".to_string(),
            });
        }
        if self.write_buffer_size == 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.column_family.write_buffer_size",
                value: self.write_buffer_size.to_string(),
                reason: "write buffer size must be greater than zero".to_string(),
            });
        }
        if self.block_size == 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.column_family.block_size",
                value: self.block_size.to_string(),
                reason: "block size must be greater than zero".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RocksDbConfig {
    pub enabled: bool,
    pub path: PathBuf,
    pub wal_enabled: bool,
    pub sync_write: bool,
    pub manual_wal_flush: bool,
    pub write_buffer_size: usize,
    pub max_write_buffer_number: i32,
    pub block_cache_size: usize,
    pub block_size: usize,
    pub bloom_filter_bits: f64,
    pub compression_type: RocksDbCompressionType,
    pub bottommost_compression_type: RocksDbCompressionType,
    pub compaction_style: RocksDbCompactionStyle,
    pub max_background_jobs: i32,
    pub max_subcompactions: u32,
    pub max_open_files: i32,
    pub create_missing_column_families: bool,
    pub statistics_enabled: bool,
    pub flush_interval_ms: usize,
    pub compaction_interval_ms: usize,
    pub checkpoint_interval_ms: usize,
    pub backup_interval_ms: usize,
    pub backup_dir: Option<PathBuf>,
    pub column_families: Vec<RocksDbColumnFamilyConfig>,
}

impl Default for RocksDbConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: PathBuf::from("store/rocksdb"),
            wal_enabled: true,
            sync_write: false,
            manual_wal_flush: true,
            write_buffer_size: 128 * 1024 * 1024,
            max_write_buffer_number: 4,
            block_cache_size: 1024 * 1024 * 1024,
            block_size: 32 * 1024,
            bloom_filter_bits: 16.0,
            compression_type: RocksDbCompressionType::Lz4,
            bottommost_compression_type: RocksDbCompressionType::Lz4,
            compaction_style: RocksDbCompactionStyle::Universal,
            max_background_jobs: 32,
            max_subcompactions: 8,
            max_open_files: -1,
            create_missing_column_families: true,
            statistics_enabled: true,
            flush_interval_ms: 0,
            compaction_interval_ms: 0,
            checkpoint_interval_ms: 0,
            backup_interval_ms: 0,
            backup_dir: None,
            column_families: vec![
                RocksDbColumnFamilyConfig::consume_queue_default(),
                RocksDbColumnFamilyConfig::consume_queue_offset(),
            ],
        }
    }
}

impl RocksDbConfig {
    pub fn consume_queue_from_message_store_config(message_store_config: &MessageStoreConfig) -> Self {
        Self {
            enabled: message_store_config.is_enable_rocksdb_store(),
            path: PathBuf::from(get_store_path_consume_queue(
                message_store_config.store_path_root_dir.as_str(),
            )),
            wal_enabled: false,
            sync_write: false,
            column_families: vec![
                RocksDbColumnFamilyConfig::consume_queue_default(),
                RocksDbColumnFamilyConfig::consume_queue_offset(),
            ],
            ..Self::operational_from_message_store_config(message_store_config)
        }
    }

    pub fn message_from_message_store_config(message_store_config: &MessageStoreConfig) -> Self {
        Self {
            enabled: message_store_config.is_enable_rocksdb_store(),
            path: PathBuf::from(message_store_config.store_path_root_dir.as_str()).join(ROCKSDB_MESSAGE_DIRECTORY),
            wal_enabled: false,
            sync_write: false,
            column_families: vec![
                RocksDbColumnFamilyConfig::message_index_default(),
                RocksDbColumnFamilyConfig::message_timer(),
                RocksDbColumnFamilyConfig::message_transaction(),
            ],
            ..Self::operational_from_message_store_config(message_store_config)
        }
    }

    fn operational_from_message_store_config(message_store_config: &MessageStoreConfig) -> Self {
        Self {
            flush_interval_ms: message_store_config.mem_table_flush_interval_ms,
            compaction_interval_ms: message_store_config
                .clean_rocksdb_dirty_cq_interval_min
                .saturating_mul(60 * 1000),
            checkpoint_interval_ms: message_store_config.rocksdb_checkpoint_interval_ms,
            backup_interval_ms: message_store_config.rocksdb_backup_interval_ms,
            backup_dir: message_store_config
                .rocksdb_backup_dir
                .as_ref()
                .map(|path| PathBuf::from(path.as_str())),
            ..Self::default()
        }
    }

    pub fn validate(&self) -> Result<(), RocketMQError> {
        if self.path.as_os_str().is_empty() {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.path",
                value: String::new(),
                reason: "path must not be empty".to_string(),
            });
        }
        if self.write_buffer_size == 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.write_buffer_size",
                value: self.write_buffer_size.to_string(),
                reason: "write buffer size must be greater than zero".to_string(),
            });
        }
        for column_family in &self.column_families {
            column_family.validate()?;
        }
        Ok(())
    }

    pub fn write_profile(&self) -> RocksDbWriteProfile {
        match (self.wal_enabled, self.sync_write) {
            (false, _) => RocksDbWriteProfile::DisableWal,
            (true, false) => RocksDbWriteProfile::Wal,
            (true, true) => RocksDbWriteProfile::SyncWal,
        }
    }
}
