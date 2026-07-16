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

use crate::options::RocksDbWriteProfile;

pub trait RocksDbConfigSource {
    fn store_path_root_dir(&self) -> &str;

    fn rocksdb_store_enabled(&self) -> bool;

    fn use_separate_store_path_for_rocksdb_cq(&self) -> bool;

    fn mem_table_flush_interval_ms(&self) -> usize;

    fn clean_rocksdb_dirty_cq_interval_min(&self) -> usize;

    fn rocksdb_checkpoint_interval_ms(&self) -> usize;

    fn rocksdb_backup_interval_ms(&self) -> usize;

    fn rocksdb_backup_dir(&self) -> Option<&str>;
}

const ROCKSDB_MESSAGE_DIRECTORY: &str = "rocksdbstore";
const KB: usize = 1024;
const MB: usize = 1024 * KB;
const GB: usize = 1024 * MB;
const KB_U64: u64 = 1024;
const MB_U64: u64 = 1024 * KB_U64;
const GB_U64: u64 = 1024 * MB_U64;

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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum RocksDbWalRecoveryMode {
    TolerateCorruptedTailRecords,
    AbsoluteConsistency,
    #[default]
    PointInTime,
    SkipAnyCorruptedRecord,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum RocksDbBlockBasedIndexType {
    #[default]
    BinarySearch,
    HashSearch,
    TwoLevelIndexSearch,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum RocksDbDataBlockIndexType {
    #[default]
    BinarySearch,
    BinaryAndHash,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RocksDbColumnFamilyConfig {
    pub name: String,
    pub write_buffer_size: usize,
    pub max_write_buffer_number: i32,
    pub block_cache_size: usize,
    pub block_size: usize,
    pub metadata_block_size: Option<usize>,
    pub bloom_filter_bits: f64,
    pub format_version: i32,
    pub index_type: RocksDbBlockBasedIndexType,
    pub data_block_index_type: RocksDbDataBlockIndexType,
    pub data_block_hash_ratio: Option<f64>,
    pub whole_key_filtering: bool,
    pub compression_type: RocksDbCompressionType,
    pub bottommost_compression_type: RocksDbCompressionType,
    pub compaction_style: RocksDbCompactionStyle,
    pub min_write_buffer_number_to_merge: i32,
    pub num_levels: i32,
    pub level_zero_file_num_compaction_trigger: i32,
    pub level_zero_slowdown_writes_trigger: i32,
    pub level_zero_stop_writes_trigger: i32,
    pub target_file_size_base: u64,
    pub target_file_size_multiplier: i32,
    pub max_bytes_for_level_base: Option<u64>,
    pub max_bytes_for_level_multiplier: Option<f64>,
    pub max_compaction_bytes: Option<u64>,
    pub soft_pending_compaction_bytes_limit: Option<usize>,
    pub hard_pending_compaction_bytes_limit: Option<usize>,
    pub report_bg_io_stats: bool,
    pub optimize_filters_for_hits: bool,
    pub optimize_filters_for_memory: bool,
    pub cache_index_and_filter_blocks: bool,
    pub pin_l0_filter_and_index_blocks_in_cache: bool,
    pub pin_top_level_index_and_filter: bool,
    pub inplace_update_support: bool,
}

impl RocksDbColumnFamilyConfig {
    pub fn consume_queue_default() -> Self {
        let mut config = Self::java_cf_base(
            "default",
            128 * MB,
            4,
            1024 * MB,
            32 * KB,
            RocksDbCompressionType::Lz4,
            RocksDbCompressionType::Lz4,
            RocksDbCompactionStyle::Universal,
        );
        config.max_compaction_bytes = Some(100 * GB_U64);
        config.soft_pending_compaction_bytes_limit = Some(100 * GB);
        config.hard_pending_compaction_bytes_limit = Some(256 * GB);
        config.use_data_block_hash_index();
        config.report_bg_io_stats = true;
        config.optimize_filters_for_hits = true;
        config
    }

    pub fn consume_queue_offset() -> Self {
        let mut config = Self::java_cf_base(
            "offset",
            64 * MB,
            4,
            128 * MB,
            32 * KB,
            RocksDbCompressionType::None,
            RocksDbCompressionType::None,
            RocksDbCompactionStyle::Level,
        );
        config.target_file_size_base = 64 * MB_U64;
        config.max_bytes_for_level_base = Some(256 * MB_U64);
        config.max_bytes_for_level_multiplier = Some(2.0);
        config.metadata_block_size = None;
        config.inplace_update_support = true;
        config
    }

    pub fn message_index_default() -> Self {
        let mut config = Self::java_cf_base(
            "default",
            128 * MB,
            6,
            1024 * MB,
            128 * KB,
            RocksDbCompressionType::None,
            RocksDbCompressionType::None,
            RocksDbCompactionStyle::Universal,
        );
        config.max_compaction_bytes = Some(256 * MB_U64);
        config.soft_pending_compaction_bytes_limit = Some(100 * GB);
        config.hard_pending_compaction_bytes_limit = Some(256 * GB);
        config.level_zero_file_num_compaction_trigger = 8;
        config.level_zero_slowdown_writes_trigger = 8;
        config.level_zero_stop_writes_trigger = 20;
        config.use_data_block_hash_index();
        config.report_bg_io_stats = true;
        config.optimize_filters_for_hits = true;
        config
    }

    pub fn message_timer() -> Self {
        let mut config = Self::java_cf_base(
            "timer",
            256 * MB,
            6,
            2048 * MB,
            128 * KB,
            RocksDbCompressionType::Zstd,
            RocksDbCompressionType::None,
            RocksDbCompactionStyle::Level,
        );
        config.max_compaction_bytes = Some(256 * MB_U64);
        config.soft_pending_compaction_bytes_limit = Some(100 * GB);
        config.hard_pending_compaction_bytes_limit = Some(256 * GB);
        config.max_bytes_for_level_base = Some(512 * MB_U64);
        config.use_data_block_hash_index();
        config.report_bg_io_stats = true;
        config.optimize_filters_for_hits = true;
        config
    }

    pub fn message_transaction() -> Self {
        let mut config = Self::java_cf_base(
            "trans",
            128 * MB,
            6,
            1024 * MB,
            128 * KB,
            RocksDbCompressionType::None,
            RocksDbCompressionType::None,
            RocksDbCompactionStyle::Universal,
        );
        config.max_compaction_bytes = Some(100 * GB_U64);
        config.soft_pending_compaction_bytes_limit = Some(100 * GB);
        config.hard_pending_compaction_bytes_limit = Some(256 * GB);
        config.use_data_block_hash_index();
        config.report_bg_io_stats = true;
        config.optimize_filters_for_hits = true;
        config
    }

    pub fn pop(name: &str, block_cache_size: usize, write_buffer_size: usize) -> Self {
        let mut config = Self::java_cf_base(
            name,
            write_buffer_size,
            4,
            block_cache_size,
            32 * KB,
            RocksDbCompressionType::None,
            RocksDbCompressionType::None,
            RocksDbCompactionStyle::Universal,
        );
        config.max_compaction_bytes = Some(100 * GB_U64);
        config.soft_pending_compaction_bytes_limit = Some(100 * GB);
        config.hard_pending_compaction_bytes_limit = Some(256 * GB);
        config.use_data_block_hash_index();
        config.report_bg_io_stats = true;
        config.optimize_filters_for_hits = true;
        config
    }

    pub fn broker_config(name: &str, block_cache_size: usize, write_buffer_size: usize) -> Self {
        let mut config = Self::java_cf_base(
            name,
            write_buffer_size,
            4,
            block_cache_size,
            32 * KB,
            RocksDbCompressionType::None,
            RocksDbCompressionType::None,
            RocksDbCompactionStyle::Level,
        );
        config.level_zero_file_num_compaction_trigger = 4;
        config.level_zero_slowdown_writes_trigger = 8;
        config.level_zero_stop_writes_trigger = 12;
        config.target_file_size_base = 64 * MB_U64;
        config.max_bytes_for_level_base = Some(256 * MB_U64);
        config.max_bytes_for_level_multiplier = Some(2.0);
        config.metadata_block_size = None;
        config.cache_index_and_filter_blocks = true;
        config.inplace_update_support = true;
        config
    }

    fn use_data_block_hash_index(&mut self) {
        self.data_block_index_type = RocksDbDataBlockIndexType::BinaryAndHash;
        self.data_block_hash_ratio = Some(0.75);
    }

    fn java_cf_base(
        name: &str,
        write_buffer_size: usize,
        max_write_buffer_number: i32,
        block_cache_size: usize,
        block_size: usize,
        compression_type: RocksDbCompressionType,
        bottommost_compression_type: RocksDbCompressionType,
        compaction_style: RocksDbCompactionStyle,
    ) -> Self {
        Self {
            name: name.to_string(),
            write_buffer_size,
            max_write_buffer_number,
            block_cache_size,
            block_size,
            metadata_block_size: Some(4 * KB),
            bloom_filter_bits: 16.0,
            format_version: 5,
            index_type: RocksDbBlockBasedIndexType::BinarySearch,
            data_block_index_type: RocksDbDataBlockIndexType::BinarySearch,
            data_block_hash_ratio: None,
            whole_key_filtering: true,
            compression_type,
            bottommost_compression_type,
            compaction_style,
            min_write_buffer_number_to_merge: 1,
            num_levels: 7,
            level_zero_file_num_compaction_trigger: 2,
            level_zero_slowdown_writes_trigger: 8,
            level_zero_stop_writes_trigger: 10,
            target_file_size_base: 256 * MB_U64,
            target_file_size_multiplier: 2,
            max_bytes_for_level_base: None,
            max_bytes_for_level_multiplier: None,
            max_compaction_bytes: None,
            soft_pending_compaction_bytes_limit: None,
            hard_pending_compaction_bytes_limit: None,
            report_bg_io_stats: false,
            optimize_filters_for_hits: false,
            optimize_filters_for_memory: false,
            cache_index_and_filter_blocks: false,
            pin_l0_filter_and_index_blocks_in_cache: false,
            pin_top_level_index_and_filter: true,
            inplace_update_support: false,
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
    pub db_write_buffer_size: Option<usize>,
    pub bytes_per_sync: u64,
    pub wal_bytes_per_sync: Option<u64>,
    pub max_log_file_size: usize,
    pub keep_log_file_num: usize,
    pub max_manifest_file_size: usize,
    pub allow_concurrent_memtable_write: bool,
    pub paranoid_checks: bool,
    pub compaction_readahead_size: usize,
    pub use_direct_io_for_flush_and_compaction: bool,
    pub use_direct_reads: bool,
    pub wal_recovery_mode: RocksDbWalRecoveryMode,
    pub stats_dump_period_sec: u32,
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
            db_write_buffer_size: None,
            bytes_per_sync: MB_U64,
            wal_bytes_per_sync: None,
            max_log_file_size: GB,
            keep_log_file_num: 5,
            max_manifest_file_size: GB,
            allow_concurrent_memtable_write: false,
            paranoid_checks: true,
            compaction_readahead_size: 4 * MB,
            use_direct_io_for_flush_and_compaction: false,
            use_direct_reads: false,
            wal_recovery_mode: RocksDbWalRecoveryMode::PointInTime,
            stats_dump_period_sec: 0,
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
    pub fn consume_queue_path_from_message_store_config<S>(message_store_config: &S) -> PathBuf
    where
        S: RocksDbConfigSource + ?Sized,
    {
        let root_dir = PathBuf::from(message_store_config.store_path_root_dir());
        if message_store_config.use_separate_store_path_for_rocksdb_cq() {
            root_dir.join("consumequeue_rocksdb")
        } else {
            root_dir.join("consumequeue")
        }
    }

    pub fn consume_queue_conflict_path_from_message_store_config<S>(message_store_config: &S) -> PathBuf
    where
        S: RocksDbConfigSource + ?Sized,
    {
        let root_dir = PathBuf::from(message_store_config.store_path_root_dir());
        if message_store_config.use_separate_store_path_for_rocksdb_cq() {
            root_dir.join("consumequeue")
        } else {
            root_dir.join("consumequeue_rocksdb")
        }
    }

    pub fn consume_queue_from_message_store_config<S>(message_store_config: &S) -> Self
    where
        S: RocksDbConfigSource + ?Sized,
    {
        Self {
            enabled: message_store_config.rocksdb_store_enabled(),
            path: Self::consume_queue_path_from_message_store_config(message_store_config),
            wal_enabled: false,
            sync_write: false,
            column_families: vec![
                RocksDbColumnFamilyConfig::consume_queue_default(),
                RocksDbColumnFamilyConfig::consume_queue_offset(),
            ],
            ..Self::operational_from_message_store_config(message_store_config)
        }
    }

    pub fn message_from_message_store_config<S>(message_store_config: &S) -> Self
    where
        S: RocksDbConfigSource + ?Sized,
    {
        Self {
            enabled: message_store_config.rocksdb_store_enabled(),
            path: PathBuf::from(message_store_config.store_path_root_dir()).join(ROCKSDB_MESSAGE_DIRECTORY),
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

    fn operational_from_message_store_config<S>(message_store_config: &S) -> Self
    where
        S: RocksDbConfigSource + ?Sized,
    {
        Self {
            flush_interval_ms: message_store_config.mem_table_flush_interval_ms(),
            compaction_interval_ms: message_store_config
                .clean_rocksdb_dirty_cq_interval_min()
                .saturating_mul(60 * 1000),
            checkpoint_interval_ms: message_store_config.rocksdb_checkpoint_interval_ms(),
            backup_interval_ms: message_store_config.rocksdb_backup_interval_ms(),
            backup_dir: message_store_config.rocksdb_backup_dir().map(PathBuf::from),
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
