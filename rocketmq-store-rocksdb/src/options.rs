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

use rocketmq_error::RocketMQError;

use crate::config::RocksDbBlockBasedIndexType;
use crate::config::RocksDbColumnFamilyConfig;
use crate::config::RocksDbCompactionStyle;
use crate::config::RocksDbCompressionType;
use crate::config::RocksDbConfig;
use crate::config::RocksDbDataBlockIndexType;
use crate::config::RocksDbWalRecoveryMode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RocksDbWriteProfile {
    DisableWal,
    Wal,
    SyncWal,
}

pub struct RocksDbOptionsFactory;

impl RocksDbOptionsFactory {
    pub fn db_options(config: &RocksDbConfig) -> Result<::rocksdb::Options, RocketMQError> {
        config.validate()?;
        let mut options = ::rocksdb::Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(config.create_missing_column_families);
        options.set_max_open_files(config.max_open_files);
        options.set_max_background_jobs(config.max_background_jobs);
        options.set_max_subcompactions(config.max_subcompactions);
        options.set_manual_wal_flush(config.manual_wal_flush);
        options.set_atomic_flush(true);
        options.set_write_buffer_size(config.write_buffer_size);
        options.set_max_write_buffer_number(config.max_write_buffer_number);
        options.set_compression_type(to_rocksdb_compression(config.compression_type));
        options.set_bottommost_compression_type(to_rocksdb_compression(config.bottommost_compression_type));
        options.set_compaction_style(to_rocksdb_compaction_style(config.compaction_style));
        if let Some(db_write_buffer_size) = config.db_write_buffer_size {
            options.set_db_write_buffer_size(db_write_buffer_size);
        }
        options.set_bytes_per_sync(config.bytes_per_sync);
        if let Some(wal_bytes_per_sync) = config.wal_bytes_per_sync {
            options.set_wal_bytes_per_sync(wal_bytes_per_sync);
        }
        options.set_max_log_file_size(config.max_log_file_size);
        options.set_keep_log_file_num(config.keep_log_file_num);
        options.set_max_manifest_file_size(config.max_manifest_file_size);
        options.set_allow_concurrent_memtable_write(config.allow_concurrent_memtable_write);
        options.set_paranoid_checks(config.paranoid_checks);
        options.set_compaction_readahead_size(config.compaction_readahead_size);
        options.set_use_direct_io_for_flush_and_compaction(config.use_direct_io_for_flush_and_compaction);
        options.set_use_direct_reads(config.use_direct_reads);
        options.set_wal_recovery_mode(to_rocksdb_recovery_mode(config.wal_recovery_mode));
        if config.statistics_enabled {
            options.enable_statistics();
            if config.stats_dump_period_sec > 0 {
                options.set_stats_dump_period_sec(config.stats_dump_period_sec);
            }
        }
        Ok(options)
    }

    pub fn cf_options(config: &RocksDbColumnFamilyConfig) -> Result<::rocksdb::Options, RocketMQError> {
        config.validate()?;
        let mut options = ::rocksdb::Options::default();
        options.set_write_buffer_size(config.write_buffer_size);
        options.set_max_write_buffer_number(config.max_write_buffer_number);
        options.set_compression_type(to_rocksdb_compression(config.compression_type));
        options.set_bottommost_compression_type(to_rocksdb_compression(config.bottommost_compression_type));
        options.set_compaction_style(to_rocksdb_compaction_style(config.compaction_style));
        options.set_min_write_buffer_number_to_merge(config.min_write_buffer_number_to_merge);
        options.set_num_levels(config.num_levels);
        options.set_level_zero_file_num_compaction_trigger(config.level_zero_file_num_compaction_trigger);
        options.set_level_zero_slowdown_writes_trigger(config.level_zero_slowdown_writes_trigger);
        options.set_level_zero_stop_writes_trigger(config.level_zero_stop_writes_trigger);
        options.set_target_file_size_base(config.target_file_size_base);
        options.set_target_file_size_multiplier(config.target_file_size_multiplier);
        if let Some(max_bytes_for_level_base) = config.max_bytes_for_level_base {
            options.set_max_bytes_for_level_base(max_bytes_for_level_base);
        }
        if let Some(max_bytes_for_level_multiplier) = config.max_bytes_for_level_multiplier {
            options.set_max_bytes_for_level_multiplier(max_bytes_for_level_multiplier);
        }
        if let Some(max_compaction_bytes) = config.max_compaction_bytes {
            options.set_max_compaction_bytes(max_compaction_bytes);
        }
        if let Some(soft_pending_compaction_bytes_limit) = config.soft_pending_compaction_bytes_limit {
            options.set_soft_pending_compaction_bytes_limit(soft_pending_compaction_bytes_limit);
        }
        if let Some(hard_pending_compaction_bytes_limit) = config.hard_pending_compaction_bytes_limit {
            options.set_hard_pending_compaction_bytes_limit(hard_pending_compaction_bytes_limit);
        }
        options.set_report_bg_io_stats(config.report_bg_io_stats);
        options.set_optimize_filters_for_hits(config.optimize_filters_for_hits);
        options.set_inplace_update_support(config.inplace_update_support);

        let mut block_options = ::rocksdb::BlockBasedOptions::default();
        block_options.set_format_version(config.format_version);
        block_options.set_index_type(to_rocksdb_block_index_type(config.index_type));
        block_options.set_data_block_index_type(to_rocksdb_data_block_index_type(config.data_block_index_type));
        if let Some(data_block_hash_ratio) = config.data_block_hash_ratio {
            block_options.set_data_block_hash_ratio(data_block_hash_ratio);
        }
        block_options.set_block_size(config.block_size);
        if let Some(metadata_block_size) = config.metadata_block_size {
            block_options.set_metadata_block_size(metadata_block_size);
        }
        block_options.set_bloom_filter(config.bloom_filter_bits, false);
        block_options.set_block_cache(&::rocksdb::Cache::new_lru_cache(config.block_cache_size));
        block_options.set_cache_index_and_filter_blocks(config.cache_index_and_filter_blocks);
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(config.pin_l0_filter_and_index_blocks_in_cache);
        block_options.set_pin_top_level_index_and_filter(config.pin_top_level_index_and_filter);
        block_options.set_optimize_filters_for_memory(config.optimize_filters_for_memory);
        block_options.set_whole_key_filtering(config.whole_key_filtering);
        options.set_block_based_table_factory(&block_options);
        Ok(options)
    }

    pub fn write_options(profile: RocksDbWriteProfile) -> ::rocksdb::WriteOptions {
        let mut options = ::rocksdb::WriteOptions::default();
        match profile {
            RocksDbWriteProfile::DisableWal => {
                options.disable_wal(true);
                options.set_sync(false);
            }
            RocksDbWriteProfile::Wal => {
                options.disable_wal(false);
                options.set_sync(false);
            }
            RocksDbWriteProfile::SyncWal => {
                options.disable_wal(false);
                options.set_sync(true);
            }
        }
        options
    }

    pub fn read_options(prefix_same_as_start: bool, total_order_seek: bool) -> ::rocksdb::ReadOptions {
        let mut options = ::rocksdb::ReadOptions::default();
        options.set_prefix_same_as_start(prefix_same_as_start);
        options.set_total_order_seek(total_order_seek);
        options
    }
}

fn to_rocksdb_compression(compression_type: RocksDbCompressionType) -> ::rocksdb::DBCompressionType {
    match compression_type {
        RocksDbCompressionType::None => ::rocksdb::DBCompressionType::None,
        RocksDbCompressionType::Snappy => ::rocksdb::DBCompressionType::Snappy,
        RocksDbCompressionType::Lz4 => ::rocksdb::DBCompressionType::Lz4,
        RocksDbCompressionType::Zstd => ::rocksdb::DBCompressionType::Zstd,
    }
}

fn to_rocksdb_compaction_style(compaction_style: RocksDbCompactionStyle) -> ::rocksdb::DBCompactionStyle {
    match compaction_style {
        RocksDbCompactionStyle::Level => ::rocksdb::DBCompactionStyle::Level,
        RocksDbCompactionStyle::Universal => ::rocksdb::DBCompactionStyle::Universal,
        RocksDbCompactionStyle::Fifo => ::rocksdb::DBCompactionStyle::Fifo,
    }
}

fn to_rocksdb_recovery_mode(recovery_mode: RocksDbWalRecoveryMode) -> ::rocksdb::DBRecoveryMode {
    match recovery_mode {
        RocksDbWalRecoveryMode::TolerateCorruptedTailRecords => ::rocksdb::DBRecoveryMode::TolerateCorruptedTailRecords,
        RocksDbWalRecoveryMode::AbsoluteConsistency => ::rocksdb::DBRecoveryMode::AbsoluteConsistency,
        RocksDbWalRecoveryMode::PointInTime => ::rocksdb::DBRecoveryMode::PointInTime,
        RocksDbWalRecoveryMode::SkipAnyCorruptedRecord => ::rocksdb::DBRecoveryMode::SkipAnyCorruptedRecord,
    }
}

fn to_rocksdb_block_index_type(index_type: RocksDbBlockBasedIndexType) -> ::rocksdb::BlockBasedIndexType {
    match index_type {
        RocksDbBlockBasedIndexType::BinarySearch => ::rocksdb::BlockBasedIndexType::BinarySearch,
        RocksDbBlockBasedIndexType::HashSearch => ::rocksdb::BlockBasedIndexType::HashSearch,
        RocksDbBlockBasedIndexType::TwoLevelIndexSearch => ::rocksdb::BlockBasedIndexType::TwoLevelIndexSearch,
    }
}

fn to_rocksdb_data_block_index_type(index_type: RocksDbDataBlockIndexType) -> ::rocksdb::DataBlockIndexType {
    match index_type {
        RocksDbDataBlockIndexType::BinarySearch => ::rocksdb::DataBlockIndexType::BinarySearch,
        RocksDbDataBlockIndexType::BinaryAndHash => ::rocksdb::DataBlockIndexType::BinaryAndHash,
    }
}
