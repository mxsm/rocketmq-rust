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

use crate::rocksdb::config::RocksDbColumnFamilyConfig;
use crate::rocksdb::config::RocksDbCompactionStyle;
use crate::rocksdb::config::RocksDbCompressionType;
use crate::rocksdb::config::RocksDbConfig;

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
