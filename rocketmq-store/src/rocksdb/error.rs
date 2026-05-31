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

use std::fmt;

use rocketmq_error::RocketMQError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RocksDbErrorKind {
    Open,
    ColumnFamilyMissing,
    ColumnFamilyCreate,
    Read,
    Write,
    Batch,
    Iterator,
    Snapshot,
    Flush,
    Compaction,
    Checkpoint,
    Codec,
    Corruption,
    Closed,
    InvalidConfig,
}

impl fmt::Display for RocksDbErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

pub trait RocksDbResultExt<T> {
    fn map_rocksdb(self, kind: RocksDbErrorKind) -> Result<T, RocketMQError>;
}

impl<T> RocksDbResultExt<T> for Result<T, ::rocksdb::Error> {
    fn map_rocksdb(self, kind: RocksDbErrorKind) -> Result<T, RocketMQError> {
        self.map_err(|error| match kind {
            RocksDbErrorKind::Read | RocksDbErrorKind::Iterator | RocksDbErrorKind::Snapshot => {
                RocketMQError::storage_read_failed("rocksdb", format!("{kind}: {error}"))
            }
            RocksDbErrorKind::Corruption => RocketMQError::StorageCorrupted {
                path: "rocksdb".to_string(),
            },
            RocksDbErrorKind::InvalidConfig => RocketMQError::ConfigInvalidValue {
                key: "rocksdb",
                value: String::new(),
                reason: error.to_string(),
            },
            _ => RocketMQError::storage_write_failed("rocksdb", format!("{kind}: {error}")),
        })
    }
}

pub fn closed_error() -> RocketMQError {
    RocketMQError::storage_write_failed("rocksdb", "store is closed")
}

pub fn column_family_missing_error(name: &str) -> RocketMQError {
    RocketMQError::storage_read_failed("rocksdb", format!("column family not found: {name}"))
}

pub fn codec_error(reason: impl Into<String>) -> RocketMQError {
    RocketMQError::deserialization_failed("rocksdb-codec", reason)
}
