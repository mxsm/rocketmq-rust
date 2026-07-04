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

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("RocksDB error: {0}")]
    RocksDb(String),

    #[error("Store is not started")]
    NotStarted,

    #[error("Message not found")]
    MessageNotFound,

    #[error("Store configuration error: {0}")]
    Config(String),

    #[error("Unsupported store configuration: {0}")]
    Unsupported(String),

    #[error("Invalid store state: {0}")]
    InvalidState(String),

    #[error("Store storage error: {0}")]
    Storage(String),

    #[error("Tiered store error: {0}")]
    TieredStore(String),

    #[error("HA store error: {0}")]
    Ha(String),

    #[error("DLedger store error: {0}")]
    DLedger(String),

    #[error("Mapped file not found")]
    MappedFileNotFound,
}

#[derive(Debug, Error)]
pub enum HAError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("RocksDB error: {0}")]
    RocksDb(String),

    #[error("HA service error: {0}")]
    Service(String),
}

pub type HAResult<T> = std::result::Result<T, HAError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rocksdb_error() {
        let error = StoreError::RocksDb("Database error".to_string());
        assert_eq!(format!("{}", error), "RocksDB error: Database error");
    }

    #[test]
    fn store_not_started() {
        let error = StoreError::NotStarted;
        assert_eq!(format!("{}", error), "Store is not started");
    }

    #[test]
    fn message_not_found() {
        let error = StoreError::MessageNotFound;
        assert_eq!(format!("{}", error), "Message not found");
    }

    #[test]
    fn typed_store_error() {
        let error = StoreError::InvalidState("An error occurred".to_string());
        assert_eq!(format!("{}", error), "Invalid store state: An error occurred");
    }
}
