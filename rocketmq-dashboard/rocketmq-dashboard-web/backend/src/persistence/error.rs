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

/// Errors exposed by the storage boundary. Source details stay internal and
/// are mapped to stable dashboard responses by `DashboardError`.
#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("storage configuration is invalid: {0}")]
    InvalidConfig(String),
    #[error("storage data directory is already in use")]
    LockUnavailable,
    #[error("storage layout is unsupported")]
    UnsupportedLayout,
    #[error("storage data is corrupted")]
    CorruptedData,
    #[error("storage record was not found")]
    NotFound,
    #[error("storage capacity is insufficient")]
    Capacity,
    #[error("storage backend is unavailable")]
    ConnectionUnavailable,
    #[error("storage migration failed")]
    MigrationFailed,
    #[error("storage operation timed out")]
    Timeout,
    #[error("storage write conflict")]
    Conflict,
    #[error("storage I/O failed")]
    Io(#[source] std::io::Error),
    #[error("storage serialization failed")]
    Serialization(#[source] serde_json::Error),
    #[error("storage query failed")]
    Query(#[source] sqlx::Error),
    #[error("storage runtime operation failed")]
    Runtime(#[source] rocketmq_runtime::RuntimeError),
}

impl PersistenceError {
    pub const fn stable_code(&self) -> &'static str {
        match self {
            Self::InvalidConfig(_) => "STORAGE_CONFIG_INVALID",
            Self::LockUnavailable => "STORAGE_LOCKED",
            Self::UnsupportedLayout => "STORAGE_LAYOUT_UNSUPPORTED",
            Self::CorruptedData => "STORAGE_CORRUPTED",
            Self::NotFound => "STORAGE_NOT_FOUND",
            Self::Capacity => "STORAGE_CAPACITY_EXHAUSTED",
            Self::ConnectionUnavailable => "STORAGE_UNAVAILABLE",
            Self::MigrationFailed => "STORAGE_FAILED",
            Self::Timeout => "STORAGE_TIMEOUT",
            Self::Conflict => "STORAGE_CONFLICT",
            Self::Io(_) | Self::Serialization(_) | Self::Query(_) | Self::Runtime(_) => "STORAGE_FAILED",
        }
    }
}
