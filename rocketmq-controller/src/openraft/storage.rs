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

//! Storage layer for OpenRaft
//!
//! This module provides the storage implementation for OpenRaft,
//! including log storage and state machine.

use std::path::PathBuf;

use rocketmq_rust::ArcMut;

use crate::config::ControllerConfig;
use crate::config::StorageBackendType;
use crate::error::Result;
use crate::storage::create_storage;
use crate::storage::StorageConfig;

use super::log_store::LogStore;
use super::state_machine::StateMachine;

/// Combined store containing both log store and state machine
///
/// In OpenRaft 0.10, the log store and state machine are separate components
/// that are passed to Raft::new() independently.
#[derive(Clone)]
pub struct Store {
    pub log_store: LogStore,
    pub state_machine: StateMachine,
}

impl Store {
    /// Create a new store
    pub fn new(config: ArcMut<ControllerConfig>) -> Self {
        Self {
            log_store: LogStore::new(),
            state_machine: StateMachine::new(config),
        }
    }

    pub async fn open(config: ArcMut<ControllerConfig>) -> Result<Self> {
        let backend = create_storage(storage_config(config.as_ref())?).await?;
        let log_store = LogStore::open(backend.clone()).await?;
        let state_machine = StateMachine::open(config, backend).await?;

        Ok(Self {
            log_store,
            state_machine,
        })
    }
}

fn storage_config(config: &ControllerConfig) -> Result<StorageConfig> {
    match config.storage_backend {
        StorageBackendType::Memory => Ok(StorageConfig::Memory),
        StorageBackendType::File => {
            #[cfg(feature = "storage-file")]
            {
                Ok(StorageConfig::File {
                    path: resolved_storage_path(config),
                })
            }
            #[cfg(not(feature = "storage-file"))]
            {
                Err(crate::error::ControllerError::StorageError(
                    "The file storage backend is not enabled for rocketmq-controller".to_string(),
                ))
            }
        }
        StorageBackendType::RocksDB => {
            #[cfg(feature = "storage-rocksdb")]
            {
                Ok(StorageConfig::RocksDB {
                    path: resolved_storage_path(config),
                })
            }
            #[cfg(not(feature = "storage-rocksdb"))]
            {
                Err(crate::error::ControllerError::StorageError(
                    "The rocksdb storage backend is not enabled for rocketmq-controller".to_string(),
                ))
            }
        }
    }
}

fn resolved_storage_path(config: &ControllerConfig) -> PathBuf {
    if !config.storage_path.is_empty() {
        return PathBuf::from(&config.storage_path);
    }

    if !config.controller_store_path.is_empty() {
        return PathBuf::from(&config.controller_store_path);
    }

    PathBuf::from(&config.rocketmq_home)
        .join("controller")
        .join(format!("node-{}", config.node_id))
}
