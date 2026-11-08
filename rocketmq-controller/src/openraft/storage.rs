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

use crate::config::ControllerConfig;
use crate::config::ControllerConfigReader;
use crate::config::StorageEngineType;
#[cfg(any(not(feature = "dev-single"), not(feature = "storage-rocksdb")))]
use crate::error::configuration_invalid;
use crate::error::controller_internal_by;
use crate::storage::create_storage;
use crate::storage::StorageConfig;
use rocketmq_error::Result;
use rocketmq_runtime::BlockingExecutor;

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
    pub fn new(config: ControllerConfigReader) -> Self {
        Self {
            log_store: LogStore::new(),
            state_machine: StateMachine::new(config),
        }
    }

    /// Opens the combined OpenRaft store using the configured storage engine.
    ///
    /// The startup snapshot of `config` selects the storage implementation and path, while
    /// `storage_io` provides a blocking I/O executor when required by the underlying storage engine.
    /// The created key-value store instance is supplied to the helper.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage engine creation fails or if recovering the log store
    /// or state machine fails.
    pub async fn open(config: ControllerConfigReader, storage_io: BlockingExecutor) -> Result<Self> {
        let startup_config = config.snapshot();
        let key_value_store = create_storage(storage_config(&startup_config)?, storage_io).await?;
        Self::open_with_key_value_store(config, key_value_store).await
    }

    /// Opens the log store and state machine using an existing key-value store.
    ///
    /// The supplied `key_value_store` is shared between the recovered log store and state machine.
    /// `config` is forwarded to the state machine; this helper does not create a different storage instance.
    /// It restores log, vote, and position records for the log store, alongside state-machine, membership,
    /// and snapshot records for the state machine.
    ///
    /// # Errors
    ///
    /// Returns an error with controller context if reading, decoding, or validating either
    /// component's persisted state fails during recovery.
    async fn open_with_key_value_store(
        config: ControllerConfigReader,
        key_value_store: crate::storage::SharedKeyValueStore,
    ) -> Result<Self> {
        let log_store = LogStore::open(key_value_store.clone())
            .await
            .map_err(|error| controller_internal_by("open OpenRaft log store", error))?;
        let state_machine = StateMachine::open(config, key_value_store)
            .await
            .map_err(|error| controller_internal_by("open OpenRaft state machine", error))?;

        Ok(Self {
            log_store,
            state_machine,
        })
    }
}

fn storage_config(config: &ControllerConfig) -> Result<StorageConfig> {
    match config.storage_engine_type {
        StorageEngineType::Memory => Ok(StorageConfig::Memory),
        StorageEngineType::File => {
            #[cfg(feature = "dev-single")]
            {
                Ok(StorageConfig::File {
                    path: resolved_storage_path(config),
                })
            }
            #[cfg(not(feature = "dev-single"))]
            {
                Err(configuration_invalid("storageEngineType"))
            }
        }
        StorageEngineType::RocksDB => {
            #[cfg(feature = "storage-rocksdb")]
            {
                Ok(StorageConfig::RocksDB {
                    path: resolved_storage_path(config),
                })
            }
            #[cfg(not(feature = "storage-rocksdb"))]
            {
                Err(configuration_invalid("storageEngineType"))
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
