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

//! Backend-neutral message-store composition.

use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_runtime::ChildServiceContext;
use thiserror::Error;

use crate::base::message_store::MessageStore;
use crate::base::store_enum::StoreType;
use crate::config::message_store_config::MessageStoreConfig;
use crate::config::store_runtime_config::StoreRuntimeConfig;
use crate::message_store::local_file_message_store::LocalFileMessageStore;
#[cfg(feature = "rocksdb_store")]
use crate::message_store::rocksdb_message_store::RocksDBMessageStore;
use crate::message_store::OwnedMessageStore;
use crate::stats::broker_stats_manager::BrokerStatsManager;
use crate::store_error::StoreError;
use crate::timer::timer_message_store::TimerMessageStore;

/// Complete configuration required to open one Broker message store.
pub struct StoreFactoryConfig {
    message_store: Arc<MessageStoreConfig>,
    runtime: Arc<StoreRuntimeConfig>,
    topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
    broker_stats_manager: Option<Arc<BrokerStatsManager>>,
    notify_message_arrive_in_batch: bool,
}

impl StoreFactoryConfig {
    pub fn new(
        message_store: Arc<MessageStoreConfig>,
        runtime: Arc<StoreRuntimeConfig>,
        topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
        broker_stats_manager: Option<Arc<BrokerStatsManager>>,
        notify_message_arrive_in_batch: bool,
    ) -> Self {
        Self {
            message_store,
            runtime,
            topic_config_table,
            broker_stats_manager,
            notify_message_arrive_in_batch,
        }
    }

    pub fn backend(&self) -> StoreType {
        self.message_store.store_type
    }
}

/// A fully wired store plus optional services owned by the selected backend.
pub struct OpenedStore {
    backend: StoreType,
    message_store: OwnedMessageStore,
    timer_message_store: Option<Arc<TimerMessageStore>>,
}

impl OpenedStore {
    pub const fn backend(&self) -> StoreType {
        self.backend
    }

    pub fn into_parts(self) -> (OwnedMessageStore, Option<Arc<TimerMessageStore>>) {
        (self.message_store, self.timer_message_store)
    }
}

/// Failure to construct the configured backend.
#[derive(Debug, Error)]
pub enum StoreFactoryError {
    #[error("failed to open {backend:?} message store: {source}")]
    Open {
        backend: StoreType,
        #[source]
        source: StoreError,
    },
    #[error("{backend:?} message store is not available in this build")]
    BackendUnavailable { backend: StoreType },
}

/// The only composition boundary allowed to select a concrete message-store backend.
pub struct StoreFactory;

impl StoreFactory {
    /// Opens and wires the configured backend under the injected service scope.
    ///
    /// # Errors
    ///
    /// Returns [`StoreFactoryError`] when the selected backend is unavailable or
    /// cannot establish its owned dependencies.
    pub fn open(
        config: StoreFactoryConfig,
        service_context: ChildServiceContext,
    ) -> Result<OpenedStore, StoreFactoryError> {
        match config.backend() {
            StoreType::LocalFile => Self::open_local(config, service_context),
            StoreType::RocksDB => Self::open_rocksdb(config, service_context),
        }
    }

    fn open_local(
        config: StoreFactoryConfig,
        service_context: ChildServiceContext,
    ) -> Result<OpenedStore, StoreFactoryError> {
        let backend = StoreType::LocalFile;
        let mut store = LocalFileMessageStore::try_new(
            config.message_store,
            config.runtime,
            config.topic_config_table,
            config.broker_stats_manager,
            config.notify_message_arrive_in_batch,
            service_context.child("local"),
        )
        .map_err(|source| StoreFactoryError::Open { backend, source })?;
        store
            .wire_owned_root_dependencies()
            .map_err(|source| StoreFactoryError::Open { backend, source })?;
        let timer_message_store = store.get_timer_message_store().cloned();
        Ok(OpenedStore {
            backend,
            message_store: OwnedMessageStore::local_file(store),
            timer_message_store,
        })
    }

    #[cfg(feature = "rocksdb_store")]
    fn open_rocksdb(
        config: StoreFactoryConfig,
        service_context: ChildServiceContext,
    ) -> Result<OpenedStore, StoreFactoryError> {
        let backend = StoreType::RocksDB;
        let store = RocksDBMessageStore::try_new(
            config.message_store,
            config.runtime,
            config.topic_config_table,
            config.broker_stats_manager,
            config.notify_message_arrive_in_batch,
            service_context.child("rocksdb"),
        )
        .map_err(|source| StoreFactoryError::Open { backend, source })?;
        let timer_message_store = store.get_timer_message_store().cloned();
        Ok(OpenedStore {
            backend,
            message_store: OwnedMessageStore::rocksdb(store),
            timer_message_store,
        })
    }

    #[cfg(not(feature = "rocksdb_store"))]
    fn open_rocksdb(
        _config: StoreFactoryConfig,
        _service_context: ChildServiceContext,
    ) -> Result<OpenedStore, StoreFactoryError> {
        Err(StoreFactoryError::BackendUnavailable {
            backend: StoreType::RocksDB,
        })
    }
}
