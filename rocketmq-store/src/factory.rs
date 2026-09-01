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
use std::time::Duration;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_runtime::ChildServiceContext;

use crate::base::backend_ops::BackendOps;
use crate::base::store_enum::StoreType;
use crate::config::message_store_config::MessageStoreConfig;
use crate::config::store_runtime_config::StoreRuntimeConfig;
use crate::config::timer_store_config::ValidatedTimerStoreConfig;
use crate::message_store::local_file_message_store::LocalFileMessageStore;
#[cfg(feature = "rocksdb_store")]
use crate::message_store::rocksdb_message_store::RocksDBMessageStore;
use crate::stats::broker_stats_manager::BrokerStatsManager;
use crate::store_error::StoreError;
#[cfg(not(feature = "rocksdb_store"))]
use crate::store_error::{StoreComponent, StoreOperation};
use crate::store_ports::StorePorts;
use crate::telemetry::StoreTelemetry;
use crate::timer::timer_message_store::TimerMessageStore;
use rocketmq_store_local::commit_log::append::micro_batch::MicroBatchPolicy;

/// Complete configuration required to open one Broker message store.
pub struct StoreFactoryConfig {
    message_store: Arc<MessageStoreConfig>,
    runtime: Arc<StoreRuntimeConfig>,
    topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
    broker_stats_manager: Option<Arc<BrokerStatsManager>>,
    notify_message_arrive_in_batch: bool,
    telemetry: StoreTelemetry,
    micro_batch_policy: MicroBatchPolicy,
    timer_store_config: ValidatedTimerStoreConfig,
}

impl StoreFactoryConfig {
    /// Validates the caller-owned store configuration before backend composition.
    ///
    /// Returns `None` when either the Timer limits or CommitLog micro-batch limits
    /// cannot form a valid policy.
    pub fn try_new(
        message_store: Arc<MessageStoreConfig>,
        runtime: Arc<StoreRuntimeConfig>,
        topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
        broker_stats_manager: Option<Arc<BrokerStatsManager>>,
        notify_message_arrive_in_batch: bool,
        telemetry: StoreTelemetry,
    ) -> Option<Self> {
        let timer_store_config = message_store.timer_store_config.validated()?;
        let micro_batch_policy = if message_store.commit_log_micro_batch_enabled {
            MicroBatchPolicy::try_new(
                message_store.commit_log_micro_batch_max_items,
                message_store.commit_log_micro_batch_max_bytes,
                Duration::from_micros(message_store.commit_log_micro_batch_max_wait_micros),
            )
        } else {
            MicroBatchPolicy::disabled(message_store.commit_log_append_queue_bytes)
        }?;
        Some(Self {
            message_store,
            runtime,
            topic_config_table,
            broker_stats_manager,
            notify_message_arrive_in_batch,
            telemetry,
            micro_batch_policy,
            timer_store_config,
        })
    }

    pub fn backend(&self) -> StoreType {
        self.message_store.store_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_timer_configuration_is_rejected_before_store_composition() {
        let mut message_store = MessageStoreConfig::default();
        message_store.timer_store_config.lane_count = 0;

        let config = StoreFactoryConfig::try_new(
            Arc::new(message_store),
            Arc::new(StoreRuntimeConfig::default()),
            Arc::new(DashMap::new()),
            None,
            false,
            StoreTelemetry::noop(),
        );

        assert!(config.is_none());
    }
}

/// A fully wired store plus optional services owned by the selected backend.
pub struct OpenedStore {
    backend: StoreType,
    message_store: StorePorts,
    timer_message_store: Option<Arc<TimerMessageStore>>,
}

impl OpenedStore {
    pub const fn backend(&self) -> StoreType {
        self.backend
    }

    pub fn into_parts(self) -> (StorePorts, Option<Arc<TimerMessageStore>>) {
        (self.message_store, self.timer_message_store)
    }
}

/// The only composition boundary allowed to select a concrete message-store backend.
pub struct StoreFactory;

impl StoreFactory {
    /// Opens and wires the configured backend under the injected service scope.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the selected backend is unavailable or cannot
    /// establish its owned dependencies.
    pub fn open(config: StoreFactoryConfig, service_context: ChildServiceContext) -> Result<OpenedStore, StoreError> {
        match config.backend() {
            StoreType::LocalFile => Self::open_local(config, service_context),
            StoreType::RocksDB => Self::open_rocksdb(config, service_context),
        }
    }

    fn open_local(config: StoreFactoryConfig, service_context: ChildServiceContext) -> Result<OpenedStore, StoreError> {
        let backend = StoreType::LocalFile;
        let mut store = LocalFileMessageStore::try_new_with_telemetry_validated(
            config.message_store,
            config.micro_batch_policy,
            config.timer_store_config,
            config.runtime,
            config.topic_config_table,
            config.broker_stats_manager,
            config.notify_message_arrive_in_batch,
            service_context.component("local"),
            config.telemetry,
        )?;
        store.wire_owned_root_dependencies()?;
        let timer_message_store = store.get_timer_message_store().cloned();
        Ok(OpenedStore {
            backend,
            message_store: StorePorts::local_file(store),
            timer_message_store,
        })
    }

    #[cfg(feature = "rocksdb_store")]
    fn open_rocksdb(
        config: StoreFactoryConfig,
        service_context: ChildServiceContext,
    ) -> Result<OpenedStore, StoreError> {
        let backend = StoreType::RocksDB;
        let store = RocksDBMessageStore::try_new_with_telemetry_validated(
            config.message_store,
            config.micro_batch_policy,
            config.timer_store_config,
            config.runtime,
            config.topic_config_table,
            config.broker_stats_manager,
            config.notify_message_arrive_in_batch,
            service_context.component("rocksdb"),
            config.telemetry,
        )?;
        let timer_message_store = store.get_timer_message_store().cloned();
        Ok(OpenedStore {
            backend,
            message_store: StorePorts::rocksdb(store),
            timer_message_store,
        })
    }

    #[cfg(not(feature = "rocksdb_store"))]
    fn open_rocksdb(
        _config: StoreFactoryConfig,
        _service_context: ChildServiceContext,
    ) -> Result<OpenedStore, StoreError> {
        Err(
            StoreError::new(&rocketmq_error::STORAGE_BACKEND_UNAVAILABLE, StoreOperation::Load)
                .in_component(StoreComponent::RocksDb),
        )
    }
}
