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

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_store_api::GetStatus;
use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreLifecycle;
use rocketmq_store_api::StoreOperation;
use rocketmq_store_api::WalPort;
use tracing::warn;

use crate::column_family::RocksDbColumnFamily;
use crate::config::RocksDbConfig;
use crate::config::RocksDbConfigSource;
use crate::consume_queue::RocksDbConsumeQueueStore;
use crate::index::RocksDbIndexBuildConfig;
use crate::index::RocksDbIndexBuildService;
use crate::maintenance::RocksDbMaintenanceService;
use crate::message::MessageRocksDbStorage;
use crate::resource_budget::RocksDbResourceBudget;
use crate::runtime::RocksDbRuntimeScope;
use crate::store::RocksDbStore;
use crate::timer::RocksDbTimerBuildConfig;
use crate::timer::RocksDbTimerBuildService;
use crate::transaction::RocksDbTransBuildConfig;
use crate::transaction::RocksDbTransBuildService;
use crate::value::ConsumeQueueValue;

const INDEX_KEY_TYPE: &str = "K";
const INDEX_UNIQUE_TYPE: &str = "U";
const MILLIS_PER_DAY: i64 = 24 * 60 * 60 * 1000;

enum RocksDbMessageStoreViolation {
    Disabled,
    InvalidConfiguration,
}

enum RocksDbMessageStoreError {
    Violation(RocksDbMessageStoreViolation),
    Native(::rocksdb::Error),
    Runtime(rocketmq_runtime::RuntimeError),
    Io(std::io::Error),
    Store(StoreError),
}

impl fmt::Display for RocksDbMessageStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RocksDB message-store operation failed")
    }
}

impl fmt::Debug for RocksDbMessageStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Violation(violation) => match violation {
                RocksDbMessageStoreViolation::Disabled => "Disabled",
                RocksDbMessageStoreViolation::InvalidConfiguration => "InvalidConfiguration",
            },
            Self::Native(_) => "Native",
            Self::Runtime(_) => "Runtime",
            Self::Io(_) => "Io",
            Self::Store(_) => "Store",
        };
        f.debug_struct("RocksDbMessageStoreError")
            .field("kind", &kind)
            .field("source_present", &self.source().is_some())
            .finish()
    }
}

impl StdError for RocksDbMessageStoreError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Violation(_) => None,
            Self::Native(source) => Some(source),
            Self::Runtime(source) => Some(source),
            Self::Io(source) => Some(source),
            Self::Store(source) => Some(source),
        }
    }
}

impl From<StoreError> for RocksDbMessageStoreError {
    fn from(source: StoreError) -> Self {
        Self::Store(source)
    }
}

impl From<::rocksdb::Error> for RocksDbMessageStoreError {
    fn from(source: ::rocksdb::Error) -> Self {
        Self::Native(source)
    }
}

impl From<rocketmq_runtime::RuntimeError> for RocksDbMessageStoreError {
    fn from(source: rocketmq_runtime::RuntimeError) -> Self {
        Self::Runtime(source)
    }
}

impl From<std::io::Error> for RocksDbMessageStoreError {
    fn from(source: std::io::Error) -> Self {
        Self::Io(source)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RocksDbMessageStoreOptions {
    pub timer_enabled: bool,
    pub transaction_enabled: bool,
}

/// Validated, redacted capability for opening RocksDB derived state.
#[derive(Clone)]
pub struct RocksDbOpenPlan {
    pub(crate) config: RocksDbConfig,
}

impl fmt::Debug for RocksDbOpenPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RocksDbOpenPlan").field("validated", &true).finish()
    }
}

impl RocksDbOpenPlan {
    /// Validates one raw RocksDB database configuration without opening it.
    pub fn from_config(config: RocksDbConfig) -> Option<Self> {
        if !config.enabled || !config.is_valid() {
            return None;
        }
        Some(Self { config })
    }

    /// Validates raw RocksDB configuration without opening or mutating a database.
    ///
    /// Returns `None` when RocksDB is disabled or a deterministic configuration
    /// constraint is invalid. This validation does not inspect the filesystem.
    pub fn from_message_store<S>(source: &S) -> Option<(Self, Self)>
    where
        S: RocksDbConfigSource + ?Sized,
    {
        if !source.rocksdb_store_enabled() {
            let _violation = RocksDbMessageStoreError::Violation(RocksDbMessageStoreViolation::Disabled);
            return None;
        }
        let rocksdb_config = RocksDbConfig::consume_queue_from_message_store_config(source);
        if !rocksdb_config.is_valid() {
            let _violation = RocksDbMessageStoreError::Violation(RocksDbMessageStoreViolation::InvalidConfiguration);
            return None;
        }
        let message_rocksdb_config = RocksDbConfig::message_from_message_store_config(source);
        if !message_rocksdb_config.is_valid() {
            let _violation = RocksDbMessageStoreError::Violation(RocksDbMessageStoreViolation::InvalidConfiguration);
            return None;
        }
        Some((
            Self { config: rocksdb_config },
            Self {
                config: message_rocksdb_config,
            },
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RocksDbTimeBoundary {
    Lower,
    Upper,
}

pub struct RocksDbReadRequest<'a> {
    pub topic: &'a str,
    pub queue_id: i32,
    pub offset: i64,
    pub max_message_count: i32,
    pub max_total_message_size: i32,
    pub max_pull_message_size: i32,
}

pub struct RocksDbReadRecord<S> {
    pub selection: S,
    pub queue_offset: u64,
    pub batch_count: i32,
}

pub struct RocksDbReadResult<S> {
    pub records: Vec<RocksDbReadRecord<S>>,
    pub status: GetStatus,
    pub next_begin_offset: i64,
    pub min_offset: i64,
    pub max_offset: i64,
    pub buffer_total_size: i32,
    pub message_count: i32,
}

pub struct RocksDbIndexLookup<S> {
    pub records: Vec<S>,
    pub last_update_timestamp: i64,
    pub last_update_physical_offset: i64,
}

/// Canonical RocksDB-derived state owned independently from the Local WAL.
pub struct RocksDbDerivedStore {
    rocksdb_config: RocksDbConfig,
    rocksdb_store: Arc<RocksDbStore>,
    consume_queue_store: RocksDbConsumeQueueStore,
    message_rocksdb_config: RocksDbConfig,
    message_rocksdb_storage: Arc<MessageRocksDbStorage>,
    rocksdb_index_service: Arc<RocksDbIndexBuildService>,
    rocksdb_timer_service: Option<Arc<RocksDbTimerBuildService>>,
    rocksdb_trans_service: Option<Arc<RocksDbTransBuildService>>,
    rocksdb_maintenance_service: RocksDbMaintenanceService,
    message_rocksdb_maintenance_service: RocksDbMaintenanceService,
    runtime_scope: RocksDbRuntimeScope,
}

impl RocksDbDerivedStore {
    /// Opens the two RocksDB derived-state databases without creating a message log.
    ///
    /// # Errors
    ///
    /// Returns `Ok(None)` when RocksDB is disabled or its raw configuration is
    /// invalid. Operational initialization and native database failures are
    /// returned as [`StoreError`].
    pub fn open<S>(
        source: &S,
        options: RocksDbMessageStoreOptions,
        service_context: rocketmq_runtime::ChildServiceContext,
    ) -> Result<Option<Self>, StoreError>
    where
        S: RocksDbConfigSource + ?Sized,
    {
        Self::open_with_metrics(
            source,
            options,
            service_context,
            rocketmq_observability::metrics::rocksdb::RocksDbMetricsRecorder::noop(),
        )
    }

    /// Opens the two RocksDB derived-state databases with metrics without
    /// creating a message log.
    ///
    /// Returns `Ok(None)` when RocksDB is disabled or its deterministic raw
    /// configuration is invalid.
    ///
    /// # Errors
    ///
    /// Returns an operational storage error when runtime, resource, or native
    /// database initialization fails.
    pub fn open_with_metrics<S>(
        source: &S,
        options: RocksDbMessageStoreOptions,
        service_context: rocketmq_runtime::ChildServiceContext,
        metrics: rocketmq_observability::metrics::rocksdb::RocksDbMetricsRecorder,
    ) -> Result<Option<Self>, StoreError>
    where
        S: RocksDbConfigSource + ?Sized,
    {
        let Some((rocksdb_plan, message_rocksdb_plan)) = RocksDbOpenPlan::from_message_store(source) else {
            return Ok(None);
        };
        Self::open_planned_with_metrics(rocksdb_plan, message_rocksdb_plan, options, service_context, metrics).map(Some)
    }

    /// Opens RocksDB derived state from a previously validated capability.
    ///
    /// # Errors
    ///
    /// Returns an operational storage error when runtime or native database
    /// initialization fails.
    pub fn open_planned(
        rocksdb_plan: RocksDbOpenPlan,
        message_rocksdb_plan: RocksDbOpenPlan,
        options: RocksDbMessageStoreOptions,
        service_context: rocketmq_runtime::ChildServiceContext,
    ) -> Result<Self, StoreError> {
        Self::open_planned_with_metrics(
            rocksdb_plan,
            message_rocksdb_plan,
            options,
            service_context,
            rocketmq_observability::metrics::rocksdb::RocksDbMetricsRecorder::noop(),
        )
    }

    /// Opens RocksDB derived state from validated capabilities with metrics.
    ///
    /// # Errors
    ///
    /// Returns an operational storage error when runtime, resource, or native
    /// database initialization fails. Configuration validation has already
    /// completed before this method is called.
    pub fn open_planned_with_metrics(
        rocksdb_plan: RocksDbOpenPlan,
        message_rocksdb_plan: RocksDbOpenPlan,
        options: RocksDbMessageStoreOptions,
        service_context: rocketmq_runtime::ChildServiceContext,
        metrics: rocketmq_observability::metrics::rocksdb::RocksDbMetricsRecorder,
    ) -> Result<Self, StoreError> {
        let RocksDbOpenPlan { config: rocksdb_config } = rocksdb_plan;
        let RocksDbOpenPlan {
            config: message_rocksdb_config,
        } = message_rocksdb_plan;
        let runtime_scope = RocksDbRuntimeScope::new(service_context);
        let resource_budget = Arc::new(RocksDbResourceBudget::from_config(&rocksdb_config)?);
        let block_cache_budget = Arc::clone(&resource_budget);
        metrics.register_resource_cache("rocksdb-block-cache", move || {
            rocketmq_observability::metrics::resource::ResourceCacheSnapshot {
                usage_bytes: block_cache_budget.block_cache_usage_bytes() as u64,
                budget_bytes: block_cache_budget.block_cache_budget_bytes() as u64,
            }
        });
        let write_buffer_budget = Arc::clone(&resource_budget);
        metrics.register_resource_cache("rocksdb-write-buffer", move || {
            rocketmq_observability::metrics::resource::ResourceCacheSnapshot {
                usage_bytes: write_buffer_budget.write_buffer_usage_bytes() as u64,
                budget_bytes: write_buffer_budget.write_buffer_budget_bytes() as u64,
            }
        });
        let rocksdb_store = Arc::new(RocksDbStore::open_validated_with_metrics_and_resource_budget(
            rocksdb_config.clone(),
            metrics.clone(),
            Arc::clone(&resource_budget),
        )?);
        let consume_queue_store = RocksDbConsumeQueueStore::new(Arc::clone(&rocksdb_store));
        let message_rocksdb_storage = Arc::new(MessageRocksDbStorage::open_validated_with_metrics_and_resource_budget(
            message_rocksdb_config.clone(),
            metrics,
            resource_budget,
        )?);
        let rocksdb_maintenance_service = RocksDbMaintenanceService::new(
            Arc::clone(&rocksdb_store),
            rocksdb_config.clone(),
            runtime_scope.clone(),
        );
        let message_rocksdb_maintenance_service = RocksDbMaintenanceService::new(
            message_rocksdb_storage.store_arc(),
            message_rocksdb_config.clone(),
            runtime_scope.clone(),
        );
        let rocksdb_index_service = Arc::new(RocksDbIndexBuildService::new(
            Arc::clone(&message_rocksdb_storage),
            RocksDbIndexBuildConfig::default(),
        )?);
        let rocksdb_timer_service = options
            .timer_enabled
            .then(|| {
                RocksDbTimerBuildService::new(Arc::clone(&message_rocksdb_storage), RocksDbTimerBuildConfig::default())
                    .map(Arc::new)
            })
            .transpose()?;
        let rocksdb_trans_service = options
            .transaction_enabled
            .then(|| {
                RocksDbTransBuildService::new(Arc::clone(&message_rocksdb_storage), RocksDbTransBuildConfig::default())
                    .map(Arc::new)
            })
            .transpose()?;

        Ok(Self {
            rocksdb_config,
            rocksdb_store,
            consume_queue_store,
            message_rocksdb_config,
            message_rocksdb_storage,
            rocksdb_index_service,
            rocksdb_timer_service,
            rocksdb_trans_service,
            rocksdb_maintenance_service,
            message_rocksdb_maintenance_service,
            runtime_scope,
        })
    }

    pub const fn rocksdb_config(&self) -> &RocksDbConfig {
        &self.rocksdb_config
    }

    pub const fn message_rocksdb_config(&self) -> &RocksDbConfig {
        &self.message_rocksdb_config
    }

    pub fn rocksdb_store(&self) -> Arc<RocksDbStore> {
        Arc::clone(&self.rocksdb_store)
    }

    pub fn resource_budget(&self) -> Arc<RocksDbResourceBudget> {
        self.rocksdb_store.resource_budget()
    }

    pub const fn consume_queue_store(&self) -> &RocksDbConsumeQueueStore {
        &self.consume_queue_store
    }

    pub fn message_rocksdb_storage(&self) -> Arc<MessageRocksDbStorage> {
        Arc::clone(&self.message_rocksdb_storage)
    }

    pub fn rocksdb_index_service(&self) -> Arc<RocksDbIndexBuildService> {
        Arc::clone(&self.rocksdb_index_service)
    }

    pub fn rocksdb_timer_service(&self) -> Option<Arc<RocksDbTimerBuildService>> {
        self.rocksdb_timer_service.as_ref().map(Arc::clone)
    }

    pub fn rocksdb_trans_service(&self) -> Option<Arc<RocksDbTransBuildService>> {
        self.rocksdb_trans_service.as_ref().map(Arc::clone)
    }

    pub fn is_rocksdb_maintenance_running(&self) -> bool {
        self.rocksdb_maintenance_service.is_running()
    }

    pub fn is_message_rocksdb_maintenance_running(&self) -> bool {
        self.message_rocksdb_maintenance_service.is_running()
    }

    pub fn start_maintenance(&mut self) {
        self.rocksdb_maintenance_service.start();
        self.message_rocksdb_maintenance_service.start();
    }

    pub async fn shutdown_maintenance(&mut self) -> Result<(), StoreError> {
        let consume_queue_result = self.rocksdb_maintenance_service.shutdown_gracefully().await;
        let message_result = self.message_rocksdb_maintenance_service.shutdown_gracefully().await;
        consume_queue_result
            .map_err(|source| lifecycle_error(StoreOperation::Shutdown, RocksDbMessageStoreError::Store(source)))?;
        message_result
            .map_err(|source| lifecycle_error(StoreOperation::Shutdown, RocksDbMessageStoreError::Store(source)))?;
        Ok(())
    }

    pub fn close(&self) {
        if let Err(error) = self.rocksdb_index_service.flush_pending() {
            warn!(
                descriptor = ?error.descriptor().code(),
                operation = ?error.operation(),
                component = ?error.component(),
                source_present = std::error::Error::source(&error).is_some(),
                "failed to flush pending RocksDB index records before close"
            );
        }
        if let Some(timer_service) = self.rocksdb_timer_service.as_ref() {
            if let Err(error) = timer_service.flush_pending() {
                warn!(
                    descriptor = ?error.descriptor().code(),
                    operation = ?error.operation(),
                    component = ?error.component(),
                    source_present = std::error::Error::source(&error).is_some(),
                    "failed to flush pending RocksDB timer records before close"
                );
            }
        }
        if let Some(trans_service) = self.rocksdb_trans_service.as_ref() {
            if let Err(error) = trans_service.flush_pending() {
                warn!(
                    descriptor = ?error.descriptor().code(),
                    operation = ?error.operation(),
                    component = ?error.component(),
                    source_present = std::error::Error::source(&error).is_some(),
                    "failed to flush pending RocksDB transaction records before close"
                );
            }
        }
        self.rocksdb_store.close();
        self.message_rocksdb_storage.store().close();
    }

    pub fn flush_derived(&self) -> Result<(), StoreError> {
        self.rocksdb_index_service.flush_pending()?;
        self.rocksdb_store.flush(rocketmq_store_api::StoreOperation::Flush)?;
        self.message_rocksdb_storage.store().flush(StoreOperation::Flush)?;
        Ok(())
    }

    pub fn max_offset(&self, topic: &str, queue_id: i32) -> Result<i64, StoreError> {
        self.consume_queue_store
            .get_max_offset_in_queue(topic.to_string(), queue_id)
    }

    pub fn min_offset(&self, topic: &str, queue_id: i32) -> Result<i64, StoreError> {
        self.consume_queue_store
            .get_min_offset_in_queue(topic.to_string(), queue_id)
    }

    pub fn consume_queue_value(
        &self,
        topic: &str,
        queue_id: i32,
        offset: i64,
    ) -> Result<Option<ConsumeQueueValue>, StoreError> {
        self.consume_queue_value_with_operation(StoreOperation::Read, topic, queue_id, offset)
    }

    fn consume_queue_value_with_operation(
        &self,
        operation: StoreOperation,
        topic: &str,
        queue_id: i32,
        offset: i64,
    ) -> Result<Option<ConsumeQueueValue>, StoreError> {
        self.consume_queue_store
            .get_value_with_operation(operation, topic.to_string(), queue_id, offset)
    }

    pub fn offset_by_time(
        &self,
        topic: &str,
        queue_id: i32,
        timestamp: i64,
        boundary: RocksDbTimeBoundary,
    ) -> Result<i64, StoreError> {
        let min_offset = self.min_offset(topic, queue_id)?;
        let max_offset = self.max_offset(topic, queue_id)?;
        if max_offset <= min_offset {
            return Ok(0);
        }

        let mut low = min_offset;
        let mut high = max_offset - 1;
        let mut lower = max_offset;
        while low <= high {
            let middle = low + (high - low) / 2;
            let Some(value) =
                self.consume_queue_value_with_operation(StoreOperation::QueryOffset, topic, queue_id, middle)?
            else {
                return Ok(0);
            };
            if value.msg_store_time >= timestamp {
                lower = middle;
                high = middle - 1;
            } else {
                low = middle + 1;
            }
        }
        if boundary == RocksDbTimeBoundary::Lower {
            return Ok(lower);
        }

        low = min_offset;
        high = max_offset - 1;
        let mut upper = None;
        while low <= high {
            let middle = low + (high - low) / 2;
            let Some(value) =
                self.consume_queue_value_with_operation(StoreOperation::QueryOffset, topic, queue_id, middle)?
            else {
                return Ok(0);
            };
            if value.msg_store_time <= timestamp {
                upper = Some(middle);
                low = middle + 1;
            } else {
                high = middle - 1;
            }
        }
        Ok(upper.unwrap_or(0))
    }

    pub fn topic_queue_offsets(&self) -> Result<HashMap<(String, i32), i64>, StoreError> {
        self.consume_queue_store.max_offsets_by_topic_queue()
    }

    pub fn delete_topic(&self, topic: &str) -> Result<(), StoreError> {
        self.consume_queue_store.destroy_topic(topic)?;
        Ok(())
    }

    pub fn truncate_dirty(&self, physical_offset: i64) -> Result<(), StoreError> {
        self.consume_queue_store.truncate_dirty(physical_offset)?;
        Ok(())
    }

    pub fn clean_expired(&self, min_physical_offset: i64) -> Result<(), StoreError> {
        self.consume_queue_store
            .clean_expired_background(&self.runtime_scope, min_physical_offset)?;
        Ok(())
    }

    fn index_offsets(
        &self,
        topic: &str,
        key: &str,
        index_type: Option<&str>,
        max_num: usize,
        begin: i64,
        end: i64,
        last_key: Option<&str>,
        max_query_days: usize,
    ) -> Result<(Vec<i64>, i64, i64), StoreError> {
        let (begin, end) = normalize_index_query_time_range(begin, end, max_query_days);
        let mut offsets = self.message_rocksdb_storage.query_offsets_for_index(
            topic,
            index_type.unwrap_or(INDEX_KEY_TYPE),
            key,
            begin,
            end,
            max_num,
            last_key,
        )?;
        if index_type.is_none() && offsets.is_empty() {
            offsets = self.message_rocksdb_storage.query_offsets_for_index(
                topic,
                INDEX_UNIQUE_TYPE,
                key,
                begin,
                end,
                max_num,
                last_key,
            )?;
        }
        offsets.sort_unstable();
        let last_timestamp = self
            .message_rocksdb_storage
            .get_last_store_timestamp_for_index(StoreOperation::QueryOffset)?;
        let last_offset = self
            .message_rocksdb_storage
            .get_last_offset_py(StoreOperation::QueryOffset, RocksDbColumnFamily::Default.name())?;
        Ok((offsets, last_timestamp, last_offset))
    }
}

impl StoreLifecycle for RocksDbDerivedStore {
    async fn load(&mut self) -> Result<bool, StoreError> {
        Ok(true)
    }

    async fn start(&mut self) -> Result<(), StoreError> {
        self.start_maintenance();
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), StoreError> {
        self.shutdown_maintenance().await?;
        self.close();
        Ok(())
    }
}

fn rocksdb_failure_descriptor(operation: StoreOperation) -> &'static rocketmq_error::ErrorDescriptor {
    match operation {
        StoreOperation::Load | StoreOperation::Start => &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE,
        StoreOperation::Read | StoreOperation::QueryOffset => &rocketmq_error::STORAGE_READ_FAILED,
        StoreOperation::Append | StoreOperation::Flush | StoreOperation::Replicate | StoreOperation::AppendDerived => {
            &rocketmq_error::STORAGE_WRITE_FAILED
        }
        StoreOperation::Shutdown | StoreOperation::Admin => &rocketmq_error::STORAGE_IO_FAILED,
    }
}

fn lifecycle_error(operation: StoreOperation, error: RocksDbMessageStoreError) -> StoreError {
    match error {
        RocksDbMessageStoreError::Violation(_) => StoreError::new(&rocketmq_error::STORAGE_REQUEST_INVALID, operation)
            .in_component(StoreComponent::Configuration),
        RocksDbMessageStoreError::Native(source) => StoreError::new(rocksdb_failure_descriptor(operation), operation)
            .in_component(StoreComponent::RocksDb)
            .with_source(source),
        RocksDbMessageStoreError::Runtime(source) => crate::error::runtime_error(operation, source),
        RocksDbMessageStoreError::Io(source) => StoreError::new(&rocketmq_error::STORAGE_IO_FAILED, operation)
            .in_component(StoreComponent::RocksDb)
            .with_source(source),
        RocksDbMessageStoreError::Store(source) => source,
    }
}

/// Canonical RocksDB message-store adapter over an injected Local WAL port.
pub struct RocksDbMessageStoreRoot {
    derived: RocksDbDerivedStore,
}

impl RocksDbMessageStoreRoot {
    pub const fn new(derived: RocksDbDerivedStore) -> Self {
        Self { derived }
    }

    pub const fn derived(&self) -> &RocksDbDerivedStore {
        &self.derived
    }

    pub fn derived_mut(&mut self) -> &mut RocksDbDerivedStore {
        &mut self.derived
    }
}

impl RocksDbMessageStoreRoot {
    pub fn read<L, CqFilter, MessageFilter>(
        &self,
        local: &L,
        request: RocksDbReadRequest<'_>,
        mut cq_filter: CqFilter,
        mut message_filter: MessageFilter,
    ) -> Result<RocksDbReadResult<L::Selection>, StoreError>
    where
        L: WalPort,
        CqFilter: FnMut(i64) -> bool,
        MessageFilter: FnMut(&[u8]) -> bool,
    {
        let min_offset = self.derived.min_offset(request.topic, request.queue_id)?;
        let max_offset = self.derived.max_offset(request.topic, request.queue_id)?;
        let mut status = GetStatus::NoMessageInQueue;
        let mut next_begin_offset = request.offset;
        let mut records = Vec::new();
        let mut buffer_total_size = 0;
        let mut message_count = 0;

        if max_offset == 0 {
            next_begin_offset = local.correct_queue_offset(request.offset, 0);
        } else if request.offset < min_offset {
            status = GetStatus::OffsetTooSmall;
            next_begin_offset = local.correct_queue_offset(request.offset, min_offset);
        } else if request.offset == max_offset {
            status = GetStatus::OffsetOverflowOne;
            next_begin_offset = local.correct_queue_offset(request.offset, request.offset);
        } else if request.offset > max_offset {
            status = GetStatus::OffsetOverflowBadly;
            next_begin_offset = local.correct_queue_offset(request.offset, max_offset);
        } else {
            status = GetStatus::NoMatchedMessage;
            let max_pull_size = request.max_total_message_size.clamp(100, request.max_pull_message_size);
            let read_count = request.max_message_count.max(0);
            let values = self.derived.consume_queue_store.range_query_values(
                request.topic.to_string(),
                request.queue_id,
                request.offset,
                read_count,
            )?;
            if values.is_empty() {
                status = GetStatus::OffsetFoundNull;
                next_begin_offset = local.correct_queue_offset(request.offset, request.offset.saturating_add(1));
            } else {
                for (index, value) in values.into_iter().enumerate() {
                    if message_count >= request.max_message_count || buffer_total_size >= max_pull_size {
                        break;
                    }
                    let queue_offset = request.offset + index as i64;
                    next_begin_offset = queue_offset + 1;
                    if !cq_filter(value.tag_hash_code) {
                        continue;
                    }
                    let Some(selection) = local.read_message(value.commit_log_physical_offset, value.body_size)? else {
                        if buffer_total_size == 0 {
                            status = GetStatus::MessageWasRemoving;
                        }
                        continue;
                    };
                    let bytes = local.selection_bytes(&selection);
                    if !message_filter(bytes) {
                        continue;
                    }
                    let size = i32::try_from(bytes.len()).unwrap_or(i32::MAX);
                    buffer_total_size = buffer_total_size.saturating_add(size);
                    message_count += 1;
                    records.push(RocksDbReadRecord {
                        selection,
                        queue_offset: queue_offset as u64,
                        batch_count: 1,
                    });
                    status = GetStatus::Found;
                }
            }
        }

        Ok(RocksDbReadResult {
            records,
            status,
            next_begin_offset,
            min_offset,
            max_offset,
            buffer_total_size,
            message_count,
        })
    }

    pub fn query_index<L>(
        &self,
        local: &L,
        topic: &str,
        key: &str,
        index_type: Option<&str>,
        max_num: usize,
        begin: i64,
        end: i64,
        last_key: Option<&str>,
        max_query_days: usize,
    ) -> Result<RocksDbIndexLookup<L::Selection>, StoreError>
    where
        L: WalPort,
    {
        let (offsets, last_update_timestamp, last_update_physical_offset) =
            self.derived
                .index_offsets(topic, key, index_type, max_num, begin, end, last_key, max_query_days)?;
        let mut records = Vec::with_capacity(offsets.len());
        for offset in offsets {
            if let Some(selection) = local.read_from(offset)? {
                records.push(selection);
            }
        }
        Ok(RocksDbIndexLookup {
            records,
            last_update_timestamp,
            last_update_physical_offset,
        })
    }
}

fn normalize_index_query_time_range(begin: i64, end: i64, max_query_days: usize) -> (i64, i64) {
    if begin > 0 && end > 0 && begin <= end && end != i64::MAX {
        return (begin, end);
    }
    let end = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX));
    let max_query_days = i64::try_from(max_query_days).unwrap_or(i64::MAX / MILLIS_PER_DAY);
    let begin = end.saturating_sub(max_query_days.saturating_mul(MILLIS_PER_DAY));
    (begin, end)
}

#[cfg(test)]
#[path = "message_store/tests.rs"]
mod tests;
