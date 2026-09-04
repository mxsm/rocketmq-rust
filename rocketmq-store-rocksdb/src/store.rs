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

mod operations;

use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;

use crate::batch::RocksDbBatchOperation;
use crate::batch::RocksDbWriteBatch;
use crate::config::RocksDbColumnFamilyConfig;
use crate::config::RocksDbConfig;
use crate::error::rocksdb_contract_error;
use crate::error::rocksdb_source_error;
use crate::error::RocksDbStoreResultExt;
use crate::iterator::RocksDbRangeScanOptions;
use crate::iterator::RocksDbScanItem;
use crate::iterator::RocksDbScanOptions;
use crate::message_store::RocksDbOpenPlan;
use crate::options::RocksDbOptionsFactory;
use crate::resource_budget::RocksDbResourceBudget;
use crate::runtime::RocksDbRuntimeScope;
use crate::snapshot::RocksDbSnapshot;
use operations::compact_range_cf_on_db;
use operations::create_dir_all_for_rocksdb_operation;
use operations::merge_existing_column_families;
use operations::prefix_scan_on_db;
use operations::range_scan_on_db;
use operations::unavailable_state;
use rocketmq_observability::metrics::rocksdb::RocksDbMetrics;
use rocketmq_observability::metrics::rocksdb::RocksDbMetricsCollector;
use rocketmq_observability::metrics::rocksdb::RocksDbMetricsRecorder;
use rocketmq_observability::metrics::rocksdb::RocksDbTickerMetrics;
use tracing::warn;

pub trait KeyValueStore {
    fn put_cf(&self, operation: StoreOperation, cf: &str, key: &[u8], value: &[u8]) -> Result<(), StoreError>;

    fn get_cf(&self, operation: StoreOperation, cf: &str, key: &[u8]) -> Result<Option<Bytes>, StoreError>;

    fn delete_cf(&self, operation: StoreOperation, cf: &str, key: &[u8]) -> Result<(), StoreError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RocksDbStoreState {
    Open,
    Reloading,
    Closed,
}

impl RocksDbStoreState {
    fn as_u8(self) -> u8 {
        match self {
            Self::Open => 0,
            Self::Reloading => 1,
            Self::Closed => 2,
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Open,
            1 => Self::Reloading,
            _ => Self::Closed,
        }
    }
}

pub struct RocksDbStore {
    db: Arc<::rocksdb::DB>,
    path: PathBuf,
    db_options: ::rocksdb::Options,
    resource_budget: Arc<RocksDbResourceBudget>,
    state: AtomicU8,
    write_options: ::rocksdb::WriteOptions,
    metrics: Arc<RocksDbMetricsCollector>,
    otel_metrics: RocksDbMetricsRecorder,
}

impl RocksDbStore {
    /// Opens a database from raw configuration.
    ///
    /// Returns `Ok(None)` when RocksDB is disabled or the deterministic
    /// configuration is invalid.
    ///
    /// # Errors
    ///
    /// Returns an operational storage error when resource initialization or
    /// the native database open fails.
    pub fn open(config: RocksDbConfig) -> Result<Option<Self>, StoreError> {
        Self::open_with_metrics(config, RocksDbMetricsRecorder::noop())
    }

    /// Opens a database from raw configuration with metrics.
    ///
    /// Returns `Ok(None)` when RocksDB is disabled or the deterministic
    /// configuration is invalid.
    ///
    /// # Errors
    ///
    /// Returns an operational storage error when resource initialization or
    /// the native database open fails.
    pub fn open_with_metrics(
        config: RocksDbConfig,
        otel_metrics: RocksDbMetricsRecorder,
    ) -> Result<Option<Self>, StoreError> {
        let Some(plan) = RocksDbOpenPlan::from_config(config) else {
            return Ok(None);
        };
        Self::open_planned_with_metrics(plan, otel_metrics).map(Some)
    }

    /// Opens a database from a validated capability.
    ///
    /// # Errors
    ///
    /// Returns an operational storage error when resource initialization or
    /// the native database open fails.
    pub fn open_planned(plan: RocksDbOpenPlan) -> Result<Self, StoreError> {
        Self::open_planned_with_metrics(plan, RocksDbMetricsRecorder::noop())
    }

    /// Opens a database from a validated capability with metrics.
    ///
    /// # Errors
    ///
    /// Returns an operational storage error when resource initialization or
    /// the native database open fails.
    pub fn open_planned_with_metrics(
        plan: RocksDbOpenPlan,
        otel_metrics: RocksDbMetricsRecorder,
    ) -> Result<Self, StoreError> {
        let RocksDbOpenPlan { config } = plan;
        let resource_budget = Arc::new(RocksDbResourceBudget::from_config(&config)?);
        Self::open_validated_with_metrics_and_resource_budget(config, otel_metrics, resource_budget)
    }

    /// Opens a validated database using a caller-owned native-memory budget.
    ///
    /// # Errors
    ///
    /// Returns an operational storage error when resource initialization or
    /// the native database open fails.
    pub fn open_planned_with_metrics_and_resource_budget(
        plan: RocksDbOpenPlan,
        otel_metrics: RocksDbMetricsRecorder,
        resource_budget: Arc<RocksDbResourceBudget>,
    ) -> Result<Self, StoreError> {
        let RocksDbOpenPlan { config } = plan;
        Self::open_validated_with_metrics_and_resource_budget(config, otel_metrics, resource_budget)
    }

    /// Opens a database using a caller-owned native-memory budget.
    ///
    /// Reusing the same budget across database instances makes their block
    /// cache and memtables obey one process-level limit.
    pub(crate) fn open_validated_with_metrics_and_resource_budget(
        config: RocksDbConfig,
        otel_metrics: RocksDbMetricsRecorder,
        resource_budget: Arc<RocksDbResourceBudget>,
    ) -> Result<Self, StoreError> {
        let db_options = RocksDbOptionsFactory::db_options_with_resource_budget(&config, resource_budget.as_ref())?;
        Self::open_with_column_families(
            config.clone(),
            db_options,
            config.column_families,
            otel_metrics,
            resource_budget,
        )
    }

    /// Opens a database from raw configuration while retaining existing
    /// column families.
    ///
    /// Returns `Ok(None)` when RocksDB is disabled or the deterministic
    /// configuration is invalid.
    ///
    /// # Errors
    ///
    /// Returns an operational storage error when resource initialization,
    /// existing column-family inspection, or the native database open fails.
    pub fn open_with_existing_column_families(config: RocksDbConfig) -> Result<Option<Self>, StoreError> {
        Self::open_with_existing_column_families_and_metrics(config, RocksDbMetricsRecorder::noop())
    }

    /// Opens a database from raw configuration with metrics while retaining
    /// existing column families.
    ///
    /// Returns `Ok(None)` when RocksDB is disabled or the deterministic
    /// configuration is invalid.
    ///
    /// # Errors
    ///
    /// Returns an operational storage error when resource initialization,
    /// existing column-family inspection, or the native database open fails.
    pub fn open_with_existing_column_families_and_metrics(
        config: RocksDbConfig,
        otel_metrics: RocksDbMetricsRecorder,
    ) -> Result<Option<Self>, StoreError> {
        let Some(plan) = RocksDbOpenPlan::from_config(config) else {
            return Ok(None);
        };
        Self::open_planned_with_existing_column_families_and_metrics(plan, otel_metrics).map(Some)
    }

    /// Opens a database from a validated capability while retaining existing
    /// column families.
    ///
    /// # Errors
    ///
    /// Returns an operational storage error when existing column families
    /// cannot be inspected or the native database cannot be opened.
    pub fn open_planned_with_existing_column_families_and_metrics(
        plan: RocksDbOpenPlan,
        otel_metrics: RocksDbMetricsRecorder,
    ) -> Result<Self, StoreError> {
        let RocksDbOpenPlan { config } = plan;
        let resource_budget = Arc::new(RocksDbResourceBudget::from_config(&config)?);
        let db_options = RocksDbOptionsFactory::db_options_with_resource_budget(&config, resource_budget.as_ref())?;
        let column_families = merge_existing_column_families(&config, &db_options)?;
        Self::open_with_column_families(config, db_options, column_families, otel_metrics, resource_budget)
    }

    fn open_with_column_families(
        config: RocksDbConfig,
        db_options: ::rocksdb::Options,
        column_families: Vec<RocksDbColumnFamilyConfig>,
        otel_metrics: RocksDbMetricsRecorder,
        resource_budget: Arc<RocksDbResourceBudget>,
    ) -> Result<Self, StoreError> {
        let path = config.path.clone();
        let descriptors = column_families
            .iter()
            .map(|column_family| {
                RocksDbOptionsFactory::cf_options_with_resource_budget(column_family, resource_budget.as_ref())
                    .map(|options| ::rocksdb::ColumnFamilyDescriptor::new(column_family.name.clone(), options))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let db = if descriptors.is_empty() {
            ::rocksdb::DB::open(&db_options, &config.path)
                .map_store(&rocketmq_error::STORAGE_BACKEND_UNAVAILABLE, StoreOperation::Load)?
        } else {
            ::rocksdb::DB::open_cf_descriptors(&db_options, &config.path, descriptors)
                .map_store(&rocketmq_error::STORAGE_BACKEND_UNAVAILABLE, StoreOperation::Load)?
        };

        Ok(Self {
            db: Arc::new(db),
            path,
            db_options,
            resource_budget,
            state: AtomicU8::new(RocksDbStoreState::Open.as_u8()),
            write_options: RocksDbOptionsFactory::write_options(config.write_profile()),
            metrics: Arc::new(RocksDbMetricsCollector::default()),
            otel_metrics,
        })
    }

    /// Returns the canonical configured database path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the native-memory budget shared by this database and its column families.
    pub fn resource_budget(&self) -> Arc<RocksDbResourceBudget> {
        Arc::clone(&self.resource_budget)
    }

    pub fn create_cf_if_missing(
        &self,
        operation: StoreOperation,
        column_family: RocksDbColumnFamilyConfig,
    ) -> Result<(), StoreError> {
        self.ensure_open(operation)?;
        if self.db.cf_handle(&column_family.name).is_some() {
            return Ok(());
        }
        column_family.validate()?;
        let options =
            RocksDbOptionsFactory::cf_options_with_resource_budget(&column_family, self.resource_budget.as_ref())?;
        let result = self
            .db
            .create_cf(&column_family.name, &options)
            .map_err(|error| self.map_native_error(error, &rocketmq_error::STORAGE_WRITE_FAILED, operation));
        self.record_result(&result, RocksDbMetricsCollector::record_write);
        result
    }

    pub fn write_batch(&self, operation: StoreOperation, batch: &RocksDbWriteBatch) -> Result<(), StoreError> {
        self.ensure_open(operation)?;
        if batch.is_empty() {
            return Ok(());
        }

        let mut rocksdb_batch = ::rocksdb::WriteBatch::default();
        for batch_operation in batch.operations() {
            match batch_operation {
                RocksDbBatchOperation::Put { cf, key, value } => {
                    let handle = self.cf_handle(cf, operation)?;
                    rocksdb_batch.put_cf(&handle, key, value);
                }
                RocksDbBatchOperation::Delete { cf, key } => {
                    let handle = self.cf_handle(cf, operation)?;
                    rocksdb_batch.delete_cf(&handle, key);
                }
                RocksDbBatchOperation::DeleteRange { cf, start_key, end_key } => {
                    let handle = self.cf_handle(cf, operation)?;
                    rocksdb_batch.delete_range_cf(&handle, start_key, end_key);
                }
            }
        }

        let result = self
            .db
            .write_opt(rocksdb_batch, &self.write_options)
            .map_err(|error| self.map_native_error(error, &rocketmq_error::STORAGE_WRITE_FAILED, operation));
        self.record_result(&result, RocksDbMetricsCollector::record_batch_write);
        result
    }

    pub fn prefix_scan(
        &self,
        operation: StoreOperation,
        options: &RocksDbScanOptions,
    ) -> Result<Vec<RocksDbScanItem>, StoreError> {
        self.ensure_open(operation)?;
        let handle = self.cf_handle(&options.cf, operation)?;
        let iter = self.db.iterator_cf(
            &handle,
            ::rocksdb::IteratorMode::From(&options.prefix, ::rocksdb::Direction::Forward),
        );
        let mut items = Vec::new();
        for item in iter {
            let (key, value) =
                item.map_err(|error| self.map_native_error(error, &rocketmq_error::STORAGE_READ_FAILED, operation))?;
            if !key.starts_with(&options.prefix) {
                break;
            }
            items.push(RocksDbScanItem {
                key: Bytes::copy_from_slice(&key),
                value: Bytes::copy_from_slice(&value),
            });
            if options.limit > 0 && items.len() >= options.limit {
                break;
            }
        }
        let result = Ok(items);
        self.record_result(&result, RocksDbMetricsCollector::record_scan);
        result
    }

    /// Scans a prefix beginning at an arbitrary seek key.
    pub fn prefix_scan_from(
        &self,
        operation: StoreOperation,
        options: &RocksDbScanOptions,
        start: &[u8],
    ) -> Result<Vec<RocksDbScanItem>, StoreError> {
        self.ensure_open(operation)?;
        let handle = self.cf_handle(&options.cf, operation)?;
        let iter = self.db.iterator_cf(
            &handle,
            ::rocksdb::IteratorMode::From(start, ::rocksdb::Direction::Forward),
        );
        let mut items = Vec::new();
        for item in iter {
            let (key, value) =
                item.map_err(|error| self.map_native_error(error, &rocketmq_error::STORAGE_READ_FAILED, operation))?;
            if !key.starts_with(&options.prefix) {
                break;
            }
            items.push(RocksDbScanItem {
                key: Bytes::copy_from_slice(&key),
                value: Bytes::copy_from_slice(&value),
            });
            if options.limit > 0 && items.len() >= options.limit {
                break;
            }
        }
        let result = Ok(items);
        self.record_result(&result, RocksDbMetricsCollector::record_scan);
        result
    }

    pub async fn prefix_scan_blocking(
        &self,
        runtime_scope: &RocksDbRuntimeScope,
        operation: StoreOperation,
        options: RocksDbScanOptions,
    ) -> Result<Vec<RocksDbScanItem>, StoreError> {
        self.ensure_open(operation)?;
        let db = Arc::clone(&self.db);
        let result = crate::runtime::spawn_io(runtime_scope, "rocksdb.prefix_scan", operation, move || {
            prefix_scan_on_db(&db, operation, &options)
        })
        .await?;
        self.record_result(&result, RocksDbMetricsCollector::record_scan);
        result
    }

    pub fn range_scan(
        &self,
        operation: StoreOperation,
        options: &RocksDbRangeScanOptions,
    ) -> Result<Vec<RocksDbScanItem>, StoreError> {
        self.ensure_open(operation)?;
        let handle = self.cf_handle(&options.cf, operation)?;
        let iter = self.db.iterator_cf(
            &handle,
            ::rocksdb::IteratorMode::From(&options.start, ::rocksdb::Direction::Forward),
        );
        let mut items = Vec::new();
        for item in iter {
            let (key, value) =
                item.map_err(|error| self.map_native_error(error, &rocketmq_error::STORAGE_READ_FAILED, operation))?;
            if !options.end.is_empty() && key.as_ref() >= options.end.as_slice() {
                break;
            }
            items.push(RocksDbScanItem {
                key: Bytes::copy_from_slice(&key),
                value: Bytes::copy_from_slice(&value),
            });
            if options.limit > 0 && items.len() >= options.limit {
                break;
            }
        }
        let result = Ok(items);
        self.record_result(&result, RocksDbMetricsCollector::record_scan);
        result
    }

    pub async fn range_scan_blocking(
        &self,
        runtime_scope: &RocksDbRuntimeScope,
        operation: StoreOperation,
        options: RocksDbRangeScanOptions,
    ) -> Result<Vec<RocksDbScanItem>, StoreError> {
        self.ensure_open(operation)?;
        let db = Arc::clone(&self.db);
        let result = crate::runtime::spawn_io(runtime_scope, "rocksdb.range_scan", operation, move || {
            range_scan_on_db(&db, operation, &options)
        })
        .await?;
        self.record_result(&result, RocksDbMetricsCollector::record_scan);
        result
    }

    pub fn snapshot(&self, operation: StoreOperation) -> Result<RocksDbSnapshot<'_>, StoreError> {
        self.ensure_open(operation)?;
        Ok(RocksDbSnapshot::new(self.db.as_ref()))
    }

    /// Reads a bounded group of keys from one column family with one native multi-get call.
    pub fn multi_get_cf(
        &self,
        operation: StoreOperation,
        cf: &str,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Bytes>>, StoreError> {
        self.ensure_open(operation)?;
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let handle = self.cf_handle(cf, operation)?;
        let results = self.db.multi_get_cf(keys.iter().map(|key| (&handle, key.as_slice())));
        let mut values = Vec::with_capacity(results.len());
        for result in results {
            values.push(
                result
                    .map(|value| value.map(Bytes::from))
                    .map_err(|error| self.map_native_error(error, &rocketmq_error::STORAGE_READ_FAILED, operation))?,
            );
        }
        Ok(values)
    }

    pub fn flush(&self, operation: StoreOperation) -> Result<(), StoreError> {
        self.ensure_open(operation)?;
        let result = self
            .db
            .flush()
            .map_err(|error| self.map_native_error(error, &rocketmq_error::STORAGE_WRITE_FAILED, operation));
        self.record_result(&result, RocksDbMetricsCollector::record_flush);
        result
    }

    pub fn flush_wal(&self, operation: StoreOperation, sync: bool) -> Result<(), StoreError> {
        self.ensure_open(operation)?;
        let result = self
            .db
            .flush_wal(sync)
            .map_err(|error| self.map_native_error(error, &rocketmq_error::STORAGE_WRITE_FAILED, operation));
        self.record_result(&result, RocksDbMetricsCollector::record_flush);
        result
    }

    pub fn compact_range_cf(
        &self,
        operation: StoreOperation,
        cf: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<(), StoreError> {
        self.ensure_open(operation)?;
        let result = compact_range_cf_on_db(&self.db, operation, cf, start, end);
        self.record_result(&result, RocksDbMetricsCollector::record_manual_compaction);
        result
    }

    pub async fn compact_range_cf_blocking(
        &self,
        runtime_scope: &RocksDbRuntimeScope,
        operation: StoreOperation,
        cf: String,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
    ) -> Result<(), StoreError> {
        self.ensure_open(operation)?;
        let db = Arc::clone(&self.db);
        let metrics = Arc::clone(&self.metrics);
        let result = crate::runtime::spawn_io(runtime_scope, "rocksdb.compact_range", operation, move || {
            compact_range_cf_on_db(&db, operation, &cf, start.as_deref(), end.as_deref())?;
            metrics.record_manual_compaction();
            Ok(())
        })
        .await?;
        if result.is_err() {
            self.metrics.record_error();
        }
        result
    }

    pub fn compact_range_cf_background(
        &self,
        runtime_scope: &RocksDbRuntimeScope,
        operation: StoreOperation,
        cf: String,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
    ) -> Result<(), StoreError> {
        self.ensure_open(operation)?;
        let db = Arc::clone(&self.db);
        let metrics = Arc::clone(&self.metrics);
        crate::runtime::spawn_background_io(runtime_scope, "rocksdb.manual_compaction", operation, move || {
            if let Err(error) = compact_range_cf_on_db(&db, operation, &cf, start.as_deref(), end.as_deref()) {
                warn!(
                    descriptor = ?error.descriptor().code(),
                    operation = ?error.operation(),
                    component = ?error.component(),
                    source_present = std::error::Error::source(&error).is_some(),
                    "RocksDB background compaction failed"
                );
                metrics.record_error();
                return;
            }
            metrics.record_manual_compaction();
        })
        .map(|_| ())
    }

    pub fn manual_compaction_count(&self) -> u64 {
        self.metrics().manual_compaction_count
    }

    pub fn metrics(&self) -> RocksDbMetrics {
        self.metrics.snapshot()
    }

    pub fn metrics_recorder(&self) -> RocksDbMetricsRecorder {
        self.otel_metrics.clone()
    }

    pub fn ticker_metrics(&self) -> RocksDbTickerMetrics {
        use ::rocksdb::statistics::Ticker;

        RocksDbTickerMetrics {
            bytes_written: self.db_options.get_ticker_count(Ticker::BytesWritten),
            bytes_read: self.db_options.get_ticker_count(Ticker::BytesRead),
            times_written_self: self.db_options.get_ticker_count(Ticker::WriteDoneBySelf),
            times_written_other: self.db_options.get_ticker_count(Ticker::WriteDoneByOther),
            block_cache_hit: self.db_options.get_ticker_count(Ticker::BlockCacheHit),
            block_cache_miss: self.db_options.get_ticker_count(Ticker::BlockCacheMiss),
            times_compressed: self.db_options.get_ticker_count(Ticker::NumberBlockCompressed),
            read_amplification_bytes: self.db_options.get_ticker_count(Ticker::ReadAmpTotalReadBytes),
            times_read: self.db_options.get_ticker_count(Ticker::NumberKeysRead),
        }
    }

    pub fn property_value(&self, operation: StoreOperation, property: &str) -> Result<Option<String>, StoreError> {
        self.ensure_open(operation)?;
        let result = self
            .db
            .property_value(property)
            .map_err(|error| self.map_native_error(error, &rocketmq_error::STORAGE_READ_FAILED, operation));
        self.record_result(&result, RocksDbMetricsCollector::record_property_query);
        result
    }

    pub fn property_value_cf(
        &self,
        operation: StoreOperation,
        cf: &str,
        property: &str,
    ) -> Result<Option<String>, StoreError> {
        self.ensure_open(operation)?;
        let handle = self.cf_handle(cf, operation)?;
        let result = self
            .db
            .property_value_cf(&handle, property)
            .map_err(|error| self.map_native_error(error, &rocketmq_error::STORAGE_READ_FAILED, operation));
        self.record_result(&result, RocksDbMetricsCollector::record_property_query);
        result
    }

    pub fn property_int_value(&self, operation: StoreOperation, property: &str) -> Result<Option<u64>, StoreError> {
        self.ensure_open(operation)?;
        let result = self
            .db
            .property_int_value(property)
            .map_err(|error| self.map_native_error(error, &rocketmq_error::STORAGE_READ_FAILED, operation));
        self.record_result(&result, RocksDbMetricsCollector::record_property_query);
        result
    }

    pub fn property_int_value_cf(
        &self,
        operation: StoreOperation,
        cf: &str,
        property: &str,
    ) -> Result<Option<u64>, StoreError> {
        self.ensure_open(operation)?;
        let handle = self.cf_handle(cf, operation)?;
        let result = self
            .db
            .property_int_value_cf(&handle, property)
            .map_err(|error| self.map_native_error(error, &rocketmq_error::STORAGE_READ_FAILED, operation));
        self.record_result(&result, RocksDbMetricsCollector::record_property_query);
        result
    }

    pub async fn create_checkpoint(
        &self,
        runtime_scope: &RocksDbRuntimeScope,
        target_dir: PathBuf,
    ) -> Result<(), StoreError> {
        self.ensure_open(StoreOperation::Flush)?;
        let db = Arc::clone(&self.db);
        let result = crate::runtime::spawn_io(
            runtime_scope,
            "rocksdb.create_checkpoint",
            StoreOperation::Flush,
            move || {
                let checkpoint = ::rocksdb::checkpoint::Checkpoint::new(&db)
                    .map_store(&rocketmq_error::STORAGE_WRITE_FAILED, StoreOperation::Flush)?;
                checkpoint
                    .create_checkpoint(target_dir)
                    .map_store(&rocketmq_error::STORAGE_WRITE_FAILED, StoreOperation::Flush)
            },
        )
        .await?;
        self.record_result(&result, RocksDbMetricsCollector::record_checkpoint);
        result
    }

    /// Returns the latest committed RocksDB sequence number.
    pub fn latest_sequence_number(&self) -> Result<u64, StoreError> {
        self.ensure_open(StoreOperation::Read)?;
        Ok(self.db.latest_sequence_number())
    }

    /// Creates a checkpoint on the caller's owned blocking executor.
    ///
    /// This synchronous boundary exists for cross-store snapshots that must keep an external
    /// payload barrier held until the Timeline checkpoint has captured the same generation.
    pub fn create_checkpoint_blocking(&self, target_dir: PathBuf) -> Result<(), StoreError> {
        self.ensure_open(StoreOperation::Flush)?;
        let checkpoint = ::rocksdb::checkpoint::Checkpoint::new(&self.db)
            .map_store(&rocketmq_error::STORAGE_WRITE_FAILED, StoreOperation::Flush)?;
        checkpoint
            .create_checkpoint(target_dir)
            .map_store(&rocketmq_error::STORAGE_WRITE_FAILED, StoreOperation::Flush)
    }

    /// Creates a RocksDB checkpoint without admitting or waiting past `deadline`.
    pub async fn create_checkpoint_until(
        &self,
        runtime_scope: &RocksDbRuntimeScope,
        target_dir: PathBuf,
        deadline: rocketmq_runtime::ShutdownDeadline,
    ) -> Result<(), StoreError> {
        self.ensure_open(StoreOperation::Flush)?;
        let db = Arc::clone(&self.db);
        let result = crate::runtime::spawn_io_until(
            runtime_scope,
            "rocksdb.create_release_checkpoint",
            StoreOperation::Flush,
            deadline,
            move || {
                let checkpoint = ::rocksdb::checkpoint::Checkpoint::new(&db)
                    .map_store(&rocketmq_error::STORAGE_WRITE_FAILED, StoreOperation::Flush)?;
                checkpoint
                    .create_checkpoint(target_dir)
                    .map_store(&rocketmq_error::STORAGE_WRITE_FAILED, StoreOperation::Flush)
            },
        )
        .await?;
        self.record_result(&result, RocksDbMetricsCollector::record_checkpoint);
        result
    }

    pub async fn create_backup(
        &self,
        runtime_scope: &RocksDbRuntimeScope,
        backup_dir: PathBuf,
    ) -> Result<(), StoreError> {
        self.ensure_open(StoreOperation::Flush)?;
        let db = Arc::clone(&self.db);
        let result = crate::runtime::spawn_io(
            runtime_scope,
            "rocksdb.create_backup",
            StoreOperation::Flush,
            move || {
                create_dir_all_for_rocksdb_operation(&backup_dir, StoreOperation::Flush)?;
                let env =
                    ::rocksdb::Env::new().map_store(&rocketmq_error::STORAGE_WRITE_FAILED, StoreOperation::Flush)?;
                let backup_options = ::rocksdb::backup::BackupEngineOptions::new(&backup_dir)
                    .map_store(&rocketmq_error::STORAGE_WRITE_FAILED, StoreOperation::Flush)?;
                let mut backup_engine = ::rocksdb::backup::BackupEngine::open(&backup_options, &env)
                    .map_store(&rocketmq_error::STORAGE_WRITE_FAILED, StoreOperation::Flush)?;
                backup_engine
                    .create_new_backup_flush(db.as_ref(), true)
                    .map_store(&rocketmq_error::STORAGE_WRITE_FAILED, StoreOperation::Flush)
            },
        )
        .await?;
        self.record_result(&result, RocksDbMetricsCollector::record_backup);
        result
    }

    pub async fn restore_latest_backup(
        runtime_scope: &RocksDbRuntimeScope,
        backup_dir: PathBuf,
        db_dir: PathBuf,
        wal_dir: Option<PathBuf>,
    ) -> Result<(), StoreError> {
        crate::runtime::spawn_io(
            runtime_scope,
            "rocksdb.restore_latest_backup",
            StoreOperation::Read,
            move || {
                create_dir_all_for_rocksdb_operation(&backup_dir, StoreOperation::Read)?;
                create_dir_all_for_rocksdb_operation(&db_dir, StoreOperation::Read)?;
                let wal_dir = wal_dir.unwrap_or_else(|| db_dir.clone());
                create_dir_all_for_rocksdb_operation(&wal_dir, StoreOperation::Read)?;

                let env =
                    ::rocksdb::Env::new().map_store(&rocketmq_error::STORAGE_READ_FAILED, StoreOperation::Read)?;
                let backup_options = ::rocksdb::backup::BackupEngineOptions::new(&backup_dir)
                    .map_store(&rocketmq_error::STORAGE_READ_FAILED, StoreOperation::Read)?;
                let mut backup_engine = ::rocksdb::backup::BackupEngine::open(&backup_options, &env)
                    .map_store(&rocketmq_error::STORAGE_READ_FAILED, StoreOperation::Read)?;
                let mut restore_options = ::rocksdb::backup::RestoreOptions::default();
                restore_options.set_keep_log_files(false);
                backup_engine
                    .restore_from_latest_backup(&db_dir, &wal_dir, &restore_options)
                    .map_store(&rocketmq_error::STORAGE_READ_FAILED, StoreOperation::Read)
            },
        )
        .await?
    }

    pub fn close(&self) {
        self.state.store(RocksDbStoreState::Closed.as_u8(), Ordering::Release);
    }

    pub fn state(&self) -> RocksDbStoreState {
        RocksDbStoreState::from_u8(self.state.load(Ordering::Acquire))
    }

    pub fn mark_reloading(&self) {
        let _ = self.state.compare_exchange(
            RocksDbStoreState::Open.as_u8(),
            RocksDbStoreState::Reloading.as_u8(),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub fn mark_recovered(&self) -> Result<(), StoreError> {
        match self.state() {
            RocksDbStoreState::Open => Ok(()),
            RocksDbStoreState::Closed => Err(unavailable_state(StoreOperation::Start)),
            RocksDbStoreState::Reloading => match self.state.compare_exchange(
                RocksDbStoreState::Reloading.as_u8(),
                RocksDbStoreState::Open.as_u8(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => Ok(()),
                Err(state) => match RocksDbStoreState::from_u8(state) {
                    RocksDbStoreState::Open => Ok(()),
                    RocksDbStoreState::Reloading => Err(unavailable_state(StoreOperation::Start)),
                    RocksDbStoreState::Closed => Err(unavailable_state(StoreOperation::Start)),
                },
            },
        }
    }

    fn cf_handle(
        &self,
        cf: &str,
        operation: StoreOperation,
    ) -> Result<Arc<::rocksdb::BoundColumnFamily<'_>>, StoreError> {
        let result = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| rocksdb_contract_error(&rocketmq_error::STORAGE_STATE_CORRUPTED, operation));
        if result.is_err() {
            self.metrics.record_error();
        }
        result
    }

    fn ensure_open(&self, operation: StoreOperation) -> Result<(), StoreError> {
        match self.state() {
            RocksDbStoreState::Open => Ok(()),
            RocksDbStoreState::Reloading | RocksDbStoreState::Closed => Err(unavailable_state(operation)),
        }
    }

    fn record_result<T>(&self, result: &Result<T, StoreError>, success_recorder: fn(&RocksDbMetricsCollector)) {
        match result {
            Ok(_) => success_recorder(&self.metrics),
            Err(_) => self.metrics.record_error(),
        }
    }

    fn map_native_error(
        &self,
        error: ::rocksdb::Error,
        descriptor: &'static rocketmq_error::ErrorDescriptor,
        operation: StoreOperation,
    ) -> StoreError {
        if matches!(
            error.kind(),
            ::rocksdb::ErrorKind::Aborted | ::rocksdb::ErrorKind::Corruption | ::rocksdb::ErrorKind::Unknown
        ) {
            self.mark_reloading();
        }
        rocksdb_source_error(descriptor, operation, error)
    }
}

impl KeyValueStore for RocksDbStore {
    fn put_cf(&self, operation: StoreOperation, cf: &str, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.ensure_open(operation)?;
        let handle = self.cf_handle(cf, operation)?;
        let result = self
            .db
            .put_cf_opt(&handle, key, value, &self.write_options)
            .map_err(|error| self.map_native_error(error, &rocketmq_error::STORAGE_WRITE_FAILED, operation));
        self.record_result(&result, RocksDbMetricsCollector::record_write);
        result
    }

    fn get_cf(&self, operation: StoreOperation, cf: &str, key: &[u8]) -> Result<Option<Bytes>, StoreError> {
        self.ensure_open(operation)?;
        let handle = self.cf_handle(cf, operation)?;
        let result = self
            .db
            .get_cf(&handle, key)
            .map(|value| value.map(Bytes::from))
            .map_err(|error| self.map_native_error(error, &rocketmq_error::STORAGE_READ_FAILED, operation));
        self.record_result(&result, RocksDbMetricsCollector::record_read);
        result
    }

    fn delete_cf(&self, operation: StoreOperation, cf: &str, key: &[u8]) -> Result<(), StoreError> {
        self.ensure_open(operation)?;
        let handle = self.cf_handle(cf, operation)?;
        let result = self
            .db
            .delete_cf_opt(&handle, key, &self.write_options)
            .map_err(|error| self.map_native_error(error, &rocketmq_error::STORAGE_WRITE_FAILED, operation));
        self.record_result(&result, RocksDbMetricsCollector::record_write);
        result
    }
}
