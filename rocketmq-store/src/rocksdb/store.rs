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

use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use rocketmq_error::RocketMQError;

use crate::rocksdb::batch::RocksDbBatchOperation;
use crate::rocksdb::batch::RocksDbWriteBatch;
use crate::rocksdb::config::RocksDbColumnFamilyConfig;
use crate::rocksdb::config::RocksDbConfig;
use crate::rocksdb::error::closed_error;
use crate::rocksdb::error::column_family_missing_error;
use crate::rocksdb::error::reloading_error;
use crate::rocksdb::error::RocksDbErrorKind;
use crate::rocksdb::error::RocksDbResultExt;
use crate::rocksdb::iterator::RocksDbRangeScanOptions;
use crate::rocksdb::iterator::RocksDbScanItem;
use crate::rocksdb::iterator::RocksDbScanOptions;
use crate::rocksdb::metrics::RocksDbMetrics;
use crate::rocksdb::metrics::RocksDbMetricsCollector;
use crate::rocksdb::metrics::RocksDbTickerMetrics;
use crate::rocksdb::options::RocksDbOptionsFactory;
use crate::rocksdb::snapshot::RocksDbSnapshot;
use tracing::warn;

pub trait KeyValueStore {
    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), RocketMQError>;

    fn get_cf(&self, cf: &str, key: &[u8]) -> Result<Option<Bytes>, RocketMQError>;

    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<(), RocketMQError>;
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
    db_options: ::rocksdb::Options,
    state: AtomicU8,
    write_options: ::rocksdb::WriteOptions,
    metrics: Arc<RocksDbMetricsCollector>,
}

impl RocksDbStore {
    pub fn open(config: RocksDbConfig) -> Result<Self, RocketMQError> {
        let db_options = RocksDbOptionsFactory::db_options(&config)?;
        Self::open_with_column_families(config.clone(), db_options, config.column_families)
    }

    pub fn open_with_existing_column_families(config: RocksDbConfig) -> Result<Self, RocketMQError> {
        let db_options = RocksDbOptionsFactory::db_options(&config)?;
        let column_families = merge_existing_column_families(&config, &db_options)?;
        Self::open_with_column_families(config, db_options, column_families)
    }

    fn open_with_column_families(
        config: RocksDbConfig,
        db_options: ::rocksdb::Options,
        column_families: Vec<RocksDbColumnFamilyConfig>,
    ) -> Result<Self, RocketMQError> {
        let descriptors = column_families
            .iter()
            .map(|column_family| {
                RocksDbOptionsFactory::cf_options(column_family)
                    .map(|options| ::rocksdb::ColumnFamilyDescriptor::new(column_family.name.clone(), options))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let db = if descriptors.is_empty() {
            ::rocksdb::DB::open(&db_options, &config.path).map_rocksdb(RocksDbErrorKind::Open)?
        } else {
            ::rocksdb::DB::open_cf_descriptors(&db_options, &config.path, descriptors)
                .map_rocksdb(RocksDbErrorKind::Open)?
        };

        Ok(Self {
            db: Arc::new(db),
            db_options,
            state: AtomicU8::new(RocksDbStoreState::Open.as_u8()),
            write_options: RocksDbOptionsFactory::write_options(config.write_profile()),
            metrics: Arc::new(RocksDbMetricsCollector::default()),
        })
    }

    pub fn create_cf_if_missing(&self, column_family: RocksDbColumnFamilyConfig) -> Result<(), RocketMQError> {
        self.ensure_open()?;
        if self.db.cf_handle(&column_family.name).is_some() {
            return Ok(());
        }
        column_family.validate()?;
        let options = RocksDbOptionsFactory::cf_options(&column_family)?;
        let result = self
            .db
            .create_cf(&column_family.name, &options)
            .map_err(|error| self.map_native_error(error, RocksDbErrorKind::ColumnFamilyCreate));
        self.record_result(&result, RocksDbMetricsCollector::record_write);
        result
    }

    pub fn write_batch(&self, batch: &RocksDbWriteBatch) -> Result<(), RocketMQError> {
        self.ensure_open()?;
        if batch.is_empty() {
            return Ok(());
        }

        let mut rocksdb_batch = ::rocksdb::WriteBatch::default();
        for operation in batch.operations() {
            match operation {
                RocksDbBatchOperation::Put { cf, key, value } => {
                    let handle = self.cf_handle(cf)?;
                    rocksdb_batch.put_cf(&handle, key, value);
                }
                RocksDbBatchOperation::Delete { cf, key } => {
                    let handle = self.cf_handle(cf)?;
                    rocksdb_batch.delete_cf(&handle, key);
                }
                RocksDbBatchOperation::DeleteRange { cf, start_key, end_key } => {
                    let handle = self.cf_handle(cf)?;
                    rocksdb_batch.delete_range_cf(&handle, start_key, end_key);
                }
            }
        }

        let result = self
            .db
            .write_opt(rocksdb_batch, &self.write_options)
            .map_err(|error| self.map_native_error(error, RocksDbErrorKind::Batch));
        self.record_result(&result, RocksDbMetricsCollector::record_batch_write);
        result
    }

    pub fn prefix_scan(&self, options: &RocksDbScanOptions) -> Result<Vec<RocksDbScanItem>, RocketMQError> {
        self.ensure_open()?;
        let handle = self.cf_handle(&options.cf)?;
        let iter = self.db.iterator_cf(
            &handle,
            ::rocksdb::IteratorMode::From(&options.prefix, ::rocksdb::Direction::Forward),
        );
        let mut items = Vec::new();
        for item in iter {
            let (key, value) = item.map_err(|error| self.map_native_error(error, RocksDbErrorKind::Iterator))?;
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
        options: RocksDbScanOptions,
    ) -> Result<Vec<RocksDbScanItem>, RocketMQError> {
        self.ensure_open()?;
        let db = Arc::clone(&self.db);
        let result = tokio::task::spawn_blocking(move || prefix_scan_on_db(&db, &options))
            .await
            .map_err(|error| RocketMQError::storage_read_failed("rocksdb", format!("Iterator: {error}")))?;
        self.record_result(&result, RocksDbMetricsCollector::record_scan);
        result
    }

    pub fn range_scan(&self, options: &RocksDbRangeScanOptions) -> Result<Vec<RocksDbScanItem>, RocketMQError> {
        self.ensure_open()?;
        let handle = self.cf_handle(&options.cf)?;
        let iter = self.db.iterator_cf(
            &handle,
            ::rocksdb::IteratorMode::From(&options.start, ::rocksdb::Direction::Forward),
        );
        let mut items = Vec::new();
        for item in iter {
            let (key, value) = item.map_err(|error| self.map_native_error(error, RocksDbErrorKind::Iterator))?;
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
        options: RocksDbRangeScanOptions,
    ) -> Result<Vec<RocksDbScanItem>, RocketMQError> {
        self.ensure_open()?;
        let db = Arc::clone(&self.db);
        let result = tokio::task::spawn_blocking(move || range_scan_on_db(&db, &options))
            .await
            .map_err(|error| RocketMQError::storage_read_failed("rocksdb", format!("Iterator: {error}")))?;
        self.record_result(&result, RocksDbMetricsCollector::record_scan);
        result
    }

    pub fn snapshot(&self) -> Result<RocksDbSnapshot<'_>, RocketMQError> {
        self.ensure_open()?;
        Ok(RocksDbSnapshot::new(self.db.as_ref()))
    }

    pub fn flush(&self) -> Result<(), RocketMQError> {
        self.ensure_open()?;
        let result = self
            .db
            .flush()
            .map_err(|error| self.map_native_error(error, RocksDbErrorKind::Flush));
        self.record_result(&result, RocksDbMetricsCollector::record_flush);
        result
    }

    pub fn flush_wal(&self, sync: bool) -> Result<(), RocketMQError> {
        self.ensure_open()?;
        let result = self
            .db
            .flush_wal(sync)
            .map_err(|error| self.map_native_error(error, RocksDbErrorKind::Flush));
        self.record_result(&result, RocksDbMetricsCollector::record_flush);
        result
    }

    pub fn compact_range_cf(&self, cf: &str, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<(), RocketMQError> {
        self.ensure_open()?;
        let result = compact_range_cf_on_db(&self.db, cf, start, end);
        self.record_result(&result, RocksDbMetricsCollector::record_manual_compaction);
        result
    }

    pub async fn compact_range_cf_blocking(
        &self,
        cf: String,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
    ) -> Result<(), RocketMQError> {
        self.ensure_open()?;
        let db = Arc::clone(&self.db);
        let metrics = Arc::clone(&self.metrics);
        let result = tokio::task::spawn_blocking(move || {
            compact_range_cf_on_db(&db, &cf, start.as_deref(), end.as_deref())?;
            metrics.record_manual_compaction();
            Ok(())
        })
        .await
        .map_err(|error| RocketMQError::storage_write_failed("rocksdb", format!("Compaction: {error}")))?;
        if result.is_err() {
            self.metrics.record_error();
        }
        result
    }

    pub fn compact_range_cf_background(
        &self,
        cf: String,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
    ) -> Result<(), RocketMQError> {
        self.ensure_open()?;
        let db = Arc::clone(&self.db);
        let metrics = Arc::clone(&self.metrics);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn_blocking(move || {
                if let Err(error) = compact_range_cf_on_db(&db, &cf, start.as_deref(), end.as_deref()) {
                    warn!(error = %error, "failed to run RocksDB background compaction");
                    metrics.record_error();
                    return;
                }
                metrics.record_manual_compaction();
            });
        } else {
            std::thread::Builder::new()
                .name("rocksdb-manual-compaction".to_string())
                .spawn(move || {
                    if let Err(error) = compact_range_cf_on_db(&db, &cf, start.as_deref(), end.as_deref()) {
                        warn!(error = %error, "failed to run RocksDB background compaction");
                        metrics.record_error();
                        return;
                    }
                    metrics.record_manual_compaction();
                })
                .map_err(|error| {
                    RocketMQError::storage_write_failed("rocksdb", format!("Compaction: spawn failed: {error}"))
                })?;
        }
        Ok(())
    }

    pub fn manual_compaction_count(&self) -> u64 {
        self.metrics().manual_compaction_count
    }

    pub fn metrics(&self) -> RocksDbMetrics {
        self.metrics.snapshot()
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

    pub fn property_value(&self, property: &str) -> Result<Option<String>, RocketMQError> {
        self.ensure_open()?;
        let result = self
            .db
            .property_value(property)
            .map_err(|error| self.map_native_error(error, RocksDbErrorKind::Property));
        self.record_result(&result, RocksDbMetricsCollector::record_property_query);
        result
    }

    pub fn property_value_cf(&self, cf: &str, property: &str) -> Result<Option<String>, RocketMQError> {
        self.ensure_open()?;
        let handle = self.cf_handle(cf)?;
        let result = self
            .db
            .property_value_cf(&handle, property)
            .map_err(|error| self.map_native_error(error, RocksDbErrorKind::Property));
        self.record_result(&result, RocksDbMetricsCollector::record_property_query);
        result
    }

    pub fn property_int_value(&self, property: &str) -> Result<Option<u64>, RocketMQError> {
        self.ensure_open()?;
        let result = self
            .db
            .property_int_value(property)
            .map_err(|error| self.map_native_error(error, RocksDbErrorKind::Property));
        self.record_result(&result, RocksDbMetricsCollector::record_property_query);
        result
    }

    pub fn property_int_value_cf(&self, cf: &str, property: &str) -> Result<Option<u64>, RocketMQError> {
        self.ensure_open()?;
        let handle = self.cf_handle(cf)?;
        let result = self
            .db
            .property_int_value_cf(&handle, property)
            .map_err(|error| self.map_native_error(error, RocksDbErrorKind::Property));
        self.record_result(&result, RocksDbMetricsCollector::record_property_query);
        result
    }

    pub async fn create_checkpoint(&self, target_dir: PathBuf) -> Result<(), RocketMQError> {
        self.ensure_open()?;
        let db = Arc::clone(&self.db);
        let result = tokio::task::spawn_blocking(move || {
            let checkpoint = ::rocksdb::checkpoint::Checkpoint::new(&db).map_rocksdb(RocksDbErrorKind::Checkpoint)?;
            checkpoint
                .create_checkpoint(target_dir)
                .map_rocksdb(RocksDbErrorKind::Checkpoint)
        })
        .await
        .map_err(|error| RocketMQError::storage_write_failed("rocksdb", format!("Checkpoint: {error}")))?;
        self.record_result(&result, RocksDbMetricsCollector::record_checkpoint);
        result
    }

    pub async fn create_backup(&self, backup_dir: PathBuf) -> Result<(), RocketMQError> {
        self.ensure_open()?;
        let db = Arc::clone(&self.db);
        let result = tokio::task::spawn_blocking(move || {
            create_dir_all_for_rocksdb_operation(&backup_dir, RocksDbErrorKind::Backup)?;
            let env = ::rocksdb::Env::new().map_rocksdb(RocksDbErrorKind::Backup)?;
            let backup_options =
                ::rocksdb::backup::BackupEngineOptions::new(&backup_dir).map_rocksdb(RocksDbErrorKind::Backup)?;
            let mut backup_engine =
                ::rocksdb::backup::BackupEngine::open(&backup_options, &env).map_rocksdb(RocksDbErrorKind::Backup)?;
            backup_engine
                .create_new_backup_flush(db.as_ref(), true)
                .map_rocksdb(RocksDbErrorKind::Backup)
        })
        .await
        .map_err(|error| RocketMQError::storage_write_failed("rocksdb", format!("Backup: {error}")))?;
        self.record_result(&result, RocksDbMetricsCollector::record_backup);
        result
    }

    pub async fn restore_latest_backup(
        backup_dir: PathBuf,
        db_dir: PathBuf,
        wal_dir: Option<PathBuf>,
    ) -> Result<(), RocketMQError> {
        tokio::task::spawn_blocking(move || {
            create_dir_all_for_rocksdb_operation(&backup_dir, RocksDbErrorKind::Restore)?;
            create_dir_all_for_rocksdb_operation(&db_dir, RocksDbErrorKind::Restore)?;
            let wal_dir = wal_dir.unwrap_or_else(|| db_dir.clone());
            create_dir_all_for_rocksdb_operation(&wal_dir, RocksDbErrorKind::Restore)?;

            let env = ::rocksdb::Env::new().map_rocksdb(RocksDbErrorKind::Restore)?;
            let backup_options =
                ::rocksdb::backup::BackupEngineOptions::new(&backup_dir).map_rocksdb(RocksDbErrorKind::Restore)?;
            let mut backup_engine =
                ::rocksdb::backup::BackupEngine::open(&backup_options, &env).map_rocksdb(RocksDbErrorKind::Restore)?;
            let mut restore_options = ::rocksdb::backup::RestoreOptions::default();
            restore_options.set_keep_log_files(false);
            backup_engine
                .restore_from_latest_backup(&db_dir, &wal_dir, &restore_options)
                .map_rocksdb(RocksDbErrorKind::Restore)
        })
        .await
        .map_err(|error| RocketMQError::storage_write_failed("rocksdb", format!("Restore: {error}")))?
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

    pub fn mark_recovered(&self) -> Result<(), RocketMQError> {
        match self.state() {
            RocksDbStoreState::Open => Ok(()),
            RocksDbStoreState::Closed => Err(closed_error()),
            RocksDbStoreState::Reloading => match self.state.compare_exchange(
                RocksDbStoreState::Reloading.as_u8(),
                RocksDbStoreState::Open.as_u8(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => Ok(()),
                Err(state) => match RocksDbStoreState::from_u8(state) {
                    RocksDbStoreState::Open => Ok(()),
                    RocksDbStoreState::Reloading => Err(reloading_error()),
                    RocksDbStoreState::Closed => Err(closed_error()),
                },
            },
        }
    }

    fn cf_handle(&self, cf: &str) -> Result<Arc<::rocksdb::BoundColumnFamily<'_>>, RocketMQError> {
        let result = self.db.cf_handle(cf).ok_or_else(|| column_family_missing_error(cf));
        if result.is_err() {
            self.metrics.record_error();
        }
        result
    }

    fn ensure_open(&self) -> Result<(), RocketMQError> {
        match self.state() {
            RocksDbStoreState::Open => Ok(()),
            RocksDbStoreState::Reloading => Err(reloading_error()),
            RocksDbStoreState::Closed => Err(closed_error()),
        }
    }

    fn record_result<T>(&self, result: &Result<T, RocketMQError>, success_recorder: fn(&RocksDbMetricsCollector)) {
        match result {
            Ok(_) => success_recorder(&self.metrics),
            Err(_) => self.metrics.record_error(),
        }
    }

    fn map_native_error(&self, error: ::rocksdb::Error, kind: RocksDbErrorKind) -> RocketMQError {
        if matches!(
            error.kind(),
            ::rocksdb::ErrorKind::Aborted | ::rocksdb::ErrorKind::Corruption | ::rocksdb::ErrorKind::Unknown
        ) {
            self.mark_reloading();
        }
        Err::<(), _>(error).map_rocksdb(kind).unwrap_err()
    }
}

fn create_dir_all_for_rocksdb_operation(path: &Path, kind: RocksDbErrorKind) -> Result<(), RocketMQError> {
    std::fs::create_dir_all(path)
        .map_err(|error| RocketMQError::storage_write_failed("rocksdb", format!("{kind}: create directory: {error}")))
}

fn merge_existing_column_families(
    config: &RocksDbConfig,
    db_options: &::rocksdb::Options,
) -> Result<Vec<RocksDbColumnFamilyConfig>, RocketMQError> {
    let mut column_families = if config.column_families.is_empty() {
        vec![RocksDbColumnFamilyConfig::consume_queue_default()]
    } else {
        config.column_families.clone()
    };

    if !config.path.join("CURRENT").exists() {
        return Ok(column_families);
    }

    let existing_column_families =
        ::rocksdb::DB::list_cf(db_options, &config.path).map_rocksdb(RocksDbErrorKind::Open)?;
    for existing_name in existing_column_families {
        if column_families
            .iter()
            .any(|column_family| column_family.name == existing_name)
        {
            continue;
        }
        let mut column_family = column_families
            .first()
            .cloned()
            .unwrap_or_else(RocksDbColumnFamilyConfig::consume_queue_default);
        column_family.name = existing_name;
        column_families.push(column_family);
    }
    Ok(column_families)
}

fn compact_range_cf_on_db(
    db: &::rocksdb::DB,
    cf: &str,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
) -> Result<(), RocketMQError> {
    let handle = db.cf_handle(cf).ok_or_else(|| column_family_missing_error(cf))?;
    db.compact_range_cf(&handle, start, end);
    Ok(())
}

fn prefix_scan_on_db(db: &::rocksdb::DB, options: &RocksDbScanOptions) -> Result<Vec<RocksDbScanItem>, RocketMQError> {
    let handle = db
        .cf_handle(&options.cf)
        .ok_or_else(|| column_family_missing_error(&options.cf))?;
    let iter = db.iterator_cf(
        &handle,
        ::rocksdb::IteratorMode::From(&options.prefix, ::rocksdb::Direction::Forward),
    );
    let mut items = Vec::new();
    for item in iter {
        let (key, value) = item.map_err(|error| map_rocksdb_error(error, RocksDbErrorKind::Iterator))?;
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
    Ok(items)
}

fn range_scan_on_db(
    db: &::rocksdb::DB,
    options: &RocksDbRangeScanOptions,
) -> Result<Vec<RocksDbScanItem>, RocketMQError> {
    let handle = db
        .cf_handle(&options.cf)
        .ok_or_else(|| column_family_missing_error(&options.cf))?;
    let iter = db.iterator_cf(
        &handle,
        ::rocksdb::IteratorMode::From(&options.start, ::rocksdb::Direction::Forward),
    );
    let mut items = Vec::new();
    for item in iter {
        let (key, value) = item.map_err(|error| map_rocksdb_error(error, RocksDbErrorKind::Iterator))?;
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
    Ok(items)
}

fn map_rocksdb_error(error: ::rocksdb::Error, kind: RocksDbErrorKind) -> RocketMQError {
    Err::<(), _>(error).map_rocksdb(kind).unwrap_err()
}

impl KeyValueStore for RocksDbStore {
    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), RocketMQError> {
        self.ensure_open()?;
        let handle = self.cf_handle(cf)?;
        let result = self
            .db
            .put_cf_opt(&handle, key, value, &self.write_options)
            .map_err(|error| self.map_native_error(error, RocksDbErrorKind::Write));
        self.record_result(&result, RocksDbMetricsCollector::record_write);
        result
    }

    fn get_cf(&self, cf: &str, key: &[u8]) -> Result<Option<Bytes>, RocketMQError> {
        self.ensure_open()?;
        let handle = self.cf_handle(cf)?;
        let result = self
            .db
            .get_cf(&handle, key)
            .map(|value| value.map(Bytes::from))
            .map_err(|error| self.map_native_error(error, RocksDbErrorKind::Read));
        self.record_result(&result, RocksDbMetricsCollector::record_read);
        result
    }

    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<(), RocketMQError> {
        self.ensure_open()?;
        let handle = self.cf_handle(cf)?;
        let result = self
            .db
            .delete_cf_opt(&handle, key, &self.write_options)
            .map_err(|error| self.map_native_error(error, RocksDbErrorKind::Write));
        self.record_result(&result, RocksDbMetricsCollector::record_write);
        result
    }
}
