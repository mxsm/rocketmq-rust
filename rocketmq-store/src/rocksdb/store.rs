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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use rocketmq_error::RocketMQError;

use crate::rocksdb::batch::RocksDbBatchOperation;
use crate::rocksdb::batch::RocksDbWriteBatch;
use crate::rocksdb::config::RocksDbConfig;
use crate::rocksdb::error::closed_error;
use crate::rocksdb::error::column_family_missing_error;
use crate::rocksdb::error::RocksDbErrorKind;
use crate::rocksdb::error::RocksDbResultExt;
use crate::rocksdb::iterator::RocksDbRangeScanOptions;
use crate::rocksdb::iterator::RocksDbScanItem;
use crate::rocksdb::iterator::RocksDbScanOptions;
use crate::rocksdb::options::RocksDbOptionsFactory;
use crate::rocksdb::snapshot::RocksDbSnapshot;

pub trait KeyValueStore {
    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), RocketMQError>;

    fn get_cf(&self, cf: &str, key: &[u8]) -> Result<Option<Bytes>, RocketMQError>;

    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<(), RocketMQError>;
}

pub struct RocksDbStore {
    db: Arc<::rocksdb::DB>,
    closed: AtomicBool,
    write_options: ::rocksdb::WriteOptions,
}

impl RocksDbStore {
    pub fn open(config: RocksDbConfig) -> Result<Self, RocketMQError> {
        let db_options = RocksDbOptionsFactory::db_options(&config)?;
        let descriptors = config
            .column_families
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
            closed: AtomicBool::new(false),
            write_options: RocksDbOptionsFactory::write_options(config.wal_enabled, config.sync_write),
        })
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
            }
        }

        self.db
            .write_opt(rocksdb_batch, &self.write_options)
            .map_rocksdb(RocksDbErrorKind::Batch)
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
            let (key, value) = item.map_rocksdb(RocksDbErrorKind::Iterator)?;
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

    pub fn range_scan(&self, options: &RocksDbRangeScanOptions) -> Result<Vec<RocksDbScanItem>, RocketMQError> {
        self.ensure_open()?;
        let handle = self.cf_handle(&options.cf)?;
        let iter = self.db.iterator_cf(
            &handle,
            ::rocksdb::IteratorMode::From(&options.start, ::rocksdb::Direction::Forward),
        );
        let mut items = Vec::new();
        for item in iter {
            let (key, value) = item.map_rocksdb(RocksDbErrorKind::Iterator)?;
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

    pub fn snapshot(&self) -> Result<RocksDbSnapshot<'_>, RocketMQError> {
        self.ensure_open()?;
        Ok(RocksDbSnapshot::new(self.db.as_ref()))
    }

    pub fn flush(&self) -> Result<(), RocketMQError> {
        self.ensure_open()?;
        self.db.flush().map_rocksdb(RocksDbErrorKind::Flush)
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    pub(crate) fn db(&self) -> Arc<::rocksdb::DB> {
        Arc::clone(&self.db)
    }

    fn cf_handle(&self, cf: &str) -> Result<Arc<::rocksdb::BoundColumnFamily<'_>>, RocketMQError> {
        self.db.cf_handle(cf).ok_or_else(|| column_family_missing_error(cf))
    }

    fn ensure_open(&self) -> Result<(), RocketMQError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(closed_error());
        }
        Ok(())
    }
}

impl KeyValueStore for RocksDbStore {
    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), RocketMQError> {
        self.ensure_open()?;
        let handle = self.cf_handle(cf)?;
        self.db
            .put_cf_opt(&handle, key, value, &self.write_options)
            .map_rocksdb(RocksDbErrorKind::Write)
    }

    fn get_cf(&self, cf: &str, key: &[u8]) -> Result<Option<Bytes>, RocketMQError> {
        self.ensure_open()?;
        let handle = self.cf_handle(cf)?;
        self.db
            .get_cf(&handle, key)
            .map(|value| value.map(Bytes::from))
            .map_rocksdb(RocksDbErrorKind::Read)
    }

    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<(), RocketMQError> {
        self.ensure_open()?;
        let handle = self.cf_handle(cf)?;
        self.db
            .delete_cf_opt(&handle, key, &self.write_options)
            .map_rocksdb(RocksDbErrorKind::Write)
    }
}
