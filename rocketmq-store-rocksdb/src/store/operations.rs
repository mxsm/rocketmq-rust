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

use bytes::Bytes;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;

use crate::config::RocksDbColumnFamilyConfig;
use crate::config::RocksDbConfig;
use crate::error::rocksdb_contract_error;
use crate::error::rocksdb_source_error;
use crate::error::RocksDbStoreResultExt;
use crate::iterator::RocksDbRangeScanOptions;
use crate::iterator::RocksDbScanItem;
use crate::iterator::RocksDbScanOptions;

pub(super) fn create_dir_all_for_rocksdb_operation(path: &Path, operation: StoreOperation) -> Result<(), StoreError> {
    std::fs::create_dir_all(path).map_err(|source| {
        StoreError::new(&rocketmq_error::STORAGE_IO_FAILED, operation)
            .in_component(rocketmq_store_api::StoreComponent::RocksDb)
            .with_source(source)
    })
}

pub(super) fn merge_existing_column_families(
    config: &RocksDbConfig,
    db_options: &::rocksdb::Options,
) -> Result<Vec<RocksDbColumnFamilyConfig>, StoreError> {
    let mut column_families = if config.column_families.is_empty() {
        vec![RocksDbColumnFamilyConfig::consume_queue_default()]
    } else {
        config.column_families.clone()
    };

    if !config.path.join("CURRENT").exists() {
        return Ok(column_families);
    }

    let existing_column_families = ::rocksdb::DB::list_cf(db_options, &config.path)
        .map_store(&rocketmq_error::STORAGE_BACKEND_UNAVAILABLE, StoreOperation::Load)?;
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

pub(super) fn compact_range_cf_on_db(
    db: &::rocksdb::DB,
    operation: StoreOperation,
    cf: &str,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
) -> Result<(), StoreError> {
    let handle = db
        .cf_handle(cf)
        .ok_or_else(|| rocksdb_contract_error(&rocketmq_error::STORAGE_STATE_CORRUPTED, operation))?;
    db.compact_range_cf(&handle, start, end);
    Ok(())
}

pub(super) fn prefix_scan_on_db(
    db: &::rocksdb::DB,
    operation: StoreOperation,
    options: &RocksDbScanOptions,
) -> Result<Vec<RocksDbScanItem>, StoreError> {
    let handle = db
        .cf_handle(&options.cf)
        .ok_or_else(|| rocksdb_contract_error(&rocketmq_error::STORAGE_STATE_CORRUPTED, operation))?;
    let iter = db.iterator_cf(
        &handle,
        ::rocksdb::IteratorMode::From(&options.prefix, ::rocksdb::Direction::Forward),
    );
    let mut items = Vec::new();
    for item in iter {
        let (key, value) =
            item.map_err(|source| rocksdb_source_error(&rocketmq_error::STORAGE_READ_FAILED, operation, source))?;
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

pub(super) fn range_scan_on_db(
    db: &::rocksdb::DB,
    operation: StoreOperation,
    options: &RocksDbRangeScanOptions,
) -> Result<Vec<RocksDbScanItem>, StoreError> {
    let handle = db
        .cf_handle(&options.cf)
        .ok_or_else(|| rocksdb_contract_error(&rocketmq_error::STORAGE_STATE_CORRUPTED, operation))?;
    let iter = db.iterator_cf(
        &handle,
        ::rocksdb::IteratorMode::From(&options.start, ::rocksdb::Direction::Forward),
    );
    let mut items = Vec::new();
    for item in iter {
        let (key, value) =
            item.map_err(|source| rocksdb_source_error(&rocketmq_error::STORAGE_READ_FAILED, operation, source))?;
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

pub(super) fn unavailable_state(operation: StoreOperation) -> StoreError {
    rocksdb_contract_error(&rocketmq_error::STORAGE_BACKEND_UNAVAILABLE, operation)
}
