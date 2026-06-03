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

use rocketmq_error::RocketMQError;
use rocketmq_store::rocksdb::batch::RocksDbWriteBatch;
use rocketmq_store::rocksdb::column_family::RocksDbColumnFamily;
use rocketmq_store::rocksdb::config::RocksDbColumnFamilyConfig;
use rocketmq_store::rocksdb::config::RocksDbCompactionStyle;
use rocketmq_store::rocksdb::config::RocksDbCompressionType;
use rocketmq_store::rocksdb::config::RocksDbConfig;
use rocketmq_store::rocksdb::error::codec_error;
use rocketmq_store::rocksdb::iterator::RocksDbRangeScanOptions;
use rocketmq_store::rocksdb::store::RocksDbStore;
use serde::Deserialize;
use serde::Serialize;

const POP_RECORD_KEY_SEPARATOR: u8 = b'@';
pub(crate) const POP_ROCKSDB_DIRECTORY: &str = "kvStore";

pub(crate) fn pop_rocksdb_path(store_path_root_dir: impl AsRef<Path>) -> PathBuf {
    store_path_root_dir.as_ref().join(POP_ROCKSDB_DIRECTORY)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PopConsumerRetryType {
    NormalTopic,
    RetryTopicV1,
    RetryTopicV2,
}

impl PopConsumerRetryType {
    pub(crate) fn code(self) -> i32 {
        match self {
            Self::NormalTopic => 0,
            Self::RetryTopicV1 => 1,
            Self::RetryTopicV2 => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PopConsumerRecord {
    pub(crate) pop_time: i64,
    pub(crate) group_id: String,
    pub(crate) topic_id: String,
    pub(crate) queue_id: i32,
    pub(crate) retry_flag: i32,
    pub(crate) invisible_time: i64,
    pub(crate) offset: i64,
    pub(crate) attempt_times: i32,
    pub(crate) attempt_id: String,
    pub(crate) suspend: bool,
}

impl PopConsumerRecord {
    pub(crate) fn visibility_timeout(&self) -> i64 {
        self.pop_time + self.invisible_time
    }

    pub(crate) fn is_retry(&self) -> bool {
        self.retry_flag != PopConsumerRetryType::NormalTopic.code()
    }

    pub(crate) fn key_bytes(&self) -> Result<Vec<u8>, RocketMQError> {
        validate_key_component("group_id", &self.group_id)?;
        validate_key_component("topic_id", &self.topic_id)?;

        let mut key = Vec::with_capacity(8 + self.group_id.len() + 1 + self.topic_id.len() + 1 + 4 + 1 + 8);
        key.extend_from_slice(&self.visibility_timeout().to_be_bytes());
        key.extend_from_slice(self.group_id.as_bytes());
        key.push(POP_RECORD_KEY_SEPARATOR);
        key.extend_from_slice(self.topic_id.as_bytes());
        key.push(POP_RECORD_KEY_SEPARATOR);
        key.extend_from_slice(&self.queue_id.to_be_bytes());
        key.push(POP_RECORD_KEY_SEPARATOR);
        key.extend_from_slice(&self.offset.to_be_bytes());
        Ok(key)
    }

    pub(crate) fn value_bytes(&self) -> Result<Vec<u8>, RocketMQError> {
        serde_json::to_vec(self).map_err(|error| codec_error(format!("pop consumer record encode failed: {error}")))
    }

    pub(crate) fn decode(body: &[u8]) -> Result<Self, RocketMQError> {
        serde_json::from_slice(body).map_err(|error| codec_error(format!("pop consumer record decode failed: {error}")))
    }
}

pub(crate) struct PopConsumerRocksDbStore {
    config: RocksDbConfig,
    store: RocksDbStore,
}

impl PopConsumerRocksDbStore {
    pub(crate) fn open(
        path: PathBuf,
        block_cache_size: usize,
        write_buffer_size: usize,
    ) -> Result<Self, RocketMQError> {
        let config = pop_rocksdb_config(path, block_cache_size, write_buffer_size);
        let store = RocksDbStore::open(config.clone())?;
        Ok(Self { config, store })
    }

    pub(crate) fn config(&self) -> &RocksDbConfig {
        &self.config
    }

    pub(crate) fn file_path(&self) -> &Path {
        &self.config.path
    }

    pub(crate) fn write_records(&self, records: &[PopConsumerRecord]) -> Result<(), RocketMQError> {
        let pop_state_cf = RocksDbColumnFamily::PopState.name();
        let mut batch = RocksDbWriteBatch::with_capacity(records.len());
        for record in records {
            batch.put_cf(pop_state_cf, record.key_bytes()?, record.value_bytes()?);
        }
        self.store.write_batch(&batch)
    }

    pub(crate) fn delete_records(&self, records: &[PopConsumerRecord]) -> Result<(), RocketMQError> {
        let pop_state_cf = RocksDbColumnFamily::PopState.name();
        let mut batch = RocksDbWriteBatch::with_capacity(records.len());
        for record in records {
            batch.delete_cf(pop_state_cf, record.key_bytes()?);
        }
        self.store.write_batch(&batch)
    }

    pub(crate) fn scan_expired_records(
        &self,
        lower_bound: i64,
        upper_bound: i64,
        max_count: usize,
    ) -> Result<Vec<PopConsumerRecord>, RocketMQError> {
        if max_count == 0 || lower_bound >= upper_bound {
            return Ok(Vec::new());
        }

        let options = RocksDbRangeScanOptions::new(
            RocksDbColumnFamily::PopState.name(),
            lower_bound.to_be_bytes().to_vec(),
            upper_bound.to_be_bytes().to_vec(),
            max_count,
        );
        self.store
            .range_scan(&options)?
            .into_iter()
            .map(|item| PopConsumerRecord::decode(&item.value))
            .collect()
    }

    pub(crate) fn close(&self) {
        self.store.close();
    }
}

fn pop_rocksdb_config(path: PathBuf, block_cache_size: usize, write_buffer_size: usize) -> RocksDbConfig {
    RocksDbConfig {
        enabled: true,
        path,
        wal_enabled: true,
        sync_write: true,
        column_families: vec![
            pop_column_family_config(RocksDbColumnFamily::Default.name(), block_cache_size, write_buffer_size),
            pop_column_family_config(
                RocksDbColumnFamily::PopState.name(),
                block_cache_size,
                write_buffer_size,
            ),
        ],
        ..RocksDbConfig::default()
    }
}

fn pop_column_family_config(
    name: &str,
    block_cache_size: usize,
    write_buffer_size: usize,
) -> RocksDbColumnFamilyConfig {
    RocksDbColumnFamilyConfig {
        name: name.to_string(),
        write_buffer_size,
        max_write_buffer_number: 4,
        block_cache_size,
        block_size: 32 * 1024,
        bloom_filter_bits: 16.0,
        compression_type: RocksDbCompressionType::Lz4,
        bottommost_compression_type: RocksDbCompressionType::Lz4,
        compaction_style: RocksDbCompactionStyle::Universal,
    }
}

fn validate_key_component(field: &'static str, value: &str) -> Result<(), RocketMQError> {
    if value.is_empty() {
        return Err(RocketMQError::ConfigInvalidValue {
            key: field,
            value: value.to_string(),
            reason: "Pop consumer record key component must not be empty".to_string(),
        });
    }
    if value.as_bytes().contains(&POP_RECORD_KEY_SEPARATOR) {
        return Err(RocketMQError::ConfigInvalidValue {
            key: field,
            value: value.to_string(),
            reason: "Pop consumer record key component must not contain '@'".to_string(),
        });
    }
    Ok(())
}
