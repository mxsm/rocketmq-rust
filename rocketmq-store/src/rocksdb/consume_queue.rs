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

use rocketmq_error::RocketMQError;

use crate::rocksdb::batch::RocksDbWriteBatch;
use crate::rocksdb::column_family::RocksDbColumnFamily;
use crate::rocksdb::key::ConsumeQueueKey;
use crate::rocksdb::key::ConsumeQueueOffsetBoundary;
use crate::rocksdb::key::ConsumeQueueOffsetKey;
use crate::rocksdb::store::KeyValueStore;
use crate::rocksdb::store::RocksDbStore;
use crate::rocksdb::value::ConsumeQueueOffsetValue;
use crate::rocksdb::value::ConsumeQueueValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeQueueBatchEntry {
    pub topic: String,
    pub queue_id: i32,
    pub consume_queue_offset: i64,
    pub commit_log_physical_offset: i64,
    pub body_size: i32,
    pub tag_hash_code: i64,
    pub msg_store_time: i64,
}

impl ConsumeQueueBatchEntry {
    fn key(&self) -> ConsumeQueueKey {
        ConsumeQueueKey {
            topic: self.topic.clone(),
            queue_id: self.queue_id,
            cq_offset: self.consume_queue_offset,
        }
    }

    fn value(&self) -> ConsumeQueueValue {
        ConsumeQueueValue {
            commit_log_physical_offset: self.commit_log_physical_offset,
            body_size: self.body_size,
            tag_hash_code: self.tag_hash_code,
            msg_store_time: self.msg_store_time,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeQueueOffsetUpdate {
    pub topic: String,
    pub queue_id: i32,
    pub boundary: ConsumeQueueOffsetBoundary,
    pub value: ConsumeQueueOffsetValue,
}

impl ConsumeQueueOffsetUpdate {
    pub fn max(topic: impl Into<String>, queue_id: i32, commit_log_offset: i64, consume_queue_offset: i64) -> Self {
        Self::new(
            topic,
            queue_id,
            ConsumeQueueOffsetBoundary::Max,
            commit_log_offset,
            consume_queue_offset,
        )
    }

    pub fn min(topic: impl Into<String>, queue_id: i32, commit_log_offset: i64, consume_queue_offset: i64) -> Self {
        Self::new(
            topic,
            queue_id,
            ConsumeQueueOffsetBoundary::Min,
            commit_log_offset,
            consume_queue_offset,
        )
    }

    pub fn new(
        topic: impl Into<String>,
        queue_id: i32,
        boundary: ConsumeQueueOffsetBoundary,
        commit_log_offset: i64,
        consume_queue_offset: i64,
    ) -> Self {
        Self {
            topic: topic.into(),
            queue_id,
            boundary,
            value: ConsumeQueueOffsetValue {
                commit_log_offset,
                consume_queue_offset,
            },
        }
    }

    fn key(&self) -> ConsumeQueueOffsetKey {
        ConsumeQueueOffsetKey {
            topic: self.topic.clone(),
            queue_id: self.queue_id,
            boundary: self.boundary,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ConsumeQueueBatchWriteRequest {
    pub entries: Vec<ConsumeQueueBatchEntry>,
    pub offset_updates: Vec<ConsumeQueueOffsetUpdate>,
}

pub struct RocksDbConsumeQueueBatchWriter<'a> {
    store: &'a RocksDbStore,
}

impl<'a> RocksDbConsumeQueueBatchWriter<'a> {
    pub fn new(store: &'a RocksDbStore) -> Self {
        Self { store }
    }

    pub fn build_batch(&self, request: &ConsumeQueueBatchWriteRequest) -> Result<RocksDbWriteBatch, RocketMQError> {
        let mut batch = RocksDbWriteBatch::with_capacity(request.entries.len() + request.offset_updates.len());
        for entry in &request.entries {
            let mut key = Vec::with_capacity(entry.key().encoded_len());
            entry.key().encode(&mut key)?;

            let mut value = Vec::with_capacity(ConsumeQueueValue::ENCODED_LEN);
            entry.value().encode(&mut value)?;

            batch.put_cf(RocksDbColumnFamily::Default.name(), key, value);
        }

        for offset_update in &request.offset_updates {
            let mut key = Vec::with_capacity(offset_update.key().encoded_len());
            offset_update.key().encode(&mut key)?;

            let mut value = Vec::with_capacity(ConsumeQueueOffsetValue::ENCODED_LEN);
            offset_update.value.encode(&mut value)?;

            batch.put_cf(RocksDbColumnFamily::ConsumeQueueOffset.name(), key, value);
        }
        Ok(batch)
    }

    pub fn write(&self, request: &ConsumeQueueBatchWriteRequest) -> Result<(), RocketMQError> {
        let batch = self.build_batch(request)?;
        self.store.write_batch(&batch)
    }

    pub fn get_cq_value(
        &self,
        topic: impl Into<String>,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> Result<Option<ConsumeQueueValue>, RocketMQError> {
        let key = ConsumeQueueKey {
            topic: topic.into(),
            queue_id,
            cq_offset: consume_queue_offset,
        };
        let mut encoded_key = Vec::with_capacity(key.encoded_len());
        key.encode(&mut encoded_key)?;
        self.store
            .get_cf(RocksDbColumnFamily::Default.name(), &encoded_key)?
            .map(|value| ConsumeQueueValue::decode(value.as_ref()))
            .transpose()
    }

    pub fn get_offset_value(
        &self,
        topic: impl Into<String>,
        queue_id: i32,
        boundary: ConsumeQueueOffsetBoundary,
    ) -> Result<Option<ConsumeQueueOffsetValue>, RocketMQError> {
        let key = ConsumeQueueOffsetKey {
            topic: topic.into(),
            queue_id,
            boundary,
        };
        let mut encoded_key = Vec::with_capacity(key.encoded_len());
        key.encode(&mut encoded_key)?;
        self.store
            .get_cf(RocksDbColumnFamily::ConsumeQueueOffset.name(), &encoded_key)?
            .map(|value| ConsumeQueueOffsetValue::decode(value.as_ref()))
            .transpose()
    }
}
