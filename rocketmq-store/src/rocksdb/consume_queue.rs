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

use bytes::Bytes;
use rocketmq_error::RocketMQError;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Weak;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::error;
use tracing::warn;

use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::rocksdb::batch::RocksDbWriteBatch;
use crate::rocksdb::column_family::RocksDbColumnFamily;
use crate::rocksdb::iterator::RocksDbScanOptions;
use crate::rocksdb::key::ConsumeQueueKey;
use crate::rocksdb::key::ConsumeQueueOffsetBoundary;
use crate::rocksdb::key::ConsumeQueueOffsetKey;
use crate::rocksdb::store::KeyValueStore;
use crate::rocksdb::store::RocksDbStore;
use crate::rocksdb::value::ConsumeQueueOffsetValue;
use crate::rocksdb::value::ConsumeQueueValue;
use crate::rocksdb::value::MaxPhysicalOffsetCheckpointValue;

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
    pub max_physical_offset: Option<i64>,
}

impl ConsumeQueueBatchWriteRequest {
    fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.offset_updates.is_empty() && self.max_physical_offset.is_none()
    }

    fn merge(requests: Vec<Self>) -> Self {
        let mut merged = Self::default();
        for mut request in requests {
            merged.entries.append(&mut request.entries);
            merged.offset_updates.append(&mut request.offset_updates);
            if let Some(max_physical_offset) = request.max_physical_offset {
                merged.max_physical_offset = Some(
                    merged
                        .max_physical_offset
                        .map_or(max_physical_offset, |current| current.max(max_physical_offset)),
                );
            }
        }
        merged
    }
}

pub struct RocksDbConsumeQueueBatchWriter<'a> {
    store: &'a RocksDbStore,
}

impl<'a> RocksDbConsumeQueueBatchWriter<'a> {
    pub fn new(store: &'a RocksDbStore) -> Self {
        Self { store }
    }

    pub fn build_batch(&self, request: &ConsumeQueueBatchWriteRequest) -> Result<RocksDbWriteBatch, RocketMQError> {
        let checkpoint_len = if request.max_physical_offset.is_some() { 1 } else { 0 };
        let mut batch =
            RocksDbWriteBatch::with_capacity(request.entries.len() + request.offset_updates.len() + checkpoint_len);
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

        if let Some(max_physical_offset) = request.max_physical_offset {
            let key = ConsumeQueueOffsetKey::max_physical_offset_checkpoint();
            let mut encoded_key = Vec::with_capacity(key.encoded_len());
            key.encode(&mut encoded_key)?;

            let value = MaxPhysicalOffsetCheckpointValue { max_physical_offset };
            let mut encoded_value = Vec::with_capacity(MaxPhysicalOffsetCheckpointValue::ENCODED_LEN);
            value.encode(&mut encoded_value)?;

            batch.put_cf(
                RocksDbColumnFamily::ConsumeQueueOffset.name(),
                encoded_key,
                encoded_value,
            );
        }
        Ok(batch)
    }

    pub fn build_destroy_topic_queue_batch(
        &self,
        topic: impl AsRef<str>,
        queue_id: i32,
    ) -> Result<RocksDbWriteBatch, RocketMQError> {
        let topic = topic.as_ref();
        let (cq_start_key, cq_end_key) = ConsumeQueueKey::delete_range(topic, queue_id)?;
        let mut batch = RocksDbWriteBatch::with_capacity(3);
        batch.delete_range_cf(RocksDbColumnFamily::Default.name(), cq_start_key, cq_end_key);

        for boundary in [ConsumeQueueOffsetBoundary::Min, ConsumeQueueOffsetBoundary::Max] {
            let key = ConsumeQueueOffsetKey {
                topic: topic.to_string(),
                queue_id,
                boundary,
            };
            let mut encoded_key = Vec::with_capacity(key.encoded_len());
            key.encode(&mut encoded_key)?;
            batch.delete_cf(RocksDbColumnFamily::ConsumeQueueOffset.name(), encoded_key);
        }
        Ok(batch)
    }

    pub fn destroy_topic_queue(&self, topic: impl AsRef<str>, queue_id: i32) -> Result<(), RocketMQError> {
        let batch = self.build_destroy_topic_queue_batch(topic, queue_id)?;
        self.store.write_batch(&batch)
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

    pub fn range_query_cq_values(
        &self,
        topic: impl Into<String>,
        queue_id: i32,
        start_index: i64,
        num: i32,
    ) -> Result<Vec<ConsumeQueueValue>, RocketMQError> {
        if num <= 0 {
            return Ok(Vec::new());
        }

        let topic = topic.into();
        let mut values = Vec::with_capacity(num as usize);
        for offset_delta in 0..num {
            match self.get_cq_value(topic.clone(), queue_id, start_index + i64::from(offset_delta))? {
                Some(value) => values.push(value),
                None => break,
            }
        }
        Ok(values)
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

    pub fn get_max_physical_offset_checkpoint(&self) -> Result<MaxPhysicalOffsetCheckpointValue, RocketMQError> {
        let key = ConsumeQueueOffsetKey::max_physical_offset_checkpoint();
        let mut encoded_key = Vec::with_capacity(key.encoded_len());
        key.encode(&mut encoded_key)?;

        self.store
            .get_cf(RocksDbColumnFamily::ConsumeQueueOffset.name(), &encoded_key)?
            .map(|value| MaxPhysicalOffsetCheckpointValue::decode(value.as_ref()))
            .transpose()
            .map(|value| match value {
                Some(value) => value,
                None => MaxPhysicalOffsetCheckpointValue { max_physical_offset: 0 },
            })
    }
}

#[derive(Clone)]
pub struct RocksDbConsumeQueueStore {
    store: Arc<RocksDbStore>,
}

struct ConsumeQueueOffsetEntry {
    topic: String,
    queue_id: i32,
    boundary: ConsumeQueueOffsetBoundary,
    value: ConsumeQueueOffsetValue,
}

impl RocksDbConsumeQueueStore {
    pub fn new(store: Arc<RocksDbStore>) -> Self {
        Self { store }
    }

    pub fn put_message_position(&self, requests: &[DispatchRequest]) -> Result<(), RocketMQError> {
        if requests.is_empty() {
            return Ok(());
        }

        let mut entries = Vec::with_capacity(requests.len());
        let mut max_offset_updates = HashMap::<(String, i32), ConsumeQueueOffsetUpdate>::with_capacity(requests.len());
        let mut max_physical_offset: Option<i64> = None;

        for request in requests {
            let topic = request.topic.to_string();
            entries.push(ConsumeQueueBatchEntry {
                topic: topic.clone(),
                queue_id: request.queue_id,
                consume_queue_offset: request.consume_queue_offset,
                commit_log_physical_offset: request.commit_log_offset,
                body_size: request.msg_size,
                tag_hash_code: request.tags_code,
                msg_store_time: request.store_timestamp,
            });

            let topic_queue = (topic.clone(), request.queue_id);
            let update = ConsumeQueueOffsetUpdate::max(
                topic,
                request.queue_id,
                request.commit_log_offset,
                request.consume_queue_offset,
            );
            match max_offset_updates.get(&topic_queue) {
                Some(current) if current.value.consume_queue_offset >= request.consume_queue_offset => {}
                _ => {
                    max_offset_updates.insert(topic_queue, update);
                }
            }

            let request_max_physical_offset = request
                .commit_log_offset
                .checked_add(i64::from(request.msg_size))
                .ok_or_else(|| RocketMQError::ConfigInvalidValue {
                    key: "rocksdb.consume_queue.max_physical_offset",
                    value: format!("{}+{}", request.commit_log_offset, request.msg_size),
                    reason: "commit log offset plus message size overflowed i64".to_string(),
                })?;
            max_physical_offset = Some(max_physical_offset.map_or(request_max_physical_offset, |current| {
                current.max(request_max_physical_offset)
            }));
        }

        let request = ConsumeQueueBatchWriteRequest {
            entries,
            offset_updates: max_offset_updates.into_values().collect(),
            max_physical_offset,
        };
        let writer = RocksDbConsumeQueueBatchWriter::new(self.store.as_ref());
        writer.write(&request)
    }

    pub fn get(
        &self,
        topic: impl Into<String>,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> Result<Option<Bytes>, RocketMQError> {
        let writer = RocksDbConsumeQueueBatchWriter::new(self.store.as_ref());
        writer
            .get_cq_value(topic, queue_id, consume_queue_offset)?
            .map(encode_consume_queue_value)
            .transpose()
    }

    pub fn range_query(
        &self,
        topic: impl Into<String>,
        queue_id: i32,
        start_index: i64,
        num: i32,
    ) -> Result<Vec<Bytes>, RocketMQError> {
        let writer = RocksDbConsumeQueueBatchWriter::new(self.store.as_ref());
        writer
            .range_query_cq_values(topic, queue_id, start_index, num)?
            .into_iter()
            .map(encode_consume_queue_value)
            .collect()
    }

    pub fn get_max_offset_in_queue(&self, topic: impl Into<String>, queue_id: i32) -> Result<i64, RocketMQError> {
        let writer = RocksDbConsumeQueueBatchWriter::new(self.store.as_ref());
        let Some(value) = writer.get_offset_value(topic, queue_id, ConsumeQueueOffsetBoundary::Max)? else {
            return Ok(0);
        };
        value
            .consume_queue_offset
            .checked_add(1)
            .ok_or_else(|| RocketMQError::ConfigInvalidValue {
                key: "rocksdb.consume_queue.max_offset",
                value: value.consume_queue_offset.to_string(),
                reason: "max consume queue offset overflowed i64 when converted to next offset".to_string(),
            })
    }

    pub fn get_min_offset_in_queue(&self, topic: impl Into<String>, queue_id: i32) -> Result<i64, RocketMQError> {
        let writer = RocksDbConsumeQueueBatchWriter::new(self.store.as_ref());
        Ok(writer
            .get_offset_value(topic, queue_id, ConsumeQueueOffsetBoundary::Min)?
            .map_or(0, |value| value.consume_queue_offset))
    }

    pub fn get_max_phy_offset_in_consume_queue(
        &self,
        topic: impl Into<String>,
        queue_id: i32,
    ) -> Result<Option<i64>, RocketMQError> {
        let writer = RocksDbConsumeQueueBatchWriter::new(self.store.as_ref());
        Ok(writer
            .get_offset_value(topic, queue_id, ConsumeQueueOffsetBoundary::Max)?
            .map(|value| value.commit_log_offset))
    }

    pub fn get_max_phy_offset_in_consume_queue_global(&self) -> Result<i64, RocketMQError> {
        let writer = RocksDbConsumeQueueBatchWriter::new(self.store.as_ref());
        Ok(writer.get_max_physical_offset_checkpoint()?.max_physical_offset)
    }

    pub fn destroy_topic_queue(&self, topic: impl AsRef<str>, queue_id: i32) -> Result<(), RocketMQError> {
        let writer = RocksDbConsumeQueueBatchWriter::new(self.store.as_ref());
        writer.destroy_topic_queue(topic, queue_id)
    }

    pub fn scan_queue_ids_in_topic(&self, topic: impl AsRef<str>) -> Result<Vec<i32>, RocketMQError> {
        let prefix = ConsumeQueueOffsetKey::topic_boundary_prefix(topic, ConsumeQueueOffsetBoundary::Max)?;
        let prefix_len = prefix.len();
        let items = self.store.prefix_scan(&RocksDbScanOptions {
            cf: RocksDbColumnFamily::ConsumeQueueOffset.name().to_string(),
            prefix,
            limit: 0,
        })?;
        let mut queue_ids = Vec::with_capacity(items.len());
        for item in items {
            let key = item.key.as_ref();
            if key.len() != prefix_len + 4 {
                return Err(RocketMQError::storage_read_failed(
                    "rocksdb.consume_queue.offset",
                    format!("malformed offset key length {}, expected {}", key.len(), prefix_len + 4),
                ));
            }
            let mut queue_id = [0_u8; 4];
            queue_id.copy_from_slice(&key[prefix_len..prefix_len + 4]);
            queue_ids.push(i32::from_be_bytes(queue_id));
        }
        Ok(queue_ids)
    }

    pub fn destroy_topic(&self, topic: impl AsRef<str>) -> Result<Vec<i32>, RocketMQError> {
        let topic = topic.as_ref();
        let queue_ids = self.scan_queue_ids_in_topic(topic)?;
        for queue_id in &queue_ids {
            self.destroy_topic_queue(topic, *queue_id)?;
        }
        Ok(queue_ids)
    }

    pub fn truncate_dirty(&self, offset_to_truncate: i64) -> Result<(), RocketMQError> {
        let current_max_phy_offset = self.get_max_phy_offset_in_consume_queue_global()?;
        if offset_to_truncate >= current_max_phy_offset {
            return Ok(());
        }

        let writer = RocksDbConsumeQueueBatchWriter::new(self.store.as_ref());
        writer.write(&ConsumeQueueBatchWriteRequest {
            entries: Vec::new(),
            offset_updates: Vec::new(),
            max_physical_offset: Some(offset_to_truncate),
        })?;

        for entry in self.scan_offset_entries()? {
            if entry.boundary == ConsumeQueueOffsetBoundary::Max && entry.value.commit_log_offset >= offset_to_truncate
            {
                self.correct_max_offset_for_truncate(
                    &entry.topic,
                    entry.queue_id,
                    entry.value.consume_queue_offset,
                    offset_to_truncate,
                )?;
            }
        }
        Ok(())
    }

    fn scan_offset_entries(&self) -> Result<Vec<ConsumeQueueOffsetEntry>, RocketMQError> {
        let items = self.store.prefix_scan(&RocksDbScanOptions {
            cf: RocksDbColumnFamily::ConsumeQueueOffset.name().to_string(),
            prefix: Vec::new(),
            limit: 0,
        })?;
        let mut entries = Vec::new();
        for item in items {
            if item.value.len() != ConsumeQueueOffsetValue::ENCODED_LEN {
                continue;
            }
            if let Some(entry) = parse_consume_queue_offset_entry(item.key.as_ref(), item.value.as_ref())? {
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    fn correct_max_offset_for_truncate(
        &self,
        topic: &str,
        queue_id: i32,
        max_cq_offset: i64,
        offset_to_truncate: i64,
    ) -> Result<(), RocketMQError> {
        let writer = RocksDbConsumeQueueBatchWriter::new(self.store.as_ref());
        let min_offset = writer
            .get_offset_value(topic.to_string(), queue_id, ConsumeQueueOffsetBoundary::Min)?
            .unwrap_or(ConsumeQueueOffsetValue {
                commit_log_offset: 0,
                consume_queue_offset: 0,
            });
        if min_offset.commit_log_offset > offset_to_truncate {
            return Err(RocketMQError::storage_read_failed(
                "rocksdb.consume_queue.offset",
                format!(
                    "min physical offset {} is greater than truncate offset {} for {}:{}",
                    min_offset.commit_log_offset, offset_to_truncate, topic, queue_id
                ),
            ));
        }

        let corrected = self.binary_search_max_cq_offset_before_phy(
            topic,
            queue_id,
            min_offset.consume_queue_offset,
            max_cq_offset,
            offset_to_truncate,
        )?;
        let target = corrected.unwrap_or(min_offset);
        writer.write(&ConsumeQueueBatchWriteRequest {
            entries: Vec::new(),
            offset_updates: vec![ConsumeQueueOffsetUpdate::max(
                topic.to_string(),
                queue_id,
                target.commit_log_offset,
                target.consume_queue_offset,
            )],
            max_physical_offset: None,
        })
    }

    fn binary_search_max_cq_offset_before_phy(
        &self,
        topic: &str,
        queue_id: i32,
        low: i64,
        high: i64,
        target_phy_offset: i64,
    ) -> Result<Option<ConsumeQueueOffsetValue>, RocketMQError> {
        let writer = RocksDbConsumeQueueBatchWriter::new(self.store.as_ref());
        let mut low = low;
        let mut high = high;
        let mut result = None;
        while high >= low {
            let mid = low + ((high - low) >> 1);
            let Some(value) = writer.get_cq_value(topic.to_string(), queue_id, mid)? else {
                low = mid + 1;
                continue;
            };

            if value.commit_log_physical_offset >= target_phy_offset {
                high = mid - 1;
            } else {
                result = Some(ConsumeQueueOffsetValue {
                    commit_log_offset: value.commit_log_physical_offset,
                    consume_queue_offset: mid,
                });
                low = mid + 1;
            }
        }
        Ok(result)
    }

    pub async fn clean_expired(&self, _min_phy_offset: i64) -> Result<(), RocketMQError> {
        self.store
            .compact_range_cf_blocking(RocksDbColumnFamily::Default.name().to_string(), None, None)
            .await
    }

    pub fn clean_expired_background(&self, _min_phy_offset: i64) -> Result<(), RocketMQError> {
        self.store
            .compact_range_cf_background(RocksDbColumnFamily::Default.name().to_string(), None, None)
    }
}

fn parse_consume_queue_offset_entry(
    key: &[u8],
    value: &[u8],
) -> Result<Option<ConsumeQueueOffsetEntry>, RocketMQError> {
    const MIN_OFFSET_KEY_LEN: usize = 4 + 1 + 1 + 3 + 1 + 4;
    if key.len() <= MIN_OFFSET_KEY_LEN {
        return Ok(None);
    }

    let mut topic_len = [0_u8; 4];
    topic_len.copy_from_slice(&key[0..4]);
    let topic_len = i32::from_be_bytes(topic_len);
    if topic_len < 0 {
        return Ok(None);
    }
    let topic_len = topic_len as usize;
    let expected_len = 4 + 1 + topic_len + 1 + 3 + 1 + 4;
    if key.len() != expected_len {
        return Ok(None);
    }
    if key[4] != 1 || key[5 + topic_len] != 1 || key[5 + topic_len + 1 + 3] != 1 {
        return Ok(None);
    }

    let topic = String::from_utf8_lossy(&key[5..5 + topic_len]).into_owned();
    let marker_start = 5 + topic_len + 1;
    let boundary = match &key[marker_start..marker_start + 3] {
        b"max" => ConsumeQueueOffsetBoundary::Max,
        b"min" => ConsumeQueueOffsetBoundary::Min,
        _ => return Ok(None),
    };
    let queue_id_start = marker_start + 3 + 1;
    let mut queue_id = [0_u8; 4];
    queue_id.copy_from_slice(&key[queue_id_start..queue_id_start + 4]);
    let value = ConsumeQueueOffsetValue::decode(value)?;

    Ok(Some(ConsumeQueueOffsetEntry {
        topic,
        queue_id: i32::from_be_bytes(queue_id),
        boundary,
        value,
    }))
}

pub struct CommitLogDispatcherBuildRocksDbConsumeQueue {
    store: Weak<RocksDbStore>,
}

impl CommitLogDispatcherBuildRocksDbConsumeQueue {
    pub fn new(consume_queue_store: RocksDbConsumeQueueStore) -> Self {
        Self {
            store: Arc::downgrade(&consume_queue_store.store),
        }
    }

    fn consume_queue_store(&self) -> Option<RocksDbConsumeQueueStore> {
        self.store.upgrade().map(RocksDbConsumeQueueStore::new)
    }
}

impl CommitLogDispatcher for CommitLogDispatcherBuildRocksDbConsumeQueue {
    fn dispatch(&self, dispatch_request: &mut DispatchRequest) {
        let Some(consume_queue_store) = self.consume_queue_store() else {
            warn!("RocksDB consume queue dispatcher skipped because store owner was dropped");
            return;
        };
        let request: &DispatchRequest = dispatch_request;
        if let Err(error) = consume_queue_store.put_message_position(std::slice::from_ref(request)) {
            error!(error = %error, "failed to dispatch consume queue entry to RocksDB");
        }
    }

    fn dispatch_batch(&self, dispatch_requests: &mut [DispatchRequest]) {
        let Some(consume_queue_store) = self.consume_queue_store() else {
            warn!("RocksDB consume queue dispatcher skipped batch because store owner was dropped");
            return;
        };
        if let Err(error) = consume_queue_store.put_message_position(dispatch_requests) {
            error!(error = %error, "failed to dispatch consume queue batch to RocksDB");
        }
    }

    fn dispatch_progress_offset(&self, commit_log_min_offset: i64) -> Option<i64> {
        let consume_queue_store = self.consume_queue_store()?;
        match consume_queue_store.get_max_phy_offset_in_consume_queue_global() {
            Ok(offset) if offset > 0 => Some(offset.max(commit_log_min_offset)),
            Ok(_) => None,
            Err(error) => {
                warn!(error = %error, "failed to read RocksDB consume queue dispatch progress");
                None
            }
        }
    }
}

fn encode_consume_queue_value(value: ConsumeQueueValue) -> Result<Bytes, RocketMQError> {
    let mut encoded_value = Vec::with_capacity(ConsumeQueueValue::ENCODED_LEN);
    value.encode(&mut encoded_value)?;
    Ok(Bytes::from(encoded_value))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RocksDbConsumeQueueGroupCommitConfig {
    pub queue_capacity: usize,
    pub batch_size: usize,
}

impl Default for RocksDbConsumeQueueGroupCommitConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 100_000,
            batch_size: 256,
        }
    }
}

impl RocksDbConsumeQueueGroupCommitConfig {
    fn validate(self) -> Result<Self, RocketMQError> {
        if self.queue_capacity == 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.consume_queue.group_commit.queue_capacity",
                value: self.queue_capacity.to_string(),
                reason: "queue capacity must be greater than zero".to_string(),
            });
        }
        if self.batch_size == 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.consume_queue.group_commit.batch_size",
                value: self.batch_size.to_string(),
                reason: "batch size must be greater than zero".to_string(),
            });
        }
        Ok(self)
    }
}

pub struct RocksDbConsumeQueueGroupCommitService {
    sender: mpsc::Sender<ConsumeQueueBatchWriteRequest>,
    handle: JoinHandle<Result<(), RocketMQError>>,
}

impl RocksDbConsumeQueueGroupCommitService {
    pub fn start(
        store: Arc<RocksDbStore>,
        config: RocksDbConsumeQueueGroupCommitConfig,
    ) -> Result<Self, RocketMQError> {
        let config = config.validate()?;
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let handle = tokio::spawn(run_group_commit_loop(store, receiver, config.batch_size));
        Ok(Self { sender, handle })
    }

    pub async fn submit(&self, request: ConsumeQueueBatchWriteRequest) -> Result<(), RocketMQError> {
        self.sender.send(request).await.map_err(|error| {
            RocketMQError::storage_write_failed("rocksdb", format!("group commit queue closed: {error}"))
        })
    }

    pub async fn shutdown(self) -> Result<(), RocketMQError> {
        let Self { sender, handle } = self;
        drop(sender);
        handle.await.map_err(|error| {
            RocketMQError::storage_write_failed("rocksdb", format!("group commit worker failed: {error}"))
        })?
    }
}

async fn run_group_commit_loop(
    store: Arc<RocksDbStore>,
    mut receiver: mpsc::Receiver<ConsumeQueueBatchWriteRequest>,
    batch_size: usize,
) -> Result<(), RocketMQError> {
    while let Some(first_request) = receiver.recv().await {
        let mut requests = Vec::with_capacity(batch_size);
        requests.push(first_request);

        while requests.len() < batch_size {
            match receiver.try_recv() {
                Ok(request) => requests.push(request),
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        let request = ConsumeQueueBatchWriteRequest::merge(requests);
        if request.is_empty() {
            continue;
        }

        let store = Arc::clone(&store);
        tokio::task::spawn_blocking(move || {
            let writer = RocksDbConsumeQueueBatchWriter::new(store.as_ref());
            writer.write(&request)
        })
        .await
        .map_err(|error| {
            RocketMQError::storage_write_failed("rocksdb", format!("group commit write task failed: {error}"))
        })??;
    }
    Ok(())
}
