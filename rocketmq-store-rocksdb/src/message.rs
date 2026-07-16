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

use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use rocketmq_error::RocketMQError;

use crate::batch::RocksDbWriteBatch;
use crate::column_family::RocksDbColumnFamily;
use crate::config::RocksDbConfig;
use crate::error::codec_error;
use crate::iterator::RocksDbRangeScanOptions;
use crate::iterator::RocksDbScanOptions;
use crate::key::deal_time_to_hour_stamps;
use crate::key::IndexRocksDbKey;
use crate::key::TimerRocksDbKey;
use crate::key::TransRocksDbKey;
use crate::store::KeyValueStore;
use crate::store::RocksDbStore;
use crate::value::IndexRocksDbValue;
use crate::value::TimerRocksDbValue;
use crate::value::TransRocksDbValue;

pub const TIMER_SYS_TOPIC_SCAN_OFFSET_CHECKPOINT: &[u8] = b"sys_topic_scan_offset_checkpoint";
pub const TIMER_TIMELINE_CHECKPOINT: &[u8] = b"timeline_checkpoint";

const LAST_OFFSET_PY: &[u8] = b"lastOffsetPy";
const LAST_STORE_TIMESTAMP: &[u8] = b"lastStoreTimeStamp";
const TIMER_DELETE_CACHE_CAPACITY: usize = 10_000;
const END_SUFFIX_BYTES: [u8; 512] = [0xFF; 512];

pub struct MessageRocksDbStorage {
    store: Arc<RocksDbStore>,
    timer_delete_cache: Mutex<TimerDeleteCache>,
}

impl MessageRocksDbStorage {
    pub fn open(config: RocksDbConfig) -> Result<Self, RocketMQError> {
        Ok(Self {
            store: Arc::new(RocksDbStore::open(config)?),
            timer_delete_cache: Mutex::new(TimerDeleteCache::new(TIMER_DELETE_CACHE_CAPACITY)),
        })
    }

    pub fn store(&self) -> &RocksDbStore {
        self.store.as_ref()
    }

    pub fn store_arc(&self) -> Arc<RocksDbStore> {
        Arc::clone(&self.store)
    }

    pub fn write_records_for_index(&self, records: &[IndexRocksDbRecord]) -> Result<(), RocketMQError> {
        if records.is_empty() {
            return Ok(());
        }

        let cf = RocksDbColumnFamily::Default.name();
        let mut batch = RocksDbWriteBatch::with_capacity(records.len() + 2);
        for record in records {
            let key = record.key()?;
            let mut encoded_key = Vec::with_capacity(key.encoded_len());
            key.encode(&mut encoded_key)?;

            let value = IndexRocksDbValue {
                store_time: record.store_time,
            };
            let mut encoded_value = Vec::with_capacity(IndexRocksDbValue::ENCODED_LEN);
            value.encode(&mut encoded_value)?;

            batch.put_cf(cf, encoded_key, encoded_value);
        }

        if let Some(last_record) = records.last().filter(|record| record.is_unique_progress_record()) {
            let last_offset_py = self.get_last_offset_py(cf)?;
            if last_record.offset_py > last_offset_py {
                batch.put_cf(cf, LAST_OFFSET_PY.to_vec(), encode_i64(last_record.offset_py));
            }

            let last_store_timestamp = self.get_last_store_timestamp_for_index()?;
            if last_record.store_time > last_store_timestamp {
                batch.put_cf(cf, LAST_STORE_TIMESTAMP.to_vec(), encode_i64(last_record.store_time));
            }
        }

        self.store.write_batch(&batch)
    }

    pub fn get_index_store_time(&self, key: &IndexRocksDbKey) -> Result<Option<i64>, RocketMQError> {
        let mut encoded_key = Vec::with_capacity(key.encoded_len());
        key.encode(&mut encoded_key)?;
        self.store
            .get_cf(RocksDbColumnFamily::Default.name(), &encoded_key)?
            .map(|value| IndexRocksDbValue::decode(value.as_ref()).map(|value| value.store_time))
            .transpose()
    }

    pub fn query_offsets_for_index(
        &self,
        topic: &str,
        index_type: &str,
        key: &str,
        begin_time: i64,
        end_time: i64,
        max_num: usize,
    ) -> Result<Vec<i64>, RocketMQError> {
        if max_num == 0 {
            return Ok(Vec::new());
        }
        let hours = index_hours(begin_time, end_time)?;
        let mut offsets = Vec::with_capacity(max_num);
        for hour in hours {
            let prefix = IndexRocksDbKey::query_prefix(topic, index_type, key, hour)?;
            let items = self.store.prefix_scan(&RocksDbScanOptions {
                cf: RocksDbColumnFamily::Default.name().to_string(),
                prefix,
                limit: 0,
            })?;
            for item in items {
                let value = IndexRocksDbValue::decode(item.value.as_ref())?;
                if value.store_time < begin_time || value.store_time > end_time {
                    continue;
                }
                if item.key.len() < 8 {
                    return Err(codec_error("index key is too short to contain physical offset"));
                }
                offsets.push(decode_i64(&item.key[item.key.len() - 8..])?);
                if offsets.len() >= max_num {
                    return Ok(offsets);
                }
            }
        }
        Ok(offsets)
    }

    pub fn get_last_store_timestamp_for_index(&self) -> Result<i64, RocketMQError> {
        self.get_i64_special_key(RocksDbColumnFamily::Default.name(), LAST_STORE_TIMESTAMP)
    }

    pub fn write_records_for_timer(&self, records: &[TimerRocksDbRecord]) -> Result<(), RocketMQError> {
        if records.is_empty() {
            return Ok(());
        }

        let cf = RocksDbColumnFamily::Timer.name();
        let mut batch = RocksDbWriteBatch::with_capacity(records.len());
        let mut cache = self.timer_delete_cache.lock().map_err(|error| {
            RocketMQError::storage_write_failed("rocksdb", format!("timer delete cache lock poisoned: {error}"))
        })?;

        for record in records {
            let key = record.key();
            let mut encoded_key = Vec::with_capacity(key.encoded_len());
            key.encode(&mut encoded_key)?;

            let value = record.value();
            let mut encoded_value = Vec::with_capacity(TimerRocksDbValue::ENCODED_LEN);
            value.encode(&mut encoded_value)?;

            match record.action {
                TimerRocksDbAction::Put => batch.put_cf(cf, encoded_key, encoded_value),
                TimerRocksDbAction::Delete => {
                    batch.delete_cf(cf, encoded_key.clone());
                    cache.insert(encoded_key);
                }
                TimerRocksDbAction::Update => {
                    if !cache.contains(&encoded_key) {
                        batch.put_cf(cf, encoded_key, encoded_value);
                    }
                }
            }
        }
        drop(cache);

        self.store.write_batch(&batch)
    }

    pub fn get_timer_record(&self, key: &TimerRocksDbKey) -> Result<Option<TimerRocksDbRecord>, RocketMQError> {
        let mut encoded_key = Vec::with_capacity(key.encoded_len());
        key.encode(&mut encoded_key)?;
        self.store
            .get_cf(RocksDbColumnFamily::Timer.name(), &encoded_key)?
            .map(|value| TimerRocksDbRecord::decode(&encoded_key, value.as_ref()))
            .transpose()
    }

    pub fn scan_records_for_timer(
        &self,
        lower_time: i64,
        upper_time: i64,
        size: usize,
        start_key: Option<&[u8]>,
    ) -> Result<Vec<TimerRocksDbRecord>, RocketMQError> {
        if size == 0 {
            return Ok(Vec::new());
        }
        validate_timer_range(lower_time, upper_time)?;

        let start = start_key
            .map(<[u8]>::to_vec)
            .unwrap_or_else(|| lower_time.to_be_bytes().to_vec());
        let items = self.store.range_scan(&RocksDbRangeScanOptions {
            cf: RocksDbColumnFamily::Timer.name().to_string(),
            start,
            end: upper_time.to_be_bytes().to_vec(),
            limit: if start_key.is_some() { size + 1 } else { size },
        })?;

        let mut skip_start_key = start_key.is_some();
        let mut records = Vec::with_capacity(size);
        for item in items {
            if skip_start_key {
                skip_start_key = false;
                continue;
            }
            records.push(TimerRocksDbRecord::decode(item.key.as_ref(), item.value.as_ref())?);
            if records.len() >= size {
                break;
            }
        }
        Ok(records)
    }

    pub fn delete_records_for_timer(&self, lower_time: i64, upper_time: i64) -> Result<(), RocketMQError> {
        validate_timer_range(lower_time, upper_time)?;
        let mut end_key = Vec::with_capacity(8 + END_SUFFIX_BYTES.len());
        end_key.extend_from_slice(&upper_time.to_be_bytes());
        end_key.extend_from_slice(&END_SUFFIX_BYTES);

        let mut batch = RocksDbWriteBatch::with_capacity(1);
        batch.delete_range_cf(
            RocksDbColumnFamily::Timer.name(),
            lower_time.to_be_bytes().to_vec(),
            end_key,
        );
        self.store.write_batch(&batch)
    }

    pub fn write_checkpoint_for_timer(&self, key: &[u8], value: i64) -> Result<(), RocketMQError> {
        validate_timer_checkpoint_key(key)?;
        if value < 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.timer.checkpoint",
                value: value.to_string(),
                reason: "timer checkpoint must be non-negative".to_string(),
            });
        }
        self.store
            .put_cf(RocksDbColumnFamily::Timer.name(), key, &encode_i64(value))
    }

    pub fn write_timeline_checkpoint_for_timer(&self, value: i64) -> Result<(), RocketMQError> {
        self.write_checkpoint_for_timer(TIMER_TIMELINE_CHECKPOINT, value)
    }

    pub fn get_checkpoint_for_timer(&self, key: &[u8]) -> Result<i64, RocketMQError> {
        validate_timer_checkpoint_key(key)?;
        self.get_i64_special_key(RocksDbColumnFamily::Timer.name(), key)
    }

    pub fn get_timeline_checkpoint_for_timer(&self) -> Result<i64, RocketMQError> {
        self.get_checkpoint_for_timer(TIMER_TIMELINE_CHECKPOINT)
    }

    pub fn delete_checkpoint_for_timer(&self, key: &[u8]) -> Result<(), RocketMQError> {
        validate_timer_checkpoint_key(key)?;
        self.store.delete_cf(RocksDbColumnFamily::Timer.name(), key)
    }

    pub fn delete_timeline_checkpoint_for_timer(&self) -> Result<(), RocketMQError> {
        self.delete_checkpoint_for_timer(TIMER_TIMELINE_CHECKPOINT)
    }

    pub fn write_records_for_trans(&self, records: &[TransRocksDbRecord]) -> Result<(), RocketMQError> {
        if records.is_empty() {
            return Ok(());
        }

        let cf = RocksDbColumnFamily::Transaction.name();
        let mut batch = RocksDbWriteBatch::with_capacity(records.len() + 1);
        let mut last_offset_py = 0_i64;

        for record in records {
            let key = record.key();
            let mut encoded_key = Vec::with_capacity(key.encoded_len());
            key.encode(&mut encoded_key)?;

            if record.is_op {
                batch.delete_cf(cf, encoded_key);
                continue;
            }

            let value = record.value();
            let mut encoded_value = Vec::with_capacity(TransRocksDbValue::ENCODED_LEN);
            value.encode(&mut encoded_value)?;
            batch.put_cf(cf, encoded_key, encoded_value);
            last_offset_py = last_offset_py.max(record.offset_py);
        }

        if last_offset_py > 0 {
            let stored_last_offset = self.get_last_offset_py(cf)?;
            if last_offset_py > stored_last_offset {
                batch.put_cf(cf, LAST_OFFSET_PY.to_vec(), encode_i64(last_offset_py));
            }
        }

        self.store.write_batch(&batch)
    }

    pub fn update_records_for_trans(&self, records: &[TransRocksDbRecord]) -> Result<(), RocketMQError> {
        if records.is_empty() {
            return Ok(());
        }

        let cf = RocksDbColumnFamily::Transaction.name();
        let mut batch = RocksDbWriteBatch::with_capacity(records.len());
        for record in records {
            let key = record.key();
            let mut encoded_key = Vec::with_capacity(key.encoded_len());
            key.encode(&mut encoded_key)?;
            if record.delete {
                batch.delete_cf(cf, encoded_key);
                continue;
            }

            let value = record.value();
            let mut encoded_value = Vec::with_capacity(TransRocksDbValue::ENCODED_LEN);
            value.encode(&mut encoded_value)?;
            batch.put_cf(cf, encoded_key, encoded_value);
        }

        self.store.write_batch(&batch)
    }

    pub fn get_trans_record(&self, key: &TransRocksDbKey) -> Result<Option<TransRocksDbRecord>, RocketMQError> {
        let mut encoded_key = Vec::with_capacity(key.encoded_len());
        key.encode(&mut encoded_key)?;
        self.store
            .get_cf(RocksDbColumnFamily::Transaction.name(), &encoded_key)?
            .map(|value| TransRocksDbRecord::decode(&encoded_key, value.as_ref()))
            .transpose()
    }

    pub fn scan_records_for_trans(
        &self,
        size: usize,
        start_key: Option<&[u8]>,
    ) -> Result<Vec<TransRocksDbRecord>, RocketMQError> {
        if size == 0 {
            return Ok(Vec::new());
        }

        let items = self.store.range_scan(&RocksDbRangeScanOptions {
            cf: RocksDbColumnFamily::Transaction.name().to_string(),
            start: start_key.map(<[u8]>::to_vec).unwrap_or_default(),
            end: Vec::new(),
            limit: 0,
        })?;

        let mut skip_start_key = start_key.is_some();
        let mut records = Vec::with_capacity(size);
        for item in items {
            if skip_start_key {
                skip_start_key = false;
                continue;
            }
            if item.key.as_ref() == LAST_OFFSET_PY {
                continue;
            }
            records.push(TransRocksDbRecord::decode(item.key.as_ref(), item.value.as_ref())?);
            if records.len() >= size {
                break;
            }
        }
        Ok(records)
    }

    pub fn get_last_offset_py(&self, cf: &str) -> Result<i64, RocketMQError> {
        self.get_i64_special_key(cf, LAST_OFFSET_PY)
    }

    fn get_i64_special_key(&self, cf: &str, key: &[u8]) -> Result<i64, RocketMQError> {
        self.store
            .get_cf(cf, key)?
            .map(|value| decode_i64(value.as_ref()))
            .unwrap_or(Ok(0))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRocksDbRecord {
    pub topic: String,
    pub key: Option<String>,
    pub tag: Option<String>,
    pub store_time: i64,
    pub uniq_key: String,
    pub offset_py: i64,
}

impl IndexRocksDbRecord {
    pub fn normal_key(
        topic: impl Into<String>,
        key: impl Into<String>,
        uniq_key: impl Into<String>,
        store_time: i64,
        offset_py: i64,
    ) -> Self {
        Self {
            topic: topic.into(),
            key: Some(key.into()),
            tag: None,
            store_time,
            uniq_key: uniq_key.into(),
            offset_py,
        }
    }

    pub fn tag_key(
        topic: impl Into<String>,
        tag: impl Into<String>,
        uniq_key: impl Into<String>,
        store_time: i64,
        offset_py: i64,
    ) -> Self {
        Self {
            topic: topic.into(),
            key: None,
            tag: Some(tag.into()),
            store_time,
            uniq_key: uniq_key.into(),
            offset_py,
        }
    }

    pub fn unique_key(topic: impl Into<String>, uniq_key: impl Into<String>, store_time: i64, offset_py: i64) -> Self {
        Self {
            topic: topic.into(),
            key: None,
            tag: None,
            store_time,
            uniq_key: uniq_key.into(),
            offset_py,
        }
    }

    fn key(&self) -> Result<IndexRocksDbKey, RocketMQError> {
        if let Some(key) = &self.key {
            return IndexRocksDbKey::normal_key(
                self.topic.clone(),
                key.clone(),
                self.uniq_key.clone(),
                self.store_time,
                self.offset_py,
            );
        }
        if let Some(tag) = &self.tag {
            return IndexRocksDbKey::tag_key(
                self.topic.clone(),
                tag.clone(),
                self.uniq_key.clone(),
                self.store_time,
                self.offset_py,
            );
        }
        IndexRocksDbKey::unique_key(
            self.topic.clone(),
            self.uniq_key.clone(),
            self.store_time,
            self.offset_py,
        )
    }

    fn is_unique_progress_record(&self) -> bool {
        self.key.as_deref().unwrap_or_default().is_empty() && self.tag.as_deref().unwrap_or_default().is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerRocksDbAction {
    Put,
    Delete,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerRocksDbRecord {
    pub delay_time: i64,
    pub uniq_key: String,
    pub offset_py: i64,
    pub size_py: i32,
    pub action: TimerRocksDbAction,
}

impl TimerRocksDbRecord {
    fn key(&self) -> TimerRocksDbKey {
        TimerRocksDbKey {
            delay_time: self.delay_time,
            uniq_key: self.uniq_key.clone(),
        }
    }

    fn value(&self) -> TimerRocksDbValue {
        TimerRocksDbValue {
            size_py: self.size_py,
            offset_py: self.offset_py,
        }
    }

    pub fn decode(key: &[u8], value: &[u8]) -> Result<Self, RocketMQError> {
        let key = TimerRocksDbKey::decode(key)?;
        let value = TimerRocksDbValue::decode(value)?;
        Ok(Self {
            delay_time: key.delay_time,
            uniq_key: key.uniq_key,
            offset_py: value.offset_py,
            size_py: value.size_py,
            action: TimerRocksDbAction::Put,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransRocksDbRecord {
    pub offset_py: i64,
    pub topic: String,
    pub uniq_key: String,
    pub check_times: i32,
    pub size_py: i32,
    pub is_op: bool,
    pub delete: bool,
}

impl TransRocksDbRecord {
    fn key(&self) -> TransRocksDbKey {
        TransRocksDbKey {
            offset_py: self.offset_py,
            topic: self.topic.clone(),
            uniq_key: self.uniq_key.clone(),
        }
    }

    fn value(&self) -> TransRocksDbValue {
        TransRocksDbValue {
            check_times: self.check_times,
            size_py: self.size_py,
        }
    }

    pub fn decode(key: &[u8], value: &[u8]) -> Result<Self, RocketMQError> {
        let key = TransRocksDbKey::decode(key)?;
        let value = TransRocksDbValue::decode(value)?;
        Ok(Self {
            offset_py: key.offset_py,
            topic: key.topic,
            uniq_key: key.uniq_key,
            check_times: value.check_times,
            size_py: value.size_py,
            is_op: false,
            delete: false,
        })
    }
}

#[derive(Debug)]
struct TimerDeleteCache {
    capacity: usize,
    keys: HashSet<Vec<u8>>,
    order: VecDeque<Vec<u8>>,
}

impl TimerDeleteCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            keys: HashSet::with_capacity(capacity.min(1024)),
            order: VecDeque::with_capacity(capacity.min(1024)),
        }
    }

    fn insert(&mut self, key: Vec<u8>) {
        if self.keys.insert(key.clone()) {
            self.order.push_back(key);
        }
        while self.keys.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.keys.remove(&oldest);
            } else {
                break;
            }
        }
    }

    fn contains(&self, key: &[u8]) -> bool {
        self.keys.contains(key)
    }
}

fn validate_timer_checkpoint_key(key: &[u8]) -> Result<(), RocketMQError> {
    if key == TIMER_SYS_TOPIC_SCAN_OFFSET_CHECKPOINT || key == TIMER_TIMELINE_CHECKPOINT {
        return Ok(());
    }
    Err(RocketMQError::ConfigInvalidValue {
        key: "rocksdb.timer.checkpoint_key",
        value: String::from_utf8_lossy(key).to_string(),
        reason: "timer checkpoint key is not one of the Java-supported keys".to_string(),
    })
}

fn validate_timer_range(lower_time: i64, upper_time: i64) -> Result<(), RocketMQError> {
    if lower_time <= 0 || upper_time <= 0 || lower_time > upper_time {
        return Err(RocketMQError::ConfigInvalidValue {
            key: "rocksdb.timer.range",
            value: format!("{lower_time}..{upper_time}"),
            reason: "lower/upper time must be positive and lower must not exceed upper".to_string(),
        });
    }
    Ok(())
}

fn index_hours(begin_time: i64, end_time: i64) -> Result<Vec<i64>, RocketMQError> {
    if begin_time <= 0 || end_time <= 0 || begin_time > end_time {
        return Err(RocketMQError::ConfigInvalidValue {
            key: "rocksdb.index.query_time_range",
            value: format!("{begin_time}..{end_time}"),
            reason: "begin/end time must be positive and begin must not exceed end".to_string(),
        });
    }

    let mut hours = Vec::new();
    let mut current = deal_time_to_hour_stamps(begin_time);
    let end = deal_time_to_hour_stamps(end_time);
    while current <= end {
        hours.push(current);
        if hours.len() >= 720 {
            break;
        }
        current += 3_600_000;
    }
    Ok(hours)
}

fn encode_i64(value: i64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

fn decode_i64(src: &[u8]) -> Result<i64, RocketMQError> {
    if src.len() != 8 {
        return Err(codec_error(format!("i64 value must be 8 bytes, got {}", src.len())));
    }
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(src);
    Ok(i64::from_be_bytes(bytes))
}
