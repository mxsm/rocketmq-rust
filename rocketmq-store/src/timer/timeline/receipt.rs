// Copyright 2026 The RocketMQ Rust Authors
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

use std::sync::Arc;

use rocketmq_error::RocketMQError;
use rocketmq_store_api::StoreOperation;
use rocketmq_store_api::TimerEngineEpoch;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::EXTENDED_TIMELINE_FORMAT_VERSION;
use rocketmq_store_local::timer::storage_format::crc32c;
use rocketmq_store_rocksdb::batch::RocksDbWriteBatch;
use rocketmq_store_rocksdb::store::KeyValueStore;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use rocketmq_store_rocksdb::timer::RECEIPT_CF;

use super::TimelineCompletionError;

const RECEIPT_VALUE_SIZE: usize = 60;
const RECEIPT_KEY_VERSION: u8 = 1;
const MAX_TOKEN_BYTES: usize = 4_096;

/// Durable projection of one final real-topic CommitLog fact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TimelineCompletionReceiptV1 {
    pub(crate) timer_id: TimerId,
    pub(crate) generation: TimerGeneration,
    pub(crate) owner_epoch: TimerEngineEpoch,
    pub(crate) due_time_ms: i64,
    pub(crate) lane: u16,
    pub(crate) final_physical_offset: i64,
    pub(crate) final_record_size: u32,
}

impl TimelineCompletionReceiptV1 {
    fn encode(self) -> Result<[u8; RECEIPT_VALUE_SIZE], RocketMQError> {
        if self.due_time_ms < 0 || self.final_physical_offset < 0 || self.final_record_size == 0 {
            return Err(receipt_error("invalid final CommitLog identity"));
        }
        let mut output = [0u8; RECEIPT_VALUE_SIZE];
        output[0..2].copy_from_slice(&EXTENDED_TIMELINE_FORMAT_VERSION.to_be_bytes());
        output[2..18].copy_from_slice(&self.timer_id.get().to_be_bytes());
        output[18..26].copy_from_slice(&self.generation.get().to_be_bytes());
        output[26..34].copy_from_slice(&self.owner_epoch.get().to_be_bytes());
        output[34..42].copy_from_slice(&self.due_time_ms.to_be_bytes());
        output[42..44].copy_from_slice(&self.lane.to_be_bytes());
        output[44..52].copy_from_slice(&self.final_physical_offset.to_be_bytes());
        output[52..56].copy_from_slice(&self.final_record_size.to_be_bytes());
        let checksum = crc32c(&output[..56]);
        output[56..60].copy_from_slice(&checksum.to_be_bytes());
        Ok(output)
    }

    fn decode(bytes: &[u8]) -> Result<Self, RocketMQError> {
        if bytes.len() != RECEIPT_VALUE_SIZE
            || read_u16(bytes, 0)? != EXTENDED_TIMELINE_FORMAT_VERSION
            || read_u32(bytes, 56)? != crc32c(&bytes[..56])
        {
            return Err(receipt_error("invalid completion receipt version, length, or CRC"));
        }
        let due_time_ms = read_i64(bytes, 34)?;
        let final_physical_offset = read_i64(bytes, 44)?;
        let final_record_size = read_u32(bytes, 52)?;
        if due_time_ms < 0 || final_physical_offset < 0 || final_record_size == 0 {
            return Err(receipt_error("invalid completion receipt CommitLog identity"));
        }
        Ok(Self {
            timer_id: TimerId::new(read_u128(bytes, 2)?),
            generation: TimerGeneration::new(read_u64(bytes, 18)?),
            owner_epoch: TimerEngineEpoch::new(read_u64(bytes, 26)?),
            due_time_ms,
            lane: read_u16(bytes, 42)?,
            final_physical_offset,
            final_record_size,
        })
    }
}

/// Receipt access over the isolated Extended Timeline database.
pub(crate) struct TimelineReceiptStore {
    timeline: Arc<RocksDbTimelineIndex>,
}

impl TimelineReceiptStore {
    pub(crate) fn new(timeline: Arc<RocksDbTimelineIndex>) -> Self {
        Self { timeline }
    }

    pub(crate) fn append(
        batch: &mut RocksDbWriteBatch,
        delivery_token: &str,
        receipt: TimelineCompletionReceiptV1,
    ) -> Result<(), RocketMQError> {
        batch.put_cf(RECEIPT_CF, encode_key(delivery_token)?, receipt.encode()?);
        Ok(())
    }

    pub(crate) fn get(
        &self,
        delivery_token: &str,
    ) -> Result<Option<TimelineCompletionReceiptV1>, TimelineCompletionError> {
        self.timeline
            .store()
            .get_cf(StoreOperation::Read, RECEIPT_CF, &encode_key(delivery_token)?)?
            .map(|value| TimelineCompletionReceiptV1::decode(&value))
            .transpose()
            .map_err(Into::into)
    }

    pub(crate) fn delete(batch: &mut RocksDbWriteBatch, delivery_token: &str) -> Result<(), RocketMQError> {
        batch.delete_cf(RECEIPT_CF, encode_key(delivery_token)?);
        Ok(())
    }
}

fn encode_key(delivery_token: &str) -> Result<Vec<u8>, RocketMQError> {
    if delivery_token.is_empty() || delivery_token.len() > MAX_TOKEN_BYTES {
        return Err(receipt_error("completion receipt token length is invalid"));
    }
    let mut key = Vec::with_capacity(delivery_token.len() + 1);
    key.push(RECEIPT_KEY_VERSION);
    key.extend_from_slice(delivery_token.as_bytes());
    Ok(key)
}

fn receipt_error(reason: impl Into<String>) -> RocketMQError {
    RocketMQError::storage_read_failed("timer-timeline-receipt", reason.into())
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N], RocketMQError> {
    bytes
        .get(offset..offset.saturating_add(N))
        .and_then(|value| value.try_into().ok())
        .ok_or_else(|| receipt_error("truncated completion receipt"))
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, RocketMQError> {
    Ok(u16::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, RocketMQError> {
    Ok(u32::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, RocketMQError> {
    Ok(u64::from_be_bytes(read_array(bytes, offset)?))
}

fn read_i64(bytes: &[u8], offset: usize) -> Result<i64, RocketMQError> {
    Ok(i64::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u128(bytes: &[u8], offset: usize) -> Result<u128, RocketMQError> {
    Ok(u128::from_be_bytes(read_array(bytes, offset)?))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn completion_receipt_round_trips_and_rejects_damage() {
        let directory = tempdir().expect("tempdir");
        let timeline = Arc::new(
            RocksDbTimelineIndex::open(directory.path())
                .expect("timeline")
                .expect("valid Timeline configuration"),
        );
        let store = TimelineReceiptStore::new(Arc::clone(&timeline));
        let expected = TimelineCompletionReceiptV1 {
            timer_id: TimerId::new(7),
            generation: TimerGeneration::new(2),
            owner_epoch: TimerEngineEpoch::new(5),
            due_time_ms: 8_000,
            lane: 3,
            final_physical_offset: 1_024,
            final_record_size: 128,
        };
        let mut batch = RocksDbWriteBatch::with_capacity(1);
        TimelineReceiptStore::append(&mut batch, "stable-token", expected).expect("append");
        timeline.write_batch(&batch).expect("write");
        assert_eq!(store.get("stable-token").expect("get"), Some(expected));
    }
}
