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
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_rocksdb::batch::RocksDbWriteBatch;
use rocketmq_store_rocksdb::iterator::RocksDbRangeScanOptions;
use rocketmq_store_rocksdb::timer::codec::decode_ready_key;
use rocketmq_store_rocksdb::timer::codec::encode_ready_key;
use rocketmq_store_rocksdb::timer::codec::TimelineKeyV1;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use rocketmq_store_rocksdb::timer::LATE_READY_CF;
use rocketmq_store_rocksdb::timer::READY_CF;

/// Durable handoff queues between Timeline scanning and the delivery pipeline.
pub(crate) struct TimelineReadyOutbox {
    timeline: Arc<RocksDbTimelineIndex>,
}

impl TimelineReadyOutbox {
    pub(crate) fn new(timeline: Arc<RocksDbTimelineIndex>) -> Self {
        Self { timeline }
    }

    pub(crate) fn append_ready(batch: &mut RocksDbWriteBatch, key: TimelineKeyV1, state_version: u64) {
        batch.put_cf(READY_CF, encode_ready_key(key), state_version.to_be_bytes());
    }

    pub(crate) fn delete_ready(batch: &mut RocksDbWriteBatch, key: TimelineKeyV1) {
        batch.delete_cf(READY_CF, encode_ready_key(key));
    }

    pub(crate) fn append_late_ready(batch: &mut RocksDbWriteBatch, key: TimelineKeyV1, state_version: u64) {
        batch.put_cf(LATE_READY_CF, encode_ready_key(key), state_version.to_be_bytes());
    }

    pub(crate) fn delete_late_ready(batch: &mut RocksDbWriteBatch, key: TimelineKeyV1) {
        batch.delete_cf(LATE_READY_CF, encode_ready_key(key));
    }

    /// Returns one bounded, ordered ready page for a lane.
    pub(crate) fn scan_ready(&self, lane: u16, max_messages: usize) -> Result<Vec<TimelineKeyV1>, RocketMQError> {
        self.scan_cf(READY_CF, lane, max_messages)
    }

    /// Returns one bounded late-ready page for a lane.
    pub(crate) fn scan_late_ready(&self, lane: u16, max_messages: usize) -> Result<Vec<TimelineKeyV1>, RocketMQError> {
        self.scan_cf(LATE_READY_CF, lane, max_messages)
    }

    fn scan_cf(&self, cf: &'static str, lane: u16, max_messages: usize) -> Result<Vec<TimelineKeyV1>, RocketMQError> {
        if max_messages == 0 {
            return Ok(Vec::new());
        }
        let first = encode_ready_key(TimelineKeyV1 {
            due_time_ms: i64::MIN,
            lane,
            timer_id: TimerId::new(0),
            generation: TimerGeneration::new(0),
        });
        let end = if lane == u16::MAX {
            vec![2]
        } else {
            encode_ready_key(TimelineKeyV1 {
                due_time_ms: i64::MIN,
                lane: lane + 1,
                timer_id: TimerId::new(0),
                generation: TimerGeneration::new(0),
            })
            .to_vec()
        };
        self.timeline
            .store()
            .range_scan(&RocksDbRangeScanOptions::new(cf, first, end, max_messages))?
            .into_iter()
            .map(|item| decode_ready_key(&item.key))
            .collect()
    }
}
