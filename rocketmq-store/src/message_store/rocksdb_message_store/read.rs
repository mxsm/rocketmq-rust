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

use super::*;

impl BackendReadOps for RocksDBMessageStore {
    fn is_message_in_cold_area(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        queue_offset: i64,
    ) -> bool {
        BackendReadOps::is_message_in_cold_area(self.local_file_store.as_ref(), group, topic, queue_id, queue_offset)
    }

    fn pickup_store_timestamp(&self, offset: i64, size: i32) -> i64 {
        BackendReadOps::pickup_store_timestamp(self.local_file_store.as_ref(), offset, size)
    }

    fn lmq_queue_offset(&self, topic: &CheetahString) -> i64 {
        BackendReadOps::lmq_queue_offset(self.local_file_store.as_ref(), topic)
    }

    fn contains_lmq(&self, topic: &CheetahString) -> bool {
        BackendReadOps::contains_lmq(self.local_file_store.as_ref(), topic)
    }

    fn get_lmq_topic_names(&self) -> Vec<CheetahString> {
        BackendReadOps::get_lmq_topic_names(self.local_file_store.as_ref())
    }

    fn consume_queue_statistics(&self) -> ConsumeQueueStatistics {
        BackendReadOps::consume_queue_statistics(self.local_file_store.as_ref())
    }

    async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> Option<GetMessageResult> {
        RocksDBMessageStore::get_message(self, group, topic, queue_id, offset, max_msg_nums, message_filter).await
    }

    async fn get_message_with_size_limit(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> Option<GetMessageResult> {
        RocksDBMessageStore::get_message_with_size_limit(
            self,
            group,
            topic,
            queue_id,
            offset,
            max_msg_nums,
            max_total_msg_size,
            message_filter,
        )
        .await
    }

    fn get_max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        RocksDBMessageStore::get_max_offset_in_queue(self, topic, queue_id)
    }

    fn get_max_offset_in_queue_committed(&self, topic: &CheetahString, queue_id: i32, _committed: bool) -> i64 {
        RocksDBMessageStore::get_max_offset_in_queue(self, topic, queue_id)
    }

    fn get_min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        RocksDBMessageStore::get_min_offset_in_queue(self, topic, queue_id)
    }

    fn get_commit_log_offset_in_queue(&self, topic: &CheetahString, queue_id: i32, consume_queue_offset: i64) -> i64 {
        RocksDBMessageStore::get_commit_log_offset_in_queue(self, topic, queue_id, consume_queue_offset)
    }

    fn get_offset_in_queue_by_time(&self, topic: &CheetahString, queue_id: i32, timestamp: i64) -> i64 {
        match self
            .root
            .derived()
            .offset_by_time(topic.as_str(), queue_id, timestamp, RocksDbTimeBoundary::Lower)
        {
            Ok(offset) => offset,
            Err(error) => {
                warn!(topic = %topic, queue_id, timestamp, error = %error, "failed to seek RocksDB consume queue by time");
                0
            }
        }
    }

    fn get_offset_in_queue_by_time_with_boundary(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64 {
        let boundary = match boundary_type {
            BoundaryType::Lower => RocksDbTimeBoundary::Lower,
            BoundaryType::Upper => RocksDbTimeBoundary::Upper,
        };
        match self
            .root
            .derived()
            .offset_by_time(topic.as_str(), queue_id, timestamp, boundary)
        {
            Ok(offset) => offset,
            Err(error) => {
                warn!(topic = %topic, queue_id, timestamp, error = %error, "failed to seek RocksDB consume queue by time boundary");
                0
            }
        }
    }

    async fn get_offset_in_queue_by_time_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
    ) -> Result<i64, StoreError> {
        self.root
            .derived()
            .offset_by_time(topic.as_str(), queue_id, timestamp, RocksDbTimeBoundary::Lower)
    }

    async fn get_offset_in_queue_by_time_with_boundary_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> Result<i64, StoreError> {
        let boundary = match boundary_type {
            BoundaryType::Lower => RocksDbTimeBoundary::Lower,
            BoundaryType::Upper => RocksDbTimeBoundary::Upper,
        };
        self.root
            .derived()
            .offset_by_time(topic.as_str(), queue_id, timestamp, boundary)
    }

    fn look_message_by_offset(&self, commit_log_offset: i64) -> Option<MessageExt> {
        self.local_file_store.look_message_by_offset(commit_log_offset)
    }

    fn look_message_by_offset_with_size(&self, commit_log_offset: i64, size: i32) -> Option<MessageExt> {
        self.local_file_store
            .look_message_by_offset_with_size(commit_log_offset, size)
    }

    fn select_one_message_by_offset(&self, commit_log_offset: i64) -> Option<SelectMappedBufferResult> {
        self.local_file_store.select_one_message_by_offset(commit_log_offset)
    }

    fn select_one_message_by_offset_with_size(
        &self,
        commit_log_offset: i64,
        msg_size: i32,
    ) -> Option<SelectMappedBufferResult> {
        self.local_file_store
            .select_one_message_by_offset_with_size(commit_log_offset, msg_size)
    }

    fn get_timing_message_count(&self, topic: &CheetahString) -> i64 {
        self.local_file_store.get_timing_message_count(topic)
    }

    fn get_earliest_message_time(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        let min_offset = RocksDBMessageStore::get_min_offset_in_queue(self, topic, queue_id);
        if RocksDBMessageStore::get_max_offset_in_queue(self, topic, queue_id) > min_offset {
            return RocksDBMessageStore::get_message_store_timestamp(self, topic, queue_id, min_offset);
        }
        -1
    }

    fn get_earliest_message_time_store(&self) -> i64 {
        self.local_file_store.get_earliest_message_time_store()
    }

    fn get_message_store_timestamp(&self, topic: &CheetahString, queue_id: i32, consume_queue_offset: i64) -> i64 {
        RocksDBMessageStore::get_message_store_timestamp(self, topic, queue_id, consume_queue_offset)
    }

    async fn get_message_store_timestamp_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> Result<i64, StoreError> {
        self.try_get_message_store_timestamp(topic, queue_id, consume_queue_offset)
    }

    fn get_message_total_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        let min_offset = RocksDBMessageStore::get_min_offset_in_queue(self, topic, queue_id);
        let max_offset = RocksDBMessageStore::get_max_offset_in_queue(self, topic, queue_id);
        max_offset.saturating_sub(min_offset)
    }

    async fn query_message(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> Option<QueryMessageResult> {
        match self.query_message_by_rocksdb_index(topic, key, None, max_num, begin, end, None) {
            Ok(mut result) if result.buffer_total_size > 0 => {
                self.apply_rocksdb_runtime_index_safety(&mut result);
                Some(result)
            }
            Ok(_) => {
                self.local_file_store
                    .query_message(topic, key, max_num, begin, end)
                    .await
            }
            Err(error) => {
                warn!(topic = %topic, key = %key, error = %error, "failed to query message by RocksDB index");
                self.local_file_store
                    .query_message(topic, key, max_num, begin, end)
                    .await
            }
        }
    }

    async fn query_message_with_options(&self, request: &QueryMessageRequest) -> Option<QueryMessageResult> {
        match self.query_message_by_rocksdb_index(
            &request.topic,
            &request.key,
            request.index_type.as_deref(),
            request.max_num,
            request.begin,
            request.end,
            request.last_key.as_deref(),
        ) {
            Ok(mut result) if result.buffer_total_size > 0 => {
                self.apply_rocksdb_runtime_index_safety(&mut result);
                Some(result)
            }
            Ok(mut result) if request.last_key.is_some() => {
                self.apply_rocksdb_runtime_index_safety(&mut result);
                Some(result)
            }
            Ok(_) => {
                let key = request.legacy_backend_key();
                self.local_file_store
                    .query_message(&request.topic, &key, request.max_num, request.begin, request.end)
                    .await
            }
            Err(error) if request.last_key.is_some() => {
                warn!(topic = %request.topic, key = %request.key, error = %error, "rejected invalid RocksDB index cursor");
                let mut result = QueryMessageResult::default();
                self.apply_rocksdb_runtime_index_safety(&mut result);
                Some(result)
            }
            Err(error) => {
                warn!(topic = %request.topic, key = %request.key, error = %error, "failed to query message by RocksDB index");
                let key = request.legacy_backend_key();
                self.local_file_store
                    .query_message(&request.topic, &key, request.max_num, request.begin, request.end)
                    .await
            }
        }
    }

    fn check_in_mem_by_consume_offset(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_offset: i64,
        batch_size: i32,
    ) -> bool {
        self.local_file_store
            .check_in_mem_by_consume_offset(topic, queue_id, consume_offset, batch_size)
    }

    fn check_in_store_by_consume_offset(&self, topic: &CheetahString, queue_id: i32, consume_offset: i64) -> bool {
        self.rocksdb_cq_value(topic, queue_id, consume_offset)
            .is_ok_and(|value| value.is_some())
    }
}
