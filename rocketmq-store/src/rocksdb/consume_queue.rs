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

use std::sync::Weak;

use tracing::error;
use tracing::warn;

use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::queue::local_file_consume_queue_store::ConsumeQueueStore;
use crate::rocksdb::store::RocksDbStore;

pub use rocketmq_store_rocksdb::consume_queue::ConsumeQueueBatchEntry;
pub use rocketmq_store_rocksdb::consume_queue::ConsumeQueueBatchWriteRequest;
pub use rocketmq_store_rocksdb::consume_queue::ConsumeQueueOffsetUpdate;
pub use rocketmq_store_rocksdb::consume_queue::RocksDbConsumeQueueBatchWriter;
pub use rocketmq_store_rocksdb::consume_queue::RocksDbConsumeQueueDispatch;
pub use rocketmq_store_rocksdb::consume_queue::RocksDbConsumeQueueGroupCommitConfig;
pub use rocketmq_store_rocksdb::consume_queue::RocksDbConsumeQueueGroupCommitService;
pub use rocketmq_store_rocksdb::consume_queue::RocksDbConsumeQueueStore;

impl RocksDbConsumeQueueDispatch for DispatchRequest {
    fn topic(&self) -> &str {
        self.topic.as_str()
    }

    fn queue_id(&self) -> i32 {
        self.queue_id
    }

    fn commit_log_offset(&self) -> i64 {
        self.commit_log_offset
    }

    fn message_size(&self) -> i32 {
        self.msg_size
    }

    fn tags_code(&self) -> i64 {
        self.tags_code
    }

    fn store_timestamp(&self) -> i64 {
        self.store_timestamp
    }

    fn consume_queue_offset(&self) -> i64 {
        self.consume_queue_offset
    }
}

pub struct CommitLogDispatcherBuildRocksDbConsumeQueue {
    store: Weak<RocksDbStore>,
    local_queue_offsets: Option<ConsumeQueueStore>,
}

impl CommitLogDispatcherBuildRocksDbConsumeQueue {
    pub fn new(consume_queue_store: RocksDbConsumeQueueStore) -> Self {
        Self {
            store: consume_queue_store.downgrade_store(),
            local_queue_offsets: None,
        }
    }

    pub(crate) fn new_with_local_queue_offsets(
        consume_queue_store: RocksDbConsumeQueueStore,
        local_queue_offsets: ConsumeQueueStore,
    ) -> Self {
        Self {
            store: consume_queue_store.downgrade_store(),
            local_queue_offsets: Some(local_queue_offsets),
        }
    }

    fn consume_queue_store(&self) -> Option<RocksDbConsumeQueueStore> {
        self.store.upgrade().map(RocksDbConsumeQueueStore::new)
    }

    fn advance_local_queue_offsets(&self, dispatch_requests: &[DispatchRequest]) {
        let Some(local_queue_offsets) = self.local_queue_offsets.as_ref() else {
            return;
        };
        for request in dispatch_requests {
            let message_count = i64::from(request.batch_size.max(1));
            local_queue_offsets.advance_topic_queue_offset(
                request.topic.as_str(),
                request.queue_id,
                request.consume_queue_offset.saturating_add(message_count),
            );
        }
    }
}

impl CommitLogDispatcher for CommitLogDispatcherBuildRocksDbConsumeQueue {
    fn dispatch(&self, dispatch_request: &mut DispatchRequest) {
        let Some(consume_queue_store) = self.consume_queue_store() else {
            warn!("RocksDB consume queue dispatcher skipped because store owner was dropped");
            return;
        };
        let request: &DispatchRequest = dispatch_request;
        match consume_queue_store.put_message_position(std::slice::from_ref(request)) {
            Ok(()) => self.advance_local_queue_offsets(std::slice::from_ref(request)),
            Err(error) => error!(error = %error, "failed to dispatch consume queue entry to RocksDB"),
        }
    }

    fn dispatch_batch(&self, dispatch_requests: &mut [DispatchRequest]) {
        let Some(consume_queue_store) = self.consume_queue_store() else {
            warn!("RocksDB consume queue dispatcher skipped batch because store owner was dropped");
            return;
        };
        match consume_queue_store.put_message_position(dispatch_requests) {
            Ok(()) => self.advance_local_queue_offsets(dispatch_requests),
            Err(error) => error!(error = %error, "failed to dispatch consume queue batch to RocksDB"),
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
