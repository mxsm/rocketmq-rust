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
}

impl CommitLogDispatcherBuildRocksDbConsumeQueue {
    pub fn new(consume_queue_store: RocksDbConsumeQueueStore) -> Self {
        Self {
            store: consume_queue_store.downgrade_store(),
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
