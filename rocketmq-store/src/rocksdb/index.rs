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

use std::sync::Arc;
use std::sync::Weak;

use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use tracing::warn;

use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::config::message_store_config::MessageStoreConfig;

pub use rocketmq_store_rocksdb::index::RocksDbIndexBuildConfig;
pub use rocketmq_store_rocksdb::index::RocksDbIndexBuildService;
pub use rocketmq_store_rocksdb::index::RocksDbIndexDispatch;

impl RocksDbIndexDispatch for DispatchRequest {
    fn topic(&self) -> &str {
        self.topic.as_str()
    }

    fn commit_log_offset(&self) -> i64 {
        self.commit_log_offset
    }

    fn message_size(&self) -> i32 {
        self.msg_size
    }

    fn store_timestamp(&self) -> i64 {
        self.store_timestamp
    }

    fn is_transaction_rollback(&self) -> bool {
        MessageSysFlag::get_transaction_value(self.sys_flag) == MessageSysFlag::TRANSACTION_ROLLBACK_TYPE
    }

    fn keys(&self) -> &str {
        self.keys.as_str()
    }

    fn uniq_key(&self) -> Option<&str> {
        self.uniq_key.as_ref().map(|key| key.as_str())
    }

    fn tags(&self) -> Option<&str> {
        self.properties_map
            .as_ref()
            .and_then(|properties| properties.get(MessageConst::PROPERTY_TAGS))
            .map(|tag| tag.as_str())
    }
}

pub struct CommitLogDispatcherBuildRocksDbIndex {
    index_service: Weak<RocksDbIndexBuildService>,
    message_store_config: Arc<MessageStoreConfig>,
}

impl CommitLogDispatcherBuildRocksDbIndex {
    pub fn new(index_service: Arc<RocksDbIndexBuildService>, message_store_config: Arc<MessageStoreConfig>) -> Self {
        Self {
            index_service: Arc::downgrade(&index_service),
            message_store_config,
        }
    }

    pub fn index_service(&self) -> Option<Arc<RocksDbIndexBuildService>> {
        self.index_service.upgrade()
    }
}

impl CommitLogDispatcher for CommitLogDispatcherBuildRocksDbIndex {
    fn dispatch(&self, dispatch_request: &mut DispatchRequest) {
        if !self.message_store_config.message_index_enable {
            return;
        }
        let Some(index_service) = self.index_service.upgrade() else {
            warn!("skip RocksDB index dispatch because index service has been dropped");
            return;
        };
        if let Err(error) = index_service.build_index(dispatch_request) {
            warn!(error = %error, "failed to enqueue RocksDB index record");
        }
    }

    fn dispatch_batch(&self, dispatch_requests: &mut [DispatchRequest]) {
        for request in dispatch_requests {
            self.dispatch(request);
        }
        let Some(index_service) = self.index_service.upgrade() else {
            warn!("skip RocksDB index batch flush because index service has been dropped");
            return;
        };
        if let Err(error) = index_service.flush_pending() {
            warn!(error = %error, "failed to flush RocksDB index batch");
        }
    }

    fn dispatch_progress_offset(&self, _commit_log_min_offset: i64) -> Option<i64> {
        if !self.message_store_config.message_index_enable {
            return None;
        }
        let Some(index_service) = self.index_service.upgrade() else {
            warn!("skip RocksDB index progress because index service has been dropped");
            return None;
        };
        index_service.get_dispatch_from_phy_offset().unwrap_or_else(|error| {
            warn!(error = %error, "failed to read RocksDB index dispatch progress");
            None
        })
    }
}
