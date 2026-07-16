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
use tracing::warn;

use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::config::message_store_config::MessageStoreConfig;

pub use rocketmq_store_rocksdb::transaction::RocksDbTransBuildConfig;
pub use rocketmq_store_rocksdb::transaction::RocksDbTransBuildService;
pub use rocketmq_store_rocksdb::transaction::RocksDbTransactionDispatch;
pub use rocketmq_store_rocksdb::transaction::RocksDbTransactionDispatchKind;

pub const RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC: &str = "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC";
pub const RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC: &str = "RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC";
pub const PROPERTY_TRANS_OFFSET: &str = "TRANS_OFFSET";

impl RocksDbTransactionDispatch for DispatchRequest {
    fn kind(&self) -> RocksDbTransactionDispatchKind {
        match self.topic.as_str() {
            RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC => RocksDbTransactionDispatchKind::Half,
            RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC => RocksDbTransactionDispatchKind::Operation,
            _ => RocksDbTransactionDispatchKind::Other,
        }
    }

    fn commit_log_offset(&self) -> i64 {
        self.commit_log_offset
    }

    fn message_size(&self) -> i32 {
        self.msg_size
    }

    fn real_topic(&self) -> Option<&str> {
        self.properties_map
            .as_ref()?
            .get(MessageConst::PROPERTY_REAL_TOPIC)
            .filter(|topic| !topic.is_empty())
            .map(|topic| topic.as_str())
    }

    fn transaction_id(&self) -> Option<&str> {
        self.properties_map
            .as_ref()?
            .get(MessageConst::PROPERTY_TRANSACTION_ID)
            .filter(|id| !id.is_empty())
            .map(|id| id.as_str())
    }

    fn transaction_offset(&self) -> Option<i64> {
        self.properties_map
            .as_ref()?
            .get(PROPERTY_TRANS_OFFSET)?
            .as_str()
            .parse::<i64>()
            .ok()
    }
}

pub struct CommitLogDispatcherBuildRocksDbTrans {
    trans_service: Weak<RocksDbTransBuildService>,
    message_store_config: Arc<MessageStoreConfig>,
}

impl CommitLogDispatcherBuildRocksDbTrans {
    pub fn new(trans_service: Arc<RocksDbTransBuildService>, message_store_config: Arc<MessageStoreConfig>) -> Self {
        Self {
            trans_service: Arc::downgrade(&trans_service),
            message_store_config,
        }
    }

    pub fn trans_service(&self) -> Option<Arc<RocksDbTransBuildService>> {
        self.trans_service.upgrade()
    }
}

impl CommitLogDispatcher for CommitLogDispatcherBuildRocksDbTrans {
    fn dispatch(&self, dispatch_request: &mut DispatchRequest) {
        if !self.message_store_config.trans_rocksdb_enable {
            return;
        }
        let Some(trans_service) = self.trans_service.upgrade() else {
            warn!("skip RocksDB transaction dispatch because trans service has been dropped");
            return;
        };
        if let Err(error) = trans_service.build_trans_index(dispatch_request) {
            warn!(error = %error, "failed to enqueue RocksDB transaction record");
        }
    }

    fn dispatch_batch(&self, dispatch_requests: &mut [DispatchRequest]) {
        for request in dispatch_requests {
            self.dispatch(request);
        }
        if !self.message_store_config.trans_rocksdb_enable {
            return;
        }
        let Some(trans_service) = self.trans_service.upgrade() else {
            warn!("skip RocksDB transaction batch flush because trans service has been dropped");
            return;
        };
        if let Err(error) = trans_service.flush_pending() {
            warn!(error = %error, "failed to flush RocksDB transaction batch");
        }
    }

    fn dispatch_progress_offset(&self, _commit_log_min_offset: i64) -> Option<i64> {
        if !self.message_store_config.trans_rocksdb_enable {
            return None;
        }
        let Some(trans_service) = self.trans_service.upgrade() else {
            warn!("skip RocksDB transaction progress because trans service has been dropped");
            return None;
        };
        trans_service.get_dispatch_from_phy_offset().unwrap_or_else(|error| {
            warn!(error = %error, "failed to read RocksDB transaction dispatch progress");
            None
        })
    }
}
