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
use crate::timer::timer_message_store::TIMER_TOPIC;

pub use rocketmq_store_rocksdb::timer::RocksDbTimerBuildConfig;
pub use rocketmq_store_rocksdb::timer::RocksDbTimerBuildService;
pub use rocketmq_store_rocksdb::timer::RocksDbTimerDispatch;
pub use rocketmq_store_rocksdb::timer::TimerRocksDbBuildEntry;

pub const PROPERTY_TIMER_ROLL_LABEL: &str = "TIMER_ROLL_LABEL";

impl RocksDbTimerDispatch for DispatchRequest {
    fn is_timer_topic(&self) -> bool {
        self.topic.as_str() == TIMER_TOPIC
    }

    fn commit_log_offset(&self) -> i64 {
        self.commit_log_offset
    }

    fn message_size(&self) -> i32 {
        self.msg_size
    }

    fn consume_queue_offset(&self) -> i64 {
        self.consume_queue_offset
    }

    fn delay_time_ms(&self) -> Option<i64> {
        self.properties_map
            .as_ref()?
            .get(MessageConst::PROPERTY_TIMER_OUT_MS)?
            .as_str()
            .parse::<i64>()
            .ok()
    }

    fn uniq_key(&self) -> Option<&str> {
        self.uniq_key
            .as_ref()
            .filter(|uniq_key| !uniq_key.is_empty())
            .map(|uniq_key| uniq_key.as_str())
            .or_else(|| {
                self.properties_map
                    .as_ref()?
                    .get(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                    .filter(|uniq_key| !uniq_key.is_empty())
                    .map(|uniq_key| uniq_key.as_str())
            })
    }

    fn delete_uniq_key(&self) -> Option<&str> {
        self.properties_map
            .as_ref()?
            .get(MessageConst::PROPERTY_TIMER_DEL_UNIQKEY)
            .filter(|key| !key.is_empty())
            .map(|key| key.as_str())
    }

    fn is_roll_update(&self) -> bool {
        self.properties_map
            .as_ref()
            .and_then(|properties| properties.get(PROPERTY_TIMER_ROLL_LABEL))
            .is_some_and(|label| !label.is_empty())
    }
}

pub struct CommitLogDispatcherBuildRocksDbTimer {
    timer_service: Weak<RocksDbTimerBuildService>,
    message_store_config: Arc<MessageStoreConfig>,
}

impl CommitLogDispatcherBuildRocksDbTimer {
    pub fn new(timer_service: Arc<RocksDbTimerBuildService>, message_store_config: Arc<MessageStoreConfig>) -> Self {
        Self {
            timer_service: Arc::downgrade(&timer_service),
            message_store_config,
        }
    }

    pub fn timer_service(&self) -> Option<Arc<RocksDbTimerBuildService>> {
        self.timer_service.upgrade()
    }
}

impl CommitLogDispatcher for CommitLogDispatcherBuildRocksDbTimer {
    fn dispatch(&self, dispatch_request: &mut DispatchRequest) {
        if !self.message_store_config.timer_rocksdb_enable || self.message_store_config.timer_rocksdb_stop_scan {
            return;
        }
        let Some(timer_service) = self.timer_service.upgrade() else {
            warn!("skip RocksDB timer dispatch because timer service has been dropped");
            return;
        };
        if let Err(error) = timer_service.build_timer_index(dispatch_request) {
            warn!(error = %error, "failed to enqueue RocksDB timer record");
        }
    }

    fn dispatch_batch(&self, dispatch_requests: &mut [DispatchRequest]) {
        for request in dispatch_requests {
            self.dispatch(request);
        }
        if !self.message_store_config.timer_rocksdb_enable || self.message_store_config.timer_rocksdb_stop_scan {
            return;
        }
        let Some(timer_service) = self.timer_service.upgrade() else {
            warn!("skip RocksDB timer batch flush because timer service has been dropped");
            return;
        };
        if let Err(error) = timer_service.flush_pending() {
            warn!(error = %error, "failed to flush RocksDB timer batch");
        }
    }

    fn dispatch_progress_offset(&self, commit_log_min_offset: i64) -> Option<i64> {
        if !self.message_store_config.timer_rocksdb_enable {
            return None;
        }
        let Some(timer_service) = self.timer_service.upgrade() else {
            warn!("skip RocksDB timer progress because timer service has been dropped");
            return None;
        };
        match timer_service.get_dispatch_from_queue_offset() {
            Ok(_) => Some(commit_log_min_offset),
            Err(error) => {
                warn!(error = %error, "failed to read RocksDB timer dispatch progress");
                None
            }
        }
    }
}
