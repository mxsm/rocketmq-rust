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

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::attribute::cleanup_policy::CleanupPolicy;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::CleanupPolicyUtils::get_delete_policy_arc_mut;

use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::config::message_store_config::MessageStoreConfig;
use crate::kv::compaction_store::CompactionStore;

pub struct CommitLogDispatcherCompaction {
    compaction_store: Arc<CompactionStore>,
    message_store_config: Arc<MessageStoreConfig>,
    topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
}

impl CommitLogDispatcherCompaction {
    pub fn new(
        compaction_store: Arc<CompactionStore>,
        message_store_config: Arc<MessageStoreConfig>,
        topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
    ) -> Self {
        Self {
            compaction_store,
            message_store_config,
            topic_config_table,
        }
    }

    fn is_compaction_topic(&self, topic: &CheetahString) -> bool {
        if !self.message_store_config.enable_compaction {
            return false;
        }

        let topic_config = self.topic_config_table.get(topic).map(|entry| entry.value().clone());
        get_delete_policy_arc_mut(topic_config.as_ref()) == CleanupPolicy::COMPACTION
    }
}

impl CommitLogDispatcher for CommitLogDispatcherCompaction {
    fn dispatch(&self, dispatch_request: &mut DispatchRequest) {
        if !dispatch_request.success
            || dispatch_request.msg_size <= 0
            || dispatch_request.commit_log_offset < 0
            || dispatch_request.consume_queue_offset < 0
            || !self.is_compaction_topic(&dispatch_request.topic)
        {
            return;
        }

        self.compaction_store.put_dispatch_message(dispatch_request);
    }

    fn dispatch_progress_offset(&self, commit_log_min_offset: i64) -> Option<i64> {
        self.message_store_config
            .enable_compaction
            .then(|| self.compaction_store.durable_dispatch_offset(commit_log_min_offset))
    }
}
