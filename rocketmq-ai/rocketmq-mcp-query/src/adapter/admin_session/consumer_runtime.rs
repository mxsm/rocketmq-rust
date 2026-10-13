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

//! What one connected consumer client reports about itself.

use rocketmq_admin_core::core::consumer_runtime::ConsumerClientQueueMode as AdminQueueMode;
use rocketmq_admin_core::core::consumer_runtime::ConsumerRuntimeQueryAdmin;
use rocketmq_admin_core::core::consumer_runtime::QueryConsumerClientRuntimeRequest;

use super::map_admin_error;
use super::AdminCoreSession;
use crate::infrastructure::snapshot::RetainedSize;
use crate::model::contract::observed_at_from_millis;
use crate::tools::consumer_tools::ConsumerClientQueueMode;
use crate::tools::consumer_tools::ConsumerClientQueueRow;
use crate::tools::consumer_tools::ConsumerClientSubscription;
use crate::tools::consumer_tools::ConsumerClientTopicStatus;
use crate::tools::executor::NotFoundEntity;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;

/// The running state of one consumer client, in the rows a Tool publishes.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SessionConsumerClientRuntime {
    pub consume_type: Option<String>,
    pub consume_orderly: Option<bool>,
    pub thread_pool_core_size: Option<u32>,
    pub client_version: Option<String>,
    pub started_at: Option<String>,
    pub subscriptions: Vec<ConsumerClientSubscription>,
    pub topic_status: Vec<ConsumerClientTopicStatus>,
    pub queues: Vec<ConsumerClientQueueRow>,
    pub queue_count: usize,
    pub truncated: bool,
}

impl RetainedSize for ConsumerClientSubscription {
    fn retained_heap_size(&self) -> usize {
        self.topic
            .retained_heap_size()
            .saturating_add(self.expression_type.retained_heap_size())
            .saturating_add(self.expression.retained_heap_size())
    }
}

impl RetainedSize for ConsumerClientTopicStatus {
    fn retained_heap_size(&self) -> usize {
        self.topic.retained_heap_size()
    }
}

impl RetainedSize for ConsumerClientQueueRow {
    fn retained_heap_size(&self) -> usize {
        self.topic
            .retained_heap_size()
            .saturating_add(self.broker_name.retained_heap_size())
            .saturating_add(self.last_pull_at.retained_heap_size())
            .saturating_add(self.last_lock_at.retained_heap_size())
            .saturating_add(self.last_consume_at.retained_heap_size())
    }
}

impl RetainedSize for SessionConsumerClientRuntime {
    fn retained_heap_size(&self) -> usize {
        self.consume_type
            .retained_heap_size()
            .saturating_add(self.client_version.retained_heap_size())
            .saturating_add(self.started_at.retained_heap_size())
            .saturating_add(self.subscriptions.retained_heap_size())
            .saturating_add(self.topic_status.retained_heap_size())
            .saturating_add(self.queues.retained_heap_size())
    }
}

impl AdminCoreSession {
    pub(super) async fn query_consumer_client_runtime(
        &mut self,
        consumer_group: &str,
        broker_name: &str,
        client_id: &str,
    ) -> Result<SessionConsumerClientRuntime, ToolFailure> {
        let request = QueryConsumerClientRuntimeRequest::try_new(
            self.cluster.rocketmq_cluster_name.clone(),
            consumer_group,
            broker_name,
            client_id,
        )
        .map_err(|_| ToolFailure::Rejected(ToolRejection::InvalidArguments { _source: None }))?;
        let runtime = self
            .admin_mut()?
            .query_consumer_client_runtime(&request)
            .await
            .map_err(map_admin_error(NotFoundEntity::Broker))?;
        let at = |timestamp: Option<i64>| timestamp.and_then(observed_at_from_millis);
        Ok(SessionConsumerClientRuntime {
            consume_type: runtime.consume_type,
            consume_orderly: runtime.consume_orderly,
            thread_pool_core_size: runtime.thread_pool_core_size,
            client_version: runtime.client_version,
            started_at: at(runtime.started_timestamp),
            subscriptions: runtime
                .subscriptions
                .into_iter()
                .map(|subscription| ConsumerClientSubscription {
                    topic: subscription.topic,
                    expression_type: subscription.expression_type,
                    expression: subscription.expression,
                    sub_version: subscription.sub_version,
                })
                .collect(),
            topic_status: runtime
                .topic_status
                .into_iter()
                .map(|status| ConsumerClientTopicStatus {
                    topic: status.topic,
                    pull_rt_ms: status.pull_rt,
                    pull_tps: status.pull_tps,
                    consume_rt_ms: status.consume_rt,
                    consume_ok_tps: status.consume_ok_tps,
                    consume_failed_tps: status.consume_failed_tps,
                    consume_failed_messages: status.consume_failed_msgs,
                })
                .collect(),
            queues: runtime
                .queues
                .into_iter()
                .map(|row| ConsumerClientQueueRow {
                    topic: row.topic,
                    broker_name: row.broker_name,
                    queue_id: row.queue_id,
                    mode: match row.mode {
                        AdminQueueMode::Pull => ConsumerClientQueueMode::Pull,
                        AdminQueueMode::Pop => ConsumerClientQueueMode::Pop,
                    },
                    dropped: row.dropped,
                    last_pull_at: at(row.last_pull_timestamp),
                    commit_offset: row.commit_offset,
                    cached_message_count: row.cached_message_count,
                    cached_message_size_mib: row.cached_message_size_mib,
                    locked: row.locked,
                    try_unlock_times: row.try_unlock_times,
                    last_lock_at: at(row.last_lock_timestamp),
                    last_consume_at: at(row.last_consume_timestamp),
                    wait_ack_count: row.wait_ack_count,
                })
                .collect(),
            queue_count: runtime.queue_count,
            truncated: runtime.truncated,
        })
    }
}
