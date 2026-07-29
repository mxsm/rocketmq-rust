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

use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::mem::size_of;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_client_rust::proxy_adapter_compat::MessageQueue;
use rocketmq_proxy_core::MessageQueueTarget;
use rocketmq_proxy_core::ProxyError;
use rocketmq_proxy_core::ProxyResult;
use rocketmq_proxy_core::ResourceIdentity;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::BudgetedQueue;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::QueuePushErrorKind;
#[cfg(test)]
use rocketmq_runtime::QueueSnapshot;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ResourceBudgetTree;

use super::ClusterCommand;

pub(super) const CLUSTER_COMMAND_CAPACITY: usize = 1024;
pub(super) const CLUSTER_COMMAND_BYTE_CAPACITY: usize = 64 * 1024 * 1024;
pub(super) const CLUSTER_EXECUTION_LANE_COUNT: usize = 16;
const CLUSTER_LANES_PER_CLASS: usize = 4;
const CLUSTER_DEFAULT_QUEUE_DEADLINE: Duration = Duration::from_secs(30);

pub(super) struct ClusterExecutionLanes {
    pub(super) queues: Box<[BudgetedQueue<QueuedClusterCommand>]>,
    pub(super) root_budget: ResourceBudget,
    default_queue_deadline: Duration,
}

impl ClusterExecutionLanes {
    pub(super) fn new(policy: ClusterExecutionPolicy) -> ProxyResult<Self> {
        let tree = ResourceBudgetTree::new(
            "proxy-cluster-commands",
            BudgetLimit::new(policy.capacity_count, policy.capacity_bytes, FullPolicy::Reject),
        )
        .map_err(|error| ProxyError::Transport {
            message: format!("invalid proxy cluster command budget: {error}"),
        })?;
        let root_budget = tree.root();
        let queues = (0..CLUSTER_EXECUTION_LANE_COUNT)
            .map(|lane| {
                root_budget
                    .child(
                        format!("lane-{lane}"),
                        BudgetLimit::new(policy.capacity_count, policy.capacity_bytes, FullPolicy::Reject),
                    )
                    .map(BudgetedQueue::new)
                    .map_err(|error| ProxyError::Transport {
                        message: format!("invalid proxy cluster command lane budget: {error}"),
                    })
            })
            .collect::<ProxyResult<Vec<_>>>()?
            .into_boxed_slice();
        Ok(Self {
            queues,
            root_budget,
            default_queue_deadline: policy.default_queue_deadline,
        })
    }

    pub(super) fn enqueue(&self, command: ClusterCommand) -> ProxyResult<()> {
        let lane = command.lane();
        let retained_bytes = command.retained_bytes();
        let deadline = command
            .queue_deadline()
            .unwrap_or(self.default_queue_deadline)
            .max(Duration::from_millis(1));
        let queued = QueuedClusterCommand {
            command,
            enqueued_at: Instant::now(),
            deadline,
        };
        match self.queues[lane].try_push_data(queued, retained_bytes) {
            Ok(_) => Ok(()),
            Err(error) => {
                let kind = error.kind().clone();
                let snapshot = self.queues[lane].snapshot();
                let root_snapshot = self.root_budget.snapshot();
                tracing::warn!(
                    lane,
                    depth = snapshot.depth,
                    retained_bytes = snapshot.retained_bytes,
                    oldest_age_ms = snapshot.oldest_age.map(|age| age.as_millis() as u64),
                    total_depth = root_snapshot.current_count,
                    total_retained_bytes = root_snapshot.current_bytes,
                    ?kind,
                    "proxy cluster command admission rejected"
                );
                match kind {
                    QueuePushErrorKind::BudgetExhausted(_) => {
                        Err(ProxyError::too_many_requests("proxy-cluster-command-queue"))
                    }
                    QueuePushErrorKind::Closed | QueuePushErrorKind::SlowConsumerClosed => Err(ProxyError::Transport {
                        message: "proxy cluster command execution is unavailable".to_owned(),
                    }),
                }
            }
        }
    }

    pub(super) fn queue_clones(&self) -> Vec<BudgetedQueue<QueuedClusterCommand>> {
        self.queues.to_vec()
    }

    #[cfg(test)]
    pub(super) fn snapshots(&self) -> Vec<QueueSnapshot> {
        self.queues.iter().map(BudgetedQueue::snapshot).collect()
    }
}

impl Drop for ClusterExecutionLanes {
    fn drop(&mut self) {
        for queue in &self.queues {
            queue.close();
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct ClusterExecutionPolicy {
    pub(super) capacity_count: usize,
    pub(super) capacity_bytes: usize,
    pub(super) default_queue_deadline: Duration,
}

impl Default for ClusterExecutionPolicy {
    fn default() -> Self {
        Self {
            capacity_count: CLUSTER_COMMAND_CAPACITY,
            capacity_bytes: CLUSTER_COMMAND_BYTE_CAPACITY,
            default_queue_deadline: CLUSTER_DEFAULT_QUEUE_DEADLINE,
        }
    }
}

pub(super) struct QueuedClusterCommand {
    pub(super) command: ClusterCommand,
    pub(super) enqueued_at: Instant,
    pub(super) deadline: Duration,
}

impl ClusterCommand {
    pub(super) fn lane(&self) -> usize {
        match self {
            Self::ReadinessCheck { .. } => cluster_lane(0, &"readiness"),
            Self::QueryRoute { topic, .. } | Self::QueryTopicMessageType { topic, .. } => cluster_lane(0, topic),
            Self::QueryAssignment { topic, group, .. } | Self::QuerySubscriptionGroup { topic, group, .. } => {
                cluster_lane(0, &(topic, group))
            }
            Self::QueryUser { username, .. } => cluster_lane(0, username),
            Self::QueryAcl { subject, .. } => cluster_lane(0, subject),
            Self::SendMessage {
                client_id, request_id, ..
            }
            | Self::RecallMessage {
                client_id, request_id, ..
            } => cluster_lane(1, &client_id.as_deref().unwrap_or(request_id.as_str())),
            Self::EndTransaction {
                request,
                client_id,
                request_id,
                ..
            } => cluster_lane(
                1,
                &client_id
                    .as_deref()
                    .or(request.producer_group.as_deref())
                    .unwrap_or(request_id.as_str()),
            ),
            Self::ReceiveMessage { request, .. } => cluster_lane(2, &(&request.group, &request.target.topic)),
            Self::PullMessage { request, .. } => cluster_lane(2, &(&request.group, &request.target.topic)),
            Self::AckMessage { request, .. } => cluster_lane(2, &(&request.group, &request.topic)),
            Self::ForwardMessageToDeadLetterQueue { request, .. } => cluster_lane(2, &(&request.group, &request.topic)),
            Self::ChangeInvisibleDuration { request, .. } => cluster_lane(2, &(&request.group, &request.topic)),
            Self::UpdateOffset { request, .. } => cluster_lane(2, &(&request.group, &request.target.topic)),
            Self::GetOffset { request, .. } => cluster_lane(2, &(&request.group, &request.target.topic)),
            Self::QueryOffset { request, .. } => cluster_lane(2, &request.target.topic),
            Self::LockBatchMq { request, .. } => cluster_lane(
                3,
                &(
                    request.consumer_group.as_deref().unwrap_or_default(),
                    request.client_id.as_deref().unwrap_or_default(),
                ),
            ),
            Self::UnlockBatchMq { request, .. } => cluster_lane(
                3,
                &(
                    request.consumer_group.as_deref().unwrap_or_default(),
                    request.client_id.as_deref().unwrap_or_default(),
                ),
            ),
        }
    }

    pub(super) fn queue_deadline(&self) -> Option<Duration> {
        match self {
            Self::SendMessage { request, .. } => request.timeout,
            Self::ReceiveMessage { deadline, .. }
            | Self::PullMessage { deadline, .. }
            | Self::AckMessage { deadline, .. }
            | Self::ForwardMessageToDeadLetterQueue { deadline, .. }
            | Self::ChangeInvisibleDuration { deadline, .. }
            | Self::UpdateOffset { deadline, .. }
            | Self::GetOffset { deadline, .. }
            | Self::QueryOffset { deadline, .. }
            | Self::EndTransaction { deadline, .. } => *deadline,
            Self::ReadinessCheck { .. }
            | Self::QueryRoute { .. }
            | Self::QueryAssignment { .. }
            | Self::QueryTopicMessageType { .. }
            | Self::QuerySubscriptionGroup { .. }
            | Self::QueryUser { .. }
            | Self::QueryAcl { .. }
            | Self::RecallMessage { .. }
            | Self::LockBatchMq { .. }
            | Self::UnlockBatchMq { .. } => None,
        }
    }

    pub(super) fn apply_queue_wait(&mut self, waited: Duration) {
        match self {
            Self::SendMessage { request, .. } => {
                request.timeout = request.timeout.map(|deadline| deadline.saturating_sub(waited));
            }
            Self::ReceiveMessage { deadline, .. }
            | Self::PullMessage { deadline, .. }
            | Self::AckMessage { deadline, .. }
            | Self::ForwardMessageToDeadLetterQueue { deadline, .. }
            | Self::ChangeInvisibleDuration { deadline, .. }
            | Self::UpdateOffset { deadline, .. }
            | Self::GetOffset { deadline, .. }
            | Self::QueryOffset { deadline, .. }
            | Self::EndTransaction { deadline, .. } => {
                *deadline = deadline.map(|value| value.saturating_sub(waited));
            }
            Self::ReadinessCheck { .. }
            | Self::QueryRoute { .. }
            | Self::QueryAssignment { .. }
            | Self::QueryTopicMessageType { .. }
            | Self::QuerySubscriptionGroup { .. }
            | Self::QueryUser { .. }
            | Self::QueryAcl { .. }
            | Self::RecallMessage { .. }
            | Self::LockBatchMq { .. }
            | Self::UnlockBatchMq { .. } => {}
        }
    }

    pub(super) fn retained_bytes(&self) -> usize {
        let dynamic = match self {
            Self::ReadinessCheck { .. } => 0,
            Self::QueryRoute { topic, .. } | Self::QueryTopicMessageType { topic, .. } => {
                resource_identity_bytes(topic)
            }
            Self::QueryAssignment {
                topic,
                group,
                client_id,
                ..
            } => resource_identity_bytes(topic) + resource_identity_bytes(group) + client_id.len(),
            Self::QuerySubscriptionGroup { topic, group, .. } => {
                resource_identity_bytes(topic) + resource_identity_bytes(group)
            }
            Self::QueryUser { username, .. } => username.len(),
            Self::QueryAcl { subject, .. } => subject.len(),
            Self::SendMessage {
                request,
                client_id,
                request_id,
                ..
            } => {
                option_string_bytes(client_id.as_ref())
                    + request_id.len()
                    + request
                        .messages
                        .iter()
                        .map(|entry| {
                            resource_identity_bytes(&entry.topic)
                                + entry.client_message_id.len()
                                + entry.message.topic().len()
                                + entry.message.body().map_or(0, <[u8]>::len)
                                + entry
                                    .message
                                    .properties()
                                    .iter()
                                    .map(|(key, value)| key.len() + value.len())
                                    .sum::<usize>()
                                + entry.message.transaction_id().map_or(0, str::len)
                        })
                        .sum::<usize>()
            }
            Self::RecallMessage {
                request,
                client_id,
                request_id,
                ..
            } => {
                resource_identity_bytes(&request.topic)
                    + request.recall_handle.len()
                    + option_string_bytes(client_id.as_ref())
                    + request_id.len()
            }
            Self::ReceiveMessage { request, .. } => {
                resource_identity_bytes(&request.group)
                    + resource_identity_bytes(&request.target.topic)
                    + request.target.broker_name.as_deref().map_or(0, str::len)
                    + request.target.broker_addr.as_deref().map_or(0, str::len)
                    + request.filter_expression.expression_type.len()
                    + request.filter_expression.expression.len()
                    + request.attempt_id.as_deref().map_or(0, str::len)
            }
            Self::PullMessage { request, .. } => {
                resource_identity_bytes(&request.group)
                    + message_queue_target_bytes(&request.target)
                    + request.filter_expression.expression_type.len()
                    + request.filter_expression.expression.len()
            }
            Self::AckMessage { request, .. } => {
                resource_identity_bytes(&request.group)
                    + resource_identity_bytes(&request.topic)
                    + request
                        .entries
                        .iter()
                        .map(|entry| {
                            entry.message_id.len()
                                + entry.receipt_handle.len()
                                + entry.lite_topic.as_deref().map_or(0, str::len)
                        })
                        .sum::<usize>()
            }
            Self::ForwardMessageToDeadLetterQueue { request, .. } => {
                resource_identity_bytes(&request.group)
                    + resource_identity_bytes(&request.topic)
                    + request.receipt_handle.len()
                    + request.message_id.len()
                    + request.lite_topic.as_deref().map_or(0, str::len)
            }
            Self::ChangeInvisibleDuration { request, .. } => {
                resource_identity_bytes(&request.group)
                    + resource_identity_bytes(&request.topic)
                    + request.receipt_handle.len()
                    + request.message_id.len()
                    + request.lite_topic.as_deref().map_or(0, str::len)
            }
            Self::UpdateOffset { request, .. } => {
                resource_identity_bytes(&request.group) + message_queue_target_bytes(&request.target)
            }
            Self::GetOffset { request, .. } => {
                resource_identity_bytes(&request.group) + message_queue_target_bytes(&request.target)
            }
            Self::QueryOffset { request, .. } => message_queue_target_bytes(&request.target),
            Self::EndTransaction {
                request,
                client_id,
                request_id,
                ..
            } => {
                resource_identity_bytes(&request.topic)
                    + request.message_id.len()
                    + request.transaction_id.len()
                    + request.trace_context.as_deref().map_or(0, str::len)
                    + request.producer_group.as_deref().map_or(0, str::len)
                    + request.commit_log_message_id.as_deref().map_or(0, str::len)
                    + option_string_bytes(client_id.as_ref())
                    + request_id.len()
            }
            Self::LockBatchMq { request, .. } => lock_request_bytes(
                request.consumer_group.as_ref(),
                request.client_id.as_ref(),
                &request.mq_set,
            ),
            Self::UnlockBatchMq { request, .. } => lock_request_bytes(
                request.consumer_group.as_ref(),
                request.client_id.as_ref(),
                &request.mq_set,
            ),
        };
        size_of::<Self>().saturating_add(dynamic)
    }

    pub(super) fn reject(self, error: ProxyError) {
        match self {
            Self::ReadinessCheck { reply } => {
                let _ = reply.send(Err(error));
            }
            Self::QueryRoute { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::QueryAssignment { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::QueryTopicMessageType { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::QuerySubscriptionGroup { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::QueryUser { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::QueryAcl { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::SendMessage { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::RecallMessage { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::ReceiveMessage { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::PullMessage { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::AckMessage { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::ForwardMessageToDeadLetterQueue { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::ChangeInvisibleDuration { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::UpdateOffset { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::GetOffset { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::QueryOffset { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::EndTransaction { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::LockBatchMq { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::UnlockBatchMq { reply, .. } => {
                let _ = reply.send(Err(error));
            }
        }
    }
}

pub(super) fn cluster_lane(class: usize, key: &impl Hash) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    class * CLUSTER_LANES_PER_CLASS + (hasher.finish() as usize % CLUSTER_LANES_PER_CLASS)
}

pub(super) fn cluster_lane_domain_id(base_domain_id: u64, lane: usize) -> u64 {
    base_domain_id
        .wrapping_mul(CLUSTER_EXECUTION_LANE_COUNT as u64)
        .wrapping_add(lane as u64 + 1)
}

fn resource_identity_bytes(identity: &ResourceIdentity) -> usize {
    identity.namespace().len() + identity.name().len()
}

fn message_queue_target_bytes(target: &MessageQueueTarget) -> usize {
    resource_identity_bytes(&target.topic)
        + target.broker_name.as_deref().map_or(0, str::len)
        + target.broker_addr.as_deref().map_or(0, str::len)
}

fn option_string_bytes(value: Option<&String>) -> usize {
    value.map_or(0, String::len)
}

fn lock_request_bytes(
    consumer_group: Option<&CheetahString>,
    client_id: Option<&CheetahString>,
    queues: &std::collections::HashSet<MessageQueue>,
) -> usize {
    consumer_group.map_or(0, CheetahString::len)
        + client_id.map_or(0, CheetahString::len)
        + queues
            .iter()
            .map(|queue| queue.topic().len() + queue.broker_name().len())
            .sum::<usize>()
}
