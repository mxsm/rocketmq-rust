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

use std::collections::HashMap;
use std::mem::size_of;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_proxy_core::MessageQueueTarget;
use rocketmq_proxy_core::ProxyError;
use rocketmq_proxy_core::ProxyResult;
use rocketmq_proxy_core::ReceiveTarget;
use rocketmq_proxy_core::ResourceIdentity;
use rocketmq_runtime::BudgetCapacity;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::BudgetedQueue;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::QueuePushErrorKind;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ResourceBudgetTree;
use smallvec::SmallVec;
use tokio::sync::Notify;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::build_proxy_producer_group;
use super::ClusterCommand;
use crate::config::ClusterConfig;
use crate::config::ClusterExecutionDiagnostics;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ClusterCommandClass {
    Control,
    ShortData,
    LongPoll,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct ClusterOrderingKey {
    domain: &'static str,
    components: SmallVec<[String; 8]>,
}

impl ClusterOrderingKey {
    fn new(domain: &'static str, components: impl IntoIterator<Item = String>) -> Self {
        Self {
            domain,
            components: components.into_iter().collect(),
        }
    }

    fn singleton(domain: &'static str) -> Self {
        Self::new(domain, [])
    }
}

#[derive(Clone)]
struct RegisteredLane {
    generation: u64,
    queue: BudgetedQueue<QueuedClusterCommand>,
}

#[derive(Debug, Clone)]
pub(super) struct ClusterLaneRegistration {
    pub(super) key: ClusterOrderingKey,
    pub(super) generation: u64,
    pub(super) queue: BudgetedQueue<QueuedClusterCommand>,
}

#[derive(Default)]
struct ClusterExecutionCounters {
    current_inflight: AtomicUsize,
    max_inflight: AtomicUsize,
    admitted: AtomicU64,
    rejected: AtomicU64,
    timed_out: AtomicU64,
    cancelled: AtomicU64,
    shutdown_rejected: AtomicU64,
}

pub(super) struct ClusterExecutionLanes {
    registry: Mutex<HashMap<ClusterOrderingKey, RegisteredLane>>,
    pub(super) root_budget: ResourceBudget,
    pub(super) long_poll_budget: ResourceBudget,
    policy: ClusterExecutionPolicy,
    next_generation: AtomicU64,
    closed: AtomicBool,
    total_inflight: Arc<Semaphore>,
    data_inflight: Arc<Semaphore>,
    long_poll_inflight: Arc<Semaphore>,
    counters: Arc<ClusterExecutionCounters>,
    active_lane_tasks: AtomicUsize,
    lane_tasks_idle: Notify,
}

impl ClusterExecutionLanes {
    pub(super) fn new(policy: ClusterExecutionPolicy) -> ProxyResult<Self> {
        policy.validate()?;
        let control_bytes = policy.control_reserve_bytes();
        let limit = BudgetLimit::new(policy.capacity_count, policy.capacity_bytes, FullPolicy::Reject)
            .with_control_reserve(BudgetCapacity::new(policy.control_reserve, control_bytes));
        let tree = ResourceBudgetTree::new("proxy-cluster-commands", limit).map_err(|error| ProxyError::Transport {
            message: format!("invalid proxy cluster command budget: {error}"),
        })?;
        let long_poll_tree = ResourceBudgetTree::new(
            "proxy-cluster-long-polls",
            BudgetLimit::new(policy.capacity_count, policy.capacity_bytes, FullPolicy::Reject),
        )
        .map_err(|error| ProxyError::Transport {
            message: format!("invalid proxy cluster long-poll budget: {error}"),
        })?;
        let root_budget = tree.root();
        Ok(Self {
            registry: Mutex::new(HashMap::new()),
            root_budget,
            long_poll_budget: long_poll_tree.root(),
            policy,
            next_generation: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            total_inflight: Arc::new(Semaphore::new(policy.io_max_inflight)),
            data_inflight: Arc::new(Semaphore::new(policy.io_max_inflight - policy.control_reserve)),
            long_poll_inflight: Arc::new(Semaphore::new(policy.long_poll_max_inflight)),
            counters: Arc::new(ClusterExecutionCounters::default()),
            active_lane_tasks: AtomicUsize::new(0),
            lane_tasks_idle: Notify::new(),
        })
    }

    pub(super) fn enqueue(
        &self,
        command: ClusterCommand,
        cancellation: CancellationToken,
        config: &ClusterConfig,
    ) -> ProxyResult<Option<ClusterLaneRegistration>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(cluster_execution_unavailable());
        }
        let key = command.ordering_key(config);
        let class = command.class();
        let retained_bytes = command.retained_bytes();
        let queue_deadline = command
            .queue_deadline()
            .unwrap_or(self.policy.max_queue_age)
            .min(self.policy.max_queue_age)
            .max(Duration::from_millis(1));
        let queued = QueuedClusterCommand {
            command,
            enqueued_at: Instant::now(),
            queue_deadline,
            cancellation,
            class,
        };

        let mut registry = self.registry.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.closed.load(Ordering::Acquire) {
            return Err(cluster_execution_unavailable());
        }
        let (registered, created) = match registry.get(&key) {
            Some(registered) => (registered.clone(), false),
            None => {
                let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
                let parent_budget = match class {
                    ClusterCommandClass::LongPoll => &self.long_poll_budget,
                    ClusterCommandClass::Control | ClusterCommandClass::ShortData => &self.root_budget,
                };
                let lane_limit = match class {
                    ClusterCommandClass::LongPoll => BudgetLimit::new(
                        self.policy.capacity_count,
                        self.policy.capacity_bytes,
                        FullPolicy::Reject,
                    ),
                    ClusterCommandClass::Control | ClusterCommandClass::ShortData => BudgetLimit::new(
                        self.policy.capacity_count,
                        self.policy.capacity_bytes,
                        FullPolicy::Reject,
                    )
                    .with_control_reserve(BudgetCapacity::new(
                        self.policy.control_reserve,
                        self.policy.control_reserve_bytes(),
                    )),
                };
                let queue = parent_budget
                    .child(format!("key-{generation}"), lane_limit)
                    .map(BudgetedQueue::new)
                    .map_err(|error| ProxyError::Transport {
                        message: format!("invalid proxy cluster keyed lane budget: {error}"),
                    })?;
                let registered = RegisteredLane { generation, queue };
                registry.insert(key.clone(), registered.clone());
                (registered, true)
            }
        };

        let push_result = match class {
            ClusterCommandClass::Control => registered.queue.try_push_control(queued, retained_bytes),
            ClusterCommandClass::ShortData | ClusterCommandClass::LongPoll => {
                registered.queue.try_push_data(queued, retained_bytes)
            }
        };
        match push_result {
            Ok(_) => {
                self.counters.admitted.fetch_add(1, Ordering::Relaxed);
                if created {
                    Ok(Some(ClusterLaneRegistration {
                        key,
                        generation: registered.generation,
                        queue: registered.queue,
                    }))
                } else {
                    Ok(None)
                }
            }
            Err(error) => {
                let kind = error.kind().clone();
                if created {
                    registry.remove(&key);
                    registered.queue.close();
                }
                self.counters.rejected.fetch_add(1, Ordering::Relaxed);
                let snapshot = registered.queue.snapshot();
                let root_snapshot = match class {
                    ClusterCommandClass::LongPoll => self.long_poll_budget.snapshot(),
                    ClusterCommandClass::Control | ClusterCommandClass::ShortData => self.root_budget.snapshot(),
                };
                tracing::warn!(
                    depth = snapshot.depth,
                    retained_bytes = snapshot.retained_bytes,
                    oldest_age_ms = snapshot.oldest_age.map(|age| age.as_millis() as u64),
                    total_depth = root_snapshot.current_count,
                    total_retained_bytes = root_snapshot.current_bytes,
                    ?kind,
                    "proxy cluster command admission rejected"
                );
                match kind {
                    QueuePushErrorKind::BudgetExhausted(_) | QueuePushErrorKind::DeadlineExceeded => {
                        Err(ProxyError::too_many_requests("proxy-cluster-command-queue"))
                    }
                    QueuePushErrorKind::Closed | QueuePushErrorKind::SlowConsumerClosed => Err(ProxyError::Transport {
                        message: "proxy cluster command execution is unavailable".to_owned(),
                    }),
                }
            }
        }
    }

    pub(super) fn retire(&self, registration: &ClusterLaneRegistration) -> bool {
        let mut registry = self.registry.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let should_remove = registry.get(&registration.key).is_some_and(|registered| {
            registered.generation == registration.generation
                && registered.queue.is_same_queue(&registration.queue)
                && registered.queue.is_empty()
        });
        if should_remove {
            registry.remove(&registration.key);
            registration.queue.close();
        }
        should_remove
    }

    pub(super) fn reject_failed_lane(&self, registration: &ClusterLaneRegistration, message: &str) {
        let mut registry = self.registry.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if registry
            .get(&registration.key)
            .is_some_and(|registered| registered.generation == registration.generation)
        {
            registry.remove(&registration.key);
        }
        registration.queue.close();
        while let Some(queued) = registration.queue.try_pop() {
            queued.command.reject(ProxyError::Transport {
                message: message.to_owned(),
            });
        }
    }

    pub(super) fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let registry = self.registry.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        for registered in registry.values() {
            registered.queue.close();
        }
    }

    pub(super) fn snapshot(&self) -> ClusterExecutionDiagnostics {
        let budget = self.root_budget.snapshot();
        let long_poll_budget = self.long_poll_budget.snapshot();
        let registry = self.registry.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let active_keys = registry.len();
        let oldest_queued_age_ms = registry
            .values()
            .filter_map(|registered| registered.queue.snapshot().oldest_age)
            .map(|age| age.as_millis().min(u128::from(u64::MAX)) as u64)
            .max();
        ClusterExecutionDiagnostics {
            active_keys,
            active_lane_tasks: self.active_lane_tasks.load(Ordering::Acquire),
            queued_and_active: budget.current_count.saturating_add(long_poll_budget.current_count),
            retained_bytes: budget.current_bytes.saturating_add(long_poll_budget.current_bytes),
            long_poll_queued_and_active: long_poll_budget.current_count,
            long_poll_retained_bytes: long_poll_budget.current_bytes,
            oldest_queued_age_ms,
            current_inflight: self.counters.current_inflight.load(Ordering::Relaxed),
            max_inflight: self.counters.max_inflight.load(Ordering::Relaxed),
            current_long_poll_inflight: self
                .policy
                .long_poll_max_inflight
                .saturating_sub(self.long_poll_inflight.available_permits()),
            long_poll_max_inflight: self.policy.long_poll_max_inflight,
            admitted: self.counters.admitted.load(Ordering::Relaxed),
            rejected: self.counters.rejected.load(Ordering::Relaxed),
            timed_out: self.counters.timed_out.load(Ordering::Relaxed),
            cancelled: self.counters.cancelled.load(Ordering::Relaxed),
            shutdown_rejected: self.counters.shutdown_rejected.load(Ordering::Relaxed),
            closed: self.closed.load(Ordering::Acquire),
        }
    }

    pub(super) async fn acquire_inflight(&self, class: ClusterCommandClass) -> Option<ClusterInflightPermit> {
        let (data, total, long_poll) = match class {
            ClusterCommandClass::Control => (
                None,
                Some(self.total_inflight.clone().acquire_owned().await.ok()?),
                None,
            ),
            ClusterCommandClass::ShortData => (
                Some(self.data_inflight.clone().acquire_owned().await.ok()?),
                Some(self.total_inflight.clone().acquire_owned().await.ok()?),
                None,
            ),
            ClusterCommandClass::LongPoll => (
                None,
                None,
                Some(self.long_poll_inflight.clone().acquire_owned().await.ok()?),
            ),
        };
        let current = self.counters.current_inflight.fetch_add(1, Ordering::AcqRel) + 1;
        self.counters.max_inflight.fetch_max(current, Ordering::Relaxed);
        Some(ClusterInflightPermit {
            _data: data,
            _total: total,
            _long_poll: long_poll,
            counters: self.counters.clone(),
        })
    }

    pub(super) fn record_timeout(&self) {
        self.counters.timed_out.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_cancelled(&self) {
        self.counters.cancelled.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_shutdown_rejected(&self) {
        self.counters.shutdown_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn lane_task_started(&self) {
        self.active_lane_tasks.fetch_add(1, Ordering::AcqRel);
    }

    pub(super) fn lane_task_finished(&self) {
        if self.active_lane_tasks.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.lane_tasks_idle.notify_waiters();
        }
    }

    pub(super) async fn wait_for_lane_tasks(&self, deadline: rocketmq_runtime::ShutdownDeadline) -> bool {
        loop {
            let idle = self.lane_tasks_idle.notified();
            if self.active_lane_tasks.load(Ordering::Acquire) == 0 {
                return true;
            }
            if tokio::time::timeout(deadline.remaining(), idle).await.is_err() {
                return self.active_lane_tasks.load(Ordering::Acquire) == 0;
            }
        }
    }

    pub(super) const fn idle_timeout(&self) -> Duration {
        self.policy.lane_idle_timeout
    }
}

impl Drop for ClusterExecutionLanes {
    fn drop(&mut self) {
        self.close();
    }
}

pub(super) struct ClusterInflightPermit {
    _data: Option<OwnedSemaphorePermit>,
    _total: Option<OwnedSemaphorePermit>,
    _long_poll: Option<OwnedSemaphorePermit>,
    counters: Arc<ClusterExecutionCounters>,
}

impl Drop for ClusterInflightPermit {
    fn drop(&mut self) {
        self.counters.current_inflight.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ClusterExecutionPolicy {
    pub(super) capacity_count: usize,
    pub(super) capacity_bytes: usize,
    pub(super) max_queue_age: Duration,
    pub(super) io_max_inflight: usize,
    pub(super) control_reserve: usize,
    pub(super) long_poll_max_inflight: usize,
    pub(super) lane_idle_timeout: Duration,
}

impl Default for ClusterExecutionPolicy {
    fn default() -> Self {
        Self::from_config(&ClusterConfig::default())
    }
}

impl ClusterExecutionPolicy {
    pub(super) fn from_config(config: &ClusterConfig) -> Self {
        Self {
            capacity_count: config.command_queue_capacity,
            capacity_bytes: config.command_queue_max_bytes,
            max_queue_age: config.command_queue_max_age(),
            io_max_inflight: config.io_max_inflight,
            control_reserve: config.control_reserve,
            long_poll_max_inflight: config.long_poll_max_inflight,
            lane_idle_timeout: config.execution_lane_idle_timeout(),
        }
    }

    fn validate(self) -> ProxyResult<()> {
        if self.capacity_count <= self.control_reserve {
            return Err(invalid_execution_policy(
                "command_queue_capacity must be greater than control_reserve",
            ));
        }
        if self.io_max_inflight <= self.control_reserve {
            return Err(invalid_execution_policy(
                "io_max_inflight must be greater than control_reserve",
            ));
        }
        if self.control_reserve == 0 {
            return Err(invalid_execution_policy("control_reserve must be greater than zero"));
        }
        if self.long_poll_max_inflight == 0 {
            return Err(invalid_execution_policy(
                "long_poll_max_inflight must be greater than zero",
            ));
        }
        if self.max_queue_age.is_zero() {
            return Err(invalid_execution_policy(
                "command_queue_max_age_ms must be greater than zero",
            ));
        }
        if self.lane_idle_timeout.is_zero() {
            return Err(invalid_execution_policy(
                "execution_lane_idle_timeout_ms must be greater than zero",
            ));
        }
        let control_bytes = self.control_reserve_bytes();
        if control_bytes >= self.capacity_bytes {
            return Err(invalid_execution_policy(
                "command_queue_max_bytes must leave capacity for data commands",
            ));
        }
        Ok(())
    }

    fn control_reserve_bytes(self) -> usize {
        let proportional = self
            .capacity_bytes
            .saturating_mul(self.control_reserve)
            .checked_div(self.capacity_count)
            .unwrap_or(0);
        proportional.max(size_of::<ClusterCommand>())
    }
}

pub(super) struct QueuedClusterCommand {
    pub(super) command: ClusterCommand,
    pub(super) enqueued_at: Instant,
    pub(super) queue_deadline: Duration,
    pub(super) cancellation: CancellationToken,
    pub(super) class: ClusterCommandClass,
}

impl ClusterCommand {
    pub(super) fn class(&self) -> ClusterCommandClass {
        match self {
            Self::ReadinessCheck { .. }
            | Self::SyncLiteSubscription { .. }
            | Self::AckMessage { .. }
            | Self::ForwardMessageToDeadLetterQueue { .. }
            | Self::ChangeInvisibleDuration { .. }
            | Self::UpdateOffset { .. }
            | Self::GetOffset { .. }
            | Self::QueryOffset { .. } => ClusterCommandClass::Control,
            Self::ReceiveMessage { .. } | Self::PullMessage { .. } => ClusterCommandClass::LongPoll,
            _ => ClusterCommandClass::ShortData,
        }
    }

    pub(super) fn ordering_key(&self, config: &ClusterConfig) -> ClusterOrderingKey {
        match self {
            Self::ReadinessCheck { .. } => ClusterOrderingKey::singleton("readiness"),
            Self::SyncLiteSubscription { client_id, request, .. } => ClusterOrderingKey::new(
                "consumer-control",
                [
                    client_id.clone(),
                    request.group.namespace().to_owned(),
                    request.group.name().to_owned(),
                    request.topic.namespace().to_owned(),
                    request.topic.name().to_owned(),
                ],
            ),
            Self::QueryRoute { topic, .. } | Self::QueryTopicMessageType { topic, .. } => {
                resource_ordering_key("topic", [topic])
            }
            Self::QueryAssignment { topic, group, .. } | Self::QuerySubscriptionGroup { topic, group, .. } => {
                resource_ordering_key("topic-group", [topic, group])
            }
            Self::QueryUser { username, .. } => ClusterOrderingKey::new("user", [username.clone()]),
            Self::QueryAcl { subject, .. } => ClusterOrderingKey::new("acl", [subject.clone()]),
            Self::SendMessage {
                client_id, request_id, ..
            } => ClusterOrderingKey::new(
                "producer",
                [build_proxy_producer_group(
                    config,
                    client_id.as_deref(),
                    request_id.as_str(),
                )],
            ),
            Self::RecallMessage {
                client_id, request_id, ..
            } => ClusterOrderingKey::new(
                "producer",
                [build_proxy_producer_group(
                    config,
                    client_id.as_deref(),
                    request_id.as_str(),
                )],
            ),
            Self::EndTransaction {
                request,
                client_id,
                request_id,
                ..
            } => ClusterOrderingKey::new(
                "producer",
                [request
                    .producer_group
                    .clone()
                    .unwrap_or_else(|| build_proxy_producer_group(config, client_id.as_deref(), request_id.as_str()))],
            ),
            Self::ReceiveMessage { request, .. } => {
                receive_target_ordering_key("consumer-poll", &request.group, &request.target)
            }
            Self::PullMessage { request, .. } => {
                target_ordering_key("consumer-poll", Some(&request.group), &request.target)
            }
            Self::AckMessage { request, .. } => {
                resource_ordering_key("consumer-control", [&request.group, &request.topic])
            }
            Self::ForwardMessageToDeadLetterQueue { request, .. } => {
                resource_ordering_key("consumer-control", [&request.group, &request.topic])
            }
            Self::ChangeInvisibleDuration { request, .. } => {
                resource_ordering_key("consumer-control", [&request.group, &request.topic])
            }
            Self::UpdateOffset { request, .. } => {
                target_ordering_key("consumer-offset", Some(&request.group), &request.target)
            }
            Self::GetOffset { request, .. } => {
                target_ordering_key("consumer-offset", Some(&request.group), &request.target)
            }
            Self::QueryOffset { request, .. } => target_ordering_key("consumer-offset", None, &request.target),
            Self::LockBatchMq { request, .. } => ClusterOrderingKey::new(
                "consumer-lock",
                [
                    request
                        .consumer_group
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                    request.client_id.as_ref().map(ToString::to_string).unwrap_or_default(),
                ],
            ),
            Self::UnlockBatchMq { request, .. } => ClusterOrderingKey::new(
                "consumer-lock",
                [
                    request
                        .consumer_group
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                    request.client_id.as_ref().map(ToString::to_string).unwrap_or_default(),
                ],
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
            | Self::SyncLiteSubscription { .. }
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
            | Self::SyncLiteSubscription { .. }
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
            Self::SyncLiteSubscription { client_id, request, .. } => {
                client_id.len()
                    + resource_identity_bytes(&request.topic)
                    + resource_identity_bytes(&request.group)
                    + request.lite_topic_set.iter().map(String::len).sum::<usize>()
            }
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
            Self::SyncLiteSubscription { reply, .. } => {
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

fn resource_ordering_key<'a>(
    domain: &'static str,
    identities: impl IntoIterator<Item = &'a ResourceIdentity>,
) -> ClusterOrderingKey {
    let components = identities
        .into_iter()
        .flat_map(|identity| [identity.namespace().to_owned(), identity.name().to_owned()]);
    ClusterOrderingKey::new(domain, components)
}

fn target_ordering_key(
    domain: &'static str,
    group: Option<&ResourceIdentity>,
    target: &MessageQueueTarget,
) -> ClusterOrderingKey {
    let mut components = Vec::with_capacity(8);
    if let Some(group) = group {
        components.push(group.namespace().to_owned());
        components.push(group.name().to_owned());
    }
    components.push(target.topic.namespace().to_owned());
    components.push(target.topic.name().to_owned());
    components.push(target.queue_id.to_string());
    components.push(target.broker_name.clone().unwrap_or_default());
    components.push(target.broker_addr.clone().unwrap_or_default());
    ClusterOrderingKey::new(domain, components)
}

fn receive_target_ordering_key(
    domain: &'static str,
    group: &ResourceIdentity,
    target: &ReceiveTarget,
) -> ClusterOrderingKey {
    let mut components = Vec::with_capacity(8);
    components.push(group.namespace().to_owned());
    components.push(group.name().to_owned());
    components.push(target.topic.namespace().to_owned());
    components.push(target.topic.name().to_owned());
    components.push(target.queue_id.to_string());
    components.push(target.broker_name.clone().unwrap_or_default());
    components.push(target.broker_addr.clone().unwrap_or_default());
    ClusterOrderingKey::new(domain, components)
}

fn invalid_execution_policy(message: &str) -> ProxyError {
    ProxyError::Transport {
        message: format!("invalid proxy cluster execution policy: {message}"),
    }
}

fn cluster_execution_unavailable() -> ProxyError {
    ProxyError::Transport {
        message: "proxy cluster command execution is unavailable".to_owned(),
    }
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
