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
use std::future::Future;
use std::hash::Hash;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::header::ack_message_request_header::AckMessageRequestHeader;
use rocketmq_protocol::protocol::header::change_invisible_time_request_header::ChangeInvisibleTimeRequestHeader;
use rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_proxy_core::ResourceIdentity;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ShutdownDeadline;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Notify;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::config::LocalConfig;
use crate::local::LocalBrokerCommand;
use crate::local::QueuedLocalBrokerCommand;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LocalExecutionClass {
    Control,
    ShortData,
    LongPoll,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct LocalOrderingKey {
    domain: &'static str,
    components: Vec<String>,
}

impl LocalOrderingKey {
    fn new(domain: &'static str, components: impl IntoIterator<Item = String>) -> Self {
        Self {
            domain,
            components: components.into_iter().collect(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct LocalExecutionPolicy {
    capacity_count: usize,
    max_queue_age: Duration,
    io_max_inflight: usize,
    control_reserve: usize,
    long_poll_max_inflight: usize,
    lane_idle_timeout: Duration,
}

impl LocalExecutionPolicy {
    pub(crate) fn from_config(config: &LocalConfig) -> Self {
        Self {
            capacity_count: config.command_queue_capacity,
            max_queue_age: config.command_queue_max_age(),
            io_max_inflight: config.io_max_inflight,
            control_reserve: config.control_reserve,
            long_poll_max_inflight: config.long_poll_max_inflight,
            lane_idle_timeout: config.execution_lane_idle_timeout(),
        }
    }
}

pub(crate) trait LocalCommandHandler: Send + Sync + 'static {
    fn handle(&self, command: LocalBrokerCommand) -> impl Future<Output = ()> + Send;
}

struct LocalExecutionLimits {
    total_inflight: Arc<Semaphore>,
    short_data_inflight: Arc<Semaphore>,
    long_poll_inflight: Arc<Semaphore>,
}

impl LocalExecutionLimits {
    fn new(policy: LocalExecutionPolicy) -> Self {
        Self {
            total_inflight: Arc::new(Semaphore::new(policy.io_max_inflight)),
            short_data_inflight: Arc::new(Semaphore::new(policy.io_max_inflight - policy.control_reserve)),
            long_poll_inflight: Arc::new(Semaphore::new(policy.long_poll_max_inflight)),
        }
    }

    async fn acquire(&self, class: LocalExecutionClass) -> Option<LocalInflightPermit> {
        let (short_data, total, long_poll) = match class {
            LocalExecutionClass::Control => (
                None,
                Some(self.total_inflight.clone().acquire_owned().await.ok()?),
                None,
            ),
            LocalExecutionClass::ShortData => (
                Some(self.short_data_inflight.clone().acquire_owned().await.ok()?),
                Some(self.total_inflight.clone().acquire_owned().await.ok()?),
                None,
            ),
            LocalExecutionClass::LongPoll => (
                None,
                None,
                Some(self.long_poll_inflight.clone().acquire_owned().await.ok()?),
            ),
        };
        Some(LocalInflightPermit {
            _short_data: short_data,
            _total: total,
            _long_poll: long_poll,
        })
    }
}

struct LocalInflightPermit {
    _short_data: Option<OwnedSemaphorePermit>,
    _total: Option<OwnedSemaphorePermit>,
    _long_poll: Option<OwnedSemaphorePermit>,
}

#[derive(Clone)]
struct RegisteredLane {
    generation: u64,
    sender: mpsc::Sender<QueuedLocalBrokerCommand>,
}

struct LaneRetirement {
    key: LocalOrderingKey,
    generation: u64,
    decision: oneshot::Sender<bool>,
}

struct ActiveLanes {
    count: AtomicUsize,
    idle: Notify,
}

impl ActiveLanes {
    fn new() -> Self {
        Self {
            count: AtomicUsize::new(0),
            idle: Notify::new(),
        }
    }

    fn start(self: &Arc<Self>) -> ActiveLaneGuard {
        self.count.fetch_add(1, Ordering::AcqRel);
        ActiveLaneGuard { lanes: self.clone() }
    }

    async fn wait_until_idle(&self, deadline: ShutdownDeadline) -> bool {
        loop {
            let idle = self.idle.notified();
            if self.count.load(Ordering::Acquire) == 0 {
                return true;
            }
            if tokio::time::timeout(deadline.remaining(), idle).await.is_err() {
                return self.count.load(Ordering::Acquire) == 0;
            }
        }
    }
}

struct ActiveLaneGuard {
    lanes: Arc<ActiveLanes>,
}

impl Drop for ActiveLaneGuard {
    fn drop(&mut self) {
        if self.lanes.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.lanes.idle.notify_waiters();
        }
    }
}

pub(crate) async fn run_local_execution<H>(
    policy: LocalExecutionPolicy,
    mut receiver: mpsc::Receiver<QueuedLocalBrokerCommand>,
    cancellation: CancellationToken,
    shutdown_context: ChildServiceContext,
    lane_context: ChildServiceContext,
    handler: Arc<H>,
    shutdown_timeout: Duration,
) where
    H: LocalCommandHandler,
{
    let limits = Arc::new(LocalExecutionLimits::new(policy));
    let active_lanes = Arc::new(ActiveLanes::new());
    let next_generation = AtomicU64::new(1);
    let (retirement_tx, mut retirement_rx) = mpsc::channel(policy.capacity_count);
    let mut registry = HashMap::<LocalOrderingKey, RegisteredLane>::new();
    let mut cancelled = false;

    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                cancelled = true;
                receiver.close();
                while let Ok(queued) = receiver.try_recv() {
                    queued.command.reject_unavailable();
                }
                break;
            }
            retirement = retirement_rx.recv() => {
                if let Some(retirement) = retirement {
                    decide_lane_retirement(&mut registry, retirement);
                }
            }
            queued = receiver.recv() => match queued {
                Some(queued) => dispatch_to_lane(
                    queued,
                    policy,
                    &mut registry,
                    &next_generation,
                    &retirement_tx,
                    &lane_context,
                    &active_lanes,
                    &limits,
                    &handler,
                ),
                None => break,
            },
        }
    }

    registry.clear();
    drop(retirement_tx);
    let deadline = shutdown_context
        .task_group()
        .shutdown_deadline()
        .unwrap_or_else(|| ShutdownDeadline::after(shutdown_timeout));
    if cancelled {
        lane_context.task_group().cancel();
    }
    if !active_lanes.wait_until_idle(deadline).await {
        lane_context.task_group().cancel();
    }
    let report = lane_context.task_group().shutdown_until(deadline).await;
    if !report.is_healthy() {
        tracing::warn!(report = %report.to_json(), "proxy local command lanes did not shut down cleanly");
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "lane construction keeps all lifecycle and budget owners explicit"
)]
fn dispatch_to_lane<H>(
    mut queued: QueuedLocalBrokerCommand,
    policy: LocalExecutionPolicy,
    registry: &mut HashMap<LocalOrderingKey, RegisteredLane>,
    next_generation: &AtomicU64,
    retirement_tx: &mpsc::Sender<LaneRetirement>,
    lane_context: &ChildServiceContext,
    active_lanes: &Arc<ActiveLanes>,
    limits: &Arc<LocalExecutionLimits>,
    handler: &Arc<H>,
) where
    H: LocalCommandHandler,
{
    let now = Instant::now();
    if queued.is_expired(now, policy.max_queue_age) {
        queued.command.reject_overload();
        return;
    }
    if queued.deadline_expired(now) {
        queued.command.reject_timeout(queued.timeout_budget.unwrap_or_default());
        return;
    }
    let key = queued.command.ordering_key();
    loop {
        if let Some(registered) = registry.get(&key) {
            match registered.sender.try_send(queued) {
                Ok(()) => return,
                Err(mpsc::error::TrySendError::Full(rejected)) => {
                    rejected.command.reject_overload();
                    return;
                }
                Err(mpsc::error::TrySendError::Closed(rejected)) => {
                    registry.remove(&key);
                    queued = rejected;
                    continue;
                }
            }
        }

        let generation = next_generation.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(policy.capacity_count);
        let lane_cancellation = lane_context.task_group().cancellation_token();
        let lane_retirement = retirement_tx.clone();
        let lane_key = key.clone();
        let lane_limits = limits.clone();
        let lane_handler = handler.clone();
        let active_guard = active_lanes.start();
        let spawn = lane_context.spawn_service(format!("proxy.local.lane.{generation}"), async move {
            let _active_guard = active_guard;
            run_local_lane(
                lane_key,
                generation,
                receiver,
                lane_retirement,
                lane_cancellation,
                policy,
                lane_limits,
                lane_handler,
            )
            .await;
        });
        if let Err(error) = spawn {
            queued
                .command
                .reject_with_transport(format!("failed to spawn proxy local command lane: {error}"));
            return;
        }
        registry.insert(
            key.clone(),
            RegisteredLane {
                generation,
                sender: sender.clone(),
            },
        );
        match sender.try_send(queued) {
            Ok(()) => return,
            Err(mpsc::error::TrySendError::Full(rejected)) | Err(mpsc::error::TrySendError::Closed(rejected)) => {
                registry.remove(&key);
                rejected.command.reject_overload();
                return;
            }
        }
    }
}

fn decide_lane_retirement(registry: &mut HashMap<LocalOrderingKey, RegisteredLane>, retirement: LaneRetirement) {
    let retire = registry.get(&retirement.key).is_some_and(|registered| {
        registered.generation == retirement.generation
            && registered.sender.capacity() == registered.sender.max_capacity()
    });
    if retire {
        registry.remove(&retirement.key);
    }
    let _ = retirement.decision.send(retire);
}

#[allow(
    clippy::too_many_arguments,
    reason = "lane execution keeps ownership and limits explicit"
)]
async fn run_local_lane<H>(
    key: LocalOrderingKey,
    generation: u64,
    mut receiver: mpsc::Receiver<QueuedLocalBrokerCommand>,
    retirement_tx: mpsc::Sender<LaneRetirement>,
    cancellation: CancellationToken,
    policy: LocalExecutionPolicy,
    limits: Arc<LocalExecutionLimits>,
    handler: Arc<H>,
) where
    H: LocalCommandHandler,
{
    loop {
        let queued = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                reject_lane(&mut receiver);
                return;
            }
            queued = tokio::time::timeout(policy.lane_idle_timeout, receiver.recv()) => match queued {
                Ok(Some(queued)) => queued,
                Ok(None) => return,
                Err(_) => {
                    let (decision, response) = oneshot::channel();
                    if retirement_tx
                        .send(LaneRetirement {
                            key: key.clone(),
                            generation,
                            decision,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                    match response.await {
                        Ok(true) => return,
                        Ok(false) => continue,
                        Err(_) => return,
                    }
                }
            },
        };
        execute_queued_command(queued, policy.max_queue_age, &limits, handler.as_ref(), &cancellation).await;
    }
}

async fn execute_queued_command<H>(
    mut queued: QueuedLocalBrokerCommand,
    max_queue_age: Duration,
    limits: &LocalExecutionLimits,
    handler: &H,
    cancellation: &CancellationToken,
) where
    H: LocalCommandHandler,
{
    let now = Instant::now();
    if queued.is_expired(now, max_queue_age) {
        queued.command.reject_overload();
        return;
    }
    if queued.deadline_expired(now) {
        queued.command.reject_timeout(queued.timeout_budget.unwrap_or_default());
        return;
    }
    let class = queued.command.execution_class();
    let permit = tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            queued.command.reject_unavailable();
            return;
        }
        permit = limits.acquire(class) => permit,
    };
    let Some(_permit) = permit else {
        queued.command.reject_unavailable();
        return;
    };
    let now = Instant::now();
    if queued.deadline_expired(now) {
        queued.command.reject_timeout(queued.timeout_budget.unwrap_or_default());
        return;
    }
    queued.apply_remaining_deadline(now);
    tokio::select! {
        biased;
        () = cancellation.cancelled() => {}
        () = handler.handle(queued.command) => {}
    }
}

fn reject_lane(receiver: &mut mpsc::Receiver<QueuedLocalBrokerCommand>) {
    receiver.close();
    while let Ok(queued) = receiver.try_recv() {
        queued.command.reject_unavailable();
    }
}

impl LocalBrokerCommand {
    pub(crate) fn execution_class(&self) -> LocalExecutionClass {
        match self {
            Self::QueryRoute { .. }
            | Self::QueryTopicMessageType { .. }
            | Self::QuerySubscriptionGroup { .. }
            | Self::QueryAssignment { .. } => LocalExecutionClass::Control,
            Self::ProcessRemoting { request, .. } => match RequestCode::from(request.code()) {
                RequestCode::PopMessage | RequestCode::PullMessage => LocalExecutionClass::LongPoll,
                RequestCode::AckMessage
                | RequestCode::BatchAckMessage
                | RequestCode::ChangeMessageInvisibleTime
                | RequestCode::ConsumerSendMsgBack
                | RequestCode::UpdateConsumerOffset
                | RequestCode::QueryConsumerOffset => LocalExecutionClass::Control,
                _ => LocalExecutionClass::ShortData,
            },
            Self::SendMessage { .. } | Self::RecallMessage { .. } | Self::EndTransaction { .. } => {
                LocalExecutionClass::ShortData
            }
        }
    }

    fn ordering_key(&self) -> LocalOrderingKey {
        match self {
            Self::QueryRoute { topic, .. } | Self::QueryTopicMessageType { topic, .. } => {
                resource_key("topic", [topic])
            }
            Self::QuerySubscriptionGroup { group, .. } => resource_key("consumer-control", [group]),
            Self::QueryAssignment { topic, group, .. } => resource_key("consumer-control", [topic, group]),
            Self::SendMessage {
                client_id, request_id, ..
            }
            | Self::RecallMessage {
                client_id, request_id, ..
            }
            | Self::EndTransaction {
                client_id, request_id, ..
            } => LocalOrderingKey::new("producer", [client_id.clone().unwrap_or_else(|| request_id.clone())]),
            Self::ProcessRemoting { request, .. } => remoting_ordering_key(request),
        }
    }
}

fn resource_key<'a>(
    domain: &'static str,
    resources: impl IntoIterator<Item = &'a ResourceIdentity>,
) -> LocalOrderingKey {
    LocalOrderingKey::new(
        domain,
        resources
            .into_iter()
            .flat_map(|resource| [resource.namespace().to_owned(), resource.name().to_owned()]),
    )
}

fn remoting_ordering_key(request: &rocketmq_protocol::protocol::remoting_command::RemotingCommand) -> LocalOrderingKey {
    match RequestCode::from(request.code()) {
        RequestCode::PopMessage => request
            .decode_command_custom_header::<PopMessageRequestHeader>()
            .map(|header| {
                consumer_queue_key(
                    "consumer-poll",
                    header.consumer_group.as_str(),
                    header.topic.as_str(),
                    header.queue_id,
                )
            })
            .unwrap_or_else(|_| fallback_remoting_key("consumer-poll", request)),
        RequestCode::PullMessage => request
            .decode_command_custom_header::<PullMessageRequestHeader>()
            .map(|header| {
                consumer_queue_key(
                    "consumer-poll",
                    header.consumer_group.as_str(),
                    header.topic.as_str(),
                    header.queue_id,
                )
            })
            .unwrap_or_else(|_| fallback_remoting_key("consumer-poll", request)),
        RequestCode::AckMessage => request
            .decode_command_custom_header::<AckMessageRequestHeader>()
            .map(|header| {
                consumer_queue_key(
                    "consumer-control",
                    header.consumer_group.as_str(),
                    header.topic.as_str(),
                    header.queue_id,
                )
            })
            .unwrap_or_else(|_| fallback_remoting_key("consumer-control", request)),
        RequestCode::ChangeMessageInvisibleTime => request
            .decode_command_custom_header::<ChangeInvisibleTimeRequestHeader>()
            .map(|header| {
                consumer_queue_key(
                    "consumer-control",
                    header.consumer_group.as_str(),
                    header.topic.as_str(),
                    header.queue_id,
                )
            })
            .unwrap_or_else(|_| fallback_remoting_key("consumer-control", request)),
        RequestCode::UpdateConsumerOffset => request
            .decode_command_custom_header::<UpdateConsumerOffsetRequestHeader>()
            .map(|header| {
                consumer_queue_key(
                    "consumer-offset",
                    header.consumer_group.as_str(),
                    header.topic.as_str(),
                    header.queue_id,
                )
            })
            .unwrap_or_else(|_| fallback_remoting_key("consumer-offset", request)),
        RequestCode::QueryConsumerOffset => request
            .decode_command_custom_header::<QueryConsumerOffsetRequestHeader>()
            .map(|header| {
                consumer_queue_key(
                    "consumer-offset",
                    header.consumer_group.as_str(),
                    header.topic.as_str(),
                    header.queue_id,
                )
            })
            .unwrap_or_else(|_| fallback_remoting_key("consumer-offset", request)),
        RequestCode::BatchAckMessage | RequestCode::ConsumerSendMsgBack => {
            fallback_remoting_key("consumer-control", request)
        }
        _ => fallback_remoting_key("remoting", request),
    }
}

fn consumer_queue_key(domain: &'static str, group: &str, topic: &str, queue_id: i32) -> LocalOrderingKey {
    LocalOrderingKey::new(domain, [group.to_owned(), topic.to_owned(), queue_id.to_string()])
}

fn fallback_remoting_key(
    domain: &'static str,
    request: &rocketmq_protocol::protocol::remoting_command::RemotingCommand,
) -> LocalOrderingKey {
    LocalOrderingKey::new(domain, [request.code().to_string(), request.opaque().to_string()])
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    use cheetah_string::CheetahString;
    use rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use tokio::sync::oneshot;
    use tokio::sync::Semaphore;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::local::LocalBrokerCommand;
    use crate::local::QueuedLocalBrokerCommand;

    struct BlockingHandler {
        entered: mpsc::Sender<i32>,
        release: Arc<Semaphore>,
        current: AtomicUsize,
        maximum: AtomicUsize,
    }

    impl LocalCommandHandler for BlockingHandler {
        async fn handle(&self, command: LocalBrokerCommand) {
            let id = match &command {
                LocalBrokerCommand::ProcessRemoting { request, .. } => request.opaque(),
                LocalBrokerCommand::QueryRoute { .. } => -1,
                _ => -2,
            };
            let current = self.current.fetch_add(1, Ordering::AcqRel) + 1;
            self.maximum.fetch_max(current, Ordering::AcqRel);
            let _ = self.entered.send(id).await;
            if let Ok(permit) = self.release.acquire().await {
                permit.forget();
            }
            self.current.fetch_sub(1, Ordering::AcqRel);
            command.reject_unavailable();
        }
    }

    fn queued(command: LocalBrokerCommand) -> QueuedLocalBrokerCommand {
        let count = Arc::new(Semaphore::new(1));
        let bytes = Arc::new(Semaphore::new(1));
        QueuedLocalBrokerCommand {
            command,
            enqueued_at: Instant::now(),
            deadline_at: None,
            timeout_budget: None,
            _count_permit: count.try_acquire_owned().expect("count permit"),
            _byte_permit: bytes.try_acquire_owned().expect("byte permit"),
        }
    }

    fn pop_command(queue_id: i32, opaque: i32) -> LocalBrokerCommand {
        let (reply, _receiver) = oneshot::channel();
        let mut request = RemotingCommand::create_request_command(
            RequestCode::PopMessage,
            PopMessageRequestHeader {
                consumer_group: CheetahString::from("GroupA"),
                topic: CheetahString::from("TopicA"),
                queue_id,
                poll_time: 15_000,
                ..Default::default()
            },
        )
        .set_opaque(opaque);
        request.make_custom_header_to_net();
        LocalBrokerCommand::ProcessRemoting {
            request,
            timeout: Duration::from_millis(15_500),
            reply,
        }
    }

    fn route_command() -> LocalBrokerCommand {
        let (reply, _receiver) = oneshot::channel();
        LocalBrokerCommand::QueryRoute {
            topic: ResourceIdentity::new("", "TopicA"),
            reply,
        }
    }

    fn execution_contexts(name: &str) -> (ChildServiceContext, ChildServiceContext) {
        let runtime = rocketmq_runtime::RuntimeContext::try_from_current(name).expect("runtime context");
        let service = runtime.service_context(
            rocketmq_runtime::ScopeId::try_new(format!("{name}.service"))
                .expect("local execution test contexts use the fixed .service suffix"),
        );
        let lanes = service.component("lanes");
        (service, lanes)
    }

    #[test]
    fn long_poll_and_control_commands_use_independent_domains() {
        let pop = RemotingCommand::create_request_command(
            RequestCode::PopMessage,
            PopMessageRequestHeader {
                consumer_group: CheetahString::from("GroupA"),
                topic: CheetahString::from("TopicA"),
                queue_id: 1,
                poll_time: 15_000,
                ..Default::default()
            },
        );
        let (pop_reply, _pop_receiver) = oneshot::channel();
        let pop = LocalBrokerCommand::ProcessRemoting {
            request: pop,
            timeout: Duration::from_millis(15_500),
            reply: pop_reply,
        };
        let (route_reply, _route_receiver) = oneshot::channel();
        let route = LocalBrokerCommand::QueryRoute {
            topic: ResourceIdentity::new("", "TopicA"),
            reply: route_reply,
        };

        assert_eq!(pop.execution_class(), LocalExecutionClass::LongPoll);
        assert_eq!(route.execution_class(), LocalExecutionClass::Control);
        assert_ne!(pop.ordering_key(), route.ordering_key());
    }

    #[tokio::test]
    async fn saturated_long_poll_lane_does_not_block_control_work() {
        let config = LocalConfig {
            command_queue_capacity: 4,
            io_max_inflight: 2,
            control_reserve: 1,
            long_poll_max_inflight: 1,
            ..LocalConfig::default()
        };
        let policy = LocalExecutionPolicy::from_config(&config);
        let (sender, receiver) = mpsc::channel(4);
        sender.send(queued(pop_command(0, 1))).await.expect("first poll");
        sender.send(queued(pop_command(1, 2))).await.expect("second poll");
        sender.send(queued(route_command())).await.expect("control command");
        drop(sender);
        let (entered_tx, mut entered_rx) = mpsc::channel(4);
        let release = Arc::new(Semaphore::new(0));
        let handler = Arc::new(BlockingHandler {
            entered: entered_tx,
            release: release.clone(),
            current: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let (service, lanes) = execution_contexts("proxy-local-long-poll-isolation");
        let run = run_local_execution(
            policy,
            receiver,
            CancellationToken::new(),
            service.clone(),
            lanes,
            handler.clone(),
            Duration::from_secs(1),
        );
        let observe = async {
            let first = entered_rx.recv().await.expect("first active command");
            let second = entered_rx.recv().await.expect("control command");
            assert_ne!(first, second);
            assert!([first, second].contains(&1));
            assert!([first, second].contains(&-1));
            assert!(matches!(entered_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
            release.add_permits(2);
            assert_eq!(entered_rx.recv().await, Some(2));
            release.add_permits(1);
        };
        let ((), ()) = tokio::join!(run, observe);
        assert_eq!(handler.maximum.load(Ordering::Acquire), 2);
        let report = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn same_consumer_queue_remains_fifo() {
        let config = LocalConfig {
            command_queue_capacity: 3,
            long_poll_max_inflight: 2,
            ..LocalConfig::default()
        };
        let (sender, receiver) = mpsc::channel(3);
        sender.send(queued(pop_command(0, 1))).await.expect("first poll");
        sender.send(queued(pop_command(0, 2))).await.expect("second poll");
        drop(sender);
        let (entered_tx, mut entered_rx) = mpsc::channel(3);
        let release = Arc::new(Semaphore::new(0));
        let handler = Arc::new(BlockingHandler {
            entered: entered_tx,
            release: release.clone(),
            current: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let (service, lanes) = execution_contexts("proxy-local-queue-fifo");
        let run = run_local_execution(
            LocalExecutionPolicy::from_config(&config),
            receiver,
            CancellationToken::new(),
            service.clone(),
            lanes,
            handler.clone(),
            Duration::from_secs(1),
        );
        let observe = async {
            assert_eq!(entered_rx.recv().await, Some(1));
            tokio::task::yield_now().await;
            assert!(matches!(entered_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
            release.add_permits(1);
            assert_eq!(entered_rx.recv().await, Some(2));
            release.add_permits(1);
        };
        let ((), ()) = tokio::join!(run, observe);
        assert_eq!(handler.maximum.load(Ordering::Acquire), 1);
        let report = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }
}
