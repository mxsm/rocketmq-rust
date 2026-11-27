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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_client_rust::rpc_hook_from_outbound_signer;
use rocketmq_client_rust::ClientError;
use rocketmq_client_rust::ClientRpcHook;
use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::ClientRuntimeConfig;
use rocketmq_client_rust::TelemetryHandle;
use rocketmq_model::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_model::common::message::MessageConst;
use rocketmq_protocol::protocol::body::acl_info::AclInfo;
use rocketmq_protocol::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
use rocketmq_protocol::protocol::body::response::lock_batch_response_body::LockBatchResponseBody;
use rocketmq_protocol::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_protocol::protocol::body::user_info::UserInfo;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_proxy_core::error::canonical;
use rocketmq_proxy_core::AckMessageRequest;
use rocketmq_proxy_core::AckMessageResultEntry;
use rocketmq_proxy_core::ChangeInvisibleDurationPlan;
use rocketmq_proxy_core::ChangeInvisibleDurationRequest;
use rocketmq_proxy_core::EndTransactionPlan;
use rocketmq_proxy_core::EndTransactionRequest;
use rocketmq_proxy_core::ForwardMessageToDeadLetterQueuePlan;
use rocketmq_proxy_core::ForwardMessageToDeadLetterQueueRequest;
use rocketmq_proxy_core::GetOffsetPlan;
use rocketmq_proxy_core::GetOffsetRequest;
use rocketmq_proxy_core::LiteSubscriptionSyncRequest;
use rocketmq_proxy_core::ProxyError;
use rocketmq_proxy_core::ProxyResult;
use rocketmq_proxy_core::ProxyTopicMessageType;
use rocketmq_proxy_core::PullMessagePlan;
use rocketmq_proxy_core::PullMessageRequest;
use rocketmq_proxy_core::QueryOffsetPlan;
use rocketmq_proxy_core::QueryOffsetRequest;
use rocketmq_proxy_core::RecallMessagePlan;
use rocketmq_proxy_core::RecallMessageRequest;
use rocketmq_proxy_core::ReceiveMessagePlan;
use rocketmq_proxy_core::ReceiveMessageRequest;
use rocketmq_proxy_core::ResourceIdentity;
use rocketmq_proxy_core::SendMessageRequest;
use rocketmq_proxy_core::SendMessageResultEntry;
use rocketmq_proxy_core::SubscriptionGroupMetadata;
use rocketmq_proxy_core::UpdateOffsetPlan;
use rocketmq_proxy_core::UpdateOffsetRequest;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ResourcePermit;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_security_api::OutboundSigner;
use tokio::sync::oneshot;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::build_proxy_producer_group;
use super::cluster_admission::ClusterCommandClass;
use super::cluster_admission::ClusterExecutionLanes;
use super::cluster_admission::ClusterExecutionPolicy;
use super::cluster_admission::ClusterInflightPermit;
use super::cluster_admission::ClusterLaneRegistration;
use super::collect_send_topics;
use super::effective_send_timeout_ms;
use super::handle_cluster_command;
use super::prepare_send;
use super::ClusterClientFactory;
use super::ClusterProducerFactory;
use super::ClusterWorkerState;
use super::DefaultClusterClientFactory;
use super::DefaultClusterProducerFactory;
use super::PreparedSend;
use super::SendStep;
use super::SharedProducerHandles;
use crate::config::ClusterConfig;

#[derive(Clone)]
pub(super) struct ClusterTaskExecutor {
    pub(super) lanes: Arc<ClusterExecutionLanes>,
    runtime: Arc<ClusterExecutionRuntime>,
    cancellation: CancellationToken,
}

struct ClusterExecutionRuntime {
    config: ClusterConfig,
    worker_context: ChildServiceContext,
    shutdown_context: ChildServiceContext,
    client_runtime: Arc<ClientRuntime>,
    base_domain_id: u64,
    rpc_hook: Option<Arc<ClientRpcHook>>,
    client_factory: Arc<dyn ClusterClientFactory>,
    producer_factory: Arc<dyn ClusterProducerFactory>,
    send_fast_paths: Arc<SendFastPaths>,
}

/// Started producers whose unordered sends may skip their lane, by producer group.
pub(super) type SendFastPaths = RwLock<HashMap<String, Arc<SendFastPath>>>;

/// A producer that its lane has started and refreshed for some topics and send timeout.
///
/// A normal send of the same producer group for those topics with that timeout needs nothing
/// from the lane, so the requesting task sends with a fresh handle instead of queueing on the
/// lane and waiting to be woken with a prepared send. Every request of a connection then no
/// longer passes through one task. The lane closes its fast paths before it shuts their
/// producer down; a send counts in the lane's [`LaneSendTracker`] before it checks `open`, so
/// the lane either sees it running or the send sees the path closed.
pub(super) struct SendFastPath {
    handles: SharedProducerHandles,
    /// Sorted and deduplicated, as the producer keeps them.
    topics: Vec<CheetahString>,
    timeout: u64,
    sends: Arc<LaneSendTracker>,
    open: AtomicBool,
}

impl SendFastPath {
    fn covers(&self, topics: &[CheetahString], timeout: u64) -> bool {
        self.timeout == timeout && topics.iter().all(|topic| self.topics.binary_search(topic).is_ok())
    }
}

/// A send admitted to a fast path: it holds the lane's send guard until it finishes.
struct FastSendAdmission {
    path: Arc<SendFastPath>,
    timeout: u64,
    _lane: LaneSendGuard,
}

pub(super) enum ClusterCommand {
    ReadinessCheck {
        reply: oneshot::Sender<ProxyResult<()>>,
    },
    SyncLiteSubscription {
        client_id: String,
        request: LiteSubscriptionSyncRequest,
        reply: oneshot::Sender<ProxyResult<()>>,
    },
    QueryRoute {
        topic: ResourceIdentity,
        reply: oneshot::Sender<ProxyResult<TopicRouteData>>,
    },
    QueryAssignment {
        topic: ResourceIdentity,
        group: ResourceIdentity,
        client_id: String,
        reply: oneshot::Sender<ProxyResult<Option<Vec<MessageQueueAssignment>>>>,
    },
    QueryTopicMessageType {
        topic: ResourceIdentity,
        reply: oneshot::Sender<ProxyResult<ProxyTopicMessageType>>,
    },
    QuerySubscriptionGroup {
        topic: ResourceIdentity,
        group: ResourceIdentity,
        reply: oneshot::Sender<ProxyResult<Option<SubscriptionGroupMetadata>>>,
    },
    QueryUser {
        username: String,
        reply: oneshot::Sender<ProxyResult<Option<UserInfo>>>,
    },
    QueryAcl {
        subject: String,
        reply: oneshot::Sender<ProxyResult<Option<AclInfo>>>,
    },
    SendMessage {
        request: SendMessageRequest,
        client_id: Option<String>,
        request_id: String,
        reply: oneshot::Sender<ProxyResult<SendOutcome>>,
    },
    RecallMessage {
        request: RecallMessageRequest,
        client_id: Option<String>,
        request_id: String,
        reply: oneshot::Sender<ProxyResult<RecallMessagePlan>>,
    },
    ReceiveMessage {
        request: ReceiveMessageRequest,
        deadline: Option<Duration>,
        reply: oneshot::Sender<ProxyResult<ReceiveMessagePlan>>,
    },
    PullMessage {
        request: PullMessageRequest,
        deadline: Option<Duration>,
        reply: oneshot::Sender<ProxyResult<PullMessagePlan>>,
    },
    AckMessage {
        request: AckMessageRequest,
        deadline: Option<Duration>,
        reply: oneshot::Sender<ProxyResult<Vec<AckMessageResultEntry>>>,
    },
    ForwardMessageToDeadLetterQueue {
        request: ForwardMessageToDeadLetterQueueRequest,
        deadline: Option<Duration>,
        reply: oneshot::Sender<ProxyResult<ForwardMessageToDeadLetterQueuePlan>>,
    },
    ChangeInvisibleDuration {
        request: ChangeInvisibleDurationRequest,
        deadline: Option<Duration>,
        reply: oneshot::Sender<ProxyResult<ChangeInvisibleDurationPlan>>,
    },
    UpdateOffset {
        request: UpdateOffsetRequest,
        deadline: Option<Duration>,
        reply: oneshot::Sender<ProxyResult<UpdateOffsetPlan>>,
    },
    GetOffset {
        request: GetOffsetRequest,
        deadline: Option<Duration>,
        reply: oneshot::Sender<ProxyResult<GetOffsetPlan>>,
    },
    QueryOffset {
        request: QueryOffsetRequest,
        deadline: Option<Duration>,
        reply: oneshot::Sender<ProxyResult<QueryOffsetPlan>>,
    },
    EndTransaction {
        request: EndTransactionRequest,
        client_id: Option<String>,
        request_id: String,
        deadline: Option<Duration>,
        reply: oneshot::Sender<ProxyResult<EndTransactionPlan>>,
    },
    LockBatchMq {
        request: LockBatchRequestBody,
        reply: oneshot::Sender<ProxyResult<LockBatchResponseBody>>,
    },
    UnlockBatchMq {
        request: UnlockBatchRequestBody,
        reply: oneshot::Sender<ProxyResult<()>>,
    },
    ForwardRemoting {
        broker_name: String,
        request: RemotingCommand,
        timeout_millis: u64,
        reply: oneshot::Sender<ProxyResult<RemotingCommand>>,
    },
}

/// The reply a lane gives to a send command.
pub(super) enum SendOutcome {
    /// The lane sent the messages itself.
    Completed(Vec<SendMessageResultEntry>),
    /// The lane prepared the producer; the requesting task sends the messages.
    Dispatched(DispatchedSend),
}

/// A send that runs on the requesting task instead of its lane.
///
/// It keeps the command's in-flight and queue permits until the send finishes, so the
/// execution limits still count it, and keeps its lane from retiring the producer.
pub(super) struct DispatchedSend {
    send: PreparedSend,
    _inflight: ClusterInflightPermit,
    _active: ResourcePermit,
    _lane: LaneSendGuard,
}

impl DispatchedSend {
    async fn run(self) -> Vec<SendMessageResultEntry> {
        self.send.run().await
    }
}

/// A send command taken out of its lane's order.
struct DetachableSend {
    request: SendMessageRequest,
    client_id: Option<String>,
    request_id: String,
    reply: oneshot::Sender<ProxyResult<SendOutcome>>,
}

/// Returns whether a send must wait for its lane's order.
///
/// Sends of one client share a lane because they share a producer group. Normal
/// messages have no order among themselves, so their sends may overlap, as they do on a
/// Java Proxy. Sends with a message group (FIFO) or a transaction half message keep the
/// lane order, and so does every other command.
fn send_keeps_lane_order(request: &SendMessageRequest) -> bool {
    request.messages.is_empty()
        || request.messages.iter().any(|entry| {
            let properties = entry.message.properties();
            properties.contains_key(MessageConst::PROPERTY_SHARDING_KEY)
                || properties.contains_key(MessageConst::PROPERTY_TRANSACTION_PREPARED)
        })
}

/// Counts the sends a lane has dispatched to requesting tasks.
#[derive(Default)]
struct LaneSendTracker {
    active: AtomicUsize,
    /// Fast-path sends ever entered: they never reach the lane's queue, so this tells an idle
    /// lane that its producers are still in use.
    fast_entered: AtomicU64,
    idle: Notify,
}

impl LaneSendTracker {
    fn enter(self: &Arc<Self>) -> LaneSendGuard {
        // SeqCst pairs with closing a fast path: see `SendFastPath`.
        self.active.fetch_add(1, Ordering::SeqCst);
        LaneSendGuard(self.clone())
    }

    fn enter_fast(self: &Arc<Self>) -> LaneSendGuard {
        self.fast_entered.fetch_add(1, Ordering::Relaxed);
        self.enter()
    }

    fn fast_entered(&self) -> u64 {
        self.fast_entered.load(Ordering::Relaxed)
    }

    fn is_idle(&self) -> bool {
        self.active.load(Ordering::SeqCst) == 0
    }

    async fn wait_idle(&self) {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_idle() {
                return;
            }
            notified.await;
        }
    }
}

struct LaneSendGuard(Arc<LaneSendTracker>);

impl Drop for LaneSendGuard {
    fn drop(&mut self) {
        if self.0.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

impl ClusterTaskExecutor {
    pub(super) fn new(
        config: ClusterConfig,
        signer: Option<Arc<dyn OutboundSigner>>,
        service_context: &ChildServiceContext,
        telemetry_handle: TelemetryHandle,
    ) -> ProxyResult<Self> {
        let rpc_hook = signer.map(rpc_hook_from_outbound_signer);
        Self::new_with_rpc_hook(config, rpc_hook, service_context, telemetry_handle)
    }

    fn new_with_rpc_hook(
        config: ClusterConfig,
        rpc_hook: Option<Arc<ClientRpcHook>>,
        service_context: &ChildServiceContext,
        telemetry_handle: TelemetryHandle,
    ) -> ProxyResult<Self> {
        config.validate_tls()?;
        let worker_context = service_context.component("command-worker");
        let execution_telemetry = telemetry_handle.clone();
        let client_runtime = ClientRuntime::try_new(
            worker_context.component("client-runtime"),
            ClientRuntimeConfig::default(),
            telemetry_handle,
        )
        .map_err(|error| ProxyError::from(ClientError::into_error(error)))?;
        let base_domain_id = worker_context.task_group().id().as_u64();
        let policy = ClusterExecutionPolicy::from_config(&config);
        Self::spawn_execution(
            config,
            rpc_hook,
            service_context,
            worker_context,
            client_runtime,
            base_domain_id,
            Arc::new(DefaultClusterClientFactory),
            Arc::new(DefaultClusterProducerFactory),
            policy,
            execution_telemetry,
        )
        .map(|(executor, _)| executor)
    }

    #[cfg(test)]
    pub(super) fn new_with_test_state(
        config: ClusterConfig,
        state: ClusterWorkerState,
        service_context: &ChildServiceContext,
        policy: ClusterExecutionPolicy,
    ) -> ProxyResult<(Self, tokio_util::sync::CancellationToken)> {
        let ClusterWorkerState {
            client_runtime,
            domain_id,
            rpc_hook,
            client_factory,
            producer_factory,
            ..
        } = state;
        let worker_context = service_context.component("command-worker");
        Self::spawn_execution(
            config,
            rpc_hook,
            service_context,
            worker_context,
            client_runtime,
            domain_id,
            client_factory,
            producer_factory,
            policy,
            rocketmq_observability::TelemetryHandle::noop(),
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "execution ownership is assembled at one private boundary"
    )]
    fn spawn_execution(
        config: ClusterConfig,
        rpc_hook: Option<Arc<ClientRpcHook>>,
        service_context: &ChildServiceContext,
        worker_context: ChildServiceContext,
        client_runtime: Arc<ClientRuntime>,
        base_domain_id: u64,
        client_factory: Arc<dyn ClusterClientFactory>,
        producer_factory: Arc<dyn ClusterProducerFactory>,
        policy: ClusterExecutionPolicy,
        telemetry: rocketmq_observability::TelemetryHandle,
    ) -> ProxyResult<(Self, tokio_util::sync::CancellationToken)> {
        let lanes = Arc::new(ClusterExecutionLanes::new(policy)?);
        let source = Arc::clone(&lanes);
        let long_poll_source = Arc::clone(&lanes);
        let lane_capacity_items = config.command_queue_capacity as u64;
        let lane_capacity_bytes = config.command_queue_max_bytes as u64;
        let aggregate_capacity_items = lane_capacity_items.saturating_mul(2);
        let aggregate_capacity_bytes = lane_capacity_bytes.saturating_mul(2);
        let resource_metrics = rocketmq_observability::metrics::resource::ResourceStabilityMetrics::from_handle(
            &telemetry,
            rocketmq_observability::PROXY_METER_SCOPE,
        );
        resource_metrics.register_queue("proxy-cluster", "commands", "aggregate", move || {
            let snapshot = source.snapshot();
            rocketmq_observability::metrics::resource::ResourceQueueSnapshot {
                items: snapshot.queued_and_active as u64,
                bytes: snapshot.retained_bytes as u64,
                oldest_age_millis: snapshot.oldest_queued_age_ms.unwrap_or_default(),
                capacity_items: aggregate_capacity_items,
                capacity_bytes: aggregate_capacity_bytes,
                active: snapshot.current_inflight as u64,
                rejected_total: snapshot.rejected,
            }
        });
        resource_metrics.register_queue("proxy-cluster", "commands", "long-poll", move || {
            let snapshot = long_poll_source.snapshot();
            rocketmq_observability::metrics::resource::ResourceQueueSnapshot {
                items: snapshot.long_poll_queued_and_active as u64,
                bytes: snapshot.long_poll_retained_bytes as u64,
                oldest_age_millis: snapshot.oldest_queued_age_ms.unwrap_or_default(),
                capacity_items: lane_capacity_items,
                capacity_bytes: lane_capacity_bytes,
                active: snapshot.current_long_poll_inflight as u64,
                rejected_total: snapshot.rejected,
            }
        });
        let cancellation = worker_context.task_group().cancellation_token();
        let runtime = Arc::new(ClusterExecutionRuntime {
            config: config.clone(),
            worker_context: worker_context.clone(),
            shutdown_context: service_context.clone(),
            client_runtime: client_runtime.clone(),
            base_domain_id,
            rpc_hook,
            client_factory,
            producer_factory,
            send_fast_paths: Arc::default(),
        });
        let owner_runtime = client_runtime;
        let owner_lanes = lanes.clone();
        let owner_cancellation = cancellation.clone();
        let owner_shutdown_context = service_context.clone();
        if let Err(error) = worker_context.spawn_service("proxy.cluster.execution-owner", async move {
            run_cluster_execution_owner(owner_runtime, owner_lanes, owner_cancellation, owner_shutdown_context).await;
        }) {
            worker_context.task_group().cancel();
            lanes.close();
            return Err(ProxyError::from(canonical::transport_unavailable_with_source(error)));
        }
        Ok((
            Self {
                lanes,
                runtime,
                cancellation: cancellation.clone(),
            },
            cancellation,
        ))
    }

    pub(super) async fn readiness_check(&self) -> ProxyResult<()> {
        self.execute(|reply| ClusterCommand::ReadinessCheck { reply }).await
    }

    pub(super) async fn sync_lite_subscription(
        &self,
        client_id: String,
        request: LiteSubscriptionSyncRequest,
    ) -> ProxyResult<()> {
        self.execute(|reply| ClusterCommand::SyncLiteSubscription {
            client_id,
            request,
            reply,
        })
        .await
    }

    pub(super) async fn query_route(&self, topic: ResourceIdentity) -> ProxyResult<TopicRouteData> {
        self.execute(|reply| ClusterCommand::QueryRoute { topic, reply }).await
    }

    pub(super) async fn query_assignment(
        &self,
        topic: ResourceIdentity,
        group: ResourceIdentity,
        client_id: String,
    ) -> ProxyResult<Option<Vec<MessageQueueAssignment>>> {
        self.execute(|reply| ClusterCommand::QueryAssignment {
            topic,
            group,
            client_id,
            reply,
        })
        .await
    }

    pub(super) async fn query_topic_message_type(&self, topic: ResourceIdentity) -> ProxyResult<ProxyTopicMessageType> {
        self.execute(|reply| ClusterCommand::QueryTopicMessageType { topic, reply })
            .await
    }

    pub(super) async fn query_subscription_group(
        &self,
        topic: ResourceIdentity,
        group: ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>> {
        self.execute(|reply| ClusterCommand::QuerySubscriptionGroup { topic, group, reply })
            .await
    }

    pub(super) async fn query_user(&self, username: String) -> ProxyResult<Option<UserInfo>> {
        self.execute(|reply| ClusterCommand::QueryUser { username, reply })
            .await
    }

    pub(super) async fn query_acl(&self, subject: String) -> ProxyResult<Option<AclInfo>> {
        self.execute(|reply| ClusterCommand::QueryAcl { subject, reply }).await
    }

    pub(super) async fn send_message(
        &self,
        request: SendMessageRequest,
        client_id: Option<String>,
        request_id: String,
    ) -> ProxyResult<Vec<SendMessageResultEntry>> {
        if let Some(admission) = self.admit_fast_send(&request, client_id.as_deref(), request_id.as_str()) {
            return self.run_fast_send(admission, request).await;
        }
        self.execute_then(
            |reply| ClusterCommand::SendMessage {
                request,
                client_id,
                request_id,
                reply,
            },
            |outcome| async move {
                Ok(match outcome {
                    SendOutcome::Completed(entries) => entries,
                    SendOutcome::Dispatched(send) => send.run().await,
                })
            },
        )
        .await
    }

    pub(super) async fn recall_message(
        &self,
        request: RecallMessageRequest,
        client_id: Option<String>,
        request_id: String,
    ) -> ProxyResult<RecallMessagePlan> {
        self.execute(|reply| ClusterCommand::RecallMessage {
            request,
            client_id,
            request_id,
            reply,
        })
        .await
    }

    pub(super) async fn receive_message(
        &self,
        request: ReceiveMessageRequest,
        deadline: Option<Duration>,
    ) -> ProxyResult<ReceiveMessagePlan> {
        self.execute(|reply| ClusterCommand::ReceiveMessage {
            request,
            deadline,
            reply,
        })
        .await
    }

    pub(super) async fn pull_message(
        &self,
        request: PullMessageRequest,
        deadline: Option<Duration>,
    ) -> ProxyResult<PullMessagePlan> {
        self.execute(|reply| ClusterCommand::PullMessage {
            request,
            deadline,
            reply,
        })
        .await
    }

    pub(super) async fn ack_message(
        &self,
        request: AckMessageRequest,
        deadline: Option<Duration>,
    ) -> ProxyResult<Vec<AckMessageResultEntry>> {
        self.execute(|reply| ClusterCommand::AckMessage {
            request,
            deadline,
            reply,
        })
        .await
    }

    pub(super) async fn forward_message_to_dead_letter_queue(
        &self,
        request: ForwardMessageToDeadLetterQueueRequest,
        deadline: Option<Duration>,
    ) -> ProxyResult<ForwardMessageToDeadLetterQueuePlan> {
        self.execute(|reply| ClusterCommand::ForwardMessageToDeadLetterQueue {
            request,
            deadline,
            reply,
        })
        .await
    }

    pub(super) async fn change_invisible_duration(
        &self,
        request: ChangeInvisibleDurationRequest,
        deadline: Option<Duration>,
    ) -> ProxyResult<ChangeInvisibleDurationPlan> {
        self.execute(|reply| ClusterCommand::ChangeInvisibleDuration {
            request,
            deadline,
            reply,
        })
        .await
    }

    pub(super) async fn update_offset(
        &self,
        request: UpdateOffsetRequest,
        deadline: Option<Duration>,
    ) -> ProxyResult<UpdateOffsetPlan> {
        self.execute(|reply| ClusterCommand::UpdateOffset {
            request,
            deadline,
            reply,
        })
        .await
    }

    pub(super) async fn get_offset(
        &self,
        request: GetOffsetRequest,
        deadline: Option<Duration>,
    ) -> ProxyResult<GetOffsetPlan> {
        self.execute(|reply| ClusterCommand::GetOffset {
            request,
            deadline,
            reply,
        })
        .await
    }

    pub(super) async fn query_offset(
        &self,
        request: QueryOffsetRequest,
        deadline: Option<Duration>,
    ) -> ProxyResult<QueryOffsetPlan> {
        self.execute(|reply| ClusterCommand::QueryOffset {
            request,
            deadline,
            reply,
        })
        .await
    }

    pub(super) async fn end_transaction(
        &self,
        request: EndTransactionRequest,
        client_id: Option<String>,
        request_id: String,
        deadline: Option<Duration>,
    ) -> ProxyResult<EndTransactionPlan> {
        self.execute(|reply| ClusterCommand::EndTransaction {
            request,
            client_id,
            request_id,
            deadline,
            reply,
        })
        .await
    }

    pub(super) async fn lock_batch_mq(&self, request: LockBatchRequestBody) -> ProxyResult<LockBatchResponseBody> {
        self.execute(|reply| ClusterCommand::LockBatchMq { request, reply })
            .await
    }

    pub(super) async fn unlock_batch_mq(&self, request: UnlockBatchRequestBody) -> ProxyResult<()> {
        self.execute(|reply| ClusterCommand::UnlockBatchMq { request, reply })
            .await
    }

    pub(super) async fn forward_remoting(
        &self,
        broker_name: String,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> ProxyResult<RemotingCommand> {
        self.execute(|reply| ClusterCommand::ForwardRemoting {
            broker_name,
            request,
            timeout_millis,
            reply,
        })
        .await
    }

    async fn execute<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<ProxyResult<T>>) -> ClusterCommand,
    ) -> ProxyResult<T>
    where
        T: Send + 'static,
    {
        self.execute_then(command, |value| async move { Ok(value) }).await
    }

    /// Runs `command` in its lane, then `then` on the lane's reply, both within the
    /// command's deadline and the executor's lifetime.
    async fn execute_then<T, U, Then, Next>(
        &self,
        command: impl FnOnce(oneshot::Sender<ProxyResult<T>>) -> ClusterCommand,
        then: Then,
    ) -> ProxyResult<U>
    where
        T: Send + 'static,
        Then: FnOnce(T) -> Next,
        Next: std::future::Future<Output = ProxyResult<U>>,
    {
        let (reply, receiver) = oneshot::channel();
        let command = command(reply);
        let request_deadline = command.queue_deadline();
        let command_cancellation = CancellationToken::new();
        let _drop_guard = CommandCancellationGuard(command_cancellation.clone());
        if let Some(registration) = self
            .lanes
            .enqueue(command, command_cancellation.clone(), &self.runtime.config)?
        {
            self.spawn_lane(registration)?;
        }

        let receive = async {
            let value = receiver
                .await
                .map_err(|error| ProxyError::from(canonical::transport_unavailable_with_source(error)))??;
            then(value).await
        };
        tokio::pin!(receive);
        match request_deadline {
            Some(deadline) => {
                tokio::select! {
                    biased;
                    result = &mut receive => result,
                    () = self.cancellation.cancelled() => {
                        self.lanes.record_shutdown_rejected();
                        Err(cluster_shutdown_error())
                    },
                    () = tokio::time::sleep(deadline.max(Duration::from_millis(1))) => {
                        self.lanes.record_timeout();
                        Err(cluster_command_timeout(deadline))
                    },
                }
            }
            None => {
                tokio::select! {
                    biased;
                    result = &mut receive => result,
                    () = self.cancellation.cancelled() => {
                        self.lanes.record_shutdown_rejected();
                        Err(cluster_shutdown_error())
                    },
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) fn send_fast_path_count(&self) -> usize {
        self.runtime
            .send_fast_paths
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Admits a normal send to its producer group's fast path, when the lane has one open for
    /// the send's topics and timeout.
    fn admit_fast_send(
        &self,
        request: &SendMessageRequest,
        client_id: Option<&str>,
        request_id: &str,
    ) -> Option<FastSendAdmission> {
        if send_keeps_lane_order(request) {
            return None;
        }
        let config = &self.runtime.config;
        let producer_group = build_proxy_producer_group(config, client_id, request_id);
        let path = self
            .runtime
            .send_fast_paths
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&producer_group)?
            .clone();
        let timeout = effective_send_timeout_ms(config, request.timeout);
        if !path.covers(&collect_send_topics(request), timeout) {
            return None;
        }
        let lane = path.sends.enter_fast();
        if !path.open.load(Ordering::SeqCst) {
            return None;
        }
        Some(FastSendAdmission {
            path,
            timeout,
            _lane: lane,
        })
    }

    /// Sends outside the lane, under the same in-flight limit as a lane command.
    async fn run_fast_send(
        &self,
        admission: FastSendAdmission,
        request: SendMessageRequest,
    ) -> ProxyResult<Vec<SendMessageResultEntry>> {
        let inflight = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => None,
            permit = self.lanes.acquire_inflight(ClusterCommandClass::ShortData) => permit,
        };
        let Some(_inflight) = inflight else {
            self.lanes.record_shutdown_rejected();
            return Err(cluster_shutdown_error());
        };
        let send = PreparedSend {
            producer: (admission.path.handles)(),
            entries: request.messages,
            timeout: admission.timeout,
        };
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => {
                self.lanes.record_shutdown_rejected();
                Err(cluster_shutdown_error())
            },
            entries = send.run() => Ok(entries),
        }
    }

    fn spawn_lane(&self, registration: ClusterLaneRegistration) -> ProxyResult<()> {
        let lanes = self.lanes.clone();
        let runtime = self.runtime.clone();
        let cancellation = self.cancellation.clone();
        let task_registration = registration.clone();
        lanes.lane_task_started();
        let task_lanes = lanes.clone();
        let worker_context = runtime.worker_context.clone();
        let lane_task = LaneTaskGuard {
            lanes: task_lanes.clone(),
            registration: task_registration.clone(),
            cancellation: cancellation.clone(),
            completed: false,
        };
        let spawn_result = worker_context.spawn_service("proxy.cluster.keyed-lane", async move {
            let mut lane_task = lane_task;
            let state = ClusterWorkerState::with_factories(
                runtime.client_runtime.clone(),
                runtime.base_domain_id,
                runtime.rpc_hook.clone(),
                runtime.client_factory.clone(),
                runtime.producer_factory.clone(),
            );
            run_cluster_lane(
                runtime.config.clone(),
                state,
                task_registration,
                cancellation,
                runtime.shutdown_context.clone(),
                task_lanes.clone(),
                runtime.send_fast_paths.clone(),
            )
            .await;
            lane_task.completed = true;
        });
        if let Err(error) = spawn_result {
            return Err(ProxyError::from(canonical::transport_unavailable_with_source(error)));
        }
        Ok(())
    }
}

struct LaneTaskGuard {
    lanes: Arc<ClusterExecutionLanes>,
    registration: ClusterLaneRegistration,
    cancellation: CancellationToken,
    completed: bool,
}

impl Drop for LaneTaskGuard {
    fn drop(&mut self) {
        if !self.completed {
            // Publish the failure before TaskGroup observes the panicked join.
            // Otherwise a concurrent request can create a replacement lane in
            // the gap between unwind and parent cancellation.
            self.lanes.close();
            self.cancellation.cancel();
            self.lanes.reject_failed_lane(
                &self.registration,
                "proxy cluster keyed lane terminated before completing the command",
            );
        }
        self.lanes.lane_task_finished();
    }
}

struct CommandCancellationGuard(CancellationToken);

impl Drop for CommandCancellationGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub(super) async fn run_cluster_lane(
    config: ClusterConfig,
    mut state: ClusterWorkerState,
    registration: ClusterLaneRegistration,
    cancellation: CancellationToken,
    shutdown_context: ChildServiceContext,
    lanes: Arc<ClusterExecutionLanes>,
    fast_paths: Arc<SendFastPaths>,
) {
    let queue = &registration.queue;
    let sends = Arc::new(LaneSendTracker::default());
    let mut fast_entered_at_last_idle = 0;
    loop {
        let idle = tokio::time::sleep(lanes.idle_timeout());
        tokio::pin!(idle);
        let queued = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                queue.close();
                break;
            },
            command = queue.recv_budgeted() => match command {
                Some(command) => command,
                None => break,
            },
            () = &mut idle => {
                let fast_entered = sends.fast_entered();
                if !sends.is_idle() || fast_entered != fast_entered_at_last_idle {
                    // Dispatched or fast-path sends use this lane's producers.
                    fast_entered_at_last_idle = fast_entered;
                    continue;
                }
                withdraw_send_fast_paths(&fast_paths, &state, &sends);
                shutdown_cluster_state(&config, &mut state, &shutdown_context, &sends).await;
                if lanes.retire(&registration) {
                    return;
                }
                continue;
            },
        };
        let (queued, active_permit, _) = queued.into_parts();
        let waited = queued.enqueued_at.elapsed();
        if waited >= queued.queue_deadline {
            lanes.record_timeout();
            queued.command.reject(cluster_queue_timeout(queued.queue_deadline));
            drop(active_permit);
            continue;
        }
        if queued.cancellation.is_cancelled() {
            lanes.record_cancelled();
            drop(active_permit);
            continue;
        }

        let remaining_queue_time = queued.queue_deadline.saturating_sub(waited);
        let acquire_inflight = lanes.acquire_inflight(queued.class);
        tokio::pin!(acquire_inflight);
        let inflight_permit = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                lanes.record_shutdown_rejected();
                queued.command.reject(cluster_shutdown_error());
                drop(active_permit);
                break;
            },
            () = queued.cancellation.cancelled() => {
                lanes.record_cancelled();
                drop(active_permit);
                continue;
            },
            () = tokio::time::sleep(remaining_queue_time) => {
                lanes.record_timeout();
                queued.command.reject(cluster_queue_timeout(queued.queue_deadline));
                drop(active_permit);
                continue;
            },
            permit = &mut acquire_inflight => permit,
        };
        let Some(inflight_permit) = inflight_permit else {
            lanes.record_shutdown_rejected();
            queued.command.reject(cluster_shutdown_error());
            drop(active_permit);
            break;
        };

        let mut command = queued.command;
        command.apply_queue_wait(queued.enqueued_at.elapsed());
        let command = match command {
            ClusterCommand::SendMessage {
                request,
                client_id,
                request_id,
                reply,
            } if !send_keeps_lane_order(&request) => {
                let send = DetachableSend {
                    request,
                    client_id,
                    request_id,
                    reply,
                };
                let permits = (inflight_permit, active_permit, sends.enter());
                let shutdown_during_dispatch = tokio::select! {
                    biased;
                    () = cancellation.cancelled() => {
                        lanes.record_shutdown_rejected();
                        true
                    },
                    () = queued.cancellation.cancelled() => {
                        lanes.record_cancelled();
                        false
                    },
                    () = dispatch_send(&config, &mut state, send, permits, &fast_paths, &sends) => false,
                };
                if shutdown_during_dispatch {
                    break;
                }
                continue;
            }
            command => command,
        };
        let shutdown_during_command = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                lanes.record_shutdown_rejected();
                true
            },
            () = queued.cancellation.cancelled() => {
                lanes.record_cancelled();
                false
            },
            () = handle_cluster_command(&config, &mut state, command) => false,
        };
        drop(inflight_permit);
        drop(active_permit);
        if shutdown_during_command {
            break;
        }
    }
    while let Some(queued) = queue.try_pop() {
        lanes.record_shutdown_rejected();
        queued.command.reject(ProxyError::Transport {
            message: "proxy cluster command execution stopped during shutdown".to_owned(),
        });
    }
    withdraw_send_fast_paths(&fast_paths, &state, &sends);
    shutdown_cluster_state(&config, &mut state, &shutdown_context, &sends).await;
    lanes.retire(&registration);
}

/// Prepares the producer of a send in its lane and hands the send to the requesting task,
/// or replies with the results when the lane sent the messages itself.
async fn dispatch_send(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    send: DetachableSend,
    (inflight, active, lane): (ClusterInflightPermit, ResourcePermit, LaneSendGuard),
    fast_paths: &SendFastPaths,
    sends: &Arc<LaneSendTracker>,
) {
    let DetachableSend {
        request,
        client_id,
        request_id,
        reply,
    } = send;
    let producer_group = build_proxy_producer_group(config, client_id.as_deref(), request_id.as_str());
    let timeout = effective_send_timeout_ms(config, request.timeout);
    let outcome = match prepare_send(config, state, request, client_id, request_id).await {
        SendStep::Finished(entries) => SendOutcome::Completed(entries),
        SendStep::Ready(send) => {
            publish_send_fast_path(fast_paths, state, producer_group, timeout, sends);
            SendOutcome::Dispatched(DispatchedSend {
                send,
                _inflight: inflight,
                _active: active,
                _lane: lane,
            })
        }
    };
    let _ = reply.send(Ok(outcome));
}

/// Opens or refreshes the fast path of a producer the lane just prepared, unless an open path
/// of this lane already covers its topics with this timeout.
fn publish_send_fast_path(
    fast_paths: &SendFastPaths,
    state: &ClusterWorkerState,
    producer_group: String,
    timeout: u64,
    sends: &Arc<LaneSendTracker>,
) {
    let Some(producer) = state.send_producers.get(producer_group.as_str()) else {
        return;
    };
    let mut topics = producer.topics();
    topics.sort();
    topics.dedup();
    {
        let paths = fast_paths.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(path) = paths.get(producer_group.as_str()) {
            if Arc::ptr_eq(&path.sends, sends)
                && path.open.load(Ordering::SeqCst)
                && path.timeout == timeout
                && path.topics == topics
            {
                return;
            }
        }
    }
    let Some(handles) = producer.shared_handle_factory() else {
        return;
    };
    let path = Arc::new(SendFastPath {
        handles,
        topics,
        timeout,
        sends: sends.clone(),
        open: AtomicBool::new(true),
    });
    fast_paths
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(producer_group, path);
}

/// Closes and removes this lane's fast paths before it shuts their producers down.
fn withdraw_send_fast_paths(fast_paths: &SendFastPaths, state: &ClusterWorkerState, sends: &Arc<LaneSendTracker>) {
    let mut paths = fast_paths.write().unwrap_or_else(std::sync::PoisonError::into_inner);
    for producer_group in state.send_producers.keys() {
        if paths
            .get(producer_group.as_str())
            .is_some_and(|path| Arc::ptr_eq(&path.sends, sends))
        {
            if let Some(path) = paths.remove(producer_group.as_str()) {
                path.open.store(false, Ordering::SeqCst);
            }
        }
    }
}

async fn shutdown_cluster_state(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    shutdown_context: &ChildServiceContext,
    sends: &LaneSendTracker,
) {
    let shutdown_deadline = cluster_shutdown_deadline(shutdown_context, config.shutdown_timeout());
    // Dispatched sends use clones of this lane's producers: let them finish first.
    if tokio::time::timeout(shutdown_deadline.remaining(), sends.wait_idle())
        .await
        .is_err()
    {
        tracing::warn!("proxy cluster dispatched sends exceeded the shared shutdown deadline");
    }
    for (producer_group, mut producer) in state.send_producers.drain() {
        if tokio::time::timeout(shutdown_deadline.remaining(), producer.shutdown())
            .await
            .is_err()
        {
            tracing::warn!(
                producer_group,
                "proxy cluster producer shutdown exceeded the shared deadline"
            );
        }
    }
    if let Some(client) = state.client.take() {
        if tokio::time::timeout(shutdown_deadline.remaining(), client.shutdown())
            .await
            .is_err()
        {
            tracing::warn!("proxy cluster Client shutdown exceeded the shared deadline");
        }
    }
}

async fn run_cluster_execution_owner(
    client_runtime: Arc<ClientRuntime>,
    lanes: Arc<ClusterExecutionLanes>,
    cancellation: CancellationToken,
    shutdown_context: ChildServiceContext,
) {
    cancellation.cancelled().await;
    lanes.close();
    let shutdown_deadline = shutdown_context
        .task_group()
        .shutdown_deadline()
        .unwrap_or_else(|| ShutdownDeadline::after(Duration::from_secs(5)));
    if !lanes.wait_for_lane_tasks(shutdown_deadline).await {
        let snapshot = lanes.snapshot();
        tracing::warn!(
            active_lane_tasks = snapshot.active_lane_tasks,
            active_keys = snapshot.active_keys,
            current_inflight = snapshot.current_inflight,
            "proxy cluster keyed lanes exceeded the shared shutdown deadline"
        );
    }
    let _ = client_runtime.shutdown_until(shutdown_deadline).await;
}

fn cluster_queue_timeout(deadline: Duration) -> ProxyError {
    canonical::timed_out(
        "proxy cluster command queue",
        deadline.as_millis().clamp(1, u128::from(u64::MAX)) as u64,
    )
    .into()
}

fn cluster_command_timeout(deadline: Duration) -> ProxyError {
    canonical::timed_out(
        "proxy cluster command",
        deadline.as_millis().clamp(1, u128::from(u64::MAX)) as u64,
    )
    .into()
}

fn cluster_shutdown_error() -> ProxyError {
    ProxyError::Transport {
        message: "proxy cluster command execution stopped during shutdown".to_owned(),
    }
}

fn cluster_shutdown_deadline(context: &ChildServiceContext, configured_timeout: Duration) -> ShutdownDeadline {
    let configured = ShutdownDeadline::after(configured_timeout);
    match context.task_group().shutdown_deadline() {
        Some(parent) if parent.instant() <= configured.instant() => parent,
        Some(_) | None => configured,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropping_an_unpolled_lane_future_releases_registration_and_task_count() {
        let lanes =
            Arc::new(ClusterExecutionLanes::new(ClusterExecutionPolicy::default()).expect("valid execution policy"));
        let config = ClusterConfig::default();
        let (reply, mut receiver) = oneshot::channel();
        let registration = lanes
            .enqueue(
                ClusterCommand::ReadinessCheck { reply },
                CancellationToken::new(),
                &config,
            )
            .expect("command admission")
            .expect("first key creates a lane");

        lanes.lane_task_started();
        let cancellation = CancellationToken::new();
        let guard = LaneTaskGuard {
            lanes: lanes.clone(),
            registration,
            cancellation: cancellation.clone(),
            completed: false,
        };
        let unpolled = async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        };
        drop(unpolled);

        let diagnostics = lanes.snapshot();
        assert_eq!(diagnostics.active_lane_tasks, 0);
        assert_eq!(diagnostics.active_keys, 0);
        assert_eq!(diagnostics.queued_and_active, 0);
        assert!(cancellation.is_cancelled());
        assert!(receiver.try_recv().expect("queued command is rejected").is_err());
    }
}
