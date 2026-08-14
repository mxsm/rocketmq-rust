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
use std::time::Duration;

use rocketmq_client_rust::rpc_hook_from_outbound_signer;
use rocketmq_client_rust::ClientRpcHook;
use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::ClientRuntimeConfig;
use rocketmq_client_rust::TelemetryHandle;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_protocol::protocol::body::acl_info::AclInfo;
use rocketmq_protocol::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
use rocketmq_protocol::protocol::body::response::lock_batch_response_body::LockBatchResponseBody;
use rocketmq_protocol::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_protocol::protocol::body::user_info::UserInfo;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
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
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_security_api::OutboundSigner;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::cluster_admission::ClusterExecutionLanes;
use super::cluster_admission::ClusterExecutionPolicy;
use super::cluster_admission::ClusterLaneRegistration;
use super::handle_cluster_command;
use super::ClusterClientFactory;
use super::ClusterProducerFactory;
use super::ClusterWorkerState;
use super::DefaultClusterClientFactory;
use super::DefaultClusterProducerFactory;
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
        reply: oneshot::Sender<ProxyResult<Vec<SendMessageResultEntry>>>,
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
        let worker_context = service_context.component("command-worker");
        let execution_telemetry = telemetry_handle.clone();
        let client_runtime = ClientRuntime::try_new(
            worker_context.component("client-runtime"),
            ClientRuntimeConfig::default(),
            telemetry_handle,
        )?;
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
            return Err(ProxyError::Transport {
                message: format!("failed to spawn proxy cluster execution owner: {error}"),
            });
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
        self.execute(|reply| ClusterCommand::SendMessage {
            request,
            client_id,
            request_id,
            reply,
        })
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
            receiver.await.map_err(|_| ProxyError::Transport {
                message: "proxy cluster keyed executor dropped response".to_owned(),
            })?
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
            )
            .await;
            lane_task.completed = true;
        });
        if let Err(error) = spawn_result {
            let message = format!("failed to spawn proxy cluster keyed lane: {error}");
            return Err(ProxyError::Transport { message });
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
) {
    let queue = &registration.queue;
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
                shutdown_cluster_state(&config, &mut state, &shutdown_context).await;
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
    shutdown_cluster_state(&config, &mut state, &shutdown_context).await;
    lanes.retire(&registration);
}

async fn shutdown_cluster_state(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    shutdown_context: &ChildServiceContext,
) {
    let shutdown_deadline = cluster_shutdown_deadline(shutdown_context, config.shutdown_timeout());
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
    RocketMQError::Timeout {
        operation: "proxy cluster command queue",
        timeout_ms: deadline.as_millis().clamp(1, u128::from(u64::MAX)) as u64,
    }
    .into()
}

fn cluster_command_timeout(deadline: Duration) -> ProxyError {
    RocketMQError::Timeout {
        operation: "proxy cluster command",
        timeout_ms: deadline.as_millis().clamp(1, u128::from(u64::MAX)) as u64,
    }
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
