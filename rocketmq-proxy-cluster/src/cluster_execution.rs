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

use rocketmq_client_rust::proxy_adapter_compat::rpc_hook_from_outbound_signer;
use rocketmq_client_rust::proxy_adapter_compat::ClientRpcHook;
use rocketmq_client_rust::proxy_adapter_compat::MessageQueueAssignment;
use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::ClientRuntimeConfig;
use rocketmq_client_rust::TelemetryHandle;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::body::acl_info::AclInfo;
use rocketmq_protocol::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
use rocketmq_protocol::protocol::body::response::lock_batch_response_body::LockBatchResponseBody;
use rocketmq_protocol::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_protocol::protocol::body::user_info::UserInfo;
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
use rocketmq_runtime::BudgetedQueue;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_security_api::OutboundSigner;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use super::cluster_admission::cluster_lane_domain_id;
use super::cluster_admission::ClusterExecutionLanes;
use super::cluster_admission::ClusterExecutionPolicy;
use super::cluster_admission::QueuedClusterCommand;
use super::cluster_admission::CLUSTER_EXECUTION_LANE_COUNT;
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
}

pub(super) enum ClusterCommand {
    ReadinessCheck {
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
        let worker_context = service_context.child("command-worker");
        let client_runtime = ClientRuntime::try_new(
            worker_context.child("client-runtime"),
            ClientRuntimeConfig::default(),
            telemetry_handle,
        )?;
        let base_domain_id = worker_context.task_group().id().as_u64();
        Self::spawn_execution(
            config,
            rpc_hook,
            service_context,
            worker_context,
            client_runtime,
            base_domain_id,
            Arc::new(DefaultClusterClientFactory),
            Arc::new(DefaultClusterProducerFactory),
            ClusterExecutionPolicy::default(),
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
        let worker_context = service_context.child("command-worker");
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
    ) -> ProxyResult<(Self, tokio_util::sync::CancellationToken)> {
        let lanes = Arc::new(ClusterExecutionLanes::new(policy)?);
        let cancellation = worker_context.task_group().cancellation_token();
        let queue_clones = lanes.queue_clones();
        let (completed_sender, completed_receiver) = mpsc::channel(CLUSTER_EXECUTION_LANE_COUNT);

        for (lane, queue) in queue_clones.iter().cloned().enumerate() {
            let lane_context = worker_context.child(format!("command-lane-{lane}"));
            let lane_cancellation = lane_context.task_group().cancellation_token();
            let lane_shutdown_context = service_context.clone();
            let lane_config = config.clone();
            let lane_state = ClusterWorkerState::with_factories(
                client_runtime.clone(),
                cluster_lane_domain_id(base_domain_id, lane),
                rpc_hook.clone(),
                client_factory.clone(),
                producer_factory.clone(),
            );
            let completed_sender = completed_sender.clone();
            if let Err(error) = lane_context.spawn_service(format!("proxy.cluster.lane-{lane}"), async move {
                run_cluster_lane(lane_config, lane_state, queue, lane_cancellation, lane_shutdown_context).await;
                let _ = completed_sender.try_send(());
            }) {
                worker_context.task_group().cancel();
                for queue in &queue_clones {
                    queue.close();
                }
                return Err(ProxyError::Transport {
                    message: format!("failed to spawn proxy cluster execution lane {lane}: {error}"),
                });
            }
        }
        drop(completed_sender);

        let owner_runtime = client_runtime;
        let owner_queues = queue_clones;
        let owner_cancellation = cancellation.clone();
        let owner_config = config;
        let owner_shutdown_context = service_context.clone();
        if let Err(error) = worker_context.spawn_service("proxy.cluster.execution-owner", async move {
            run_cluster_execution_owner(
                owner_config,
                owner_runtime,
                owner_queues,
                completed_receiver,
                owner_cancellation,
                owner_shutdown_context,
            )
            .await;
        }) {
            worker_context.task_group().cancel();
            for queue in &lanes.queues {
                queue.close();
            }
            return Err(ProxyError::Transport {
                message: format!("failed to spawn proxy cluster execution owner: {error}"),
            });
        }
        Ok((Self { lanes }, cancellation))
    }

    pub(super) async fn readiness_check(&self) -> ProxyResult<()> {
        self.execute(|reply| ClusterCommand::ReadinessCheck { reply }).await
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

    async fn execute<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<ProxyResult<T>>) -> ClusterCommand,
    ) -> ProxyResult<T>
    where
        T: Send + 'static,
    {
        let (reply, receiver) = oneshot::channel();
        self.lanes.enqueue(command(reply))?;
        receiver.await.map_err(|_| ProxyError::Transport {
            message: "proxy cluster worker dropped response".to_owned(),
        })?
    }
}

pub(super) async fn run_cluster_lane(
    config: ClusterConfig,
    mut state: ClusterWorkerState,
    queue: BudgetedQueue<QueuedClusterCommand>,
    cancellation: tokio_util::sync::CancellationToken,
    shutdown_context: ChildServiceContext,
) {
    loop {
        let queued = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                queue.close();
                break;
            },
            command = queue.recv_budgeted() => match command {
                Some(command) => command,
                None => break,
            }
        };
        let (queued, active_permit, _) = queued.into_parts();
        let waited = queued.enqueued_at.elapsed();
        if waited >= queued.deadline {
            queued.command.reject(cluster_queue_timeout(queued.deadline));
            drop(active_permit);
            continue;
        }
        let mut command = queued.command;
        command.apply_queue_wait(waited);
        tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            () = handle_cluster_command(&config, &mut state, command) => {}
        }
        drop(active_permit);
    }
    while let Some(queued) = queue.try_pop() {
        queued.command.reject(ProxyError::Transport {
            message: "proxy cluster command execution stopped during shutdown".to_owned(),
        });
    }
    let shutdown_deadline = cluster_shutdown_deadline(&shutdown_context, config.shutdown_timeout());
    for (producer_group, producer) in &mut state.send_producers {
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
    config: ClusterConfig,
    client_runtime: Arc<ClientRuntime>,
    queues: Vec<BudgetedQueue<QueuedClusterCommand>>,
    mut completed_receiver: mpsc::Receiver<()>,
    cancellation: tokio_util::sync::CancellationToken,
    shutdown_context: ChildServiceContext,
) {
    cancellation.cancelled().await;
    for queue in &queues {
        queue.close();
    }
    let shutdown_deadline = cluster_shutdown_deadline(&shutdown_context, config.shutdown_timeout());
    for lane in 0..CLUSTER_EXECUTION_LANE_COUNT {
        match tokio::time::timeout(shutdown_deadline.remaining(), completed_receiver.recv()).await {
            Ok(Some(())) => {}
            Ok(None) => break,
            Err(_) => {
                tracing::warn!(
                    lane,
                    "proxy cluster execution lane shutdown exceeded the shared deadline"
                );
                break;
            }
        }
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

fn cluster_shutdown_deadline(context: &ChildServiceContext, configured_timeout: Duration) -> ShutdownDeadline {
    let configured = ShutdownDeadline::after(configured_timeout);
    match context.task_group().shutdown_deadline() {
        Some(parent) if parent.instant() <= configured.instant() => parent,
        Some(_) | None => configured,
    }
}
