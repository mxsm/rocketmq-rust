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

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::stream;
use futures::Stream;
use futures::StreamExt;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use tokio::sync::mpsc;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::time::MissedTickBehavior;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::config::ProxyConfig;
use crate::context::ProxyContext;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::grpc::adapter;
use crate::processor::MessagingProcessor;
use crate::proto::v2;
use crate::service::ResourceIdentity;
use crate::session::ClientSessionRegistry;
use crate::session::ClientSettingsSnapshot;
use crate::session::PreparedTransactionRegistration;
use crate::session::ReceiptHandleRegistration;
use crate::session::TrackedReceiptHandle;
use crate::status::ProxyStatusMapper;

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Clone)]
struct ExecutionGuards {
    route: Arc<Semaphore>,
    producer: Arc<Semaphore>,
    consumer: Arc<Semaphore>,
    client_manager: Arc<Semaphore>,
}

impl ExecutionGuards {
    fn from_config(config: &ProxyConfig) -> Self {
        Self {
            route: Arc::new(Semaphore::new(config.runtime.route_permits)),
            producer: Arc::new(Semaphore::new(config.runtime.producer_permits)),
            consumer: Arc::new(Semaphore::new(config.runtime.consumer_permits)),
            client_manager: Arc::new(Semaphore::new(config.runtime.client_manager_permits)),
        }
    }

    fn try_route(&self) -> ProxyResult<OwnedSemaphorePermit> {
        self.route
            .clone()
            .try_acquire_owned()
            .map_err(|_| ProxyError::too_many_requests("route"))
    }

    fn try_consumer(&self) -> ProxyResult<OwnedSemaphorePermit> {
        self.consumer
            .clone()
            .try_acquire_owned()
            .map_err(|_| ProxyError::too_many_requests("consumer"))
    }

    fn try_producer(&self) -> ProxyResult<OwnedSemaphorePermit> {
        self.producer
            .clone()
            .try_acquire_owned()
            .map_err(|_| ProxyError::too_many_requests("producer"))
    }

    fn try_client_manager(&self) -> ProxyResult<OwnedSemaphorePermit> {
        self.client_manager
            .clone()
            .try_acquire_owned()
            .map_err(|_| ProxyError::too_many_requests("client-manager"))
    }
}

pub struct ProxyGrpcService<P> {
    config: Arc<ProxyConfig>,
    processor: Arc<P>,
    sessions: ClientSessionRegistry,
    guards: ExecutionGuards,
}

impl<P> Clone for ProxyGrpcService<P> {
    fn clone(&self) -> Self {
        Self {
            config: Arc::clone(&self.config),
            processor: Arc::clone(&self.processor),
            sessions: self.sessions.clone(),
            guards: self.guards.clone(),
        }
    }
}

impl<P> ProxyGrpcService<P> {
    pub fn new(config: Arc<ProxyConfig>, processor: Arc<P>, sessions: ClientSessionRegistry) -> Self {
        Self {
            guards: ExecutionGuards::from_config(config.as_ref()),
            config,
            processor,
            sessions,
        }
    }

    fn context<T>(&self, rpc_name: &'static str, request: &Request<T>) -> ProxyContext {
        ProxyContext::from_grpc_request(rpc_name, request)
    }

    fn status_stream<T>(&self, item: T) -> ResponseStream<T>
    where
        T: Send + 'static,
    {
        Box::pin(stream::iter(vec![Ok(item)]))
    }

    fn items_stream<T>(&self, items: Vec<T>) -> ResponseStream<T>
    where
        T: Send + 'static,
    {
        Box::pin(stream::iter(items.into_iter().map(Ok)))
    }

    pub async fn run_housekeeping_until<F>(&self, shutdown: F)
    where
        F: std::future::Future<Output = ()> + Send,
    {
        let mut interval = tokio::time::interval(self.housekeeping_interval());
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                _ = interval.tick() => {
                    self.reap_session_state();
                }
            }
        }
    }

    fn housekeeping_interval(&self) -> Duration {
        let min_ttl = self
            .config
            .session
            .client_ttl()
            .min(self.config.session.receipt_handle_ttl());
        let half_ttl_ms = min_ttl.as_millis().saturating_div(2).clamp(1_000, 30_000) as u64;
        Duration::from_millis(half_ttl_ms)
    }

    fn reap_session_state(&self) {
        let _ = self.sessions.reap_expired(
            self.config.session.client_ttl(),
            self.config.session.receipt_handle_ttl(),
        );
    }
}

impl<P> ProxyGrpcService<P>
where
    P: MessagingProcessor + 'static,
{
    fn not_implemented_status(&self, feature: &'static str) -> v2::Status {
        ProxyStatusMapper::from_error(&ProxyError::not_implemented(feature))
    }

    fn validate_client_context<'a>(&self, context: &'a ProxyContext) -> ProxyResult<&'a str> {
        context.require_client_id()
    }

    fn validate_heartbeat_request(&self, context: &ProxyContext, client_type: i32) -> ProxyResult<()> {
        self.validate_client_context(context)?;

        match v2::ClientType::try_from(client_type) {
            Ok(v2::ClientType::Producer)
            | Ok(v2::ClientType::PushConsumer)
            | Ok(v2::ClientType::SimpleConsumer)
            | Ok(v2::ClientType::PullConsumer)
            | Ok(v2::ClientType::LitePushConsumer)
            | Ok(v2::ClientType::LiteSimpleConsumer) => Ok(()),
            _ => Err(ProxyError::UnrecognizedClientType(client_type)),
        }
    }

    fn pull_status(status: v2::Status) -> v2::PullMessageResponse {
        v2::PullMessageResponse {
            content: Some(v2::pull_message_response::Content::Status(status)),
        }
    }

    fn telemetry_status(status: v2::Status) -> v2::TelemetryCommand {
        v2::TelemetryCommand {
            status: Some(status),
            command: None,
        }
    }

    fn merged_telemetry_settings(&self, settings: &v2::Settings) -> v2::Settings {
        let mut merged = settings.clone();
        if let Some(v2::settings::PubSub::Subscription(subscription)) = merged.pub_sub.as_mut() {
            if subscription.receive_batch_size.is_some_and(|value| value <= 0) {
                subscription.receive_batch_size = Some(1);
            }

            let timeout = subscription
                .long_polling_timeout
                .as_ref()
                .and_then(proto_duration)
                .unwrap_or_else(|| self.config.session.min_long_polling_timeout());
            subscription.long_polling_timeout = Some(duration_to_proto_duration(clamp_duration(
                timeout,
                self.config.session.min_long_polling_timeout(),
                self.config.session.max_long_polling_timeout(),
            )));

            if is_lite_client(merged.client_type) {
                subscription.lite_subscription_quota.get_or_insert(1200);
                subscription.max_lite_topic_size.get_or_insert(64);
            }
        }
        merged
    }

    fn effective_receive_request(
        &self,
        context: &ProxyContext,
        mut request: crate::processor::ReceiveMessageRequest,
    ) -> ProxyResult<crate::processor::ReceiveMessageRequest> {
        if let Some(client_id) = context.client_id() {
            if let Some(settings) = self.sessions.settings_for_client(client_id) {
                self.apply_receive_settings(&mut request, &settings);
            }
        }

        if let Some(deadline) = context.deadline() {
            request.long_polling_timeout = request.long_polling_timeout.min(deadline);
        }

        Ok(request)
    }

    fn apply_receive_settings(
        &self,
        request: &mut crate::processor::ReceiveMessageRequest,
        settings: &ClientSettingsSnapshot,
    ) {
        let Some(subscription) = settings.subscription.as_ref() else {
            return;
        };

        if let Some(receive_batch_size) = subscription.receive_batch_size {
            request.batch_size = request.batch_size.min(receive_batch_size.max(1));
        }

        if let Some(long_polling_timeout) = subscription.long_polling_timeout {
            request.long_polling_timeout = clamp_duration(
                long_polling_timeout,
                self.config.session.min_long_polling_timeout(),
                self.config.session.max_long_polling_timeout(),
            );
        }

        request.target.fifo |= subscription.fifo;
    }

    fn track_received_receipt_handles(
        &self,
        context: &ProxyContext,
        request: &crate::processor::ReceiveMessageRequest,
        plan: &crate::processor::ReceiveMessagePlan,
    ) {
        if !(self.config.session.auto_renew_enabled && request.auto_renew) {
            return;
        }
        let Some(client_id) = context.client_id() else {
            return;
        };

        for message in &plan.messages {
            let Some(receipt_handle) = message
                .message
                .property(&cheetah_string::CheetahString::from_static_str(
                    MessageConst::PROPERTY_POP_CK,
                ))
                .map(|value| value.to_string())
            else {
                continue;
            };

            let tracked = self.sessions.track_receipt_handle(ReceiptHandleRegistration {
                client_id: client_id.to_owned(),
                group: request.group.clone(),
                topic: ResourceIdentity::new(
                    request.target.topic.namespace().to_owned(),
                    message.message.topic().to_string(),
                ),
                message_id: message.message.msg_id().to_string(),
                receipt_handle,
                invisible_duration: message.invisible_duration,
            });
            self.spawn_auto_renew_task(context.clone(), tracked);
        }
    }

    fn reconcile_ack_result(
        &self,
        context: &ProxyContext,
        request: &crate::processor::AckMessageRequest,
        plan: &crate::processor::AckMessagePlan,
    ) {
        let Some(client_id) = context.client_id() else {
            return;
        };

        for entry in &plan.entries {
            if entry.status.is_ok() {
                let _ = self.sessions.remove_receipt_handle_matching(
                    client_id,
                    &request.group,
                    &request.topic,
                    entry.message_id.as_str(),
                    entry.receipt_handle.as_str(),
                );
            }
        }
    }

    fn reconcile_change_invisible_result(
        &self,
        context: &ProxyContext,
        request: &crate::processor::ChangeInvisibleDurationRequest,
        plan: &crate::processor::ChangeInvisibleDurationPlan,
    ) {
        let Some(client_id) = context.client_id() else {
            return;
        };
        if !plan.status.is_ok() {
            return;
        }

        let _ = self.sessions.update_receipt_handle_matching(
            client_id,
            &request.group,
            &request.topic,
            request.message_id.as_str(),
            request.receipt_handle.as_str(),
            plan.receipt_handle.as_str(),
            request.invisible_duration,
        );
    }

    fn track_prepared_transactions(
        &self,
        context: &ProxyContext,
        request: &crate::processor::SendMessageRequest,
        plan: &crate::processor::SendMessagePlan,
    ) {
        let Some(client_id) = context.client_id() else {
            return;
        };
        let producer_group =
            crate::cluster::build_proxy_producer_group(&self.config.cluster, Some(client_id), context.request_id());

        for (message, result) in request.messages.iter().zip(plan.entries.iter()) {
            if !is_transaction_message(&message.message) {
                continue;
            }
            let Some(send_result) = result.send_result.as_ref() else {
                continue;
            };
            let Some(transaction_id) = send_result.transaction_id.clone() else {
                continue;
            };
            let Some(commit_log_message_id) = send_result
                .offset_msg_id
                .clone()
                .or_else(|| send_result.msg_id.as_ref().map(ToString::to_string))
            else {
                continue;
            };
            let client_visible_message_id = send_result
                .msg_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| message.client_message_id.clone());
            self.sessions
                .track_prepared_transaction(PreparedTransactionRegistration {
                    client_id: client_id.to_owned(),
                    topic: message.topic.clone(),
                    message_id: client_visible_message_id,
                    transaction_id,
                    producer_group: producer_group.clone(),
                    transaction_state_table_offset: send_result.queue_offset,
                    commit_log_message_id,
                });
        }
    }

    fn enrich_end_transaction_request(
        &self,
        context: &ProxyContext,
        request: &mut crate::processor::EndTransactionRequest,
    ) -> ProxyResult<()> {
        let client_id = self.validate_client_context(context)?;
        let tracked = self
            .sessions
            .prepared_transaction(client_id, request.transaction_id.as_str(), request.message_id.as_str())
            .ok_or_else(|| {
                ProxyError::invalid_transaction_id(format!(
                    "transaction '{}' was not found in proxy session state",
                    request.transaction_id
                ))
            })?;
        request.producer_group = Some(tracked.producer_group);
        request.transaction_state_table_offset = Some(tracked.transaction_state_table_offset);
        request.commit_log_message_id = Some(tracked.commit_log_message_id);
        Ok(())
    }

    fn reconcile_end_transaction_result(
        &self,
        context: &ProxyContext,
        request: &crate::processor::EndTransactionRequest,
        plan: &crate::processor::EndTransactionPlan,
    ) {
        if !plan.status.is_ok() {
            return;
        }
        let Some(client_id) = context.client_id() else {
            return;
        };
        let _ = self.sessions.remove_prepared_transaction(
            client_id,
            request.transaction_id.as_str(),
            request.message_id.as_str(),
        );
    }

    fn spawn_auto_renew_task(&self, context: ProxyContext, tracked: TrackedReceiptHandle) {
        let processor = Arc::clone(&self.processor);
        let sessions = self.sessions.clone();
        tokio::spawn(async move {
            let client_id = tracked.client_id.clone();
            let group = tracked.group.clone();
            let topic = tracked.topic.clone();
            let message_id = tracked.message_id.clone();

            loop {
                let Some(current) =
                    sessions.tracked_receipt_handle(client_id.as_str(), &group, &topic, message_id.as_str())
                else {
                    break;
                };
                let delay = auto_renew_interval(current.invisible_duration);
                let cancellation = current.cancellation.clone();

                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    _ = tokio::time::sleep(delay) => {}
                }

                let Some(current) =
                    sessions.tracked_receipt_handle(client_id.as_str(), &group, &topic, message_id.as_str())
                else {
                    break;
                };

                let renew_request = crate::processor::ChangeInvisibleDurationRequest {
                    group: group.clone(),
                    topic: topic.clone(),
                    receipt_handle: current.receipt_handle.clone(),
                    invisible_duration: current.invisible_duration,
                    message_id: message_id.clone(),
                    lite_topic: None,
                    suspend: None,
                };

                match processor
                    .change_invisible_duration(&context, renew_request.clone())
                    .await
                {
                    Ok(plan) if plan.status.is_ok() => {
                        let next_receipt_handle = if plan.receipt_handle.is_empty() {
                            renew_request.receipt_handle
                        } else {
                            plan.receipt_handle
                        };
                        let _ = sessions.update_receipt_handle(
                            client_id.as_str(),
                            &group,
                            &topic,
                            message_id.as_str(),
                            next_receipt_handle.as_str(),
                            renew_request.invisible_duration,
                        );
                    }
                    Ok(plan) if plan.status.code() == v2::Code::InvalidReceiptHandle as i32 => {
                        let _ = sessions.remove_receipt_handle(client_id.as_str(), &group, &topic, message_id.as_str());
                        break;
                    }
                    Ok(_) | Err(_) => {}
                }
            }
        });
    }

    async fn handle_telemetry_command(
        &self,
        context: &ProxyContext,
        command: v2::TelemetryCommand,
    ) -> v2::TelemetryCommand {
        self.sessions.upsert_from_context(context);

        match command.command {
            Some(v2::telemetry_command::Command::Settings(settings)) => {
                let merged = self.merged_telemetry_settings(&settings);
                let _ = self.sessions.update_settings_from_telemetry(context, &merged);
                v2::TelemetryCommand {
                    status: Some(ProxyStatusMapper::ok()),
                    command: Some(v2::telemetry_command::Command::Settings(merged)),
                }
            }
            Some(v2::telemetry_command::Command::ThreadStackTrace(_))
            | Some(v2::telemetry_command::Command::VerifyMessageResult(_)) => {
                Self::telemetry_status(ProxyStatusMapper::ok())
            }
            Some(_) => Self::telemetry_status(ProxyStatusMapper::from_code(
                v2::Code::BadRequest,
                "client sent an unsupported telemetry command",
            )),
            None => Self::telemetry_status(ProxyStatusMapper::ok()),
        }
    }
}

fn clamp_duration(duration: Duration, minimum: Duration, maximum: Duration) -> Duration {
    duration.max(minimum).min(maximum)
}

fn auto_renew_interval(invisible_duration: Duration) -> Duration {
    let half_millis = invisible_duration.as_millis().saturating_div(2).clamp(1_000, 15_000) as u64;
    Duration::from_millis(half_millis)
}

fn proto_duration(duration: &prost_types::Duration) -> Option<Duration> {
    if duration.seconds < 0 || duration.nanos < 0 || duration.nanos >= 1_000_000_000 {
        return None;
    }

    let seconds = u64::try_from(duration.seconds).ok()?;
    let nanos = u32::try_from(duration.nanos).ok()?;
    Some(Duration::new(seconds, nanos))
}

fn duration_to_proto_duration(duration: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    }
}

fn is_lite_client(client_type: Option<i32>) -> bool {
    matches!(
        client_type.and_then(|client_type| v2::ClientType::try_from(client_type).ok()),
        Some(v2::ClientType::LitePushConsumer | v2::ClientType::LiteSimpleConsumer)
    )
}

fn is_transaction_message(message: &rocketmq_common::common::message::message_single::Message) -> bool {
    message
        .property(&cheetah_string::CheetahString::from_static_str(
            MessageConst::PROPERTY_TRANSACTION_PREPARED,
        ))
        .is_some()
}

#[tonic::async_trait]
impl<P> v2::messaging_service_server::MessagingService for ProxyGrpcService<P>
where
    P: MessagingProcessor + 'static,
{
    type ReceiveMessageStream = ResponseStream<v2::ReceiveMessageResponse>;
    type PullMessageStream = ResponseStream<v2::PullMessageResponse>;
    type TelemetryStream = ResponseStream<v2::TelemetryCommand>;

    async fn query_route(
        &self,
        request: Request<v2::QueryRouteRequest>,
    ) -> Result<Response<v2::QueryRouteResponse>, Status> {
        self.reap_session_state();
        let context = self.context("QueryRoute", &request);
        let request = request.into_inner();

        let response = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_query_route_request(self.config.as_ref(), &request))
        {
            Ok(input) => match self.guards.try_route() {
                Ok(_permit) => match self.processor.query_route(&context, input).await {
                    Ok(plan) => adapter::build_query_route_response(&request, &plan),
                    Err(error) => adapter::error_query_route_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_query_route_response(ProxyStatusMapper::from_error(&error)),
            },
            Err(error) => adapter::error_query_route_response(ProxyStatusMapper::from_error(&error)),
        };

        Ok(Response::new(response))
    }

    async fn heartbeat(
        &self,
        request: Request<v2::HeartbeatRequest>,
    ) -> Result<Response<v2::HeartbeatResponse>, Status> {
        self.reap_session_state();
        let context = self.context("Heartbeat", &request);
        let request = request.into_inner();

        let status = match self.guards.try_client_manager() {
            Ok(_permit) => match self.validate_heartbeat_request(&context, request.client_type) {
                Ok(()) => {
                    self.sessions
                        .upsert_from_context_with_client_type(&context, Some(request.client_type));
                    ProxyStatusMapper::ok()
                }
                Err(error) => ProxyStatusMapper::from_error(&error),
            },
            Err(error) => ProxyStatusMapper::from_error(&error),
        };

        Ok(Response::new(v2::HeartbeatResponse { status: Some(status) }))
    }

    async fn send_message(
        &self,
        request: Request<v2::SendMessageRequest>,
    ) -> Result<Response<v2::SendMessageResponse>, Status> {
        self.reap_session_state();
        let context = self.context("SendMessage", &request);
        let request = request.into_inner();

        let response = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_send_message_request(&context, &request))
        {
            Ok(input) => match self.guards.try_producer() {
                Ok(_permit) => match self.processor.send_message(&context, input.clone()).await {
                    Ok(plan) => {
                        self.track_prepared_transactions(&context, &input, &plan);
                        adapter::build_send_message_response(&plan, &input)
                    }
                    Err(error) => adapter::error_send_message_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_send_message_response(ProxyStatusMapper::from_error(&error)),
            },
            Err(error) => adapter::error_send_message_response(ProxyStatusMapper::from_error(&error)),
        };

        Ok(Response::new(response))
    }

    async fn query_assignment(
        &self,
        request: Request<v2::QueryAssignmentRequest>,
    ) -> Result<Response<v2::QueryAssignmentResponse>, Status> {
        self.reap_session_state();
        let context = self.context("QueryAssignment", &request);
        let request = request.into_inner();

        let response = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_query_assignment_request(self.config.as_ref(), &request))
        {
            Ok(input) => match self.guards.try_route() {
                Ok(_permit) => match self.processor.query_assignment(&context, input).await {
                    Ok(plan) => adapter::build_query_assignment_response(&request, &plan),
                    Err(error) => adapter::error_query_assignment_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_query_assignment_response(ProxyStatusMapper::from_error(&error)),
            },
            Err(error) => adapter::error_query_assignment_response(ProxyStatusMapper::from_error(&error)),
        };

        Ok(Response::new(response))
    }

    async fn receive_message(
        &self,
        request: Request<v2::ReceiveMessageRequest>,
    ) -> Result<Response<Self::ReceiveMessageStream>, Status> {
        self.reap_session_state();
        let context = self.context("ReceiveMessage", &request);
        let request = request.into_inner();
        let responses = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_receive_message_request(&request))
            .and_then(|input| self.effective_receive_request(&context, input))
        {
            Ok(input) => match self.guards.try_consumer() {
                Ok(_permit) => {
                    self.sessions.upsert_from_context(&context);
                    match self.processor.receive_message(&context, input.clone()).await {
                        Ok(plan) => {
                            self.track_received_receipt_handles(&context, &input, &plan);
                            adapter::build_receive_message_responses(&plan)
                        }
                        Err(error) => adapter::error_receive_message_responses(ProxyStatusMapper::from_error(&error)),
                    }
                }
                Err(error) => adapter::error_receive_message_responses(ProxyStatusMapper::from_error(&error)),
            },
            Err(error) => adapter::error_receive_message_responses(ProxyStatusMapper::from_error(&error)),
        };

        Ok(Response::new(self.items_stream(responses)))
    }

    async fn ack_message(
        &self,
        request: Request<v2::AckMessageRequest>,
    ) -> Result<Response<v2::AckMessageResponse>, Status> {
        self.reap_session_state();
        let context = self.context("AckMessage", &request);
        let request = request.into_inner();

        let response = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_ack_message_request(&request))
        {
            Ok(input) => match self.guards.try_consumer() {
                Ok(_permit) => match self.processor.ack_message(&context, input.clone()).await {
                    Ok(plan) => {
                        self.reconcile_ack_result(&context, &input, &plan);
                        adapter::build_ack_message_response(&plan)
                    }
                    Err(error) => adapter::error_ack_message_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_ack_message_response(ProxyStatusMapper::from_error(&error)),
            },
            Err(error) => adapter::error_ack_message_response(ProxyStatusMapper::from_error(&error)),
        };

        Ok(Response::new(response))
    }

    async fn forward_message_to_dead_letter_queue(
        &self,
        _request: Request<v2::ForwardMessageToDeadLetterQueueRequest>,
    ) -> Result<Response<v2::ForwardMessageToDeadLetterQueueResponse>, Status> {
        Ok(Response::new(v2::ForwardMessageToDeadLetterQueueResponse {
            status: Some(self.not_implemented_status("ForwardMessageToDeadLetterQueue")),
        }))
    }

    async fn pull_message(
        &self,
        request: Request<v2::PullMessageRequest>,
    ) -> Result<Response<Self::PullMessageStream>, Status> {
        self.reap_session_state();
        let context = self.context("PullMessage", &request);
        let status = match self
            .validate_client_context(&context)
            .and_then(|_| self.guards.try_consumer().map(|_| ()))
        {
            Ok(()) => self.not_implemented_status("PullMessage"),
            Err(error) => ProxyStatusMapper::from_error(&error),
        };
        Ok(Response::new(self.status_stream(Self::pull_status(status))))
    }

    async fn update_offset(
        &self,
        _request: Request<v2::UpdateOffsetRequest>,
    ) -> Result<Response<v2::UpdateOffsetResponse>, Status> {
        Ok(Response::new(v2::UpdateOffsetResponse {
            status: Some(self.not_implemented_status("UpdateOffset")),
        }))
    }

    async fn get_offset(
        &self,
        _request: Request<v2::GetOffsetRequest>,
    ) -> Result<Response<v2::GetOffsetResponse>, Status> {
        Ok(Response::new(v2::GetOffsetResponse {
            status: Some(self.not_implemented_status("GetOffset")),
            offset: 0,
        }))
    }

    async fn query_offset(
        &self,
        _request: Request<v2::QueryOffsetRequest>,
    ) -> Result<Response<v2::QueryOffsetResponse>, Status> {
        Ok(Response::new(v2::QueryOffsetResponse {
            status: Some(self.not_implemented_status("QueryOffset")),
            offset: 0,
        }))
    }

    async fn end_transaction(
        &self,
        request: Request<v2::EndTransactionRequest>,
    ) -> Result<Response<v2::EndTransactionResponse>, Status> {
        self.reap_session_state();
        let context = self.context("EndTransaction", &request);
        let request = request.into_inner();

        let response = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_end_transaction_request(&request))
            .and_then(|mut input| {
                self.enrich_end_transaction_request(&context, &mut input)?;
                Ok(input)
            }) {
            Ok(input) => match self.guards.try_producer() {
                Ok(_permit) => match self.processor.end_transaction(&context, input.clone()).await {
                    Ok(plan) => {
                        self.reconcile_end_transaction_result(&context, &input, &plan);
                        adapter::build_end_transaction_response(&plan)
                    }
                    Err(error) => adapter::error_end_transaction_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_end_transaction_response(ProxyStatusMapper::from_error(&error)),
            },
            Err(error) => adapter::error_end_transaction_response(ProxyStatusMapper::from_error(&error)),
        };

        Ok(Response::new(response))
    }

    async fn telemetry(
        &self,
        request: Request<tonic::Streaming<v2::TelemetryCommand>>,
    ) -> Result<Response<Self::TelemetryStream>, Status> {
        self.reap_session_state();
        let context = self.context("Telemetry", &request);
        let mut inbound = request.into_inner();
        let permit = match self
            .validate_client_context(&context)
            .and_then(|_| self.guards.try_client_manager())
        {
            Ok(permit) => permit,
            Err(error) => {
                return Ok(Response::new(
                    self.status_stream(Self::telemetry_status(ProxyStatusMapper::from_error(&error))),
                ));
            }
        };

        self.sessions.upsert_from_context(&context);
        let service = self.clone();
        let (sender, receiver) = mpsc::channel(16);
        tokio::spawn(async move {
            let _permit = permit;
            loop {
                match inbound.next().await {
                    Some(Ok(command)) => {
                        let response = service.handle_telemetry_command(&context, command).await;
                        if sender.send(Ok(response)).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(error)) => {
                        let _ = sender.send(Err(error)).await;
                        break;
                    }
                    None => break,
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    async fn notify_client_termination(
        &self,
        request: Request<v2::NotifyClientTerminationRequest>,
    ) -> Result<Response<v2::NotifyClientTerminationResponse>, Status> {
        self.reap_session_state();
        let context = self.context("NotifyClientTermination", &request);
        let status = match self
            .guards
            .try_client_manager()
            .and_then(|_| self.validate_client_context(&context))
        {
            Ok(client_id) => {
                self.sessions.remove_client(client_id);
                ProxyStatusMapper::ok()
            }
            Err(error) => ProxyStatusMapper::from_error(&error),
        };

        Ok(Response::new(v2::NotifyClientTerminationResponse {
            status: Some(status),
        }))
    }

    async fn change_invisible_duration(
        &self,
        request: Request<v2::ChangeInvisibleDurationRequest>,
    ) -> Result<Response<v2::ChangeInvisibleDurationResponse>, Status> {
        self.reap_session_state();
        let context = self.context("ChangeInvisibleDuration", &request);
        let request = request.into_inner();

        let response = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_change_invisible_duration_request(&request))
        {
            Ok(input) => match self.guards.try_consumer() {
                Ok(_permit) => match self.processor.change_invisible_duration(&context, input.clone()).await {
                    Ok(plan) => {
                        self.reconcile_change_invisible_result(&context, &input, &plan);
                        adapter::build_change_invisible_duration_response(&plan)
                    }
                    Err(error) => {
                        adapter::error_change_invisible_duration_response(ProxyStatusMapper::from_error(&error))
                    }
                },
                Err(error) => adapter::error_change_invisible_duration_response(ProxyStatusMapper::from_error(&error)),
            },
            Err(error) => adapter::error_change_invisible_duration_response(ProxyStatusMapper::from_error(&error)),
        };

        Ok(Response::new(response))
    }

    async fn recall_message(
        &self,
        _request: Request<v2::RecallMessageRequest>,
    ) -> Result<Response<v2::RecallMessageResponse>, Status> {
        Ok(Response::new(v2::RecallMessageResponse {
            status: Some(self.not_implemented_status("RecallMessage")),
            message_id: String::new(),
        }))
    }

    async fn sync_lite_subscription(
        &self,
        request: Request<v2::SyncLiteSubscriptionRequest>,
    ) -> Result<Response<v2::SyncLiteSubscriptionResponse>, Status> {
        self.reap_session_state();
        let context = self.context("SyncLiteSubscription", &request);
        let request = request.into_inner();

        let status = match self.validate_client_context(&context).and_then(|client_id| {
            let _permit = self.guards.try_client_manager()?;
            let input = crate::session::build_lite_subscription_sync_request(&request)?;
            self.sessions.upsert_from_context(&context);
            let settings = self.sessions.settings_for_client(client_id);
            self.sessions
                .sync_lite_subscription(client_id, input, settings.as_ref())
                .map(|_| ProxyStatusMapper::ok())
        }) {
            Ok(status) => status,
            Err(error) => ProxyStatusMapper::from_error(&error),
        };

        Ok(Response::new(v2::SyncLiteSubscriptionResponse { status: Some(status) }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use futures::StreamExt;
    use rocketmq_client_rust::producer::send_result::SendResult;
    use rocketmq_client_rust::producer::send_status::SendStatus;
    use rocketmq_common::common::message::message_ext::MessageExt;
    use rocketmq_common::common::message::MessageConst;
    use rocketmq_common::common::message::MessageTrait;
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
    use rocketmq_remoting::protocol::route::route_data_view::QueueData;
    use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
    use tonic::metadata::MetadataValue;
    use tonic::Request;

    use super::ProxyGrpcService;
    use crate::config::ProxyConfig;
    use crate::grpc::adapter;
    use crate::processor::AckMessageRequest;
    use crate::processor::AckMessageResultEntry;
    use crate::processor::ChangeInvisibleDurationPlan;
    use crate::processor::ChangeInvisibleDurationRequest;
    use crate::processor::DefaultMessagingProcessor;
    use crate::processor::ReceiveMessagePlan;
    use crate::processor::ReceiveMessageRequest;
    use crate::processor::ReceivedMessage;
    use crate::processor::SendMessageRequest;
    use crate::processor::SendMessageResultEntry;
    use crate::proto::v2;
    use crate::proto::v2::messaging_service_server::MessagingService;
    use crate::service::ClusterServiceManager;
    use crate::service::ConsumerService;
    use crate::service::DefaultConsumerService;
    use crate::service::MessageService;
    use crate::service::ProxyTopicMessageType;
    use crate::service::ResourceIdentity;
    use crate::service::StaticMessageService;
    use crate::service::StaticMetadataService;
    use crate::service::StaticRouteService;
    use crate::service::SubscriptionGroupMetadata;
    use crate::session::ClientSessionRegistry;
    use crate::status::ProxyPayloadStatus;
    use crate::status::ProxyStatusMapper;

    struct PartialMessageService;

    struct TestConsumerService;

    #[async_trait]
    impl MessageService for PartialMessageService {
        async fn send_message(
            &self,
            _context: &crate::context::ProxyContext,
            request: &SendMessageRequest,
        ) -> crate::error::ProxyResult<Vec<SendMessageResultEntry>> {
            Ok(request
                .messages
                .iter()
                .enumerate()
                .map(|(index, message)| {
                    if index == 0 {
                        let send_result = SendResult::new(
                            SendStatus::SendOk,
                            Some(CheetahString::from(message.client_message_id.as_str())),
                            None,
                            None,
                            0,
                        );
                        SendMessageResultEntry {
                            status: ProxyStatusMapper::from_send_result_payload(&send_result),
                            send_result: Some(send_result),
                        }
                    } else {
                        SendMessageResultEntry {
                            status: ProxyPayloadStatus::new(
                                v2::Code::TopicNotFound as i32,
                                "Topic 'TopicA' does not exist",
                            ),
                            send_result: None,
                        }
                    }
                })
                .collect())
        }
    }

    #[async_trait]
    impl ConsumerService for TestConsumerService {
        async fn receive_message(
            &self,
            _context: &crate::context::ProxyContext,
            _request: &ReceiveMessageRequest,
        ) -> crate::error::ProxyResult<ReceiveMessagePlan> {
            let mut message = MessageExt::default();
            message.set_topic(CheetahString::from("TopicA"));
            message.set_body(Bytes::from_static(b"hello"));
            message.set_msg_id(CheetahString::from("server-msg-id"));
            message.set_queue_id(3);
            message.set_queue_offset(42);
            message.set_reconsume_times(1);
            message.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK),
                CheetahString::from("receipt-handle"),
            );

            Ok(ReceiveMessagePlan {
                status: ProxyStatusMapper::ok_payload(),
                delivery_timestamp_ms: Some(1_710_000_000_000),
                messages: vec![ReceivedMessage {
                    message,
                    invisible_duration: std::time::Duration::from_secs(30),
                }],
            })
        }

        async fn ack_message(
            &self,
            _context: &crate::context::ProxyContext,
            request: &AckMessageRequest,
        ) -> crate::error::ProxyResult<Vec<AckMessageResultEntry>> {
            Ok(request
                .entries
                .iter()
                .map(|entry| AckMessageResultEntry {
                    message_id: entry.message_id.clone(),
                    receipt_handle: entry.receipt_handle.clone(),
                    status: ProxyStatusMapper::ok_payload(),
                })
                .collect())
        }

        async fn change_invisible_duration(
            &self,
            _context: &crate::context::ProxyContext,
            request: &ChangeInvisibleDurationRequest,
        ) -> crate::error::ProxyResult<ChangeInvisibleDurationPlan> {
            Ok(ChangeInvisibleDurationPlan {
                status: ProxyStatusMapper::ok_payload(),
                receipt_handle: format!("{}-renewed", request.receipt_handle),
            })
        }
    }

    fn test_service(
        route_service: StaticRouteService,
        metadata_service: StaticMetadataService,
    ) -> ProxyGrpcService<DefaultMessagingProcessor> {
        test_service_with_message_service(
            route_service,
            metadata_service,
            Arc::new(crate::service::DefaultMessageService),
        )
    }

    fn test_service_with_message_service(
        route_service: StaticRouteService,
        metadata_service: StaticMetadataService,
        message_service: Arc<dyn crate::service::MessageService>,
    ) -> ProxyGrpcService<DefaultMessagingProcessor> {
        test_service_with_services(
            route_service,
            metadata_service,
            message_service,
            Arc::new(DefaultConsumerService),
        )
    }

    fn test_service_with_services(
        route_service: StaticRouteService,
        metadata_service: StaticMetadataService,
        message_service: Arc<dyn crate::service::MessageService>,
        consumer_service: Arc<dyn crate::service::ConsumerService>,
    ) -> ProxyGrpcService<DefaultMessagingProcessor> {
        let manager = ClusterServiceManager::with_services(
            Arc::new(route_service),
            Arc::new(metadata_service),
            Arc::new(crate::service::DefaultAssignmentService),
            message_service,
            consumer_service,
            Arc::new(crate::service::DefaultTransactionService),
        );
        let processor = Arc::new(DefaultMessagingProcessor::new(Arc::new(manager)));
        ProxyGrpcService::new(
            Arc::new(ProxyConfig::default()),
            processor,
            ClientSessionRegistry::default(),
        )
    }

    fn sample_route() -> TopicRouteData {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(0_u64, CheetahString::from("127.0.0.1:10911"));

        TopicRouteData {
            queue_datas: vec![QueueData::new(CheetahString::from("broker-a"), 1, 1, 6, 0)],
            broker_datas: vec![BrokerData::new(
                CheetahString::from("cluster-a"),
                CheetahString::from("broker-a"),
                broker_addrs,
                None,
            )],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn query_route_returns_route_entries() {
        let route_service = StaticRouteService::default();
        route_service.insert(ResourceIdentity::new("", "TopicA"), sample_route());

        let metadata_service = StaticMetadataService::default();
        metadata_service.set_topic_message_type(ResourceIdentity::new("", "TopicA"), ProxyTopicMessageType::Normal);

        let service = test_service(route_service, metadata_service);
        let mut request = Request::new(v2::QueryRouteRequest {
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            endpoints: Some(v2::Endpoints {
                scheme: v2::AddressScheme::IPv4 as i32,
                addresses: vec![v2::Address {
                    host: "127.0.0.1".to_owned(),
                    port: 8081,
                }],
            }),
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.query_route(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert_eq!(response.message_queues.len(), 1);
    }

    #[tokio::test]
    async fn query_assignment_uses_fifo_group_semantics() {
        let route_service = StaticRouteService::default();
        route_service.insert(ResourceIdentity::new("", "TopicA"), sample_route());

        let metadata_service = StaticMetadataService::default();
        metadata_service.set_subscription_group(
            ResourceIdentity::new("", "GroupA"),
            SubscriptionGroupMetadata {
                consume_message_orderly: true,
                lite_bind_topic: None,
            },
        );

        let service = test_service(route_service, metadata_service);
        let mut request = Request::new(v2::QueryAssignmentRequest {
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            group: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "GroupA".to_owned(),
            }),
            endpoints: Some(v2::Endpoints {
                scheme: v2::AddressScheme::IPv4 as i32,
                addresses: vec![v2::Address {
                    host: "127.0.0.1".to_owned(),
                    port: 8081,
                }],
            }),
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.query_assignment(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert_eq!(response.assignments[0].message_queue.as_ref().unwrap().id, 0);
    }

    #[tokio::test]
    async fn heartbeat_requires_client_id_header() {
        let service = test_service(StaticRouteService::default(), StaticMetadataService::default());
        let request = Request::new(v2::HeartbeatRequest {
            group: None,
            client_type: v2::ClientType::Producer as i32,
        });

        let response = service.heartbeat(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::ClientIdRequired as i32);
    }

    #[tokio::test]
    async fn send_message_returns_send_result_entry() {
        let service = test_service_with_message_service(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
        );
        let mut request = Request::new(v2::SendMessageRequest {
            messages: vec![v2::Message {
                topic: Some(v2::Resource {
                    resource_namespace: String::new(),
                    name: "TopicA".to_owned(),
                }),
                user_properties: HashMap::new(),
                system_properties: Some(v2::SystemProperties {
                    message_id: "msg-1".to_owned(),
                    body_encoding: v2::Encoding::Identity as i32,
                    ..Default::default()
                }),
                body: Bytes::from_static(b"hello").to_vec(),
            }],
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.send_message(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert_eq!(response.entries.len(), 1);
        assert_eq!(response.entries[0].message_id, "msg-1");
    }

    #[tokio::test]
    async fn send_message_accepts_batch_requests() {
        let service = test_service_with_message_service(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
        );
        let mut request = Request::new(v2::SendMessageRequest {
            messages: vec![
                v2::Message {
                    topic: Some(v2::Resource {
                        resource_namespace: String::new(),
                        name: "TopicA".to_owned(),
                    }),
                    user_properties: HashMap::new(),
                    system_properties: Some(v2::SystemProperties {
                        message_id: "msg-1".to_owned(),
                        body_encoding: v2::Encoding::Identity as i32,
                        ..Default::default()
                    }),
                    body: Bytes::from_static(b"hello").to_vec(),
                },
                v2::Message {
                    topic: Some(v2::Resource {
                        resource_namespace: String::new(),
                        name: "TopicA".to_owned(),
                    }),
                    user_properties: HashMap::new(),
                    system_properties: Some(v2::SystemProperties {
                        message_id: "msg-2".to_owned(),
                        body_encoding: v2::Encoding::Identity as i32,
                        ..Default::default()
                    }),
                    body: Bytes::from_static(b"world").to_vec(),
                },
            ],
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.send_message(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert_eq!(response.entries.len(), 2);
        assert_eq!(response.entries[0].message_id, "msg-1");
        assert_eq!(response.entries[1].message_id, "msg-2");
    }

    #[tokio::test]
    async fn send_message_uses_multiple_results_for_partial_failures() {
        let service = test_service_with_message_service(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(PartialMessageService),
        );
        let mut request = Request::new(v2::SendMessageRequest {
            messages: vec![
                v2::Message {
                    topic: Some(v2::Resource {
                        resource_namespace: String::new(),
                        name: "TopicA".to_owned(),
                    }),
                    user_properties: HashMap::new(),
                    system_properties: Some(v2::SystemProperties {
                        message_id: "msg-1".to_owned(),
                        body_encoding: v2::Encoding::Identity as i32,
                        ..Default::default()
                    }),
                    body: Bytes::from_static(b"hello").to_vec(),
                },
                v2::Message {
                    topic: Some(v2::Resource {
                        resource_namespace: String::new(),
                        name: "TopicA".to_owned(),
                    }),
                    user_properties: HashMap::new(),
                    system_properties: Some(v2::SystemProperties {
                        message_id: "msg-2".to_owned(),
                        body_encoding: v2::Encoding::Identity as i32,
                        ..Default::default()
                    }),
                    body: Bytes::from_static(b"world").to_vec(),
                },
            ],
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.send_message(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::MultipleResults as i32);
        assert_eq!(response.entries.len(), 2);
        assert_eq!(response.entries[0].status.as_ref().unwrap().code, v2::Code::Ok as i32);
        assert_eq!(
            response.entries[1].status.as_ref().unwrap().code,
            v2::Code::TopicNotFound as i32
        );
    }

    #[tokio::test]
    async fn receive_message_streams_delivery_timestamp_message_and_status() {
        let service = test_service_with_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            Arc::new(TestConsumerService),
        );
        let mut request = Request::new(v2::ReceiveMessageRequest {
            group: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "GroupA".to_owned(),
            }),
            message_queue: Some(v2::MessageQueue {
                topic: Some(v2::Resource {
                    resource_namespace: String::new(),
                    name: "TopicA".to_owned(),
                }),
                id: 3,
                permission: v2::Permission::ReadWrite as i32,
                broker: Some(v2::Broker {
                    name: "broker-a".to_owned(),
                    id: 0,
                    endpoints: Some(v2::Endpoints {
                        scheme: v2::AddressScheme::IPv4 as i32,
                        addresses: vec![v2::Address {
                            host: "127.0.0.1".to_owned(),
                            port: 10911,
                        }],
                    }),
                }),
                accept_message_types: vec![v2::MessageType::Normal as i32],
            }),
            filter_expression: None,
            batch_size: 1,
            invisible_duration: Some(prost_types::Duration { seconds: 30, nanos: 0 }),
            auto_renew: false,
            long_polling_timeout: Some(prost_types::Duration { seconds: 1, nanos: 0 }),
            attempt_id: None,
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let mut stream = service.receive_message(request).await.unwrap().into_inner();
        let responses: Vec<_> = stream.by_ref().collect::<Vec<_>>().await;

        assert_eq!(responses.len(), 3);
        assert!(matches!(
            responses[0].as_ref().unwrap().content,
            Some(v2::receive_message_response::Content::DeliveryTimestamp(_))
        ));
        assert!(matches!(
            responses[1].as_ref().unwrap().content,
            Some(v2::receive_message_response::Content::Message(_))
        ));
        assert_eq!(
            match responses[2].as_ref().unwrap().content.as_ref().unwrap() {
                v2::receive_message_response::Content::Status(status) => status.code,
                _ => 0,
            },
            v2::Code::Ok as i32
        );
    }

    #[tokio::test]
    async fn ack_message_returns_entry_results() {
        let service = test_service_with_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            Arc::new(TestConsumerService),
        );
        let mut request = Request::new(v2::AckMessageRequest {
            group: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "GroupA".to_owned(),
            }),
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            entries: vec![v2::AckMessageEntry {
                message_id: "msg-1".to_owned(),
                receipt_handle: "handle-1".to_owned(),
                lite_topic: None,
            }],
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.ack_message(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert_eq!(response.entries.len(), 1);
        assert_eq!(response.entries[0].message_id, "msg-1");
    }

    #[tokio::test]
    async fn change_invisible_duration_returns_new_receipt_handle() {
        let service = test_service_with_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            Arc::new(TestConsumerService),
        );
        let mut request = Request::new(v2::ChangeInvisibleDurationRequest {
            group: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "GroupA".to_owned(),
            }),
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            receipt_handle: "handle-1".to_owned(),
            invisible_duration: Some(prost_types::Duration { seconds: 30, nanos: 0 }),
            message_id: "msg-1".to_owned(),
            lite_topic: None,
            suspend: None,
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.change_invisible_duration(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert_eq!(response.receipt_handle, "handle-1-renewed");
    }

    #[test]
    fn telemetry_settings_are_merged_with_proxy_bounds() {
        let service = test_service(StaticRouteService::default(), StaticMetadataService::default());
        let merged = service.merged_telemetry_settings(&v2::Settings {
            client_type: Some(v2::ClientType::PushConsumer as i32),
            access_point: None,
            backoff_policy: None,
            request_timeout: None,
            pub_sub: Some(v2::settings::PubSub::Subscription(v2::Subscription {
                group: Some(v2::Resource {
                    resource_namespace: String::new(),
                    name: "GroupA".to_owned(),
                }),
                subscriptions: Vec::new(),
                fifo: Some(true),
                receive_batch_size: Some(64),
                long_polling_timeout: Some(prost_types::Duration { seconds: 1, nanos: 0 }),
                lite_subscription_quota: None,
                max_lite_topic_size: None,
            })),
            user_agent: None,
            metric: None,
        });

        let subscription = match merged.pub_sub.unwrap() {
            v2::settings::PubSub::Subscription(subscription) => subscription,
            _ => panic!("expected subscription settings"),
        };
        assert_eq!(subscription.receive_batch_size, Some(64));
        assert_eq!(
            subscription.long_polling_timeout,
            Some(prost_types::Duration { seconds: 5, nanos: 0 })
        );
    }

    #[test]
    fn telemetry_settings_fill_lite_subscription_defaults_for_lite_clients() {
        let service = test_service(StaticRouteService::default(), StaticMetadataService::default());
        let merged = service.merged_telemetry_settings(&v2::Settings {
            client_type: Some(v2::ClientType::LitePushConsumer as i32),
            access_point: None,
            backoff_policy: None,
            request_timeout: None,
            pub_sub: Some(v2::settings::PubSub::Subscription(v2::Subscription {
                group: Some(v2::Resource {
                    resource_namespace: String::new(),
                    name: "GroupA".to_owned(),
                }),
                subscriptions: Vec::new(),
                fifo: Some(false),
                receive_batch_size: Some(8),
                long_polling_timeout: Some(prost_types::Duration { seconds: 6, nanos: 0 }),
                lite_subscription_quota: None,
                max_lite_topic_size: None,
            })),
            user_agent: None,
            metric: None,
        });

        let subscription = match merged.pub_sub.unwrap() {
            v2::settings::PubSub::Subscription(subscription) => subscription,
            _ => panic!("expected subscription settings"),
        };
        assert_eq!(subscription.lite_subscription_quota, Some(1200));
        assert_eq!(subscription.max_lite_topic_size, Some(64));
    }

    #[test]
    fn effective_receive_request_uses_telemetry_settings() {
        let service = test_service(StaticRouteService::default(), StaticMetadataService::default());
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));
        let context = service.context("ReceiveMessage", &request);

        let merged = service.merged_telemetry_settings(&v2::Settings {
            client_type: Some(v2::ClientType::PushConsumer as i32),
            access_point: None,
            backoff_policy: None,
            request_timeout: None,
            pub_sub: Some(v2::settings::PubSub::Subscription(v2::Subscription {
                group: Some(v2::Resource {
                    resource_namespace: String::new(),
                    name: "GroupA".to_owned(),
                }),
                subscriptions: Vec::new(),
                fifo: Some(true),
                receive_batch_size: Some(32),
                long_polling_timeout: Some(prost_types::Duration { seconds: 1, nanos: 0 }),
                lite_subscription_quota: None,
                max_lite_topic_size: None,
            })),
            user_agent: None,
            metric: None,
        });
        let _ = service.sessions.update_settings_from_telemetry(&context, &merged);

        let request = service
            .effective_receive_request(
                &context,
                adapter::build_receive_message_request(&v2::ReceiveMessageRequest {
                    group: Some(v2::Resource {
                        resource_namespace: String::new(),
                        name: "GroupA".to_owned(),
                    }),
                    message_queue: Some(v2::MessageQueue {
                        topic: Some(v2::Resource {
                            resource_namespace: String::new(),
                            name: "TopicA".to_owned(),
                        }),
                        id: 3,
                        permission: v2::Permission::ReadWrite as i32,
                        broker: None,
                        accept_message_types: vec![v2::MessageType::Normal as i32],
                    }),
                    filter_expression: None,
                    batch_size: 64,
                    invisible_duration: Some(prost_types::Duration { seconds: 30, nanos: 0 }),
                    auto_renew: false,
                    long_polling_timeout: Some(prost_types::Duration { seconds: 30, nanos: 0 }),
                    attempt_id: None,
                })
                .unwrap(),
            )
            .unwrap();

        assert_eq!(request.batch_size, 32);
        assert_eq!(request.long_polling_timeout, std::time::Duration::from_secs(5));
        assert!(request.target.fifo);
    }

    #[tokio::test]
    async fn receive_message_with_auto_renew_tracks_receipt_handles() {
        let service = test_service_with_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            Arc::new(TestConsumerService),
        );
        let mut request = Request::new(v2::ReceiveMessageRequest {
            group: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "GroupA".to_owned(),
            }),
            message_queue: Some(v2::MessageQueue {
                topic: Some(v2::Resource {
                    resource_namespace: String::new(),
                    name: "TopicA".to_owned(),
                }),
                id: 3,
                permission: v2::Permission::ReadWrite as i32,
                broker: None,
                accept_message_types: vec![v2::MessageType::Normal as i32],
            }),
            filter_expression: None,
            batch_size: 1,
            invisible_duration: Some(prost_types::Duration { seconds: 30, nanos: 0 }),
            auto_renew: true,
            long_polling_timeout: Some(prost_types::Duration { seconds: 1, nanos: 0 }),
            attempt_id: None,
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let mut stream = service.receive_message(request).await.unwrap().into_inner();
        let _ = stream.next().await;
        let _ = stream.next().await;
        let _ = stream.next().await;

        let tracked = service
            .sessions
            .tracked_receipt_handle(
                "client-a",
                &ResourceIdentity::new("", "GroupA"),
                &ResourceIdentity::new("", "TopicA"),
                "server-msg-id",
            )
            .expect("receipt handle should be tracked");
        assert_eq!(tracked.receipt_handle, "receipt-handle");
        tracked.cancellation.cancel();
    }

    #[tokio::test]
    async fn ack_message_success_clears_tracked_receipt_handle() {
        let service = test_service_with_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            Arc::new(TestConsumerService),
        );
        service
            .sessions
            .track_receipt_handle(crate::session::ReceiptHandleRegistration {
                client_id: "client-a".to_owned(),
                group: ResourceIdentity::new("", "GroupA"),
                topic: ResourceIdentity::new("", "TopicA"),
                message_id: "msg-1".to_owned(),
                receipt_handle: "handle-1".to_owned(),
                invisible_duration: std::time::Duration::from_secs(30),
            });

        let mut request = Request::new(v2::AckMessageRequest {
            group: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "GroupA".to_owned(),
            }),
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            entries: vec![v2::AckMessageEntry {
                message_id: "msg-1".to_owned(),
                receipt_handle: "handle-1".to_owned(),
                lite_topic: None,
            }],
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.ack_message(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert_eq!(service.sessions.tracked_handle_count(), 0);
    }

    #[tokio::test]
    async fn change_invisible_duration_updates_tracked_receipt_handle() {
        let service = test_service_with_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            Arc::new(TestConsumerService),
        );
        service
            .sessions
            .track_receipt_handle(crate::session::ReceiptHandleRegistration {
                client_id: "client-a".to_owned(),
                group: ResourceIdentity::new("", "GroupA"),
                topic: ResourceIdentity::new("", "TopicA"),
                message_id: "msg-1".to_owned(),
                receipt_handle: "handle-1".to_owned(),
                invisible_duration: std::time::Duration::from_secs(30),
            });

        let mut request = Request::new(v2::ChangeInvisibleDurationRequest {
            group: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "GroupA".to_owned(),
            }),
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            receipt_handle: "handle-1".to_owned(),
            invisible_duration: Some(prost_types::Duration { seconds: 45, nanos: 0 }),
            message_id: "msg-1".to_owned(),
            lite_topic: None,
            suspend: None,
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.change_invisible_duration(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        let tracked = service
            .sessions
            .tracked_receipt_handle(
                "client-a",
                &ResourceIdentity::new("", "GroupA"),
                &ResourceIdentity::new("", "TopicA"),
                "msg-1",
            )
            .expect("receipt handle should remain tracked");
        assert_eq!(tracked.receipt_handle, "handle-1-renewed");
        assert_eq!(tracked.invisible_duration, std::time::Duration::from_secs(45));
    }

    #[tokio::test]
    async fn notify_client_termination_clears_session_and_receipt_handles() {
        let service = test_service_with_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            Arc::new(TestConsumerService),
        );
        let mut heartbeat = Request::new(v2::HeartbeatRequest {
            group: None,
            client_type: v2::ClientType::SimpleConsumer as i32,
        });
        heartbeat
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));
        let _ = service.heartbeat(heartbeat).await.unwrap();
        service
            .sessions
            .track_receipt_handle(crate::session::ReceiptHandleRegistration {
                client_id: "client-a".to_owned(),
                group: ResourceIdentity::new("", "GroupA"),
                topic: ResourceIdentity::new("", "TopicA"),
                message_id: "msg-1".to_owned(),
                receipt_handle: "handle-1".to_owned(),
                invisible_duration: std::time::Duration::from_secs(30),
            });

        let mut request = Request::new(v2::NotifyClientTerminationRequest { group: None });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.notify_client_termination(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert!(service.sessions.get("client-a").is_none());
        assert_eq!(service.sessions.tracked_handle_count(), 0);
    }

    #[tokio::test]
    async fn sync_lite_subscription_updates_registry() {
        let service = test_service(StaticRouteService::default(), StaticMetadataService::default());
        let mut context_request = Request::new(());
        context_request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));
        let context = service.context("Telemetry", &context_request);
        let _ = service.sessions.update_settings_from_telemetry(
            &context,
            &service.merged_telemetry_settings(&v2::Settings {
                client_type: Some(v2::ClientType::LitePushConsumer as i32),
                access_point: None,
                backoff_policy: None,
                request_timeout: None,
                pub_sub: Some(v2::settings::PubSub::Subscription(v2::Subscription {
                    group: Some(v2::Resource {
                        resource_namespace: String::new(),
                        name: "GroupA".to_owned(),
                    }),
                    subscriptions: Vec::new(),
                    fifo: Some(false),
                    receive_batch_size: Some(8),
                    long_polling_timeout: Some(prost_types::Duration { seconds: 6, nanos: 0 }),
                    lite_subscription_quota: Some(2),
                    max_lite_topic_size: Some(64),
                })),
                user_agent: None,
                metric: None,
            }),
        );

        let mut request = Request::new(v2::SyncLiteSubscriptionRequest {
            action: v2::LiteSubscriptionAction::CompleteAdd as i32,
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            group: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "GroupA".to_owned(),
            }),
            lite_topic_set: vec!["lite-a".to_owned(), "lite-b".to_owned()],
            version: Some(1),
            offset_option: None,
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.sync_lite_subscription(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        let snapshot = service
            .sessions
            .lite_subscription(
                "client-a",
                &ResourceIdentity::new("", "GroupA"),
                &ResourceIdentity::new("", "TopicA"),
            )
            .expect("lite subscription should be tracked");
        assert_eq!(snapshot.lite_topic_set.len(), 2);
        assert!(snapshot.lite_topic_set.contains("lite-a"));
        assert!(snapshot.lite_topic_set.contains("lite-b"));
    }

    #[tokio::test]
    async fn sync_lite_subscription_rejects_quota_exceeded() {
        let service = test_service(StaticRouteService::default(), StaticMetadataService::default());
        let mut context_request = Request::new(());
        context_request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));
        let context = service.context("Telemetry", &context_request);
        let _ = service.sessions.update_settings_from_telemetry(
            &context,
            &service.merged_telemetry_settings(&v2::Settings {
                client_type: Some(v2::ClientType::LitePushConsumer as i32),
                access_point: None,
                backoff_policy: None,
                request_timeout: None,
                pub_sub: Some(v2::settings::PubSub::Subscription(v2::Subscription {
                    group: Some(v2::Resource {
                        resource_namespace: String::new(),
                        name: "GroupA".to_owned(),
                    }),
                    subscriptions: Vec::new(),
                    fifo: Some(false),
                    receive_batch_size: Some(8),
                    long_polling_timeout: Some(prost_types::Duration { seconds: 6, nanos: 0 }),
                    lite_subscription_quota: Some(1),
                    max_lite_topic_size: Some(64),
                })),
                user_agent: None,
                metric: None,
            }),
        );

        let mut request = Request::new(v2::SyncLiteSubscriptionRequest {
            action: v2::LiteSubscriptionAction::CompleteAdd as i32,
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            group: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "GroupA".to_owned(),
            }),
            lite_topic_set: vec!["lite-a".to_owned(), "lite-b".to_owned()],
            version: Some(1),
            offset_option: None,
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.sync_lite_subscription(request).await.unwrap().into_inner();
        assert_eq!(
            response.status.unwrap().code,
            v2::Code::LiteSubscriptionQuotaExceeded as i32
        );
    }
}
