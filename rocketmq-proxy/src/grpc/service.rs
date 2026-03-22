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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

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

use crate::auth;
use crate::auth::AuthenticatedPrincipal;
use crate::auth::ProxyAuthRuntime;
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
    next_reap_at_ms: Arc<AtomicU64>,
    auth_runtime: Option<Arc<ProxyAuthRuntime>>,
}

impl<P> Clone for ProxyGrpcService<P> {
    fn clone(&self) -> Self {
        Self {
            config: Arc::clone(&self.config),
            processor: Arc::clone(&self.processor),
            sessions: self.sessions.clone(),
            guards: self.guards.clone(),
            next_reap_at_ms: Arc::clone(&self.next_reap_at_ms),
            auth_runtime: self.auth_runtime.clone(),
        }
    }
}

impl<P> ProxyGrpcService<P> {
    pub fn new(config: Arc<ProxyConfig>, processor: Arc<P>, sessions: ClientSessionRegistry) -> Self {
        let interval_ms = Self::housekeeping_interval_from_config(config.as_ref())
            .as_millis()
            .clamp(1, u128::from(u64::MAX)) as u64;
        Self {
            guards: ExecutionGuards::from_config(config.as_ref()),
            config,
            processor,
            sessions,
            next_reap_at_ms: Arc::new(AtomicU64::new(current_epoch_millis().saturating_add(interval_ms))),
            auth_runtime: None,
        }
    }

    pub fn with_auth_runtime(mut self, auth_runtime: Option<ProxyAuthRuntime>) -> Self {
        self.auth_runtime = auth_runtime.map(Arc::new);
        self
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

    async fn authenticate_request<T: 'static>(
        &self,
        context: &mut ProxyContext,
        request: &Request<T>,
    ) -> ProxyResult<Option<AuthenticatedPrincipal>> {
        let principal = match self.auth_runtime.as_ref() {
            Some(auth_runtime) if auth_runtime.enabled() => {
                auth_runtime.authenticate_request(context.rpc_name(), request).await?
            }
            _ => None,
        };
        if let Some(principal) = principal.as_ref() {
            context.set_authenticated_principal(principal.clone());
        }
        Ok(principal)
    }

    async fn authorize_contexts(
        &self,
        context: &ProxyContext,
        principal: Option<&AuthenticatedPrincipal>,
        authorization_contexts: &[auth::AuthorizationContextSpec],
    ) -> ProxyResult<()> {
        match self.auth_runtime.as_ref() {
            Some(auth_runtime) if auth_runtime.enabled() => {
                auth_runtime
                    .authorize_request(context.rpc_name(), principal, authorization_contexts)
                    .await
            }
            _ => Ok(()),
        }
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
                    self.dispatch_due_prepared_transaction_recoveries();
                    self.schedule_next_reap();
                }
            }
        }
    }

    fn housekeeping_interval(&self) -> Duration {
        Self::housekeeping_interval_from_config(self.config.as_ref())
    }

    fn housekeeping_interval_from_config(config: &ProxyConfig) -> Duration {
        let min_ttl = config.session.client_ttl().min(config.session.receipt_handle_ttl());
        Duration::from_millis(min_ttl.as_millis().saturating_div(2).clamp(1_000, 30_000) as u64)
    }

    fn reap_session_state(&self) {
        let _ = self.sessions.reap_expired(
            self.config.session.client_ttl(),
            self.config.session.receipt_handle_ttl(),
        );
    }

    fn schedule_next_reap(&self) {
        let next = current_epoch_millis()
            .saturating_add(self.housekeeping_interval().as_millis().clamp(1, u128::from(u64::MAX)) as u64);
        self.next_reap_at_ms.store(next, Ordering::Relaxed);
    }

    fn reap_session_state_if_due(&self) {
        let now = current_epoch_millis();
        let next = self.next_reap_at_ms.load(Ordering::Relaxed);
        if now < next {
            return;
        }

        let interval_ms = self.housekeeping_interval().as_millis().clamp(1, u128::from(u64::MAX)) as u64;
        if self
            .next_reap_at_ms
            .compare_exchange(
                next,
                now.saturating_add(interval_ms),
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.reap_session_state();
        }
    }

    fn dispatch_due_prepared_transaction_recoveries(&self) {
        self.dispatch_due_prepared_transaction_recoveries_for_client(None);
    }

    fn dispatch_due_prepared_transaction_recoveries_for_client(&self, client_id: Option<&str>) {
        let now = SystemTime::now();
        let due_transactions = self.sessions.prepared_transactions_due_for_recovery(now);
        for tracked in due_transactions {
            if client_id.is_some_and(|expected| tracked.client_id != expected) {
                continue;
            }

            if self.queue_recover_orphaned_transaction_command(
                tracked.client_id.as_str(),
                tracked.message.clone(),
                tracked.transaction_id.clone(),
            ) {
                let _ = self.sessions.prepared_transaction(
                    tracked.client_id.as_str(),
                    tracked.transaction_id.as_str(),
                    tracked.message_id.as_str(),
                );
            }
        }
    }

    fn queue_recover_orphaned_transaction_command(
        &self,
        client_id: &str,
        message: v2::Message,
        transaction_id: impl Into<String>,
    ) -> bool {
        self.sessions.send_telemetry_command(
            client_id,
            v2::TelemetryCommand {
                status: Some(ProxyStatusMapper::ok()),
                command: Some(v2::telemetry_command::Command::RecoverOrphanedTransactionCommand(
                    v2::RecoverOrphanedTransactionCommand {
                        message: Some(message),
                        transaction_id: transaction_id.into(),
                    },
                )),
            },
        )
    }
}

impl<P> ProxyGrpcService<P>
where
    P: MessagingProcessor + 'static,
{
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

    fn telemetry_status(status: v2::Status) -> v2::TelemetryCommand {
        v2::TelemetryCommand {
            status: Some(status),
            command: None,
        }
    }

    pub fn send_reconnect_endpoints_command(&self, client_id: &str, nonce: impl Into<String>) -> bool {
        self.send_server_telemetry_command(
            client_id,
            v2::TelemetryCommand {
                status: Some(ProxyStatusMapper::ok()),
                command: Some(v2::telemetry_command::Command::ReconnectEndpointsCommand(
                    v2::ReconnectEndpointsCommand { nonce: nonce.into() },
                )),
            },
        )
    }

    pub fn send_print_thread_stack_trace_command(&self, client_id: &str, nonce: impl Into<String>) -> bool {
        self.send_server_telemetry_command(
            client_id,
            v2::TelemetryCommand {
                status: Some(ProxyStatusMapper::ok()),
                command: Some(v2::telemetry_command::Command::PrintThreadStackTraceCommand(
                    v2::PrintThreadStackTraceCommand { nonce: nonce.into() },
                )),
            },
        )
    }

    pub fn send_verify_message_command(&self, client_id: &str, nonce: impl Into<String>, message: v2::Message) -> bool {
        self.send_server_telemetry_command(
            client_id,
            v2::TelemetryCommand {
                status: Some(ProxyStatusMapper::ok()),
                command: Some(v2::telemetry_command::Command::VerifyMessageCommand(
                    v2::VerifyMessageCommand {
                        nonce: nonce.into(),
                        message: Some(message),
                    },
                )),
            },
        )
    }

    pub fn send_recover_orphaned_transaction_command(
        &self,
        client_id: &str,
        message: v2::Message,
        transaction_id: impl Into<String>,
    ) -> bool {
        self.send_server_telemetry_command(
            client_id,
            v2::TelemetryCommand {
                status: Some(ProxyStatusMapper::ok()),
                command: Some(v2::telemetry_command::Command::RecoverOrphanedTransactionCommand(
                    v2::RecoverOrphanedTransactionCommand {
                        message: Some(message),
                        transaction_id: transaction_id.into(),
                    },
                )),
            },
        )
    }

    pub fn send_notify_unsubscribe_lite_command(&self, client_id: &str, lite_topic: impl Into<String>) -> bool {
        self.send_server_telemetry_command(
            client_id,
            v2::TelemetryCommand {
                status: Some(ProxyStatusMapper::ok()),
                command: Some(v2::telemetry_command::Command::NotifyUnsubscribeLiteCommand(
                    v2::NotifyUnsubscribeLiteCommand {
                        lite_topic: lite_topic.into(),
                    },
                )),
            },
        )
    }

    fn send_server_telemetry_command(&self, client_id: &str, command: v2::TelemetryCommand) -> bool {
        self.sessions.send_telemetry_command(client_id, command)
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
        grpc_request: &v2::SendMessageRequest,
        request: &crate::processor::SendMessageRequest,
        plan: &crate::processor::SendMessagePlan,
    ) {
        let Some(client_id) = context.client_id() else {
            return;
        };
        let producer_group =
            crate::cluster::build_proxy_producer_group(&self.config.cluster, Some(client_id), context.request_id());

        for ((grpc_message, message), result) in grpc_request
            .messages
            .iter()
            .zip(request.messages.iter())
            .zip(plan.entries.iter())
        {
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
                    message: grpc_message.clone(),
                    orphaned_transaction_recovery_duration: grpc_message
                        .system_properties
                        .as_ref()
                        .and_then(|system| system.orphaned_transaction_recovery_duration.as_ref())
                        .and_then(proto_duration),
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
        principal: Option<&AuthenticatedPrincipal>,
        command: v2::TelemetryCommand,
    ) -> v2::TelemetryCommand {
        self.sessions.upsert_from_context(context);

        match command.command {
            Some(v2::telemetry_command::Command::Settings(settings)) => {
                let merged = self.merged_telemetry_settings(&settings);
                match self
                    .authorize_contexts(
                        context,
                        principal,
                        &auth::telemetry_command_contexts(&v2::TelemetryCommand {
                            status: None,
                            command: Some(v2::telemetry_command::Command::Settings(merged.clone())),
                        }),
                    )
                    .await
                {
                    Ok(()) => {
                        let _ = self.sessions.update_settings_from_telemetry(context, &merged);
                        v2::TelemetryCommand {
                            status: Some(ProxyStatusMapper::ok()),
                            command: Some(v2::telemetry_command::Command::Settings(merged)),
                        }
                    }
                    Err(error) => Self::telemetry_status(ProxyStatusMapper::from_error(&error)),
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

fn current_epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().clamp(0, u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
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
        self.reap_session_state_if_due();
        let mut context = self.context("QueryRoute", &request);
        let principal = match self.authenticate_request(&mut context, &request).await {
            Ok(principal) => principal,
            Err(error) => {
                return Ok(Response::new(adapter::error_query_route_response(
                    ProxyStatusMapper::from_error(&error),
                )));
            }
        };
        let request = request.into_inner();

        let response = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_query_route_request(self.config.as_ref(), &request))
        {
            Ok(input) => match self
                .authorize_contexts(&context, principal.as_ref(), &auth::query_route_contexts(&input))
                .await
            {
                Ok(()) => match self.guards.try_route() {
                    Ok(_permit) => match self.processor.query_route(&context, input).await {
                        Ok(plan) => adapter::build_query_route_response(&request, &plan),
                        Err(error) => adapter::error_query_route_response(ProxyStatusMapper::from_error(&error)),
                    },
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
        self.reap_session_state_if_due();
        let mut context = self.context("Heartbeat", &request);
        let principal = match self.authenticate_request(&mut context, &request).await {
            Ok(principal) => principal,
            Err(error) => {
                return Ok(Response::new(v2::HeartbeatResponse {
                    status: Some(ProxyStatusMapper::from_error(&error)),
                }));
            }
        };
        let request = request.into_inner();

        let status = match self.guards.try_client_manager() {
            Ok(_permit) => match self.validate_heartbeat_request(&context, request.client_type) {
                Ok(()) => match self
                    .authorize_contexts(&context, principal.as_ref(), &auth::heartbeat_contexts(&request))
                    .await
                {
                    Ok(()) => {
                        self.sessions
                            .upsert_from_context_with_client_type(&context, Some(request.client_type));
                        ProxyStatusMapper::ok()
                    }
                    Err(error) => ProxyStatusMapper::from_error(&error),
                },
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
        self.reap_session_state_if_due();
        let mut context = self.context("SendMessage", &request);
        let principal = match self.authenticate_request(&mut context, &request).await {
            Ok(principal) => principal,
            Err(error) => {
                return Ok(Response::new(adapter::error_send_message_response(
                    ProxyStatusMapper::from_error(&error),
                )));
            }
        };
        let request = request.into_inner();

        let response = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_send_message_request(&context, &request))
        {
            Ok(input) => match self
                .authorize_contexts(&context, principal.as_ref(), &auth::send_message_contexts(&input))
                .await
            {
                Ok(()) => match self.guards.try_producer() {
                    Ok(_permit) => match self.processor.send_message(&context, input.clone()).await {
                        Ok(plan) => {
                            self.track_prepared_transactions(&context, &request, &input, &plan);
                            adapter::build_send_message_response(&plan, &input)
                        }
                        Err(error) => adapter::error_send_message_response(ProxyStatusMapper::from_error(&error)),
                    },
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
        self.reap_session_state_if_due();
        let mut context = self.context("QueryAssignment", &request);
        let principal = match self.authenticate_request(&mut context, &request).await {
            Ok(principal) => principal,
            Err(error) => {
                return Ok(Response::new(adapter::error_query_assignment_response(
                    ProxyStatusMapper::from_error(&error),
                )));
            }
        };
        let request = request.into_inner();

        let response = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_query_assignment_request(self.config.as_ref(), &request))
        {
            Ok(input) => match self
                .authorize_contexts(&context, principal.as_ref(), &auth::query_assignment_contexts(&input))
                .await
            {
                Ok(()) => match self.guards.try_route() {
                    Ok(_permit) => match self.processor.query_assignment(&context, input).await {
                        Ok(plan) => adapter::build_query_assignment_response(&request, &plan),
                        Err(error) => adapter::error_query_assignment_response(ProxyStatusMapper::from_error(&error)),
                    },
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
        self.reap_session_state_if_due();
        let mut context = self.context("ReceiveMessage", &request);
        let principal = match self.authenticate_request(&mut context, &request).await {
            Ok(principal) => principal,
            Err(error) => {
                return Ok(Response::new(self.items_stream(
                    adapter::error_receive_message_responses(ProxyStatusMapper::from_error(&error)),
                )));
            }
        };
        let request = request.into_inner();
        let responses = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_receive_message_request(&request))
            .and_then(|input| self.effective_receive_request(&context, input))
        {
            Ok(input) => match self
                .authorize_contexts(&context, principal.as_ref(), &auth::receive_message_contexts(&input))
                .await
            {
                Ok(()) => match self.guards.try_consumer() {
                    Ok(_permit) => {
                        self.sessions.upsert_from_context(&context);
                        match self.processor.receive_message(&context, input.clone()).await {
                            Ok(plan) => {
                                self.track_received_receipt_handles(&context, &input, &plan);
                                adapter::build_receive_message_responses(&plan)
                            }
                            Err(error) => {
                                adapter::error_receive_message_responses(ProxyStatusMapper::from_error(&error))
                            }
                        }
                    }
                    Err(error) => adapter::error_receive_message_responses(ProxyStatusMapper::from_error(&error)),
                },
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
        self.reap_session_state_if_due();
        let mut context = self.context("AckMessage", &request);
        let principal = match self.authenticate_request(&mut context, &request).await {
            Ok(principal) => principal,
            Err(error) => {
                return Ok(Response::new(adapter::error_ack_message_response(
                    ProxyStatusMapper::from_error(&error),
                )));
            }
        };
        let request = request.into_inner();

        let response = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_ack_message_request(&request))
        {
            Ok(input) => match self
                .authorize_contexts(&context, principal.as_ref(), &auth::ack_message_contexts(&input))
                .await
            {
                Ok(()) => match self.guards.try_consumer() {
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
            },
            Err(error) => adapter::error_ack_message_response(ProxyStatusMapper::from_error(&error)),
        };

        Ok(Response::new(response))
    }

    async fn forward_message_to_dead_letter_queue(
        &self,
        request: Request<v2::ForwardMessageToDeadLetterQueueRequest>,
    ) -> Result<Response<v2::ForwardMessageToDeadLetterQueueResponse>, Status> {
        self.reap_session_state_if_due();
        let mut context = self.context("ForwardMessageToDeadLetterQueue", &request);
        let principal = match self.authenticate_request(&mut context, &request).await {
            Ok(principal) => principal,
            Err(error) => {
                return Ok(Response::new(
                    adapter::error_forward_message_to_dead_letter_queue_response(ProxyStatusMapper::from_error(&error)),
                ));
            }
        };
        let request = request.into_inner();

        let response = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_forward_message_to_dead_letter_queue_request(&request))
        {
            Ok(input) => match self
                .authorize_contexts(
                    &context,
                    principal.as_ref(),
                    &auth::forward_message_to_dead_letter_queue_contexts(&input),
                )
                .await
            {
                Ok(()) => match self.guards.try_consumer() {
                    Ok(_permit) => match self
                        .processor
                        .forward_message_to_dead_letter_queue(&context, input)
                        .await
                    {
                        Ok(plan) => adapter::build_forward_message_to_dead_letter_queue_response(&plan),
                        Err(error) => adapter::error_forward_message_to_dead_letter_queue_response(
                            ProxyStatusMapper::from_error(&error),
                        ),
                    },
                    Err(error) => adapter::error_forward_message_to_dead_letter_queue_response(
                        ProxyStatusMapper::from_error(&error),
                    ),
                },
                Err(error) => {
                    adapter::error_forward_message_to_dead_letter_queue_response(ProxyStatusMapper::from_error(&error))
                }
            },
            Err(error) => {
                adapter::error_forward_message_to_dead_letter_queue_response(ProxyStatusMapper::from_error(&error))
            }
        };

        Ok(Response::new(response))
    }

    async fn pull_message(
        &self,
        request: Request<v2::PullMessageRequest>,
    ) -> Result<Response<Self::PullMessageStream>, Status> {
        self.reap_session_state_if_due();
        let mut context = self.context("PullMessage", &request);
        let principal = match self.authenticate_request(&mut context, &request).await {
            Ok(principal) => principal,
            Err(error) => {
                return Ok(Response::new(self.items_stream(adapter::error_pull_message_responses(
                    ProxyStatusMapper::from_error(&error),
                ))));
            }
        };
        let request = request.into_inner();
        let responses = match adapter::build_pull_message_request(&request) {
            Ok(input) => match self
                .validate_client_context(&context)
                .and_then(|_| self.guards.try_consumer().map(|_| ()))
            {
                Ok(_permit) => match self
                    .authorize_contexts(&context, principal.as_ref(), &auth::pull_message_contexts(&input))
                    .await
                {
                    Ok(()) => {
                        self.sessions.upsert_from_context(&context);
                        match self.processor.pull_message(&context, input).await {
                            Ok(plan) => adapter::build_pull_message_responses(&plan),
                            Err(error) => adapter::error_pull_message_responses(ProxyStatusMapper::from_error(&error)),
                        }
                    }
                    Err(error) => adapter::error_pull_message_responses(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_pull_message_responses(ProxyStatusMapper::from_error(&error)),
            },
            Err(error) => adapter::error_pull_message_responses(ProxyStatusMapper::from_error(&error)),
        };
        Ok(Response::new(self.items_stream(responses)))
    }

    async fn update_offset(
        &self,
        request: Request<v2::UpdateOffsetRequest>,
    ) -> Result<Response<v2::UpdateOffsetResponse>, Status> {
        self.reap_session_state_if_due();
        let mut context = self.context("UpdateOffset", &request);
        let principal = match self.authenticate_request(&mut context, &request).await {
            Ok(principal) => principal,
            Err(error) => {
                return Ok(Response::new(adapter::error_update_offset_response(
                    ProxyStatusMapper::from_error(&error),
                )));
            }
        };
        let request = request.into_inner();
        let response = match adapter::build_update_offset_request(&request) {
            Ok(input) => match self
                .validate_client_context(&context)
                .and_then(|_| self.guards.try_consumer().map(|_| ()))
            {
                Ok(_permit) => match self
                    .authorize_contexts(&context, principal.as_ref(), &auth::update_offset_contexts(&input))
                    .await
                {
                    Ok(()) => match self.processor.update_offset(&context, input).await {
                        Ok(plan) => adapter::build_update_offset_response(&plan),
                        Err(error) => adapter::error_update_offset_response(ProxyStatusMapper::from_error(&error)),
                    },
                    Err(error) => adapter::error_update_offset_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_update_offset_response(ProxyStatusMapper::from_error(&error)),
            },
            Err(error) => adapter::error_update_offset_response(ProxyStatusMapper::from_error(&error)),
        };
        Ok(Response::new(response))
    }

    async fn get_offset(
        &self,
        request: Request<v2::GetOffsetRequest>,
    ) -> Result<Response<v2::GetOffsetResponse>, Status> {
        self.reap_session_state_if_due();
        let mut context = self.context("GetOffset", &request);
        let principal = match self.authenticate_request(&mut context, &request).await {
            Ok(principal) => principal,
            Err(error) => {
                return Ok(Response::new(adapter::error_get_offset_response(
                    ProxyStatusMapper::from_error(&error),
                )));
            }
        };
        let request = request.into_inner();
        let response = match adapter::build_get_offset_request(&request) {
            Ok(input) => match self
                .validate_client_context(&context)
                .and_then(|_| self.guards.try_consumer().map(|_| ()))
            {
                Ok(_permit) => match self
                    .authorize_contexts(&context, principal.as_ref(), &auth::get_offset_contexts(&input))
                    .await
                {
                    Ok(()) => match self.processor.get_offset(&context, input).await {
                        Ok(plan) => adapter::build_get_offset_response(&plan),
                        Err(error) => adapter::error_get_offset_response(ProxyStatusMapper::from_error(&error)),
                    },
                    Err(error) => adapter::error_get_offset_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_get_offset_response(ProxyStatusMapper::from_error(&error)),
            },
            Err(error) => adapter::error_get_offset_response(ProxyStatusMapper::from_error(&error)),
        };
        Ok(Response::new(response))
    }

    async fn query_offset(
        &self,
        request: Request<v2::QueryOffsetRequest>,
    ) -> Result<Response<v2::QueryOffsetResponse>, Status> {
        self.reap_session_state_if_due();
        let mut context = self.context("QueryOffset", &request);
        let principal = match self.authenticate_request(&mut context, &request).await {
            Ok(principal) => principal,
            Err(error) => {
                return Ok(Response::new(adapter::error_query_offset_response(
                    ProxyStatusMapper::from_error(&error),
                )));
            }
        };
        let request = request.into_inner();
        let response = match adapter::build_query_offset_request(&request) {
            Ok(input) => match self
                .validate_client_context(&context)
                .and_then(|_| self.guards.try_consumer().map(|_| ()))
            {
                Ok(_permit) => match self
                    .authorize_contexts(&context, principal.as_ref(), &auth::query_offset_contexts(&input))
                    .await
                {
                    Ok(()) => match self.processor.query_offset(&context, input).await {
                        Ok(plan) => adapter::build_query_offset_response(&plan),
                        Err(error) => adapter::error_query_offset_response(ProxyStatusMapper::from_error(&error)),
                    },
                    Err(error) => adapter::error_query_offset_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_query_offset_response(ProxyStatusMapper::from_error(&error)),
            },
            Err(error) => adapter::error_query_offset_response(ProxyStatusMapper::from_error(&error)),
        };
        Ok(Response::new(response))
    }

    async fn end_transaction(
        &self,
        request: Request<v2::EndTransactionRequest>,
    ) -> Result<Response<v2::EndTransactionResponse>, Status> {
        self.reap_session_state_if_due();
        let mut context = self.context("EndTransaction", &request);
        let principal = match self.authenticate_request(&mut context, &request).await {
            Ok(principal) => principal,
            Err(error) => {
                return Ok(Response::new(adapter::error_end_transaction_response(
                    ProxyStatusMapper::from_error(&error),
                )));
            }
        };
        let request = request.into_inner();

        let response = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_end_transaction_request(&request))
            .and_then(|mut input| {
                self.enrich_end_transaction_request(&context, &mut input)?;
                Ok(input)
            }) {
            Ok(input) => match self
                .authorize_contexts(&context, principal.as_ref(), &auth::end_transaction_contexts(&input))
                .await
            {
                Ok(()) => match self.guards.try_producer() {
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
            },
            Err(error) => adapter::error_end_transaction_response(ProxyStatusMapper::from_error(&error)),
        };

        Ok(Response::new(response))
    }

    async fn telemetry(
        &self,
        request: Request<tonic::Streaming<v2::TelemetryCommand>>,
    ) -> Result<Response<Self::TelemetryStream>, Status> {
        self.reap_session_state_if_due();
        let mut context = self.context("Telemetry", &request);
        let principal = match self.authenticate_request(&mut context, &request).await {
            Ok(principal) => principal,
            Err(error) => {
                return Ok(Response::new(
                    self.status_stream(Self::telemetry_status(ProxyStatusMapper::from_error(&error))),
                ));
            }
        };
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
        let Some(client_id) = context.client_id().map(ToOwned::to_owned) else {
            return Ok(Response::new(self.status_stream(Self::telemetry_status(
                ProxyStatusMapper::from_error(&ProxyError::ClientIdRequired),
            ))));
        };
        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();
        self.sessions.bind_telemetry_link(client_id.clone(), outbound_tx);
        self.dispatch_due_prepared_transaction_recoveries_for_client(Some(client_id.as_str()));
        let service = self.clone();
        let (sender, receiver) = mpsc::channel(16);
        tokio::spawn(async move {
            let _permit = permit;
            loop {
                tokio::select! {
                    outbound = outbound_rx.recv() => {
                        match outbound {
                            Some(command) => {
                                if sender.send(Ok(command)).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                    inbound_item = inbound.next() => {
                        match inbound_item {
                            Some(Ok(command)) => {
                                let response = service
                                    .handle_telemetry_command(&context, principal.as_ref(), command)
                                    .await;
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
                }
            }
            service.sessions.unbind_telemetry_link(client_id.as_str());
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    async fn notify_client_termination(
        &self,
        request: Request<v2::NotifyClientTerminationRequest>,
    ) -> Result<Response<v2::NotifyClientTerminationResponse>, Status> {
        self.reap_session_state_if_due();
        let mut context = self.context("NotifyClientTermination", &request);
        let principal = match self.authenticate_request(&mut context, &request).await {
            Ok(principal) => principal,
            Err(error) => {
                return Ok(Response::new(v2::NotifyClientTerminationResponse {
                    status: Some(ProxyStatusMapper::from_error(&error)),
                }));
            }
        };
        let request = request.into_inner();
        let status = match self
            .guards
            .try_client_manager()
            .and_then(|_| self.validate_client_context(&context))
        {
            Ok(client_id) => match self
                .authorize_contexts(
                    &context,
                    principal.as_ref(),
                    &auth::notify_client_termination_contexts(&request),
                )
                .await
            {
                Ok(()) => {
                    self.sessions.remove_client(client_id);
                    ProxyStatusMapper::ok()
                }
                Err(error) => ProxyStatusMapper::from_error(&error),
            },
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
        self.reap_session_state_if_due();
        let mut context = self.context("ChangeInvisibleDuration", &request);
        let principal = match self.authenticate_request(&mut context, &request).await {
            Ok(principal) => principal,
            Err(error) => {
                return Ok(Response::new(adapter::error_change_invisible_duration_response(
                    ProxyStatusMapper::from_error(&error),
                )));
            }
        };
        let request = request.into_inner();

        let response = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_change_invisible_duration_request(&request))
        {
            Ok(input) => match self
                .authorize_contexts(
                    &context,
                    principal.as_ref(),
                    &auth::change_invisible_duration_contexts(&input),
                )
                .await
            {
                Ok(()) => match self.guards.try_consumer() {
                    Ok(_permit) => match self.processor.change_invisible_duration(&context, input.clone()).await {
                        Ok(plan) => {
                            self.reconcile_change_invisible_result(&context, &input, &plan);
                            adapter::build_change_invisible_duration_response(&plan)
                        }
                        Err(error) => {
                            adapter::error_change_invisible_duration_response(ProxyStatusMapper::from_error(&error))
                        }
                    },
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
        request: Request<v2::RecallMessageRequest>,
    ) -> Result<Response<v2::RecallMessageResponse>, Status> {
        self.reap_session_state_if_due();
        let mut context = self.context("RecallMessage", &request);
        let principal = match self.authenticate_request(&mut context, &request).await {
            Ok(principal) => principal,
            Err(error) => {
                return Ok(Response::new(adapter::error_recall_message_response(
                    ProxyStatusMapper::from_error(&error),
                )));
            }
        };
        let request = request.into_inner();

        let response = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_recall_message_request(&request))
        {
            Ok(input) => match self
                .authorize_contexts(&context, principal.as_ref(), &auth::recall_message_contexts(&input))
                .await
            {
                Ok(()) => match self.guards.try_producer() {
                    Ok(_permit) => match self.processor.recall_message(&context, input).await {
                        Ok(plan) => adapter::build_recall_message_response(&plan),
                        Err(error) => adapter::error_recall_message_response(ProxyStatusMapper::from_error(&error)),
                    },
                    Err(error) => adapter::error_recall_message_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_recall_message_response(ProxyStatusMapper::from_error(&error)),
            },
            Err(error) => adapter::error_recall_message_response(ProxyStatusMapper::from_error(&error)),
        };

        Ok(Response::new(response))
    }

    async fn sync_lite_subscription(
        &self,
        request: Request<v2::SyncLiteSubscriptionRequest>,
    ) -> Result<Response<v2::SyncLiteSubscriptionResponse>, Status> {
        self.reap_session_state_if_due();
        let mut context = self.context("SyncLiteSubscription", &request);
        let principal = match self.authenticate_request(&mut context, &request).await {
            Ok(principal) => principal,
            Err(error) => {
                return Ok(Response::new(v2::SyncLiteSubscriptionResponse {
                    status: Some(ProxyStatusMapper::from_error(&error)),
                }));
            }
        };
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
            Ok(status) => match self
                .authorize_contexts(
                    &context,
                    principal.as_ref(),
                    &auth::sync_lite_subscription_contexts(&request),
                )
                .await
            {
                Ok(()) => status,
                Err(error) => ProxyStatusMapper::from_error(&error),
            },
            Err(error) => ProxyStatusMapper::from_error(&error),
        };

        Ok(Response::new(v2::SyncLiteSubscriptionResponse { status: Some(status) }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use futures::StreamExt;
    use hmac::Hmac;
    use hmac::Mac;
    use rocketmq_auth::authentication::enums::user_status::UserStatus;
    use rocketmq_auth::authentication::enums::user_type::UserType;
    use rocketmq_auth::authentication::model::user::User;
    use rocketmq_auth::authorization::enums::decision::Decision;
    use rocketmq_auth::authorization::model::acl::Acl;
    use rocketmq_auth::authorization::model::policy::Policy;
    use rocketmq_auth::authorization::model::resource::Resource;
    use rocketmq_client_rust::producer::send_result::SendResult;
    use rocketmq_client_rust::producer::send_status::SendStatus;
    use rocketmq_common::common::action::Action;
    use rocketmq_common::common::message::message_ext::MessageExt;
    use rocketmq_common::common::message::MessageConst;
    use rocketmq_common::common::message::MessageTrait;
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
    use rocketmq_remoting::protocol::route::route_data_view::QueueData;
    use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
    use sha1::Sha1;
    use tokio::sync::mpsc;
    use tonic::metadata::MetadataValue;
    use tonic::Request;

    use super::ProxyGrpcService;
    use crate::auth::ProxyAuthRuntime;
    use crate::config::ProxyAuthConfig;
    use crate::config::ProxyConfig;
    use crate::grpc::adapter;
    use crate::processor::AckMessageRequest;
    use crate::processor::AckMessageResultEntry;
    use crate::processor::ChangeInvisibleDurationPlan;
    use crate::processor::ChangeInvisibleDurationRequest;
    use crate::processor::DefaultMessagingProcessor;
    use crate::processor::EndTransactionPlan;
    use crate::processor::EndTransactionRequest;
    use crate::processor::ForwardMessageToDeadLetterQueuePlan;
    use crate::processor::ForwardMessageToDeadLetterQueueRequest;
    use crate::processor::GetOffsetPlan;
    use crate::processor::GetOffsetRequest;
    use crate::processor::PullMessagePlan;
    use crate::processor::PullMessageRequest;
    use crate::processor::QueryOffsetPlan;
    use crate::processor::QueryOffsetPolicy;
    use crate::processor::QueryOffsetRequest;
    use crate::processor::ReceiveMessagePlan;
    use crate::processor::ReceiveMessageRequest;
    use crate::processor::ReceivedMessage;
    use crate::processor::SendMessageRequest;
    use crate::processor::SendMessageResultEntry;
    use crate::processor::UpdateOffsetPlan;
    use crate::processor::UpdateOffsetRequest;
    use crate::proto::v2;
    use crate::proto::v2::messaging_service_server::MessagingService;
    use crate::service::ClusterServiceManager;
    use crate::service::ConsumerService;
    use crate::service::DefaultConsumerService;
    use crate::service::DefaultTransactionService;
    use crate::service::MessageService;
    use crate::service::ProxyTopicMessageType;
    use crate::service::ResourceIdentity;
    use crate::service::StaticMessageService;
    use crate::service::StaticMetadataService;
    use crate::service::StaticRouteService;
    use crate::service::SubscriptionGroupMetadata;
    use crate::service::TransactionService;
    use crate::session::ClientSessionRegistry;
    use crate::status::ProxyPayloadStatus;
    use crate::status::ProxyStatusMapper;
    use crate::PreparedTransactionRegistration;

    struct PartialMessageService;

    #[derive(Default)]
    struct TestConsumerService {
        dlq_requests: Mutex<Vec<ForwardMessageToDeadLetterQueueRequest>>,
        updated_offsets: Mutex<Vec<UpdateOffsetRequest>>,
    }

    #[derive(Default)]
    struct TestTransactionService {
        requests: Mutex<Vec<EndTransactionRequest>>,
    }

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

        async fn recall_message(
            &self,
            _context: &crate::context::ProxyContext,
            request: &crate::processor::RecallMessageRequest,
        ) -> crate::error::ProxyResult<crate::processor::RecallMessagePlan> {
            Ok(crate::processor::RecallMessagePlan {
                status: ProxyStatusMapper::ok_payload(),
                message_id: request.recall_handle.clone(),
            })
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

        async fn pull_message(
            &self,
            _context: &crate::context::ProxyContext,
            request: &PullMessageRequest,
        ) -> crate::error::ProxyResult<PullMessagePlan> {
            let mut message = MessageExt::default();
            message.set_topic(CheetahString::from(request.target.topic.to_string()));
            message.set_body(Bytes::from_static(b"pull"));
            message.set_msg_id(CheetahString::from("pull-msg-id"));
            message.set_queue_id(request.target.queue_id);
            message.set_queue_offset(request.offset);

            Ok(PullMessagePlan {
                status: ProxyStatusMapper::ok_payload(),
                next_offset: request.offset + 1,
                messages: vec![message],
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

        async fn forward_message_to_dead_letter_queue(
            &self,
            _context: &crate::context::ProxyContext,
            request: &ForwardMessageToDeadLetterQueueRequest,
        ) -> crate::error::ProxyResult<ForwardMessageToDeadLetterQueuePlan> {
            self.dlq_requests
                .lock()
                .expect("dlq requests mutex poisoned")
                .push(request.clone());
            Ok(ForwardMessageToDeadLetterQueuePlan {
                status: ProxyStatusMapper::ok_payload(),
            })
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

        async fn update_offset(
            &self,
            _context: &crate::context::ProxyContext,
            request: &UpdateOffsetRequest,
        ) -> crate::error::ProxyResult<UpdateOffsetPlan> {
            self.updated_offsets
                .lock()
                .expect("updated offsets mutex poisoned")
                .push(request.clone());
            Ok(UpdateOffsetPlan {
                status: ProxyStatusMapper::ok_payload(),
            })
        }

        async fn get_offset(
            &self,
            _context: &crate::context::ProxyContext,
            _request: &GetOffsetRequest,
        ) -> crate::error::ProxyResult<GetOffsetPlan> {
            Ok(GetOffsetPlan {
                status: ProxyStatusMapper::ok_payload(),
                offset: 42,
            })
        }

        async fn query_offset(
            &self,
            _context: &crate::context::ProxyContext,
            request: &QueryOffsetRequest,
        ) -> crate::error::ProxyResult<QueryOffsetPlan> {
            let offset = match request.policy {
                QueryOffsetPolicy::Beginning => 0,
                QueryOffsetPolicy::End => 128,
                QueryOffsetPolicy::Timestamp => request.timestamp_ms.unwrap_or_default(),
            };
            Ok(QueryOffsetPlan {
                status: ProxyStatusMapper::ok_payload(),
                offset,
            })
        }
    }

    #[async_trait]
    impl TransactionService for TestTransactionService {
        async fn end_transaction(
            &self,
            _context: &crate::context::ProxyContext,
            request: &EndTransactionRequest,
        ) -> crate::error::ProxyResult<EndTransactionPlan> {
            self.requests
                .lock()
                .expect("transaction service mutex poisoned")
                .push(request.clone());
            Ok(EndTransactionPlan {
                status: ProxyStatusMapper::ok_payload(),
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

    fn test_service_with_all_services(
        route_service: StaticRouteService,
        metadata_service: StaticMetadataService,
        message_service: Arc<dyn crate::service::MessageService>,
        consumer_service: Arc<dyn crate::service::ConsumerService>,
        transaction_service: Arc<dyn crate::service::TransactionService>,
    ) -> ProxyGrpcService<DefaultMessagingProcessor> {
        let manager = ClusterServiceManager::with_services(
            Arc::new(route_service),
            Arc::new(metadata_service),
            Arc::new(crate::service::DefaultAssignmentService),
            message_service,
            consumer_service,
            transaction_service,
        );
        let processor = Arc::new(DefaultMessagingProcessor::new(Arc::new(manager)));
        ProxyGrpcService::new(
            Arc::new(ProxyConfig::default()),
            processor,
            ClientSessionRegistry::default(),
        )
    }

    fn test_service_with_services(
        route_service: StaticRouteService,
        metadata_service: StaticMetadataService,
        message_service: Arc<dyn crate::service::MessageService>,
        consumer_service: Arc<dyn crate::service::ConsumerService>,
    ) -> ProxyGrpcService<DefaultMessagingProcessor> {
        test_service_with_all_services(
            route_service,
            metadata_service,
            message_service,
            consumer_service,
            Arc::new(DefaultTransactionService),
        )
    }

    const AUTH_TEST_DATETIME: &str = "20260322T010203Z";

    async fn test_auth_runtime(authentication_enabled: bool, authorization_enabled: bool) -> ProxyAuthRuntime {
        ProxyAuthRuntime::from_proxy_config(&ProxyAuthConfig {
            authentication_enabled,
            authorization_enabled,
            auth_config_path: format!("target/proxy-auth-tests-{}", uuid::Uuid::new_v4()),
            ..ProxyAuthConfig::default()
        })
        .await
        .expect("auth runtime should build")
        .expect("auth runtime should be enabled")
    }

    async fn seed_normal_user(auth_runtime: &ProxyAuthRuntime, username: &str, secret: &str) {
        let mut user = User::of_with_type(username, secret, UserType::Normal);
        user.set_user_status(UserStatus::Enable);
        auth_runtime.create_user(user).await.expect("user should be created");
    }

    async fn allow_topic_actions(auth_runtime: &ProxyAuthRuntime, username: &str, topic: &str, actions: Vec<Action>) {
        auth_runtime
            .create_acl(Acl::of(
                username,
                rocketmq_auth::authentication::enums::subject_type::SubjectType::User,
                Policy::of(vec![Resource::of_topic(topic)], actions, None, Decision::Allow),
            ))
            .await
            .expect("acl should be created");
    }

    async fn allow_group_actions(auth_runtime: &ProxyAuthRuntime, username: &str, group: &str, actions: Vec<Action>) {
        auth_runtime
            .create_acl(Acl::of(
                username,
                rocketmq_auth::authentication::enums::subject_type::SubjectType::User,
                Policy::of(
                    vec![Resource::of_group(group.to_owned())],
                    actions,
                    None,
                    Decision::Allow,
                ),
            ))
            .await
            .expect("group acl should be created");
    }

    fn apply_auth_headers<T>(request: &mut Request<T>, client_id: &str, username: &str, secret: &str) {
        type HmacSha1 = Hmac<Sha1>;

        let mut mac = HmacSha1::new_from_slice(secret.as_bytes()).expect("test hmac should build");
        mac.update(AUTH_TEST_DATETIME.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());
        let authorization =
            format!("MQv2-HMAC-SHA1 Credential={username}, SignedHeaders=x-mq-date-time, Signature={signature}",);

        request.metadata_mut().insert(
            "x-mq-client-id",
            MetadataValue::try_from(client_id).expect("client id metadata"),
        );
        request
            .metadata_mut()
            .insert("x-mq-date-time", MetadataValue::from_static(AUTH_TEST_DATETIME));
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(authorization.as_str()).expect("auth metadata"),
        );
        request
            .metadata_mut()
            .insert("channel-id", MetadataValue::from_static("auth-channel"));
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
    async fn query_route_returns_unauthorized_when_authentication_fails() {
        let route_service = StaticRouteService::default();
        route_service.insert(ResourceIdentity::new("", "TopicA"), sample_route());
        let auth_runtime = test_auth_runtime(true, false).await;
        let service =
            test_service(route_service, StaticMetadataService::default()).with_auth_runtime(Some(auth_runtime));

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
        assert_eq!(response.status.unwrap().code, v2::Code::Unauthorized as i32);
    }

    #[tokio::test]
    async fn query_route_accepts_valid_grpc_authentication_headers() {
        let route_service = StaticRouteService::default();
        route_service.insert(ResourceIdentity::new("", "TopicA"), sample_route());
        let auth_runtime = test_auth_runtime(true, false).await;
        seed_normal_user(&auth_runtime, "alice", "secret").await;
        let service =
            test_service(route_service, StaticMetadataService::default()).with_auth_runtime(Some(auth_runtime));

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
        apply_auth_headers(&mut request, "client-a", "alice", "secret");

        let response = service.query_route(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
    }

    #[tokio::test]
    async fn query_route_returns_forbidden_when_acl_denies_topic_access() {
        let route_service = StaticRouteService::default();
        route_service.insert(ResourceIdentity::new("", "TopicA"), sample_route());
        let auth_runtime = test_auth_runtime(true, true).await;
        seed_normal_user(&auth_runtime, "alice", "secret").await;
        allow_topic_actions(
            &auth_runtime,
            "alice",
            "TopicB",
            vec![Action::Pub, Action::Sub, Action::Get],
        )
        .await;
        let service =
            test_service(route_service, StaticMetadataService::default()).with_auth_runtime(Some(auth_runtime));

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
        apply_auth_headers(&mut request, "client-a", "alice", "secret");

        let response = service.query_route(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Forbidden as i32);
    }

    #[tokio::test]
    async fn telemetry_settings_returns_forbidden_when_acl_denies_subscription_topic() {
        let auth_runtime = test_auth_runtime(true, true).await;
        seed_normal_user(&auth_runtime, "alice", "secret").await;
        allow_group_actions(&auth_runtime, "alice", "GroupA", vec![Action::Sub]).await;
        let service = test_service(StaticRouteService::default(), StaticMetadataService::default())
            .with_auth_runtime(Some(auth_runtime));

        let mut auth_request = Request::new(());
        apply_auth_headers(&mut auth_request, "client-a", "alice", "secret");
        let mut context = service.context("Telemetry", &auth_request);
        let principal = service
            .authenticate_request(&mut context, &auth_request)
            .await
            .expect("authentication should succeed");
        assert_eq!(
            context
                .authenticated_principal()
                .expect("principal should be attached to context")
                .username(),
            "alice"
        );

        let response = service
            .handle_telemetry_command(
                &context,
                principal.as_ref(),
                v2::TelemetryCommand {
                    status: None,
                    command: Some(v2::telemetry_command::Command::Settings(v2::Settings {
                        client_type: Some(v2::ClientType::PushConsumer as i32),
                        access_point: None,
                        backoff_policy: None,
                        request_timeout: None,
                        pub_sub: Some(v2::settings::PubSub::Subscription(v2::Subscription {
                            group: Some(v2::Resource {
                                resource_namespace: String::new(),
                                name: "GroupA".to_owned(),
                            }),
                            subscriptions: vec![v2::SubscriptionEntry {
                                topic: Some(v2::Resource {
                                    resource_namespace: String::new(),
                                    name: "TopicA".to_owned(),
                                }),
                                expression: None,
                            }],
                            fifo: Some(false),
                            receive_batch_size: Some(16),
                            long_polling_timeout: Some(prost_types::Duration { seconds: 5, nanos: 0 }),
                            lite_subscription_quota: None,
                            max_lite_topic_size: None,
                        })),
                        user_agent: None,
                        metric: None,
                    })),
                },
            )
            .await;

        assert_eq!(response.status.unwrap().code, v2::Code::Forbidden as i32);
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
    async fn recall_message_returns_recalled_message_id() {
        let service = test_service_with_message_service(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
        );
        let mut request = Request::new(v2::RecallMessageRequest {
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            recall_handle: "recall-handle-1".to_owned(),
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.recall_message(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert_eq!(response.message_id, "recall-handle-1");
    }

    #[tokio::test]
    async fn send_message_tracks_prepared_transactions_for_transactional_entries() {
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
                    message_type: v2::MessageType::Transaction as i32,
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
        assert_eq!(service.sessions.prepared_transaction_count(), 1);
        assert_eq!(response.entries[0].transaction_id, "tx-msg-1");

        let tracked = service
            .sessions
            .prepared_transaction("client-a", "tx-msg-1", "msg-1")
            .expect("transaction should be tracked in session registry");
        assert_eq!(tracked.producer_group, "PROXY_SEND-client-a");
        assert_eq!(tracked.transaction_state_table_offset, 0);
        assert_eq!(tracked.commit_log_message_id, "offset-msg-1");
        assert_eq!(
            tracked
                .message
                .system_properties
                .as_ref()
                .map(|properties| properties.message_id.as_str()),
            Some("msg-1")
        );
    }

    #[tokio::test]
    async fn due_prepared_transaction_dispatches_recovery_command() {
        let service = test_service(StaticRouteService::default(), StaticMetadataService::default());
        let (sender, mut receiver) = mpsc::unbounded_channel();
        service.sessions.bind_telemetry_link("client-a", sender);
        service
            .sessions
            .track_prepared_transaction(PreparedTransactionRegistration {
                client_id: "client-a".to_owned(),
                topic: ResourceIdentity::new("", "TopicA"),
                message_id: "msg-1".to_owned(),
                transaction_id: "tx-1".to_owned(),
                producer_group: "PROXY_SEND-client-a".to_owned(),
                transaction_state_table_offset: 7,
                commit_log_message_id: "offset-msg-1".to_owned(),
                message: v2::Message {
                    topic: Some(v2::Resource {
                        resource_namespace: String::new(),
                        name: "TopicA".to_owned(),
                    }),
                    user_properties: HashMap::new(),
                    system_properties: Some(v2::SystemProperties {
                        message_id: "msg-1".to_owned(),
                        orphaned_transaction_recovery_duration: Some(prost_types::Duration { seconds: 0, nanos: 0 }),
                        ..Default::default()
                    }),
                    body: Bytes::from_static(b"hello").to_vec(),
                },
                orphaned_transaction_recovery_duration: Some(std::time::Duration::ZERO),
            });

        service.dispatch_due_prepared_transaction_recoveries();

        let command = receiver.recv().await.expect("recovery command should be queued");
        match command.command {
            Some(v2::telemetry_command::Command::RecoverOrphanedTransactionCommand(command)) => {
                assert_eq!(command.transaction_id, "tx-1");
                let message = command.message.expect("recovery command should carry message");
                assert_eq!(
                    message
                        .system_properties
                        .as_ref()
                        .map(|properties| properties.message_id.as_str()),
                    Some("msg-1")
                );
            }
            other => panic!("unexpected telemetry command: {other:?}"),
        }
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
            Arc::new(TestConsumerService::default()),
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
            Arc::new(TestConsumerService::default()),
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
    async fn forward_message_to_dead_letter_queue_records_request() {
        let consumer_service = Arc::new(TestConsumerService::default());
        let service = test_service_with_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            consumer_service.clone(),
        );
        let mut request = Request::new(v2::ForwardMessageToDeadLetterQueueRequest {
            group: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "GroupA".to_owned(),
            }),
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            receipt_handle: "receipt-handle".to_owned(),
            message_id: "msg-1".to_owned(),
            delivery_attempt: 3,
            max_delivery_attempts: 3,
            lite_topic: Some("LiteTopicA".to_owned()),
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service
            .forward_message_to_dead_letter_queue(request)
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);

        let requests = consumer_service
            .dlq_requests
            .lock()
            .expect("dlq requests mutex poisoned");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].message_id, "msg-1");
        assert_eq!(requests[0].lite_topic.as_deref(), Some("LiteTopicA"));
    }

    #[tokio::test]
    async fn change_invisible_duration_returns_new_receipt_handle() {
        let service = test_service_with_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            Arc::new(TestConsumerService::default()),
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

    #[tokio::test]
    async fn pull_message_streams_message_next_offset_and_status() {
        let service = test_service_with_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            Arc::new(TestConsumerService::default()),
        );
        let mut request = Request::new(v2::PullMessageRequest {
            group: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "GroupA".to_owned(),
            }),
            message_queue: Some(v2::MessageQueue {
                topic: Some(v2::Resource {
                    resource_namespace: String::new(),
                    name: "TopicA".to_owned(),
                }),
                id: 1,
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
            offset: 7,
            batch_size: 1,
            filter_expression: None,
            long_polling_timeout: Some(prost_types::Duration { seconds: 1, nanos: 0 }),
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let mut stream = service.pull_message(request).await.unwrap().into_inner();
        let responses: Vec<_> = stream.by_ref().collect::<Vec<_>>().await;

        assert_eq!(responses.len(), 3);
        assert!(matches!(
            responses[0].as_ref().unwrap().content,
            Some(v2::pull_message_response::Content::Message(_))
        ));
        assert!(matches!(
            responses[1].as_ref().unwrap().content,
            Some(v2::pull_message_response::Content::NextOffset(8))
        ));
        assert_eq!(
            match responses[2].as_ref().unwrap().content.as_ref().unwrap() {
                v2::pull_message_response::Content::Status(status) => status.code,
                _ => 0,
            },
            v2::Code::Ok as i32
        );
    }

    #[tokio::test]
    async fn update_offset_records_consumer_progress() {
        let consumer_service = Arc::new(TestConsumerService::default());
        let service = test_service_with_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            consumer_service.clone(),
        );
        let mut request = Request::new(v2::UpdateOffsetRequest {
            group: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "GroupA".to_owned(),
            }),
            message_queue: Some(v2::MessageQueue {
                topic: Some(v2::Resource {
                    resource_namespace: String::new(),
                    name: "TopicA".to_owned(),
                }),
                id: 1,
                permission: v2::Permission::ReadWrite as i32,
                broker: None,
                accept_message_types: vec![],
            }),
            offset: 12,
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.update_offset(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        let recorded = consumer_service
            .updated_offsets
            .lock()
            .expect("updated offsets mutex poisoned");
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].offset, 12);
    }

    #[tokio::test]
    async fn get_offset_returns_offset_value() {
        let service = test_service_with_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            Arc::new(TestConsumerService::default()),
        );
        let mut request = Request::new(v2::GetOffsetRequest {
            group: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "GroupA".to_owned(),
            }),
            message_queue: Some(v2::MessageQueue {
                topic: Some(v2::Resource {
                    resource_namespace: String::new(),
                    name: "TopicA".to_owned(),
                }),
                id: 1,
                permission: v2::Permission::ReadWrite as i32,
                broker: None,
                accept_message_types: vec![],
            }),
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.get_offset(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert_eq!(response.offset, 42);
    }

    #[tokio::test]
    async fn query_offset_supports_timestamp_policy() {
        let service = test_service_with_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            Arc::new(TestConsumerService::default()),
        );
        let mut request = Request::new(v2::QueryOffsetRequest {
            message_queue: Some(v2::MessageQueue {
                topic: Some(v2::Resource {
                    resource_namespace: String::new(),
                    name: "TopicA".to_owned(),
                }),
                id: 1,
                permission: v2::Permission::ReadWrite as i32,
                broker: None,
                accept_message_types: vec![],
            }),
            query_offset_policy: v2::QueryOffsetPolicy::Timestamp as i32,
            timestamp: Some(prost_types::Timestamp {
                seconds: 1_710_000_000,
                nanos: 0,
            }),
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.query_offset(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert_eq!(response.offset, 1_710_000_000_000);
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

    #[tokio::test]
    async fn server_side_telemetry_command_is_queued_for_bound_client() {
        let service = test_service(StaticRouteService::default(), StaticMetadataService::default());
        let (sender, mut receiver) = mpsc::unbounded_channel();
        service.sessions.bind_telemetry_link("client-a", sender);

        assert!(service.send_reconnect_endpoints_command("client-a", "nonce-a"));

        let command = receiver.recv().await.expect("telemetry command should be queued");
        match command.command {
            Some(v2::telemetry_command::Command::ReconnectEndpointsCommand(command)) => {
                assert_eq!(command.nonce, "nonce-a");
            }
            other => panic!("unexpected telemetry command: {other:?}"),
        }
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
            Arc::new(TestConsumerService::default()),
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
            Arc::new(TestConsumerService::default()),
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
            Arc::new(TestConsumerService::default()),
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
            Arc::new(TestConsumerService::default()),
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
    async fn end_transaction_uses_prepared_transaction_state_and_clears_it() {
        let transaction_service = Arc::new(TestTransactionService::default());
        let service = test_service_with_all_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            Arc::new(DefaultConsumerService),
            transaction_service.clone(),
        );
        let mut send_request = Request::new(v2::SendMessageRequest {
            messages: vec![v2::Message {
                topic: Some(v2::Resource {
                    resource_namespace: String::new(),
                    name: "TopicA".to_owned(),
                }),
                user_properties: HashMap::new(),
                system_properties: Some(v2::SystemProperties {
                    message_id: "msg-1".to_owned(),
                    body_encoding: v2::Encoding::Identity as i32,
                    message_type: v2::MessageType::Transaction as i32,
                    ..Default::default()
                }),
                body: Bytes::from_static(b"hello").to_vec(),
            }],
        });
        send_request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let send_response = service.send_message(send_request).await.unwrap().into_inner();
        assert_eq!(service.sessions.prepared_transaction_count(), 1);

        let mut request = Request::new(v2::EndTransactionRequest {
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            message_id: send_response.entries[0].message_id.clone(),
            transaction_id: send_response.entries[0].transaction_id.clone(),
            resolution: v2::TransactionResolution::Commit as i32,
            source: v2::TransactionSource::SourceClient as i32,
            trace_context: "trace-a".to_owned(),
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.end_transaction(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert_eq!(service.sessions.prepared_transaction_count(), 0);

        let recorded = transaction_service
            .requests
            .lock()
            .expect("transaction service mutex poisoned");
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].producer_group.as_deref(), Some("PROXY_SEND-client-a"));
        assert_eq!(recorded[0].transaction_state_table_offset, Some(0));
        assert_eq!(recorded[0].commit_log_message_id.as_deref(), Some("offset-msg-1"));
    }

    #[tokio::test]
    async fn end_transaction_rejects_unknown_transaction_id() {
        let transaction_service = Arc::new(TestTransactionService::default());
        let service = test_service_with_all_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            Arc::new(DefaultConsumerService),
            transaction_service.clone(),
        );
        let mut request = Request::new(v2::EndTransactionRequest {
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            message_id: "msg-1".to_owned(),
            transaction_id: "tx-missing".to_owned(),
            resolution: v2::TransactionResolution::Commit as i32,
            source: v2::TransactionSource::SourceClient as i32,
            trace_context: String::new(),
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.end_transaction(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::InvalidTransactionId as i32);
        assert_eq!(
            transaction_service
                .requests
                .lock()
                .expect("transaction service mutex poisoned")
                .len(),
            0
        );
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
