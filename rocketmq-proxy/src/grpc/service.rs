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

use std::io::Read;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context as TaskContext;
use std::task::Poll;
use std::time::Duration;
use std::time::SystemTime;

use futures::stream;
use futures::Stream;
use futures::StreamExt;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::BlockingKind;
use rocketmq_runtime::ResourcePermit;
use rocketmq_runtime::TaskGroup;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::Instrument;
use tracing::Span;

use crate::auth;
use crate::auth::AuthenticatedPrincipal;
use crate::auth::ProxyAuthRuntime;
use crate::config::ProxyConfig;
use crate::context::ProxyContext;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::grpc::adapter;
use crate::observability::ProxyHookChain;
use crate::observability::ProxyMetrics;
use crate::observability::ProxyMetricsSnapshot;
use crate::observability::ProxyRequestOutcome;
use crate::processor::MessagingProcessor;
use crate::proto::v2;
use crate::session::ClientSessionRegistry;
use crate::status::ProxyStatusMapper;

use rocketmq_proxy_core::ingress::grpc::service::admission::estimated_protobuf_retained_bytes;
use rocketmq_proxy_core::ingress::grpc::service::consumer;
use rocketmq_proxy_core::ingress::grpc::service::housekeeping;
use rocketmq_proxy_core::ingress::grpc::service::telemetry;
use rocketmq_proxy_core::ingress::grpc::service::topic;
use rocketmq_proxy_core::ingress::grpc::service::transaction;
use rocketmq_proxy_core::ingress::grpc::service::ExecutionGuards;
use rocketmq_proxy_core::ingress::grpc::service::ReapSchedule;

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

struct GuardedItemsStream<I> {
    items: I,
    permit: Option<ResourcePermit>,
    observation: Option<RequestObservation>,
}

impl<I> Stream for GuardedItemsStream<I>
where
    I: Iterator + Unpin,
{
    type Item = Result<I::Item, Status>;

    fn poll_next(mut self: Pin<&mut Self>, _context: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        match this.items.next() {
            Some(item) => Poll::Ready(Some(Ok(item))),
            None => {
                this.permit.take();
                this.observation.take();
                Poll::Ready(None)
            }
        }
    }
}

const RECEIPT_RENEWAL_CLAIM_LEASE: Duration = Duration::from_secs(5);
const RECEIPT_RENEWAL_EXPIRY_MARGIN: Duration = Duration::from_millis(250);

fn receive_message_error_stream_plan(status: v2::Status) -> (v2::Status, usize, adapter::ReceiveMessageResponseIter) {
    let retained_bytes = std::mem::size_of::<v2::ReceiveMessageResponse>().saturating_add(status.message.len());
    let responses = adapter::error_receive_message_response_iter(status.clone());
    (status, retained_bytes, responses)
}

fn pull_message_error_stream_plan(status: v2::Status) -> (v2::Status, usize, adapter::PullMessageResponseIter) {
    let retained_bytes = std::mem::size_of::<v2::PullMessageResponse>().saturating_add(status.message.len());
    let responses = adapter::error_pull_message_response_iter(status.clone());
    (status, retained_bytes, responses)
}

fn renewal_attempt_timeout(invisible_duration: Duration) -> Duration {
    Duration::from_millis(invisible_duration.as_millis().saturating_div(4).clamp(250, 3_000) as u64)
}

fn renewal_retry_delay(invisible_duration: Duration) -> Duration {
    Duration::from_millis(invisible_duration.as_millis().saturating_div(4).clamp(100, 1_000) as u64)
}

mod observation;

use observation::RequestObservation;
use observation::TelemetryStreamState;

// Kept as private facade aliases for the existing behavior tests while Core
// owns the canonical policy values.
#[cfg(test)]
const DEFAULT_MAX_BODY_SIZE_BYTES: i32 = telemetry::DEFAULT_MAX_BODY_SIZE_BYTES;
#[cfg(test)]
const DEFAULT_PRODUCER_MAX_ATTEMPTS: i32 = telemetry::DEFAULT_PRODUCER_MAX_ATTEMPTS;
#[cfg(test)]
const DEFAULT_CONSUMER_MAX_ATTEMPTS: i32 = telemetry::DEFAULT_CONSUMER_MAX_ATTEMPTS;
#[cfg(test)]
const DEFAULT_CONSUMER_RECEIVE_BATCH_SIZE: i32 = telemetry::DEFAULT_CONSUMER_RECEIVE_BATCH_SIZE;
#[cfg(test)]
const DEFAULT_CONSUMER_CUSTOMIZED_BACKOFF_MS: [u64; 18] = telemetry::DEFAULT_CONSUMER_CUSTOMIZED_BACKOFF_MS;

pub struct ProxyGrpcService<P> {
    config: Arc<ProxyConfig>,
    processor: Arc<P>,
    sessions: ClientSessionRegistry,
    guards: ExecutionGuards,
    reap_schedule: ReapSchedule,
    auth_runtime: Option<Arc<ProxyAuthRuntime>>,
    hooks: ProxyHookChain,
    metrics: ProxyMetrics,
    drain: rocketmq_proxy_core::ProxyDrainController,
    cpu_crypto: Option<BlockingExecutor>,
}

pub type ProxyHousekeepingRunReport = housekeeping::GrpcHousekeepingRunReport;

impl<P> Clone for ProxyGrpcService<P> {
    fn clone(&self) -> Self {
        Self {
            config: Arc::clone(&self.config),
            processor: Arc::clone(&self.processor),
            sessions: self.sessions.clone(),
            guards: self.guards.clone(),
            reap_schedule: self.reap_schedule.clone(),
            auth_runtime: self.auth_runtime.clone(),
            hooks: self.hooks.clone(),
            metrics: self.metrics.clone(),
            drain: self.drain.clone(),
            cpu_crypto: self.cpu_crypto.clone(),
        }
    }
}

impl<P> ProxyGrpcService<P> {
    pub(crate) fn try_execution_guards(config: &ProxyConfig) -> ProxyResult<ExecutionGuards> {
        ExecutionGuards::try_from_config(&config.runtime)
    }

    pub(crate) fn from_execution_guards(
        config: Arc<ProxyConfig>,
        processor: Arc<P>,
        sessions: ClientSessionRegistry,
        guards: ExecutionGuards,
    ) -> Self {
        let interval_ms = Self::housekeeping_interval_from_config(config.as_ref())
            .as_millis()
            .clamp(1, u128::from(u64::MAX)) as u64;
        Self {
            guards,
            config,
            processor,
            sessions,
            reap_schedule: ReapSchedule::new(Duration::from_millis(interval_ms)),
            auth_runtime: None,
            hooks: ProxyHookChain::default(),
            metrics: ProxyMetrics::default(),
            drain: rocketmq_proxy_core::ProxyDrainController::default(),
            cpu_crypto: None,
        }
    }

    /// Builds a gRPC service after validating all runtime resource budgets.
    ///
    /// # Errors
    ///
    /// Returns a typed Proxy error when the configured resource limits are
    /// invalid or the process memory limit cannot be detected.
    pub fn try_new(config: Arc<ProxyConfig>, processor: Arc<P>, sessions: ClientSessionRegistry) -> ProxyResult<Self> {
        let guards = Self::try_execution_guards(config.as_ref())?;
        Ok(Self::from_execution_guards(config, processor, sessions, guards))
    }

    /// Builds a gRPC service for compatibility with existing embedders.
    ///
    /// Production composition should prefer [`Self::try_new`] so invalid
    /// resource limits are reported as startup errors.
    ///
    /// # Panics
    ///
    /// Panics when runtime resource-budget validation fails.
    pub fn new(config: Arc<ProxyConfig>, processor: Arc<P>, sessions: ClientSessionRegistry) -> Self {
        Self::try_new(config, processor, sessions)
            .unwrap_or_else(|error| panic!("invalid Proxy gRPC resource limits: {error}"))
    }

    pub fn with_auth_runtime(mut self, auth_runtime: Option<ProxyAuthRuntime>) -> Self {
        self.auth_runtime = auth_runtime.map(Arc::new);
        self
    }

    pub fn with_hooks(mut self, hooks: ProxyHookChain) -> Self {
        self.hooks = hooks;
        self
    }

    pub fn with_metrics(mut self, metrics: ProxyMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn with_drain_controller(mut self, drain: rocketmq_proxy_core::ProxyDrainController) -> Self {
        self.drain = drain;
        self
    }

    pub fn with_cpu_crypto_executor(mut self, executor: BlockingExecutor) -> Self {
        self.cpu_crypto = Some(executor);
        self
    }

    async fn normalize_send_body_encodings(
        &self,
        request: v2::SendMessageRequest,
    ) -> ProxyResult<v2::SendMessageRequest> {
        let needs_gzip = request.messages.iter().any(|message| {
            message.system_properties.as_ref().is_some_and(|properties| {
                v2::Encoding::try_from(properties.body_encoding).unwrap_or(v2::Encoding::Unspecified)
                    == v2::Encoding::Gzip
            })
        });
        if !needs_gzip {
            return Ok(request);
        }

        let Some(executor) = self.cpu_crypto.clone() else {
            return Err(ProxyError::not_implemented("SendMessage(gzip executor unavailable)"));
        };
        let max_body_size = self.config.grpc.max_message_body_size;
        executor
            .spawn("proxy.grpc.decode_gzip", BlockingKind::CpuBound, move || {
                decode_gzip_message_bodies(request, max_body_size)
            })
            .await
            .map_err(|error| ProxyError::Transport {
                message: format!("gzip decode task failed: {error}"),
            })?
    }

    pub fn metrics_snapshot(&self) -> ProxyMetricsSnapshot {
        let auth = self
            .auth_runtime
            .as_ref()
            .map(|runtime| runtime.auth_metrics_snapshot());
        self.metrics.snapshot(&self.sessions, auth)
    }

    #[must_use]
    pub fn consumer_response_budget_snapshot(&self) -> rocketmq_runtime::BudgetSnapshot {
        self.guards.consumer_response_snapshot()
    }

    fn context<T>(&self, rpc_name: &'static str, request: &Request<T>) -> Result<ProxyContext, Status> {
        ProxyContext::from_grpc_request(rpc_name, request).map_err(|error| ProxyStatusMapper::to_tonic_status(&error))
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

    fn guarded_items_stream<T, I>(
        &self,
        items: I,
        permit: ResourcePermit,
        observation: RequestObservation,
    ) -> ResponseStream<T>
    where
        T: Send + 'static,
        I: Iterator<Item = T> + Send + Unpin + 'static,
    {
        Box::pin(GuardedItemsStream {
            items,
            permit: Some(permit),
            observation: Some(observation),
        })
    }

    async fn begin_observation(
        &self,
        context: ProxyContext,
        drain_admission: rocketmq_proxy_core::ProxyDrainAdmission,
    ) -> RequestObservation {
        let observation = RequestObservation::new(context, drain_admission);
        self.metrics.record_request_started(observation.context().rpc_name());
        self.hooks
            .before_request(observation.context())
            .instrument(observation.span())
            .await;
        observation
    }

    async fn finish_observation(&self, observation: &RequestObservation, outcome: &ProxyRequestOutcome) {
        observation.rpc_span().record("result", outcome.metric_result());
        self.metrics
            .record_request_completed(observation.context().rpc_name(), outcome, observation.elapsed());
        if let Some((span, elapsed)) = observation.forward() {
            rocketmq_observability::trace::proxy::record_outcome(span, proxy_span_outcome(outcome));
            self.metrics.record_forward_completed(elapsed);
        }
        self.hooks
            .after_request(observation.context(), outcome)
            .instrument(observation.span())
            .await;
    }

    async fn finish_unary_payload(&self, observation: &RequestObservation, status: &v2::Status) {
        self.finish_observation(observation, &ProxyRequestOutcome::from_payload_status(status))
            .await;
    }

    async fn finish_stream_payload(&self, observation: &RequestObservation, status: &v2::Status) {
        self.finish_observation(observation, &ProxyRequestOutcome::from_payload_status(status))
            .await;
    }

    fn record_transport_failure(&self, rpc_name: &'static str, status: &Status) {
        self.metrics.record_request_started(rpc_name);
        self.metrics.record_request_completed(
            rpc_name,
            &ProxyRequestOutcome::from_tonic_status(status),
            Duration::ZERO,
        );
    }

    async fn enter_unary<TRequest: 'static, TResponse>(
        &self,
        rpc_name: &'static str,
        request: Request<TRequest>,
        payload_builder: impl FnOnce(v2::Status) -> TResponse,
    ) -> Result<
        (
            RequestObservation,
            ProxyContext,
            Option<AuthenticatedPrincipal>,
            TRequest,
        ),
        Result<Response<TResponse>, Status>,
    > {
        self.reap_session_state_if_due();
        let drain_admission = match self.drain.try_admit() {
            Ok(admission) => admission,
            Err(_) => {
                let status = ProxyStatusMapper::to_tonic_status(&ProxyError::Draining);
                self.record_transport_failure(rpc_name, &status);
                return Err(Err(status));
            }
        };
        let mut context = match self.context(rpc_name, &request) {
            Ok(context) => context,
            Err(status) => {
                self.record_transport_failure(rpc_name, &status);
                return Err(Err(status));
            }
        };
        let mut observation = self.begin_observation(context.clone(), drain_admission).await;
        let principal = match self
            .authenticate_request(observation.rpc_span(), &mut context, &request)
            .await
        {
            Ok(principal) => principal,
            Err(error) => {
                return if ProxyStatusMapper::should_use_tonic_status(&error) {
                    let status = ProxyStatusMapper::to_tonic_status(&error);
                    self.finish_observation(&observation, &ProxyRequestOutcome::from_tonic_status(&status))
                        .await;
                    Err(Err(status))
                } else {
                    let payload_status = ProxyStatusMapper::from_error(&error);
                    self.finish_unary_payload(&observation, &payload_status).await;
                    Err(Ok(Response::new(payload_builder(payload_status))))
                };
            }
        };
        observation.record_principal(principal.as_ref());
        observation.begin_forward();
        Ok((observation, context, principal, request.into_inner()))
    }

    async fn enter_stream<TRequest: 'static, TItem>(
        &self,
        rpc_name: &'static str,
        request: Request<TRequest>,
        payload_builder: impl FnOnce(v2::Status) -> ResponseStream<TItem>,
    ) -> Result<
        (
            RequestObservation,
            ProxyContext,
            Option<AuthenticatedPrincipal>,
            TRequest,
        ),
        Result<Response<ResponseStream<TItem>>, Status>,
    > {
        self.reap_session_state_if_due();
        let drain_admission = match self.drain.try_admit() {
            Ok(admission) => admission,
            Err(_) => {
                let status = ProxyStatusMapper::to_tonic_status(&ProxyError::Draining);
                self.record_transport_failure(rpc_name, &status);
                return Err(Err(status));
            }
        };
        let mut context = match self.context(rpc_name, &request) {
            Ok(context) => context,
            Err(status) => {
                self.record_transport_failure(rpc_name, &status);
                return Err(Err(status));
            }
        };
        let mut observation = self.begin_observation(context.clone(), drain_admission).await;
        let principal = match self
            .authenticate_request(observation.rpc_span(), &mut context, &request)
            .await
        {
            Ok(principal) => principal,
            Err(error) => {
                return if ProxyStatusMapper::should_use_tonic_status(&error) {
                    let status = ProxyStatusMapper::to_tonic_status(&error);
                    self.finish_observation(&observation, &ProxyRequestOutcome::from_tonic_status(&status))
                        .await;
                    Err(Err(status))
                } else {
                    let payload_status = ProxyStatusMapper::from_error(&error);
                    self.finish_stream_payload(&observation, &payload_status).await;
                    Err(Ok(Response::new(payload_builder(payload_status))))
                };
            }
        };
        observation.record_principal(principal.as_ref());
        observation.begin_forward();
        Ok((observation, context, principal, request.into_inner()))
    }

    async fn authenticate_request<T: 'static>(
        &self,
        parent: Span,
        context: &mut ProxyContext,
        request: &Request<T>,
    ) -> ProxyResult<Option<AuthenticatedPrincipal>> {
        let span = rocketmq_observability::trace::proxy::auth_span(&parent, context.rpc_name());
        let authentication_enabled = self
            .auth_runtime
            .as_ref()
            .is_some_and(|auth_runtime| auth_runtime.enabled());
        let result = async {
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
        .instrument(span.clone())
        .await;
        let outcome = match &result {
            Ok(_) if authentication_enabled => rocketmq_observability::trace::proxy::ProxySpanOutcome::Success,
            Ok(_) => rocketmq_observability::trace::proxy::ProxySpanOutcome::Bypassed,
            Err(_) => rocketmq_observability::trace::proxy::ProxySpanOutcome::Denied,
        };
        rocketmq_observability::trace::proxy::record_outcome(&span, outcome);
        result
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

    pub async fn run_housekeeping_until<F>(&self, shutdown: F, task_group: TaskGroup)
    where
        F: std::future::Future<Output = ()> + Send,
        P: MessagingProcessor + 'static,
    {
        let _ = self.run_housekeeping_until_with_report(shutdown, task_group).await;
    }

    pub async fn run_housekeeping_until_with_report<F>(
        &self,
        shutdown: F,
        task_group: TaskGroup,
    ) -> ProxyHousekeepingRunReport
    where
        F: std::future::Future<Output = ()> + Send,
        P: MessagingProcessor + 'static,
    {
        self.run_housekeeping_until_with_task_group(shutdown, task_group).await
    }

    pub async fn run_housekeeping_until_with_task_group<F>(
        &self,
        shutdown: F,
        task_group: TaskGroup,
    ) -> ProxyHousekeepingRunReport
    where
        F: std::future::Future<Output = ()> + Send,
        P: MessagingProcessor + 'static,
    {
        let housekeeping_service = self.clone();
        let renewal_service = self.clone();
        housekeeping::run_housekeeping_until(
            self.housekeeping_interval(),
            shutdown,
            task_group,
            move || {
                let service = housekeeping_service.clone();
                async move {
                    service.run_housekeeping_once().await;
                }
            },
            async move {
                renewal_service.run_receipt_renewal_loop().await;
            },
        )
        .await
    }

    async fn run_housekeeping_once(&self)
    where
        P: MessagingProcessor + 'static,
    {
        self.reap_session_state();
        self.dispatch_due_prepared_transaction_recoveries();
        self.schedule_next_reap();
    }

    async fn run_receipt_renewal_loop(&self)
    where
        P: MessagingProcessor + 'static,
    {
        loop {
            self.sessions.wait_for_receipt_renewal().await;
            self.renew_due_receipt_handles().await;
        }
    }

    fn housekeeping_interval(&self) -> Duration {
        housekeeping::housekeeping_interval(&self.config.session)
    }

    fn housekeeping_interval_from_config(config: &ProxyConfig) -> Duration {
        housekeeping::housekeeping_interval(&config.session)
    }

    fn reap_session_state(&self) {
        let _ = self.sessions.reap_expired(
            self.config.session.client_ttl(),
            self.config.session.receipt_handle_ttl(),
        );
    }

    fn schedule_next_reap(&self) {
        self.reap_schedule.schedule_next(self.housekeeping_interval());
    }

    fn reap_session_state_if_due(&self) {
        if self.reap_schedule.claim_if_due(self.housekeeping_interval()) {
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
        topic::validate_client_context(context)
    }

    fn validate_heartbeat_request(&self, context: &ProxyContext, client_type: i32) -> ProxyResult<()> {
        topic::validate_heartbeat_request(context, client_type)
    }

    fn telemetry_status(status: v2::Status) -> v2::TelemetryCommand {
        telemetry::telemetry_status(status)
    }

    pub fn send_reconnect_endpoints_command(&self, client_id: &str, nonce: impl Into<String>) -> bool {
        telemetry::send_reconnect_endpoints(&self.sessions, client_id, nonce)
    }

    pub fn send_print_thread_stack_trace_command(&self, client_id: &str, nonce: impl Into<String>) -> bool {
        telemetry::send_print_thread_stack_trace(&self.sessions, client_id, nonce)
    }

    pub fn send_verify_message_command(&self, client_id: &str, nonce: impl Into<String>, message: v2::Message) -> bool {
        telemetry::send_verify_message(&self.sessions, client_id, nonce, message)
    }

    pub fn send_recover_orphaned_transaction_command(
        &self,
        client_id: &str,
        message: v2::Message,
        transaction_id: impl Into<String>,
    ) -> bool {
        telemetry::send_recover_orphaned_transaction(&self.sessions, client_id, message, transaction_id)
    }

    pub fn send_notify_unsubscribe_lite_command(&self, client_id: &str, lite_topic: impl Into<String>) -> bool {
        telemetry::send_notify_unsubscribe_lite(&self.sessions, client_id, lite_topic)
    }

    fn merged_telemetry_settings(&self, settings: &v2::Settings) -> v2::Settings {
        telemetry::merged_settings(settings, &self.config.session)
    }

    fn effective_receive_request(
        &self,
        context: &ProxyContext,
        request: crate::processor::ReceiveMessageRequest,
    ) -> ProxyResult<crate::processor::ReceiveMessageRequest> {
        consumer::effective_receive_request(&self.sessions, &self.config.session, context, request)
    }

    fn track_received_receipt_handles(
        &self,
        context: &ProxyContext,
        request: &crate::processor::ReceiveMessageRequest,
        plan: &crate::processor::ReceiveMessagePlan,
    ) {
        consumer::track_received_receipt_handles(&self.sessions, &self.config.session, context, request, plan);
    }

    fn reconcile_ack_result(
        &self,
        context: &ProxyContext,
        request: &crate::processor::AckMessageRequest,
        plan: &crate::processor::AckMessagePlan,
    ) {
        consumer::reconcile_ack_result(&self.sessions, context, request, plan);
    }

    fn reconcile_change_invisible_result(
        &self,
        context: &ProxyContext,
        request: &crate::processor::ChangeInvisibleDurationRequest,
        plan: &crate::processor::ChangeInvisibleDurationPlan,
    ) {
        consumer::reconcile_change_invisible_result(&self.sessions, context, request, plan);
    }

    fn track_prepared_transactions(
        &self,
        context: &ProxyContext,
        producer_group: &str,
        grpc_request: &v2::SendMessageRequest,
        request: &crate::processor::SendMessageRequest,
        plan: &crate::processor::SendMessagePlan,
    ) {
        transaction::track_prepared_transactions(&self.sessions, producer_group, context, grpc_request, request, plan);
    }

    fn enrich_end_transaction_request(
        &self,
        context: &ProxyContext,
        request: &mut crate::processor::EndTransactionRequest,
    ) -> ProxyResult<()> {
        transaction::enrich_end_transaction_request(&self.sessions, context, request)
    }

    fn reconcile_end_transaction_result(
        &self,
        context: &ProxyContext,
        request: &crate::processor::EndTransactionRequest,
        plan: &crate::processor::EndTransactionPlan,
    ) {
        transaction::reconcile_end_transaction_result(&self.sessions, context, request, plan);
    }

    async fn renew_due_receipt_handles(&self) {
        let due_handles = self.sessions.claim_due_receipt_handles(
            self.config.session.auto_renew_max_inflight(),
            RECEIPT_RENEWAL_CLAIM_LEASE,
        );
        if due_handles.is_empty() {
            return;
        }

        let processor = Arc::clone(&self.processor);
        let sessions = self.sessions.clone();
        stream::iter(due_handles)
            .for_each_concurrent(Some(self.config.session.auto_renew_max_inflight()), move |claim| {
                let processor = Arc::clone(&processor);
                let sessions = sessions.clone();
                async move {
                    if !sessions.receipt_renewal_claim_is_current(&claim) {
                        return;
                    }

                    let tracked = &claim.tracked;
                    let remaining_visibility = claim.remaining_visibility();
                    if remaining_visibility <= RECEIPT_RENEWAL_EXPIRY_MARGIN {
                        let _ = sessions.retry_receipt_handle_renewal(&claim, remaining_visibility);
                        return;
                    }

                    let context =
                        ProxyContext::for_internal_client("AutoRenewReceiptHandle", tracked.client_id.clone());
                    let renew_request = crate::processor::ChangeInvisibleDurationRequest {
                        group: tracked.group.clone(),
                        topic: tracked.topic.clone(),
                        receipt_handle: tracked.receipt_handle.clone(),
                        invisible_duration: tracked.invisible_duration,
                        message_id: tracked.message_id.clone(),
                        lite_topic: None,
                        suspend: None,
                    };

                    let attempt_timeout = renewal_attempt_timeout(tracked.invisible_duration)
                        .min(remaining_visibility.saturating_sub(RECEIPT_RENEWAL_EXPIRY_MARGIN));
                    match tokio::time::timeout(
                        attempt_timeout,
                        processor.change_invisible_duration(&context.without_principal(), renew_request.clone()),
                    )
                    .await
                    {
                        Ok(Ok(plan)) if plan.status.is_ok() => {
                            let next_receipt_handle = if plan.receipt_handle.is_empty() {
                                renew_request.receipt_handle
                            } else {
                                plan.receipt_handle
                            };
                            let _ = sessions.complete_receipt_handle_renewal(
                                &claim,
                                next_receipt_handle.as_str(),
                                renew_request.invisible_duration,
                            );
                        }
                        Ok(Ok(plan)) if plan.status.code() == v2::Code::InvalidReceiptHandle as i32 => {
                            let _ = sessions.invalidate_receipt_handle_renewal(&claim);
                        }
                        Ok(Ok(_)) | Ok(Err(_)) | Err(_) => {
                            let delay = renewal_retry_delay(tracked.invisible_duration).min(
                                claim
                                    .remaining_visibility()
                                    .saturating_sub(RECEIPT_RENEWAL_EXPIRY_MARGIN),
                            );
                            let _ = sessions.retry_receipt_handle_renewal(&claim, delay);
                        }
                    }
                }
            })
            .await;
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
            Some(command) => telemetry::handle_client_report(&self.sessions, context.client_id(), &command)
                .unwrap_or_else(|| {
                    Self::telemetry_status(ProxyStatusMapper::from_code(
                        v2::Code::InternalError,
                        "telemetry command was not handled",
                    ))
                }),
            None => Self::telemetry_status(ProxyStatusMapper::ok()),
        }
    }
}

fn decode_gzip_message_bodies(
    mut request: v2::SendMessageRequest,
    max_body_size: usize,
) -> ProxyResult<v2::SendMessageRequest> {
    for message in &mut request.messages {
        let Some(system) = message.system_properties.as_mut() else {
            continue;
        };
        let encoding = v2::Encoding::try_from(system.body_encoding).unwrap_or(v2::Encoding::Unspecified);
        if encoding != v2::Encoding::Gzip {
            continue;
        }

        let mut decoded = Vec::with_capacity(message.body.len().min(max_body_size));
        let decoder = flate2::read::GzDecoder::new(message.body.as_ref());
        let limit = u64::try_from(max_body_size).unwrap_or(u64::MAX).saturating_add(1);
        decoder.take(limit).read_to_end(&mut decoded).map_err(|error| {
            rocketmq_error::RocketMQError::illegal_argument(format!("invalid gzip message body: {error}"))
        })?;
        if decoded.len() > max_body_size {
            return Err(rocketmq_error::RocketMQError::illegal_argument(format!(
                "decoded message body exceeds the configured maximum {max_body_size} bytes"
            ))
            .into());
        }
        message.body = decoded.into();
        system.body_encoding = v2::Encoding::Identity as i32;
    }
    Ok(request)
}

fn proxy_span_outcome(outcome: &ProxyRequestOutcome) -> rocketmq_observability::trace::proxy::ProxySpanOutcome {
    match outcome {
        ProxyRequestOutcome::Payload(status) if status.is_ok() => {
            rocketmq_observability::trace::proxy::ProxySpanOutcome::Success
        }
        ProxyRequestOutcome::Payload(_) => rocketmq_observability::trace::proxy::ProxySpanOutcome::PayloadFailure,
        ProxyRequestOutcome::Transport { .. } => {
            rocketmq_observability::trace::proxy::ProxySpanOutcome::TransportFailure
        }
    }
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
        let (observation, context, principal, request) = match self
            .enter_unary("QueryRoute", request, adapter::error_query_route_response)
            .await
        {
            Ok(state) => state,
            Err(response) => return response,
        };

        let response = async {
            match self
                .validate_client_context(&context)
                .and_then(|_| adapter::build_query_route_request(self.config.as_ref(), &request))
            {
                Ok(input) => match self
                    .authorize_contexts(&context, principal.as_ref(), &auth::query_route_contexts(&input))
                    .await
                {
                    Ok(()) => match self.guards.try_route(estimated_protobuf_retained_bytes(&request)) {
                        Ok(_permit) => match self.processor.query_route(&context.without_principal(), input).await {
                            Ok(plan) => adapter::build_query_route_response(&request, &plan),
                            Err(error) => adapter::error_query_route_response(ProxyStatusMapper::from_error(&error)),
                        },
                        Err(error) => adapter::error_query_route_response(ProxyStatusMapper::from_error(&error)),
                    },
                    Err(error) => adapter::error_query_route_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_query_route_response(ProxyStatusMapper::from_error(&error)),
            }
        }
        .instrument(observation.span())
        .await;

        let status = response.status.clone().unwrap_or_else(ProxyStatusMapper::ok);
        self.finish_unary_payload(&observation, &status).await;
        Ok(Response::new(response))
    }

    async fn heartbeat(
        &self,
        request: Request<v2::HeartbeatRequest>,
    ) -> Result<Response<v2::HeartbeatResponse>, Status> {
        let (observation, context, principal, request) = match self
            .enter_unary("Heartbeat", request, |status| v2::HeartbeatResponse {
                status: Some(status),
            })
            .await
        {
            Ok(state) => state,
            Err(response) => return response,
        };

        let status = async {
            match self
                .guards
                .try_client_manager(estimated_protobuf_retained_bytes(&request))
            {
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
            }
        }
        .instrument(observation.span())
        .await;

        self.finish_unary_payload(&observation, &status).await;
        Ok(Response::new(v2::HeartbeatResponse { status: Some(status) }))
    }

    async fn send_message(
        &self,
        request: Request<v2::SendMessageRequest>,
    ) -> Result<Response<v2::SendMessageResponse>, Status> {
        let (observation, context, principal, request) = match self
            .enter_unary("SendMessage", request, adapter::error_send_message_response)
            .await
        {
            Ok(state) => state,
            Err(response) => return response,
        };

        let response = async {
            if let Err(error) = self.validate_client_context(&context) {
                return adapter::error_send_message_response(ProxyStatusMapper::from_error(&error));
            }
            let _permit = match self.guards.try_producer(estimated_protobuf_retained_bytes(&request)) {
                Ok(permit) => permit,
                Err(error) => return adapter::error_send_message_response(ProxyStatusMapper::from_error(&error)),
            };
            let request = match self.normalize_send_body_encodings(request).await {
                Ok(request) => request,
                Err(error) => return adapter::error_send_message_response(ProxyStatusMapper::from_error(&error)),
            };
            let input = match adapter::build_send_message_request_with_config(
                &self.config.grpc,
                &context.without_principal(),
                &request,
            ) {
                Ok(input) => input,
                Err(error) => return adapter::error_send_message_response(ProxyStatusMapper::from_error(&error)),
            };
            if let Err(error) = self
                .authorize_contexts(&context, principal.as_ref(), &auth::send_message_contexts(&input))
                .await
            {
                return adapter::error_send_message_response(ProxyStatusMapper::from_error(&error));
            }
            match self
                .processor
                .send_message(&context.without_principal(), input.clone())
                .await
            {
                Ok(plan) => {
                    if let Some(producer_group) =
                        self.processor.transaction_producer_group(&context.without_principal())
                    {
                        self.track_prepared_transactions(&context, producer_group.as_str(), &request, &input, &plan);
                    }
                    adapter::build_send_message_response(&plan, &input)
                }
                Err(error) => adapter::error_send_message_response(ProxyStatusMapper::from_error(&error)),
            }
        }
        .instrument(observation.span())
        .await;

        let status = response.status.clone().unwrap_or_else(ProxyStatusMapper::ok);
        self.finish_unary_payload(&observation, &status).await;
        Ok(Response::new(response))
    }

    async fn query_assignment(
        &self,
        request: Request<v2::QueryAssignmentRequest>,
    ) -> Result<Response<v2::QueryAssignmentResponse>, Status> {
        let (observation, context, principal, request) = match self
            .enter_unary("QueryAssignment", request, adapter::error_query_assignment_response)
            .await
        {
            Ok(state) => state,
            Err(response) => return response,
        };

        let response = async {
            match self
                .validate_client_context(&context)
                .and_then(|_| adapter::build_query_assignment_request(self.config.as_ref(), &request))
            {
                Ok(input) => match self
                    .authorize_contexts(&context, principal.as_ref(), &auth::query_assignment_contexts(&input))
                    .await
                {
                    Ok(()) => match self.guards.try_route(estimated_protobuf_retained_bytes(&request)) {
                        Ok(_permit) => match self
                            .processor
                            .query_assignment(&context.without_principal(), input)
                            .await
                        {
                            Ok(plan) => adapter::build_query_assignment_response(&request, &plan),
                            Err(error) => {
                                adapter::error_query_assignment_response(ProxyStatusMapper::from_error(&error))
                            }
                        },
                        Err(error) => adapter::error_query_assignment_response(ProxyStatusMapper::from_error(&error)),
                    },
                    Err(error) => adapter::error_query_assignment_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_query_assignment_response(ProxyStatusMapper::from_error(&error)),
            }
        }
        .instrument(observation.span())
        .await;

        let status = response.status.clone().unwrap_or_else(ProxyStatusMapper::ok);
        self.finish_unary_payload(&observation, &status).await;
        Ok(Response::new(response))
    }

    async fn receive_message(
        &self,
        request: Request<v2::ReceiveMessageRequest>,
    ) -> Result<Response<Self::ReceiveMessageStream>, Status> {
        let (observation, context, principal, request) = match self
            .enter_stream("ReceiveMessage", request, |status| {
                self.items_stream(adapter::error_receive_message_responses(status))
            })
            .await
        {
            Ok(state) => state,
            Err(response) => return response,
        };
        let (status, retained_bytes, responses) = async {
            match self
                .validate_client_context(&context)
                .and_then(|_| adapter::build_receive_message_request(&request))
                .and_then(|input| self.effective_receive_request(&context, input))
            {
                Ok(input) => match self
                    .authorize_contexts(&context, principal.as_ref(), &auth::receive_message_contexts(&input))
                    .await
                {
                    Ok(()) => match self.guards.try_consumer(estimated_protobuf_retained_bytes(&request)) {
                        Ok(_permit) => {
                            self.sessions.upsert_from_context(&context);
                            match self
                                .processor
                                .receive_message(&context.without_principal(), input.clone())
                                .await
                            {
                                Ok(plan) => {
                                    self.track_received_receipt_handles(&context, &input, &plan);
                                    let status = plan.status.clone().into();
                                    let retained_bytes = adapter::receive_message_response_retained_bytes(&plan);
                                    (status, retained_bytes, adapter::receive_message_response_iter(plan))
                                }
                                Err(error) => receive_message_error_stream_plan(ProxyStatusMapper::from_error(&error)),
                            }
                        }
                        Err(error) => receive_message_error_stream_plan(ProxyStatusMapper::from_error(&error)),
                    },
                    Err(error) => receive_message_error_stream_plan(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => receive_message_error_stream_plan(ProxyStatusMapper::from_error(&error)),
            }
        }
        .instrument(observation.span())
        .await;

        let response_permit = match self.guards.try_consumer_response(retained_bytes) {
            Ok(permit) => permit,
            Err(error) => {
                let status = ProxyStatusMapper::from_error(&error);
                self.finish_stream_payload(&observation, &status).await;
                return Ok(Response::new(
                    self.items_stream(adapter::error_receive_message_responses(status)),
                ));
            }
        };
        self.finish_stream_payload(&observation, &status).await;
        Ok(Response::new(self.guarded_items_stream(
            responses,
            response_permit,
            observation,
        )))
    }

    async fn ack_message(
        &self,
        request: Request<v2::AckMessageRequest>,
    ) -> Result<Response<v2::AckMessageResponse>, Status> {
        let (observation, context, principal, request) = match self
            .enter_unary("AckMessage", request, adapter::error_ack_message_response)
            .await
        {
            Ok(state) => state,
            Err(response) => return response,
        };

        let response = async {
            match self
                .validate_client_context(&context)
                .and_then(|_| adapter::build_ack_message_request(&request))
            {
                Ok(input) => match self
                    .authorize_contexts(&context, principal.as_ref(), &auth::ack_message_contexts(&input))
                    .await
                {
                    Ok(()) => match self.guards.try_consumer(estimated_protobuf_retained_bytes(&request)) {
                        Ok(_permit) => match self
                            .processor
                            .ack_message(&context.without_principal(), input.clone())
                            .await
                        {
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
            }
        }
        .instrument(observation.span())
        .await;

        let status = response.status.clone().unwrap_or_else(ProxyStatusMapper::ok);
        self.finish_unary_payload(&observation, &status).await;
        Ok(Response::new(response))
    }

    async fn forward_message_to_dead_letter_queue(
        &self,
        request: Request<v2::ForwardMessageToDeadLetterQueueRequest>,
    ) -> Result<Response<v2::ForwardMessageToDeadLetterQueueResponse>, Status> {
        let (observation, context, principal, request) = match self
            .enter_unary(
                "ForwardMessageToDeadLetterQueue",
                request,
                adapter::error_forward_message_to_dead_letter_queue_response,
            )
            .await
        {
            Ok(state) => state,
            Err(response) => return response,
        };

        let response = async {
            match self
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
                    Ok(()) => match self.guards.try_consumer(estimated_protobuf_retained_bytes(&request)) {
                        Ok(_permit) => match self
                            .processor
                            .forward_message_to_dead_letter_queue(&context.without_principal(), input)
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
                    Err(error) => adapter::error_forward_message_to_dead_letter_queue_response(
                        ProxyStatusMapper::from_error(&error),
                    ),
                },
                Err(error) => {
                    adapter::error_forward_message_to_dead_letter_queue_response(ProxyStatusMapper::from_error(&error))
                }
            }
        }
        .instrument(observation.span())
        .await;

        let status = response.status.clone().unwrap_or_else(ProxyStatusMapper::ok);
        self.finish_unary_payload(&observation, &status).await;
        Ok(Response::new(response))
    }

    async fn pull_message(
        &self,
        request: Request<v2::PullMessageRequest>,
    ) -> Result<Response<Self::PullMessageStream>, Status> {
        let (observation, context, principal, request) = match self
            .enter_stream("PullMessage", request, |status| {
                self.items_stream(adapter::error_pull_message_responses(status))
            })
            .await
        {
            Ok(state) => state,
            Err(response) => return response,
        };
        let (status, retained_bytes, responses) = async {
            match adapter::build_pull_message_request(&request) {
                Ok(input) => match self
                    .validate_client_context(&context)
                    .and_then(|_| self.guards.try_consumer(estimated_protobuf_retained_bytes(&request)))
                {
                    Ok(_permit) => match self
                        .authorize_contexts(&context, principal.as_ref(), &auth::pull_message_contexts(&input))
                        .await
                    {
                        Ok(()) => {
                            self.sessions.upsert_from_context(&context);
                            match self.processor.pull_message(&context.without_principal(), input).await {
                                Ok(plan) => {
                                    let status = plan.status.clone().into();
                                    let retained_bytes = adapter::pull_message_response_retained_bytes(&plan);
                                    (status, retained_bytes, adapter::pull_message_response_iter(plan))
                                }
                                Err(error) => pull_message_error_stream_plan(ProxyStatusMapper::from_error(&error)),
                            }
                        }
                        Err(error) => pull_message_error_stream_plan(ProxyStatusMapper::from_error(&error)),
                    },
                    Err(error) => pull_message_error_stream_plan(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => pull_message_error_stream_plan(ProxyStatusMapper::from_error(&error)),
            }
        }
        .instrument(observation.span())
        .await;

        let response_permit = match self.guards.try_consumer_response(retained_bytes) {
            Ok(permit) => permit,
            Err(error) => {
                let status = ProxyStatusMapper::from_error(&error);
                self.finish_stream_payload(&observation, &status).await;
                return Ok(Response::new(
                    self.items_stream(adapter::error_pull_message_responses(status)),
                ));
            }
        };
        self.finish_stream_payload(&observation, &status).await;
        Ok(Response::new(self.guarded_items_stream(
            responses,
            response_permit,
            observation,
        )))
    }

    async fn update_offset(
        &self,
        request: Request<v2::UpdateOffsetRequest>,
    ) -> Result<Response<v2::UpdateOffsetResponse>, Status> {
        let (observation, context, principal, request) = match self
            .enter_unary("UpdateOffset", request, adapter::error_update_offset_response)
            .await
        {
            Ok(state) => state,
            Err(response) => return response,
        };
        let response = async {
            match adapter::build_update_offset_request(&request) {
                Ok(input) => match self
                    .validate_client_context(&context)
                    .and_then(|_| self.guards.try_consumer(estimated_protobuf_retained_bytes(&request)))
                {
                    Ok(_permit) => match self
                        .authorize_contexts(&context, principal.as_ref(), &auth::update_offset_contexts(&input))
                        .await
                    {
                        Ok(()) => match self.processor.update_offset(&context.without_principal(), input).await {
                            Ok(plan) => adapter::build_update_offset_response(&plan),
                            Err(error) => adapter::error_update_offset_response(ProxyStatusMapper::from_error(&error)),
                        },
                        Err(error) => adapter::error_update_offset_response(ProxyStatusMapper::from_error(&error)),
                    },
                    Err(error) => adapter::error_update_offset_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_update_offset_response(ProxyStatusMapper::from_error(&error)),
            }
        }
        .instrument(observation.span())
        .await;
        let status = response.status.clone().unwrap_or_else(ProxyStatusMapper::ok);
        self.finish_unary_payload(&observation, &status).await;
        Ok(Response::new(response))
    }

    async fn get_offset(
        &self,
        request: Request<v2::GetOffsetRequest>,
    ) -> Result<Response<v2::GetOffsetResponse>, Status> {
        let (observation, context, principal, request) = match self
            .enter_unary("GetOffset", request, adapter::error_get_offset_response)
            .await
        {
            Ok(state) => state,
            Err(response) => return response,
        };
        let response = async {
            match adapter::build_get_offset_request(&request) {
                Ok(input) => match self
                    .validate_client_context(&context)
                    .and_then(|_| self.guards.try_consumer(estimated_protobuf_retained_bytes(&request)))
                {
                    Ok(_permit) => match self
                        .authorize_contexts(&context, principal.as_ref(), &auth::get_offset_contexts(&input))
                        .await
                    {
                        Ok(()) => match self.processor.get_offset(&context.without_principal(), input).await {
                            Ok(plan) => adapter::build_get_offset_response(&plan),
                            Err(error) => adapter::error_get_offset_response(ProxyStatusMapper::from_error(&error)),
                        },
                        Err(error) => adapter::error_get_offset_response(ProxyStatusMapper::from_error(&error)),
                    },
                    Err(error) => adapter::error_get_offset_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_get_offset_response(ProxyStatusMapper::from_error(&error)),
            }
        }
        .instrument(observation.span())
        .await;
        let status = response.status.clone().unwrap_or_else(ProxyStatusMapper::ok);
        self.finish_unary_payload(&observation, &status).await;
        Ok(Response::new(response))
    }

    async fn query_offset(
        &self,
        request: Request<v2::QueryOffsetRequest>,
    ) -> Result<Response<v2::QueryOffsetResponse>, Status> {
        let (observation, context, principal, request) = match self
            .enter_unary("QueryOffset", request, adapter::error_query_offset_response)
            .await
        {
            Ok(state) => state,
            Err(response) => return response,
        };
        let response = async {
            match adapter::build_query_offset_request(&request) {
                Ok(input) => match self
                    .validate_client_context(&context)
                    .and_then(|_| self.guards.try_consumer(estimated_protobuf_retained_bytes(&request)))
                {
                    Ok(_permit) => match self
                        .authorize_contexts(&context, principal.as_ref(), &auth::query_offset_contexts(&input))
                        .await
                    {
                        Ok(()) => match self.processor.query_offset(&context.without_principal(), input).await {
                            Ok(plan) => adapter::build_query_offset_response(&plan),
                            Err(error) => adapter::error_query_offset_response(ProxyStatusMapper::from_error(&error)),
                        },
                        Err(error) => adapter::error_query_offset_response(ProxyStatusMapper::from_error(&error)),
                    },
                    Err(error) => adapter::error_query_offset_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_query_offset_response(ProxyStatusMapper::from_error(&error)),
            }
        }
        .instrument(observation.span())
        .await;
        let status = response.status.clone().unwrap_or_else(ProxyStatusMapper::ok);
        self.finish_unary_payload(&observation, &status).await;
        Ok(Response::new(response))
    }

    async fn end_transaction(
        &self,
        request: Request<v2::EndTransactionRequest>,
    ) -> Result<Response<v2::EndTransactionResponse>, Status> {
        let (observation, context, principal, request) = match self
            .enter_unary("EndTransaction", request, adapter::error_end_transaction_response)
            .await
        {
            Ok(state) => state,
            Err(response) => return response,
        };

        let response = async {
            match self
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
                    Ok(()) => match self.guards.try_producer(estimated_protobuf_retained_bytes(&request)) {
                        Ok(_permit) => match self
                            .processor
                            .end_transaction(&context.without_principal(), input.clone())
                            .await
                        {
                            Ok(plan) => {
                                self.reconcile_end_transaction_result(&context, &input, &plan);
                                adapter::build_end_transaction_response(&plan)
                            }
                            Err(error) => {
                                adapter::error_end_transaction_response(ProxyStatusMapper::from_error(&error))
                            }
                        },
                        Err(error) => adapter::error_end_transaction_response(ProxyStatusMapper::from_error(&error)),
                    },
                    Err(error) => adapter::error_end_transaction_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_end_transaction_response(ProxyStatusMapper::from_error(&error)),
            }
        }
        .instrument(observation.span())
        .await;

        let status = response.status.clone().unwrap_or_else(ProxyStatusMapper::ok);
        self.finish_unary_payload(&observation, &status).await;
        Ok(Response::new(response))
    }

    async fn telemetry(
        &self,
        request: Request<tonic::Streaming<v2::TelemetryCommand>>,
    ) -> Result<Response<Self::TelemetryStream>, Status> {
        let (observation, context, principal, inbound) = match self
            .enter_stream("Telemetry", request, |status| {
                self.status_stream(Self::telemetry_status(status))
            })
            .await
        {
            Ok(state) => state,
            Err(response) => return response,
        };
        let inbound = inbound;
        let permit = match self
            .validate_client_context(&context)
            .and_then(|_| self.guards.try_client_manager(context.client_id().map_or(0, str::len)))
        {
            Ok(permit) => permit,
            Err(error) => {
                let status = ProxyStatusMapper::from_error(&error);
                self.finish_stream_payload(&observation, &status).await;
                return Ok(Response::new(self.status_stream(Self::telemetry_status(status))));
            }
        };

        self.sessions.upsert_from_context(&context);
        let Some(client_id) = context.client_id().map(ToOwned::to_owned) else {
            let status = ProxyStatusMapper::from_error(&ProxyError::ClientIdRequired);
            self.finish_stream_payload(&observation, &status).await;
            return Ok(Response::new(self.status_stream(Self::telemetry_status(status))));
        };
        let outbound = match self.guards.telemetry_queue(&client_id) {
            Ok(outbound) => outbound,
            Err(error) => {
                let status = ProxyStatusMapper::from_error(&error);
                self.finish_stream_payload(&observation, &status).await;
                return Ok(Response::new(self.status_stream(Self::telemetry_status(status))));
            }
        };
        self.sessions.bind_telemetry_link(client_id.clone(), outbound.clone());
        self.dispatch_due_prepared_transaction_recoveries_for_client(Some(client_id.as_str()));

        self.finish_stream_payload(&observation, &ProxyStatusMapper::ok()).await;
        let state = TelemetryStreamState {
            service: self.clone(),
            context,
            principal,
            client_id,
            _permit: permit,
            outbound,
            inbound,
            done: false,
        };
        let stream = stream::unfold(state, |mut state| async move {
            if state.done {
                return None;
            }

            tokio::select! {
                outbound = state.outbound.recv() => {
                    outbound.map(|command| (Ok(command), state))
                }
                inbound_item = state.inbound.next() => {
                    match inbound_item {
                        Some(Ok(command)) => {
                            let response = match state
                                .service
                                .guards
                                .try_telemetry_command(estimated_protobuf_retained_bytes(&command))
                            {
                                Ok(_command_permit) => {
                                    state
                                        .service
                                        .handle_telemetry_command(
                                            &state.context,
                                            state.principal.as_ref(),
                                            command,
                                        )
                                        .await
                                }
                                Err(error) => {
                                    Self::telemetry_status(ProxyStatusMapper::from_error(&error))
                                }
                            };
                            Some((Ok(response), state))
                        }
                        Some(Err(error)) => {
                            state.done = true;
                            Some((Err(error), state))
                        }
                        None => None,
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn notify_client_termination(
        &self,
        request: Request<v2::NotifyClientTerminationRequest>,
    ) -> Result<Response<v2::NotifyClientTerminationResponse>, Status> {
        let (observation, context, principal, request) = match self
            .enter_unary("NotifyClientTermination", request, |status| {
                v2::NotifyClientTerminationResponse { status: Some(status) }
            })
            .await
        {
            Ok(state) => state,
            Err(response) => return response,
        };
        let status = async {
            match self.validate_client_context(&context) {
                Ok(client_id) => match self
                    .guards
                    .try_client_manager(estimated_protobuf_retained_bytes(&request))
                {
                    Ok(_permit) => match self
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
                },
                Err(error) => ProxyStatusMapper::from_error(&error),
            }
        }
        .instrument(observation.span())
        .await;

        self.finish_unary_payload(&observation, &status).await;
        Ok(Response::new(v2::NotifyClientTerminationResponse {
            status: Some(status),
        }))
    }

    async fn change_invisible_duration(
        &self,
        request: Request<v2::ChangeInvisibleDurationRequest>,
    ) -> Result<Response<v2::ChangeInvisibleDurationResponse>, Status> {
        let (observation, context, principal, request) = match self
            .enter_unary(
                "ChangeInvisibleDuration",
                request,
                adapter::error_change_invisible_duration_response,
            )
            .await
        {
            Ok(state) => state,
            Err(response) => return response,
        };

        let response = async {
            match self
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
                    Ok(()) => match self.guards.try_consumer(estimated_protobuf_retained_bytes(&request)) {
                        Ok(_permit) => match self
                            .processor
                            .change_invisible_duration(&context.without_principal(), input.clone())
                            .await
                        {
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
                    Err(error) => {
                        adapter::error_change_invisible_duration_response(ProxyStatusMapper::from_error(&error))
                    }
                },
                Err(error) => adapter::error_change_invisible_duration_response(ProxyStatusMapper::from_error(&error)),
            }
        }
        .instrument(observation.span())
        .await;

        let status = response.status.clone().unwrap_or_else(ProxyStatusMapper::ok);
        self.finish_unary_payload(&observation, &status).await;
        Ok(Response::new(response))
    }

    async fn recall_message(
        &self,
        request: Request<v2::RecallMessageRequest>,
    ) -> Result<Response<v2::RecallMessageResponse>, Status> {
        let (observation, context, principal, request) = match self
            .enter_unary("RecallMessage", request, adapter::error_recall_message_response)
            .await
        {
            Ok(state) => state,
            Err(response) => return response,
        };

        let response = async {
            match self
                .validate_client_context(&context)
                .and_then(|_| adapter::build_recall_message_request(&request))
            {
                Ok(input) => match self
                    .authorize_contexts(&context, principal.as_ref(), &auth::recall_message_contexts(&input))
                    .await
                {
                    Ok(()) => match self.guards.try_producer(estimated_protobuf_retained_bytes(&request)) {
                        Ok(_permit) => match self.processor.recall_message(&context.without_principal(), input).await {
                            Ok(plan) => adapter::build_recall_message_response(&plan),
                            Err(error) => adapter::error_recall_message_response(ProxyStatusMapper::from_error(&error)),
                        },
                        Err(error) => adapter::error_recall_message_response(ProxyStatusMapper::from_error(&error)),
                    },
                    Err(error) => adapter::error_recall_message_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_recall_message_response(ProxyStatusMapper::from_error(&error)),
            }
        }
        .instrument(observation.span())
        .await;

        let status = response.status.clone().unwrap_or_else(ProxyStatusMapper::ok);
        self.finish_unary_payload(&observation, &status).await;
        Ok(Response::new(response))
    }

    async fn sync_lite_subscription(
        &self,
        request: Request<v2::SyncLiteSubscriptionRequest>,
    ) -> Result<Response<v2::SyncLiteSubscriptionResponse>, Status> {
        let (observation, context, principal, request) = match self
            .enter_unary("SyncLiteSubscription", request, |status| {
                v2::SyncLiteSubscriptionResponse { status: Some(status) }
            })
            .await
        {
            Ok(state) => state,
            Err(response) => return response,
        };

        let status = async {
            match self.validate_client_context(&context).and_then(|client_id| {
                crate::session::build_lite_subscription_sync_request(&request).map(|input| (client_id, input))
            }) {
                Ok((client_id, input)) => match self
                    .guards
                    .try_client_manager(estimated_protobuf_retained_bytes(&request))
                {
                    Ok(_permit) => match self
                        .authorize_contexts(
                            &context,
                            principal.as_ref(),
                            &auth::sync_lite_subscription_contexts(&request),
                        )
                        .await
                    {
                        Ok(()) => {
                            self.sessions.upsert_from_context(&context);
                            let settings = self.sessions.settings_for_client(client_id);
                            match self
                                .sessions
                                .validate_lite_subscription_sync(client_id, &input, settings.as_ref())
                            {
                                Ok(()) => match self
                                    .processor
                                    .sync_lite_subscription(&context.without_principal(), client_id, input.clone())
                                    .await
                                {
                                    Ok(()) => self
                                        .sessions
                                        .sync_lite_subscription(client_id, input, settings.as_ref())
                                        .map(|_| ProxyStatusMapper::ok())
                                        .unwrap_or_else(|error| ProxyStatusMapper::from_error(&error)),
                                    Err(error) => ProxyStatusMapper::from_error(&error),
                                },
                                Err(error) => ProxyStatusMapper::from_error(&error),
                            }
                        }
                        Err(error) => ProxyStatusMapper::from_error(&error),
                    },
                    Err(error) => ProxyStatusMapper::from_error(&error),
                },
                Err(error) => ProxyStatusMapper::from_error(&error),
            }
        }
        .instrument(observation.span())
        .await;

        self.finish_unary_payload(&observation, &status).await;
        Ok(Response::new(v2::SyncLiteSubscriptionResponse { status: Some(status) }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use futures::StreamExt;
    use hmac::digest::KeyInit;
    use hmac::Hmac;
    use hmac::Mac;
    use rocketmq_auth::Acl;
    use rocketmq_auth::Decision;
    use rocketmq_auth::Policy;
    use rocketmq_auth::Resource;
    use rocketmq_auth::User;
    use rocketmq_auth::UserStatus;
    use rocketmq_auth::UserType;
    use rocketmq_model::result::SendResult;
    use rocketmq_model::result::SendStatus;
    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
    use rocketmq_protocol::protocol::route::route_data_view::QueueData;
    use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
    use rocketmq_security_api::Action;
    use sha1::Sha1;
    use tonic::metadata::MetadataValue;
    use tonic::Request;

    use super::decode_gzip_message_bodies;
    use super::ProxyGrpcService;
    use super::DEFAULT_CONSUMER_CUSTOMIZED_BACKOFF_MS;
    use super::DEFAULT_CONSUMER_MAX_ATTEMPTS;
    use super::DEFAULT_CONSUMER_RECEIVE_BATCH_SIZE;
    use super::DEFAULT_MAX_BODY_SIZE_BYTES;
    use super::DEFAULT_PRODUCER_MAX_ATTEMPTS;
    use crate::auth::ProxyAuthRuntime;
    use crate::config::ProxyAuthConfig;
    use crate::config::ProxyConfig;
    use crate::grpc::adapter;
    use crate::observability::ProxyHook;
    use crate::observability::ProxyHookChain;
    use crate::observability::ProxyRequestOutcome;
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
    use crate::session::TelemetryCommandKind;
    use crate::status::ProxyPayloadStatus;
    use crate::status::ProxyStatusMapper;

    fn gzip_body(body: &[u8]) -> Bytes {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(body).expect("write gzip body");
        Bytes::from(encoder.finish().expect("finish gzip body"))
    }

    #[test]
    fn gzip_message_body_decodes_to_identity_with_a_hard_limit() {
        let request = v2::SendMessageRequest {
            messages: vec![v2::Message {
                system_properties: Some(v2::SystemProperties {
                    body_encoding: v2::Encoding::Gzip as i32,
                    ..Default::default()
                }),
                body: gzip_body(b"hello"),
                ..Default::default()
            }],
        };

        let decoded = decode_gzip_message_bodies(request, 5).expect("decode gzip");

        assert_eq!(decoded.messages[0].body.as_ref(), b"hello");
        assert_eq!(
            decoded.messages[0]
                .system_properties
                .as_ref()
                .expect("system properties")
                .body_encoding,
            v2::Encoding::Identity as i32
        );
    }

    #[test]
    fn gzip_message_body_rejects_invalid_and_oversized_payloads() {
        let request_with = |body: Bytes| v2::SendMessageRequest {
            messages: vec![v2::Message {
                system_properties: Some(v2::SystemProperties {
                    body_encoding: v2::Encoding::Gzip as i32,
                    ..Default::default()
                }),
                body,
                ..Default::default()
            }],
        };

        assert!(decode_gzip_message_bodies(request_with(Bytes::from_static(b"invalid")), 16).is_err());
        assert!(decode_gzip_message_bodies(request_with(gzip_body(b"too large")), 4).is_err());
    }
    use crate::PreparedTransactionRegistration;
    use rocketmq_proxy_core::ProxyContext as CoreProxyContext;
    use rocketmq_proxy_core::ProxyMessage;
    use rocketmq_proxy_core::ProxyMessageExt;

    struct PartialMessageService;

    #[derive(Default)]
    struct TestConsumerService {
        dlq_requests: Mutex<Vec<ForwardMessageToDeadLetterQueueRequest>>,
        updated_offsets: Mutex<Vec<UpdateOffsetRequest>>,
        change_invisible_requests: Mutex<Vec<ChangeInvisibleDurationRequest>>,
        change_invisible_called: tokio::sync::Notify,
    }

    #[derive(Default)]
    struct TestTransactionService {
        requests: Mutex<Vec<EndTransactionRequest>>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct HookEvent {
        phase: &'static str,
        rpc_name: &'static str,
        client_id: Option<String>,
        principal: Option<String>,
        outcome_code: Option<i32>,
    }

    #[derive(Default)]
    struct RecordingHook {
        events: Mutex<Vec<HookEvent>>,
    }

    impl RecordingHook {
        fn events(&self) -> Vec<HookEvent> {
            self.events.lock().expect("hook events mutex poisoned").clone()
        }
    }

    #[async_trait]
    impl ProxyHook for RecordingHook {
        async fn before_request(&self, context: &crate::context::ProxyContext) -> crate::error::ProxyResult<()> {
            self.events.lock().expect("hook events mutex poisoned").push(HookEvent {
                phase: "before",
                rpc_name: context.rpc_name(),
                client_id: context.client_id().map(str::to_owned),
                principal: context
                    .authenticated_principal()
                    .map(|principal| principal.username().to_owned()),
                outcome_code: None,
            });
            Ok(())
        }

        async fn after_request(
            &self,
            context: &crate::context::ProxyContext,
            outcome: &ProxyRequestOutcome,
        ) -> crate::error::ProxyResult<()> {
            self.events.lock().expect("hook events mutex poisoned").push(HookEvent {
                phase: "after",
                rpc_name: context.rpc_name(),
                client_id: context.client_id().map(str::to_owned),
                principal: context
                    .authenticated_principal()
                    .map(|principal| principal.username().to_owned()),
                outcome_code: match outcome {
                    ProxyRequestOutcome::Payload(status) => Some(status.code()),
                    ProxyRequestOutcome::Transport { .. } => None,
                },
            });
            Ok(())
        }
    }

    impl MessageService for PartialMessageService {
        fn send_message<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            request: &'a SendMessageRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, Vec<SendMessageResultEntry>> {
            Box::pin(async move {
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
            })
        }

        fn recall_message<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            request: &'a crate::processor::RecallMessageRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, crate::processor::RecallMessagePlan> {
            Box::pin(async move {
                Ok(crate::processor::RecallMessagePlan {
                    status: ProxyStatusMapper::ok_payload(),
                    message_id: request.recall_handle.clone(),
                })
            })
        }
    }

    impl ConsumerService for TestConsumerService {
        fn receive_message<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            _request: &'a ReceiveMessageRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, ReceiveMessagePlan> {
            Box::pin(async {
                let mut payload = ProxyMessage::new("TopicA", b"hello".to_vec());
                payload.put_property("POP_CK", "receipt-handle");
                let message = ProxyMessageExt {
                    message: payload,
                    msg_id: "server-msg-id".to_owned(),
                    queue_id: 3,
                    queue_offset: 42,
                    reconsume_times: 1,
                    ..ProxyMessageExt::default()
                };

                Ok(ReceiveMessagePlan {
                    status: ProxyStatusMapper::ok_payload(),
                    delivery_timestamp_ms: Some(1_710_000_000_000),
                    messages: vec![ReceivedMessage {
                        message,
                        invisible_duration: std::time::Duration::from_secs(30),
                    }],
                })
            })
        }

        fn pull_message<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            request: &'a PullMessageRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, PullMessagePlan> {
            Box::pin(async move {
                let message = ProxyMessageExt {
                    message: ProxyMessage::new(request.target.topic.to_string(), b"pull".to_vec()),
                    msg_id: "pull-msg-id".to_owned(),
                    queue_id: request.target.queue_id,
                    queue_offset: request.offset,
                    ..ProxyMessageExt::default()
                };

                Ok(PullMessagePlan {
                    status: ProxyStatusMapper::ok_payload(),
                    next_offset: request.offset + 1,
                    min_offset: 0,
                    max_offset: request.offset + 1024,
                    messages: vec![message],
                })
            })
        }

        fn ack_message<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            request: &'a AckMessageRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, Vec<AckMessageResultEntry>> {
            Box::pin(async move {
                Ok(request
                    .entries
                    .iter()
                    .map(|entry| AckMessageResultEntry {
                        message_id: entry.message_id.clone(),
                        receipt_handle: entry.receipt_handle.clone(),
                        status: ProxyStatusMapper::ok_payload(),
                    })
                    .collect())
            })
        }

        fn forward_message_to_dead_letter_queue<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            request: &'a ForwardMessageToDeadLetterQueueRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, ForwardMessageToDeadLetterQueuePlan> {
            Box::pin(async move {
                self.dlq_requests
                    .lock()
                    .expect("dlq requests mutex poisoned")
                    .push(request.clone());
                Ok(ForwardMessageToDeadLetterQueuePlan {
                    status: ProxyStatusMapper::ok_payload(),
                })
            })
        }

        fn change_invisible_duration<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            request: &'a ChangeInvisibleDurationRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, ChangeInvisibleDurationPlan> {
            Box::pin(async move {
                self.change_invisible_requests
                    .lock()
                    .expect("change invisible requests mutex poisoned")
                    .push(request.clone());
                self.change_invisible_called.notify_one();
                Ok(ChangeInvisibleDurationPlan {
                    status: ProxyStatusMapper::ok_payload(),
                    receipt_handle: format!("{}-renewed", request.receipt_handle),
                })
            })
        }

        fn update_offset<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            request: &'a UpdateOffsetRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, UpdateOffsetPlan> {
            Box::pin(async move {
                self.updated_offsets
                    .lock()
                    .expect("updated offsets mutex poisoned")
                    .push(request.clone());
                Ok(UpdateOffsetPlan {
                    status: ProxyStatusMapper::ok_payload(),
                })
            })
        }

        fn get_offset<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            _request: &'a GetOffsetRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, GetOffsetPlan> {
            Box::pin(async {
                Ok(GetOffsetPlan {
                    status: ProxyStatusMapper::ok_payload(),
                    offset: 42,
                })
            })
        }

        fn query_offset<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            request: &'a QueryOffsetRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, QueryOffsetPlan> {
            Box::pin(async move {
                let offset = match request.policy {
                    QueryOffsetPolicy::Beginning => 0,
                    QueryOffsetPolicy::End => 128,
                    QueryOffsetPolicy::Timestamp => request.timestamp_ms.unwrap_or_default(),
                };
                Ok(QueryOffsetPlan {
                    status: ProxyStatusMapper::ok_payload(),
                    offset,
                })
            })
        }
    }

    impl TransactionService for TestTransactionService {
        fn transaction_producer_group(&self, context: &CoreProxyContext) -> Option<String> {
            Some(format!(
                "PROXY_SEND-{}",
                context.client_id().unwrap_or(context.request_id())
            ))
        }

        fn end_transaction<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            request: &'a EndTransactionRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, EndTransactionPlan> {
            Box::pin(async move {
                self.requests
                    .lock()
                    .expect("transaction service mutex poisoned")
                    .push(request.clone());
                Ok(EndTransactionPlan {
                    status: ProxyStatusMapper::ok_payload(),
                })
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
        test_service_with_config_and_all_services(
            ProxyConfig::default(),
            route_service,
            metadata_service,
            message_service,
            consumer_service,
            transaction_service,
        )
    }

    fn test_service_with_config_and_all_services(
        config: ProxyConfig,
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
        ProxyGrpcService::new(Arc::new(config), processor, ClientSessionRegistry::default())
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

    fn receive_message_request(client_id: &'static str) -> Request<v2::ReceiveMessageRequest> {
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
            .insert("x-mq-client-id", MetadataValue::from_static(client_id));
        request
    }

    const AUTH_TEST_DATETIME: &str = "20260322T010203Z";

    async fn test_auth_runtime(authentication_enabled: bool, authorization_enabled: bool) -> ProxyAuthRuntime {
        let runtime = rocketmq_runtime::RuntimeContext::from_current("proxy-grpc-auth-test");
        ProxyAuthRuntime::from_proxy_config(
            &ProxyAuthConfig {
                authentication_enabled,
                authorization_enabled,
                auth_config_path: format!("target/proxy-auth-tests-{}", uuid::Uuid::new_v4()),
                ..ProxyAuthConfig::default()
            },
            &runtime.service_context("proxy-grpc-auth"),
        )
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
                rocketmq_auth::SubjectType::User,
                Policy::of(vec![Resource::of_topic(topic)], actions, None, Decision::Allow),
            ))
            .await
            .expect("acl should be created");
    }

    async fn allow_group_actions(auth_runtime: &ProxyAuthRuntime, username: &str, group: &str, actions: Vec<Action>) {
        auth_runtime
            .create_acl(Acl::of(
                username,
                rocketmq_auth::SubjectType::User,
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
    async fn query_route_rejects_invalid_grpc_timeout_as_transport_status() {
        let route_service = StaticRouteService::default();
        route_service.insert(ResourceIdentity::new("", "TopicA"), sample_route());
        let service = test_service(route_service, StaticMetadataService::default());

        let mut request = Request::new(v2::QueryRouteRequest {
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            endpoints: Some(v2::Endpoints {
                scheme: v2::AddressScheme::IPv4 as i32,
                addresses: vec![v2::Address {
                    host: "127.0.0.1".to_owned(),
                    port: 8080,
                }],
            }),
        });
        request
            .metadata_mut()
            .insert("grpc-timeout", MetadataValue::from_static("bad-timeout"));

        let status = service
            .query_route(request)
            .await
            .expect_err("invalid timeout metadata should fail ingress");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("grpc-timeout"));
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
    async fn query_route_observability_records_hook_and_metrics() {
        let route_service = StaticRouteService::default();
        route_service.insert(ResourceIdentity::new("", "TopicA"), sample_route());
        let hook = Arc::new(RecordingHook::default());
        let service = test_service(route_service, StaticMetadataService::default())
            .with_hooks(ProxyHookChain::new(vec![hook.clone()]));

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
        assert_eq!(
            response.status.as_ref().map(|status| status.code),
            Some(v2::Code::Ok as i32)
        );

        let snapshot = service.metrics_snapshot();
        assert_eq!(snapshot.auth, None);
        let rpc = snapshot
            .rpcs
            .iter()
            .find(|rpc| rpc.rpc_name == "QueryRoute")
            .expect("query route metrics should exist");
        assert_eq!(rpc.started, 1);
        assert_eq!(rpc.completed, 1);
        assert_eq!(rpc.succeeded, 1);
        assert_eq!(rpc.payload_failures, 0);
        assert_eq!(rpc.transport_failures, 0);

        let events = hook.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].phase, "before");
        assert_eq!(events[0].rpc_name, "QueryRoute");
        assert_eq!(events[0].client_id.as_deref(), Some("client-a"));
        assert_eq!(events[0].principal, None);
        assert_eq!(events[1].phase, "after");
        assert_eq!(events[1].rpc_name, "QueryRoute");
        assert_eq!(events[1].outcome_code, Some(v2::Code::Ok as i32));
    }

    #[tokio::test]
    async fn query_route_observability_records_payload_failure_for_auth_rejection() {
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
        assert_eq!(
            response.status.as_ref().map(|status| status.code),
            Some(v2::Code::Unauthorized as i32)
        );

        let snapshot = service.metrics_snapshot();
        let auth = snapshot.auth.expect("auth metrics should be exported");
        assert_eq!(auth.whitelist_misses, 1);
        assert_eq!(auth.authentication_failures, 1);
        let rpc = snapshot
            .rpcs
            .iter()
            .find(|rpc| rpc.rpc_name == "QueryRoute")
            .expect("query route metrics should exist");
        assert_eq!(rpc.started, 1);
        assert_eq!(rpc.completed, 1);
        assert_eq!(rpc.succeeded, 0);
        assert_eq!(rpc.payload_failures, 1);
        assert_eq!(rpc.transport_failures, 0);
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
        let mut context = service
            .context("Telemetry", &auth_request)
            .expect("context should be constructed");
        let principal = service
            .authenticate_request(tracing::Span::none(), &mut context, &auth_request)
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
                body: Bytes::from_static(b"hello"),
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
        let service = test_service_with_all_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            Arc::new(DefaultConsumerService),
            Arc::new(TestTransactionService::default()),
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
                body: Bytes::from_static(b"hello"),
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
        let receiver = service
            .guards
            .telemetry_queue("client-a")
            .expect("client telemetry queue");
        service.sessions.bind_telemetry_link("client-a", receiver.clone());
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
                    body: Bytes::from_static(b"hello"),
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
                    body: Bytes::from_static(b"hello"),
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
                    body: Bytes::from_static(b"world"),
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
                    body: Bytes::from_static(b"hello"),
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
                    body: Bytes::from_static(b"world"),
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
    async fn receive_stream_holds_response_budget_and_drain_admission_until_drop() {
        let mut config = ProxyConfig::default();
        config.runtime.consumer_response_permits = 1;
        config.runtime.consumer_response_bytes = 1024 * 1024;
        let service = test_service_with_config_and_all_services(
            config,
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            Arc::new(TestConsumerService::default()),
            Arc::new(DefaultTransactionService),
        );

        let first = service
            .receive_message(receive_message_request("stream-a"))
            .await
            .expect("first response stream")
            .into_inner();
        assert_eq!(service.drain.snapshot(&service.sessions).pending.rpc_in_flight, 1);
        let occupied = service.consumer_response_budget_snapshot();
        assert_eq!(occupied.current_count, 1);
        assert!(occupied.current_bytes > 0);

        let mut rejected = service
            .receive_message(receive_message_request("stream-b"))
            .await
            .expect("budget rejection response")
            .into_inner();
        let rejection = rejected.next().await.expect("status frame").expect("valid frame");
        assert_eq!(
            match rejection.content.expect("status content") {
                v2::receive_message_response::Content::Status(status) => status.code,
                other => panic!("expected status frame, got {other:?}"),
            },
            v2::Code::TooManyRequests as i32
        );
        assert_eq!(service.consumer_response_budget_snapshot().rejected_count, 1);

        drop(first);
        assert_eq!(service.drain.snapshot(&service.sessions).pending.rpc_in_flight, 0);
        assert_eq!(service.consumer_response_budget_snapshot().current_count, 0);
        let mut after_drop = service
            .receive_message(receive_message_request("stream-c"))
            .await
            .expect("response stream after release")
            .into_inner();
        assert!(matches!(
            after_drop
                .next()
                .await
                .expect("first frame")
                .expect("valid frame")
                .content,
            Some(v2::receive_message_response::Content::DeliveryTimestamp(_))
        ));
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

        let backoff_policy = merged.backoff_policy.clone().expect("consumer retry policy");
        let subscription = match merged.pub_sub.unwrap() {
            v2::settings::PubSub::Subscription(subscription) => subscription,
            _ => panic!("expected subscription settings"),
        };
        assert_eq!(
            subscription.receive_batch_size,
            Some(DEFAULT_CONSUMER_RECEIVE_BATCH_SIZE)
        );
        assert_eq!(
            subscription.long_polling_timeout,
            Some(prost_types::Duration { seconds: 20, nanos: 0 })
        );
        assert_eq!(backoff_policy.max_attempts, DEFAULT_CONSUMER_MAX_ATTEMPTS);
        match backoff_policy.strategy.expect("consumer retry strategy") {
            v2::retry_policy::Strategy::CustomizedBackoff(backoff) => {
                assert_eq!(backoff.next.len(), DEFAULT_CONSUMER_CUSTOMIZED_BACKOFF_MS.len());
                assert_eq!(backoff.next[0], prost_types::Duration { seconds: 1, nanos: 0 });
                assert_eq!(
                    backoff.next[17],
                    prost_types::Duration {
                        seconds: 7200,
                        nanos: 0
                    }
                );
            }
            other => panic!("unexpected consumer retry strategy: {other:?}"),
        }
    }

    #[test]
    fn telemetry_settings_fill_producer_defaults() {
        let service = test_service(StaticRouteService::default(), StaticMetadataService::default());
        let merged = service.merged_telemetry_settings(&v2::Settings {
            client_type: Some(v2::ClientType::Producer as i32),
            access_point: None,
            backoff_policy: None,
            request_timeout: None,
            pub_sub: Some(v2::settings::PubSub::Publishing(v2::Publishing {
                topics: vec![v2::Resource {
                    resource_namespace: String::new(),
                    name: "TopicA".to_owned(),
                }],
                max_body_size: 0,
                validate_message_type: false,
            })),
            user_agent: None,
            metric: None,
        });

        let backoff_policy = merged.backoff_policy.clone().expect("producer retry policy");
        let publishing = match merged.pub_sub.unwrap() {
            v2::settings::PubSub::Publishing(publishing) => publishing,
            _ => panic!("expected publishing settings"),
        };
        assert_eq!(publishing.max_body_size, DEFAULT_MAX_BODY_SIZE_BYTES);
        assert!(publishing.validate_message_type);
        assert_eq!(backoff_policy.max_attempts, DEFAULT_PRODUCER_MAX_ATTEMPTS);
        match backoff_policy.strategy.expect("producer retry strategy") {
            v2::retry_policy::Strategy::ExponentialBackoff(backoff) => {
                assert_eq!(
                    backoff.initial,
                    Some(prost_types::Duration {
                        seconds: 0,
                        nanos: 10_000_000
                    })
                );
                assert_eq!(backoff.max, Some(prost_types::Duration { seconds: 1, nanos: 0 }));
                assert_eq!(backoff.multiplier, 2.0);
            }
            other => panic!("unexpected producer retry strategy: {other:?}"),
        }
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
        let receiver = service
            .guards
            .telemetry_queue("client-a")
            .expect("client telemetry queue");
        service.sessions.bind_telemetry_link("client-a", receiver.clone());

        assert!(service.send_reconnect_endpoints_command("client-a", "nonce-a"));
        assert!(service.sessions.has_pending_telemetry_command(
            "client-a",
            TelemetryCommandKind::ReconnectEndpoints,
            "nonce-a",
        ));

        let command = receiver.recv().await.expect("telemetry command should be queued");
        match command.command {
            Some(v2::telemetry_command::Command::ReconnectEndpointsCommand(command)) => {
                assert_eq!(command.nonce, "nonce-a");
            }
            other => panic!("unexpected telemetry command: {other:?}"),
        }
    }

    #[tokio::test]
    async fn print_thread_stack_trace_command_tracks_pending_nonce_and_consumes_client_report() {
        let service = test_service(StaticRouteService::default(), StaticMetadataService::default());
        let receiver = service
            .guards
            .telemetry_queue("client-a")
            .expect("client telemetry queue");
        service.sessions.bind_telemetry_link("client-a", receiver.clone());

        assert!(service.send_print_thread_stack_trace_command("client-a", "nonce-a"));
        assert!(service.sessions.has_pending_telemetry_command(
            "client-a",
            TelemetryCommandKind::PrintThreadStackTrace,
            "nonce-a",
        ));

        let command = receiver.recv().await.expect("telemetry command should be queued");
        match command.command {
            Some(v2::telemetry_command::Command::PrintThreadStackTraceCommand(command)) => {
                assert_eq!(command.nonce, "nonce-a");
            }
            other => panic!("unexpected telemetry command: {other:?}"),
        }

        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));
        let context = service
            .context("Telemetry", &request)
            .expect("context should be constructed");

        let response = service
            .handle_telemetry_command(
                &context,
                None,
                v2::TelemetryCommand {
                    status: None,
                    command: Some(v2::telemetry_command::Command::ThreadStackTrace(v2::ThreadStackTrace {
                        nonce: "nonce-a".to_owned(),
                        thread_stack_trace: Some("trace".to_owned()),
                    })),
                },
            )
            .await;

        assert_eq!(
            response
                .status
                .as_ref()
                .expect("telemetry response should include status")
                .code,
            v2::Code::Ok as i32
        );
        assert!(!service.sessions.has_pending_telemetry_command(
            "client-a",
            TelemetryCommandKind::PrintThreadStackTrace,
            "nonce-a",
        ));
        let report = service
            .sessions
            .thread_stack_trace_report("client-a", "nonce-a")
            .expect("thread stack trace report should be stored");
        assert_eq!(report.thread_stack_trace.as_deref(), Some("trace"));
    }

    #[tokio::test]
    async fn verify_message_result_rejects_unknown_telemetry_nonce() {
        let service = test_service(StaticRouteService::default(), StaticMetadataService::default());
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));
        let context = service
            .context("Telemetry", &request)
            .expect("context should be constructed");

        let response = service
            .handle_telemetry_command(
                &context,
                None,
                v2::TelemetryCommand {
                    status: None,
                    command: Some(v2::telemetry_command::Command::VerifyMessageResult(
                        v2::VerifyMessageResult {
                            nonce: "nonce-missing".to_owned(),
                        },
                    )),
                },
            )
            .await;

        assert_eq!(
            response
                .status
                .as_ref()
                .expect("telemetry response should include status")
                .code,
            v2::Code::BadRequest as i32
        );
    }

    #[tokio::test]
    async fn verify_message_result_stores_report_for_matching_nonce() {
        let service = test_service(StaticRouteService::default(), StaticMetadataService::default());
        let message = v2::Message {
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            user_properties: HashMap::new(),
            system_properties: Some(v2::SystemProperties {
                message_id: "msg-1".to_owned(),
                ..Default::default()
            }),
            body: Bytes::from_static(b"hello"),
        };
        let receiver = service
            .guards
            .telemetry_queue("client-a")
            .expect("client telemetry queue");
        service.sessions.bind_telemetry_link("client-a", receiver.clone());
        assert!(service.send_verify_message_command("client-a", "nonce-a", message));
        let _ = receiver.recv().await.expect("verify message command should be queued");

        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));
        let context = service
            .context("Telemetry", &request)
            .expect("context should be constructed");

        let response = service
            .handle_telemetry_command(
                &context,
                None,
                v2::TelemetryCommand {
                    status: None,
                    command: Some(v2::telemetry_command::Command::VerifyMessageResult(
                        v2::VerifyMessageResult {
                            nonce: "nonce-a".to_owned(),
                        },
                    )),
                },
            )
            .await;

        assert_eq!(
            response
                .status
                .as_ref()
                .expect("telemetry response should include status")
                .code,
            v2::Code::Ok as i32
        );
        let report = service
            .sessions
            .verify_message_report("client-a", "nonce-a")
            .expect("verify message report should be stored");
        assert_eq!(report.nonce, "nonce-a");
    }

    #[tokio::test]
    async fn notify_unsubscribe_lite_command_registers_pending_notice() {
        let service = test_service(StaticRouteService::default(), StaticMetadataService::default());
        let receiver = service
            .guards
            .telemetry_queue("client-a")
            .expect("client telemetry queue");
        service.sessions.bind_telemetry_link("client-a", receiver.clone());

        assert!(service.send_notify_unsubscribe_lite_command("client-a", "lite-a"));
        assert!(service
            .sessions
            .has_pending_lite_unsubscribe_notice("client-a", "lite-a"));

        let command = receiver.recv().await.expect("unsubscribe command should be queued");
        match command.command {
            Some(v2::telemetry_command::Command::NotifyUnsubscribeLiteCommand(command)) => {
                assert_eq!(command.lite_topic, "lite-a");
            }
            other => panic!("unexpected telemetry command: {other:?}"),
        }
    }

    #[tokio::test]
    async fn telemetry_command_state_is_exposed_via_metrics_snapshot() {
        let service = test_service(StaticRouteService::default(), StaticMetadataService::default());
        let receiver = service
            .guards
            .telemetry_queue("client-a")
            .expect("client telemetry queue");
        service.sessions.bind_telemetry_link("client-a", receiver.clone());

        assert!(service.send_print_thread_stack_trace_command("client-a", "nonce-trace"));
        assert!(service.send_notify_unsubscribe_lite_command("client-a", "lite-a"));
        let verify_message = v2::Message {
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            user_properties: HashMap::new(),
            system_properties: Some(v2::SystemProperties {
                message_id: "msg-1".to_owned(),
                ..Default::default()
            }),
            body: Bytes::from_static(b"hello"),
        };
        assert!(service.send_verify_message_command("client-a", "nonce-verify", verify_message));

        for _ in 0..3 {
            let _ = receiver.recv().await.expect("telemetry command should be queued");
        }

        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));
        let context = service
            .context("Telemetry", &request)
            .expect("context should be constructed");

        let _ = service
            .handle_telemetry_command(
                &context,
                None,
                v2::TelemetryCommand {
                    status: None,
                    command: Some(v2::telemetry_command::Command::ThreadStackTrace(v2::ThreadStackTrace {
                        nonce: "nonce-trace".to_owned(),
                        thread_stack_trace: Some("trace".to_owned()),
                    })),
                },
            )
            .await;
        let _ = service
            .handle_telemetry_command(
                &context,
                None,
                v2::TelemetryCommand {
                    status: None,
                    command: Some(v2::telemetry_command::Command::VerifyMessageResult(
                        v2::VerifyMessageResult {
                            nonce: "nonce-verify".to_owned(),
                        },
                    )),
                },
            )
            .await;

        let snapshot = service.metrics_snapshot();
        assert_eq!(snapshot.pending_telemetry_commands, 0);
        assert_eq!(snapshot.thread_stack_trace_reports, 1);
        assert_eq!(snapshot.verify_message_reports, 1);
        assert_eq!(snapshot.pending_lite_unsubscribe_notices, 1);
    }

    #[test]
    fn effective_receive_request_uses_telemetry_settings() {
        let service = test_service(StaticRouteService::default(), StaticMetadataService::default());
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));
        let context = service
            .context("ReceiveMessage", &request)
            .expect("context should be constructed");

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
        assert_eq!(request.long_polling_timeout, std::time::Duration::from_secs(20));
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

    #[tokio::test(start_paused = true)]
    async fn auto_renew_loop_wakes_at_half_of_the_invisible_window() {
        let consumer = Arc::new(TestConsumerService::default());
        let service = test_service_with_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            consumer.clone(),
        );
        service
            .sessions
            .track_receipt_handle(crate::session::ReceiptHandleRegistration {
                client_id: "client-a".to_owned(),
                group: ResourceIdentity::new("", "GroupA"),
                topic: ResourceIdentity::new("", "TopicA"),
                message_id: "msg-1".to_owned(),
                receipt_handle: "handle-1".to_owned(),
                invisible_duration: Duration::from_secs(15),
            });
        let renewal_called = consumer.change_invisible_called.notified();
        let renewal_task = {
            let service = service.clone();
            tokio::spawn(async move { service.run_receipt_renewal_loop().await })
        };
        tokio::task::yield_now().await;

        tokio::time::advance(Duration::from_millis(7_499)).await;
        tokio::task::yield_now().await;
        assert!(consumer
            .change_invisible_requests
            .lock()
            .expect("change invisible requests mutex poisoned")
            .is_empty());

        tokio::time::advance(Duration::from_millis(1)).await;
        renewal_called.await;
        tokio::task::yield_now().await;
        let tracked = service
            .sessions
            .tracked_receipt_handle(
                "client-a",
                &ResourceIdentity::new("", "GroupA"),
                &ResourceIdentity::new("", "TopicA"),
                "msg-1",
            )
            .expect("renewed receipt handle");
        assert_eq!(tracked.receipt_handle, "handle-1-renewed");
        assert_eq!(service.sessions.receipt_renewal_metrics_snapshot().successes, 1);

        renewal_task.abort();
        let _ = renewal_task.await;
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
                body: Bytes::from_static(b"hello"),
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
        let context = service
            .context("Telemetry", &context_request)
            .expect("context should be constructed");
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
        let context = service
            .context("Telemetry", &context_request)
            .expect("context should be constructed");
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
