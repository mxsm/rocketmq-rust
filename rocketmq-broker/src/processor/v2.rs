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

//! Broker-owned aggregate for the formal V2 processor contract.
//!
//! Production composition constructs this graph directly from formal V2 leaves;
//! no V1 aggregate is built or adapted on the listener startup path.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use rocketmq_auth::AuthRuntime;
use rocketmq_auth::RemotingAuthContext;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_store::BrokerStorePort;
use rocketmq_transport::api::v1::command_from_error_with_factory_and_opaque;
use rocketmq_transport::api::v1::command_from_error_with_factory_remark_and_opaque;
use rocketmq_transport::api::v1::request_code_not_supported_with_factory;
use rocketmq_transport::api::v1::request_code_not_supported_with_factory_and_opaque;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::IngressRequestView;
use rocketmq_transport::api::v2::RejectRequestDecision;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestOrdering;
use rocketmq_transport::api::v2::RequestProcessorV2;
use rocketmq_transport::api::v2::ResponsePlan;
use rocketmq_transport::api::v2::ResponseWriteObservationV2;
use tracing::warn;

use super::ack_message_processor::AckMessageProcessor;
use super::admin_broker_processor::AdminBrokerProcessor;
use super::change_invisible_time_processor::ChangeInvisibleTimeProcessor;
use super::client_manage_processor::ClientManageProcessor;
use super::consumer_manage_processor::ConsumerManageProcessor;
use super::end_transaction_processor::EndTransactionProcessor;
use super::fast_failure_dispatch;
use super::fast_failure_queue_kind;
use super::is_privileged_maintenance_request;
use super::lite_manager_processor::LiteManagerProcessor;
use super::lite_subscription_ctl_processor::LiteSubscriptionCtlProcessor;
use super::maintenance_request_processor::MaintenanceRequestProcessor;
use super::notification_processor::NotificationProcessor;
use super::peek_message_processor::PeekMessageProcessor;
use super::polling_info_processor::PollingInfoProcessor;
use super::pop_lite_message_processor::PopLiteMessageProcessor;
use super::pop_message_processor::PopMessageProcessor;
use super::pull_message_processor::PullMessageProcessor;
use super::query_assignment_processor::QueryAssignmentProcessor;
use super::query_message_processor::QueryMessageProcessor;
use super::query_message_processor::QueryMessageStoreCapability;
use super::recall_message_processor::RecallMessageProcessor;
use super::reply_message_processor::ReplyMessageProcessor;
use super::request_ordering;
use super::send_message_processor::SendMessageProcessor;
use crate::latency::broker_fast_failure::BrokerFastFailure;
use crate::processor::response_plan::BrokerResponseParts;
use crate::transaction::transactional_message_service::TransactionalMessageService;

/// Every Broker leaf that implements the formal V2 processor contract.
pub enum BrokerProcessorTypeV2<MS: BrokerStorePort, TS> {
    Send(Arc<SendMessageProcessor<MS, TS>>),
    Pull(Arc<PullMessageProcessor<MS>>),
    Peek(Arc<PeekMessageProcessor<MS>>),
    Pop(Arc<PopMessageProcessor<MS>>),
    PopLite(Arc<PopLiteMessageProcessor<MS>>),
    Ack(Arc<AckMessageProcessor<MS>>),
    ChangeInvisible(Arc<ChangeInvisibleTimeProcessor<MS>>),
    Notification(Arc<NotificationProcessor<MS>>),
    PollingInfo(Arc<PollingInfoProcessor>),
    Reply(Arc<ReplyMessageProcessor<MS, TS>>),
    Recall(Arc<RecallMessageProcessor<MS>>),
    QueryMessage(Arc<QueryMessageProcessor<QueryMessageStoreCapability<MS>>>),
    ClientManage(Arc<ClientManageProcessor<MS>>),
    ConsumerManage(Arc<ConsumerManageProcessor<MS>>),
    QueryAssignment(Arc<QueryAssignmentProcessor>),
    LiteManager(Arc<LiteManagerProcessor<MS>>),
    LiteSubscriptionCtl(Arc<LiteSubscriptionCtlProcessor<MS>>),
    EndTransaction(Arc<EndTransactionProcessor<TS, MS>>),
    Maintenance(Arc<MaintenanceRequestProcessor>),
    AdminBroker(Arc<AdminBrokerProcessor<MS>>),
}

impl<MS, TS> Clone for BrokerProcessorTypeV2<MS, TS>
where
    MS: BrokerStorePort,
{
    fn clone(&self) -> Self {
        match self {
            Self::Send(processor) => Self::Send(Arc::clone(processor)),
            Self::Pull(processor) => Self::Pull(Arc::clone(processor)),
            Self::Peek(processor) => Self::Peek(Arc::clone(processor)),
            Self::Pop(processor) => Self::Pop(Arc::clone(processor)),
            Self::PopLite(processor) => Self::PopLite(Arc::clone(processor)),
            Self::Ack(processor) => Self::Ack(Arc::clone(processor)),
            Self::ChangeInvisible(processor) => Self::ChangeInvisible(Arc::clone(processor)),
            Self::Notification(processor) => Self::Notification(Arc::clone(processor)),
            Self::PollingInfo(processor) => Self::PollingInfo(Arc::clone(processor)),
            Self::Reply(processor) => Self::Reply(Arc::clone(processor)),
            Self::Recall(processor) => Self::Recall(Arc::clone(processor)),
            Self::QueryMessage(processor) => Self::QueryMessage(Arc::clone(processor)),
            Self::ClientManage(processor) => Self::ClientManage(Arc::clone(processor)),
            Self::ConsumerManage(processor) => Self::ConsumerManage(Arc::clone(processor)),
            Self::QueryAssignment(processor) => Self::QueryAssignment(Arc::clone(processor)),
            Self::LiteManager(processor) => Self::LiteManager(Arc::clone(processor)),
            Self::LiteSubscriptionCtl(processor) => Self::LiteSubscriptionCtl(Arc::clone(processor)),
            Self::EndTransaction(processor) => Self::EndTransaction(Arc::clone(processor)),
            Self::Maintenance(processor) => Self::Maintenance(Arc::clone(processor)),
            Self::AdminBroker(processor) => Self::AdminBroker(Arc::clone(processor)),
        }
    }
}

#[cfg(test)]
impl<MS: BrokerStorePort, TS> BrokerProcessorTypeV2<MS, TS> {
    const fn variant_name_for_test(&self) -> &'static str {
        match self {
            Self::Send(_) => "Send",
            Self::Pull(_) => "Pull",
            Self::Peek(_) => "Peek",
            Self::Pop(_) => "Pop",
            Self::PopLite(_) => "PopLite",
            Self::Ack(_) => "Ack",
            Self::ChangeInvisible(_) => "ChangeInvisible",
            Self::Notification(_) => "Notification",
            Self::PollingInfo(_) => "PollingInfo",
            Self::Reply(_) => "Reply",
            Self::Recall(_) => "Recall",
            Self::QueryMessage(_) => "QueryMessage",
            Self::ClientManage(_) => "ClientManage",
            Self::ConsumerManage(_) => "ConsumerManage",
            Self::QueryAssignment(_) => "QueryAssignment",
            Self::LiteManager(_) => "LiteManager",
            Self::LiteSubscriptionCtl(_) => "LiteSubscriptionCtl",
            Self::EndTransaction(_) => "EndTransaction",
            Self::Maintenance(_) => "Maintenance",
            Self::AdminBroker(_) => "AdminBroker",
        }
    }
}

impl<MS, TS> RequestProcessorV2 for BrokerProcessorTypeV2<MS, TS>
where
    MS: BrokerStorePort + Send + Sync + 'static,
    TS: TransactionalMessageService + Send + Sync + 'static,
{
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        match self {
            Self::Send(processor) => processor.process_v2_shared(request).await,
            Self::Pull(processor) => processor.process_v2_shared(request).await,
            Self::Peek(processor) => processor.process_v2_shared(request).await,
            Self::Pop(processor) => processor.process_v2_shared(request).await,
            Self::PopLite(processor) => processor.process_v2_shared(request).await,
            Self::Ack(processor) => processor.process_v2_shared(request).await,
            Self::ChangeInvisible(processor) => processor.process_v2_shared(request).await,
            Self::Notification(processor) => processor.process_v2_shared(request).await,
            Self::PollingInfo(processor) => processor.process_v2_shared(request).await,
            Self::Reply(processor) => processor.process_v2_shared(request).await,
            Self::Recall(processor) => processor.process_v2_shared(request).await,
            Self::QueryMessage(processor) => processor.process_v2_shared(request).await,
            Self::ClientManage(processor) => processor.process_v2_shared(request).await,
            Self::ConsumerManage(processor) => processor.process_v2_shared(request).await,
            Self::QueryAssignment(processor) => processor.process_v2_shared(request).await,
            Self::LiteManager(processor) => processor.process_v2_shared(request).await,
            Self::LiteSubscriptionCtl(processor) => processor.process_v2_shared(request).await,
            Self::EndTransaction(processor) => processor.process_v2_shared(request).await,
            Self::Maintenance(processor) => processor.process_v2_shared(request).await,
            Self::AdminBroker(processor) => processor.process_v2_shared(request).await,
        }
    }

    fn reject_request(&self, _code: i32) -> RejectRequestDecision {
        let response = match self {
            Self::Send(processor) => processor.rejection_response(),
            Self::Pull(processor) => processor.rejection_response(),
            Self::Peek(_)
            | Self::Pop(_)
            | Self::PopLite(_)
            | Self::Ack(_)
            | Self::ChangeInvisible(_)
            | Self::Notification(_)
            | Self::PollingInfo(_)
            | Self::Reply(_)
            | Self::Recall(_)
            | Self::QueryMessage(_)
            | Self::ClientManage(_)
            | Self::ConsumerManage(_)
            | Self::QueryAssignment(_)
            | Self::LiteManager(_)
            | Self::LiteSubscriptionCtl(_)
            | Self::EndTransaction(_)
            | Self::Maintenance(_) => None,
            Self::AdminBroker(_) => None,
        };
        response.map_or(RejectRequestDecision::Proceed, |response| {
            response_rejection(response, "Broker V2 leaf rejection")
        })
    }

    fn observe_response_write(&self, observation: ResponseWriteObservationV2) {
        match self {
            Self::Send(processor) => RequestProcessorV2::observe_response_write(processor.as_ref(), observation),
            Self::Pull(processor) => RequestProcessorV2::observe_response_write(processor.as_ref(), observation),
            Self::Peek(processor) => RequestProcessorV2::observe_response_write(processor.as_ref(), observation),
            Self::Pop(processor) => RequestProcessorV2::observe_response_write(processor.as_ref(), observation),
            Self::PopLite(processor) => RequestProcessorV2::observe_response_write(processor.as_ref(), observation),
            Self::Ack(processor) => RequestProcessorV2::observe_response_write(processor.as_ref(), observation),
            Self::ChangeInvisible(processor) => {
                RequestProcessorV2::observe_response_write(processor.as_ref(), observation);
            }
            Self::Notification(processor) => {
                RequestProcessorV2::observe_response_write(processor.as_ref(), observation);
            }
            Self::PollingInfo(processor) => {
                RequestProcessorV2::observe_response_write(processor.as_ref(), observation);
            }
            Self::Reply(processor) => RequestProcessorV2::observe_response_write(processor.as_ref(), observation),
            Self::Recall(processor) => RequestProcessorV2::observe_response_write(processor.as_ref(), observation),
            Self::QueryMessage(processor) => {
                RequestProcessorV2::observe_response_write(processor.as_ref(), observation);
            }
            Self::ClientManage(processor) => {
                RequestProcessorV2::observe_response_write(processor.as_ref(), observation);
            }
            Self::ConsumerManage(processor) => {
                RequestProcessorV2::observe_response_write(processor.as_ref(), observation);
            }
            Self::QueryAssignment(processor) => {
                RequestProcessorV2::observe_response_write(processor.as_ref(), observation);
            }
            Self::LiteManager(processor) => {
                RequestProcessorV2::observe_response_write(processor.as_ref(), observation);
            }
            Self::LiteSubscriptionCtl(processor) => {
                RequestProcessorV2::observe_response_write(processor.as_ref(), observation);
            }
            Self::EndTransaction(processor) => {
                RequestProcessorV2::observe_response_write(processor.as_ref(), observation);
            }
            Self::Maintenance(processor) => {
                RequestProcessorV2::observe_response_write(processor.as_ref(), observation);
            }
            Self::AdminBroker(_) => {}
        }
    }
}

fn response_rejection(response: RemotingCommand, source: &'static str) -> RejectRequestDecision {
    match ResponsePlan::from_command(response) {
        Ok(plan) => RejectRequestDecision::Reject(plan),
        Err(error) => {
            warn!(%error, source, "Broker V2 rejection response could not become an owned plan");
            fallback_rejection(source)
        }
    }
}

fn fallback_rejection(source: &'static str) -> RejectRequestDecision {
    warn!(source, "Broker V2 rejection is using the canonical fallback response");
    RejectRequestDecision::Reject(ResponsePlan::empty_response(ResponseCode::SystemBusy as i32))
}

/// Code-indexed Broker V2 router over a statically selected formal leaf type.
///
/// The generic form keeps tests and later composition code independent of the
/// concrete enum while production will use [`BrokerProcessorTypeV2`].
pub struct BrokerRequestProcessorV2<P> {
    command_factory: RemotingCommandFactory,
    process_table: Arc<HashMap<i32, P>>,
    default_request_processor: Option<Arc<P>>,
    maintenance_routes: Arc<HashSet<i32>>,
    broker_fast_failure: Option<BrokerFastFailure>,
    auth: BrokerAuthState,
}

#[derive(Clone, Default)]
enum BrokerAuthState {
    #[default]
    Unconfigured,
    DisabledByValidatedConfig,
    Runtime(Arc<AuthRuntime>),
}

impl<P> BrokerRequestProcessorV2<P>
where
    P: Clone,
{
    pub fn new() -> Self {
        Self::new_with_factory(application_remoting_command_factory())
    }

    pub fn new_with_factory(command_factory: RemotingCommandFactory) -> Self {
        Self {
            command_factory,
            process_table: Arc::new(HashMap::new()),
            default_request_processor: None,
            maintenance_routes: Arc::new(HashSet::new()),
            broker_fast_failure: None,
            auth: BrokerAuthState::Unconfigured,
        }
    }

    pub fn register_processor(&mut self, request_code: i32, processor: P) {
        Arc::make_mut(&mut self.process_table).insert(request_code, processor);
    }

    pub fn register_maintenance_processor(&mut self, request_code: i32, processor: P) {
        self.register_processor(request_code, processor);
        Arc::make_mut(&mut self.maintenance_routes).insert(request_code);
    }

    pub fn register_default_processor(&mut self, processor: P) {
        self.default_request_processor = Some(Arc::new(processor));
    }

    pub fn set_broker_fast_failure(&mut self, broker_fast_failure: BrokerFastFailure) {
        if broker_fast_failure.legacy_send_detach_requested() {
            warn!("sendRequestExecutorDetachedEnable is deprecated and ignored by structured V2 fast-failure dispatch");
        }
        self.broker_fast_failure = Some(broker_fast_failure);
    }

    /// Installs the Broker ACL runtime used before any ordinary leaf dispatch.
    pub(crate) fn set_auth_runtime(&mut self, auth_runtime: Arc<AuthRuntime>) {
        self.auth = BrokerAuthState::Runtime(auth_runtime);
    }

    /// Explicitly records that both Broker authentication and authorization
    /// were disabled by validated composition configuration.
    pub(crate) fn set_auth_disabled_by_validated_config(&mut self) {
        self.auth = BrokerAuthState::DisabledByValidatedConfig;
    }

    /// Returns whether composition made an explicit fail-closed ACL decision.
    #[must_use]
    pub(crate) const fn is_auth_configured(&self) -> bool {
        !matches!(self.auth, BrokerAuthState::Unconfigured)
    }
}

impl<P> Default for BrokerRequestProcessorV2<P>
where
    P: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl<MS: BrokerStorePort, TS> BrokerRequestProcessorV2<BrokerProcessorTypeV2<MS, TS>> {
    pub(crate) fn dispatch_processor_variant_for_test(&self, request_code: RequestCode) -> Option<&'static str> {
        self.process_table
            .get(&request_code.to_i32())
            .or(self.default_request_processor.as_deref())
            .map(BrokerProcessorTypeV2::variant_name_for_test)
    }
}

impl<P> Clone for BrokerRequestProcessorV2<P> {
    fn clone(&self) -> Self {
        Self {
            command_factory: self.command_factory,
            process_table: Arc::clone(&self.process_table),
            default_request_processor: self.default_request_processor.clone(),
            maintenance_routes: Arc::clone(&self.maintenance_routes),
            broker_fast_failure: self.broker_fast_failure.clone(),
            auth: self.auth.clone(),
        }
    }
}

impl<P> RequestProcessorV2 for BrokerRequestProcessorV2<P>
where
    P: RequestProcessorV2 + Clone + Sync + 'static,
{
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let original = request.original_identity();
        let request_code = original.original_code();
        let opaque = original.original_opaque();
        if is_privileged_maintenance_request(RequestCode::from(request_code))
            && !self.maintenance_routes.contains(&request_code)
        {
            let error = rocketmq_error::RocketMQError::authentication_failed(
                "Broker maintenance API is disabled or unavailable",
            );
            let response = command_from_error_with_factory_remark_and_opaque(
                &self.command_factory,
                &error,
                error.to_string(),
                opaque,
            );
            return BrokerResponseParts::from_command(response)?.into_handler_outcome();
        }

        if !is_privileged_maintenance_request(RequestCode::from(request_code)) {
            match &self.auth {
                BrokerAuthState::Unconfigured => {
                    let error = rocketmq_error::RocketMQError::authentication_failed(
                        "Broker V2 authentication is not configured",
                    );
                    let response = command_from_error_with_factory_remark_and_opaque(
                        &self.command_factory,
                        &error,
                        error.to_string(),
                        opaque,
                    );
                    return BrokerResponseParts::from_command(response)?.into_handler_outcome();
                }
                BrokerAuthState::DisabledByValidatedConfig => {}
                BrokerAuthState::Runtime(auth_runtime) => {
                    let auth_context = match RemotingAuthContext::from_request(request) {
                        Ok(auth_context) => auth_context,
                        Err(error) => {
                            let response = command_from_error_with_factory_remark_and_opaque(
                                &self.command_factory,
                                &error,
                                error.to_string(),
                                opaque,
                            );
                            return BrokerResponseParts::from_command(response)?.into_handler_outcome();
                        }
                    };
                    if let Err(error) = auth_runtime
                        .check_remoting_for_code(&auth_context, request.command(), request_code)
                        .await
                    {
                        let response = command_from_error_with_factory_remark_and_opaque(
                            &self.command_factory,
                            &error,
                            error.to_string(),
                            opaque,
                        );
                        return BrokerResponseParts::from_command(response)?.into_handler_outcome();
                    }
                }
            }
        }

        let (processor, default_processor) = match self.process_table.get(&request_code).cloned() {
            Some(processor) => (processor, false),
            None => match self.default_request_processor.as_ref() {
                Some(processor) => (processor.as_ref().clone(), true),
                None => {
                    let response =
                        request_code_not_supported_with_factory_and_opaque(&self.command_factory, request_code, opaque);
                    return BrokerResponseParts::from_command(response)?.into_handler_outcome();
                }
            },
        };

        let result = self
            .process_with_optional_fast_failure(
                fast_failure_queue_kind(request_code, default_processor),
                processor,
                request,
            )
            .await;
        map_request_header_error(&self.command_factory, result, opaque)
    }

    fn reject_request(&self, code: i32) -> RejectRequestDecision {
        match self.process_table.get(&code) {
            Some(processor) => processor.reject_request(code),
            None => match self.default_request_processor.as_ref() {
                Some(processor) => processor.reject_request(code),
                None => {
                    let response = request_code_not_supported_with_factory(&self.command_factory, code);
                    response_rejection(response, "unsupported request code")
                }
            },
        }
    }

    fn request_ordering(&self, ingress: IngressRequestView<'_>) -> RequestOrdering {
        request_ordering::broker_request_ordering_v2(ingress)
    }

    fn observe_response_write(&self, observation: ResponseWriteObservationV2) {
        let original_code = observation.original_code();
        if let Some(processor) = self.process_table.get(&original_code) {
            processor.observe_response_write(observation);
        } else if let Some(processor) = self.default_request_processor.as_ref() {
            processor.observe_response_write(observation);
        }
    }
}

impl<P> BrokerRequestProcessorV2<P>
where
    P: RequestProcessorV2 + Clone + Sync + 'static,
{
    async fn process_with_optional_fast_failure(
        &self,
        queue_kind: Option<crate::latency::broker_fast_failure::FastFailureQueueKind>,
        mut processor: P,
        request: &mut RemotingRequest,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let Some(queue_kind) = queue_kind else {
            return processor.process(request).await;
        };
        let Some(broker_fast_failure) = &self.broker_fast_failure else {
            return processor.process(request).await;
        };
        if !broker_fast_failure.is_enabled() {
            return processor.process(request).await;
        }

        let opaque = request.original_identity().original_opaque();
        let metadata = fast_failure_dispatch::FastFailureRequestMetadata::from_command(request.command());
        let admission = match fast_failure_dispatch::try_admit(broker_fast_failure, queue_kind, metadata) {
            Ok(admission) => admission,
            Err(rejection) => {
                return rejection
                    .into_response_plan()
                    .map(HandlerOutcome::Reply)
                    .map_err(|error| rocketmq_error::RocketMQError::internal("broker-v2-fast-failure", error));
            }
        };
        let run = match admission
            .await_run(fast_failure_dispatch::FastFailureControl::from(request.control()))
            .await
        {
            Ok(run) => run,
            Err(fast_failure_dispatch::FastFailureAwaitError::Rejected(rejection)) => {
                return rejection
                    .into_response_plan()
                    .map(HandlerOutcome::Reply)
                    .map_err(|error| rocketmq_error::RocketMQError::internal("broker-v2-fast-failure", error));
            }
            Err(fast_failure_dispatch::FastFailureAwaitError::LifecycleStopped) => {
                return Err(rocketmq_error::RocketMQError::invariant_violated(
                    "V2 fast-failure request lifecycle stopped before Broker dispatch",
                ));
            }
        };
        let result = processor.process(request).await;
        run.complete_v2();
        match result {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                let response = command_from_error_with_factory_and_opaque(&self.command_factory, &error, opaque);
                BrokerResponseParts::from_command(response)?.into_handler_outcome()
            }
        }
    }
}

fn map_request_header_error(
    command_factory: &RemotingCommandFactory,
    result: rocketmq_error::RocketMQResult<HandlerOutcome>,
    opaque: i32,
) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
    match result {
        Err(error) if error.kind() == rocketmq_error::ErrorKind::RequestHeaderError => {
            let response = command_from_error_with_factory_and_opaque(command_factory, &error, opaque);
            BrokerResponseParts::from_command(response)?.into_handler_outcome()
        }
        result => result,
    }
}

#[cfg(test)]
mod tests;
