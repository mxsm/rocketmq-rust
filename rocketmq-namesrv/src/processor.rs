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
use std::sync::Arc;
use std::time::Instant;

use rocketmq_auth::AuthRuntime;
use rocketmq_auth::RemotingAuthContext;
use rocketmq_error::RocketMQError;
use rocketmq_observability::metrics::namesrv::NameServerAdmissionOutcome;
use rocketmq_observability::metrics::namesrv::NameServerMetrics;
use rocketmq_observability::metrics::namesrv::NameServerRequestOutcome;
use rocketmq_observability::metrics::namesrv::NameServerSecurityEvent;
use rocketmq_observability::metrics::namesrv::NameServerWorkloadClass;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_security_api::combine_layered_authorization;
use rocketmq_security_api::DetailedDecision;
use rocketmq_security_api::IngressDecision;
use rocketmq_security_api::LayerRequirement;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestProcessorV2;
use rocketmq_transport::api::v2::ResponsePlan;
use rocketmq_transport::api::v2::ResponseWriteObservationV2;
use rocketmq_transport::api::v2::ResponseWriteOutcomeV2;

pub use self::client_request_processor::ClientRequestProcessor;
pub use self::cluster_test_request_processor::ClusterTestRequestProcessor;
pub(crate) use self::cluster_test_request_processor::ClusterTestRouteLookup;
pub(crate) use self::cluster_test_request_processor::TransportClusterTestRouteLookup;
use crate::bootstrap::InFlightRequestTracker;
use crate::bootstrap::NameServerRuntimeHandle;
use crate::processor::default_request_processor::DefaultRequestProcessor;
use crate::processor::response_factory::NameServerResponseFactoryExt;
use crate::processor::workload_admission::NameServerWorkloadAdmission;
use crate::processor::workload_admission::WorkloadAdmissionClass;
use crate::security::classify_namesrv_request;

pub(crate) mod client_request_processor;
mod cluster_test_request_processor;
pub mod default_request_processor;
mod response_factory;
#[doc(hidden)]
pub mod workload_admission;

const NAMESPACE_ORDER_TOPIC_CONFIG: &str = "ORDER_TOPIC_CONFIG";

#[derive(Clone)]
pub enum NameServerRequestProcessorWrapper {
    ClientRequestProcessor(Arc<ClientRequestProcessor>),
    ClusterTestRequestProcessor(Arc<ClusterTestRequestProcessor>),
    DefaultRequestProcessor(Arc<DefaultRequestProcessor>),
}

impl RequestProcessorV2 for NameServerRequestProcessorWrapper {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let response = match self {
            NameServerRequestProcessorWrapper::ClientRequestProcessor(processor) => {
                processor.handle_request(request.command_mut()).await
            }
            NameServerRequestProcessorWrapper::ClusterTestRequestProcessor(processor) => {
                processor.handle_request(request.command_mut()).await
            }
            NameServerRequestProcessorWrapper::DefaultRequestProcessor(processor) => {
                processor.handle_request(request).await
            }
        }?;
        response_outcome(response)
    }
}

pub(crate) type RequestCodeType = i32;

#[derive(Clone)]
pub struct NameServerRequestProcessor {
    processor_table: Arc<HashMap<RequestCodeType, NameServerRequestProcessorWrapper>>,
    default_request_processor: Option<NameServerRequestProcessorWrapper>,
    in_flight_requests: Option<Arc<InFlightRequestTracker>>,
    auth_runtime: Option<Arc<AuthRuntime>>,
    detailed_authorization_requirement: Option<LayerRequirement>,
    runtime_handle: Option<NameServerRuntimeHandle>,
    workload_admission: Option<Arc<NameServerWorkloadAdmission>>,
    metrics: NameServerMetrics,
    command_factory: RemotingCommandFactory,
}

impl Default for NameServerRequestProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl NameServerRequestProcessor {
    pub fn new() -> Self {
        Self {
            processor_table: Arc::new(HashMap::new()),
            default_request_processor: None,
            in_flight_requests: None,
            auth_runtime: None,
            detailed_authorization_requirement: None,
            runtime_handle: None,
            workload_admission: None,
            metrics: NameServerMetrics::noop(),
            command_factory: application_remoting_command_factory(),
        }
    }

    pub(crate) fn new_with_in_flight_tracker(
        in_flight_requests: Arc<InFlightRequestTracker>,
        metrics: NameServerMetrics,
        command_factory: RemotingCommandFactory,
    ) -> Self {
        Self {
            processor_table: Arc::new(HashMap::new()),
            default_request_processor: None,
            in_flight_requests: Some(in_flight_requests),
            auth_runtime: None,
            detailed_authorization_requirement: None,
            runtime_handle: None,
            workload_admission: None,
            metrics,
            command_factory,
        }
    }

    pub fn register_processor(&mut self, request_code: RequestCode, processor: NameServerRequestProcessorWrapper) {
        Arc::make_mut(&mut self.processor_table).insert(request_code as i32, processor);
    }

    pub fn register_default_processor(&mut self, processor: NameServerRequestProcessorWrapper) {
        self.default_request_processor = Some(processor);
    }

    pub(crate) fn with_auth_runtime(mut self, auth_runtime: Option<Arc<AuthRuntime>>) -> Self {
        self.auth_runtime = auth_runtime;
        self
    }

    pub(crate) fn with_detailed_authorization_requirement(mut self, requirement: LayerRequirement) -> Self {
        self.detailed_authorization_requirement = Some(requirement);
        self
    }

    pub(crate) fn with_runtime_handle(mut self, runtime_handle: NameServerRuntimeHandle) -> Self {
        self.runtime_handle = Some(runtime_handle);
        self
    }

    pub(crate) fn with_workload_admission(mut self, admission: Arc<NameServerWorkloadAdmission>) -> Self {
        self.workload_admission = Some(admission);
        self
    }
}

impl NameServerRequestProcessor {
    pub(crate) async fn process_command(
        &mut self,
        auth_context: &RemotingAuthContext,
        original_code: i32,
        request: &mut RemotingCommand,
        broker_session: Option<crate::route::types::BrokerSession>,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_started = Instant::now();
        let _in_flight_guard = self
            .in_flight_requests
            .as_ref()
            .map(|in_flight_requests| in_flight_requests.enter());
        let route_request_started = (original_code == RequestCode::GetRouteinfoByTopic as i32).then(Instant::now);
        let request_class = match classify_namesrv_request(RequestCode::from(original_code)) {
            Some(request_class) => request_class,
            None => {
                let error = RocketMQError::authentication_failed("request code is not authorized by NameServer");
                self.metrics.record_security_event(NameServerSecurityEvent::AuthDenied);
                return Ok(Some(self.command_factory.command_from_error_with_remark_and_opaque(
                    &error,
                    "NameServer request is not authorized",
                    request.opaque(),
                )));
            }
        };
        let metric_class = metric_workload_class(WorkloadAdmissionClass::from(request_class));
        let detailed_requirement = self.detailed_authorization_requirement.unwrap_or_else(|| {
            self.auth_runtime
                .as_ref()
                .map(|auth_runtime| auth_runtime.detailed_authorization_requirement())
                .unwrap_or(LayerRequirement::Optional)
        });
        let detailed = match &self.auth_runtime {
            Some(auth_runtime) => {
                auth_runtime
                    .evaluate_remoting_detailed_for_code(auth_context, request, original_code)
                    .await
            }
            None => Ok(DetailedDecision::Abstain),
        };
        if matches!(
            combine_layered_authorization(Ok(IngressDecision::AllowToContinue), detailed_requirement, || detailed,),
            rocketmq_security_api::Decision::Deny { .. }
        ) {
            tracing::warn!(
                remote_endpoint = %auth_context.source_ip().unwrap_or("embedded"),
                request_class = request_class.as_str(),
                reason_code = "protocol-auth-denied",
                "NameServer request denied"
            );
            let error =
                RocketMQError::authentication_failed("NameServer request authentication or authorization failed");
            self.metrics.record_security_event(NameServerSecurityEvent::AuthDenied);
            self.metrics.record_request(
                metric_class,
                NameServerRequestOutcome::Rejected,
                request_started.elapsed(),
                0,
            );
            return Ok(Some(self.command_factory.command_from_error_with_remark_and_opaque(
                &error,
                error.to_string(),
                request.opaque(),
            )));
        }
        let admission_class = WorkloadAdmissionClass::from(request_class);
        let admission_config = self
            .runtime_handle
            .as_ref()
            .map(NameServerRuntimeHandle::name_server_config);
        let _admission_lease = if let (Some(admission), Some(config)) = (&self.workload_admission, admission_config) {
            if !config.namesrv_workload_admission_enable {
                None
            } else if config.namesrv_workload_admission_observe_only {
                let lease = admission.try_observe(admission_class);
                let outcome = if lease.is_some() {
                    NameServerAdmissionOutcome::Acquired
                } else {
                    NameServerAdmissionOutcome::ObserveSaturated
                };
                record_admission_metric(&self.metrics, admission, admission_class, outcome);
                lease
            } else {
                match admission.acquire(admission_class).await {
                    Ok(lease) => {
                        let outcome = if lease.was_queued() {
                            NameServerAdmissionOutcome::Queued
                        } else {
                            NameServerAdmissionOutcome::Acquired
                        };
                        record_admission_metric(&self.metrics, admission, admission_class, outcome);
                        Some(lease)
                    }
                    Err(rejection) => {
                        let outcome = match rejection {
                            workload_admission::WorkloadAdmissionRejection::QueueFull => {
                                NameServerAdmissionOutcome::Rejected
                            }
                            workload_admission::WorkloadAdmissionRejection::TimedOut => {
                                NameServerAdmissionOutcome::TimedOut
                            }
                        };
                        record_admission_metric(&self.metrics, admission, admission_class, outcome);
                        tracing::warn!(
                            request_class = admission_class.as_str(),
                            reason = rejection.as_str(),
                            "NameServer workload admission rejected request"
                        );
                        if let Some(started) = route_request_started {
                            self.metrics.record_route_request(started.elapsed());
                            self.metrics.record_route_error(
                                rocketmq_observability::metrics::namesrv::NameServerRouteErrorKind::Rejected,
                            );
                        }
                        let response =
                            workload_admission_rejection_response(&self.command_factory, rejection, request.opaque());
                        let response_bytes = response.body().map_or(0, bytes::Bytes::len);
                        self.metrics.record_request(
                            metric_class,
                            NameServerRequestOutcome::Rejected,
                            request_started.elapsed(),
                            response_bytes,
                        );
                        return Ok(Some(response));
                    }
                }
            }
        } else {
            None
        };
        let had_admission_lease = _admission_lease.is_some();
        let response = match self.processor_table.get(&original_code).cloned() {
            None => match self.default_request_processor.clone() {
                None => {
                    let response = self
                        .command_factory
                        .request_code_not_supported_with_opaque(original_code, request.opaque());
                    Ok(Some(response))
                }
                Some(mut processor) => processor_response(&mut processor, request, broker_session).await,
            },
            Some(mut processor) => processor_response(&mut processor, request, broker_session).await,
        };
        if let Some(started) = route_request_started {
            self.metrics.record_route_request(started.elapsed());
            match &response {
                Ok(Some(command))
                    if command.code() == rocketmq_protocol::code::response_code::ResponseCode::TopicNotExist as i32 =>
                {
                    self.metrics.record_route_error(
                        rocketmq_observability::metrics::namesrv::NameServerRouteErrorKind::NotFound,
                    );
                }
                Ok(Some(command))
                    if command.code() != rocketmq_protocol::code::response_code::ResponseCode::Success as i32 =>
                {
                    self.metrics.record_route_error(
                        rocketmq_observability::metrics::namesrv::NameServerRouteErrorKind::Rejected,
                    );
                }
                Err(_) => {
                    self.metrics.record_route_error(
                        rocketmq_observability::metrics::namesrv::NameServerRouteErrorKind::Internal,
                    );
                }
                Ok(_) => {}
            }
        }
        let (request_outcome, response_bytes) = match &response {
            Ok(Some(command))
                if command.code() == rocketmq_protocol::code::response_code::ResponseCode::Success as i32 =>
            {
                (
                    NameServerRequestOutcome::Success,
                    command.body().map_or(0, bytes::Bytes::len),
                )
            }
            Ok(Some(command)) => (
                NameServerRequestOutcome::Rejected,
                command.body().map_or(0, bytes::Bytes::len),
            ),
            Ok(None) => (NameServerRequestOutcome::Success, 0),
            Err(_) => (NameServerRequestOutcome::Error, 0),
        };
        self.metrics
            .record_request(metric_class, request_outcome, request_started.elapsed(), response_bytes);
        drop(_admission_lease);
        if had_admission_lease {
            if let Some(admission) = &self.workload_admission {
                record_admission_metric(
                    &self.metrics,
                    admission,
                    admission_class,
                    NameServerAdmissionOutcome::Released,
                );
            }
        }
        response
    }
}

impl RequestProcessorV2 for NameServerRequestProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let auth_context = RemotingAuthContext::from_request(request)?;
        let original_code = request.original_identity().original_code();
        let broker_session = if matches!(
            RequestCode::from(original_code),
            RequestCode::RegisterBroker | RequestCode::BrokerHeartbeat
        ) {
            Some(crate::processor::default_request_processor::broker_session_from_request(request)?)
        } else {
            None
        };
        let response = self
            .process_command(&auth_context, original_code, request.command_mut(), broker_session)
            .await?;
        response_outcome(response)
    }

    fn observe_response_write(&self, observation: ResponseWriteObservationV2) {
        if observation.original_code() == RequestCode::GetRouteinfoByTopic as i32 {
            self.metrics.record_route_response_write(
                observation.write_elapsed(),
                observation.end_to_end_elapsed(),
                matches!(observation.outcome(), ResponseWriteOutcomeV2::Written(_)),
            );
        }
    }
}

async fn processor_response(
    processor: &mut NameServerRequestProcessorWrapper,
    request: &mut RemotingCommand,
    broker_session: Option<crate::route::types::BrokerSession>,
) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
    match processor {
        NameServerRequestProcessorWrapper::ClientRequestProcessor(processor) => processor.handle_request(request).await,
        NameServerRequestProcessorWrapper::ClusterTestRequestProcessor(processor) => {
            processor.handle_request(request).await
        }
        NameServerRequestProcessorWrapper::DefaultRequestProcessor(processor) => {
            processor.handle_command(request, broker_session).await
        }
    }
}

pub(crate) fn response_outcome(response: Option<RemotingCommand>) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
    let Some(response) = response else {
        return Err(RocketMQError::invariant_violated(
            "NameServer V2 processor returned no response without a protocol marker",
        ));
    };
    let plan = ResponsePlan::from_command(response)
        .map_err(|error| RocketMQError::response_process_failed("namesrv.response_plan", error.to_string()))?;
    Ok(HandlerOutcome::Reply(plan))
}

fn record_admission_metric(
    metrics: &NameServerMetrics,
    admission: &NameServerWorkloadAdmission,
    class: WorkloadAdmissionClass,
    outcome: NameServerAdmissionOutcome,
) {
    let metric_class = metric_workload_class(class);
    let (inflight, waiting) = admission.class_counts(class);
    metrics.record_workload_admission(metric_class, outcome, inflight, waiting);
}

fn metric_workload_class(class: WorkloadAdmissionClass) -> NameServerWorkloadClass {
    match class {
        WorkloadAdmissionClass::RouteRead => NameServerWorkloadClass::RouteRead,
        WorkloadAdmissionClass::BrokerControl => NameServerWorkloadClass::BrokerControl,
        WorkloadAdmissionClass::Admin => NameServerWorkloadClass::Admin,
    }
}

fn workload_admission_rejection_response(
    command_factory: &RemotingCommandFactory,
    rejection: workload_admission::WorkloadAdmissionRejection,
    opaque: i32,
) -> RemotingCommand {
    command_factory
        .create_response_command_with_code(rocketmq_protocol::code::response_code::ResponseCode::SystemBusy)
        .set_remark(format!(
            "NameServer workload admission rejected request: {}",
            rejection.as_str()
        ))
        .set_opaque(opaque)
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_auth::AuthConfig;
    use rocketmq_auth::AuthRuntimeBuilder;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
    use rocketmq_runtime::RuntimeContext;

    use super::*;

    fn route_request(opaque: i32) -> RemotingCommand {
        let mut request = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new(CheetahString::from_static_str("layered-auth-topic"), Some(true)),
        )
        .set_opaque(opaque);
        request.make_custom_header_to_net();
        request
    }

    fn route_processor(
        bootstrap: &crate::bootstrap::NameServerBootstrap,
        auth_runtime: Option<Arc<AuthRuntime>>,
        requirement: LayerRequirement,
    ) -> NameServerRequestProcessor {
        let runtime_handle = NameServerRuntimeHandle::new(&bootstrap.runtime_inner());
        let mut processor = NameServerRequestProcessor::new()
            .with_auth_runtime(auth_runtime)
            .with_detailed_authorization_requirement(requirement);
        processor.register_processor(
            RequestCode::GetRouteinfoByTopic,
            NameServerRequestProcessorWrapper::ClientRequestProcessor(Arc::new(ClientRequestProcessor::new(
                runtime_handle,
            ))),
        );
        processor
    }

    fn auth_context() -> RemotingAuthContext {
        RemotingAuthContext::network("127.0.0.1", "namesrv-v2-test-session")
    }

    #[test]
    fn cloned_nameserver_processors_share_the_dispatch_table() {
        let processor = NameServerRequestProcessor::new();
        let cloned = processor.clone();

        assert!(Arc::ptr_eq(&processor.processor_table, &cloned.processor_table));
    }

    #[test]
    fn admission_rejection_is_stable_system_busy_and_preserves_opaque() {
        let factory = rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory::new(
            rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandDefaults::new(
                655,
                rocketmq_protocol::protocol::SerializeType::ROCKETMQ,
            ),
        );
        let response = workload_admission_rejection_response(
            &factory,
            workload_admission::WorkloadAdmissionRejection::QueueFull,
            9191,
        );

        assert_eq!(response.code(), ResponseCode::SystemBusy as i32);
        assert_eq!(response.opaque(), 9191);
        assert_eq!(response.version(), 655);
        assert_eq!(
            response.serialize_type(),
            rocketmq_protocol::protocol::SerializeType::ROCKETMQ
        );
        assert_eq!(
            response.remark().map(cheetah_string::CheetahString::as_str),
            Some("NameServer workload admission rejected request: queue-full")
        );
    }

    #[tokio::test]
    async fn anonymous_broker_control_request_returns_no_permission_with_opaque() {
        let runtime = RuntimeContext::from_current("namesrv-auth-processor-test");
        let auth_runtime = Arc::new(
            AuthRuntimeBuilder::new(
                AuthConfig {
                    authentication_enabled: true,
                    authorization_enabled: true,
                    ..AuthConfig::default()
                },
                runtime.service_context("namesrv.auth"),
            )
            .build()
            .await
            .expect("test auth runtime should initialize"),
        );
        let mut processor = NameServerRequestProcessor::new().with_auth_runtime(Some(Arc::clone(&auth_runtime)));
        let mut request =
            RemotingCommand::create_remoting_command(RequestCode::RegisterBroker.to_i32()).set_opaque(0x5a5a);

        let response = processor
            .process_command(&auth_context(), request.code(), &mut request, None)
            .await
            .expect("authorization denial should be encoded as a response")
            .expect("authorization denial should return a command");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::NoPermission);
        assert_eq!(response.opaque(), 0x5a5a);
        auth_runtime.shutdown().await.expect("auth runtime should shut down");
    }

    #[tokio::test]
    async fn mutated_command_code_cannot_replace_the_original_authorization_operation() {
        let runtime = RuntimeContext::from_current("namesrv-original-auth-code-test");
        let mutated_code = RequestCode::GetRouteinfoByTopic.to_i32().to_string();
        let auth_runtime = Arc::new(
            AuthRuntimeBuilder::new(
                AuthConfig {
                    authentication_enabled: true,
                    authorization_enabled: true,
                    authentication_whitelist: CheetahString::from_string(mutated_code.clone()),
                    authorization_whitelist: CheetahString::from_string(mutated_code),
                    ..AuthConfig::default()
                },
                runtime.service_context("namesrv.original-auth-code"),
            )
            .build()
            .await
            .expect("test auth runtime should initialize"),
        );
        let mut processor = NameServerRequestProcessor::new().with_auth_runtime(Some(Arc::clone(&auth_runtime)));
        let mut request = route_request(0x5a5f);

        let response = processor
            .process_command(
                &auth_context(),
                RequestCode::RegisterBroker.to_i32(),
                &mut request,
                None,
            )
            .await
            .expect("authorization denial should be encoded as a response")
            .expect("authorization denial should return a command");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::NoPermission);
        auth_runtime.shutdown().await.expect("auth runtime should shut down");
    }

    #[tokio::test]
    async fn required_abstention_denies_before_the_route_handler() {
        let runtime = RuntimeContext::from_current("namesrv-required-abstain-test");
        let bootstrap = crate::bootstrap::Builder::new(
            runtime.service_context("namesrv-bootstrap"),
            rocketmq_observability::TelemetryHandle::noop(),
        )
        .build();
        let mut processor = route_processor(&bootstrap, None, LayerRequirement::Required);
        let mut request = route_request(0x5a5b);

        let response = processor
            .process_command(&auth_context(), request.code(), &mut request, None)
            .await
            .expect("required abstention should produce a response")
            .expect("required abstention should return a command");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::NoPermission);
        assert_eq!(response.opaque(), 0x5a5b);
    }

    #[tokio::test]
    async fn optional_abstention_allows_the_route_handler_to_run() {
        let runtime = RuntimeContext::from_current("namesrv-optional-abstain-test");
        let bootstrap = crate::bootstrap::Builder::new(
            runtime.service_context("namesrv-bootstrap"),
            rocketmq_observability::TelemetryHandle::noop(),
        )
        .build();
        let mut processor = route_processor(&bootstrap, None, LayerRequirement::Optional);
        let mut request = route_request(0x5a5c);

        let response = processor
            .process_command(&auth_context(), request.code(), &mut request, None)
            .await
            .expect("optional abstention should reach the route handler")
            .expect("route handler should return a command");

        assert_ne!(ResponseCode::from(response.code()), ResponseCode::NoPermission);
        assert_ne!(
            ResponseCode::from(response.code()),
            ResponseCode::RequestCodeNotSupported
        );
    }

    #[tokio::test]
    async fn leaf_lookup_uses_the_original_code_after_command_mutation() {
        let runtime = RuntimeContext::from_current("namesrv-original-route-code-test");
        let bootstrap = crate::bootstrap::Builder::new(
            runtime.service_context("namesrv-bootstrap"),
            rocketmq_observability::TelemetryHandle::noop(),
        )
        .build();
        let mut processor = route_processor(&bootstrap, None, LayerRequirement::Optional);
        let mut request = route_request(0x5a60);
        request.set_code_ref(RequestCode::GetBrokerClusterInfo.to_i32());

        let response = processor
            .process_command(
                &auth_context(),
                RequestCode::GetRouteinfoByTopic.to_i32(),
                &mut request,
                None,
            )
            .await
            .expect("original route must reach the registered leaf")
            .expect("route handler must return a command");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::TopicNotExist);
    }

    #[tokio::test]
    async fn auth_whitelists_allow_the_route_handler_but_detailed_denial_blocks_it() {
        let runtime = RuntimeContext::from_current("namesrv-layered-auth-handler-test");
        let route_code = RequestCode::GetRouteinfoByTopic.to_i32().to_string();
        let whitelisted_runtime = Arc::new(
            AuthRuntimeBuilder::new(
                AuthConfig {
                    authentication_enabled: true,
                    authorization_enabled: true,
                    authentication_whitelist: CheetahString::from_string(route_code.clone()),
                    authorization_whitelist: CheetahString::from_string(route_code),
                    ..AuthConfig::default()
                },
                runtime.service_context("namesrv-whitelist-auth"),
            )
            .build()
            .await
            .expect("whitelisted auth runtime should initialize"),
        );
        let bootstrap = crate::bootstrap::Builder::new(
            runtime.service_context("namesrv-bootstrap"),
            rocketmq_observability::TelemetryHandle::noop(),
        )
        .build();
        let mut allowed_processor = route_processor(
            &bootstrap,
            Some(Arc::clone(&whitelisted_runtime)),
            LayerRequirement::Required,
        );
        let mut allowed_request = route_request(0x5a5d);
        let allowed = allowed_processor
            .process_command(&auth_context(), allowed_request.code(), &mut allowed_request, None)
            .await
            .expect("whitelisted authorization should reach the route handler")
            .expect("route handler should return a command");
        assert_eq!(ResponseCode::from(allowed.code()), ResponseCode::TopicNotExist);

        let denying_runtime = Arc::new(
            AuthRuntimeBuilder::new(
                AuthConfig {
                    authentication_enabled: true,
                    authorization_enabled: true,
                    ..AuthConfig::default()
                },
                runtime.service_context("namesrv-denying-auth"),
            )
            .build()
            .await
            .expect("denying auth runtime should initialize"),
        );
        let mut denied_processor = route_processor(
            &bootstrap,
            Some(Arc::clone(&denying_runtime)),
            LayerRequirement::Required,
        );
        let mut denied_request = route_request(0x5a5e);
        let denied = denied_processor
            .process_command(&auth_context(), denied_request.code(), &mut denied_request, None)
            .await
            .expect("detailed denial should produce a response")
            .expect("detailed denial should return a command");
        assert_eq!(ResponseCode::from(denied.code()), ResponseCode::NoPermission);

        whitelisted_runtime
            .shutdown()
            .await
            .expect("whitelisted auth runtime should shut down");
        denying_runtime
            .shutdown()
            .await
            .expect("denying auth runtime should shut down");
    }
}
