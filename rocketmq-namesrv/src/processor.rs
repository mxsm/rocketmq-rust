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
use rocketmq_error::RocketMQError;
use rocketmq_observability::metrics::namesrv::NameServerAdmissionOutcome;
use rocketmq_observability::metrics::namesrv::NameServerMetrics;
use rocketmq_observability::metrics::namesrv::NameServerRequestOutcome;
use rocketmq_observability::metrics::namesrv::NameServerSecurityEvent;
use rocketmq_observability::metrics::namesrv::NameServerWorkloadClass;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::api::v1::command_from_error_with_remark_and_opaque;
use rocketmq_transport::api::v1::request_code_not_supported_with_opaque;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use rocketmq_transport::api::v1::RejectRequestResponse;
use rocketmq_transport::api::v1::RequestProcessor;
use rocketmq_transport::api::v1::ResponseWriteObservation;
use rocketmq_transport::api::v1::ResponseWriteOutcome;

pub use self::client_request_processor::ClientRequestProcessor;
pub use self::cluster_test_request_processor::ClusterTestRequestProcessor;
pub(crate) use self::cluster_test_request_processor::ClusterTestRouteLookup;
pub(crate) use self::cluster_test_request_processor::TransportClusterTestRouteLookup;
use crate::bootstrap::InFlightRequestTracker;
use crate::bootstrap::NameServerRuntimeHandle;
use crate::processor::default_request_processor::DefaultRequestProcessor;
use crate::processor::workload_admission::NameServerWorkloadAdmission;
use crate::processor::workload_admission::WorkloadAdmissionClass;
use crate::security::classify_namesrv_request;

pub(crate) mod client_request_processor;
mod cluster_test_request_processor;
pub mod default_request_processor;
#[doc(hidden)]
pub mod workload_admission;

const NAMESPACE_ORDER_TOPIC_CONFIG: &str = "ORDER_TOPIC_CONFIG";

#[derive(Clone)]
pub enum NameServerRequestProcessorWrapper {
    ClientRequestProcessor(Arc<ClientRequestProcessor>),
    ClusterTestRequestProcessor(Arc<ClusterTestRequestProcessor>),
    DefaultRequestProcessor(Arc<DefaultRequestProcessor>),
}

impl RequestProcessor for NameServerRequestProcessorWrapper {
    async fn process_request(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match self {
            NameServerRequestProcessorWrapper::ClientRequestProcessor(processor) => {
                processor.handle_request(request).await
            }
            NameServerRequestProcessorWrapper::ClusterTestRequestProcessor(processor) => {
                processor.handle_request(request).await
            }
            NameServerRequestProcessorWrapper::DefaultRequestProcessor(processor) => {
                processor.handle_request(channel, request).await
            }
        }
    }

    fn reject_request(&self, code: i32) -> RejectRequestResponse {
        match self {
            NameServerRequestProcessorWrapper::ClientRequestProcessor(processor) => {
                RequestProcessor::reject_request(processor.as_ref(), code)
            }
            NameServerRequestProcessorWrapper::ClusterTestRequestProcessor(processor) => {
                RequestProcessor::reject_request(processor.as_ref(), code)
            }
            NameServerRequestProcessorWrapper::DefaultRequestProcessor(processor) => {
                RequestProcessor::reject_request(processor.as_ref(), code)
            }
        }
    }
}

pub(crate) type RequestCodeType = i32;

#[derive(Clone, Default)]
pub struct NameServerRequestProcessor {
    processor_table: Arc<HashMap<RequestCodeType, NameServerRequestProcessorWrapper>>,
    default_request_processor: Option<NameServerRequestProcessorWrapper>,
    in_flight_requests: Option<Arc<InFlightRequestTracker>>,
    auth_runtime: Option<Arc<AuthRuntime>>,
    runtime_handle: Option<NameServerRuntimeHandle>,
    workload_admission: Option<Arc<NameServerWorkloadAdmission>>,
    metrics: NameServerMetrics,
}

impl NameServerRequestProcessor {
    pub fn new() -> Self {
        Self {
            processor_table: Arc::new(HashMap::new()),
            default_request_processor: None,
            in_flight_requests: None,
            auth_runtime: None,
            runtime_handle: None,
            workload_admission: None,
            metrics: NameServerMetrics::noop(),
        }
    }

    pub(crate) fn new_with_in_flight_tracker(
        in_flight_requests: Arc<InFlightRequestTracker>,
        metrics: NameServerMetrics,
    ) -> Self {
        Self {
            in_flight_requests: Some(in_flight_requests),
            metrics,
            ..Self::new()
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

    pub(crate) fn with_runtime_handle(mut self, runtime_handle: NameServerRuntimeHandle) -> Self {
        self.runtime_handle = Some(runtime_handle);
        self
    }

    pub(crate) fn with_workload_admission(mut self, admission: Arc<NameServerWorkloadAdmission>) -> Self {
        self.workload_admission = Some(admission);
        self
    }
}

impl RequestProcessor for NameServerRequestProcessor {
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_started = Instant::now();
        let _in_flight_guard = self
            .in_flight_requests
            .as_ref()
            .map(|in_flight_requests| in_flight_requests.enter());
        let route_request_started = (request.code() == RequestCode::GetRouteinfoByTopic as i32).then(Instant::now);
        let request_class = match classify_namesrv_request(RequestCode::from(request.code())) {
            Some(request_class) => request_class,
            None => {
                let error = RocketMQError::authentication_failed("request code is not authorized by NameServer");
                self.metrics.record_security_event(NameServerSecurityEvent::AuthDenied);
                return Ok(Some(command_from_error_with_remark_and_opaque(
                    &error,
                    "NameServer request is not authorized",
                    request.opaque(),
                )));
            }
        };
        let metric_class = metric_workload_class(WorkloadAdmissionClass::from(request_class));
        if let Some(auth_runtime) = &self.auth_runtime {
            if auth_runtime.check_remoting(&ctx, request).await.is_err() {
                tracing::warn!(
                    remote_endpoint = %ctx.remote_address(),
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
                return Ok(Some(command_from_error_with_remark_and_opaque(
                    &error,
                    error.to_string(),
                    request.opaque(),
                )));
            }
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
                        let response = workload_admission_rejection_response(rejection, request.opaque());
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
        let response = match self.processor_table.get(request.code_ref()).cloned() {
            None => match self.default_request_processor.clone() {
                None => {
                    let response = request_code_not_supported_with_opaque(request.code(), request.opaque());
                    Ok(Some(response))
                }
                Some(mut processor) => RequestProcessor::process_request(&mut processor, channel, ctx, request).await,
            },
            Some(mut processor) => RequestProcessor::process_request(&mut processor, channel, ctx, request).await,
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

    fn observe_response_write(&self, observation: ResponseWriteObservation) {
        if observation.request_code == RequestCode::GetRouteinfoByTopic as i32 {
            self.metrics.record_route_response_write(
                observation.write_elapsed,
                observation.end_to_end_elapsed,
                observation.outcome == ResponseWriteOutcome::Sent,
            );
        }
    }
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
    rejection: workload_admission::WorkloadAdmissionRejection,
    opaque: i32,
) -> RemotingCommand {
    RemotingCommand::create_response_command_with_code(rocketmq_protocol::code::response_code::ResponseCode::SystemBusy)
        .set_remark(format!(
            "NameServer workload admission rejected request: {}",
            rejection.as_str()
        ))
        .set_opaque(opaque)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use rocketmq_auth::AuthConfig;
    use rocketmq_auth::AuthRuntimeBuilder;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_transport::api::v1::ConnectionHandlerContextWrapper;
    use rocketmq_transport::test_support::Connection;
    use rocketmq_transport::test_support::TestChannelBuilder;

    use super::*;

    #[test]
    fn cloned_nameserver_processors_share_the_dispatch_table() {
        let processor = NameServerRequestProcessor::new();
        let cloned = processor.clone();

        assert!(Arc::ptr_eq(&processor.processor_table, &cloned.processor_table));
    }

    #[test]
    fn admission_rejection_is_stable_system_busy_and_preserves_opaque() {
        let response =
            workload_admission_rejection_response(workload_admission::WorkloadAdmissionRejection::QueueFull, 9191);

        assert_eq!(response.code(), ResponseCode::SystemBusy as i32);
        assert_eq!(response.opaque(), 9191);
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
        let channel_service = runtime.service_context("namesrv.channel");
        let (transport, _peer) = tokio::io::duplex(4096);
        let channel = TestChannelBuilder::new(
            Connection::new_with_plaintext_stream(transport),
            channel_service.task_group().clone(),
        )
        .addresses(
            SocketAddr::from(([127, 0, 0, 1], 9876)),
            SocketAddr::from(([127, 0, 0, 1], 10911)),
        )
        .build()
        .expect("test channel should initialize");
        let context = Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut processor = NameServerRequestProcessor::new().with_auth_runtime(Some(Arc::clone(&auth_runtime)));
        let mut request =
            RemotingCommand::create_remoting_command(RequestCode::RegisterBroker.to_i32()).set_opaque(0x5a5a);

        let response = processor
            .process_request(channel, context, &mut request)
            .await
            .expect("authorization denial should be encoded as a response")
            .expect("authorization denial should return a command");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::NoPermission);
        assert_eq!(response.opaque(), 0x5a5a);
        auth_runtime.shutdown().await.expect("auth runtime should shut down");
        let report = channel_service.task_group().shutdown(Duration::from_secs(1)).await;
        report.assert_no_task_leak().expect("channel tasks should be owned");
    }
}
