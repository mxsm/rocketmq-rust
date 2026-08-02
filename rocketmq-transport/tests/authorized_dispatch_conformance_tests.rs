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

use std::future;
use std::future::Future;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_error::RocketMQError;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::headers::PullMessageRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeContext;
use rocketmq_security_api::AuthenticatedRequestContext;
use rocketmq_security_api::Decision;
use rocketmq_security_api::Principal;
use rocketmq_security_api::RequestPolicy;
use rocketmq_transport::transport_io_snapshot;
use rocketmq_transport::AdmissionClass;
use rocketmq_transport::AdmissionController;
use rocketmq_transport::AdmissionLimits;
use rocketmq_transport::AdmissionResource;
use rocketmq_transport::AdmissionScope;
use rocketmq_transport::AuthorizedCommandDispatcher;
use rocketmq_transport::Channel;
use rocketmq_transport::Connection;
use rocketmq_transport::ConnectionHandlerContext;
use rocketmq_transport::DispatchError;
use rocketmq_transport::RemotingRequestProcessor;
use rocketmq_transport::RequestContext;
use rocketmq_transport::RequestContextError;
use rocketmq_transport::RequestDeadline;
use rocketmq_transport::ResourceLimit;
use rocketmq_transport::ResponseSinkError;
use rocketmq_transport::RocketMQServer;
use rocketmq_transport::ServerConfig;
use rocketmq_transport::SessionRequestProcessor;
use rocketmq_transport::SessionRequestProcessorAdapter;
use rocketmq_transport::TransportSecurity;
use rocketmq_transport::TransportTelemetry;
use tokio::net::TcpStream;
use tokio::sync::oneshot;

#[derive(Clone, Copy)]
enum ProcessorBehavior {
    Echo,
    DecodeRequiredHeader,
    Error,
    Pending,
}

#[derive(Clone)]
struct ConformanceProcessor {
    behavior: ProcessorBehavior,
    calls: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
}

impl ConformanceProcessor {
    fn new(behavior: ProcessorBehavior) -> Self {
        Self {
            behavior,
            calls: Arc::new(AtomicUsize::new(0)),
            entered: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

impl RemotingRequestProcessor for ConformanceProcessor {
    async fn process_request(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_waiters();
        match self.behavior {
            ProcessorBehavior::Echo => Ok(Some(
                RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                    .set_body(request.body().cloned().unwrap_or_default()),
            )),
            ProcessorBehavior::DecodeRequiredHeader => {
                let _: PullMessageRequestHeader = request.decode_command_custom_header()?;
                Ok(Some(RemotingCommand::create_response_command_with_code(
                    ResponseCode::Success,
                )))
            }
            ProcessorBehavior::Error => Err(RocketMQError::response_process_failed(
                "authorized_dispatch_conformance",
                "injected handler failure",
            )),
            ProcessorBehavior::Pending => future::pending().await,
        }
    }
}

struct AllowOnlyNamedPrincipal;

impl RequestPolicy for AllowOnlyNamedPrincipal {
    fn evaluate_authenticated(&self, context: AuthenticatedRequestContext<'_>) -> Decision {
        if context.principal().id() == "allowed" {
            Decision::Allow
        } else {
            Decision::deny("principal is not authorized")
        }
    }
}

struct DispatchFixture {
    service: ChildServiceContext,
    processor: ConformanceProcessor,
    dispatcher: Arc<AuthorizedCommandDispatcher<ConformanceProcessor>>,
    security: Arc<TransportSecurity>,
    admission: Arc<AdmissionController>,
}

struct SessionContractProcessor {
    calls: Arc<AtomicUsize>,
}

impl SessionRequestProcessor for SessionContractProcessor {
    fn process(
        &self,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = rocketmq_error::RocketMQResult<RemotingCommand>> + Send + '_>> {
        let calls = Arc::clone(&self.calls);
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_opaque(request.opaque()))
        })
    }
}

fn dispatch_fixture(
    runtime: &RuntimeContext,
    name: &'static str,
    behavior: ProcessorBehavior,
    limits: AdmissionLimits,
) -> DispatchFixture {
    let service = runtime.service_context(name);
    let process_budget = service.process_budget();
    let admission = Arc::new(
        AdmissionController::try_new_with_budget(limits, &process_budget)
            .expect("test admission limits should be valid"),
    );
    let security = Arc::new(TransportSecurity::secure_enforced(
        Some(Arc::new(AllowOnlyNamedPrincipal)),
        None,
    ));
    let processor = ConformanceProcessor::new(behavior);
    let dispatcher = Arc::new(
        AuthorizedCommandDispatcher::try_new(
            processor.clone(),
            Vec::new(),
            &process_budget,
            TransportTelemetry::noop(),
            Arc::clone(&security),
            Arc::clone(&admission),
        )
        .expect("test dispatcher should fit the process budget"),
    );
    DispatchFixture {
        service,
        processor,
        dispatcher,
        security,
        admission,
    }
}

fn embedded_context(principal: &str, deadline: Option<RequestDeadline>) -> RequestContext {
    RequestContext::try_embedded(Some(Principal::new(principal)), deadline)
        .expect("test principal should create an embedded context")
}

async fn dispatch_embedded(
    fixture: &DispatchFixture,
    principal: &str,
    deadline: Option<RequestDeadline>,
    command: RemotingCommand,
) -> RemotingCommand {
    fixture
        .dispatcher
        .dispatch_embedded(
            fixture.service.task_group(),
            embedded_context(principal, deadline),
            command,
        )
        .await
        .expect("embedded request should produce a protocol response")
}

async fn network_round_trip(fixture: &DispatchFixture, principal: &str, command: RemotingCommand) -> RemotingCommand {
    let config = Arc::new(ServerConfig {
        bind_address: "127.0.0.1".to_owned(),
        listen_port: 0,
        ..ServerConfig::default()
    });
    let mut server = RocketMQServer::new(config, fixture.service.component("network"))
        .with_transport_security(Arc::clone(&fixture.security), Some(Principal::new(principal)))
        .with_authorized_dispatcher(Arc::clone(&fixture.dispatcher));
    let (startup_tx, startup_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let processor = fixture.processor.clone();
    let server_task = tokio::spawn(async move {
        server
            .run_with_shutdown_report_and_startup(
                processor,
                None,
                async {
                    let _ = shutdown_rx.await;
                },
                startup_tx,
            )
            .await
    });
    let address = startup_rx
        .await
        .expect("server should report startup")
        .expect("server should bind");
    let mut connection = Connection::new(TcpStream::connect(address).await.expect("client should connect"));
    connection
        .send_command(command)
        .await
        .expect("request frame should be written");
    let response = tokio::time::timeout(Duration::from_secs(2), connection.receive_command())
        .await
        .expect("network request should complete before the test timeout")
        .expect("network read should succeed")
        .expect("server should return one response frame");
    drop(connection);
    let _ = shutdown_tx.send(());
    let report = server_task
        .await
        .expect("server task should not panic")
        .expect("server should report shutdown");
    assert!(report.is_healthy(), "{}", report.to_json());
    response
}

fn assert_equivalent(actual: &RemotingCommand, expected: &RemotingCommand) {
    assert_eq!(actual.code(), expected.code());
    assert_eq!(actual.opaque(), expected.opaque());
    assert_eq!(actual.remark(), expected.remark());
    assert_eq!(actual.ext_fields(), expected.ext_fields());
    assert_eq!(actual.body(), expected.body());
}

#[tokio::test]
async fn session_processor_adapter_runs_through_authorized_dispatch() {
    let runtime = RuntimeContext::from_current("session-processor-adapter-conformance");
    let service = runtime.service_context("session-adapter");
    let process_budget = service.process_budget();
    let admission = Arc::new(
        AdmissionController::try_new_with_budget(AdmissionLimits::default(), &process_budget)
            .expect("test admission limits should be valid"),
    );
    let security = Arc::new(TransportSecurity::secure_enforced(
        Some(Arc::new(AllowOnlyNamedPrincipal)),
        None,
    ));
    let calls = Arc::new(AtomicUsize::new(0));
    let processor: Arc<dyn SessionRequestProcessor> = Arc::new(SessionContractProcessor {
        calls: Arc::clone(&calls),
    });
    let dispatcher = Arc::new(
        AuthorizedCommandDispatcher::try_new(
            SessionRequestProcessorAdapter::from_shared(processor),
            Vec::new(),
            &process_budget,
            TransportTelemetry::noop(),
            security,
            admission,
        )
        .expect("adapter dispatcher should fit the process budget"),
    );

    let denied = dispatcher
        .dispatch_embedded(
            service.task_group(),
            embedded_context("denied", None),
            RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(81),
        )
        .await
        .expect("denied request should produce a protocol response");
    assert_eq!(denied.code(), ResponseCode::NoPermission.to_i32());
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let allowed = dispatcher
        .dispatch_embedded(
            service.task_group(),
            embedded_context("allowed", None),
            RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(82),
        )
        .await
        .expect("allowed request should produce a protocol response");
    assert_eq!(allowed.code(), ResponseCode::Success.to_i32());
    assert_eq!(allowed.opaque(), 82);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn network_and_embedded_adapters_share_authorized_dispatch_semantics() {
    let runtime = RuntimeContext::from_current("authorized-dispatch-conformance");

    let valid = dispatch_fixture(&runtime, "valid", ProcessorBehavior::Echo, AdmissionLimits::default());
    let request = RemotingCommand::create_remoting_command(RequestCode::SendMessage)
        .set_opaque(41)
        .set_body(b"shared-dispatch".to_vec());
    let io_before = transport_io_snapshot();
    let embedded = dispatch_embedded(&valid, "allowed", None, request.clone()).await;
    let io_after = transport_io_snapshot();
    assert_eq!(
        io_after, io_before,
        "embedded dispatch must not encode or write a network frame"
    );
    let network = network_round_trip(&valid, "allowed", request).await;
    assert_equivalent(&embedded, &network);
    assert_eq!(valid.processor.calls.load(Ordering::SeqCst), 2);

    assert_eq!(
        RequestContext::try_embedded(None, None).expect_err("missing embedded identity must fail closed"),
        RequestContextError::MissingEmbeddedPrincipal
    );
    let denied = dispatch_fixture(&runtime, "denied", ProcessorBehavior::Echo, AdmissionLimits::default());
    let denied_request = RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(42);
    let embedded_denied = dispatch_embedded(&denied, "denied", None, denied_request.clone()).await;
    let network_denied = network_round_trip(&denied, "denied", denied_request).await;
    assert_equivalent(&embedded_denied, &network_denied);
    assert_eq!(embedded_denied.code(), ResponseCode::NoPermission.to_i32());
    assert_eq!(denied.processor.calls.load(Ordering::SeqCst), 0);

    let malformed = dispatch_fixture(
        &runtime,
        "malformed",
        ProcessorBehavior::DecodeRequiredHeader,
        AdmissionLimits::default(),
    );
    let malformed_request = RemotingCommand::create_remoting_command(RequestCode::PullMessage).set_opaque(43);
    let embedded_malformed = dispatch_embedded(&malformed, "allowed", None, malformed_request.clone()).await;
    let network_malformed = network_round_trip(&malformed, "allowed", malformed_request).await;
    assert_equivalent(&embedded_malformed, &network_malformed);
    assert_eq!(embedded_malformed.code(), ResponseCode::SystemError.to_i32());

    let handler_error = dispatch_fixture(
        &runtime,
        "handler-error",
        ProcessorBehavior::Error,
        AdmissionLimits::default(),
    );
    let error_request = RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(44);
    let embedded_error = dispatch_embedded(&handler_error, "allowed", None, error_request.clone()).await;
    let network_error = network_round_trip(&handler_error, "allowed", error_request).await;
    assert_equivalent(&embedded_error, &network_error);
    assert_eq!(embedded_error.code(), ResponseCode::SystemError.to_i32());

    let deadline = dispatch_fixture(
        &runtime,
        "deadline",
        ProcessorBehavior::Echo,
        AdmissionLimits::default(),
    );
    let deadline_response = dispatch_embedded(
        &deadline,
        "allowed",
        Some(RequestDeadline::after(Duration::ZERO)),
        RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(45),
    )
    .await;
    assert_eq!(deadline_response.code(), ResponseCode::SystemError.to_i32());
    assert_eq!(deadline.processor.calls.load(Ordering::SeqCst), 0);

    let limits = AdmissionLimits {
        processors: ResourceLimit {
            count: 1,
            bytes: 1024 * 1024,
        },
        control_reserve: ResourceLimit { count: 0, bytes: 0 },
        ..AdmissionLimits::default()
    };
    let overloaded = dispatch_fixture(&runtime, "overloaded", ProcessorBehavior::Echo, limits);
    let _held_processor = overloaded
        .admission
        .try_acquire(
            AdmissionResource::Processor,
            AdmissionScope::new(IpAddr::V4(Ipv4Addr::LOCALHOST)).with_session(999),
            1,
            AdmissionClass::Data,
        )
        .expect("test should exhaust the single processor permit");
    let overloaded_request = RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(46);
    let embedded_overloaded = dispatch_embedded(&overloaded, "allowed", None, overloaded_request.clone()).await;
    let network_overloaded = network_round_trip(&overloaded, "allowed", overloaded_request).await;
    assert_equivalent(&embedded_overloaded, &network_overloaded);
    assert_eq!(embedded_overloaded.code(), ResponseCode::SystemBusy.to_i32());
    assert_eq!(overloaded.processor.calls.load(Ordering::SeqCst), 0);

    let cancelled = dispatch_fixture(
        &runtime,
        "cancelled",
        ProcessorBehavior::Pending,
        AdmissionLimits::default(),
    );
    let task_group = cancelled.service.task_group().clone();
    let dispatcher = Arc::clone(&cancelled.dispatcher);
    let dispatch_task_group = task_group.clone();
    let dispatch_task = tokio::spawn(async move {
        dispatcher
            .dispatch_embedded(
                &dispatch_task_group,
                embedded_context("allowed", None),
                RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(47),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), cancelled.processor.entered.notified())
        .await
        .expect("pending processor should start");
    task_group.cancel();
    let error = match dispatch_task.await.expect("dispatch task should not panic") {
        Ok(_) => panic!("parent cancellation should stop embedded dispatch"),
        Err(error) => error,
    };
    assert!(matches!(error, DispatchError::Response(ResponseSinkError::Cancelled)));
}
