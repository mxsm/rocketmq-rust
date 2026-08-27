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

#![cfg(feature = "test-support")]

use std::future;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
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
use rocketmq_security_api::IngressDecision;
use rocketmq_security_api::IngressPolicy;
use rocketmq_security_api::LayerEvaluation;
use rocketmq_security_api::Principal;
use rocketmq_security_api::RequestPolicy;
use rocketmq_transport::api::v1::AdmissionClass;
use rocketmq_transport::api::v1::AdmissionController;
use rocketmq_transport::api::v1::AdmissionLimits;
use rocketmq_transport::api::v1::AdmissionResource;
use rocketmq_transport::api::v1::AdmissionScope;
use rocketmq_transport::api::v1::AuthorizedCommandDispatcher;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use rocketmq_transport::api::v1::DispatchError;
use rocketmq_transport::api::v1::RequestContext;
use rocketmq_transport::api::v1::RequestContextError;
use rocketmq_transport::api::v1::RequestDeadline;
use rocketmq_transport::api::v1::RequestOrdering;
use rocketmq_transport::api::v1::RequestProcessor;
use rocketmq_transport::api::v1::ResourceLimit;
use rocketmq_transport::api::v1::ResponseSinkError;
use rocketmq_transport::api::v1::ServerConfig;
use rocketmq_transport::api::v1::TransportSecurity;
use rocketmq_transport::api::v1::TransportServer;
use rocketmq_transport::api::v1::TransportTelemetry;
use rocketmq_transport::test_support::transport_io_snapshot;
use rocketmq_transport::test_support::Connection;
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

impl RequestProcessor for ConformanceProcessor {
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum CloneIdentityEvent {
    Clone { from: usize, to: usize },
    Ordering { generation: usize, code: i32 },
    Process { generation: usize, code: i32 },
}

struct CloneIdentityProcessor {
    generation: usize,
    events: Arc<Mutex<Vec<CloneIdentityEvent>>>,
}

impl CloneIdentityProcessor {
    fn supplied(events: Arc<Mutex<Vec<CloneIdentityEvent>>>) -> Self {
        Self { generation: 0, events }
    }
}

impl Clone for CloneIdentityProcessor {
    fn clone(&self) -> Self {
        let generation = self.generation + 1;
        self.events
            .lock()
            .expect("clone identity event lock")
            .push(CloneIdentityEvent::Clone {
                from: self.generation,
                to: generation,
            });
        Self {
            generation,
            events: Arc::clone(&self.events),
        }
    }
}

impl RequestProcessor for CloneIdentityProcessor {
    async fn process_request(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.events
            .lock()
            .expect("clone identity event lock")
            .push(CloneIdentityEvent::Process {
                generation: self.generation,
                code: request.code(),
            });
        Ok(Some(
            RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                .set_body(vec![self.generation as u8]),
        ))
    }

    fn request_ordering(&self, request: &RemotingCommand) -> RequestOrdering {
        self.events
            .lock()
            .expect("clone identity event lock")
            .push(CloneIdentityEvent::Ordering {
                generation: self.generation,
                code: request.code(),
            });
        RequestOrdering::Concurrent
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

struct CountingLegacyPolicy {
    calls: Arc<AtomicUsize>,
}

struct RecordingResourcePolicy {
    requests: Arc<Mutex<Vec<(i32, String)>>>,
}

impl RequestPolicy for RecordingResourcePolicy {
    fn evaluate_authenticated(&self, context: AuthenticatedRequestContext<'_>) -> Decision {
        self.requests
            .lock()
            .expect("recording resource policy lock")
            .push((context.request().code(), context.resource().name().to_owned()));
        Decision::Allow
    }
}

impl RequestPolicy for CountingLegacyPolicy {
    fn evaluate_authenticated(&self, _context: AuthenticatedRequestContext<'_>) -> Decision {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Decision::Allow
    }
}

struct DenyIngressPolicy {
    calls: Arc<AtomicUsize>,
}

impl IngressPolicy for DenyIngressPolicy {
    fn evaluate_ingress(
        &self,
        _request: rocketmq_security_api::SecurityRequestView<'_>,
    ) -> LayerEvaluation<IngressDecision> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(IngressDecision::Deny)
    }
}

struct RecordingNetworkPeerIngressPolicy {
    peers: Arc<Mutex<Vec<Option<SocketAddr>>>>,
}

impl IngressPolicy for RecordingNetworkPeerIngressPolicy {
    fn evaluate_ingress(
        &self,
        request: rocketmq_security_api::SecurityRequestView<'_>,
    ) -> LayerEvaluation<IngressDecision> {
        self.peers
            .lock()
            .expect("recording network peer ingress policy lock")
            .push(request.peer().map(|peer| peer.address()));
        Ok(IngressDecision::Deny)
    }
}

struct DispatchFixture {
    service: ChildServiceContext,
    processor: ConformanceProcessor,
    dispatcher: Arc<AuthorizedCommandDispatcher<ConformanceProcessor>>,
    security: Arc<TransportSecurity>,
    admission: Arc<AdmissionController>,
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
    network_round_trip_with_principal(fixture, Some(principal), command).await
}

async fn network_round_trip_with_principal(
    fixture: &DispatchFixture,
    principal: Option<&str>,
    command: RemotingCommand,
) -> RemotingCommand {
    network_round_trip_with_principal_and_peer(fixture, principal, command)
        .await
        .0
}

async fn network_round_trip_with_principal_and_peer(
    fixture: &DispatchFixture,
    principal: Option<&str>,
    command: RemotingCommand,
) -> (RemotingCommand, SocketAddr) {
    let config = Arc::new(ServerConfig {
        bind_address: "127.0.0.1".to_owned(),
        listen_port: 0,
        ..ServerConfig::default()
    });
    let mut server = TransportServer::new(config, fixture.service.component("network"))
        .with_transport_security(Arc::clone(&fixture.security), principal.map(Principal::new))
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
    let stream = TcpStream::connect(address).await.expect("client should connect");
    let peer_seen_by_server = stream
        .local_addr()
        .expect("connected client stream should expose its local address");
    let mut connection = Connection::new(stream);
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
    (response, peer_seen_by_server)
}

#[tokio::test]
async fn secure_network_dispatch_rejects_command_claimed_authentication_and_origin() {
    let runtime = RuntimeContext::from_current("authorized-dispatch-forged-ingress-facts");
    let fixture = dispatch_fixture(
        &runtime,
        "forged-ingress-facts",
        ProcessorBehavior::Echo,
        AdmissionLimits::default(),
    );
    let mut command = RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(50);
    command.add_ext_field("principal", "allowed");
    command.add_ext_field("origin", "embedded");

    let response = network_round_trip_with_principal(&fixture, None, command).await;

    assert_eq!(response.code(), ResponseCode::NoPermission.to_i32());
    assert_eq!(response.opaque(), 50);
    assert_eq!(fixture.processor.calls.load(Ordering::SeqCst), 0);
    let report = fixture.service.task_group().shutdown(Duration::from_secs(1)).await;
    report.assert_no_task_leak().expect("test tasks should be owned");
}

#[tokio::test]
async fn network_ingress_policy_receives_actual_peer_despite_forged_origin_extension() {
    let runtime = RuntimeContext::from_current("authorized-dispatch-forged-network-origin");
    let service = runtime.service_context("forged-network-origin");
    let process_budget = service.process_budget();
    let admission = Arc::new(
        AdmissionController::try_new_with_budget(AdmissionLimits::default(), &process_budget)
            .expect("test admission limits should be valid"),
    );
    let peers = Arc::new(Mutex::new(Vec::new()));
    let security = Arc::new(
        TransportSecurity::secure_enforced(Some(Arc::new(AllowOnlyNamedPrincipal)), None).with_ingress_policy(
            Arc::new(RecordingNetworkPeerIngressPolicy {
                peers: Arc::clone(&peers),
            }),
        ),
    );
    let processor = ConformanceProcessor::new(ProcessorBehavior::Echo);
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
    let fixture = DispatchFixture {
        service,
        processor,
        dispatcher,
        security,
        admission,
    };
    let mut command = RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(51);
    command.add_ext_field("origin", "embedded");

    let (response, actual_peer) = network_round_trip_with_principal_and_peer(&fixture, None, command).await;

    assert_eq!(response.code(), ResponseCode::NoPermission.to_i32());
    assert_eq!(response.opaque(), 51);
    assert_eq!(fixture.processor.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        peers
            .lock()
            .expect("recording network peer ingress policy lock")
            .as_slice(),
        [Some(actual_peer)],
    );
    let report = fixture.service.task_group().shutdown(Duration::from_secs(1)).await;
    report.assert_no_task_leak().expect("test tasks should be owned");
}

fn assert_equivalent(actual: &RemotingCommand, expected: &RemotingCommand) {
    assert_eq!(actual.code(), expected.code());
    assert_eq!(actual.opaque(), expected.opaque());
    assert_eq!(actual.remark(), expected.remark());
    assert_eq!(actual.ext_fields(), expected.ext_fields());
    assert_eq!(actual.body(), expected.body());
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
    assert_eq!(embedded.opaque(), 41);
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
    assert_eq!(embedded_denied.opaque(), 42);
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
    assert_eq!(embedded_malformed.opaque(), 43);

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
    assert_eq!(embedded_error.opaque(), 44);

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
    assert_eq!(deadline_response.opaque(), 45);
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
    assert_eq!(embedded_overloaded.opaque(), 46);
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

#[tokio::test]
async fn configured_ingress_deny_short_circuits_legacy_policy_and_handler() {
    let runtime = RuntimeContext::from_current("authorized-dispatch-ingress-short-circuit");
    let service = runtime.service_context("ingress-short-circuit");
    let process_budget = service.process_budget();
    let admission = Arc::new(
        AdmissionController::try_new_with_budget(AdmissionLimits::default(), &process_budget)
            .expect("test admission limits should be valid"),
    );
    let ingress_calls = Arc::new(AtomicUsize::new(0));
    let legacy_calls = Arc::new(AtomicUsize::new(0));
    let security = Arc::new(
        TransportSecurity::secure_enforced(
            Some(Arc::new(CountingLegacyPolicy {
                calls: Arc::clone(&legacy_calls),
            })),
            None,
        )
        .with_ingress_policy(Arc::new(DenyIngressPolicy {
            calls: Arc::clone(&ingress_calls),
        })),
    );
    let processor = ConformanceProcessor::new(ProcessorBehavior::Echo);
    let dispatcher = Arc::new(
        AuthorizedCommandDispatcher::try_new(
            processor.clone(),
            Vec::new(),
            &process_budget,
            TransportTelemetry::noop(),
            security,
            admission,
        )
        .expect("test dispatcher should fit the process budget"),
    );

    let response = dispatcher
        .dispatch_embedded(
            service.task_group(),
            embedded_context("allowed", None),
            RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(48),
        )
        .await
        .expect("ingress denial should produce a response");

    assert_eq!(response.code(), ResponseCode::NoPermission.to_i32());
    assert_eq!(response.opaque(), 48);
    assert_eq!(ingress_calls.load(Ordering::SeqCst), 1);
    assert_eq!(legacy_calls.load(Ordering::SeqCst), 0);
    assert_eq!(processor.calls.load(Ordering::SeqCst), 0);
    let report = service.task_group().shutdown(Duration::from_secs(1)).await;
    report.assert_no_task_leak().expect("test tasks should be owned");
}

#[tokio::test]
async fn unknown_raw_code_is_preserved_in_the_authorization_resource() {
    const UNKNOWN_RAW_CODE: i32 = -91_763;

    let runtime = RuntimeContext::from_current("authorized-dispatch-unknown-resource");
    let service = runtime.service_context("unknown-resource");
    let process_budget = service.process_budget();
    let admission = Arc::new(
        AdmissionController::try_new_with_budget(AdmissionLimits::default(), &process_budget)
            .expect("test admission limits should be valid"),
    );
    let requests = Arc::new(Mutex::new(Vec::new()));
    let security = Arc::new(TransportSecurity::secure_enforced(
        Some(Arc::new(RecordingResourcePolicy {
            requests: Arc::clone(&requests),
        })),
        None,
    ));
    let processor = ConformanceProcessor::new(ProcessorBehavior::Echo);
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
    let fixture = DispatchFixture {
        service,
        processor,
        dispatcher,
        security,
        admission,
    };
    let command = RemotingCommand::create_remoting_command(UNKNOWN_RAW_CODE).set_opaque(49);

    let embedded = dispatch_embedded(&fixture, "allowed", None, command.clone()).await;
    let network = network_round_trip(&fixture, "allowed", command).await;

    assert_eq!(embedded.code(), ResponseCode::Success.to_i32());
    assert_eq!(network.code(), embedded.code());
    assert_eq!(network.opaque(), embedded.opaque());
    assert_eq!(
        requests.lock().expect("recorded authorization requests").as_slice(),
        [
            (UNKNOWN_RAW_CODE, UNKNOWN_RAW_CODE.to_string()),
            (UNKNOWN_RAW_CODE, UNKNOWN_RAW_CODE.to_string()),
        ]
    );
    assert_eq!(fixture.processor.calls.load(Ordering::SeqCst), 2);
    let report = fixture.service.task_group().shutdown(Duration::from_secs(1)).await;
    report.assert_no_task_leak().expect("test tasks should be owned");
}

#[tokio::test]
async fn v1_embedded_retains_the_supplied_processor_while_network_uses_its_admitted_clone() {
    const EMBEDDED_CODE: i32 = 61_000;
    const NETWORK_CODE: i32 = 61_001;

    let runtime = RuntimeContext::from_current("authorized-dispatch-clone-identity");
    let service = runtime.service_context("clone-identity");
    let process_budget = service.process_budget();
    let admission = Arc::new(
        AdmissionController::try_new_with_budget(AdmissionLimits::default(), &process_budget)
            .expect("test admission limits should be valid"),
    );
    let security = Arc::new(TransportSecurity::development_insecure_loopback(None, None));
    let events = Arc::new(Mutex::new(Vec::new()));
    let dispatcher = Arc::new(
        AuthorizedCommandDispatcher::try_new(
            CloneIdentityProcessor::supplied(Arc::clone(&events)),
            Vec::new(),
            &process_budget,
            TransportTelemetry::noop(),
            Arc::clone(&security),
            admission,
        )
        .expect("test dispatcher should fit the process budget"),
    );

    assert_eq!(
        events.lock().expect("clone identity event lock").as_slice(),
        [CloneIdentityEvent::Clone { from: 0, to: 1 }]
    );
    events.lock().expect("clone identity event lock").clear();

    let embedded = dispatcher
        .dispatch_embedded(
            service.task_group(),
            RequestContext::try_embedded(Some(Principal::new("clone-identity")), None)
                .expect("embedded identity should be valid"),
            RemotingCommand::create_remoting_command(EMBEDDED_CODE).set_opaque(60),
        )
        .await
        .expect("embedded processor should respond");
    assert_eq!(embedded.body().map(|body| body.as_ref()), Some([1_u8].as_slice()));
    assert_eq!(
        events.lock().expect("clone identity event lock").as_slice(),
        [
            CloneIdentityEvent::Ordering {
                generation: 0,
                code: EMBEDDED_CODE,
            },
            CloneIdentityEvent::Clone { from: 0, to: 1 },
            CloneIdentityEvent::Process {
                generation: 1,
                code: EMBEDDED_CODE,
            },
        ]
    );
    events.lock().expect("clone identity event lock").clear();

    let config = Arc::new(ServerConfig {
        bind_address: "127.0.0.1".to_owned(),
        listen_port: 0,
        ..ServerConfig::default()
    });
    let mut server = TransportServer::new(config, service.component("network"))
        .with_transport_security(Arc::clone(&security), Some(Principal::new("clone-identity")))
        .with_authorized_dispatcher(Arc::clone(&dispatcher));
    let (startup_tx, startup_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let automatic_events = Arc::new(Mutex::new(Vec::new()));
    let server_task = tokio::spawn(async move {
        server
            .run_with_shutdown_report_and_startup(
                CloneIdentityProcessor::supplied(automatic_events),
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
    assert!(events.lock().expect("clone identity event lock").is_empty());

    let stream = TcpStream::connect(address).await.expect("client should connect");
    let mut connection = Connection::new(stream);
    connection
        .send_command(RemotingCommand::create_remoting_command(NETWORK_CODE).set_opaque(61))
        .await
        .expect("network request should be written");
    let network = tokio::time::timeout(Duration::from_secs(2), connection.receive_command())
        .await
        .expect("network request should complete before the test timeout")
        .expect("network read should succeed")
        .expect("server should return one response frame");
    assert_eq!(network.body().map(|body| body.as_ref()), Some([2_u8].as_slice()));
    assert_eq!(
        events.lock().expect("clone identity event lock").as_slice(),
        [
            CloneIdentityEvent::Ordering {
                generation: 1,
                code: NETWORK_CODE,
            },
            CloneIdentityEvent::Clone { from: 1, to: 2 },
            CloneIdentityEvent::Process {
                generation: 2,
                code: NETWORK_CODE,
            },
        ]
    );

    drop(connection);
    let _ = shutdown_tx.send(());
    let report = server_task
        .await
        .expect("server task should not panic")
        .expect("server should report shutdown");
    assert!(report.is_healthy(), "{}", report.to_json());
    let report = service.task_group().shutdown(Duration::from_secs(1)).await;
    report.assert_no_task_leak().expect("test tasks should be owned");
}
