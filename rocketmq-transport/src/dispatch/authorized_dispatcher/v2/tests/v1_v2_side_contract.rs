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

//! Conformance coverage for the intentionally shared V1/V2 dispatcher side contract.
//!
//! V2 dispatch is crate-private, so this module uses its real session harness rather than
//! claiming that the public V2 processor surface is a dispatcher. The V1 cases use the real
//! network listener with an ephemeral loopback port. Both paths record the same data-only events.

use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeContext;
use rocketmq_security_api::Principal;
use tokio::net::TcpStream;
use tokio::sync::oneshot;

use super::super::AuthorizedCommandDispatcherV2;
use super::event_log::admitted_events as shared_admitted_events;
use super::event_log::admitted_events_without_dispatcher_write as shared_admitted_events_without_dispatcher_write;
use super::event_log::admitted_events_without_hooks as shared_admitted_events_without_hooks;
use super::event_log::rejected_events as shared_rejected_events;
use super::event_log::DispatcherEvent as SideEvent;
use super::event_log::EventLog;
use super::harness::*;
use crate::admission::AdmissionController;
use crate::admission::AdmissionLimits;
use crate::config::ServerConfig;
use crate::dispatch::AuthorizedCommandDispatcher;
use crate::dispatch::HandlerOutcome;
use crate::dispatch::ResponseBodyKind;
use crate::dispatch::ResponseDisposition;
use crate::dispatch::ResponsePlan;
use crate::net::channel::Channel;
use crate::remoting_server::rocketmq_tokio_server::TransportServer;
use crate::request_ordering::RequestOrdering;
use crate::request_ordering::RequestOrderingKey;
use crate::runtime::connection_handler_context::ConnectionHandlerContext;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::processor::ResponseWriteObservation;
use crate::runtime::processor::ResponseWriteOutcome;
use crate::runtime::processor_v2::RejectRequestDecision;
use crate::runtime::processor_v2::RequestProcessorV2;
use crate::runtime::processor_v2::ResponseWriteObservationV2;
use crate::runtime::processor_v2::ResponseWriteOutcomeV2;
use crate::runtime::processor_v2::ResponseWritePath;
use crate::runtime::RPCHook;
use crate::security::TransportSecurity;
use crate::telemetry::TransportTelemetry;
use crate::test_support::Connection;

const STANDARD: i32 = 220;
const REJECT: i32 = 221;
const ONEWAY: i32 = 222;
const SENTINEL: i32 = 223;
const DIRECT_WRITE: i32 = 224;
const V2_NO_REPLY: i32 = 39;
const ORIGINAL_OPAQUE: i32 = 811;
const SENTINEL_OPAQUE: i32 = 812;
const MUTATED_CODE: i32 = 1_220;
const MUTATED_OPAQUE: i32 = 1_811;
const RESPONSE_OPAQUE: i32 = 2_811;
const ORDERING_KEY: RequestOrderingKey = RequestOrderingKey::new(9_792);
const EXTENSION_KEY: &str = "dsp04-stage";
const INGRESS_EXTENSION_VALUE: &str = "ingress";
const HOOK_EXTENSION_VALUE: &str = "hook-mutated";

fn admitted_events(code: i32, opaque: i32) -> Vec<SideEvent> {
    shared_admitted_events(
        code,
        opaque,
        MUTATED_CODE,
        MUTATED_OPAQUE,
        INGRESS_EXTENSION_VALUE,
        HOOK_EXTENSION_VALUE,
        ResponseCode::Success.to_i32(),
    )
}

fn admitted_events_without_dispatcher_write(code: i32, opaque: i32) -> Vec<SideEvent> {
    shared_admitted_events_without_dispatcher_write(
        code,
        opaque,
        MUTATED_CODE,
        MUTATED_OPAQUE,
        INGRESS_EXTENSION_VALUE,
        HOOK_EXTENSION_VALUE,
    )
}

fn admitted_events_without_hooks(code: i32, opaque: i32) -> Vec<SideEvent> {
    shared_admitted_events_without_hooks(code, opaque, INGRESS_EXTENSION_VALUE, ResponseCode::Success.to_i32())
}

fn rejected_events(code: i32, opaque: i32) -> Vec<SideEvent> {
    shared_rejected_events(code, opaque, INGRESS_EXTENSION_VALUE, ResponseCode::SystemBusy.to_i32())
}

fn extension_value(command: &RemotingCommand) -> String {
    command
        .ext_fields()
        .and_then(|fields| fields.get(EXTENSION_KEY))
        .map(ToString::to_string)
        .expect("side-contract request should preserve its extension field")
}

#[derive(Clone, Copy)]
enum V1Behavior {
    Standard,
    Reject,
    DirectWriteThenNone,
}

struct V1State {
    events: EventLog<SideEvent>,
    clones: AtomicUsize,
    record_dispatch_clones: AtomicBool,
    observations: Mutex<Vec<ResponseWriteObservation>>,
}

impl V1State {
    fn new() -> Self {
        Self {
            events: EventLog::default(),
            clones: AtomicUsize::new(0),
            record_dispatch_clones: AtomicBool::new(false),
            observations: Mutex::new(Vec::new()),
        }
    }
}

struct V1Processor {
    behavior: V1Behavior,
    state: Arc<V1State>,
}

impl V1Processor {
    fn new(behavior: V1Behavior, state: Arc<V1State>) -> Self {
        Self { behavior, state }
    }

    fn detached(behavior: V1Behavior) -> Self {
        Self::new(behavior, Arc::new(V1State::new()))
    }
}

impl Clone for V1Processor {
    fn clone(&self) -> Self {
        self.state.clones.fetch_add(1, Ordering::SeqCst);
        if self.state.record_dispatch_clones.load(Ordering::SeqCst) {
            self.state.events.push(SideEvent::Clone);
        }
        Self {
            behavior: self.behavior,
            state: Arc::clone(&self.state),
        }
    }
}

impl RequestProcessor for V1Processor {
    async fn process_request(
        &mut self,
        _channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.state.events.push(SideEvent::Process {
            code: request.code(),
            opaque: request.opaque(),
            extension_value: extension_value(request),
        });
        match self.behavior {
            V1Behavior::DirectWriteThenNone if request.code() == DIRECT_WRITE => {
                ctx.try_write_response(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                        .set_opaque(request.opaque())
                        .set_body(b"legacy direct write".to_vec()),
                )
                .await
                .map_err(|error| {
                    rocketmq_error::RocketMQError::response_process_failed("v1_v2_side_contract", error.to_string())
                })?;
                self.state.events.push(SideEvent::V1DirectWriteThenNone);
                Ok(None)
            }
            V1Behavior::Standard | V1Behavior::Reject | V1Behavior::DirectWriteThenNone => Ok(Some(
                RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_opaque(RESPONSE_OPAQUE),
            )),
        }
    }

    fn reject_request(&self, code: i32) -> (bool, Option<RemotingCommand>) {
        self.state.events.push(SideEvent::RejectCheck { code });
        if matches!(self.behavior, V1Behavior::Reject) && code == REJECT {
            self.state.events.push(SideEvent::Reject { code });
            (
                true,
                Some(RemotingCommand::create_response_command_with_code(
                    ResponseCode::SystemBusy,
                )),
            )
        } else {
            (false, None)
        }
    }

    fn request_ordering(&self, request: &RemotingCommand) -> RequestOrdering {
        self.state.events.push(SideEvent::Ordering {
            code: request.code(),
            opaque: request.opaque(),
            extension_value: extension_value(request),
        });
        RequestOrdering::Ordered(ORDERING_KEY)
    }

    fn observe_response_write(&self, observation: ResponseWriteObservation) {
        self.state
            .observations
            .lock()
            .expect("V1 side-contract observations lock")
            .push(observation);
        self.state.events.push(SideEvent::Observe {
            request_code: observation.request_code,
            response_code: observation.response_code,
        });
        assert_eq!(observation.outcome, ResponseWriteOutcome::Sent);
    }
}

struct SideHook {
    events: EventLog<SideEvent>,
    clear_oneway: bool,
}

impl RPCHook for SideHook {
    fn do_before_request(
        &self,
        _remote_addr: SocketAddr,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.events.push(SideEvent::Before {
            code: request.code(),
            opaque: request.opaque(),
            extension_value: extension_value(request),
        });
        if self.clear_oneway && request.is_oneway_rpc() {
            *request = RemotingCommand::create_remoting_command(request.code()).set_opaque(request.opaque());
            assert!(!request.is_oneway_rpc());
        }
        request.set_code_mut(MUTATED_CODE);
        request.set_opaque_mut(MUTATED_OPAQUE);
        request.add_ext_field(EXTENSION_KEY, HOOK_EXTENSION_VALUE);
        Ok(())
    }

    fn do_after_response(
        &self,
        _remote_addr: SocketAddr,
        request: &RemotingCommand,
        _response: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.events.push(SideEvent::After {
            code: request.code(),
            opaque: request.opaque(),
            extension_value: extension_value(request),
        });
        Ok(())
    }
}

struct V1Fixture {
    service: ChildServiceContext,
    dispatcher: Arc<AuthorizedCommandDispatcher<V1Processor>>,
    security: Arc<TransportSecurity>,
    behavior: V1Behavior,
    state: Arc<V1State>,
}

fn v1_fixture(
    runtime: &RuntimeContext,
    name: &'static str,
    behavior: V1Behavior,
    with_hook: bool,
    clear_hook_oneway: bool,
) -> V1Fixture {
    let service = runtime.service_context(name);
    let state = Arc::new(V1State::new());
    let security = Arc::new(TransportSecurity::development_insecure_loopback(None, None));
    let admission = Arc::new(
        AdmissionController::try_new_with_budget(AdmissionLimits::default(), &service.process_budget())
            .expect("V1 side-contract admission limits should be valid"),
    );
    let hooks = with_hook
        .then(|| {
            Arc::new(SideHook {
                events: state.events.clone(),
                clear_oneway: clear_hook_oneway,
            }) as Arc<dyn RPCHook>
        })
        .into_iter()
        .collect();
    let dispatcher = Arc::new(
        AuthorizedCommandDispatcher::try_new(
            V1Processor::new(behavior, Arc::clone(&state)),
            hooks,
            &service.process_budget(),
            TransportTelemetry::noop(),
            Arc::clone(&security),
            admission,
        )
        .expect("V1 side-contract dispatcher should fit the process budget"),
    );
    state.record_dispatch_clones.store(true, Ordering::SeqCst);
    V1Fixture {
        service,
        dispatcher,
        security,
        behavior,
        state,
    }
}

async fn run_v1_network(
    fixture: &V1Fixture,
    commands: Vec<RemotingCommand>,
    expected_frames: usize,
) -> Vec<RemotingCommand> {
    let config = Arc::new(ServerConfig {
        bind_address: "127.0.0.1".to_owned(),
        listen_port: 0,
        ..ServerConfig::default()
    });
    let mut server = TransportServer::new(config, fixture.service.component("network"))
        .with_transport_security(Arc::clone(&fixture.security), Some(Principal::new("side-contract")))
        .with_authorized_dispatcher(Arc::clone(&fixture.dispatcher));
    let (startup_tx, startup_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_processor = V1Processor::detached(fixture.behavior);
    let server_task = tokio::spawn(async move {
        server
            .run_with_shutdown_report_and_startup(
                server_processor,
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
        .expect("V1 side-contract server should report startup")
        .expect("V1 side-contract server should bind to an ephemeral port");
    let mut connection = Connection::new(TcpStream::connect(address).await.expect("V1 client should connect"));
    let waits_for_sentinel = commands.len() > 1;
    for (index, command) in commands.into_iter().enumerate() {
        connection
            .send_command(command)
            .await
            .expect("V1 side-contract request frame should be written");
        if index == 0 && waits_for_sentinel {
            match fixture.behavior {
                V1Behavior::Standard | V1Behavior::Reject => {
                    tokio::time::timeout(
                        Duration::from_secs(2),
                        fixture
                            .state
                            .events
                            .wait_for(|events| events.iter().any(|event| matches!(event, SideEvent::After { .. }))),
                    )
                    .await
                    .expect("V1 first request should reach its after-hook sentinel barrier");
                }
                V1Behavior::DirectWriteThenNone => {
                    tokio::time::timeout(
                        Duration::from_secs(2),
                        fixture.state.events.wait_for(|events| {
                            events
                                .iter()
                                .any(|event| matches!(event, SideEvent::V1DirectWriteThenNone))
                        }),
                    )
                    .await
                    .expect("V1 direct write should reach its sentinel barrier");
                }
            }
        }
    }

    let mut responses = Vec::with_capacity(expected_frames);
    for _ in 0..expected_frames {
        let response = tokio::time::timeout(Duration::from_secs(2), connection.receive_command())
            .await
            .expect("V1 side-contract response should arrive")
            .expect("V1 side-contract session should remain open")
            .expect("V1 side-contract response should decode");
        responses.push(response);
    }

    connection.shutdown().await.expect("close V1 peer write half");
    let _ = shutdown_tx.send(());
    let report = tokio::time::timeout(Duration::from_secs(3), server_task)
        .await
        .expect("V1 side-contract server should shut down")
        .expect("V1 side-contract server task should not panic")
        .expect("V1 side-contract server should report shutdown");
    assert!(report.is_healthy(), "{}", report.to_json());
    let eof = tokio::time::timeout(Duration::from_secs(2), connection.receive_command())
        .await
        .expect("V1 side-contract peer should observe EOF after the ordered sentinel drains");
    match eof {
        None => {}
        Some(Ok(response)) => panic!(
            "V1 peer received an unexpected frame before EOF: code={}",
            response.code()
        ),
        Some(Err(error)) => panic!("V1 peer failed before EOF: {error}"),
    }
    responses
}

async fn wait_for_v1_observations(state: &V1State, expected: usize) {
    tokio::time::timeout(
        Duration::from_secs(2),
        state.events.wait_for(|events| {
            events
                .iter()
                .filter(|event| matches!(event, SideEvent::Observe { .. }))
                .count()
                >= expected
        }),
    )
    .await
    .expect("V1 side-contract observation barrier");
}

#[derive(Clone, Copy)]
enum V2Behavior {
    Standard,
    Reject,
    ProtocolNoResponse,
}

struct V2State {
    events: EventLog<SideEvent>,
    clones: AtomicUsize,
    record_dispatch_clones: AtomicBool,
    observations: Mutex<Vec<ResponseWriteObservationV2>>,
}

impl V2State {
    fn new() -> Self {
        Self {
            events: EventLog::default(),
            clones: AtomicUsize::new(0),
            record_dispatch_clones: AtomicBool::new(false),
            observations: Mutex::new(Vec::new()),
        }
    }
}

struct V2Processor {
    behavior: V2Behavior,
    state: Arc<V2State>,
    clear_oneway: bool,
}

impl V2Processor {
    fn new(behavior: V2Behavior, state: Arc<V2State>, clear_oneway: bool) -> Self {
        Self {
            behavior,
            state,
            clear_oneway,
        }
    }
}

impl Clone for V2Processor {
    fn clone(&self) -> Self {
        self.state.clones.fetch_add(1, Ordering::SeqCst);
        if self.state.record_dispatch_clones.load(Ordering::SeqCst) {
            self.state.events.push(SideEvent::Clone);
        }
        Self {
            behavior: self.behavior,
            state: Arc::clone(&self.state),
            clear_oneway: self.clear_oneway,
        }
    }
}

impl RequestProcessorV2 for V2Processor {
    async fn process(
        &mut self,
        request: &mut crate::dispatch::RemotingRequest,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.state.events.push(SideEvent::Process {
            code: request.command().code(),
            opaque: request.command().opaque(),
            extension_value: extension_value(request.command()),
        });
        if self.clear_oneway && request.command().is_oneway_rpc() {
            let code = request.command().code();
            let opaque = request.command().opaque();
            *request.command_mut() = RemotingCommand::create_remoting_command(code).set_opaque(opaque);
            request.command_mut().add_ext_field(EXTENSION_KEY, HOOK_EXTENSION_VALUE);
            assert!(!request.command().is_oneway_rpc());
        }
        match self.behavior {
            V2Behavior::Standard | V2Behavior::Reject => Ok(HandlerOutcome::Reply(
                ResponsePlan::command(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                        .set_opaque(RESPONSE_OPAQUE),
                )
                .expect("V2 side-contract response plan should be valid"),
            )),
            V2Behavior::ProtocolNoResponse if request.original_identity().original_code() == V2_NO_REPLY => {
                let marker = request.protocol_no_response(ProtocolNoResponseReason::CallbackHandled)?;
                self.state.events.push(SideEvent::V2ProtocolNoResponse);
                Ok(HandlerOutcome::NoReply(marker))
            }
            V2Behavior::ProtocolNoResponse => Ok(HandlerOutcome::Reply(
                ResponsePlan::command(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                        .set_opaque(RESPONSE_OPAQUE),
                )
                .expect("V2 side-contract sentinel response plan should be valid"),
            )),
        }
    }

    fn reject_request(&self, code: i32) -> RejectRequestDecision {
        self.state.events.push(SideEvent::RejectCheck { code });
        if matches!(self.behavior, V2Behavior::Reject) && code == REJECT {
            self.state.events.push(SideEvent::Reject { code });
            RejectRequestDecision::Reject(
                ResponsePlan::command(RemotingCommand::create_response_command_with_code(
                    ResponseCode::SystemBusy,
                ))
                .expect("V2 side-contract rejection plan should be valid"),
            )
        } else {
            RejectRequestDecision::Proceed
        }
    }

    fn request_ordering(&self, ingress: crate::dispatch::IngressRequestView<'_>) -> RequestOrdering {
        let original = ingress.original_identity();
        let extension_value = ingress
            .ext_fields()
            .and_then(|fields| fields.get(EXTENSION_KEY))
            .map(ToString::to_string)
            .expect("side-contract V2 ingress should preserve its extension field");
        self.state.events.push(SideEvent::Ordering {
            code: original.original_code(),
            opaque: original.original_opaque(),
            extension_value,
        });
        RequestOrdering::Ordered(ORDERING_KEY)
    }

    fn observe_response_write(&self, observation: ResponseWriteObservationV2) {
        self.state
            .observations
            .lock()
            .expect("V2 side-contract observations lock")
            .push(observation);
        self.state.events.push(SideEvent::Observe {
            request_code: observation.original_code(),
            response_code: observation.response_code(),
        });
    }
}

fn v2_command(code: i32, opaque: i32, one_way: bool) -> RemotingCommand {
    let mut command = RemotingCommand::create_remoting_command(code).set_opaque(opaque);
    command.add_ext_field(EXTENSION_KEY, INGRESS_EXTENSION_VALUE);
    if one_way {
        command.mark_oneway_rpc()
    } else {
        command
    }
}

fn record_v2_dispatch_clones(state: &V2State) {
    state.record_dispatch_clones.store(true, Ordering::SeqCst);
}

async fn wait_for_v2_observations(state: &V2State, expected: usize) {
    tokio::time::timeout(
        Duration::from_secs(2),
        state.events.wait_for(|events| {
            events
                .iter()
                .filter(|event| matches!(event, SideEvent::Observe { .. }))
                .count()
                >= expected
        }),
    )
    .await
    .expect("V2 side-contract observation barrier");
}

fn assert_v1_standard_observation(state: &V1State, request_code: i32) {
    let observations = state.observations.lock().expect("V1 side-contract observations lock");
    assert_eq!(observations.len(), 1);
    let observation = observations[0];
    assert_eq!(observation.request_code, request_code);
    assert_eq!(observation.response_code, ResponseCode::Success.to_i32());
    assert_eq!(observation.outcome, ResponseWriteOutcome::Sent);
}

fn assert_v2_standard_observation(state: &V2State, request_id: crate::dispatch::RequestId, request_code: i32) {
    let observations = state.observations.lock().expect("V2 side-contract observations lock");
    assert_eq!(observations.len(), 1);
    let observation = observations[0];
    assert_eq!(observation.request_id(), request_id);
    assert_eq!(observation.original_code(), request_code);
    assert_eq!(observation.response_code(), ResponseCode::Success.to_i32());
    assert_eq!(observation.body_kind(), ResponseBodyKind::Empty);
    assert_eq!(observation.path(), ResponseWritePath::Inline);
    assert!(matches!(
        observation.outcome(),
        ResponseWriteOutcomeV2::Written(receipt)
            if receipt.request_id() == request_id
                && receipt.disposition() == ResponseDisposition::TransportWritten
    ));
}

fn assert_no_v2_dispatch_failures(dispatcher: &AuthorizedCommandDispatcherV2<V2Processor>) {
    let failure_categories = dispatcher.reported_failure_categories();
    assert!(
        failure_categories.is_empty(),
        "V2 side-contract dispatch should not report failure categories: {failure_categories:?}"
    );
}

#[tokio::test]
async fn v1_and_v2_admitted_requests_share_ordering_hook_clone_binding_and_observation_contracts() {
    let runtime = RuntimeContext::from_current("v1-v2-side-contract-standard");
    let v1 = v1_fixture(&runtime, "v1", V1Behavior::Standard, true, false);
    let v1_clone_baseline = v1.state.clones.load(Ordering::SeqCst);
    let v1_responses = run_v1_network(&v1, vec![v2_command(STANDARD, ORIGINAL_OPAQUE, false)], 1).await;
    wait_for_v1_observations(&v1.state, 1).await;

    assert_eq!(v1_responses.len(), 1);
    assert_eq!(v1_responses[0].code(), ResponseCode::Success.to_i32());
    assert_eq!(v1_responses[0].opaque(), ORIGINAL_OPAQUE);
    assert_eq!(v1.state.clones.load(Ordering::SeqCst) - v1_clone_baseline, 1);
    assert_eq!(v1.state.events.snapshot(), admitted_events(STANDARD, ORIGINAL_OPAQUE));
    assert_v1_standard_observation(&v1.state, STANDARD);

    let mut v2 = DispatchHarness::new("v1-v2-side-contract-standard-v2").await;
    let v2_state = Arc::new(V2State::new());
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
        V2Processor::new(V2Behavior::Standard, Arc::clone(&v2_state), false),
        vec![Arc::new(SideHook {
            events: v2_state.events.clone(),
            clear_oneway: false,
        })],
    ));
    record_v2_dispatch_clones(&v2_state);
    let v2_clone_baseline = v2_state.clones.load(Ordering::SeqCst);
    let command = v2_command(STANDARD, ORIGINAL_OPAQUE, false);
    let (session, original) = v2.request_session(&command);
    dispatcher
        .dispatch(&v2.authorized, session, v2.context(None), command, 256, None)
        .await
        .expect("V2 standard side-contract dispatch should be admitted");
    let response = v2.receive().await;
    wait_for_v2_observations(&v2_state, 1).await;

    assert_eq!(response.code(), ResponseCode::Success.to_i32());
    assert_eq!(response.opaque(), ORIGINAL_OPAQUE);
    assert_eq!(v2_state.clones.load(Ordering::SeqCst) - v2_clone_baseline, 1);
    assert_eq!(v2_state.events.snapshot(), admitted_events(STANDARD, ORIGINAL_OPAQUE));
    assert_v2_standard_observation(&v2_state, original.request_id(), STANDARD);
    v2.drain_requests().await;
    assert_no_v2_dispatch_failures(&dispatcher);
    v2.drain_close_and_assert_eof().await;
    v2.shutdown().await;
}

#[tokio::test]
async fn v1_and_v2_rejections_short_circuit_hooks_and_processors_after_ordering() {
    let runtime = RuntimeContext::from_current("v1-v2-side-contract-reject");
    let v1 = v1_fixture(&runtime, "v1", V1Behavior::Reject, true, false);
    let v1_clone_baseline = v1.state.clones.load(Ordering::SeqCst);
    let v1_responses = run_v1_network(&v1, vec![v2_command(REJECT, ORIGINAL_OPAQUE, false)], 1).await;
    wait_for_v1_observations(&v1.state, 1).await;

    assert_eq!(v1_responses.len(), 1);
    assert_eq!(v1_responses[0].code(), ResponseCode::SystemBusy.to_i32());
    assert_eq!(v1_responses[0].opaque(), ORIGINAL_OPAQUE);
    assert_eq!(v1.state.clones.load(Ordering::SeqCst) - v1_clone_baseline, 1);
    assert_eq!(v1.state.events.snapshot(), rejected_events(REJECT, ORIGINAL_OPAQUE));

    let mut v2 = DispatchHarness::new("v1-v2-side-contract-reject-v2").await;
    let v2_state = Arc::new(V2State::new());
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
        V2Processor::new(V2Behavior::Reject, Arc::clone(&v2_state), false),
        vec![Arc::new(SideHook {
            events: v2_state.events.clone(),
            clear_oneway: false,
        })],
    ));
    record_v2_dispatch_clones(&v2_state);
    let v2_clone_baseline = v2_state.clones.load(Ordering::SeqCst);
    let command = v2_command(REJECT, ORIGINAL_OPAQUE, false);
    let (session, _original) = v2.request_session(&command);
    dispatcher
        .dispatch(&v2.authorized, session, v2.context(None), command, 256, None)
        .await
        .expect("V2 rejection side-contract dispatch should be admitted");
    let response = v2.receive().await;
    wait_for_v2_observations(&v2_state, 1).await;

    assert_eq!(response.code(), ResponseCode::SystemBusy.to_i32());
    assert_eq!(response.opaque(), ORIGINAL_OPAQUE);
    assert_eq!(v2_state.clones.load(Ordering::SeqCst) - v2_clone_baseline, 1);
    assert_eq!(v2_state.events.snapshot(), rejected_events(REJECT, ORIGINAL_OPAQUE));
    v2.drain_requests().await;
    assert_no_v2_dispatch_failures(&dispatcher);
    v2.drain_close_and_assert_eof().await;
    v2.shutdown().await;
}

#[tokio::test]
async fn v1_and_v2_immutable_oneway_requests_suppress_dispatcher_owned_writes_before_a_sentinel() {
    let runtime = RuntimeContext::from_current("v1-v2-side-contract-oneway");
    let v1 = v1_fixture(&runtime, "v1", V1Behavior::Standard, true, true);
    let v1_clone_baseline = v1.state.clones.load(Ordering::SeqCst);
    let v1_responses = run_v1_network(
        &v1,
        vec![
            v2_command(ONEWAY, ORIGINAL_OPAQUE, true),
            v2_command(SENTINEL, SENTINEL_OPAQUE, false),
        ],
        1,
    )
    .await;
    wait_for_v1_observations(&v1.state, 1).await;

    assert_eq!(v1_responses.len(), 1);
    assert_eq!(v1_responses[0].opaque(), SENTINEL_OPAQUE);
    assert_eq!(v1.state.clones.load(Ordering::SeqCst) - v1_clone_baseline, 2);
    let mut expected = admitted_events_without_dispatcher_write(ONEWAY, ORIGINAL_OPAQUE);
    expected.extend(admitted_events(SENTINEL, SENTINEL_OPAQUE));
    assert_eq!(v1.state.events.snapshot(), expected);
    assert_v1_standard_observation(&v1.state, SENTINEL);

    let mut v2 = DispatchHarness::new("v1-v2-side-contract-oneway-v2").await;
    let v2_state = Arc::new(V2State::new());
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
        V2Processor::new(V2Behavior::Standard, Arc::clone(&v2_state), true),
        vec![Arc::new(SideHook {
            events: v2_state.events.clone(),
            clear_oneway: false,
        })],
    ));
    record_v2_dispatch_clones(&v2_state);
    let v2_clone_baseline = v2_state.clones.load(Ordering::SeqCst);
    let one_way = v2_command(ONEWAY, ORIGINAL_OPAQUE, true);
    let (one_way_session, one_way_original) = v2.request_session(&one_way);
    dispatcher
        .dispatch(&v2.authorized, one_way_session, v2.context(None), one_way, 256, None)
        .await
        .expect("V2 one-way side-contract dispatch should be admitted");
    tokio::time::timeout(
        Duration::from_secs(2),
        v2_state
            .events
            .wait_for(|events| events.iter().any(|event| matches!(event, SideEvent::After { .. }))),
    )
    .await
    .expect("V2 one-way request should reach its after-hook sentinel barrier");
    let sentinel = v2_command(SENTINEL, SENTINEL_OPAQUE, false);
    let (sentinel_session, sentinel_original) = v2.request_session(&sentinel);
    assert_ne!(one_way_original.request_id(), sentinel_original.request_id());
    dispatcher
        .dispatch(&v2.authorized, sentinel_session, v2.context(None), sentinel, 256, None)
        .await
        .expect("V2 sentinel side-contract dispatch should be admitted");
    let response = v2.receive().await;
    wait_for_v2_observations(&v2_state, 1).await;

    assert_eq!(response.opaque(), SENTINEL_OPAQUE);
    assert_eq!(v2_state.clones.load(Ordering::SeqCst) - v2_clone_baseline, 2);
    let mut expected = admitted_events_without_dispatcher_write(ONEWAY, ORIGINAL_OPAQUE);
    expected.extend(admitted_events(SENTINEL, SENTINEL_OPAQUE));
    assert_eq!(v2_state.events.snapshot(), expected);
    assert_v2_standard_observation(&v2_state, sentinel_original.request_id(), SENTINEL);
    v2.drain_requests().await;
    assert_no_v2_dispatch_failures(&dispatcher);
    v2.drain_close_and_assert_eof().await;
    v2.shutdown().await;
}

#[tokio::test]
async fn v1_direct_write_none_and_v2_protocol_no_response_remain_intentionally_asymmetric() {
    // V1's direct context write followed by `None` reaches the legacy
    // `legacy_ambiguous_none` terminal telemetry branch. It is not a V2-style no-response proof.
    let runtime = RuntimeContext::from_current("v1-v2-side-contract-none-asymmetry");
    let v1 = v1_fixture(&runtime, "v1", V1Behavior::DirectWriteThenNone, false, false);
    let v1_clone_baseline = v1.state.clones.load(Ordering::SeqCst);
    let v1_responses = run_v1_network(
        &v1,
        vec![
            v2_command(DIRECT_WRITE, ORIGINAL_OPAQUE, false),
            v2_command(SENTINEL, SENTINEL_OPAQUE, false),
        ],
        2,
    )
    .await;
    wait_for_v1_observations(&v1.state, 1).await;

    assert_eq!(v1_responses.len(), 2);
    assert_eq!(v1_responses[0].opaque(), ORIGINAL_OPAQUE);
    assert_eq!(
        v1_responses[0].body().map(|body| body.as_ref()),
        Some(b"legacy direct write".as_slice())
    );
    assert_eq!(v1_responses[1].opaque(), SENTINEL_OPAQUE);
    assert_eq!(v1.state.clones.load(Ordering::SeqCst) - v1_clone_baseline, 2);
    let mut v1_expected = vec![
        SideEvent::Ordering {
            code: DIRECT_WRITE,
            opaque: ORIGINAL_OPAQUE,
            extension_value: INGRESS_EXTENSION_VALUE.into(),
        },
        SideEvent::Clone,
        SideEvent::RejectCheck { code: DIRECT_WRITE },
        SideEvent::Process {
            code: DIRECT_WRITE,
            opaque: ORIGINAL_OPAQUE,
            extension_value: INGRESS_EXTENSION_VALUE.into(),
        },
        SideEvent::V1DirectWriteThenNone,
    ];
    v1_expected.extend(admitted_events_without_hooks(SENTINEL, SENTINEL_OPAQUE));
    assert_eq!(v1.state.events.snapshot(), v1_expected);
    assert_v1_standard_observation(&v1.state, SENTINEL);

    // V2 has no direct-write outcome. Its no-response path consumes a sealed,
    // allowlisted ProtocolNoResponse marker and therefore performs no write observation.
    let mut v2 = DispatchHarness::new("v1-v2-side-contract-none-asymmetry-v2").await;
    let v2_state = Arc::new(V2State::new());
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
        V2Processor::new(V2Behavior::ProtocolNoResponse, Arc::clone(&v2_state), false),
        vec![Arc::new(SideHook {
            events: v2_state.events.clone(),
            clear_oneway: false,
        })],
    ));
    record_v2_dispatch_clones(&v2_state);
    let v2_clone_baseline = v2_state.clones.load(Ordering::SeqCst);
    let command = v2_command(V2_NO_REPLY, ORIGINAL_OPAQUE, false);
    let (session, _) = v2.request_session(&command);
    dispatcher
        .dispatch(&v2.authorized, session, v2.context(None), command, 256, None)
        .await
        .expect("V2 sealed no-response dispatch should be admitted");
    tokio::time::timeout(
        Duration::from_secs(2),
        v2_state.events.wait_for(|events| {
            events
                .iter()
                .any(|event| matches!(event, SideEvent::V2ProtocolNoResponse))
        }),
    )
    .await
    .expect("V2 sealed no-response request should reach its sentinel barrier");
    let sentinel = v2_command(SENTINEL, SENTINEL_OPAQUE, false);
    let (sentinel_session, sentinel_original) = v2.request_session(&sentinel);
    dispatcher
        .dispatch(&v2.authorized, sentinel_session, v2.context(None), sentinel, 256, None)
        .await
        .expect("V2 no-response sentinel dispatch should be admitted");
    let response = v2.receive().await;
    wait_for_v2_observations(&v2_state, 1).await;

    assert_eq!(response.opaque(), SENTINEL_OPAQUE);
    assert_eq!(v2_state.clones.load(Ordering::SeqCst) - v2_clone_baseline, 2);
    let mut v2_expected = vec![
        SideEvent::Ordering {
            code: V2_NO_REPLY,
            opaque: ORIGINAL_OPAQUE,
            extension_value: INGRESS_EXTENSION_VALUE.into(),
        },
        SideEvent::Clone,
        SideEvent::RejectCheck { code: V2_NO_REPLY },
        SideEvent::Before {
            code: V2_NO_REPLY,
            opaque: ORIGINAL_OPAQUE,
            extension_value: INGRESS_EXTENSION_VALUE.into(),
        },
        SideEvent::Process {
            code: MUTATED_CODE,
            opaque: MUTATED_OPAQUE,
            extension_value: HOOK_EXTENSION_VALUE.into(),
        },
        SideEvent::V2ProtocolNoResponse,
    ];
    v2_expected.extend(admitted_events(SENTINEL, SENTINEL_OPAQUE));
    assert_eq!(v2_state.events.snapshot(), v2_expected);
    assert_v2_standard_observation(&v2_state, sentinel_original.request_id(), SENTINEL);
    v2.drain_requests().await;
    assert_no_v2_dispatch_failures(&dispatcher);
    v2.drain_close_and_assert_eof().await;
    v2.shutdown().await;
}
