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

use std::error::Error as _;
use std::future::Future;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use std::task::Poll;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_error::ViewValueRef;
use rocketmq_error::TRANSPORT_SESSION_FAILED;
use rocketmq_error::TRANSPORT_START_FAILED;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
use rocketmq_protocol::protocol::header::get_consumer_status_request_header::GetConsumerStatusRequestHeader;
use rocketmq_protocol::protocol::header::notify_consumer_ids_changed_request_header::NotifyConsumerIdsChangedRequestHeader;
use rocketmq_protocol::protocol::header::reset_offset_request_header::ResetOffsetRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_security_api::IngressDecision;
use rocketmq_security_api::IngressPolicy;
use rocketmq_security_api::LayerEvaluation;
use rocketmq_security_api::SecurityRequestView;
use tokio::net::TcpStream;

use super::*;
use crate::dispatch::DeferredAdmission;
use crate::dispatch::DeferredAdmissionAcquireOutcome;
use crate::dispatch::DeferredClaimOutcome;
use crate::dispatch::DeferredParts;
use crate::dispatch::DeferredRegistry;
use crate::dispatch::DeferredRegistryOutcome;
use crate::dispatch::DeferredRequest;
use crate::dispatch::DeferredResponderOutcome;
use crate::dispatch::DeferredResumeOutcome;
use crate::dispatch::DeferredResumeRetainedSize;
use crate::dispatch::DeferredRetainedSizeParts;
use crate::dispatch::DeferredWaitLimits;
use crate::dispatch::DeferredWakeReason;
use crate::dispatch::HandlerOutcome;
use crate::dispatch::ProtocolNoResponseReason;
use crate::dispatch::RemotingRequest;
use crate::dispatch::RemotingResponse;
use crate::error::TransportError;
use crate::runtime::processor::RejectRequestDecision;
use crate::runtime::RPCHook;
use crate::session_registry::ServerPushCommand;
use crate::session_registry::ServerPushKind;
use crate::session_registry::ServerPushOutcome;
use crate::session_registry::ServerRequestCommand;
use crate::session_registry::ServerRequestOutcome;
use crate::session_registry::SessionCloseOutcome;
use crate::session_registry::SessionCloseReason;
use crate::session_registry::SessionEvent;
use crate::session_registry::SessionRegistry;
use crate::session_view::SessionId;

#[path = "server/deferred_expiry.rs"]
mod deferred_expiry;
#[path = "server/harness.rs"]
mod harness;
#[path = "server/inline_deferred_state.rs"]
mod inline_deferred_state;

use harness::expect_start_error;
use harness::loopback_server_config;
use harness::start_server;
use harness::start_server_with_shutdown_observer;
use harness::TestRuntime;

fn assert_transport_operation(error: &TransportError, expected_operation: &'static str) {
    let view = error.diagnostic_view().expect("transport error diagnostic view");
    assert!(
        view.fields()
            .any(|field| { field.name() == "operation" && field.value() == ViewValueRef::Text(expected_operation) }),
        "transport diagnostic operation must retain its fixed safe label"
    );
}

fn expect_server_request_error(result: Result<ServerRequestOutcome, TransportError>, message: &str) -> TransportError {
    match result {
        Err(error) => error,
        Ok(_) => panic!("{message}"),
    }
}

#[derive(Default)]
struct ProcessorState {
    clones: AtomicUsize,
    processes: AtomicUsize,
    processor_admission_count: AtomicUsize,
    ordered_entered: tokio::sync::Notify,
    request_sequences: Mutex<Vec<u64>>,
    session: Mutex<Option<crate::session_view::SessionId>>,
}

struct TcpProcessor {
    state: Arc<ProcessorState>,
    admission: Option<Arc<AdmissionController>>,
}

impl Clone for TcpProcessor {
    fn clone(&self) -> Self {
        self.state.clones.fetch_add(1, Ordering::SeqCst);
        Self {
            state: Arc::clone(&self.state),
            admission: self.admission.clone(),
        }
    }
}

impl RequestProcessor for TcpProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> RocketMQResult<HandlerOutcome> {
        self.state.processes.fetch_add(1, Ordering::SeqCst);
        self.state
            .request_sequences
            .lock()
            .expect("request sequence capture lock")
            .push(request.original_identity().request_id().sequence());
        let signal_no_response = request.command().code() == 39 || request.original_identity().is_one_way();
        if signal_no_response {
            self.state.ordered_entered.notify_one();
        }
        if let Some(admission) = &self.admission {
            self.state
                .processor_admission_count
                .store(admission.snapshot().processors.current_count, Ordering::SeqCst);
        }
        *self.state.session.lock().expect("session capture lock") = Some(request.session().id());
        if request.command().code() == 39 {
            return Ok(HandlerOutcome::NoReply(
                request
                    .protocol_no_response(ProtocolNoResponseReason::CallbackHandled)
                    .map_err(|error| RocketMQError::internal("create protocol no-response", error))?,
            ));
        }
        Ok(HandlerOutcome::Reply(
            RemotingResponse::bytes(
                RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_opaque(-9),
                Bytes::from_static(b"tcp-response"),
            )
            .expect("test remoting response"),
        ))
    }

    fn request_ordering(
        &self,
        _ingress: crate::dispatch::IngressRequestView<'_>,
    ) -> crate::request_ordering::RequestOrdering {
        crate::request_ordering::RequestOrdering::Ordered(crate::request_ordering::RequestOrderingKey::new(9800))
    }

    fn reject_request(&self, code: i32) -> RejectRequestDecision {
        if code == 703 {
            return RejectRequestDecision::Reject(
                RemotingResponse::command(RemotingCommand::create_response_command_with_code(44))
                    .expect("test rejection plan"),
            );
        }
        RejectRequestDecision::Proceed
    }
}

struct DropTrackedProcessor {
    drops: Arc<AtomicUsize>,
}

impl Clone for DropTrackedProcessor {
    fn clone(&self) -> Self {
        Self {
            drops: Arc::clone(&self.drops),
        }
    }
}

impl Drop for DropTrackedProcessor {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl RequestProcessor for DropTrackedProcessor {
    async fn process(&mut self, _request: &mut RemotingRequest) -> RocketMQResult<HandlerOutcome> {
        Ok(HandlerOutcome::Reply(
            RemotingResponse::command(RemotingCommand::create_response_command_with_code(
                ResponseCode::Success,
            ))
            .expect("drop-tracked remoting response"),
        ))
    }
}

#[derive(Clone)]
struct DrainingProcessor {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl RequestProcessor for DrainingProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> RocketMQResult<HandlerOutcome> {
        if request.command().code() == 39 {
            self.started.notify_one();
            self.release.notified().await;
            return Ok(HandlerOutcome::NoReply(
                request
                    .protocol_no_response(ProtocolNoResponseReason::CallbackHandled)
                    .map_err(|error| RocketMQError::internal("create protocol no-response", error))?,
            ));
        }
        Ok(HandlerOutcome::Reply(
            RemotingResponse::bytes(
                RemotingCommand::create_response_command_with_code(ResponseCode::Success),
                Bytes::from_static(b"drained-before-retire"),
            )
            .expect("draining remoting response"),
        ))
    }
}

struct CountingHook {
    before: Arc<AtomicUsize>,
    after: Arc<AtomicUsize>,
}

struct OrderedHook {
    id: &'static str,
    events: Arc<Mutex<Vec<(&'static str, &'static str)>>>,
}

struct CountingPolicy {
    calls: Arc<AtomicUsize>,
}

impl IngressPolicy for CountingPolicy {
    fn evaluate_ingress(&self, _request: SecurityRequestView<'_>) -> LayerEvaluation<IngressDecision> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(IngressDecision::AllowToContinue)
    }
}

impl RPCHook for CountingHook {
    fn do_before_request(&self, _remote_addr: SocketAddr, _request: &mut RemotingCommand) -> RocketMQResult<()> {
        self.before.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn do_after_response(
        &self,
        _remote_addr: SocketAddr,
        _request: &RemotingCommand,
        _response: &mut RemotingCommand,
    ) -> RocketMQResult<()> {
        self.after.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl RPCHook for OrderedHook {
    fn do_before_request(&self, _remote_addr: SocketAddr, _request: &mut RemotingCommand) -> RocketMQResult<()> {
        self.events
            .lock()
            .expect("ordered hook event lock")
            .push(("before", self.id));
        Ok(())
    }

    fn do_after_response(
        &self,
        _remote_addr: SocketAddr,
        _request: &RemotingCommand,
        _response: &mut RemotingCommand,
    ) -> RocketMQResult<()> {
        self.events
            .lock()
            .expect("ordered hook event lock")
            .push(("after", self.id));
        Ok(())
    }
}

fn loopback_security() -> Arc<TransportSecurity> {
    Arc::new(TransportSecurity::development_insecure_loopback(None, None))
}

async fn receive_deferred_registration(
    registrations: &mut tokio::sync::mpsc::UnboundedReceiver<NetworkDeferredRegistration>,
    opaque: i32,
) -> NetworkDeferredRegistration {
    let registration = tokio::time::timeout(Duration::from_secs(1), registrations.recv())
        .await
        .expect("network deferred registration deadline")
        .expect("network deferred registration channel");
    assert_eq!(registration.opaque, opaque);
    registration
}

#[derive(Clone)]
struct NetworkDeferredCleanupProcessor {
    registry: DeferredRegistry<usize>,
    admission: DeferredAdmission,
    registered: tokio::sync::mpsc::UnboundedSender<NetworkDeferredRegistration>,
    precommit_opaque: i32,
    release_precommit: Arc<tokio::sync::Notify>,
}

#[derive(Clone, Copy, Debug)]
struct NetworkDeferredRegistration {
    opaque: i32,
    session_id: crate::session_view::SessionId,
    id: crate::dispatch::DeferredId,
}

impl RequestProcessor for NetworkDeferredCleanupProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> RocketMQResult<HandlerOutcome> {
        if request.command().code() == 706 {
            return Ok(HandlerOutcome::Reply(
                RemotingResponse::bytes(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success),
                    Bytes::from_static(b"other-session-live"),
                )
                .expect("other-session remoting response"),
            ));
        }

        let opaque = request.original_identity().original_opaque();
        let responder = match request.take_deferred_responder() {
            DeferredResponderOutcome::Taken(responder) => responder,
            DeferredResponderOutcome::OneWayRequest
            | DeferredResponderOutcome::Unavailable
            | DeferredResponderOutcome::AlreadyTaken
            | DeferredResponderOutcome::OutcomeCompleted => {
                return Err(RocketMQError::illegal_argument("deferred responder is unavailable"));
            }
        };
        let retained = DeferredRegistry::<usize>::try_retained_size(DeferredRetainedSizeParts::new(0))
            .map_err(|error| RocketMQError::internal("size network deferred registration", error))?;
        let permit = match self.admission.try_reserve(retained) {
            DeferredAdmissionAcquireOutcome::Acquired(permit) => permit,
            DeferredAdmissionAcquireOutcome::WaiterCapacityExhausted(_)
            | DeferredAdmissionAcquireOutcome::RetainedByteCapacityExhausted(_)
            | DeferredAdmissionAcquireOutcome::ParentCapacityExhausted(_) => {
                return Err(RocketMQError::illegal_argument(
                    "deferred admission capacity is exhausted",
                ));
            }
        };
        let registration = match self.registry.register(DeferredRequest::new(
            opaque as usize,
            DeferredParts::new(responder, permit),
        )) {
            DeferredRegistryOutcome::Registered(registration) => registration,
            DeferredRegistryOutcome::DuplicateRequest(recovery)
            | DeferredRegistryOutcome::IdentityExhausted(recovery) => {
                drop(recovery);
                return Err(RocketMQError::illegal_argument(
                    "deferred registry rejected the request",
                ));
            }
            DeferredRegistryOutcome::ParentCancelled
            | DeferredRegistryOutcome::SessionClosed
            | DeferredRegistryOutcome::DeadlineExpired => {
                return Err(RocketMQError::illegal_argument("deferred registry lifecycle ended"));
            }
            DeferredRegistryOutcome::BuilderRejected { error, parts } => {
                drop(parts);
                match error {}
            }
            DeferredRegistryOutcome::ContractViolation { violation, recovery } => {
                drop(recovery);
                return Err(RocketMQError::internal("register network deferred request", violation));
            }
            DeferredRegistryOutcome::OperationalFailure { error, recovery } => {
                drop(recovery);
                return Err(RocketMQError::internal("register network deferred request", error));
            }
        };
        self.registered
            .send(NetworkDeferredRegistration {
                opaque,
                session_id: request.session().id(),
                id: registration.deferred_id(),
            })
            .map_err(|_| rocketmq_error::RocketMQError::illegal_argument("registration observer closed"))?;
        if opaque == self.precommit_opaque {
            self.release_precommit.notified().await;
        }
        Ok(HandlerOutcome::Deferred(registration))
    }
}

#[tokio::test]
async fn real_tcp_routes_requests_once_and_drops_unexpected_responses() {
    let runtime = TestRuntime::new("transport-tcp");
    let state = Arc::new(ProcessorState::default());
    let security_calls = Arc::new(AtomicUsize::new(0));
    let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let processor = TcpProcessor {
        state: Arc::clone(&state),
        admission: Some(Arc::clone(&admission)),
    };
    let server = TransportServer::new(loopback_server_config(), runtime.service_context(), processor)
        .with_transport_security(
            Arc::new(
                TransportSecurity::development_insecure_loopback(None, None).with_ingress_policy(Arc::new(
                    CountingPolicy {
                        calls: Arc::clone(&security_calls),
                    },
                )),
            ),
            None,
        )
        .with_admission_controller(admission);
    let (mut client, _address, mut running) = start_server(runtime, server).await;
    state.clones.store(0, Ordering::SeqCst);

    client
        .send_command(RemotingCommand::create_response_command_with_code(91).set_opaque(4_001))
        .await
        .expect("send unexpected response");
    client
        .send_command(RemotingCommand::create_remoting_command(701).set_opaque(4_002))
        .await
        .expect("send request after unexpected response");
    let response = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("TCP response deadline")
        .expect("TCP client remains connected")
        .expect("TCP response frame");
    assert_eq!(response.opaque(), 4_002);
    assert_eq!(
        response.get_type(),
        rocketmq_protocol::protocol::RemotingCommandType::RESPONSE
    );
    assert_eq!(response.body(), Some(&Bytes::from_static(b"tcp-response")));
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert_eq!(state.clones.load(Ordering::SeqCst), 1);
    assert_eq!(security_calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.processor_admission_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        state
            .request_sequences
            .lock()
            .expect("request sequence capture lock")
            .as_slice(),
        [1],
        "the unexpected RESPONSE must not consume a request identity"
    );
    client
        .send_command(RemotingCommand::create_remoting_command(703).set_opaque(4_003))
        .await
        .expect("send rejection request");
    let rejected = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("rejection deadline")
        .expect("connection after rejection")
        .expect("rejection response");
    assert_eq!((rejected.code(), rejected.opaque()), (44, 4_003));

    client
        .send_command(RemotingCommand::create_remoting_command(39).set_opaque(4_004))
        .await
        .expect("send protocol no-response request");
    tokio::time::timeout(Duration::from_secs(1), state.ordered_entered.notified())
        .await
        .expect("no-response processor enters the ordered section");
    client
        .send_command(RemotingCommand::create_remoting_command(701).set_opaque(4_005))
        .await
        .expect("send sentinel after protocol no-response");
    let no_response_sentinel = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("protocol no-response sentinel deadline")
        .expect("protocol no-response keeps the connection open")
        .expect("protocol no-response sentinel frame");
    assert_eq!(
        (no_response_sentinel.code(), no_response_sentinel.opaque()),
        (ResponseCode::Success.to_i32(), 4_005)
    );
    assert_eq!(no_response_sentinel.body(), Some(&Bytes::from_static(b"tcp-response")));

    client
        .send_command(
            RemotingCommand::create_remoting_command(701)
                .set_opaque(4_006)
                .mark_oneway_rpc(),
        )
        .await
        .expect("send one-way request");
    tokio::time::timeout(Duration::from_secs(1), state.ordered_entered.notified())
        .await
        .expect("one-way processor enters the ordered section");
    client
        .send_command(RemotingCommand::create_remoting_command(701).set_opaque(4_007))
        .await
        .expect("send sentinel after one-way request");
    let oneway_sentinel = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("one-way sentinel deadline")
        .expect("one-way keeps the connection open")
        .expect("one-way sentinel frame");
    assert_eq!(
        (oneway_sentinel.code(), oneway_sentinel.opaque()),
        (ResponseCode::Success.to_i32(), 4_007)
    );
    assert_eq!(oneway_sentinel.body(), Some(&Bytes::from_static(b"tcp-response")));

    running.begin_shutdown();
    running.finish().await;
    let eof = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("shutdown should publish EOF");
    assert!(eof.is_none(), "shutdown must not leave an extra response frame");
}

#[tokio::test]
async fn registry_capabilities_write_typed_push_and_close_the_same_canonical_session() {
    let runtime = TestRuntime::new("transport-session-capabilities");
    let state = Arc::new(ProcessorState::default());
    let registry = Arc::new(SessionRegistry::new());
    let processor = TcpProcessor {
        state: Arc::clone(&state),
        admission: None,
    };
    let server = TransportServer::new(loopback_server_config(), runtime.service_context(), processor)
        .with_session_registry(Arc::clone(&registry));
    let (mut client, _address, mut running) = start_server(runtime, server).await;

    client
        .send_command(RemotingCommand::create_remoting_command(701).set_opaque(9_857))
        .await
        .expect("send session identity request");
    let _ = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("identity response deadline")
        .expect("identity session remains connected")
        .expect("identity response frame");
    let session_id = state.session.lock().expect("session capture lock").expect("session id");
    let (push, close) = registry
        .capabilities(session_id)
        .expect("registered session capabilities");

    let ServerPushOutcome::Sent(receipt) = push
        .send(
            ServerPushCommand::NotifyConsumerIdsChanged {
                header: NotifyConsumerIdsChangedRequestHeader {
                    consumer_group: CheetahString::from_static_str("GroupA"),
                    rpc_request_header: None,
                },
                opaque: Some(9_858),
            },
            Duration::from_secs(1),
        )
        .await
        .expect("typed server push write must not fail operationally")
    else {
        panic!("typed server push must complete its canonical write");
    };
    assert_eq!(receipt.session_id(), session_id);
    assert_eq!(receipt.kind(), ServerPushKind::NotifyConsumerIdsChanged);
    let pushed = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("server push deadline")
        .expect("server push keeps session connected")
        .expect("server push frame");
    assert_eq!(pushed.opaque(), 9_858);
    assert!(pushed.is_oneway_rpc());

    let ServerPushOutcome::Sent(transaction_receipt) = push
        .send(
            ServerPushCommand::CheckTransactionState {
                header: CheckTransactionStateRequestHeader {
                    tran_state_table_offset: 11,
                    commit_log_offset: 22,
                    ..CheckTransactionStateRequestHeader::default()
                },
                body: Bytes::from_static(b"transaction-message"),
            },
            Duration::from_secs(1),
        )
        .await
        .expect("typed transaction push write must not fail operationally")
    else {
        panic!("typed transaction push must complete its canonical write");
    };
    assert_eq!(transaction_receipt.kind(), ServerPushKind::CheckTransactionState);
    let transaction = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("transaction push deadline")
        .expect("transaction push connection")
        .expect("transaction push frame");
    assert_eq!(
        RequestCode::from(transaction.code()),
        RequestCode::CheckTransactionState
    );
    assert_eq!(transaction.body(), Some(&Bytes::from_static(b"transaction-message")));
    assert!(transaction.is_oneway_rpc());

    let ServerPushOutcome::Sent(reset_receipt) = push
        .send(
            ServerPushCommand::ResetConsumerClientOffset {
                header: ResetOffsetRequestHeader {
                    topic: CheetahString::from_static_str("TopicA"),
                    group: CheetahString::from_static_str("GroupA"),
                    timestamp: -1,
                    is_force: true,
                    ..ResetOffsetRequestHeader::default()
                },
                body: Bytes::from_static(b"reset-offsets"),
            },
            Duration::from_secs(1),
        )
        .await
        .expect("typed reset push write must not fail operationally")
    else {
        panic!("typed reset push must complete its canonical write");
    };
    assert_eq!(reset_receipt.kind(), ServerPushKind::ResetConsumerClientOffset);
    let reset = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("reset push deadline")
        .expect("reset push connection")
        .expect("reset push frame");
    assert_eq!(RequestCode::from(reset.code()), RequestCode::ResetConsumerClientOffset);
    assert_eq!(reset.body(), Some(&Bytes::from_static(b"reset-offsets")));
    assert!(reset.is_oneway_rpc());

    let concurrent_close = close.clone();
    let (first_close, second_close) = tokio::join!(
        close.close(SessionCloseReason::Administrative),
        concurrent_close.close(SessionCloseReason::HeartbeatTimeout),
    );
    let first_close = first_close.expect("first typed close should complete the server-owned finalizer");
    let second_close = second_close.expect("cloned typed close should share the same completion");
    assert!(
        matches!(
            (first_close, second_close),
            (SessionCloseOutcome::Closed, SessionCloseOutcome::AlreadyClosed)
                | (SessionCloseOutcome::AlreadyClosed, SessionCloseOutcome::Closed)
        ),
        "concurrent closes must share full completion while exactly one caller initiates it"
    );
    let completion = close.completion_snapshot().await;
    assert!(completion.healthy);
    assert_eq!(completion.remaining_inline_tasks, 0);
    assert_eq!(completion.remaining_resume_tasks, 0);
    assert_eq!(completion.removed_waiters, 0);
    assert_eq!(completion.cleanup_panicked_targets, 0);
    assert_eq!(completion.remaining_wait_permits, 0);
    assert_eq!(completion.remaining_server_outbound_leases, 0);
    assert!(!completion.disconnected_panicked);
    assert!(completion.writer_healthy);
    assert_eq!(completion.writer_queued_items, 0);
    assert_eq!(completion.writer_queued_bytes, 0);
    assert!(!registry.contains(session_id));
    assert_eq!(
        close
            .close(SessionCloseReason::Administrative)
            .await
            .expect("repeated typed close should reuse completed shutdown"),
        SessionCloseOutcome::AlreadyClosed
    );
    let eof = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("typed close EOF deadline");
    assert!(eof.is_none());

    running.begin_shutdown();
    running.finish().await;
}

struct PanickingDisconnectListener;

impl crate::session_registry::SessionLifecycleListener for PanickingDisconnectListener {
    fn on_session_connected(&self, _session: &crate::session_view::SessionView) {}

    fn on_session_disconnected(&self, _session_id: SessionId) {
        panic!("test disconnected listener panic");
    }
}

struct PanickingConnectListener;

impl crate::session_registry::SessionLifecycleListener for PanickingConnectListener {
    fn on_session_connected(&self, _session: &crate::session_view::SessionView) {
        panic!("test connected listener panic");
    }

    fn on_session_disconnected(&self, _session_id: SessionId) {}
}

#[tokio::test]
async fn connected_listener_panic_rolls_back_registration_and_finishes_session() {
    let runtime = TestRuntime::new("transport-connected-listener-panic");
    let state = Arc::new(ProcessorState::default());
    let registry = Arc::new(SessionRegistry::with_lifecycle_listener(Arc::new(
        PanickingConnectListener,
    )));
    let processor = TcpProcessor { state, admission: None };
    let server = TransportServer::new(loopback_server_config(), runtime.service_context(), processor)
        .with_session_registry(Arc::clone(&registry));
    let (mut client, _address, mut running) = start_server(runtime, server).await;

    let _ = client.send_command(RemotingCommand::create_remoting_command(701)).await;
    let eof = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("connected-listener panic session completion deadline");
    assert!(eof.is_none());
    assert!(registry.is_empty());

    running.begin_shutdown();
    tokio::time::timeout(Duration::from_secs(1), running.finish())
        .await
        .expect("connected-listener panic server shutdown deadline");
}

struct BlockingDisconnectListener {
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

struct BlockingPanickingDisconnectListener {
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl crate::session_registry::SessionLifecycleListener for BlockingPanickingDisconnectListener {
    fn on_session_connected(&self, _session: &crate::session_view::SessionView) {}

    fn on_session_disconnected(&self, _session_id: SessionId) {
        if let Some(entered) = self.entered.lock().expect("panicking listener entered lock").take() {
            let _ = entered.send(());
        }
        self.release
            .lock()
            .expect("panicking listener release lock")
            .recv()
            .expect("release panicking disconnect listener");
        panic!("test disconnect listener panic after close initiation");
    }
}

impl crate::session_registry::SessionLifecycleListener for BlockingDisconnectListener {
    fn on_session_connected(&self, _session: &crate::session_view::SessionView) {}

    fn on_session_disconnected(&self, _session_id: SessionId) {
        if let Some(entered) = self.entered.lock().expect("blocking listener entered lock").take() {
            let _ = entered.send(());
        }
        self.release
            .lock()
            .expect("blocking listener release lock")
            .recv()
            .expect("release blocked disconnect listener");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_transition_rejects_retained_push_and_request_capabilities() {
    let runtime = TestRuntime::new("transport-close-outbound-admission");
    let state = Arc::new(ProcessorState::default());
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let registry = Arc::new(SessionRegistry::with_lifecycle_listener(Arc::new(
        BlockingDisconnectListener {
            entered: Mutex::new(Some(entered_tx)),
            release: Mutex::new(release_rx),
        },
    )));
    let processor = TcpProcessor {
        state: Arc::clone(&state),
        admission: None,
    };
    let server = TransportServer::new(loopback_server_config(), runtime.service_context(), processor)
        .with_session_registry(Arc::clone(&registry));
    let (mut client, _address, mut running) = start_server(runtime, server).await;
    establish_session(&mut client).await;
    let session_id = state
        .session
        .lock()
        .expect("outbound admission session lock")
        .expect("outbound admission session id");
    let (push, close) = registry.capabilities(session_id).expect("retained push capability");
    let request = registry
        .server_request_sender(session_id)
        .expect("retained request capability");
    let close_task = tokio::spawn(async move { close.close(SessionCloseReason::Administrative).await });

    tokio::time::timeout(Duration::from_secs(1), entered_rx)
        .await
        .expect("close must reach blocked disconnect listener")
        .expect("blocked disconnect listener entry signal");
    assert!(matches!(
        push.send(
            ServerPushCommand::NotifyConsumerIdsChanged {
                header: NotifyConsumerIdsChangedRequestHeader {
                    consumer_group: CheetahString::from_static_str("GroupA"),
                    rpc_request_header: None,
                },
                opaque: Some(9_860),
            },
            Duration::from_secs(1),
        )
        .await
        .expect("retained push must not fail operationally"),
        ServerPushOutcome::SessionClosed
    ));
    let request_outcome = request
        .request(consumer_status_request(), Duration::from_secs(1))
        .await
        .expect("retained request must not fail operationally");
    assert!(matches!(request_outcome, ServerRequestOutcome::SessionClosed));
    assert_eq!(request.pending_usage().count, 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), client.receive_command())
            .await
            .is_err(),
        "rejected retained capabilities must not enqueue a client frame"
    );

    release_tx.send(()).expect("release blocked disconnect listener");
    let close_outcome = close_task
        .await
        .expect("close task join")
        .expect("ordered close after listener release");
    assert_eq!(close_outcome, SessionCloseOutcome::Closed);
    assert!(client.receive_command().await.is_none());
    running.begin_shutdown();
    running.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_initiator_preserves_the_winning_close_cause_for_later_waiters() {
    let runtime = TestRuntime::new("transport-dropped-close-initiator");
    let state = Arc::new(ProcessorState::default());
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let registry = Arc::new(SessionRegistry::with_lifecycle_listener(Arc::new(
        BlockingPanickingDisconnectListener {
            entered: Mutex::new(Some(entered_tx)),
            release: Mutex::new(release_rx),
        },
    )));
    let processor = TcpProcessor {
        state: Arc::clone(&state),
        admission: None,
    };
    let server = TransportServer::new(loopback_server_config(), runtime.service_context(), processor)
        .with_session_registry(Arc::clone(&registry));
    let (mut client, _address, mut running) = start_server(runtime, server).await;
    establish_session(&mut client).await;
    let session_id = state
        .session
        .lock()
        .expect("dropped initiator session lock")
        .expect("dropped initiator session id");
    let (_, close) = registry
        .capabilities(session_id)
        .expect("dropped initiator close capability");
    let initiator = close.clone();
    let initiating_task = tokio::spawn(async move { initiator.close(SessionCloseReason::Administrative).await });

    tokio::time::timeout(Duration::from_secs(1), entered_rx)
        .await
        .expect("administrative close reaches listener")
        .expect("administrative close entry signal");
    initiating_task.abort();
    assert!(initiating_task
        .await
        .expect_err("initiating future is dropped")
        .is_cancelled());
    release_tx.send(()).expect("release panicking disconnect listener");

    let error = close
        .close(SessionCloseReason::HeartbeatTimeout)
        .await
        .expect_err("shared unhealthy close retains the original winner");
    assert_eq!(error.code(), TRANSPORT_SESSION_FAILED.code());
    assert_transport_operation(&error, "close_administrative");
    let completion = close.completion_snapshot().await;
    assert!(!completion.healthy);
    assert!(completion.disconnected_panicked);
    assert!(client.receive_command().await.is_none());
    running.begin_shutdown();
    running.finish().await;
}

#[tokio::test]
async fn typed_close_waits_for_deferred_cleanup_executor_drain_and_writer_completion() {
    const OPAQUE: i32 = 9_859;

    let runtime = TestRuntime::new("transport-typed-close-coordinator");
    let deferred_registry = DeferredRegistry::<usize>::new();
    let admission_controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let deferred_admission =
        DeferredAdmission::try_configure(&admission_controller, DeferredWaitLimits::new(4, 4 * 1024 * 1024))
            .expect("typed-close deferred admission");
    let session_registry = Arc::new(SessionRegistry::new());
    let (registered_tx, mut registered_rx) = tokio::sync::mpsc::unbounded_channel();
    let processor = NetworkDeferredCleanupProcessor {
        registry: deferred_registry.clone(),
        admission: deferred_admission.clone(),
        registered: registered_tx,
        precommit_opaque: -1,
        release_precommit: Arc::new(tokio::sync::Notify::new()),
    };
    let server = TransportServer::new(loopback_server_config(), runtime.service_context(), processor)
        .with_admission_controller(Arc::clone(&admission_controller))
        .with_session_registry(Arc::clone(&session_registry));
    let (mut client, _address, mut running) = start_server(runtime, server).await;

    client
        .send_command(RemotingCommand::create_remoting_command(705).set_opaque(OPAQUE))
        .await
        .expect("send typed-close deferred request");
    let registered = receive_deferred_registration(&mut registered_rx, OPAQUE).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while !deferred_registry.test_contains(registered.id) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("deferred registration commit deadline");
    let (_, close) = session_registry
        .capabilities(registered.session_id)
        .expect("typed close capability");
    let cloned = close.clone();

    let (first, second) = tokio::join!(
        close.close(SessionCloseReason::Administrative),
        cloned.close(SessionCloseReason::ServiceShutdown),
    );
    let first = first.expect("first typed close completion");
    let second = second.expect("cloned typed close completion");
    assert!(
        matches!(
            (first, second),
            (SessionCloseOutcome::Closed, SessionCloseOutcome::AlreadyClosed)
                | (SessionCloseOutcome::AlreadyClosed, SessionCloseOutcome::Closed)
        ),
        "concurrent closes must share completion while exactly one caller initiates it"
    );
    let completion = close.completion_snapshot().await;
    assert!(completion.healthy);
    assert_eq!(completion.remaining_inline_tasks, 0);
    assert_eq!(completion.remaining_resume_tasks, 0);
    assert_eq!(completion.removed_waiters, 1);
    assert_eq!(completion.cleanup_panicked_targets, 0);
    assert_eq!(completion.remaining_wait_permits, 0);
    assert_eq!(completion.remaining_server_outbound_leases, 0);
    assert!(!completion.disconnected_panicked);
    assert!(completion.writer_healthy);
    assert_eq!(completion.writer_queued_items, 0);
    assert_eq!(completion.writer_queued_bytes, 0);
    assert!(!session_registry.contains(registered.session_id));
    assert_eq!(deferred_registry.test_index_counts(), (0, 0, 0));
    assert_eq!(deferred_registry.test_claim_marker_count(), 0);
    assert_eq!(deferred_admission.snapshot().waiting_count(), 0);
    assert_eq!(deferred_admission.snapshot().retained_bytes(), 0);
    let admission = admission_controller.snapshot();
    assert_eq!(admission.queued.current_count, 0);
    assert_eq!(admission.inflight.current_count, 0);
    assert_eq!(admission.processors.current_count, 0);
    assert!(client.receive_command().await.is_none());

    assert_eq!(
        close
            .close(SessionCloseReason::Administrative)
            .await
            .expect("repeated typed close completion"),
        SessionCloseOutcome::AlreadyClosed
    );
    running.begin_shutdown();
    running.finish().await;
}

#[tokio::test]
async fn cleanup_callback_panic_returns_typed_unhealthy_close_after_writer_completion() {
    let runtime = TestRuntime::new("transport-cleanup-panic-close");
    let state = Arc::new(ProcessorState::default());
    let registry = Arc::new(SessionRegistry::new());
    registry.panic_next_cleanup_for_test();
    let processor = TcpProcessor {
        state: Arc::clone(&state),
        admission: None,
    };
    let server = TransportServer::new(loopback_server_config(), runtime.service_context(), processor)
        .with_session_registry(Arc::clone(&registry));
    let (mut client, _address, mut running) = start_server(runtime, server).await;
    establish_session(&mut client).await;
    let session_id = state
        .session
        .lock()
        .expect("cleanup panic session lock")
        .expect("cleanup panic session id");
    let (_, close) = registry
        .capabilities(session_id)
        .expect("cleanup panic close capability");

    let error = tokio::time::timeout(Duration::from_secs(1), close.close(SessionCloseReason::Administrative))
        .await
        .expect("cleanup panic typed close deadline")
        .expect_err("cleanup panic must produce typed unhealthy close");
    assert_eq!(error.code(), TRANSPORT_SESSION_FAILED.code());
    // The close reason remains available only as the fixed diagnostic operation
    // label; the opaque TransportError intentionally does not expose a session id.
    assert_transport_operation(&error, "close_administrative");
    let completion = close.completion_snapshot().await;
    assert!(!completion.healthy);
    assert_eq!(completion.cleanup_panicked_targets, 1);
    assert_eq!(completion.remaining_inline_tasks, 0);
    assert_eq!(completion.remaining_resume_tasks, 0);
    assert_eq!(completion.remaining_wait_permits, 0);
    assert_eq!(completion.remaining_server_outbound_leases, 0);
    assert!(!completion.disconnected_panicked);
    assert!(completion.writer_healthy);
    assert_eq!(completion.writer_queued_items, 0);
    assert_eq!(completion.writer_queued_bytes, 0);
    assert!(!registry.contains(session_id));
    assert!(client.receive_command().await.is_none());

    running.begin_shutdown();
    running.finish().await;
}

#[tokio::test]
async fn disconnected_panic_cannot_leave_typed_close_waiting_forever() {
    let runtime = TestRuntime::new("transport-disconnected-panic-close");
    let state = Arc::new(ProcessorState::default());
    let registry = Arc::new(SessionRegistry::with_lifecycle_listener(Arc::new(
        PanickingDisconnectListener,
    )));
    let processor = TcpProcessor {
        state: Arc::clone(&state),
        admission: None,
    };
    let server = TransportServer::new(loopback_server_config(), runtime.service_context(), processor)
        .with_session_registry(Arc::clone(&registry));
    let (mut client, _address, mut running) = start_server(runtime, server).await;
    establish_session(&mut client).await;
    let session_id = state
        .session
        .lock()
        .expect("disconnect panic session lock")
        .expect("disconnect panic session id");
    let (_, close) = registry
        .capabilities(session_id)
        .expect("disconnect panic close capability");

    let error = tokio::time::timeout(Duration::from_secs(1), close.close(SessionCloseReason::Administrative))
        .await
        .expect("finalizer guard must bound typed close")
        .expect_err("disconnected panic must publish unhealthy completion");
    assert_eq!(error.code(), TRANSPORT_SESSION_FAILED.code());
    assert_transport_operation(&error, "close_administrative");
    let completion = close.completion_snapshot().await;
    assert!(!completion.healthy);
    assert!(completion.disconnected_panicked);
    assert_eq!(completion.remaining_server_outbound_leases, 0);
    assert!(completion.writer_healthy);
    assert_eq!(completion.writer_queued_items, 0);
    assert_eq!(completion.writer_queued_bytes, 0);
    assert!(!registry.contains(session_id));
    assert!(client.receive_command().await.is_none());

    running.begin_shutdown();
    running.finish().await;
}

#[tokio::test]
async fn registry_lookups_fail_closed_after_close_now_before_disconnect_cleanup() {
    let runtime = TestRuntime::new("transport-close-now-fail-closed");
    let state = Arc::new(ProcessorState::default());
    let registry = Arc::new(SessionRegistry::new());
    let mut events = registry.subscribe();
    let processor = TcpProcessor { state, admission: None };
    let server = TransportServer::new(loopback_server_config(), runtime.service_context(), processor)
        .with_session_registry(Arc::clone(&registry));
    let (mut client, _address, mut running) = start_server(runtime, server).await;
    establish_session(&mut client).await;
    let session_id = next_connected_session(&mut events).await;

    assert!(registry.capabilities(session_id).is_some());
    assert!(registry.server_request_sender(session_id).is_some());
    assert!(registry.close_now(session_id));

    // This current-thread test has not yielded since close_now, so disconnect
    // cleanup cannot yet have removed the physical registration.
    assert!(registry.capabilities(session_id).is_none());
    assert!(registry.server_request_sender(session_id).is_none());

    let eof = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("close-now session EOF deadline");
    assert!(eof.is_none());
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("close-now session unregister deadline")
            .expect("session event stream");
        if matches!(event, SessionEvent::Disconnected(id) if id == session_id) {
            break;
        }
    }
    assert!(!registry.contains(session_id));

    running.begin_shutdown();
    running.finish().await;
}

#[tokio::test]
async fn server_requests_correlate_by_session_owner_and_fail_on_disconnect_and_deadline() {
    let runtime = TestRuntime::new("transport-server-requests");
    let state = Arc::new(ProcessorState::default());
    let registry = Arc::new(SessionRegistry::new());
    let mut events = registry.subscribe();
    let processor = TcpProcessor { state, admission: None };
    let server = TransportServer::new(loopback_server_config(), runtime.service_context(), processor)
        .with_session_registry(Arc::clone(&registry));
    let (mut first_client, address, mut running) = start_server(runtime, server).await;
    establish_session(&mut first_client).await;
    let first_session = next_connected_session(&mut events).await;
    let mut second_client = crate::connection::Connection::new(
        TcpStream::connect(address)
            .await
            .expect("connect second server-request client"),
    );
    establish_session(&mut second_client).await;
    let second_session = next_connected_session(&mut events).await;
    let first_sender = registry
        .server_request_sender(first_session)
        .expect("first typed server-request sender");
    let second_sender = registry
        .server_request_sender(second_session)
        .expect("second typed server-request sender");

    let first_response = first_sender.request(consumer_status_request(), Duration::from_secs(1));
    tokio::pin!(first_response);
    let first_wire = tokio::select! {
        _ = &mut first_response => panic!("response completed before client frame"),
        frame = first_client.receive_command() => frame
            .expect("first client connection")
            .expect("first server-request frame"),
    };
    assert_eq!(
        RequestCode::from(first_wire.code()),
        RequestCode::GetConsumerStatusFromClient
    );

    second_client
        .send_command(
            RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                .set_opaque(first_wire.opaque())
                .set_body(Bytes::from_static(b"wrong-session")),
        )
        .await
        .expect("send same opaque on wrong session");
    tokio::select! {
        biased;
        _ = &mut first_response => panic!("wrong session completed pending request"),
        () = tokio::task::yield_now() => {}
    }
    first_client
        .send_command(
            RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                .set_opaque(first_wire.opaque())
                .set_body(Bytes::from_static(b"first-session")),
        )
        .await
        .expect("send correctly owned response");
    let ServerRequestOutcome::Responded(response) = first_response
        .await
        .expect("correct session request must not fail operationally")
    else {
        panic!("correct session must complete its owned request");
    };
    assert_eq!(response.session_id(), first_session);
    assert_eq!(response.code(), ResponseCode::Success as i32);
    assert_eq!(response.body(), Some(&Bytes::from_static(b"first-session")));

    let disconnected = second_sender.request(consumer_status_request(), Duration::from_secs(1));
    tokio::pin!(disconnected);
    let _second_wire = tokio::select! {
        _ = &mut disconnected => panic!("disconnect request completed before frame"),
        frame = second_client.receive_command() => frame
            .expect("second client connection")
            .expect("second server-request frame"),
    };
    second_client.shutdown().await.expect("disconnect second client");
    let disconnect_result = tokio::time::timeout(Duration::from_secs(1), &mut disconnected)
        .await
        .expect("disconnect must fail pending response promptly");
    let disconnect_error = expect_server_request_error(disconnect_result, "disconnect cannot produce a response");
    assert_eq!(disconnect_error.code(), TRANSPORT_SESSION_FAILED.code());
    assert_transport_operation(&disconnect_error, "request_await_response");

    let interrupted = first_sender.request(consumer_status_request(), Duration::from_secs(5));
    tokio::pin!(interrupted);
    let _interrupted_wire = tokio::select! {
        _ = &mut interrupted => panic!("second pending request completed before frame"),
        frame = first_client.receive_command() => frame
            .expect("second pending client connection")
            .expect("second pending server-request frame"),
    };
    let timed_out = first_sender.request(consumer_status_request(), Duration::from_millis(100));
    tokio::pin!(timed_out);
    let _timeout_wire = tokio::select! {
        _ = &mut timed_out => panic!("timeout request completed before frame"),
        frame = first_client.receive_command() => frame
            .expect("timeout client connection")
            .expect("timeout server-request frame"),
    };
    std::future::poll_fn(|context| match timed_out.as_mut().poll(context) {
        Poll::Pending if registry.capabilities(first_session).is_some() => Poll::Pending,
        Poll::Pending => {
            assert!(registry.server_request_sender(first_session).is_none());
            Poll::Ready(())
        }
        Poll::Ready(_) => panic!("timeout request completed before its fail-closed transition was observed"),
    })
    .await;
    let timeout_error = expect_server_request_error(timed_out.await, "missing response must expire absolute deadline");
    assert_eq!(timeout_error.code(), TRANSPORT_SESSION_FAILED.code());
    assert_transport_operation(&timeout_error, "request_await_response");
    assert!(registry.capabilities(first_session).is_none());
    assert!(registry.server_request_sender(first_session).is_none());

    let eof = tokio::time::timeout(Duration::from_secs(1), first_client.receive_command())
        .await
        .expect("timed-out session EOF deadline");
    assert!(eof.is_none(), "request timeout must close the canonical socket");
    let interrupted_result = tokio::time::timeout(Duration::from_secs(1), &mut interrupted)
        .await
        .expect("same-session pending request must fail promptly");
    let interrupted_error = expect_server_request_error(
        interrupted_result,
        "same-session pending request cannot survive timeout retirement",
    );
    assert_eq!(interrupted_error.code(), TRANSPORT_SESSION_FAILED.code());
    assert_transport_operation(&interrupted_error, "request_await_response");

    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("timed-out session unregister deadline")
            .expect("session event stream");
        if matches!(event, SessionEvent::Disconnected(id) if id == first_session) {
            break;
        }
    }
    assert!(!registry.contains(first_session));
    assert!(registry.server_request_sender(first_session).is_none());
    let retired_outcome = first_sender
        .request(consumer_status_request(), Duration::from_secs(1))
        .await
        .expect("retained sender rejection must not fail operationally");
    assert!(matches!(retired_outcome, ServerRequestOutcome::SessionClosed));

    let pending = first_sender.pending_usage();
    assert_eq!(pending.count, 0);
    assert_eq!(pending.bytes, 0);

    running.begin_shutdown();
    running.finish().await;
}

#[tokio::test]
async fn typed_close_completes_healthily_with_a_written_pending_server_request() {
    let runtime = TestRuntime::new("transport-close-pending-server-request");
    let state = Arc::new(ProcessorState::default());
    let registry = Arc::new(SessionRegistry::new());
    let mut events = registry.subscribe();
    let processor = TcpProcessor { state, admission: None };
    let server = TransportServer::new(loopback_server_config(), runtime.service_context(), processor)
        .with_session_registry(Arc::clone(&registry));
    let (mut client, _address, mut running) = start_server(runtime, server).await;
    establish_session(&mut client).await;
    let session_id = next_connected_session(&mut events).await;
    let (_, close) = registry
        .capabilities(session_id)
        .expect("pending request close capability");
    let sender = registry
        .server_request_sender(session_id)
        .expect("pending request sender");
    let request_task = {
        let sender = sender.clone();
        tokio::spawn(async move { sender.request(consumer_status_request(), Duration::from_secs(5)).await })
    };
    let wire = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("pending server request write deadline")
        .expect("pending server request connection")
        .expect("pending server request frame");
    assert_eq!(RequestCode::from(wire.code()), RequestCode::GetConsumerStatusFromClient);
    assert_eq!(sender.pending_usage().count, 1);

    assert_eq!(
        close
            .close(SessionCloseReason::Administrative)
            .await
            .expect("pending server request must not make ordered close unhealthy"),
        SessionCloseOutcome::Closed
    );
    let request_result = tokio::time::timeout(Duration::from_secs(1), request_task)
        .await
        .expect("pending request close completion deadline")
        .expect("pending request task join");
    let request_error =
        expect_server_request_error(request_result, "session close must fail the pending response wait");
    assert_eq!(request_error.code(), TRANSPORT_SESSION_FAILED.code());
    assert_transport_operation(&request_error, "request_await_response");
    let completion = close.completion_snapshot().await;
    assert!(completion.healthy);
    assert_eq!(completion.remaining_server_outbound_leases, 0);
    assert!(!completion.disconnected_panicked);
    assert!(completion.writer_healthy);
    assert_eq!(completion.writer_queued_items, 0);
    assert_eq!(completion.writer_queued_bytes, 0);
    assert_eq!(sender.pending_usage().count, 0);
    assert!(!registry.contains(session_id));
    assert!(client.receive_command().await.is_none());

    running.begin_shutdown();
    running.finish().await;
}

#[tokio::test]
async fn aborting_a_written_server_request_retires_its_owner_and_rejects_late_response_reuse() {
    let runtime = TestRuntime::new("transport-server-request-cancellation");
    let state = Arc::new(ProcessorState::default());
    let registry = Arc::new(SessionRegistry::new());
    let mut events = registry.subscribe();
    let processor = TcpProcessor { state, admission: None };
    let server = TransportServer::new(loopback_server_config(), runtime.service_context(), processor)
        .with_session_registry(Arc::clone(&registry));
    let (mut client, address, mut running) = start_server(runtime, server).await;
    establish_session(&mut client).await;
    let session_id = next_connected_session(&mut events).await;
    let sender = registry
        .server_request_sender(session_id)
        .expect("typed server-request sender");

    let request = {
        let sender = sender.clone();
        tokio::spawn(async move { sender.request(consumer_status_request(), Duration::from_secs(5)).await })
    };
    let written = client
        .receive_command()
        .await
        .expect("server-request client connection")
        .expect("written server-request frame");

    request.abort();
    match request.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("request caller must be aborted"),
    }
    assert!(registry.capabilities(session_id).is_none());
    assert!(registry.server_request_sender(session_id).is_none());
    assert_eq!(sender.pending_usage().count, 0);

    let retired = sender
        .request(consumer_status_request(), Duration::from_secs(1))
        .await
        .expect("cancelled response owner rejection must not fail operationally");
    assert!(matches!(retired, ServerRequestOutcome::SessionClosed));

    let _ = client
        .send_command(
            RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                .set_opaque(written.opaque())
                .set_body(Bytes::from_static(b"late-response")),
        )
        .await;
    if let Some(Ok(_frame)) = client.receive_command().await {
        panic!("cancelled request owner produced an unexpected frame");
    }
    loop {
        let event = events.recv().await.expect("session event stream");
        if matches!(event, SessionEvent::Disconnected(id) if id == session_id) {
            break;
        }
    }
    assert!(!registry.contains(session_id));
    drop(sender);

    let mut replacement = crate::connection::Connection::new(
        TcpStream::connect(address)
            .await
            .expect("connect replacement server-request client"),
    );
    establish_session(&mut replacement).await;
    let replacement_id = next_connected_session(&mut events).await;
    let replacement_sender = registry
        .server_request_sender(replacement_id)
        .expect("replacement server-request sender");
    let replacement_request = replacement_sender.request(consumer_status_request(), Duration::from_secs(1));
    tokio::pin!(replacement_request);
    let replacement_wire = tokio::select! {
        _ = &mut replacement_request => panic!("late response polluted the replacement owner"),
        frame = replacement.receive_command() => frame
            .expect("replacement client connection")
            .expect("replacement server-request frame"),
    };
    tokio::select! {
        biased;
        _ = &mut replacement_request => panic!("replacement request completed without its owned response"),
        () = tokio::task::yield_now() => {}
    }
    replacement
        .send_command(
            RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                .set_opaque(replacement_wire.opaque())
                .set_body(Bytes::from_static(b"replacement-response")),
        )
        .await
        .expect("send replacement response");
    let ServerRequestOutcome::Responded(response) = replacement_request
        .await
        .expect("replacement owner request must not fail operationally")
    else {
        panic!("replacement owner must correlate its own response");
    };
    assert_eq!(response.session_id(), replacement_id);
    assert_eq!(response.body(), Some(&Bytes::from_static(b"replacement-response")));

    let (_, replacement_close) = registry
        .capabilities(replacement_id)
        .expect("replacement close capability");
    assert_eq!(
        replacement_close
            .close(SessionCloseReason::Administrative)
            .await
            .expect("gracefully close replacement session"),
        SessionCloseOutcome::Closed
    );
    assert!(replacement.receive_command().await.is_none());

    running.begin_shutdown();
    running.finish().await;
}

fn consumer_status_request() -> ServerRequestCommand {
    ServerRequestCommand::GetConsumerStatusFromClient {
        header: GetConsumerStatusRequestHeader::new(
            CheetahString::from_static_str("TopicA"),
            CheetahString::from_static_str("GroupA"),
        ),
    }
}

async fn establish_session(client: &mut crate::connection::Connection) {
    client
        .send_command(RemotingCommand::create_remoting_command(701))
        .await
        .expect("send session establishment request");
    tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("session establishment response deadline")
        .expect("session establishment connection")
        .expect("session establishment response");
}

async fn next_connected_session(events: &mut tokio::sync::broadcast::Receiver<SessionEvent>) -> SessionId {
    loop {
        match tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("session event deadline")
            .expect("session event stream")
        {
            SessionEvent::Connected(session) => return session.id(),
            SessionEvent::Disconnected(_) => {}
        }
    }
}

#[tokio::test]
async fn injected_boundary_conflicts_fail_before_hooks_are_merged() {
    let state = Arc::new(ProcessorState::default());
    let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let dispatcher = Arc::new(AuthorizedCommandDispatcher::new(
        TcpProcessor { state, admission: None },
        Vec::new(),
        loopback_security(),
        admission,
    ));
    let before = Arc::new(AtomicUsize::new(0));
    let after = Arc::new(AtomicUsize::new(0));
    let hook = Arc::new(CountingHook {
        before: Arc::clone(&before),
        after: Arc::clone(&after),
    });
    let security_runtime = TestRuntime::new("transport-conflict");
    let mut server = TransportServer::new_with_authorized_dispatcher(
        loopback_server_config(),
        security_runtime.service_context(),
        Arc::clone(&dispatcher),
    )
    .with_transport_security(loopback_security(), None);
    server.register_rpc_hook(hook);
    let error = expect_start_error(security_runtime, server).await;
    assert_eq!(error.code(), TRANSPORT_START_FAILED.code());
    assert_transport_operation(&error, "start");
    assert!(matches!(
        error.source().and_then(|source| source.downcast_ref::<RocketMQError>()),
        Some(RocketMQError::ConfigInvalidValue { .. })
    ));

    let admission_runtime = TestRuntime::new("transport-admission-conflict");
    let mut admission_conflict = TransportServer::new_with_authorized_dispatcher(
        loopback_server_config(),
        admission_runtime.service_context(),
        Arc::clone(&dispatcher),
    )
    .with_admission_controller(Arc::new(AdmissionController::new(AdmissionLimits::default())));
    admission_conflict.register_rpc_hook(Arc::new(CountingHook {
        before: Arc::clone(&before),
        after: Arc::clone(&after),
    }));
    let error = expect_start_error(admission_runtime, admission_conflict).await;
    assert_eq!(error.code(), TRANSPORT_START_FAILED.code());
    assert_transport_operation(&error, "start");

    let matching_runtime = TestRuntime::new("transport-unpolluted");
    let matching_server = TransportServer::new_with_authorized_dispatcher(
        loopback_server_config(),
        matching_runtime.service_context(),
        dispatcher,
    );
    let (mut client, _address, mut running) = start_server(matching_runtime, matching_server).await;
    client
        .send_command(RemotingCommand::create_remoting_command(701).set_opaque(4_003))
        .await
        .expect("send request through unpolluted dispatcher");
    let _ = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("unpolluted response deadline")
        .expect("unpolluted connection")
        .expect("unpolluted response");
    assert_eq!(before.load(Ordering::SeqCst), 0);
    assert_eq!(after.load(Ordering::SeqCst), 0);
    running.begin_shutdown();
    running.finish().await;
}

#[tokio::test]
async fn dispatcher_injection_immediately_drops_the_automatic_processor_source() {
    let runtime = TestRuntime::new("transport-processor-replacement");
    let automatic_drops = Arc::new(AtomicUsize::new(0));
    let injected_drops = Arc::new(AtomicUsize::new(0));
    let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let dispatcher = Arc::new(AuthorizedCommandDispatcher::new(
        DropTrackedProcessor {
            drops: Arc::clone(&injected_drops),
        },
        Vec::new(),
        loopback_security(),
        admission,
    ));
    let server = TransportServer::new(
        loopback_server_config(),
        runtime.service_context(),
        DropTrackedProcessor {
            drops: Arc::clone(&automatic_drops),
        },
    );

    let server = server.with_authorized_dispatcher(dispatcher);
    assert_eq!(automatic_drops.load(Ordering::SeqCst), 1);
    assert_eq!(injected_drops.load(Ordering::SeqCst), 0);
    drop(server);
    assert_eq!(injected_drops.load(Ordering::SeqCst), 1);
    runtime.finish().await;
}

#[tokio::test]
async fn shutdown_drains_accepted_work_and_flushes_its_writer_before_retirement() {
    let runtime = TestRuntime::new("transport-drain");
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let write_checked = Arc::new(tokio::sync::Notify::new());
    let resume_write = Arc::new(tokio::sync::Notify::new());
    let server = TransportServer::new(
        loopback_server_config(),
        runtime.service_context(),
        DrainingProcessor {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        },
    )
    .with_write_preflight_barrier(crate::write_strategy::WritePreflightBarrier::new(
        Arc::clone(&write_checked),
        Arc::clone(&resume_write),
    ));
    let (mut client, _address, mut running, shutdown_seen_rx) =
        start_server_with_shutdown_observer(runtime, server).await;
    client
        .send_command(RemotingCommand::create_remoting_command(704).set_opaque(4_006))
        .await
        .expect("send response that reaches the writer");
    tokio::time::timeout(Duration::from_secs(1), write_checked.notified())
        .await
        .expect("response writer claims the accepted frame");
    client
        .send_command(RemotingCommand::create_remoting_command(39).set_opaque(4_007))
        .await
        .expect("send accepted work that must drain");
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("second processor starts before shutdown");

    running.begin_shutdown();
    shutdown_seen_rx.await.expect("shutdown future completed");
    release.notify_one();
    resume_write.notify_one();

    let response = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("drained response deadline")
        .expect("draining connection remains open")
        .expect("accepted response is flushed");
    assert_eq!(response.opaque(), 4_006);
    assert_eq!(response.body(), Some(&Bytes::from_static(b"drained-before-retire")));
    running.finish().await;
}

#[tokio::test]
async fn shutdown_drains_a_writer_claimed_deferred_resume_to_one_receipt_and_frame() {
    const OPAQUE: i32 = 4_108;

    let runtime = TestRuntime::new("transport-deferred-writer-drain");
    let registry = DeferredRegistry::<usize>::new();
    let admission_controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let deferred_admission =
        DeferredAdmission::try_configure(&admission_controller, DeferredWaitLimits::new(4, 4 * 1024 * 1024))
            .expect("writer-drain deferred admission");
    let (registered_tx, mut registered_rx) = tokio::sync::mpsc::unbounded_channel();
    let processor = NetworkDeferredCleanupProcessor {
        registry: registry.clone(),
        admission: deferred_admission.clone(),
        registered: registered_tx,
        precommit_opaque: -1,
        release_precommit: Arc::new(tokio::sync::Notify::new()),
    };
    let write_checked = Arc::new(tokio::sync::Notify::new());
    let resume_write = Arc::new(tokio::sync::Notify::new());
    let server = TransportServer::new(loopback_server_config(), runtime.service_context(), processor)
        .with_admission_controller(Arc::clone(&admission_controller))
        .with_write_preflight_barrier(crate::write_strategy::WritePreflightBarrier::new(
            Arc::clone(&write_checked),
            Arc::clone(&resume_write),
        ));
    let (mut client, _address, mut running, shutdown_seen) = start_server_with_shutdown_observer(runtime, server).await;

    client
        .send_command(RemotingCommand::create_remoting_command(705).set_opaque(OPAQUE))
        .await
        .expect("send writer-drain deferred request");
    let registered = receive_deferred_registration(&mut registered_rx, OPAQUE).await;
    let claim = registry
        .claim(registered.id, DeferredWakeReason::MessageArrived)
        .await
        .expect("claim writer-drain deferred request operationally succeeds");
    let DeferredClaimOutcome::Claimed(claim) = claim else {
        panic!("writer-drain claim must transfer the deferred request");
    };
    let resume = claim.resume(
        DeferredResumeRetainedSize::default(),
        move |opaque, reason| async move {
            assert_eq!(opaque, OPAQUE as usize);
            assert_eq!(reason, DeferredWakeReason::MessageArrived);
            Ok(RemotingResponse::bytes(
                RemotingCommand::create_response_command_with_code(ResponseCode::Success),
                Bytes::from_static(b"deferred-writer-drained"),
            )
            .expect("writer-drain deferred remoting response"))
        },
    );
    tokio::pin!(resume);
    tokio::select! {
        biased;
        result = &mut resume => panic!("deferred resume completed before writer barrier: {result:?}"),
        () = write_checked.notified() => {}
    }

    running.begin_shutdown();
    shutdown_seen.await.expect("writer-drain shutdown observed");
    tokio::select! {
        biased;
        result = &mut resume => panic!("deferred resume completed while writer remained blocked: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    resume_write.notify_one();
    assert!(matches!(
        resume
            .await
            .expect("writer-drain deferred response remains operationally healthy"),
        DeferredResumeOutcome::Completed(_)
    ));

    let response = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("writer-drain response deadline")
        .expect("writer-drain connection remains until flush")
        .expect("writer-drain response frame");
    assert_eq!(response.opaque(), OPAQUE);
    assert_eq!(response.body(), Some(&Bytes::from_static(b"deferred-writer-drained")));
    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "shutdown emits no retry frame"
    );

    let snapshot = admission_controller.snapshot();
    assert_eq!(snapshot.queued.current_count, 0);
    assert_eq!(snapshot.queued.current_bytes, 0);
    assert_eq!(snapshot.inflight.current_count, 0);
    assert_eq!(snapshot.inflight.current_bytes, 0);
    assert_eq!(snapshot.processors.current_count, 0);
    assert_eq!(snapshot.processors.current_bytes, 0);
    assert_eq!(registry.test_index_counts(), (0, 0, 0));
    assert_eq!(registry.test_claim_marker_count(), 0);
    assert_eq!(deferred_admission.snapshot().waiting_count(), 0);
    assert_eq!(deferred_admission.snapshot().retained_bytes(), 0);
}

#[tokio::test]
async fn real_tcp_disconnect_cleans_deferred_state_before_drain_and_preserves_other_session() {
    const OTHER_OPAQUE: i32 = 5_100;
    const RUNNING_OPAQUE: i32 = 5_101;
    const HELD_OPAQUE: i32 = 5_102;
    const PRECOMMIT_OPAQUE: i32 = 5_103;

    let runtime = TestRuntime::new("transport-deferred-disconnect");
    let registry = DeferredRegistry::<usize>::new();
    let admission_controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let deferred_admission =
        DeferredAdmission::try_configure(&admission_controller, DeferredWaitLimits::new(16, 16 * 1024 * 1024))
            .expect("network deferred admission");
    let (registered_tx, mut registered_rx) = tokio::sync::mpsc::unbounded_channel();
    let release_precommit = Arc::new(tokio::sync::Notify::new());
    let processor = NetworkDeferredCleanupProcessor {
        registry: registry.clone(),
        admission: deferred_admission.clone(),
        registered: registered_tx,
        precommit_opaque: PRECOMMIT_OPAQUE,
        release_precommit: Arc::clone(&release_precommit),
    };
    let server = TransportServer::new(loopback_server_config(), runtime.service_context(), processor)
        .with_admission_controller(Arc::clone(&admission_controller));
    let (mut first_client, address, mut server_handle) = start_server(runtime, server).await;
    let mut second_client =
        crate::connection::Connection::new(TcpStream::connect(address).await.expect("connect second client"));

    second_client
        .send_command(RemotingCommand::create_remoting_command(705).set_opaque(OTHER_OPAQUE))
        .await
        .expect("send other-session deferred request");
    let other = receive_deferred_registration(&mut registered_rx, OTHER_OPAQUE).await;

    first_client
        .send_command(RemotingCommand::create_remoting_command(705).set_opaque(RUNNING_OPAQUE))
        .await
        .expect("send running deferred request");
    let running = receive_deferred_registration(&mut registered_rx, RUNNING_OPAQUE).await;
    assert_ne!(running.session_id, other.session_id);
    let running_claim = tokio::time::timeout(
        Duration::from_secs(1),
        registry.claim(running.id, DeferredWakeReason::MessageArrived),
    )
    .await
    .expect("running claim deadline")
    .expect("running claim operationally succeeds");
    let DeferredClaimOutcome::Claimed(running_claim) = running_claim else {
        panic!("running claim after commit must transfer the deferred request");
    };

    first_client
        .send_command(RemotingCommand::create_remoting_command(705).set_opaque(HELD_OPAQUE))
        .await
        .expect("send held deferred request");
    let held = receive_deferred_registration(&mut registered_rx, HELD_OPAQUE).await;
    assert_eq!(held.session_id, running.session_id);
    let held_claim = tokio::time::timeout(
        Duration::from_secs(1),
        registry.claim(held.id, DeferredWakeReason::ForcedRefresh),
    )
    .await
    .expect("held claim deadline")
    .expect("held claim operationally succeeds");
    let DeferredClaimOutcome::Claimed(held_claim) = held_claim else {
        panic!("held claim after commit must transfer the deferred request");
    };

    let (resume_started_tx, resume_started_rx) = oneshot::channel();
    let release_resume = Arc::new(tokio::sync::Notify::new());
    let handler_release = Arc::clone(&release_resume);
    let running_resume = running_claim.resume(DeferredResumeRetainedSize::new(0), move |_, _| async move {
        let _ = resume_started_tx.send(());
        handler_release.notified().await;
        Ok(RemotingResponse::bytes(
            RemotingCommand::create_response_command_with_code(ResponseCode::Success),
            Bytes::from_static(b"must-not-be-written"),
        )
        .expect("blocked resume remoting response"))
    });
    tokio::pin!(running_resume);
    tokio::select! {
        biased;
        result = &mut running_resume => panic!("running resume completed before close: {result:?}"),
        started = resume_started_rx => started.expect("running resume starts in session executor"),
    }

    first_client
        .send_command(RemotingCommand::create_remoting_command(705).set_opaque(PRECOMMIT_OPAQUE))
        .await
        .expect("send precommit deferred request");
    let precommit = receive_deferred_registration(&mut registered_rx, PRECOMMIT_OPAQUE).await;
    assert_eq!(precommit.session_id, running.session_id);
    let precommit_ticket = registry.claim(precommit.id, DeferredWakeReason::Timeout);
    tokio::pin!(precommit_ticket);
    tokio::select! {
        biased;
        _ = &mut precommit_ticket => panic!("precommit ticket completed before disconnect"),
        () = tokio::task::yield_now() => {}
    }

    first_client.shutdown().await.expect("half-close first client");
    let ticket_outcome = tokio::time::timeout(Duration::from_secs(1), &mut precommit_ticket)
        .await
        .expect("disconnect cleanup precedes executor drain")
        .expect("disconnect cleanup remains operationally healthy");
    assert!(matches!(ticket_outcome, DeferredClaimOutcome::SessionClosed));
    assert_eq!(registry.test_index_counts(), (1, 1, 1));
    assert_eq!(registry.test_session_member_count(running.session_id), 0);
    assert_eq!(registry.test_session_member_count(other.session_id), 1);
    assert_eq!(registry.test_claim_marker_count(), 0);
    assert!(matches!(
        registry
            .claim(precommit.id, DeferredWakeReason::Timeout)
            .await
            .expect("post-cleanup claim remains operationally healthy"),
        DeferredClaimOutcome::NotFound
    ));

    let held_handler_called = Arc::new(AtomicUsize::new(0));
    let handler_called = Arc::clone(&held_handler_called);
    let held_outcome = held_claim
        .resume(DeferredResumeRetainedSize::new(0), move |_, _| async move {
            handler_called.fetch_add(1, Ordering::SeqCst);
            Ok(
                RemotingResponse::command(RemotingCommand::create_response_command_with_code(
                    ResponseCode::Success,
                ))
                .expect("held remoting response"),
            )
        })
        .await
        .expect("new resume submission remains operationally healthy after begin-close");
    assert_eq!(held_outcome, DeferredResumeOutcome::SessionClosed);
    assert_eq!(held_handler_called.load(Ordering::SeqCst), 0);
    assert_eq!(deferred_admission.snapshot().waiting_count(), 1);

    release_precommit.notify_one();
    release_resume.notify_one();
    let running_outcome = tokio::time::timeout(Duration::from_secs(1), &mut running_resume)
        .await
        .expect("accepted resume drains")
        .expect("accepted resume remains operationally healthy after close");
    assert_eq!(running_outcome, DeferredResumeOutcome::SessionClosed);
    let eof = tokio::time::timeout(Duration::from_secs(1), first_client.receive_command())
        .await
        .expect("first session retires after drain");
    assert!(eof.is_none(), "closed session must not emit a second response frame");
    assert_eq!(admission_controller.snapshot().processors.current_count, 0);
    assert_eq!(deferred_admission.snapshot().waiting_count(), 1);
    assert_eq!(registry.test_index_counts(), (1, 1, 1));

    second_client
        .send_command(RemotingCommand::create_remoting_command(706).set_opaque(5_104))
        .await
        .expect("send request on unaffected session");
    let response = tokio::time::timeout(Duration::from_secs(1), second_client.receive_command())
        .await
        .expect("other-session response deadline")
        .expect("other session remains connected")
        .expect("other-session response frame");
    assert_eq!(response.body(), Some(&Bytes::from_static(b"other-session-live")));

    server_handle.begin_shutdown();
    server_handle.finish().await;
    assert_eq!(registry.test_index_counts(), (0, 0, 0));
    assert_eq!(registry.test_claim_marker_count(), 0);
    assert_eq!(deferred_admission.snapshot().waiting_count(), 0);
    assert_eq!(deferred_admission.snapshot().retained_bytes(), 0);
    let snapshot = admission_controller.snapshot();
    assert_eq!(snapshot.queued.current_count, 0);
    assert_eq!(snapshot.queued.current_bytes, 0);
    assert_eq!(snapshot.inflight.current_count, 0);
    assert_eq!(snapshot.inflight.current_bytes, 0);
    assert_eq!(snapshot.processors.current_count, 0);
    assert_eq!(snapshot.processors.current_bytes, 0);
}

#[tokio::test]
async fn hooks_registered_before_and_after_injection_append_once_to_existing_registry() {
    let runtime = TestRuntime::new("transport-hook-merge");
    let state = Arc::new(ProcessorState::default());
    let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let security = loopback_security();
    let events = Arc::new(Mutex::new(Vec::new()));
    let new_hook = |id: &'static str| {
        Arc::new(OrderedHook {
            id,
            events: Arc::clone(&events),
        }) as Arc<dyn RPCHook>
    };
    let dispatcher = Arc::new(AuthorizedCommandDispatcher::new(
        TcpProcessor {
            state: Arc::clone(&state),
            admission: None,
        },
        vec![new_hook("dispatcher-initial")],
        Arc::clone(&security),
        Arc::clone(&admission),
    ));
    let mut server = TransportServer::new(
        loopback_server_config(),
        runtime.service_context(),
        TcpProcessor { state, admission: None },
    );
    server.register_rpc_hook(new_hook("pre-injection"));
    let mut server = server
        .with_authorized_dispatcher(dispatcher)
        .with_transport_security(Arc::clone(&security), None)
        .with_admission_controller(Arc::clone(&admission));
    server.register_rpc_hook(new_hook("post-injection"));
    let (mut client, _address, mut running) = start_server(runtime, server).await;

    client
        .send_command(RemotingCommand::create_remoting_command(701).set_opaque(4_004))
        .await
        .expect("send hook merge request");
    let _ = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("hook response deadline")
        .expect("hook connection")
        .expect("hook response");
    assert_eq!(
        events.lock().expect("ordered hook event lock").as_slice(),
        [
            ("before", "dispatcher-initial"),
            ("before", "pre-injection"),
            ("before", "post-injection"),
            ("after", "dispatcher-initial"),
            ("after", "pre-injection"),
            ("after", "post-injection"),
        ]
    );

    running.begin_shutdown();
    running.finish().await;
}
