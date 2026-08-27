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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Mutex;

use bytes::Bytes;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeContext;
use rocketmq_security_api::IngressDecision;
use rocketmq_security_api::IngressPolicy;
use rocketmq_security_api::LayerEvaluation;
use rocketmq_security_api::SecurityRequestView;
use tokio::net::TcpStream;

use super::*;
use crate::dispatch::bridge_construction_counts;
use crate::dispatch::HandlerOutcome;
use crate::dispatch::ProtocolNoResponseReason;
use crate::dispatch::RemotingRequest;
use crate::dispatch::ResponsePlan;
use crate::runtime::processor_v2::RejectRequestDecision;
use crate::runtime::RPCHook;

#[derive(Default)]
struct ProcessorState {
    clones: AtomicUsize,
    processes: AtomicUsize,
    processor_admission_count: AtomicUsize,
    ordered_entered: tokio::sync::Notify,
    request_sequences: Mutex<Vec<u64>>,
    session: Mutex<Option<crate::session_view::SessionId>>,
}

struct TcpV2Processor {
    state: Arc<ProcessorState>,
    admission: Option<Arc<AdmissionController>>,
}

impl Clone for TcpV2Processor {
    fn clone(&self) -> Self {
        self.state.clones.fetch_add(1, Ordering::SeqCst);
        Self {
            state: Arc::clone(&self.state),
            admission: self.admission.clone(),
        }
    }
}

impl RequestProcessorV2 for TcpV2Processor {
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
                request.protocol_no_response(ProtocolNoResponseReason::CallbackHandled)?,
            ));
        }
        Ok(HandlerOutcome::Reply(
            ResponsePlan::bytes(
                RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_opaque(-9),
                Bytes::from_static(b"v2-tcp"),
            )
            .expect("test response plan"),
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
                ResponsePlan::command(RemotingCommand::create_response_command_with_code(44))
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

impl RequestProcessorV2 for DropTrackedProcessor {
    async fn process(&mut self, _request: &mut RemotingRequest) -> RocketMQResult<HandlerOutcome> {
        Ok(HandlerOutcome::Reply(
            ResponsePlan::command(RemotingCommand::create_response_command_with_code(
                ResponseCode::Success,
            ))
            .expect("drop-tracked response plan"),
        ))
    }
}

#[derive(Clone)]
struct DrainingProcessor {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl RequestProcessorV2 for DrainingProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> RocketMQResult<HandlerOutcome> {
        if request.command().code() == 39 {
            self.started.notify_one();
            self.release.notified().await;
            return Ok(HandlerOutcome::NoReply(
                request.protocol_no_response(ProtocolNoResponseReason::CallbackHandled)?,
            ));
        }
        Ok(HandlerOutcome::Reply(
            ResponsePlan::bytes(
                RemotingCommand::create_response_command_with_code(ResponseCode::Success),
                Bytes::from_static(b"drained-before-retire"),
            )
            .expect("draining response plan"),
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

fn service_context(name: &'static str) -> ChildServiceContext {
    RuntimeContext::from_current(name).service_context("transport-v2-test")
}

fn loopback_security() -> Arc<TransportSecurity> {
    Arc::new(TransportSecurity::development_insecure_loopback(None, None))
}

async fn start_server(
    server: TransportServerV2<TcpV2Processor>,
) -> (
    crate::connection::Connection,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<ShutdownReport, ServerStartError>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind V2 test listener");
    let address = listener.local_addr().expect("V2 test listener address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (startup_tx, startup_rx) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        server
            .try_serve_bound_listener_until_with_startup(
                listener,
                None,
                async {
                    let _ = shutdown_rx.await;
                },
                startup_tx,
            )
            .await
    });
    assert_eq!(
        startup_rx
            .await
            .expect("V2 startup result channel")
            .expect("V2 startup succeeds"),
        address
    );
    let client = crate::connection::Connection::new(TcpStream::connect(address).await.expect("connect V2 client"));
    (client, shutdown_tx, server_task)
}

#[tokio::test]
async fn real_tcp_v2_routes_requests_once_and_drops_unexpected_responses_without_legacy_state() {
    let state = Arc::new(ProcessorState::default());
    let security_calls = Arc::new(AtomicUsize::new(0));
    let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let processor = TcpV2Processor {
        state: Arc::clone(&state),
        admission: Some(Arc::clone(&admission)),
    };
    let server = TransportServerV2::new(
        Arc::new(ServerConfig::default()),
        service_context("transport-v2-tcp"),
        processor,
    )
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
    let (mut client, shutdown_tx, server_task) = start_server(server).await;
    state.clones.store(0, Ordering::SeqCst);

    client
        .send_command(RemotingCommand::create_response_command_with_code(91).set_opaque(4_001))
        .await
        .expect("send unexpected V2 response");
    client
        .send_command(RemotingCommand::create_remoting_command(701).set_opaque(4_002))
        .await
        .expect("send V2 request after unexpected response");
    let response = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("V2 TCP response deadline")
        .expect("V2 TCP client remains connected")
        .expect("V2 TCP response frame");
    assert_eq!(response.opaque(), 4_002);
    assert_eq!(
        response.get_type(),
        rocketmq_protocol::protocol::RemotingCommandType::RESPONSE
    );
    assert_eq!(response.body(), Some(&Bytes::from_static(b"v2-tcp")));
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
    let session = state
        .session
        .lock()
        .expect("session capture lock")
        .expect("V2 session id");
    assert_eq!(bridge_construction_counts(session), (0, 0));

    client
        .send_command(RemotingCommand::create_remoting_command(703).set_opaque(4_003))
        .await
        .expect("send V2 rejection request");
    let rejected = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("V2 rejection deadline")
        .expect("V2 connection after rejection")
        .expect("V2 rejection response");
    assert_eq!((rejected.code(), rejected.opaque()), (44, 4_003));

    client
        .send_command(RemotingCommand::create_remoting_command(39).set_opaque(4_004))
        .await
        .expect("send V2 protocol no-response request");
    tokio::time::timeout(Duration::from_secs(1), state.ordered_entered.notified())
        .await
        .expect("no-response processor enters the ordered section");
    client
        .send_command(RemotingCommand::create_remoting_command(701).set_opaque(4_005))
        .await
        .expect("send sentinel after V2 protocol no-response");
    let no_response_sentinel = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("protocol no-response sentinel deadline")
        .expect("protocol no-response keeps the connection open")
        .expect("protocol no-response sentinel frame");
    assert_eq!(
        (no_response_sentinel.code(), no_response_sentinel.opaque()),
        (ResponseCode::Success.to_i32(), 4_005)
    );
    assert_eq!(no_response_sentinel.body(), Some(&Bytes::from_static(b"v2-tcp")));

    client
        .send_command(
            RemotingCommand::create_remoting_command(701)
                .set_opaque(4_006)
                .mark_oneway_rpc(),
        )
        .await
        .expect("send V2 one-way request");
    tokio::time::timeout(Duration::from_secs(1), state.ordered_entered.notified())
        .await
        .expect("one-way processor enters the ordered section");
    client
        .send_command(RemotingCommand::create_remoting_command(701).set_opaque(4_007))
        .await
        .expect("send sentinel after V2 one-way request");
    let oneway_sentinel = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("one-way sentinel deadline")
        .expect("one-way keeps the connection open")
        .expect("one-way sentinel frame");
    assert_eq!(
        (oneway_sentinel.code(), oneway_sentinel.opaque()),
        (ResponseCode::Success.to_i32(), 4_007)
    );
    assert_eq!(oneway_sentinel.body(), Some(&Bytes::from_static(b"v2-tcp")));

    let _ = shutdown_tx.send(());
    let report = server_task.await.expect("join V2 server").expect("V2 shutdown report");
    assert!(report.is_healthy(), "{}", report.to_json());
    let eof = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("V2 shutdown should publish EOF");
    assert!(eof.is_none(), "V2 shutdown must not leave an extra response frame");
}

#[tokio::test]
async fn injected_boundary_conflicts_fail_before_hooks_are_merged() {
    let state = Arc::new(ProcessorState::default());
    let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
        TcpV2Processor { state, admission: None },
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
    let mut server = TransportServerV2::new_with_authorized_dispatcher(
        Arc::new(ServerConfig::default()),
        service_context("transport-v2-conflict"),
        Arc::clone(&dispatcher),
    )
    .with_transport_security(loopback_security(), None);
    server.register_rpc_hook(hook);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind conflict listener");
    let error = server
        .try_serve_bound_listener_until(listener, None, std::future::pending::<()>())
        .await
        .expect_err("foreign security owner must fail");
    assert!(matches!(error, ServerStartError::Configuration { .. }));

    let mut admission_conflict = TransportServerV2::new_with_authorized_dispatcher(
        Arc::new(ServerConfig::default()),
        service_context("transport-v2-admission-conflict"),
        Arc::clone(&dispatcher),
    )
    .with_admission_controller(Arc::new(AdmissionController::new(AdmissionLimits::default())));
    admission_conflict.register_rpc_hook(Arc::new(CountingHook {
        before: Arc::clone(&before),
        after: Arc::clone(&after),
    }));
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind admission conflict listener");
    let error = admission_conflict
        .try_serve_bound_listener_until(listener, None, std::future::pending::<()>())
        .await
        .expect_err("foreign admission owner must fail");
    assert!(matches!(error, ServerStartError::Configuration { .. }));

    let matching_server = TransportServerV2::new_with_authorized_dispatcher(
        Arc::new(ServerConfig::default()),
        service_context("transport-v2-unpolluted"),
        dispatcher,
    );
    let (mut client, shutdown_tx, server_task) = start_server(matching_server).await;
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
    let _ = shutdown_tx.send(());
    let _ = server_task
        .await
        .expect("join unpolluted server")
        .expect("shutdown report");
}

#[tokio::test]
async fn dispatcher_injection_immediately_drops_the_automatic_processor_source() {
    let automatic_drops = Arc::new(AtomicUsize::new(0));
    let injected_drops = Arc::new(AtomicUsize::new(0));
    let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
        DropTrackedProcessor {
            drops: Arc::clone(&injected_drops),
        },
        Vec::new(),
        loopback_security(),
        admission,
    ));
    let server = TransportServerV2::new(
        Arc::new(ServerConfig::default()),
        service_context("transport-v2-processor-replacement"),
        DropTrackedProcessor {
            drops: Arc::clone(&automatic_drops),
        },
    );

    let server = server.with_authorized_dispatcher(dispatcher);
    assert_eq!(automatic_drops.load(Ordering::SeqCst), 1);
    assert_eq!(injected_drops.load(Ordering::SeqCst), 0);
    drop(server);
    assert_eq!(injected_drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shutdown_drains_accepted_work_and_flushes_its_writer_before_retirement() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind draining V2 listener");
    let address = listener.local_addr().expect("draining V2 listener address");
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let write_checked = Arc::new(tokio::sync::Notify::new());
    let resume_write = Arc::new(tokio::sync::Notify::new());
    let server = TransportServerV2::new(
        Arc::new(ServerConfig::default()),
        service_context("transport-v2-drain"),
        DrainingProcessor {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        },
    )
    .with_write_preflight_barrier(crate::write_strategy::WritePreflightBarrier::new(
        Arc::clone(&write_checked),
        Arc::clone(&resume_write),
    ));
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (shutdown_seen_tx, shutdown_seen_rx) = oneshot::channel::<()>();
    let (startup_tx, startup_rx) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        server
            .try_serve_bound_listener_until_with_startup(
                listener,
                None,
                async {
                    let _ = shutdown_rx.await;
                    let _ = shutdown_seen_tx.send(());
                },
                startup_tx,
            )
            .await
    });
    assert_eq!(
        startup_rx
            .await
            .expect("draining startup channel")
            .expect("draining startup succeeds"),
        address
    );
    let mut client =
        crate::connection::Connection::new(TcpStream::connect(address).await.expect("connect draining V2 client"));
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

    let _ = shutdown_tx.send(());
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
    let report = tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("V2 shutdown awaits drain and writer")
        .expect("join draining V2 server")
        .expect("draining V2 shutdown report");
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test]
async fn hooks_registered_before_and_after_injection_append_once_to_existing_registry() {
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
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
        TcpV2Processor {
            state: Arc::clone(&state),
            admission: None,
        },
        vec![new_hook("dispatcher-initial")],
        Arc::clone(&security),
        Arc::clone(&admission),
    ));
    let mut server = TransportServerV2::new(
        Arc::new(ServerConfig::default()),
        service_context("transport-v2-hook-merge"),
        TcpV2Processor { state, admission: None },
    );
    server.register_rpc_hook(new_hook("pre-injection"));
    let mut server = server
        .with_authorized_dispatcher(dispatcher)
        .with_transport_security(Arc::clone(&security), None)
        .with_admission_controller(Arc::clone(&admission));
    server.register_rpc_hook(new_hook("post-injection"));
    let (mut client, shutdown_tx, server_task) = start_server(server).await;

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

    let _ = shutdown_tx.send(());
    let _ = server_task
        .await
        .expect("join hook server")
        .expect("hook shutdown report");
}
