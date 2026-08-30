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

use std::num::NonZeroUsize;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

use super::harness::loopback_server_config;
use super::harness::start_server;
use super::harness::TestRuntime;
use super::loopback_security;
use super::AdmissionController;
use super::AdmissionLimits;
use super::DeferredAdmission;
use super::DeferredParts;
use super::DeferredRegistry;
use super::DeferredRequest;
use super::DeferredResumeRetainedSize;
use super::DeferredRetainedSizeParts;
use super::DeferredWaitLimits;
use super::DeferredWakeReason;
use super::HandlerOutcome;
use super::RemotingRequest;
use super::RemotingResponse;
use super::RequestProcessor;
use super::TransportServer;
use crate::dispatch::DeferredExpiryMargins;
use crate::dispatch::DeferredTerminalReason;
use crate::telemetry::TransportTelemetry;

#[derive(Clone, Copy)]
struct ExpiryPolicy {
    protocol_after: Duration,
    margins: DeferredExpiryMargins,
}

#[derive(Clone, Copy)]
struct RegistrationObservation {
    id: crate::dispatch::DeferredId,
    scheduled_at: tokio::time::Instant,
}

struct ExpiryProcessorState {
    processes: AtomicUsize,
    resumes: AtomicUsize,
    saw_owner_deadline: AtomicBool,
    committed: tokio::sync::Notify,
}

impl Default for ExpiryProcessorState {
    fn default() -> Self {
        Self {
            processes: AtomicUsize::new(0),
            resumes: AtomicUsize::new(0),
            saw_owner_deadline: AtomicBool::new(false),
            committed: tokio::sync::Notify::new(),
        }
    }
}

#[derive(Clone)]
struct TcpDeferredExpiryProcessor {
    registry: DeferredRegistry<i32>,
    admission: DeferredAdmission,
    policy: ExpiryPolicy,
    state: Arc<ExpiryProcessorState>,
    registrations: tokio::sync::mpsc::UnboundedSender<RegistrationObservation>,
}

impl RequestProcessor for TcpDeferredExpiryProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        if request.command().code() == 12 {
            self.state.committed.notify_one();
            return RemotingResponse::command(RemotingCommand::create_response_command_with_code(
                ResponseCode::Success,
            ))
            .map(HandlerOutcome::Reply)
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()));
        }
        self.state.processes.fetch_add(1, Ordering::SeqCst);
        let owner_deadline = request.control().deadline();
        self.state
            .saw_owner_deadline
            .store(owner_deadline.is_some(), Ordering::SeqCst);
        let responder = request
            .take_deferred_responder()
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        let retained = DeferredRegistry::<i32>::try_retained_size(DeferredRetainedSizeParts::new(0))
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        let permit = self
            .admission
            .try_reserve(retained)
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        let now = tokio::time::Instant::now();
        let protocol_at = now + self.policy.protocol_after;
        let scheduled_at = owner_deadline
            .and_then(|deadline| deadline.instant().checked_sub(self.policy.margins.write()))
            .and_then(|write_cutoff| write_cutoff.checked_sub(self.policy.margins.recovery()))
            .map_or(protocol_at, |owner_cutoff| owner_cutoff.min(protocol_at));
        let parts = DeferredParts::new(responder, permit)
            .try_with_expiry(protocol_at, self.policy.margins)
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        let opaque = request.original_identity().original_opaque();
        let registration = self
            .registry
            .register(DeferredRequest::new(opaque, parts))
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        self.registrations
            .send(RegistrationObservation {
                id: registration.deferred_id(),
                scheduled_at,
            })
            .map_err(|_| RocketMQError::illegal_argument("expiry registration observer closed"))?;
        Ok(HandlerOutcome::Deferred(registration))
    }

    fn request_ordering(
        &self,
        _ingress: crate::dispatch::IngressRequestView<'_>,
    ) -> crate::request_ordering::RequestOrdering {
        crate::request_ordering::RequestOrdering::Ordered(crate::request_ordering::RequestOrderingKey::new(9_819))
    }
}

struct TcpExpiryHarness {
    runtime: TestRuntime,
    admission_controller: Arc<AdmissionController>,
    admission_events: tokio::sync::mpsc::Receiver<crate::admission::AdmissionEvent>,
    registry: DeferredRegistry<i32>,
    admission: DeferredAdmission,
    state: Arc<ExpiryProcessorState>,
    registrations: tokio::sync::mpsc::UnboundedReceiver<RegistrationObservation>,
    terminals: Arc<parking_lot::Mutex<Vec<(&'static str, &'static str)>>>,
    server: TransportServer<TcpDeferredExpiryProcessor>,
}

fn expiry_harness(name: &'static str, policy: ExpiryPolicy) -> TcpExpiryHarness {
    let runtime = TestRuntime::new(name);
    let registry = DeferredRegistry::<i32>::new();
    let (admission_event_tx, admission_events) = tokio::sync::mpsc::channel(64);
    let admission_controller = Arc::new(AdmissionController::with_observer(
        AdmissionLimits::default(),
        admission_event_tx,
    ));
    let admission = DeferredAdmission::try_configure(
        admission_controller.as_ref(),
        DeferredWaitLimits::new(8, 8 * 1024 * 1024),
    )
    .expect("TCP expiry admission");
    let state = Arc::new(ExpiryProcessorState::default());
    let (registered_tx, registrations) = tokio::sync::mpsc::unbounded_channel();
    let processor = TcpDeferredExpiryProcessor {
        registry: registry.clone(),
        admission: admission.clone(),
        policy,
        state: Arc::clone(&state),
        registrations: registered_tx,
    };
    let (telemetry, terminals) = TransportTelemetry::with_deferred_terminal_capture();
    let server = TransportServer::new(loopback_server_config(), runtime.service_context(), processor)
        .with_transport_security(loopback_security(), None)
        .with_admission_controller(Arc::clone(&admission_controller))
        .with_telemetry(telemetry);
    TcpExpiryHarness {
        runtime,
        admission_controller,
        admission_events,
        registry,
        admission,
        state,
        registrations,
        terminals,
        server,
    }
}

async fn send_deferred_request(
    client: &mut crate::connection::Connection,
    registrations: &mut tokio::sync::mpsc::UnboundedReceiver<RegistrationObservation>,
    opaque: i32,
) -> RegistrationObservation {
    client
        .send_command(RemotingCommand::create_remoting_command(11).set_opaque(opaque))
        .await
        .expect("send real TCP deferred request");
    registrations.recv().await.expect("real TCP deferred registration")
}

async fn await_commit_barrier(client: &mut crate::connection::Connection, state: &ExpiryProcessorState, opaque: i32) {
    client
        .send_command(
            RemotingCommand::create_remoting_command(12)
                .set_opaque(opaque)
                .mark_oneway_rpc(),
        )
        .await
        .expect("send ordered commit sentinel");
    state.committed.notified().await;
}

async fn await_inflight_release(events: &mut tokio::sync::mpsc::Receiver<crate::admission::AdmissionEvent>) {
    loop {
        let event = events.recv().await.expect("admission observer remains open");
        if event.resource == crate::admission::AdmissionResource::Inflight
            && event.outcome == crate::admission::AdmissionOutcome::Released
        {
            break;
        }
    }
}

#[tokio::test]
async fn real_tcp_protocol_timeout_sweeps_and_resumes_exactly_once() {
    let mut harness = expiry_harness(
        "transport-protocol-expiry",
        ExpiryPolicy {
            protocol_after: Duration::from_millis(100),
            margins: DeferredExpiryMargins::new(Duration::from_millis(20), Duration::from_millis(20)),
        },
    );
    let registry = harness.registry.clone();
    let admission_controller = Arc::clone(&harness.admission_controller);
    let admission = harness.admission.clone();
    let state = Arc::clone(&harness.state);
    let terminals = Arc::clone(&harness.terminals);
    let (mut client, _address, mut running) = start_server(harness.runtime, harness.server).await;
    let observed = send_deferred_request(&mut client, &mut harness.registrations, 9_001).await;
    await_inflight_release(&mut harness.admission_events).await;
    await_commit_barrier(&mut client, &state, 9_011).await;
    await_inflight_release(&mut harness.admission_events).await;

    let batch =
        registry.sweep_expired_at_for_test(observed.scheduled_at, NonZeroUsize::new(8).expect("non-zero sweep"));
    assert_eq!(batch.stats().long_poll_claims(), 1);
    assert_eq!(batch.stats().owner_expired(), 0);
    let mut claims = batch.into_claims();
    assert_eq!(claims.len(), 1);
    let claim = claims.pop().expect("one protocol timeout claim");
    assert_eq!(claim.deferred_id(), observed.id);
    assert_eq!(claim.reason(), DeferredWakeReason::Timeout);
    let resumed = Arc::clone(&state);
    claim
        .resume(
            DeferredResumeRetainedSize::default(),
            move |opaque, reason| async move {
                resumed.resumes.fetch_add(1, Ordering::SeqCst);
                assert_eq!(reason, DeferredWakeReason::Timeout);
                assert_eq!(opaque, 9_001);
                RemotingResponse::bytes(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success),
                    Bytes::from_static(b"protocol-timeout"),
                )
                .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
            },
        )
        .await
        .expect("protocol timeout resumes through the canonical session executor");

    let response = client
        .receive_command()
        .await
        .expect("protocol timeout connection remains open")
        .expect("protocol timeout response frame");
    assert_eq!(response.opaque(), 9_001);
    assert_eq!(response.body(), Some(&Bytes::from_static(b"protocol-timeout")));
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert_eq!(state.resumes.load(Ordering::SeqCst), 1);
    assert!(!state.saw_owner_deadline.load(Ordering::SeqCst));
    assert_eq!(admission.snapshot().waiting_count(), 0);
    assert_eq!(admission.snapshot().retained_bytes(), 0);
    assert_eq!(registry.test_index_counts(), (0, 0, 0));
    assert_eq!(registry.test_claim_marker_count(), 0);
    let snapshot = admission_controller.snapshot();
    assert_eq!(snapshot.queued.current_count, 0);
    assert_eq!(snapshot.queued.current_bytes, 0);
    assert_eq!(snapshot.inflight.current_count, 0);
    assert_eq!(snapshot.inflight.current_bytes, 0);
    assert_eq!(snapshot.processors.current_count, 0);
    assert_eq!(snapshot.processors.current_bytes, 0);
    assert!(
        terminals.lock().is_empty(),
        "successful delivery has no terminal reason"
    );
    running.begin_shutdown();
    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "one timeout claim must produce exactly one response frame"
    );
}

#[tokio::test]
async fn real_tcp_owner_cutoff_removes_without_resuming_or_writing() {
    let mut harness = expiry_harness(
        "transport-owner-expiry",
        ExpiryPolicy {
            protocol_after: Duration::from_secs(5),
            margins: DeferredExpiryMargins::new(Duration::from_millis(100), Duration::from_millis(100)),
        },
    );
    harness.server = harness.server.with_test_request_deadline(Duration::from_millis(350));
    let registry = harness.registry.clone();
    let admission_controller = Arc::clone(&harness.admission_controller);
    let admission = harness.admission.clone();
    let state = Arc::clone(&harness.state);
    let terminals = Arc::clone(&harness.terminals);
    let (mut client, _address, mut running) = start_server(harness.runtime, harness.server).await;
    let observed = send_deferred_request(&mut client, &mut harness.registrations, 9_002).await;
    await_inflight_release(&mut harness.admission_events).await;
    await_commit_barrier(&mut client, &state, 9_012).await;
    await_inflight_release(&mut harness.admission_events).await;

    let batch =
        registry.sweep_expired_at_for_test(observed.scheduled_at, NonZeroUsize::new(8).expect("non-zero sweep"));
    assert_eq!(batch.stats().long_poll_claims(), 0);
    assert_eq!(batch.stats().owner_expired(), 1);
    assert!(batch.into_claims().is_empty());
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert_eq!(state.resumes.load(Ordering::SeqCst), 0);
    assert!(state.saw_owner_deadline.load(Ordering::SeqCst));
    assert_eq!(admission.snapshot().waiting_count(), 0);
    assert_eq!(admission.snapshot().retained_bytes(), 0);
    assert_eq!(registry.test_index_counts(), (0, 0, 0));
    assert_eq!(registry.test_claim_marker_count(), 0);
    let snapshot = admission_controller.snapshot();
    assert_eq!(snapshot.queued.current_count, 0);
    assert_eq!(snapshot.queued.current_bytes, 0);
    assert_eq!(snapshot.inflight.current_count, 0);
    assert_eq!(snapshot.inflight.current_bytes, 0);
    assert_eq!(snapshot.processors.current_count, 0);
    assert_eq!(snapshot.processors.current_bytes, 0);
    assert_eq!(terminals.lock().as_slice(), [("pull_message", "owner_deadline")]);
    running.begin_shutdown();
    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "owner expiry must not synthesize a response frame"
    );
}

#[tokio::test]
async fn real_tcp_parent_service_shutdown_terminalizes_accepted_resume_without_a_frame() {
    let mut harness = expiry_harness(
        "transport-service-stop-expiry",
        ExpiryPolicy {
            protocol_after: Duration::from_secs(5),
            margins: DeferredExpiryMargins::new(Duration::from_millis(100), Duration::from_millis(100)),
        },
    );
    let registry = harness.registry.clone();
    let admission_controller = Arc::clone(&harness.admission_controller);
    let admission = harness.admission.clone();
    let state = Arc::clone(&harness.state);
    let terminals = Arc::clone(&harness.terminals);
    let (mut client, _address, mut running) = start_server(harness.runtime, harness.server).await;
    let observed = send_deferred_request(&mut client, &mut harness.registrations, 9_003).await;
    let claim = registry
        .claim(observed.id, DeferredWakeReason::ForcedRefresh)
        .await
        .expect("claim committed request before service stop");
    let entered = Arc::new(tokio::sync::Notify::new());
    let never_release = Arc::new(tokio::sync::Notify::new());
    let handler_entered = Arc::clone(&entered);
    let handler_release = Arc::clone(&never_release);
    let resume = claim.resume(DeferredResumeRetainedSize::default(), move |_, _| async move {
        handler_entered.notify_one();
        handler_release.notified().await;
        RemotingResponse::command(RemotingCommand::create_response_command_with_code(
            ResponseCode::Success,
        ))
        .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
    });
    tokio::pin!(resume);
    tokio::select! {
        biased;
        result = &mut resume => panic!("accepted resume completed before service stop: {result:?}"),
        () = entered.notified() => {}
    }

    running.begin_shutdown();
    let error = (&mut resume)
        .await
        .expect_err("service stop cannot write a deferred response");
    assert_eq!(
        error.prior_terminal_reason(),
        Some(DeferredTerminalReason::ParentCancelled),
        "the server task-group cancellation precedes session retirement"
    );
    running.finish().await;
    let frame = client.receive_command().await;
    assert!(frame.is_none(), "service stop must not emit a response frame");
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert_eq!(state.resumes.load(Ordering::SeqCst), 0);
    assert_eq!(admission.snapshot().waiting_count(), 0);
    assert_eq!(admission.snapshot().retained_bytes(), 0);
    assert_eq!(registry.test_index_counts(), (0, 0, 0));
    assert_eq!(registry.test_claim_marker_count(), 0);
    let snapshot = admission_controller.snapshot();
    assert_eq!(snapshot.queued.current_count, 0);
    assert_eq!(snapshot.queued.current_bytes, 0);
    assert_eq!(snapshot.inflight.current_count, 0);
    assert_eq!(snapshot.inflight.current_bytes, 0);
    assert_eq!(snapshot.processors.current_count, 0);
    assert_eq!(snapshot.processors.current_bytes, 0);
    assert_eq!(
        terminals.lock().as_slice(),
        [("pull_message", "parent_cancelled")],
        "one terminal owner records the service-stop cancellation"
    );
}
