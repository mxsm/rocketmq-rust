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

use std::future::Future;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use rocketmq_error::RocketMQResult;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::TaskKind;
use rocketmq_security_api::PeerInfo;
use tokio::sync::Notify;
use tracing::span::Attributes;
use tracing::span::Id;
use tracing::span::Record;
use tracing::Event;
use tracing::Metadata;
use tracing::Subscriber;

use super::execute_work;
use super::DeferredResumeWork;
use super::ResumeStopView;
use super::ResumeWorkImpl;
use crate::admission::AdmissionClass;
use crate::admission::AdmissionController;
use crate::admission::AdmissionLimits;
use crate::admission::AdmissionScope;
use crate::dispatch::AuthenticationState;
use crate::dispatch::ClaimedDeferred;
use crate::dispatch::DeferredAdmission;
use crate::dispatch::DeferredAdmissionAcquireOutcome;
use crate::dispatch::DeferredClaimOutcome;
use crate::dispatch::DeferredParts;
use crate::dispatch::DeferredRegistry;
use crate::dispatch::DeferredRegistryOutcome;
use crate::dispatch::DeferredRequest;
use crate::dispatch::DeferredResumeOutcome;
use crate::dispatch::DeferredResumeSubmitOutcome;
use crate::dispatch::DeferredRetainedSizeParts;
use crate::dispatch::DeferredTerminalReason;
use crate::dispatch::DeferredWaitLimits;
use crate::dispatch::DeferredWakeReason;
use crate::dispatch::OriginalRequestIdentity;
use crate::dispatch::RemotingResponse;
use crate::dispatch::RequestControlView;
use crate::dispatch::RequestMeta;
use crate::dispatch::RequestOrigin;
use crate::dispatch::ResponseSink;
use crate::dispatch::ResponseState;
use crate::dispatch::ResponseStateOutcome;
use crate::dispatch::ResponseTerminalState;
use crate::request_ordering::RequestOrdering;
use crate::session_executor::DeferredResumeExecutor;
use crate::session_executor::SessionExecutor;
use crate::session_view::EmbeddedSessionRecord;
use crate::telemetry::TransportTelemetry;

fn acquired(outcome: DeferredAdmissionAcquireOutcome) -> crate::dispatch::DeferredWaitPermit {
    match outcome {
        DeferredAdmissionAcquireOutcome::Acquired(permit) => permit,
        DeferredAdmissionAcquireOutcome::WaiterCapacityExhausted(_)
        | DeferredAdmissionAcquireOutcome::RetainedByteCapacityExhausted(_)
        | DeferredAdmissionAcquireOutcome::ParentCapacityExhausted(_) => {
            panic!("terminal ownership wait capacity is available")
        }
    }
}

fn registered<R>(outcome: DeferredRegistryOutcome<R>) -> crate::dispatch::DeferredRegistration
where
    R: Send + 'static,
{
    match outcome {
        DeferredRegistryOutcome::Registered(registration) => registration,
        DeferredRegistryOutcome::DuplicateRequest(_)
        | DeferredRegistryOutcome::IdentityExhausted(_)
        | DeferredRegistryOutcome::ParentCancelled
        | DeferredRegistryOutcome::SessionClosed
        | DeferredRegistryOutcome::DeadlineExpired
        | DeferredRegistryOutcome::ContractViolation { .. }
        | DeferredRegistryOutcome::OperationalFailure { .. } => {
            panic!("terminal ownership registration succeeds")
        }
        DeferredRegistryOutcome::BuilderRejected { error, .. } => match error {},
    }
}

async fn claimed<R>(registry: &DeferredRegistry<R>, id: crate::dispatch::DeferredId) -> ClaimedDeferred<R>
where
    R: Send + 'static,
{
    match registry
        .claim(id, DeferredWakeReason::MessageArrived)
        .await
        .expect("terminal ownership claim has no operational failure")
    {
        DeferredClaimOutcome::Claimed(claim) => claim,
        DeferredClaimOutcome::NotFound
        | DeferredClaimOutcome::AlreadyClaimed
        | DeferredClaimOutcome::AlreadyCompleted
        | DeferredClaimOutcome::ParentCancelled
        | DeferredClaimOutcome::SessionClosed
        | DeferredClaimOutcome::DeadlineExpired => panic!("terminal ownership registration is claimable"),
    }
}

struct FixedSpanSubscriber {
    entered: Arc<AtomicUsize>,
}

impl Subscriber for FixedSpanSubscriber {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _attributes: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, _event: &Event<'_>) {}

    fn enter(&self, _span: &Id) {
        self.entered.fetch_add(1, Ordering::AcqRel);
    }

    fn exit(&self, _span: &Id) {}
}

async fn claimed_for_terminal_test(
    sink: ResponseSink,
    session: &EmbeddedSessionRecord,
    control: RequestControlView,
    executor: Option<DeferredResumeExecutor>,
    opaque: i32,
) -> (ClaimedDeferred<()>, DeferredAdmission) {
    let original = OriginalRequestIdentity::capture(
        98_330,
        &AtomicU64::new(1),
        &RemotingCommand::create_remoting_command(39).set_opaque(opaque),
    )
    .expect("terminal ownership identity");
    let mut seed = sink.deferred_seed_for_test(TransportTelemetry::noop(), session.view().id(), control);
    if let Some(executor) = executor {
        seed = seed.with_resume_context(RequestOrdering::Concurrent, AdmissionClass::Data, executor);
    }
    let responder = seed.into_responder(original);
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = DeferredAdmission::try_configure(&controller, DeferredWaitLimits::new(1, 4096))
        .expect("terminal ownership admission");
    let retained = DeferredRegistry::<()>::try_retained_size(DeferredRetainedSizeParts::new(0))
        .expect("terminal ownership retained size");
    let permit = acquired(admission.try_reserve(retained));
    let registry = DeferredRegistry::new();
    let registration = registered(registry.register(DeferredRequest::new((), DeferredParts::new(responder, permit))));
    let id = registration.deferred_id();
    registration.commit().expect("publish terminal ownership registration");
    let claim = claimed(&registry, id).await;
    (claim, admission)
}

fn remoting_response() -> RocketMQResult<RemotingResponse> {
    Ok(
        RemotingResponse::command(RemotingCommand::create_response_command_with_code(0))
            .expect("terminal ownership remoting response"),
    )
}

#[tokio::test]
async fn claimed_resume_handler_reenters_the_original_request_span() {
    let runtime = RuntimeOwner::plan(RuntimeConfig::server_default("deferred-resume-request-span"))
        .expect("test runtime configuration is valid")
        .build()
        .expect("request span runtime");
    let parent = runtime.root_context().component("request-span").task_group().clone();
    let session = EmbeddedSessionRecord::new(98_332);
    let control = RequestControlView::from_meta(
        &RequestMeta::new(Instant::now(), None),
        session.view().state().clone(),
        &parent,
    );
    let original = OriginalRequestIdentity::capture(
        98_332,
        &AtomicU64::new(1),
        &RemotingCommand::create_remoting_command(39).set_opaque(835),
    )
    .expect("request span identity");
    let entered = Arc::new(AtomicUsize::new(0));
    let dispatch = tracing::Dispatch::new(FixedSpanSubscriber {
        entered: Arc::clone(&entered),
    });
    let span = tracing::dispatcher::with_default(&dispatch, || tracing::info_span!("test-request"));
    assert_eq!(span.id().expect("test request span id").into_u64(), 1);
    let observation = TransportTelemetry::noop()
        .begin_observation(
            original,
            Instant::now(),
            &RequestOrigin::Network {
                peer: PeerInfo::new("127.0.0.1:10911".parse().expect("test peer"), false),
            },
            &AuthenticationState::Anonymous,
            None,
            0,
        )
        .with_span_for_test(span);
    observation.bind_response_observer(|_| {});
    let (sink, _receiver) = ResponseSink::local(control.clone());
    let responder = sink
        .deferred_seed_for_test(TransportTelemetry::noop(), session.view().id(), control)
        .with_observation(observation)
        .into_responder(original);
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = DeferredAdmission::try_configure(&controller, DeferredWaitLimits::new(1, 4096))
        .expect("request span admission");
    let retained = DeferredRegistry::<()>::try_retained_size(DeferredRetainedSizeParts::new(0))
        .expect("request span retained size");
    let permit = acquired(admission.try_reserve(retained));
    let registry = DeferredRegistry::new();
    let registration = registered(registry.register(DeferredRequest::new((), DeferredParts::new(responder, permit))));
    let id = registration.deferred_id();
    registration.commit().expect("publish request span registration");
    let claim = claimed(&registry, id).await;
    let parts = claim.into_execution_parts();
    let stop_view = ResumeStopView::from_execution_parts(&parts);
    let work: Box<dyn DeferredResumeWork> = Box::new(ResumeWorkImpl {
        parts: Some(parts),
        handler: Some(move |(), _reason| async move { remoting_response() }),
        stop_view,
    });

    assert!(matches!(work.execute().await, super::ResumeAttempt::Completed(_)));
    assert!(
        entered.load(Ordering::Acquire) > 0,
        "claimed resume work must enter the observation's original request span"
    );
}

struct ReadyRetainedFuture {
    output: Option<RocketMQResult<RemotingResponse>>,
    drops: Arc<AtomicUsize>,
}

impl Future for ReadyRetainedFuture {
    type Output = RocketMQResult<RemotingResponse>;

    fn poll(mut self: Pin<&mut Self>, _context: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        std::task::Poll::Ready(self.output.take().expect("retained future is polled once"))
    }
}

impl Drop for ReadyRetainedFuture {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Copy, Debug)]
enum PublishedTerminalWinner {
    SessionClosed,
    ReceiverDropped,
    ProcessorUnavailable,
    ServiceStopping,
}

impl PublishedTerminalWinner {
    const fn reason(self) -> DeferredTerminalReason {
        match self {
            Self::SessionClosed => DeferredTerminalReason::SessionClosed,
            Self::ReceiverDropped => DeferredTerminalReason::ReceiverDropped,
            Self::ProcessorUnavailable => DeferredTerminalReason::ProcessorUnavailable,
            Self::ServiceStopping => DeferredTerminalReason::ServiceStopping,
        }
    }

    const fn state(self) -> ResponseTerminalState {
        match self {
            Self::SessionClosed | Self::ReceiverDropped => ResponseTerminalState::Closed,
            Self::ProcessorUnavailable | Self::ServiceStopping => ResponseTerminalState::Cancelled,
        }
    }

    const fn resume_outcome(self) -> DeferredResumeOutcome {
        match self {
            Self::SessionClosed | Self::ReceiverDropped => DeferredResumeOutcome::SessionClosed,
            Self::ProcessorUnavailable | Self::ServiceStopping => DeferredResumeOutcome::Cancelled,
        }
    }

    fn publish(self, state: &ResponseState) {
        let outcome = match self {
            Self::SessionClosed => {
                state.close_with_reason(crate::dispatch::deferred_response::DeferredSystemCloseReason::SESSION_CLOSED)
            }
            Self::ReceiverDropped => state.cancel_receiver_dropped(),
            Self::ProcessorUnavailable => state.cancel_with_reason(
                crate::dispatch::deferred_response::DeferredSystemCancellationReason::PROCESSOR_UNAVAILABLE,
            ),
            Self::ServiceStopping => state.cancel_with_reason(
                crate::dispatch::deferred_response::DeferredSystemCancellationReason::SERVICE_STOPPING,
            ),
        };
        assert_eq!(
            outcome,
            Ok(ResponseStateOutcome::Applied(())),
            "{self:?} must publish the sole response terminal winner"
        );
    }
}

struct TerminalWinnerFuture {
    state: Arc<ResponseState>,
    winner: PublishedTerminalWinner,
    output: Option<RocketMQResult<RemotingResponse>>,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl Future for TerminalWinnerFuture {
    type Output = RocketMQResult<RemotingResponse>;

    fn poll(mut self: Pin<&mut Self>, _context: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        assert_eq!(
            this.polls.fetch_add(1, Ordering::AcqRel),
            0,
            "the resumed handler future is polled exactly once"
        );
        this.winner.publish(&this.state);
        std::task::Poll::Ready(this.output.take().expect("terminal winner future is polled once"))
    }
}

impl Drop for TerminalWinnerFuture {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}

#[tokio::test]
async fn completed_handler_future_is_retained_until_canonical_response_handoff_terminal() {
    let runtime = RuntimeOwner::plan(RuntimeConfig::server_default("deferred-resume-handler-owner"))
        .expect("test runtime configuration is valid")
        .build()
        .expect("handler-owner runtime");
    let parent = runtime.root_context().component("handler-owner").task_group().clone();
    let session = EmbeddedSessionRecord::new(98_330);
    let control = RequestControlView::from_meta(
        &RequestMeta::new(Instant::now(), None),
        session.view().state().clone(),
        &parent,
    );
    let checked = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (sink, _receiver, _attempts) =
        ResponseSink::local_with_handoff_gate(control.clone(), Arc::clone(&checked), Arc::clone(&release));
    let original = OriginalRequestIdentity::capture(
        98_330,
        &AtomicU64::new(1),
        &RemotingCommand::create_remoting_command(39).set_opaque(833),
    )
    .expect("handler-owner identity");
    let responder = sink
        .deferred_seed_for_test(TransportTelemetry::noop(), session.view().id(), control)
        .into_responder(original);
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = DeferredAdmission::try_configure(&controller, DeferredWaitLimits::new(1, 4096))
        .expect("handler-owner admission");
    let retained = DeferredRegistry::<()>::try_retained_size(DeferredRetainedSizeParts::new(0))
        .expect("handler-owner retained size");
    let permit = acquired(admission.try_reserve(retained));
    let registry = DeferredRegistry::new();
    let registration = registered(registry.register(DeferredRequest::new((), DeferredParts::new(responder, permit))));
    let id = registration.deferred_id();
    registration.commit().expect("publish handler-owner registration");
    let claim = claimed(&registry, id).await;
    let parts = claim.into_execution_parts();
    let stop_view = ResumeStopView::from_execution_parts(&parts);
    let drops = Arc::new(AtomicUsize::new(0));
    let future_drops = Arc::clone(&drops);
    let execution = tokio::spawn(execute_work(
        parts,
        move |(), _reason| ReadyRetainedFuture {
            output: Some(Ok(RemotingResponse::command(
                RemotingCommand::create_response_command_with_code(0),
            )
            .expect("handler-owner remoting response"))),
            drops: future_drops,
        },
        stop_view,
    ));

    checked.notified().await;
    assert_eq!(
        drops.load(Ordering::Acquire),
        0,
        "a ready handler future still owns affine terminal resources while response delivery is blocked"
    );
    release.notify_one();
    assert!(matches!(
        execution.await.expect("handler-owner execution task"),
        super::ResumeAttempt::Completed(_)
    ));
    assert_eq!(drops.load(Ordering::Acquire), 1);
    assert_eq!(admission.snapshot().waiting_count(), 0);
}

#[tokio::test]
async fn blocked_writer_is_owned_by_session_after_producer_submit_and_observer_runs_once_at_terminal() {
    let runtime = RuntimeOwner::plan(RuntimeConfig::server_default("deferred-submit-terminal-owner"))
        .expect("test runtime configuration is valid")
        .build()
        .expect("submit terminal runtime");
    let component = runtime.root_context().component("submit-terminal-owner");
    let producer_group = component
        .task_group()
        .try_child("producer-submit")
        .expect("producer submit group");
    let controller = AdmissionController::new(AdmissionLimits::default());
    let scope = controller
        .prepare_scope(AdmissionScope::new(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)).with_session(98_331))
        .expect("session executor scope");
    let executor = SessionExecutor::try_new(component.task_group(), scope).expect("session executor");
    let session = EmbeddedSessionRecord::new(98_331);
    let control = RequestControlView::from_meta(
        &RequestMeta::new(Instant::now(), None),
        session.view().state().clone(),
        component.task_group(),
    );
    let checked = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (sink, receiver, attempts) =
        ResponseSink::local_with_handoff_gate(control.clone(), Arc::clone(&checked), Arc::clone(&release));
    let (claim, admission) =
        claimed_for_terminal_test(sink, &session, control, Some(executor.deferred_resume_executor()), 834).await;
    let terminal_calls = Arc::new(AtomicUsize::new(0));
    let terminal_succeeded = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let terminal_signal = Arc::new(Notify::new());
    let calls = Arc::clone(&terminal_calls);
    let succeeded = Arc::clone(&terminal_succeeded);
    let signal = Arc::clone(&terminal_signal);
    let (submitted_tx, submitted_rx) = tokio::sync::oneshot::channel();
    producer_group
        .spawn("submit-one-claim", TaskKind::Worker, async move {
            let submitted = claim.submit(
                crate::dispatch::DeferredResumeRetainedSize::default(),
                |(), _reason| async { remoting_response() },
                move |result| {
                    calls.fetch_add(1, Ordering::AcqRel);
                    succeeded.store(result.is_ok(), Ordering::Release);
                    signal.notify_one();
                },
            );
            let _ = submitted_tx.send(submitted);
        })
        .expect("spawn producer submitter");

    assert_eq!(
        submitted_rx
            .await
            .expect("producer submit result")
            .expect("session accepts claimed execution"),
        DeferredResumeSubmitOutcome::Submitted
    );
    checked.notified().await;
    assert_eq!(terminal_calls.load(Ordering::Acquire), 0);
    let producer_report = producer_group
        .shutdown_until(ShutdownDeadline::after(std::time::Duration::from_millis(50)))
        .await;
    assert!(
        producer_report.is_healthy(),
        "producer owner drains before the blocked writer"
    );
    assert_eq!(terminal_calls.load(Ordering::Acquire), 0);

    release.notify_one();
    terminal_signal.notified().await;
    let _plan = receiver.receive().await.expect("one local response frame");
    assert_eq!(attempts.load(Ordering::Acquire), 1);
    assert_eq!(terminal_calls.load(Ordering::Acquire), 1);
    assert!(terminal_succeeded.load(Ordering::Acquire));
    assert_eq!(admission.snapshot().waiting_count(), 0);
    let session_report = executor
        .drain_until(ShutdownDeadline::after(std::time::Duration::from_secs(1)))
        .await;
    assert!(session_report.is_healthy());
}

#[tokio::test]
async fn submit_observer_is_exactly_once_for_session_and_receiver_close_and_sync_rejection() {
    let runtime = RuntimeOwner::plan(RuntimeConfig::server_default("deferred-submit-terminal-failures"))
        .expect("test runtime configuration is valid")
        .build()
        .expect("submit failure runtime");
    let component = runtime.root_context().component("submit-terminal-failures");
    let controller = AdmissionController::new(AdmissionLimits::default());
    let scope = controller
        .prepare_scope(AdmissionScope::new(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)).with_session(98_332))
        .expect("failure session executor scope");
    let executor = SessionExecutor::try_new(component.task_group(), scope).expect("failure session executor");

    for (opaque, cancel_session) in [(835, true), (836, false)] {
        let session = EmbeddedSessionRecord::new(opaque as u64);
        let control = RequestControlView::from_meta(
            &RequestMeta::new(Instant::now(), None),
            session.view().state().clone(),
            component.task_group(),
        );
        let checked = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (sink, receiver, attempts) =
            ResponseSink::local_with_handoff_gate(control.clone(), Arc::clone(&checked), Arc::clone(&release));
        let mut receiver = Some(receiver);
        let (claim, admission) = claimed_for_terminal_test(
            sink,
            &session,
            control,
            Some(executor.deferred_resume_executor()),
            opaque,
        )
        .await;
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_outcome = Arc::new(AtomicU8::new(0));
        let signal = Arc::new(Notify::new());
        assert_eq!(
            claim
                .submit(
                    crate::dispatch::DeferredResumeRetainedSize::default(),
                    |(), _reason| async { remoting_response() },
                    {
                        let calls = Arc::clone(&calls);
                        let observed_outcome = Arc::clone(&observed_outcome);
                        let signal = Arc::clone(&signal);
                        move |result| {
                            calls.fetch_add(1, Ordering::AcqRel);
                            let outcome = match result {
                                Ok(DeferredResumeOutcome::SessionClosed) => 1,
                                Ok(other) => panic!("unexpected source-free resume outcome: {other:?}"),
                                Err(_) => panic!("known close must not become an operational error"),
                            };
                            observed_outcome.store(outcome, Ordering::Release);
                            signal.notify_one();
                        }
                    },
                )
                .expect("failure case is accepted by the session owner"),
            DeferredResumeSubmitOutcome::Submitted
        );
        checked.notified().await;
        if cancel_session {
            session.close();
        } else {
            drop(receiver.take());
        }
        release.notify_one();
        signal.notified().await;
        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert_eq!(observed_outcome.load(Ordering::Acquire), 1);
        assert!(attempts.load(Ordering::Acquire) <= 1);
        assert_eq!(admission.snapshot().waiting_count(), 0);
        if cancel_session {
            let result = receiver
                .take()
                .expect("cancelled receiver remains owned")
                .receive()
                .await;
            assert!(result.is_err());
        }
    }

    let session = EmbeddedSessionRecord::new(98_337);
    let control = RequestControlView::from_meta(
        &RequestMeta::new(Instant::now(), None),
        session.view().state().clone(),
        component.task_group(),
    );
    let (sink, _receiver) = ResponseSink::local(control.clone());
    let (claim, admission) = claimed_for_terminal_test(sink, &session, control, None, 837).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let outcome = claim
        .submit(
            crate::dispatch::DeferredResumeRetainedSize::default(),
            {
                let handler_calls = Arc::clone(&handler_calls);
                move |(), _reason| async move {
                    handler_calls.fetch_add(1, Ordering::AcqRel);
                    remoting_response()
                }
            },
            {
                let calls = Arc::clone(&calls);
                move |_result| {
                    calls.fetch_add(1, Ordering::AcqRel);
                }
            },
        )
        .expect("missing session executor is a normal lifecycle cancellation");
    assert_eq!(outcome, DeferredResumeSubmitOutcome::Cancelled);
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(handler_calls.load(Ordering::Acquire), 0);
    assert_eq!(admission.snapshot().waiting_count(), 0);

    let session_report = executor
        .drain_until(ShutdownDeadline::after(std::time::Duration::from_secs(1)))
        .await;
    assert!(session_report.is_healthy());
}

#[tokio::test]
async fn terminal_winner_before_resumed_response_handoff_converges_without_transport_error() {
    let runtime = RuntimeOwner::plan(RuntimeConfig::server_default("deferred-resume-terminal-winner"))
        .expect("test runtime configuration is valid")
        .build()
        .expect("terminal winner runtime");
    let component = runtime.root_context().component("terminal-winner");

    for (index, winner) in [
        PublishedTerminalWinner::SessionClosed,
        PublishedTerminalWinner::ReceiverDropped,
        PublishedTerminalWinner::ProcessorUnavailable,
        PublishedTerminalWinner::ServiceStopping,
    ]
    .into_iter()
    .enumerate()
    {
        let session_id = 98_340 + u64::try_from(index).expect("small terminal winner index");
        let controller = AdmissionController::new(AdmissionLimits::default());
        let scope = controller
            .prepare_scope(AdmissionScope::new(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)).with_session(session_id))
            .expect("terminal winner session executor scope");
        let executor = SessionExecutor::try_new(component.task_group(), scope).expect("terminal winner executor");
        let session = EmbeddedSessionRecord::new(session_id);
        let control = RequestControlView::from_meta(
            &RequestMeta::new(Instant::now(), None),
            session.view().state().clone(),
            component.task_group(),
        );
        let checked = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (sink, receiver, attempts) =
            ResponseSink::local_with_handoff_gate(control.clone(), Arc::clone(&checked), Arc::clone(&release));
        // A stored permit makes an accidental handoff complete and fail the exact zero-attempt assertion,
        // instead of leaving this regression test blocked at the handoff gate.
        release.notify_one();
        let (claim, admission) = claimed_for_terminal_test(
            sink,
            &session,
            control,
            Some(executor.deferred_resume_executor()),
            840 + i32::try_from(index).expect("small terminal winner index"),
        )
        .await;
        let state = claim.response_state_for_test();
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let future_polls = Arc::new(AtomicUsize::new(0));
        let future_drops = Arc::new(AtomicUsize::new(0));
        let handler_state = Arc::clone(&state);
        let handler_calls_for_future = Arc::clone(&handler_calls);
        let future_polls_for_handler = Arc::clone(&future_polls);
        let future_drops_for_handler = Arc::clone(&future_drops);

        let result = super::resume_claimed(
            claim,
            crate::dispatch::DeferredResumeRetainedSize::default(),
            move |(), _reason| {
                handler_calls_for_future.fetch_add(1, Ordering::AcqRel);
                TerminalWinnerFuture {
                    state: Arc::clone(&handler_state),
                    winner,
                    output: Some(remoting_response()),
                    polls: Arc::clone(&future_polls_for_handler),
                    drops: Arc::clone(&future_drops_for_handler),
                }
            },
        )
        .await;
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(_) => panic!("{winner:?} is a source-free normal resume outcome, not a transport error"),
        };

        assert_eq!(outcome, winner.resume_outcome());
        assert_eq!(state.terminal_state(), Some(winner.state()));
        assert_eq!(state.terminal_reason(), Some(winner.reason()));
        assert_eq!(handler_calls.load(Ordering::Acquire), 1);
        assert_eq!(future_polls.load(Ordering::Acquire), 1);
        assert_eq!(future_drops.load(Ordering::Acquire), 1);
        assert_eq!(
            attempts.load(Ordering::Acquire),
            0,
            "{winner:?} must win before the canonical local response handoff"
        );
        assert_eq!(admission.snapshot().waiting_count(), 0);
        assert_eq!(admission.snapshot().retained_bytes(), 0);
        drop(receiver);
        assert_eq!(attempts.load(Ordering::Acquire), 0);

        let session_report = executor
            .drain_until(ShutdownDeadline::after(std::time::Duration::from_secs(1)))
            .await;
        assert_eq!(session_report.aborted, 0);
        assert!(session_report.is_healthy());
        let snapshot = controller.snapshot();
        assert_eq!(snapshot.queued.current_count, 0);
        assert_eq!(snapshot.queued.current_bytes, 0);
        assert_eq!(snapshot.inflight.current_count, 0);
        assert_eq!(snapshot.inflight.current_bytes, 0);
        assert_eq!(snapshot.processors.current_count, 0);
        assert_eq!(snapshot.processors.current_bytes, 0);
    }
}
