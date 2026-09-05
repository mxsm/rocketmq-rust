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

use super::harness::*;
use crate::dispatch::DeferredAdmission;
use crate::dispatch::DeferredAdmissionAcquireOutcome;
use crate::dispatch::DeferredClaimOutcome;
use crate::dispatch::DeferredCommitErrorKind;
use crate::dispatch::DeferredRegistration;
use crate::dispatch::DeferredRegistryOutcome;
use crate::dispatch::DeferredResponder;
use crate::dispatch::DeferredResponderOutcome;
use crate::dispatch::DeferredResponseOutcome;
use crate::dispatch::DeferredResumeOutcome;
use crate::dispatch::DeferredResumeRetainedSize;
use crate::dispatch::DeferredWaitLimits;
use crate::dispatch::DeferredWakeReason;
use crate::dispatch::OriginalRequestIdentity;
use crate::dispatch::RequestControlView;
use crate::dispatch::RequestMeta;
use crate::dispatch::ResponseCompletionOutcome;
use crate::dispatch::ResponseSink;
use crate::runtime::processor::ResponseObservation;
use crate::runtime::processor::ResponseObservationMode;
use crate::runtime::processor::ResponseObservationOutcome;
use crate::session_view::EmbeddedSessionRecord;
use crate::telemetry::TransportTelemetry;

fn expect_deferred_permit(
    outcome: DeferredAdmissionAcquireOutcome,
    context: &str,
) -> crate::dispatch::DeferredWaitPermit {
    match outcome {
        DeferredAdmissionAcquireOutcome::Acquired(permit) => permit,
        DeferredAdmissionAcquireOutcome::WaiterCapacityExhausted(_) => {
            panic!("{context}: waiter capacity exhausted")
        }
        DeferredAdmissionAcquireOutcome::RetainedByteCapacityExhausted(_) => {
            panic!("{context}: retained-byte capacity exhausted")
        }
        DeferredAdmissionAcquireOutcome::ParentCapacityExhausted(_) => {
            panic!("{context}: parent capacity exhausted")
        }
    }
}

fn expect_registered<R>(outcome: DeferredRegistryOutcome<R>, context: &str) -> DeferredRegistration
where
    R: Send + 'static,
{
    match outcome {
        DeferredRegistryOutcome::Registered(registration) => registration,
        DeferredRegistryOutcome::DuplicateRequest(_) => panic!("{context}: duplicate request"),
        DeferredRegistryOutcome::IdentityExhausted(_) => panic!("{context}: identity exhausted"),
        DeferredRegistryOutcome::ParentCancelled => panic!("{context}: parent cancelled"),
        DeferredRegistryOutcome::SessionClosed => panic!("{context}: session closed"),
        DeferredRegistryOutcome::DeadlineExpired => panic!("{context}: deadline expired"),
        DeferredRegistryOutcome::BuilderRejected { .. } => panic!("{context}: builder rejected"),
        DeferredRegistryOutcome::ContractViolation { .. } => panic!("{context}: contract violation"),
        DeferredRegistryOutcome::OperationalFailure { .. } => panic!("{context}: operational failure"),
    }
}

fn expect_claimed<R>(
    result: Result<DeferredClaimOutcome<R>, crate::error::TransportError>,
    context: &str,
) -> crate::dispatch::ClaimedDeferred<R>
where
    R: Send + 'static,
{
    match result {
        Ok(DeferredClaimOutcome::Claimed(claimed)) => claimed,
        Ok(DeferredClaimOutcome::NotFound) => panic!("{context}: request not found"),
        Ok(DeferredClaimOutcome::AlreadyClaimed) => panic!("{context}: request already claimed"),
        Ok(DeferredClaimOutcome::AlreadyCompleted) => panic!("{context}: request already completed"),
        Ok(DeferredClaimOutcome::ParentCancelled) => panic!("{context}: parent cancelled"),
        Ok(DeferredClaimOutcome::SessionClosed) => panic!("{context}: session closed"),
        Ok(DeferredClaimOutcome::DeadlineExpired) => panic!("{context}: deadline expired"),
        Err(_) => panic!("{context}: operational claim failure"),
    }
}

fn expect_deferred_responder(outcome: DeferredResponderOutcome, context: &str) -> DeferredResponder {
    match outcome {
        DeferredResponderOutcome::Taken(responder) => responder,
        DeferredResponderOutcome::OneWayRequest => panic!("{context}: one-way request"),
        DeferredResponderOutcome::Unavailable => panic!("{context}: responder unavailable"),
        DeferredResponderOutcome::AlreadyTaken => panic!("{context}: responder already taken"),
        DeferredResponderOutcome::OutcomeCompleted => panic!("{context}: outcome already completed"),
    }
}

fn expect_deferred_completed(outcome: DeferredResumeOutcome, context: &str) -> crate::dispatch::ResponseReceipt {
    match outcome {
        DeferredResumeOutcome::Completed(receipt) => receipt,
        DeferredResumeOutcome::Cancelled => panic!("{context}: request cancelled"),
        DeferredResumeOutcome::SessionClosed => panic!("{context}: session closed"),
        DeferredResumeOutcome::AdmissionRejected => panic!("{context}: admission rejected"),
    }
}

#[derive(Clone, Default)]
struct PublicDeferredProcessor {
    completed: Arc<AtomicBool>,
}

#[derive(Clone)]
struct RegistryDeferredProcessor {
    registry: DeferredRegistry<String>,
    admission: DeferredAdmission,
    registered_id: Arc<Mutex<Option<DeferredId>>>,
    observations: Arc<Mutex<Vec<ResponseObservation>>>,
    commit_checkpoint: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
}

#[derive(Clone)]
struct RollbackRegistrationProcessor {
    registration: Arc<Mutex<Option<DeferredRegistration>>>,
    take_current: bool,
}

#[derive(Clone, Copy)]
struct CommitFailureProcessor {
    kind: DeferredCommitErrorKind,
}

impl RequestProcessor for CommitFailureProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        request
            .mark_deferred_response_taken()
            .map_err(|_| RocketMQError::illegal_argument("test deferred response reservation failed"))?;
        Ok(HandlerOutcome::Deferred(
            DeferredRegistration::with_commit_error_for_test(request.original_identity().request_id(), self.kind),
        ))
    }
}

impl RequestProcessor for RollbackRegistrationProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        if self.take_current {
            drop(expect_deferred_responder(
                request.take_deferred_responder(),
                "rollback responder extraction",
            ));
        }
        let registration = self
            .registration
            .lock()
            .expect("real registration lock")
            .take()
            .ok_or_else(|| RocketMQError::illegal_argument("real registration already consumed"))?;
        Ok(HandlerOutcome::Deferred(registration))
    }
}

struct RealRegistrationFixture {
    _runtime: rocketmq_runtime::RuntimeOwner,
    _session: EmbeddedSessionRecord,
    registry: DeferredRegistry<()>,
    admission: DeferredAdmission,
    id: DeferredId,
}

fn real_registration_fixture(name: &'static str, owner: u64) -> (RealRegistrationFixture, DeferredRegistration) {
    let runtime = rocketmq_runtime::RuntimeOwner::plan(rocketmq_runtime::RuntimeConfig::server_default(name))
        .expect("test runtime configuration is valid")
        .build()
        .expect("real registration runtime");
    let parent = runtime.root_context().component(name).task_group().clone();
    let session = EmbeddedSessionRecord::new(owner);
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = DeferredAdmission::try_configure(&controller, DeferredWaitLimits::new(4, 1024 * 1024))
        .expect("real registration admission");
    let sequence = AtomicU64::new(1);
    let original =
        OriginalRequestIdentity::capture(owner, &sequence, &request(false)).expect("real registration identity");
    let control = RequestControlView::from_meta(
        &RequestMeta::new(Instant::now(), None),
        session.view().state().clone(),
        &parent,
    );
    let (sink, _receiver) = ResponseSink::local(control.clone());
    let responder = sink
        .deferred_seed_for_test(TransportTelemetry::noop(), session.view().id(), control)
        .into_responder(original);
    let retained = DeferredRegistry::<()>::try_retained_size(DeferredRetainedSizeParts::new(0))
        .expect("real registration retained size");
    let permit = expect_deferred_permit(admission.try_reserve(retained), "real registration permit");
    let registry = DeferredRegistry::new();
    let registration = expect_registered(
        registry.register(DeferredRequest::new((), DeferredParts::new(responder, permit))),
        "real provisional registration",
    );
    let id = registration.deferred_id();
    (
        RealRegistrationFixture {
            _runtime: runtime,
            _session: session,
            registry,
            admission,
            id,
        },
        registration,
    )
}

impl RequestProcessor for RegistryDeferredProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let responder = expect_deferred_responder(request.take_deferred_responder(), "registry responder extraction");
        let retained = DeferredRegistry::<String>::try_retained_size(DeferredRetainedSizeParts::new(0))
            .map_err(|_| RocketMQError::illegal_argument("deferred responder extraction failed"))?;
        let permit = expect_deferred_permit(self.admission.try_reserve(retained), "dispatcher deferred permit");
        let mut registration = expect_registered(
            self.registry.register(DeferredRequest::new(
                "dispatcher-owned deferred resume".to_owned(),
                DeferredParts::new(responder, permit),
            )),
            "dispatcher deferred registration",
        );
        if let Some(checkpoint) = self.commit_checkpoint.clone() {
            registration.set_commit_checkpoint(move || checkpoint());
        }
        *self.registered_id.lock().expect("registered deferred id lock") = Some(registration.deferred_id());
        Ok(HandlerOutcome::Deferred(registration))
    }

    fn observe_response(&self, observation: ResponseObservation) {
        self.observations
            .lock()
            .expect("deferred observation lock")
            .push(observation);
    }
}

impl RequestProcessor for PublicDeferredProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let request_id = request.original_identity().request_id();
        let original_opaque = request.original_identity().original_opaque();
        let session_id = request.session().id();
        let responder = expect_deferred_responder(request.take_deferred_responder(), "public responder extraction");
        assert_eq!(responder.request_id(), request_id);
        assert_eq!(responder.session_id(), session_id);
        assert!(responder.control().same_lifecycle_view(request.control()));
        let receipt = match responder
            .respond(
                RemotingResponse::bytes(
                    RemotingCommand::create_response_command_with_code(0).set_opaque(original_opaque + 1),
                    Bytes::from_static(b"public-deferred"),
                )
                .expect("deferred remoting response"),
            )
            .await
            .map_err(|_| RocketMQError::illegal_argument("deferred response construction failed"))?
        {
            DeferredResponseOutcome::Completed(receipt) => receipt,
            DeferredResponseOutcome::AlreadyCompleted
            | DeferredResponseOutcome::DeadlineExceeded
            | DeferredResponseOutcome::Cancelled
            | DeferredResponseOutcome::SessionClosed
            | DeferredResponseOutcome::QueueSaturated => {
                return Err(RocketMQError::illegal_argument("deferred response was rejected"));
            }
        };
        assert_eq!(receipt.request_id(), request_id);
        self.completed.store(true, Ordering::SeqCst);
        Ok(HandlerOutcome::Deferred(
            crate::dispatch::DeferredRegistration::for_test(request_id),
        ))
    }
}

#[tokio::test]
async fn public_deferred_responder_uses_the_requests_canonical_network_sink_and_identity() {
    let mut harness = DispatchHarness::new("dispatch-public-deferred").await;
    let processor = PublicDeferredProcessor::default();
    let completed = Arc::clone(&processor.completed);
    let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new(processor, Vec::new()));
    let mut command = request(false);
    command.set_opaque_mut(-704);
    let (session, original) = harness.request_session(&command);

    dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("dispatch public deferred response");
    let response = harness.receive().await;
    harness.drain_requests().await;
    assert_eq!(response.opaque(), original.original_opaque());
    assert_eq!(response.body().map(Bytes::as_ref), Some(&b"public-deferred"[..]));
    assert!(completed.load(Ordering::SeqCst));
    harness.assert_no_response().await;
    harness.shutdown().await;
}

#[tokio::test]
async fn dispatcher_commits_a_real_registry_registration_before_returning_deferred() {
    let mut harness = DispatchHarness::new("dispatch-real-registry-commit").await;
    let admission = DeferredAdmission::try_configure(
        harness.admission_controller.as_ref(),
        DeferredWaitLimits::new(4, 1024 * 1024),
    )
    .expect("configure deferred registry admission");
    let registry = DeferredRegistry::<String>::new();
    let registered_id = Arc::new(Mutex::new(None));
    let observations = Arc::new(Mutex::new(Vec::new()));
    let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new(
        RegistryDeferredProcessor {
            registry: registry.clone(),
            admission: admission.clone(),
            registered_id: Arc::clone(&registered_id),
            observations: Arc::clone(&observations),
            commit_checkpoint: None,
        },
        Vec::new(),
    ));
    let command = request(false);
    let (session, original) = harness.request_session(&command);

    let outcome = dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("dispatch real deferred registration");
    let DispatchOutcome::Accepted(_) = outcome else {
        panic!("deferred request must enter session execution");
    };
    while harness.authorized.operation_context().active_task_count() > 0 {
        tokio::task::yield_now().await;
    }

    let id = registered_id
        .lock()
        .expect("registered deferred id lock")
        .expect("processor should publish deferred id");
    assert!(registry.test_contains(id));
    assert_eq!(registry.test_index_counts(), (1, 1, 1));
    assert_eq!(registry.test_claim_marker_count(), 0);
    assert_eq!(admission.snapshot().waiting_count(), 1);
    assert!(admission.snapshot().retained_bytes() > 0);
    assert!(
        observations.lock().expect("deferred observation lock").is_empty(),
        "durable registration is not a terminal response observation"
    );
    let claimed = expect_claimed(
        registry
            .claim(id, crate::dispatch::DeferredWakeReason::MessageArrived)
            .await,
        "commit should publish an active claim",
    );
    assert_eq!(claimed.resume_data(), "dispatcher-owned deferred resume");
    assert!(!registry.test_contains(id));
    assert_eq!(registry.test_claim_marker_count(), 1);
    assert_eq!(admission.snapshot().waiting_count(), 1);
    harness.assert_no_response().await;

    let handler_admission = admission.clone();
    let handler_controller = Arc::clone(&harness.admission_controller);
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&handler_calls);
    let receipt = expect_deferred_completed(
        claimed
            .resume(
                DeferredResumeRetainedSize::default(),
                move |resume, reason| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(handler_admission.snapshot().waiting_count(), 0);
                    assert_eq!(handler_admission.snapshot().retained_bytes(), 0);
                    let execution = handler_controller.snapshot();
                    assert_eq!(execution.queued.current_count, 0);
                    assert_eq!(execution.queued.current_bytes, 0);
                    assert_eq!(execution.inflight.current_count, 1);
                    assert!(execution.inflight.current_bytes > 0);
                    assert_eq!(execution.processors.current_count, 1);
                    assert!(execution.processors.current_bytes > 0);
                    assert_eq!(resume, "dispatcher-owned deferred resume");
                    assert_eq!(reason, DeferredWakeReason::MessageArrived);
                    RemotingResponse::command(RemotingCommand::create_response_command_with_code(0))
                        .map_err(|_| RocketMQError::illegal_argument("deferred response construction failed"))
                },
            )
            .await
            .expect("resume should use the same session executor and writer"),
        "deferred resume",
    );
    assert_eq!(receipt.request_id(), original.request_id());
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
    {
        let observed = observations.lock().expect("deferred observation lock");
        assert_eq!(observed.len(), 1, "deferred completion is observed exactly once");
        assert_eq!(observed[0].metadata().mode(), ResponseObservationMode::Deferred);
        assert!(matches!(
            observed[0].metadata().outcome(),
            ResponseObservationOutcome::Written(observed_receipt) if observed_receipt == receipt
        ));
    }
    assert_eq!(registry.test_index_counts(), (0, 0, 0));
    assert_eq!(registry.test_claim_marker_count(), 0);
    assert_eq!(admission.snapshot().waiting_count(), 0);
    assert_eq!(admission.snapshot().retained_bytes(), 0);
    harness.drain_requests().await;
    let execution = harness.admission_controller.snapshot();
    assert_eq!(execution.queued.current_count, 0);
    assert_eq!(execution.queued.current_bytes, 0);
    assert_eq!(execution.inflight.current_count, 0);
    assert_eq!(execution.inflight.current_bytes, 0);
    assert_eq!(execution.processors.current_count, 0);
    assert_eq!(execution.processors.current_bytes, 0);
    let response = harness.receive().await;
    assert_eq!(response.opaque(), original.original_opaque());
    let writer = harness.session.writer_snapshot();
    assert_eq!(writer.accepted, 1);
    assert_eq!(writer.queued_items, 0);
    assert_eq!(writer.queued_bytes, 0);
    assert_eq!(writer.completed, 1);
    drop(dispatcher);
    drop(registry);
    assert_eq!(admission.snapshot().waiting_count(), 0);
    harness.shutdown().await;
}

#[tokio::test]
async fn deferred_commit_session_close_is_a_normal_terminal_race_without_recording_registration() {
    let harness = DispatchHarness::new("dispatch-deferred-commit-session-close").await;
    let admission = DeferredAdmission::try_configure(
        harness.admission_controller.as_ref(),
        DeferredWaitLimits::new(4, 1024 * 1024),
    )
    .expect("configure deferred registry admission");
    let registry = DeferredRegistry::<String>::new();
    let registered_id = Arc::new(Mutex::new(None));
    let observations = Arc::new(Mutex::new(Vec::new()));
    let (telemetry, metric_adjustments, registered_events) = TransportTelemetry::with_deferred_metric_capture();
    let commit_session = harness.session.clone();
    let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new_with_telemetry(
        RegistryDeferredProcessor {
            registry: registry.clone(),
            admission: admission.clone(),
            registered_id: Arc::clone(&registered_id),
            observations: Arc::clone(&observations),
            commit_checkpoint: Some(Arc::new(move || commit_session.abort())),
        },
        Vec::new(),
        telemetry,
    ));
    let command = request(false);
    let (session, _original) = harness.request_session(&command);

    let outcome = dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("request admission succeeds before deferred commit");
    assert!(matches!(outcome, DispatchOutcome::Accepted(_)));
    harness.drain_requests().await;

    assert!(dispatcher.reported_failure_categories().is_empty());
    assert_eq!(registered_events.load(Ordering::SeqCst), 0);
    {
        let adjustments = metric_adjustments.lock();
        assert_eq!(adjustments.as_slice(), &[(1, 256), (-1, -256)]);
        assert_eq!(adjustments.iter().map(|(inflight, _)| inflight).sum::<i64>(), 0);
        assert_eq!(adjustments.iter().map(|(_, bytes)| bytes).sum::<i64>(), 0);
    }
    {
        let observed = observations.lock().expect("deferred observation lock");
        assert_eq!(observed.len(), 1, "commit failure publishes one terminal observation");
        assert!(matches!(
            observed[0].metadata().outcome(),
            ResponseObservationOutcome::Cancelled(crate::dispatch::DeferredTerminalReason::SessionClosed)
        ));
    }
    assert_eq!(registry.test_index_counts(), (0, 0, 0));
    assert_eq!(admission.snapshot().waiting_count(), 0);
    assert_eq!(admission.snapshot().retained_bytes(), 0);
    drop(dispatcher);
    drop(registry);
    harness.shutdown().await;
}

#[tokio::test]
async fn deferred_commit_classifies_only_invariants_as_admitted_failures() {
    for (index, (kind, expected_failure)) in [
        (DeferredCommitErrorKind::ParentCancelled, None),
        (DeferredCommitErrorKind::SessionClosed, None),
        (DeferredCommitErrorKind::DeadlineExpired, None),
        (DeferredCommitErrorKind::ResponseState, Some("response_state")),
        (DeferredCommitErrorKind::RegistryInvariant, Some("registry_invariant")),
    ]
    .into_iter()
    .enumerate()
    {
        let name = match index {
            0 => "dispatch-commit-parent-cancelled",
            1 => "dispatch-commit-session-closed",
            2 => "dispatch-commit-deadline-expired",
            3 => "dispatch-commit-response-state",
            4 => "dispatch-commit-registry-invariant",
            _ => unreachable!("the exact commit-kind table has five entries"),
        };
        let mut harness = DispatchHarness::new(name).await;
        let (telemetry, metric_adjustments, registered_events) = TransportTelemetry::with_deferred_metric_capture();
        let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new_with_telemetry(
            CommitFailureProcessor { kind },
            Vec::new(),
            telemetry,
        ));
        let command = request(false);
        let (session, _) = harness.request_session(&command);

        let outcome = dispatcher
            .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
            .await
            .expect("dispatch admission succeeds before the deferred commit");
        assert!(matches!(outcome, DispatchOutcome::Accepted(_)));
        harness.drain_requests().await;

        assert_eq!(registered_events.load(Ordering::SeqCst), 0);
        assert_eq!(
            dispatcher.reported_failure_categories().as_slice(),
            expected_failure.as_slice()
        );
        {
            let adjustments = metric_adjustments.lock();
            assert_eq!(adjustments.as_slice(), &[(1, 256), (-1, -256)]);
        }
        harness.assert_no_response().await;
        drop(dispatcher);
        harness.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn expired_resume_cancels_without_polling_the_handler_or_writing_a_response() {
    let mut harness = DispatchHarness::new("dispatch-deferred-resume-deadline").await;
    let admission = DeferredAdmission::try_configure(
        harness.admission_controller.as_ref(),
        DeferredWaitLimits::new(4, 1024 * 1024),
    )
    .expect("configure deferred registry admission");
    let registry = DeferredRegistry::<String>::new();
    let registered_id = Arc::new(Mutex::new(None));
    let observations = Arc::new(Mutex::new(Vec::new()));
    let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new(
        RegistryDeferredProcessor {
            registry: registry.clone(),
            admission: admission.clone(),
            registered_id: Arc::clone(&registered_id),
            observations: Arc::clone(&observations),
            commit_checkpoint: None,
        },
        Vec::new(),
    ));
    let command = request(false);
    let (session, _original) = harness.request_session(&command);
    dispatcher
        .dispatch(
            &harness.authorized,
            session,
            harness.context(Some(crate::deadline::RequestDeadline::after(Duration::from_secs(5)))),
            command,
            256,
            None,
        )
        .await
        .expect("dispatch deferred deadline request");
    while harness.authorized.operation_context().active_task_count() > 0 {
        tokio::task::yield_now().await;
    }
    let id = registered_id
        .lock()
        .expect("registered deferred id lock")
        .expect("processor should publish deferred id");
    let claimed = expect_claimed(
        registry.claim(id, DeferredWakeReason::Timeout).await,
        "claim before deadline",
    );
    let handler_called = Arc::new(AtomicBool::new(false));
    tokio::time::advance(Duration::from_secs(5)).await;
    let called = Arc::clone(&handler_called);
    let outcome = claimed
        .resume(DeferredResumeRetainedSize::default(), move |_, _| {
            called.store(true, Ordering::SeqCst);
            async move {
                RemotingResponse::command(RemotingCommand::create_response_command_with_code(0))
                    .map_err(|_| RocketMQError::illegal_argument("deferred response construction failed"))
            }
        })
        .await
        .expect("owner deadline is a normal resume outcome");
    assert!(matches!(outcome, DeferredResumeOutcome::Cancelled));
    assert!(!handler_called.load(Ordering::SeqCst));
    assert_eq!(registry.test_index_counts(), (0, 0, 0));
    assert_eq!(registry.test_claim_marker_count(), 0);
    assert_eq!(admission.snapshot().waiting_count(), 0);
    assert_eq!(admission.snapshot().retained_bytes(), 0);
    let execution = harness.admission_controller.snapshot();
    assert_eq!(execution.queued.current_count, 0);
    assert_eq!(execution.queued.current_bytes, 0);
    assert_eq!(execution.inflight.current_count, 0);
    assert_eq!(execution.inflight.current_bytes, 0);
    assert_eq!(execution.processors.current_count, 0);
    assert_eq!(execution.processors.current_bytes, 0);
    harness.assert_no_response().await;
    drop(dispatcher);
    drop(registry);
    harness.shutdown().await;
}

#[tokio::test]
async fn real_registry_guards_rollback_on_unclaimed_wrong_and_oneway_handler_outcomes() {
    for (name, owner, one_way, take_current) in [
        ("dispatch-real-rollback-unclaimed", u64::MAX - 101, false, false),
        ("dispatch-real-rollback-wrong", u64::MAX - 102, false, true),
        ("dispatch-real-rollback-oneway", u64::MAX - 103, true, false),
    ] {
        let mut harness = DispatchHarness::new(name).await;
        let (fixture, registration) = real_registration_fixture(name, owner);
        assert!(fixture.registry.test_contains(fixture.id));
        assert_eq!(fixture.registry.test_index_counts(), (1, 1, 1));
        assert_eq!(fixture.registry.test_claim_marker_count(), 0);
        assert_eq!(fixture.admission.snapshot().waiting_count(), 1);
        assert!(fixture.admission.snapshot().retained_bytes() > 0);
        let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new(
            RollbackRegistrationProcessor {
                registration: Arc::new(Mutex::new(Some(registration))),
                take_current,
            },
            Vec::new(),
        ));
        let command = request(one_way);
        let (session, _) = harness.request_session(&command);

        dispatcher
            .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
            .await
            .expect("submit invalid real deferred outcome");
        harness.drain_requests().await;

        assert_eq!(dispatcher.reported_failure_categories(), ["contract"]);
        assert!(!fixture.registry.test_contains(fixture.id));
        assert_eq!(fixture.registry.test_index_counts(), (0, 0, 0));
        assert_eq!(fixture.registry.test_claim_marker_count(), 0);
        assert_eq!(fixture.admission.snapshot().waiting_count(), 0);
        assert_eq!(fixture.admission.snapshot().retained_bytes(), 0);
        harness.assert_no_response().await;

        drop(dispatcher);
        assert_eq!(fixture.admission.snapshot().waiting_count(), 0);
        assert_eq!(fixture.admission.snapshot().retained_bytes(), 0);
        harness.shutdown().await;
    }
}

#[tokio::test]
async fn one_way_reply_and_structured_rejection_are_consumed_without_write_or_observation() {
    for (name, behavior, expected_processes) in [
        ("dispatch-oneway-reply", Behavior::Reply, 1),
        ("dispatch-oneway-reject", Behavior::Reject, 0),
    ] {
        let mut harness = DispatchHarness::new(name).await;
        let (processor, state) = TestProcessor::new(behavior);
        let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new(processor, Vec::new()));
        let command = request(true);
        let (session, _) = harness.request_session(&command);

        dispatcher
            .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
            .await
            .expect("dispatch one-way request");
        harness.drain_requests().await;
        harness.assert_no_response().await;

        assert_eq!(state.clones.load(Ordering::SeqCst), 1);
        assert_eq!(state.processes.load(Ordering::SeqCst), expected_processes);
        assert!(state.observations.lock().expect("observation lock").is_empty());
        harness.shutdown().await;
    }
}

#[tokio::test]
async fn one_way_processor_error_reports_fixed_category_without_write_observation() {
    let mut harness = DispatchHarness::new("dispatch-oneway-processor-error").await;
    let (processor, state) = TestProcessor::new(Behavior::Error);
    let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new(processor, Vec::new()));
    let command = request(true);
    let (session, _) = harness.request_session(&command);

    dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("submit one-way processor error");
    tokio::time::timeout(Duration::from_secs(2), async {
        while dispatcher.reported_failure_categories().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("one-way processor error reporter");
    harness.drain_requests().await;

    assert_eq!(dispatcher.reported_failure_categories(), ["processor_error"]);
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert!(state.observations.lock().expect("observation lock").is_empty());
    harness.assert_no_response().await;
    harness.shutdown().await;
}

#[tokio::test]
async fn one_way_processor_and_after_hook_errors_report_one_combined_category() {
    let mut harness = DispatchHarness::new("dispatch-oneway-combined-error").await;
    let (processor, state) = TestProcessor::new(Behavior::Error);
    let hook_events = Arc::new(Mutex::new(Vec::new()));
    let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new(
        processor,
        vec![Arc::new(hook(true, false, Arc::clone(&hook_events)))],
    ));
    let command = request(true);
    let (session, _) = harness.request_session(&command);

    dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("submit combined one-way failure");
    tokio::time::timeout(Duration::from_secs(2), async {
        while dispatcher.reported_failure_categories().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("combined one-way failure reporter");
    harness.drain_requests().await;

    assert_eq!(
        dispatcher.reported_failure_categories(),
        ["processor_error_after_hook_error"]
    );
    assert_eq!(
        hook_events.lock().expect("hook event lock").as_slice(),
        ["before", "after"]
    );
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert!(state.observations.lock().expect("observation lock").is_empty());
    harness.assert_no_response().await;
    harness.shutdown().await;
}

#[tokio::test]
async fn queued_deadline_expiry_suppresses_original_oneway_before_binding_or_write() {
    let mut harness = DispatchHarness::new("dispatch-queued-oneway-deadline").await;
    let (processor, state) = TestProcessor::new(Behavior::WaitReply);
    let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new(processor, Vec::new()));

    let first = request(false);
    let (first_session, _) = harness.request_session(&first);
    dispatcher
        .dispatch(
            &harness.authorized,
            first_session,
            harness.context(None),
            first,
            256,
            None,
        )
        .await
        .expect("submit ordered predecessor");
    state.entered.notified().await;

    let deadline = RequestDeadline::after(Duration::from_millis(10));
    let second = request(true);
    let (second_session, _) = harness.request_session(&second);
    dispatcher
        .dispatch(
            &harness.authorized,
            second_session,
            harness.context(Some(deadline)),
            second,
            256,
            None,
        )
        .await
        .expect("queue ordered one-way request");
    tokio::time::sleep(deadline.remaining()).await;
    assert!(deadline.is_expired());

    state.resume.notify_one();
    let response = harness.receive().await;
    assert_eq!(response.code(), 71, "only the ordered predecessor may write");
    harness.drain_requests().await;
    harness.assert_no_response().await;

    assert_eq!(state.clones.load(Ordering::SeqCst), 2);
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert_eq!(state.observations.lock().expect("observation lock").len(), 1);
    harness.shutdown().await;
}

#[tokio::test]
async fn cancellation_during_processor_wait_binds_one_terminal_observer_without_an_extra_clone() {
    let mut harness = DispatchHarness::new("dispatch-cancel-in-process-observation").await;
    let (processor, state) = TestProcessor::new(Behavior::WaitReply);
    let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new(
        TestProcessor::new(Behavior::Reply).0,
        Vec::new(),
    ));
    let command = request(false);
    let (session, original) = harness.request_session(&command);
    let builder = RemotingRequestBuilder::new(
        original,
        Instant::now(),
        harness.context(None),
        RequestLifecycleProvenance::from_network_session(&session),
        command,
    );
    let per_request_processor = processor.clone();
    let remote_address = session.remote_addr();
    let execution = tokio::spawn(async move {
        dispatcher
            .execute_admitted(
                ExplicitProcessor::new(per_request_processor),
                session,
                AdmissionClass::Data,
                original,
                remote_address,
                Instant::now(),
                builder,
            )
            .await
    });
    state.entered.notified().await;
    execution.abort();
    assert!(execution.await.expect_err("cancel admitted task").is_cancelled());
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let notified = state.observed.notified();
            if state
                .terminal_observations
                .lock()
                .expect("terminal observation lock")
                .len()
                == 1
            {
                break;
            }
            notified.await;
        }
    })
    .await
    .expect("cancelled terminal observation");

    {
        let observations = state.terminal_observations.lock().expect("terminal observation lock");
        assert_eq!(observations.len(), 1);
        assert!(matches!(
            observations[0].metadata().outcome(),
            ResponseObservationOutcome::Failed {
                completion: None,
                progress: Some(WriteProgress::NotStarted),
            }
        ));
    }
    assert_eq!(state.clones.load(Ordering::SeqCst), 1);
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);

    harness.assert_no_response().await;
    harness.shutdown().await;
}

#[tokio::test]
async fn direct_expired_oneway_admission_completes_without_reaching_binding() {
    let mut harness = DispatchHarness::new("dispatch-direct-oneway-deadline").await;
    let (processor, state) = TestProcessor::new(Behavior::Reply);
    let dispatcher = TestAuthorizedDispatcherCore::new(TestProcessor::new(Behavior::Reply).0, Vec::new());
    let command = request(true);
    let (session, original) = harness.request_session(&command);
    let builder = RemotingRequestBuilder::new(
        original,
        Instant::now(),
        harness.context(Some(RequestDeadline::after(Duration::ZERO))),
        RequestLifecycleProvenance::from_network_session(&session),
        command,
    );

    dispatcher
        .execute_admitted(
            ExplicitProcessor::new(processor),
            session.clone(),
            AdmissionClass::Data,
            original,
            session.remote_addr(),
            Instant::now(),
            builder,
        )
        .await
        .expect("expired one-way plan must be consumed before binding");

    harness.assert_no_response().await;
    assert_eq!(state.processes.load(Ordering::SeqCst), 0);
    assert!(state.observations.lock().expect("observation lock").is_empty());
    harness.shutdown().await;
}

#[tokio::test]
async fn one_way_deferred_outcome_is_a_policy_error_without_inline_write_or_observation() {
    let mut harness = DispatchHarness::new("dispatch-oneway-deferred-policy").await;
    let (processor, state) = TestProcessor::new(Behavior::Deferred);
    let dispatcher = TestAuthorizedDispatcherCore::new(TestProcessor::new(Behavior::Reply).0, Vec::new());
    let builder_command = request(false);
    let (session, builder_original) = harness.request_session(&builder_command);
    let builder = RemotingRequestBuilder::new(
        builder_original,
        Instant::now(),
        harness.context(None),
        RequestLifecycleProvenance::from_network_session(&session),
        builder_command,
    )
    .reserve_deferred_response();
    // One-way requests cannot reserve deferred response capability.
    // Calling the private admitted seam with a separate immutable one-way
    // identity isolates the post-resolution policy branch defensively.
    let one_way_command = request(true);
    let one_way_original =
        OriginalRequestIdentity::capture(session.session_id(), &AtomicU64::new(41), &one_way_command)
            .expect("capture one-way policy identity");

    let result = dispatcher
        .execute_admitted(
            ExplicitProcessor::new(processor),
            session.clone(),
            AdmissionClass::Data,
            one_way_original,
            session.remote_addr(),
            Instant::now(),
            builder,
        )
        .await;

    assert!(matches!(
        result,
        Err(AuthorizedDispatchError::OneWayOutcome { outcome: "deferred" })
    ));
    harness.assert_no_response().await;
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert!(state.observations.lock().expect("observation lock").is_empty());
    harness.shutdown().await;
}

#[tokio::test]
async fn one_way_no_reply_marker_failure_maps_resolves_and_drops_without_write() {
    let mut harness = DispatchHarness::new("dispatch-oneway-no-reply-policy").await;
    let (processor, state) = TestProcessor::new(Behavior::NoReply);
    let dispatcher = TestAuthorizedDispatcherCore::new(TestProcessor::new(Behavior::Reply).0, Vec::new());
    let mut command = request(true).set_code(39);
    command.set_opaque_mut(811);
    let (session, original) = harness.request_session(&command);
    let builder = RemotingRequestBuilder::new(
        original,
        Instant::now(),
        harness.context(None),
        RequestLifecycleProvenance::from_network_session(&session),
        command,
    );

    dispatcher
        .execute_admitted(
            ExplicitProcessor::new(processor),
            session.clone(),
            AdmissionClass::Control,
            original,
            session.remote_addr(),
            Instant::now(),
            builder,
        )
        .await
        .expect("one-way marker construction error maps to a consumed reply");

    harness.assert_no_response().await;
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert!(state.observations.lock().expect("observation lock").is_empty());
    harness.shutdown().await;
}

#[tokio::test]
async fn queued_deadline_expiry_attempts_one_plan_write_and_observes_not_started() {
    let mut harness = DispatchHarness::new("dispatch-queued-deadline-observation").await;
    let (processor, state) = TestProcessor::new(Behavior::WaitReply);
    let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new(processor, Vec::new()));

    let first = request(false);
    let (first_session, _) = harness.request_session(&first);
    dispatcher
        .dispatch(
            &harness.authorized,
            first_session,
            harness.context(None),
            first,
            256,
            None,
        )
        .await
        .expect("submit ordered predecessor");
    state.entered.notified().await;

    let deadline = RequestDeadline::after(Duration::from_millis(10));
    let second = request(false);
    let (second_session, _) = harness.request_session(&second);
    dispatcher
        .dispatch(
            &harness.authorized,
            second_session,
            harness.context(Some(deadline)),
            second,
            256,
            None,
        )
        .await
        .expect("queue ordered deadline request");
    tokio::time::sleep(deadline.remaining()).await;
    assert!(deadline.is_expired());

    state.resume.notify_one();
    let response = harness.receive().await;
    assert_eq!(response.code(), 71, "only the ordered predecessor may write");
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.observations.lock().expect("observation lock").len() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("queued deadline observation");
    harness.assert_no_response().await;

    {
        let observations = state.observations.lock().expect("observation lock");
        assert_eq!(observations.len(), 2);
        assert!(matches!(
            observations[1].outcome(),
            ResponseWriteOutcome::Failed {
                completion: Some(ResponseCompletionOutcome::DeadlineExpired),
                progress: Some(WriteProgress::NotStarted),
            }
        ));
    }
    assert_eq!(state.clones.load(Ordering::SeqCst), 2);
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    harness.shutdown().await;
}

#[tokio::test]
async fn legal_protocol_no_response_resolves_without_inline_write_or_observation() {
    let mut harness = DispatchHarness::new("dispatch-no-reply").await;
    let (processor, state) = TestProcessor::new(Behavior::NoReply);
    let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new(processor, Vec::new()));
    let mut command = request(false).set_code(39);
    command.set_opaque_mut(811);
    let (session, _) = harness.request_session(&command);

    dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("dispatch protocol no-response");
    harness.drain_requests().await;
    harness.assert_no_response().await;

    assert_eq!(state.clones.load(Ordering::SeqCst), 1);
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert!(state.observations.lock().expect("observation lock").is_empty());
    harness.shutdown().await;
}

#[tokio::test]
async fn deferred_transfer_and_reply_after_defer_resolve_once_without_inline_write() {
    for (name, behavior, expected_error) in [
        ("dispatch-deferred", Behavior::Deferred, false),
        ("dispatch-reply-after-defer", Behavior::ReplyAfterDeferred, true),
    ] {
        let mut harness = DispatchHarness::new(name).await;
        let (processor, state) = TestProcessor::new(behavior);
        let dispatcher = TestAuthorizedDispatcherCore::new(TestProcessor::new(Behavior::Reply).0, Vec::new());
        let command = request(false);
        let (session, original) = harness.request_session(&command);
        let builder = RemotingRequestBuilder::new(
            original,
            Instant::now(),
            harness.context(None),
            RequestLifecycleProvenance::from_network_session(&session),
            command,
        )
        .reserve_deferred_response();

        let result = dispatcher
            .execute_admitted(
                ExplicitProcessor::new(processor),
                session.clone(),
                AdmissionClass::Data,
                original,
                session.remote_addr(),
                Instant::now(),
                builder,
            )
            .await;

        if expected_error {
            assert!(matches!(
                result,
                Err(AuthorizedDispatchError::Contract(
                    crate::contract::TransportContractViolation::ReplyAfterDeferredTaken
                ))
            ));
        } else {
            result.expect("sealed deferred proof should complete without an inline write");
        }
        harness.assert_no_response().await;
        assert_eq!(state.processes.load(Ordering::SeqCst), 1);
        assert!(state.observations.lock().expect("observation lock").is_empty());
        harness.shutdown().await;
    }
}

#[tokio::test]
async fn real_dispatch_reports_handler_contract_failure_with_fixed_category() {
    let mut harness = DispatchHarness::new("dispatch-handler-contract-report").await;
    let (processor, state) = TestProcessor::new(Behavior::UnclaimedDeferred);
    let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new(processor, Vec::new()));
    let command = request(false);
    let (session, _) = harness.request_session(&command);

    dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("submit handler-contract failure");
    tokio::time::timeout(Duration::from_secs(2), async {
        while dispatcher.reported_failure_categories().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("handler-contract reporter");
    harness.drain_requests().await;

    assert_eq!(dispatcher.reported_failure_categories(), ["contract"]);
    assert_eq!(state.clones.load(Ordering::SeqCst), 1);
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert!(state.observations.lock().expect("observation lock").is_empty());
    harness.assert_no_response().await;
    harness.shutdown().await;
}
