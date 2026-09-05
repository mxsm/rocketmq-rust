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

use std::alloc::Layout;
use std::error::Error;
use std::mem::size_of;
use std::net::IpAddr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ShutdownDeadline;
use tokio::sync::Notify;

use super::checked_execution_charge;
use super::checked_resume_fixed_sum;
use super::deferred_resume_fixed_bytes;
use super::execution_retained_bytes;
use super::ClaimExecutionParts;
use super::DeferredResponseAttempt;
use super::DeferredResumeEnqueueOutcome;
use super::DeferredResumeJob;
use super::DeferredResumeWork;
use super::ResumeAttempt;
use super::ResumeCompletion;
use super::ResumeJobCell;
use super::ResumeStopView;
use super::WorkFuture;
use crate::admission::AdmissionClass;
use crate::admission::AdmissionController;
use crate::admission::AdmissionLimits;
use crate::admission::AdmissionScope;
use crate::admission::ResourceLimit;
use crate::deadline::RequestDeadline;
use crate::dispatch::DeferredId;
use crate::dispatch::DeferredResumeOutcome;
use crate::dispatch::RequestControlView;
use crate::dispatch::RequestId;
use crate::dispatch::RequestMeta;
use crate::request_ordering::RequestOrdering;
use crate::request_ordering::RequestOrderingKey;
use crate::session_executor::DeferredResumeExecutor;
use crate::session_executor::SessionDispatchAttempt;
use crate::session_executor::SessionExecutor;
use crate::session_view::EmbeddedSessionRecord;

#[path = "deferred_resume/stop.rs"]
mod stop;

#[repr(align(128))]
struct HighAlignHandler([u8; 33]);

#[repr(align(256))]
struct HighAlignFuture([u8; 65]);

struct WorkOracle<R, F>
where
    R: Send + 'static,
{
    _parts: Option<ClaimExecutionParts<R>>,
    _handler: Option<F>,
    _stop_view: ResumeStopView,
}

fn arc_allocation<T>() -> usize {
    let header = Layout::array::<AtomicUsize>(2).expect("Arc header layout");
    let (allocation, _) = header.extend(Layout::new::<T>()).expect("Arc data layout");
    allocation.pad_to_align().size()
}

struct ProbeWork {
    wait_released: Arc<AtomicBool>,
    entered: Arc<Notify>,
    release: Option<Arc<Notify>>,
    executions: Arc<AtomicUsize>,
}

struct StoppedWork {
    stopped: Arc<parking_lot::Mutex<Vec<super::ResumeStop>>>,
}

impl DeferredResumeWork for StoppedWork {
    fn release_wait_permit(&mut self) {}

    fn execute(self: Box<Self>) -> WorkFuture {
        panic!("an operation-owner failure must not execute the resume work")
    }

    fn reject(self: Box<Self>, _error: crate::admission::AdmissionRejection) -> WorkFuture {
        panic!("an operation-owner failure must not reject the resume work")
    }

    fn finish_admission_rejected(self: Box<Self>, _error: crate::admission::AdmissionRejection) -> super::ResumeResult {
        panic!("an operation-owner failure must not become an admission rejection")
    }

    fn finish_stopped(
        self: Box<Self>,
        stop: super::ResumeStop,
        _source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> super::ResumeResult {
        self.stopped.lock().push(stop);
        match stop {
            super::ResumeStop::SessionClosed => ResumeAttempt::SessionClosed,
            super::ResumeStop::ParentCancelled
            | super::ResumeStop::OwnerDeadline
            | super::ResumeStop::ProcessorUnavailable
            | super::ResumeStop::ServiceStopping => ResumeAttempt::Cancelled,
        }
    }
}

impl DeferredResumeWork for ProbeWork {
    fn release_wait_permit(&mut self) {
        self.wait_released.store(true, Ordering::Release);
    }

    fn execute(self: Box<Self>) -> WorkFuture {
        Box::pin(async move {
            assert!(self.wait_released.load(Ordering::Acquire));
            self.executions.fetch_add(1, Ordering::AcqRel);
            self.entered.notify_one();
            if let Some(release) = self.release {
                release.notified().await;
            }
            ResumeAttempt::Operational(super::ResumeOperationalFailure::TaskTerminated)
        })
    }

    fn reject(self: Box<Self>, _error: crate::admission::AdmissionRejection) -> WorkFuture {
        Box::pin(async move {
            assert!(self.wait_released.load(Ordering::Acquire));
            self.entered.notify_one();
            ResumeAttempt::AdmissionRejected
        })
    }

    fn finish_admission_rejected(self: Box<Self>, _error: crate::admission::AdmissionRejection) -> super::ResumeResult {
        assert!(self.wait_released.load(Ordering::Acquire));
        ResumeAttempt::AdmissionRejected
    }

    fn finish_stopped(
        self: Box<Self>,
        stop: super::ResumeStop,
        _source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> super::ResumeResult {
        match stop {
            super::ResumeStop::SessionClosed => ResumeAttempt::SessionClosed,
            super::ResumeStop::ParentCancelled
            | super::ResumeStop::OwnerDeadline
            | super::ResumeStop::ProcessorUnavailable
            | super::ResumeStop::ServiceStopping => ResumeAttempt::Cancelled,
        }
    }
}

fn probe_job(
    retained_bytes: usize,
    ordering: RequestOrdering,
    release: Option<Arc<Notify>>,
) -> (
    DeferredResumeJob,
    Arc<ResumeCompletion>,
    Arc<AtomicBool>,
    Arc<Notify>,
    Arc<AtomicUsize>,
) {
    probe_job_with_stop(retained_bytes, ordering, release, ResumeStopView::never())
}

fn probe_job_with_stop(
    retained_bytes: usize,
    ordering: RequestOrdering,
    release: Option<Arc<Notify>>,
    stop_view: ResumeStopView,
) -> (
    DeferredResumeJob,
    Arc<ResumeCompletion>,
    Arc<AtomicBool>,
    Arc<Notify>,
    Arc<AtomicUsize>,
) {
    let completion = ResumeCompletion::new(
        DeferredId::for_test(9814),
        RequestId::real(9814, 1).expect("test request id"),
        None,
    );
    let wait_released = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(Notify::new());
    let executions = Arc::new(AtomicUsize::new(0));
    let work = ProbeWork {
        wait_released: Arc::clone(&wait_released),
        entered: Arc::clone(&entered),
        release,
        executions: Arc::clone(&executions),
    };
    (
        DeferredResumeJob::new(
            retained_bytes,
            AdmissionClass::Data,
            ordering,
            stop_view,
            Box::new(work),
            Arc::clone(&completion),
        ),
        completion,
        wait_released,
        entered,
        executions,
    )
}

fn executor_with_limits(
    name: &'static str,
    limits: AdmissionLimits,
) -> (RuntimeOwner, AdmissionController, SessionExecutor) {
    let runtime = RuntimeOwner::plan(RuntimeConfig::server_default(name))
        .expect("test runtime configuration is valid")
        .build()
        .expect("resume test runtime");
    let service = runtime.root_context().component(name);
    let controller = AdmissionController::new(limits);
    let scope = controller
        .prepare_scope(AdmissionScope::new(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)).with_session(9814))
        .expect("resume admission scope");
    let executor = SessionExecutor::try_new(service.task_group(), scope).expect("resume session executor");
    (runtime, controller, executor)
}

fn is_submitted(outcome: &DeferredResumeEnqueueOutcome) -> bool {
    matches!(outcome, DeferredResumeEnqueueOutcome::Submitted(_))
}

#[test]
fn response_path_preserves_closed_terminal_winners() {
    for reason in [
        crate::dispatch::DeferredTerminalReason::SessionClosed,
        crate::dispatch::DeferredTerminalReason::ReceiverDropped,
    ] {
        let result = super::map_response(
            DeferredId::for_test(981_400),
            RequestId::real(9814, 400).expect("closed-winner request id"),
            Ok(DeferredResponseAttempt::AlreadyCompleted {
                state: reason.terminal_state(),
                reason: Some(reason),
            }),
        );
        assert!(matches!(result, ResumeAttempt::SessionClosed));
    }
}

#[test]
fn response_path_preserves_source_free_service_terminal_winners() {
    for reason in [
        crate::dispatch::DeferredTerminalReason::ProcessorUnavailable,
        crate::dispatch::DeferredTerminalReason::ServiceStopping,
    ] {
        let result = super::map_response(
            DeferredId::for_test(981_401),
            RequestId::real(9814, 401).expect("service-winner request id"),
            Ok(DeferredResponseAttempt::AlreadyCompleted {
                state: reason.terminal_state(),
                reason: Some(reason),
            }),
        );
        assert!(matches!(result, ResumeAttempt::Cancelled));
    }
}

#[test]
fn execution_charge_counts_each_handler_component_once_and_checks_every_addition() {
    assert_eq!(checked_execution_charge(11, 13, 17, 19, 23), Some(83));
    assert_eq!(checked_execution_charge(usize::MAX, 1, 0, 0, 0), None);
    assert_eq!(checked_execution_charge(usize::MAX - 1, 1, 1, 0, 0), None);
    assert_eq!(checked_execution_charge(usize::MAX - 2, 1, 1, 1, 0), None);
    assert_eq!(checked_execution_charge(usize::MAX - 3, 1, 1, 1, 1), None);
}

#[test]
fn fixed_resume_metadata_checks_completion_and_job_cell_additions() {
    assert_eq!(checked_resume_fixed_sum(11, 13), Some(24));
    assert_eq!(checked_resume_fixed_sum(usize::MAX, 1), None);
}

#[test]
fn high_alignment_execution_storage_matches_an_independent_layout_oracle() {
    let original = 29usize;
    let dynamic = 31usize;
    let work = Layout::new::<WorkOracle<u128, HighAlignHandler>>()
        .pad_to_align()
        .size();
    let expected = original
        .checked_add(size_of::<HighAlignHandler>())
        .and_then(|bytes| bytes.checked_add(size_of::<HighAlignFuture>()))
        .and_then(|bytes| bytes.checked_add(dynamic))
        .and_then(|bytes| bytes.checked_add(work));
    assert_eq!(
        execution_retained_bytes::<u128, HighAlignHandler, HighAlignFuture>(original, dynamic),
        expected
    );
    assert_eq!(
        deferred_resume_fixed_bytes(),
        arc_allocation::<ResumeCompletion>().checked_add(arc_allocation::<ResumeJobCell>())
    );
    let _ = HighAlignHandler([0; 33]).0;
    let _ = HighAlignFuture([0; 65]).0;
}

#[tokio::test]
async fn real_queued_admission_rejects_the_checked_high_alignment_charge_and_returns_the_exact_job() {
    let charge = execution_retained_bytes::<u128, HighAlignHandler, HighAlignFuture>(29, 31)
        .expect("checked high-alignment charge");
    let defaults = AdmissionLimits::default();
    let control_reserve = ResourceLimit { count: 1, bytes: 64 };
    let limits = AdmissionLimits {
        queued: ResourceLimit {
            bytes: charge + control_reserve.bytes - 1,
            ..defaults.queued
        },
        control_reserve,
        ..defaults
    };
    let (_runtime, controller, executor) = executor_with_limits("deferred-resume-queued-reject", limits);
    let (job, completion, wait_released, _entered, _executions) = probe_job(charge, RequestOrdering::Concurrent, None);
    let cell = Arc::new(ResumeJobCell::new(job));
    cell.release_wait_permit();
    assert!(wait_released.load(Ordering::Acquire));
    let returned = match executor
        .deferred_resume_executor()
        .try_execute_resume(Arc::clone(&cell))
    {
        DeferredResumeEnqueueOutcome::AdmissionRejected { cell, .. } => cell,
        DeferredResumeEnqueueOutcome::ExecutorClosing { .. } => panic!("executor unexpectedly closed"),
        DeferredResumeEnqueueOutcome::OperationalFailure { .. } => {
            panic!("executor unexpectedly returned an operational failure")
        }
        DeferredResumeEnqueueOutcome::Submitted(_) => {
            panic!("under-capacity queued budget accepted the resume job")
        }
    };
    assert!(Arc::ptr_eq(&cell, &returned));
    drop(returned);
    drop(cell);
    assert_eq!(
        completion.wait().await.expect("unexecuted job terminalizes normally"),
        DeferredResumeOutcome::Cancelled
    );
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.queued.current_count, 0);
    assert_eq!(snapshot.queued.current_bytes, 0);
    assert_eq!(snapshot.queued.rejected_count, 1);
    assert_eq!(snapshot.inflight.current_count, 0);
    assert_eq!(snapshot.inflight.current_bytes, 0);
    assert_eq!(snapshot.inflight.rejected_count, 0);
    assert_eq!(snapshot.processors.current_count, 0);
    assert_eq!(snapshot.processors.current_bytes, 0);
    assert_eq!(snapshot.processors.rejected_count, 0);
}

#[tokio::test]
async fn detached_submit_admission_failure_terminalizes_and_observes_exactly_once() {
    let charge = 512usize;
    let defaults = AdmissionLimits::default();
    let control_reserve = ResourceLimit { count: 1, bytes: 64 };
    let limits = AdmissionLimits {
        queued: ResourceLimit {
            bytes: charge + control_reserve.bytes - 1,
            ..defaults.queued
        },
        control_reserve,
        ..defaults
    };
    let (_runtime, controller, executor) = executor_with_limits("deferred-submit-admission-reject", limits);
    let observed = Arc::new(AtomicUsize::new(0));
    let observed_admission = Arc::new(AtomicBool::new(false));
    let calls = Arc::clone(&observed);
    let admission = Arc::clone(&observed_admission);
    let completion = ResumeCompletion::new(
        DeferredId::for_test(9815),
        RequestId::real(9815, 1).expect("submit admission request id"),
        Some(Box::new(move |result| {
            calls.fetch_add(1, Ordering::AcqRel);
            admission.store(
                matches!(result, Ok(DeferredResumeOutcome::AdmissionRejected)),
                Ordering::Release,
            );
        })),
    );
    let wait_released = Arc::new(AtomicBool::new(false));
    let job = DeferredResumeJob::new(
        charge,
        AdmissionClass::Data,
        RequestOrdering::Concurrent,
        ResumeStopView::never(),
        Box::new(ProbeWork {
            wait_released: Arc::clone(&wait_released),
            entered: Arc::new(Notify::new()),
            release: None,
            executions: Arc::new(AtomicUsize::new(0)),
        }),
        Arc::clone(&completion),
    );
    let cell = Arc::new(ResumeJobCell::new(job));
    cell.release_wait_permit();
    let (error, returned) = match executor
        .deferred_resume_executor()
        .try_execute_resume(Arc::clone(&cell))
    {
        DeferredResumeEnqueueOutcome::AdmissionRejected { error, cell } => (error, cell),
        DeferredResumeEnqueueOutcome::ExecutorClosing { .. } => panic!("executor unexpectedly closed"),
        DeferredResumeEnqueueOutcome::OperationalFailure { .. } => {
            panic!("executor unexpectedly returned an operational failure")
        }
        DeferredResumeEnqueueOutcome::Submitted(_) => {
            panic!("under-capacity queued budget accepted the resume job")
        }
    };
    returned
        .take()
        .expect("rejected cell retains the resume job")
        .finish_admission_rejected(error);
    let result = completion.take_finished();
    assert_eq!(
        result.expect("admission is a normal terminal outcome"),
        DeferredResumeOutcome::AdmissionRejected
    );
    assert_eq!(observed.load(Ordering::Acquire), 1);
    assert!(observed_admission.load(Ordering::Acquire));
    assert!(wait_released.load(Ordering::Acquire));
    drop(returned);
    drop(cell);
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.queued.current_count, 0);
    assert_eq!(snapshot.inflight.current_count, 0);
    assert_eq!(snapshot.processors.current_count, 0);
}

#[tokio::test]
async fn inflight_admission_rejection_returns_the_exact_job_and_releases_queued_capacity() {
    let charge = 512usize;
    let defaults = AdmissionLimits::default();
    let control_reserve = ResourceLimit { count: 1, bytes: 64 };
    let limits = AdmissionLimits {
        inflight: ResourceLimit {
            bytes: charge + control_reserve.bytes - 1,
            ..defaults.inflight
        },
        control_reserve,
        ..defaults
    };
    let (_runtime, controller, executor) = executor_with_limits("deferred-resume-inflight-reject", limits);
    let (job, completion, _wait_released, _entered, _executions) = probe_job(charge, RequestOrdering::Concurrent, None);
    let cell = Arc::new(ResumeJobCell::new(job));
    cell.release_wait_permit();
    let returned = match executor
        .deferred_resume_executor()
        .try_execute_resume(Arc::clone(&cell))
    {
        DeferredResumeEnqueueOutcome::AdmissionRejected { cell, .. } => cell,
        DeferredResumeEnqueueOutcome::ExecutorClosing { .. } => panic!("executor unexpectedly closed"),
        DeferredResumeEnqueueOutcome::OperationalFailure { .. } => {
            panic!("executor unexpectedly returned an operational failure")
        }
        DeferredResumeEnqueueOutcome::Submitted(_) => {
            panic!("under-capacity inflight budget accepted the resume job")
        }
    };
    assert!(Arc::ptr_eq(&cell, &returned));
    drop(returned);
    drop(cell);
    assert_eq!(
        completion.wait().await.expect("unexecuted job terminalizes normally"),
        DeferredResumeOutcome::Cancelled
    );
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.queued.current_count, 0);
    assert_eq!(snapshot.queued.current_bytes, 0);
    assert_eq!(snapshot.queued.rejected_count, 0);
    assert_eq!(snapshot.inflight.current_count, 0);
    assert_eq!(snapshot.inflight.current_bytes, 0);
    assert_eq!(snapshot.inflight.rejected_count, 1);
    assert_eq!(snapshot.processors.current_count, 0);
    assert_eq!(snapshot.processors.current_bytes, 0);
    assert_eq!(snapshot.processors.rejected_count, 0);
}

#[tokio::test]
async fn processor_rejection_runs_inside_the_owned_task_and_releases_all_capacity() {
    let charge = 512usize;
    let defaults = AdmissionLimits::default();
    let control_reserve = ResourceLimit { count: 1, bytes: 64 };
    let limits = AdmissionLimits {
        processors: ResourceLimit {
            bytes: charge + control_reserve.bytes - 1,
            ..defaults.processors
        },
        control_reserve,
        ..defaults
    };
    let (_runtime, controller, executor) = executor_with_limits("deferred-resume-processor-reject", limits);
    let (job, completion, wait_released, rejected, executions) = probe_job(charge, RequestOrdering::Concurrent, None);
    let cell = Arc::new(ResumeJobCell::new(job));
    cell.release_wait_permit();
    assert!(wait_released.load(Ordering::Acquire));
    let submitted = executor
        .deferred_resume_executor()
        .try_execute_resume(Arc::clone(&cell));
    assert!(
        is_submitted(&submitted),
        "processor rejection happens inside an accepted task"
    );
    drop(cell);
    rejected.notified().await;
    assert_eq!(executions.load(Ordering::Acquire), 0);
    assert_eq!(
        completion.wait().await.expect("processor rejection result"),
        DeferredResumeOutcome::AdmissionRejected
    );
    let report = executor
        .drain_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    assert_eq!(report.aborted, 0);
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.queued.current_count, 0);
    assert_eq!(snapshot.queued.current_bytes, 0);
    assert_eq!(snapshot.queued.rejected_count, 0);
    assert_eq!(snapshot.inflight.current_count, 0);
    assert_eq!(snapshot.inflight.current_bytes, 0);
    assert_eq!(snapshot.inflight.rejected_count, 0);
    assert_eq!(snapshot.processors.current_count, 0);
    assert_eq!(snapshot.processors.current_bytes, 0);
    assert_eq!(snapshot.processors.rejected_count, 1);
}

#[tokio::test]
async fn operation_close_at_spawn_returns_the_exact_cell_and_source_free_cancelled_outcome() {
    let (_runtime, controller, executor) =
        executor_with_limits("deferred-resume-spawn-reject", AdmissionLimits::default());
    executor.close_resume_operation_before_spawn_for_test();
    let (job, completion, wait_released, _entered, _executions) = probe_job(512, RequestOrdering::Concurrent, None);
    let cell = Arc::new(ResumeJobCell::new(job));
    cell.release_wait_permit();
    assert!(wait_released.load(Ordering::Acquire));
    let returned = match executor
        .deferred_resume_executor()
        .try_execute_resume(Arc::clone(&cell))
    {
        DeferredResumeEnqueueOutcome::ExecutorClosing { cell } => cell,
        DeferredResumeEnqueueOutcome::AdmissionRejected { .. } => panic!("capacity unexpectedly rejected"),
        DeferredResumeEnqueueOutcome::OperationalFailure { .. } => {
            panic!("closed operation must remain a source-free control outcome")
        }
        DeferredResumeEnqueueOutcome::Submitted(_) => panic!("closed operation accepted a resume task"),
    };
    assert!(Arc::ptr_eq(&cell, &returned));
    returned
        .take()
        .expect("closing rejection retains the resume job")
        .finish_executor_closed();
    drop(returned);
    drop(cell);
    assert_eq!(
        completion
            .wait()
            .await
            .expect("unspawned job terminalizes without a source"),
        DeferredResumeOutcome::Cancelled
    );
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.queued.current_count, 0);
    assert_eq!(snapshot.inflight.current_count, 0);
    assert_eq!(snapshot.processors.current_count, 0);
}

#[tokio::test]
async fn retired_resume_executor_returns_the_exact_cell_and_source_free_cancelled_outcome() {
    let (job, completion, wait_released, _entered, _executions) = probe_job(512, RequestOrdering::Concurrent, None);
    let cell = Arc::new(ResumeJobCell::new(job));
    cell.release_wait_permit();
    assert!(wait_released.load(Ordering::Acquire));

    let executor = DeferredResumeExecutor::retired();
    let returned = match executor.try_execute_resume(Arc::clone(&cell)) {
        DeferredResumeEnqueueOutcome::ExecutorClosing { cell } => cell,
        DeferredResumeEnqueueOutcome::AdmissionRejected { .. }
        | DeferredResumeEnqueueOutcome::OperationalFailure { .. }
        | DeferredResumeEnqueueOutcome::Submitted(_) => {
            panic!("retired executor must return a source-free closing outcome")
        }
    };
    assert!(Arc::ptr_eq(&cell, &returned));
    returned
        .take()
        .expect("retired executor retains the resume job")
        .finish_executor_closed();
    drop(returned);
    drop(cell);
    assert_eq!(
        completion
            .wait()
            .await
            .expect("retired executor terminalizes without a source"),
        DeferredResumeOutcome::Cancelled
    );
}

#[tokio::test]
async fn preclosed_resume_executor_returns_the_exact_cell_and_source_free_cancelled_outcome() {
    let (_runtime, controller, executor) =
        executor_with_limits("deferred-resume-preclosed", AdmissionLimits::default());
    let report = executor
        .drain_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    assert!(report.is_healthy());

    let (job, completion, wait_released, _entered, _executions) = probe_job(512, RequestOrdering::Concurrent, None);
    let cell = Arc::new(ResumeJobCell::new(job));
    cell.release_wait_permit();
    assert!(wait_released.load(Ordering::Acquire));
    let returned = match executor
        .deferred_resume_executor()
        .try_execute_resume(Arc::clone(&cell))
    {
        DeferredResumeEnqueueOutcome::ExecutorClosing { cell } => cell,
        DeferredResumeEnqueueOutcome::AdmissionRejected { .. }
        | DeferredResumeEnqueueOutcome::OperationalFailure { .. }
        | DeferredResumeEnqueueOutcome::Submitted(_) => {
            panic!("preclosed executor must return a source-free closing outcome")
        }
    };
    assert!(Arc::ptr_eq(&cell, &returned));
    returned
        .take()
        .expect("preclosed executor retains the resume job")
        .finish_executor_closed();
    drop(returned);
    drop(cell);
    assert_eq!(
        completion
            .wait()
            .await
            .expect("preclosed executor terminalizes without a source"),
        DeferredResumeOutcome::Cancelled
    );
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.queued.current_count, 0);
    assert_eq!(snapshot.inflight.current_count, 0);
    assert_eq!(snapshot.processors.current_count, 0);
}

#[tokio::test]
async fn terminal_winner_does_not_mask_an_operation_owner_failure_or_leak_admission() {
    for (name, expected_stop) in [
        ("deferred-resume-owner-parent", super::ResumeStop::ParentCancelled),
        ("deferred-resume-owner-session", super::ResumeStop::SessionClosed),
        ("deferred-resume-owner-deadline", super::ResumeStop::OwnerDeadline),
    ] {
        let (runtime, controller, executor) = executor_with_limits(name, AdmissionLimits::default());
        let conflicting_owner = runtime.root_context().component("deferred-resume-conflicting-owner");
        let binding_task = conflicting_owner
            .task_group()
            .spawn_draining_operation(
                executor.operation_context(),
                "deferred-resume-conflicting-owner",
                async {},
            )
            .expect("bind the resume operation to a conflicting owner");
        assert!(
            conflicting_owner
                .task_group()
                .wait_task(binding_task, Duration::from_secs(1))
                .await,
            "conflicting owner task must finish"
        );

        let session = Arc::new(EmbeddedSessionRecord::new(9_824));
        let lifecycle = runtime.root_context().component("deferred-resume-terminal-winner");
        let deadline =
            (expected_stop == super::ResumeStop::OwnerDeadline).then(|| RequestDeadline::after(Duration::ZERO));
        let control = RequestControlView::from_meta(
            &RequestMeta::new(std::time::Instant::now(), deadline),
            session.view().state().clone(),
            lifecycle.task_group(),
        );
        match expected_stop {
            super::ResumeStop::ParentCancelled => lifecycle.task_group().cancel(),
            super::ResumeStop::SessionClosed => session.close(),
            super::ResumeStop::OwnerDeadline => {}
            super::ResumeStop::ProcessorUnavailable | super::ResumeStop::ServiceStopping => {
                unreachable!("the test covers externally selected terminal winners")
            }
        }
        let stop_view = ResumeStopView::new(control, None);
        assert_eq!(stop_view.current_before_resume(), Some(expected_stop));

        let completion = ResumeCompletion::new(
            DeferredId::for_test(9824),
            RequestId::real(9824, 1).expect("operation-owner request id"),
            None,
        );
        let stopped = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let job = DeferredResumeJob::new(
            512,
            AdmissionClass::Data,
            RequestOrdering::Concurrent,
            stop_view.clone(),
            Box::new(StoppedWork {
                stopped: Arc::clone(&stopped),
            }),
            Arc::clone(&completion),
        );
        let cell = Arc::new(ResumeJobCell::new(job));
        cell.release_wait_permit();
        let (source, returned) = match executor
            .deferred_resume_executor()
            .try_execute_resume(Arc::clone(&cell))
        {
            DeferredResumeEnqueueOutcome::OperationalFailure { source, cell } => (source, cell),
            DeferredResumeEnqueueOutcome::AdmissionRejected { .. }
            | DeferredResumeEnqueueOutcome::ExecutorClosing { .. }
            | DeferredResumeEnqueueOutcome::Submitted(_) => {
                panic!("an operation-owner invariant must remain operational")
            }
        };
        assert_eq!(source.operation(), rocketmq_runtime::RuntimeOperation::OperationOwner);
        assert!(Arc::ptr_eq(&cell, &returned));
        returned
            .take()
            .expect("the operational rejection retains the resume job")
            .finish_executor_failure(source);
        assert!(returned.take().is_none(), "the resume cell is consumed exactly once");
        drop(returned);
        drop(cell);

        let error = completion
            .wait()
            .await
            .expect_err("runtime owner mismatch remains operational");
        let resume_failure = error
            .source()
            .and_then(|source| source.downcast_ref::<super::ResumeOperationalFailure>())
            .expect("transport error retains the typed resume failure");
        let runtime_error = resume_failure
            .source()
            .and_then(|source| source.downcast_ref::<rocketmq_runtime::RuntimeError>())
            .expect("resume failure retains the typed runtime source");
        assert_eq!(
            runtime_error.operation(),
            rocketmq_runtime::RuntimeOperation::OperationOwner
        );
        assert_eq!(stop_view.current_before_resume(), Some(expected_stop));
        assert_eq!(stopped.lock().as_slice(), &[expected_stop]);

        let snapshot = controller.snapshot();
        assert_eq!(snapshot.queued.current_count, 0);
        assert_eq!(snapshot.inflight.current_count, 0);
        assert_eq!(snapshot.processors.current_count, 0);
        runtime
            .shutdown_tasks()
            .await
            .assert_no_task_leak()
            .expect("runtime owner tasks must drain");
    }
}

#[tokio::test]
async fn accepted_never_polled_is_service_stopped_without_leaking_admission() {
    let (_runtime, _controller, executor) =
        executor_with_limits("deferred-resume-never-polled", AdmissionLimits::default());
    let (job, completion, wait_released, _entered, _executions) = probe_job(128, RequestOrdering::Concurrent, None);
    let first_poll_entered = Arc::new(Notify::new());
    let never_release = Arc::new(Notify::new());
    let cell = Arc::new(ResumeJobCell::with_first_poll_gate(
        job,
        Arc::clone(&first_poll_entered),
        never_release,
    ));
    cell.release_wait_permit();
    assert!(wait_released.load(Ordering::Acquire));
    let submitted = executor
        .deferred_resume_executor()
        .try_execute_resume(Arc::clone(&cell));
    assert!(is_submitted(&submitted), "resume task must be accepted");
    drop(cell);
    first_poll_entered.notified().await;
    let report = executor
        .drain_report_until(ShutdownDeadline::after(Duration::ZERO))
        .await;
    assert_eq!(report.active_inline_tasks, 0);
    assert_eq!(report.active_resume_tasks, 1);
    assert_eq!(report.remaining_inline_tasks, 0);
    assert_eq!(report.remaining_resume_tasks, 1);
    assert_eq!(report.shutdown.aborted, 1);
    assert_eq!(
        completion.wait().await.expect("aborted owner terminalizes the job"),
        DeferredResumeOutcome::Cancelled
    );
    let settled = executor
        .drain_report_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    assert_eq!(settled.remaining_inline_tasks, 0);
    assert_eq!(settled.remaining_resume_tasks, 0);
}

#[tokio::test]
async fn dropping_the_caller_completion_after_acceptance_does_not_cancel_the_owned_job() {
    let (_runtime, _controller, executor) =
        executor_with_limits("deferred-resume-caller-drop", AdmissionLimits::default());
    let (job, completion, _wait_released, entered, executions) = probe_job(128, RequestOrdering::Concurrent, None);
    let cell = Arc::new(ResumeJobCell::new(job));
    cell.release_wait_permit();
    let submitted = executor
        .deferred_resume_executor()
        .try_execute_resume(Arc::clone(&cell));
    assert!(is_submitted(&submitted), "resume task must be accepted");
    drop(cell);
    drop(completion);
    entered.notified().await;
    assert_eq!(executions.load(Ordering::Acquire), 1);
    let report = executor
        .drain_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    assert_eq!(report.aborted, 0);
}

#[tokio::test]
async fn drain_report_separates_inline_and_resume_tasks_and_joins_both() {
    let (_runtime, controller, executor) =
        executor_with_limits("session-drain-composite-counts", AdmissionLimits::default());
    let inline_entered = Arc::new(Notify::new());
    let inline_release = Arc::new(Notify::new());
    let task_inline_entered = Arc::clone(&inline_entered);
    let task_inline_release = Arc::clone(&inline_release);
    let inline_attempt = executor
        .try_execute(
            128,
            AdmissionClass::Data,
            None,
            RequestOrdering::Concurrent,
            move |_operation| async move {
                task_inline_entered.notify_one();
                task_inline_release.notified().await;
            },
            move |_operation, _error| async {},
        )
        .expect("inline request accepted");
    assert!(matches!(inline_attempt, SessionDispatchAttempt::Accepted(_)));
    inline_entered.notified().await;

    let resume_release = Arc::new(Notify::new());
    let (job, completion, _wait_released, resume_entered, _executions) =
        probe_job(128, RequestOrdering::Concurrent, Some(Arc::clone(&resume_release)));
    let cell = Arc::new(ResumeJobCell::new(job));
    cell.release_wait_permit();
    let submitted = executor
        .deferred_resume_executor()
        .try_execute_resume(Arc::clone(&cell));
    assert!(is_submitted(&submitted), "resume accepted");
    drop(cell);
    resume_entered.notified().await;

    let release = tokio::spawn(async move {
        tokio::task::yield_now().await;
        inline_release.notify_one();
        resume_release.notify_one();
    });
    let report = executor
        .drain_report_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    release.await.expect("release task");
    assert_eq!(report.active_inline_tasks, 1);
    assert_eq!(report.active_resume_tasks, 1);
    assert_eq!(report.remaining_inline_tasks, 0);
    assert_eq!(report.remaining_resume_tasks, 0);
    assert_eq!(report.shutdown.completed, 2);
    assert!(report.is_healthy());
    let error = completion.wait().await.expect_err("probe response");
    assert_eq!(error.code(), rocketmq_error::TRANSPORT_DISPATCH_FAILED.code());
    assert!(error.source().is_some());
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.queued.current_count, 0);
    assert_eq!(snapshot.inflight.current_count, 0);
    assert_eq!(snapshot.processors.current_count, 0);
}

#[tokio::test]
async fn same_key_resume_jobs_remain_serialized_while_the_first_job_is_running() {
    let (_runtime, _controller, executor) =
        executor_with_limits("deferred-resume-ordering", AdmissionLimits::default());
    let ordering = RequestOrdering::Ordered(RequestOrderingKey::new(17));
    let release_first = Arc::new(Notify::new());
    let (first_job, first_completion, _first_wait, first_entered, first_executions) =
        probe_job(128, ordering, Some(Arc::clone(&release_first)));
    let second_before_ordering = Arc::new(Notify::new());
    let (second_job, second_completion, _second_wait, second_entered, second_executions) =
        probe_job(128, ordering, None);
    let first = Arc::new(ResumeJobCell::new(first_job));
    let second = Arc::new(ResumeJobCell::new(
        second_job.with_before_ordering(Arc::clone(&second_before_ordering)),
    ));
    first.release_wait_permit();
    second.release_wait_permit();
    let route = executor.deferred_resume_executor();
    let first_submitted = route.try_execute_resume(Arc::clone(&first));
    assert!(is_submitted(&first_submitted), "first resume must be accepted");
    drop(first);
    first_entered.notified().await;
    let second_submitted = route.try_execute_resume(Arc::clone(&second));
    assert!(is_submitted(&second_submitted), "second resume must be accepted");
    drop(second);
    second_before_ordering.notified().await;
    assert_eq!(first_executions.load(Ordering::Acquire), 1);
    assert_eq!(second_executions.load(Ordering::Acquire), 0);
    release_first.notify_one();
    second_entered.notified().await;
    assert_eq!(second_executions.load(Ordering::Acquire), 1);
    assert_eq!(
        first_completion.wait().await.expect_err("probe response").code(),
        rocketmq_error::TRANSPORT_DISPATCH_FAILED.code()
    );
    assert_eq!(
        second_completion.wait().await.expect_err("probe response").code(),
        rocketmq_error::TRANSPORT_DISPATCH_FAILED.code()
    );
    let report = executor
        .drain_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    assert_eq!(report.aborted, 0);
}
