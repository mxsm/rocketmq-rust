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
use super::DeferredResumeJob;
use super::DeferredResumeSubmitError;
use super::DeferredResumeWork;
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
use crate::dispatch::DeferredResumeError;
use crate::dispatch::DeferredResumeErrorKind;
use crate::dispatch::DeferredTerminalReason;
use crate::dispatch::RequestControlView;
use crate::dispatch::RequestId;
use crate::dispatch::RequestMeta;
use crate::request_ordering::RequestOrdering;
use crate::request_ordering::RequestOrderingKey;
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
            Err(test_resume_error(DeferredResumeErrorKind::Response))
        })
    }

    fn reject(self: Box<Self>, _error: crate::admission::AdmissionError) -> WorkFuture {
        Box::pin(async move {
            assert!(self.wait_released.load(Ordering::Acquire));
            self.entered.notify_one();
            Err(test_resume_error(DeferredResumeErrorKind::Admission))
        })
    }

    fn finish_admission_rejected(
        self: Box<Self>,
        _error: crate::admission::AdmissionError,
    ) -> Result<crate::dispatch::ResponseReceipt, DeferredResumeError> {
        assert!(self.wait_released.load(Ordering::Acquire));
        Err(test_resume_error(DeferredResumeErrorKind::Admission))
    }

    fn finish_stopped(
        self: Box<Self>,
        stop: super::ResumeStop,
        _source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Result<crate::dispatch::ResponseReceipt, DeferredResumeError> {
        Err(DeferredResumeError::new_with_reason(
            DeferredResumeErrorKind::ExecutorClosing,
            DeferredId::for_test(9814),
            RequestId::real(9814, 1).expect("test request id"),
            None,
            Some(stop.terminal_reason()),
            None,
            None,
        ))
    }
}

fn test_resume_error(kind: DeferredResumeErrorKind) -> DeferredResumeError {
    DeferredResumeError::new(
        kind,
        DeferredId::for_test(9814),
        RequestId::real(9814, 1).expect("test request id"),
        None,
        None,
        None,
    )
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
        Err(DeferredResumeSubmitError::Admission { cell, .. }) => cell,
        Err(DeferredResumeSubmitError::Closing { .. }) => panic!("executor unexpectedly closed"),
        Ok(_) => panic!("under-capacity queued budget accepted the resume job"),
    };
    assert!(Arc::ptr_eq(&cell, &returned));
    drop(returned);
    drop(cell);
    assert_eq!(
        completion.wait().await.expect_err("unexecuted job terminates").kind(),
        DeferredResumeErrorKind::ExecutorClosing
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
                result
                    .as_ref()
                    .is_err_and(|error| error.kind() == DeferredResumeErrorKind::Admission),
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
        Err(DeferredResumeSubmitError::Admission { error, cell }) => (error, cell),
        Err(DeferredResumeSubmitError::Closing { .. }) => panic!("executor unexpectedly closed"),
        Ok(_) => panic!("under-capacity queued budget accepted the resume job"),
    };
    returned
        .take()
        .expect("rejected cell retains the resume job")
        .finish_admission_rejected(error);
    let result = completion.take_finished();
    assert_eq!(
        result.expect_err("admission terminal").kind(),
        DeferredResumeErrorKind::Admission
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
        Err(DeferredResumeSubmitError::Admission { cell, .. }) => cell,
        Err(DeferredResumeSubmitError::Closing { .. }) => panic!("executor unexpectedly closed"),
        Ok(_) => panic!("under-capacity inflight budget accepted the resume job"),
    };
    assert!(Arc::ptr_eq(&cell, &returned));
    drop(returned);
    drop(cell);
    assert_eq!(
        completion.wait().await.expect_err("unexecuted job terminates").kind(),
        DeferredResumeErrorKind::ExecutorClosing
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
    assert!(submitted.is_ok(), "processor rejection happens inside an accepted task");
    drop(cell);
    rejected.notified().await;
    assert_eq!(executions.load(Ordering::Acquire), 0);
    let error = completion.wait().await.expect_err("processor rejection result");
    assert_eq!(error.kind(), DeferredResumeErrorKind::Admission);
    assert_eq!(error.prior_terminal_reason(), None);
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
async fn operation_close_at_spawn_returns_the_exact_job_and_releases_both_permits() {
    let (_runtime, controller, executor) =
        executor_with_limits("deferred-resume-spawn-reject", AdmissionLimits::default());
    executor.close_resume_operation_before_spawn_for_test();
    let (job, completion, _wait_released, _entered, _executions) = probe_job(512, RequestOrdering::Concurrent, None);
    let cell = Arc::new(ResumeJobCell::new(job));
    cell.release_wait_permit();
    let returned = match executor
        .deferred_resume_executor()
        .try_execute_resume(Arc::clone(&cell))
    {
        Err(DeferredResumeSubmitError::Closing { cell, .. }) => cell,
        Err(DeferredResumeSubmitError::Admission { .. }) => panic!("capacity unexpectedly rejected"),
        Ok(_) => panic!("closed operation accepted a resume task"),
    };
    assert!(Arc::ptr_eq(&cell, &returned));
    drop(returned);
    drop(cell);
    assert_eq!(
        completion.wait().await.expect_err("unspawned job terminates").kind(),
        DeferredResumeErrorKind::ExecutorClosing
    );
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.queued.current_count, 0);
    assert_eq!(snapshot.inflight.current_count, 0);
    assert_eq!(snapshot.processors.current_count, 0);
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
    assert!(submitted.is_ok(), "resume task must be accepted");
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
    let error = completion.wait().await.expect_err("aborted owner terminalizes the job");
    assert_eq!(error.kind(), DeferredResumeErrorKind::ExecutorClosing);
    assert_eq!(
        error.prior_terminal_reason(),
        Some(DeferredTerminalReason::ServiceStopping)
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
    assert!(submitted.is_ok(), "resume task must be accepted");
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
    executor
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
    inline_entered.notified().await;

    let resume_release = Arc::new(Notify::new());
    let (job, completion, _wait_released, resume_entered, _executions) =
        probe_job(128, RequestOrdering::Concurrent, Some(Arc::clone(&resume_release)));
    let cell = Arc::new(ResumeJobCell::new(job));
    cell.release_wait_permit();
    assert!(
        executor
            .deferred_resume_executor()
            .try_execute_resume(Arc::clone(&cell))
            .is_ok(),
        "resume accepted"
    );
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
    assert_eq!(
        completion.wait().await.expect_err("probe response").kind(),
        DeferredResumeErrorKind::Response
    );
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
    assert!(first_submitted.is_ok(), "first resume must be accepted");
    drop(first);
    first_entered.notified().await;
    let second_submitted = route.try_execute_resume(Arc::clone(&second));
    assert!(second_submitted.is_ok(), "second resume must be accepted");
    drop(second);
    second_before_ordering.notified().await;
    assert_eq!(first_executions.load(Ordering::Acquire), 1);
    assert_eq!(second_executions.load(Ordering::Acquire), 0);
    release_first.notify_one();
    second_entered.notified().await;
    assert_eq!(second_executions.load(Ordering::Acquire), 1);
    assert_eq!(
        first_completion.wait().await.expect_err("probe response").kind(),
        DeferredResumeErrorKind::Response
    );
    assert_eq!(
        second_completion.wait().await.expect_err("probe response").kind(),
        DeferredResumeErrorKind::Response
    );
    let report = executor
        .drain_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    assert_eq!(report.aborted, 0);
}
