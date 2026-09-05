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

use super::*;
use crate::dispatch::DeferredTerminalReason;

#[tokio::test(start_paused = true)]
async fn owner_cutoff_is_live_early_and_terminal_at_equal_and_late_instants() {
    let (runtime, _controller, executor) =
        executor_with_limits("deferred-resume-cutoff-boundary", AdmissionLimits::default());
    let session = Arc::new(EmbeddedSessionRecord::new(9_820));
    let lifecycle = runtime
        .root_context()
        .component("deferred-resume-cutoff-boundary-owner");
    let control = RequestControlView::from_meta(
        &RequestMeta::new(
            std::time::Instant::now(),
            Some(RequestDeadline::after(Duration::from_millis(10))),
        ),
        session.view().state().clone(),
        lifecycle.task_group(),
    );
    let stop_view = ResumeStopView::new(control, None);

    assert_eq!(stop_view.current_before_resume(), None);
    assert_eq!(stop_view.current_before_write(), None);
    tokio::time::advance(Duration::from_millis(9)).await;
    assert_eq!(stop_view.current_before_resume(), None, "early remains live");
    assert_eq!(stop_view.current_before_write(), None, "early write remains live");

    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(
        stop_view.current_before_resume().map(|stop| stop.terminal_reason()),
        Some(DeferredTerminalReason::OwnerDeadline),
        "the equal instant is terminal"
    );
    assert_eq!(
        stop_view.current_before_write().map(|stop| stop.terminal_reason()),
        Some(DeferredTerminalReason::OwnerDeadline)
    );

    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(
        stop_view.current_before_resume().map(|stop| stop.terminal_reason()),
        Some(DeferredTerminalReason::OwnerDeadline),
        "late remains terminal"
    );
    let report = executor
        .drain_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    assert_eq!(report.aborted, 0);
}

#[tokio::test(start_paused = true)]
async fn owner_cutoff_cancels_an_ordered_waiter_before_processor_execution() {
    let (runtime, controller, executor) =
        executor_with_limits("deferred-resume-ordering-cutoff", AdmissionLimits::default());
    let ordering = RequestOrdering::Ordered(RequestOrderingKey::new(18));
    let release_first = Arc::new(Notify::new());
    let (first_job, first_completion, _first_wait, first_entered, first_executions) =
        probe_job(128, ordering, Some(Arc::clone(&release_first)));

    let session = Arc::new(EmbeddedSessionRecord::new(9819));
    let lifecycle = runtime
        .root_context()
        .component("deferred-resume-ordering-cutoff-owner");
    let control = RequestControlView::from_meta(
        &RequestMeta::new(
            std::time::Instant::now(),
            Some(RequestDeadline::after(Duration::from_millis(10))),
        ),
        session.view().state().clone(),
        lifecycle.task_group(),
    );
    let stop_view = ResumeStopView::new(control, None);
    let second_before_ordering = Arc::new(Notify::new());
    let (second_job, second_completion, _second_wait, _second_entered, second_executions) =
        probe_job_with_stop(128, ordering, None, stop_view);

    let first = Arc::new(ResumeJobCell::new(first_job));
    let second = Arc::new(ResumeJobCell::new(
        second_job.with_before_ordering(Arc::clone(&second_before_ordering)),
    ));
    first.release_wait_permit();
    second.release_wait_permit();
    let route = executor.deferred_resume_executor();
    let first_submitted = route.try_execute_resume(Arc::clone(&first));
    assert!(is_submitted(&first_submitted));
    drop(first);
    first_entered.notified().await;
    let second_submitted = route.try_execute_resume(Arc::clone(&second));
    assert!(is_submitted(&second_submitted));
    drop(second);
    second_before_ordering.notified().await;

    tokio::time::advance(Duration::from_millis(10)).await;
    assert_eq!(
        second_completion
            .wait()
            .await
            .expect("owner cutoff is a normal cancellation outcome"),
        DeferredResumeOutcome::Cancelled
    );
    assert_eq!(first_executions.load(Ordering::Acquire), 1);
    assert_eq!(second_executions.load(Ordering::Acquire), 0);
    release_first.notify_one();
    let error = first_completion
        .wait()
        .await
        .expect_err("probe work terminates operationally");
    assert_eq!(error.descriptor(), &rocketmq_error::TRANSPORT_DISPATCH_FAILED);
    let report = executor
        .drain_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    assert_eq!(report.aborted, 0);
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.queued.current_count, 0);
    assert_eq!(snapshot.queued.current_bytes, 0);
    assert_eq!(snapshot.inflight.current_count, 0);
    assert_eq!(snapshot.inflight.current_bytes, 0);
    assert_eq!(snapshot.processors.current_count, 0);
    assert_eq!(snapshot.processors.current_bytes, 0);
}
