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
    assert!(route.try_execute_resume(Arc::clone(&first)).is_ok());
    drop(first);
    first_entered.notified().await;
    assert!(route.try_execute_resume(Arc::clone(&second)).is_ok());
    drop(second);
    second_before_ordering.notified().await;

    tokio::time::advance(Duration::from_millis(10)).await;
    let error = second_completion
        .wait()
        .await
        .expect_err("owner cutoff terminalizes the ordered waiter");
    assert_eq!(error.kind(), DeferredResumeErrorKind::ExecutorClosing);
    assert_eq!(
        error.prior_terminal_reason(),
        Some(DeferredTerminalReason::OwnerDeadline)
    );
    assert_eq!(first_executions.load(Ordering::Acquire), 1);
    assert_eq!(second_executions.load(Ordering::Acquire), 0);

    release_first.notify_one();
    assert_eq!(
        first_completion.wait().await.expect_err("probe response").kind(),
        DeferredResumeErrorKind::Response
    );
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
