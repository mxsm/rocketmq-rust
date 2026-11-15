// Copyright 2023 The RocketMQ Rust Authors
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
use std::panic::AssertUnwindSafe;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Instant;

use futures::future::FutureExt;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::TaskGroup;
use super::TaskGroupLifecycleState;
use crate::error::RuntimeError;
use crate::error::RuntimeResult;
use crate::shutdown_deadline::ShutdownDeadline;
use crate::shutdown_deadline::ABORT_CONFIRMATION_TIMEOUT;
use crate::CancellationObserver;
use crate::RuntimeOperation;

/// Short-lived tasks of one owner, counted instead of registered one by one.
///
/// The other spawn methods of [`TaskGroup`] give every task an id, metadata and an abort handle
/// in the group's registry, so the group can report, wait for and abort each one. That costs
/// several locks and allocations per task, which dominates tasks that live for one request.
/// A set keeps only a count of its running tasks.
///
/// Its tasks run on the group's runtime and are tracked by the group, so the group's shutdown
/// waits for them like for its draining operation tasks. Every task races the cancellation
/// token the set was created with: cancelling it drops each running task's future at its next
/// poll, which is when an aborted task stops too. The owner closes the set and waits for it with
/// [`drain_until`](Self::drain_until). Tasks of a set its owner dropped keep running, owned by
/// the group: a group shutdown that reaches its deadline cancels the token of every set it
/// created, where it aborts its registered tasks.
pub struct CountedTaskSet {
    inner: Arc<CountedTaskSetInner>,
}

pub(super) struct CountedTaskSetInner {
    group: TaskGroup,
    // Checked and awaited by every task, so through an observer rather than the token's lock.
    stop: CancellationObserver,
    active: AtomicUsize,
    closed: AtomicBool,
    // Set by a waiter; only then does a task that leaves the set idle notify `idle`.
    draining: AtomicBool,
    idle: Notify,
    panicked: AtomicU64,
}

impl TaskGroup {
    /// Creates a [`CountedTaskSet`] whose tasks run on this group's runtime, are tracked by this
    /// group and stop when `stop` is cancelled.
    pub fn counted_task_set(&self, stop: CancellationToken) -> CountedTaskSet {
        let inner = Arc::new(CountedTaskSetInner {
            group: self.clone(),
            stop: CancellationObserver::new(stop),
            active: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            draining: AtomicBool::new(false),
            idle: Notify::new(),
            panicked: AtomicU64::new(0),
        });
        let mut sets = self.inner.counted_sets.lock();
        sets.retain(|set| set.strong_count() > 0);
        sets.push(Arc::downgrade(&inner));
        drop(sets);
        CountedTaskSet { inner }
    }
}

/// Cancels the token of every live set in `sets` and returns how many of their tasks were running.
pub(super) fn stop_counted_sets(sets: &parking_lot::Mutex<Vec<Weak<CountedTaskSetInner>>>) -> usize {
    let live: Vec<_> = sets.lock().iter().filter_map(Weak::upgrade).collect();
    live.iter()
        .map(|set| {
            set.stop.token().cancel();
            set.active.load(Ordering::SeqCst)
        })
        .sum()
}

impl CountedTaskSet {
    /// Spawns `future` as a counted task.
    ///
    /// A panic in the future ends only its task and is counted in [`Self::panicked`].
    ///
    /// # Errors
    ///
    /// Returns a closed error once the set is closed or its token cancelled, and the group's
    /// admission error once the group no longer accepts tasks.
    pub fn spawn<F>(&self, future: F) -> RuntimeResult<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let state = self.inner.group.lifecycle_state();
        if state != TaskGroupLifecycleState::Open {
            return Err(state.admission_error(RuntimeOperation::SpawnTaskGroupTask));
        }
        // Counted before the closed check, and `close` publishes before a waiter reads the
        // count (both sequentially consistent): a spawn that a waiter does not see sees the set
        // closed and backs out.
        self.inner.active.fetch_add(1, Ordering::SeqCst);
        let task = CountedTask {
            inner: Arc::clone(&self.inner),
        };
        if self.inner.closed.load(Ordering::SeqCst) || self.inner.stop.is_cancelled() {
            drop(task);
            return Err(RuntimeError::closed(RuntimeOperation::SpawnTaskGroupTask));
        }
        let tracked = self.inner.group.inner.tracker.track_future(task.run(future));
        drop(self.inner.group.inner.runtime.tokio_handle().spawn(tracked));
        Ok(())
    }

    /// Stops accepting tasks. Running tasks continue.
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::SeqCst);
    }

    /// Returns the number of tasks that have not finished.
    pub fn active(&self) -> usize {
        self.inner.active.load(Ordering::SeqCst)
    }

    /// Returns the number of tasks whose future panicked.
    pub fn panicked(&self) -> u64 {
        self.inner.panicked.load(Ordering::Relaxed)
    }

    /// Closes the set and waits until every task has finished or `deadline` passes.
    ///
    /// Tasks still running at the deadline are stopped through the set's token, and the call
    /// waits up to one second for their futures to be dropped, as
    /// [`OperationContext::wait`](crate::OperationContext::wait) does for aborted tasks. Returns
    /// whether every task finished before the deadline.
    pub async fn drain_until(&self, deadline: ShutdownDeadline) -> bool {
        self.close();
        if self.wait_idle(deadline.instant()).await {
            return true;
        }
        self.inner.stop.token().cancel();
        let _ = self.wait_idle(Instant::now() + ABORT_CONFIRMATION_TIMEOUT).await;
        false
    }

    async fn wait_idle(&self, until: Instant) -> bool {
        self.inner.draining.store(true, Ordering::SeqCst);
        loop {
            // Enabled before the count is read, so a task that finishes in between is not missed.
            let notified = self.inner.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.inner.active.load(Ordering::SeqCst) == 0 {
                return true;
            }
            if tokio::time::timeout_at(until.into(), notified).await.is_err() {
                return self.inner.active.load(Ordering::SeqCst) == 0;
            }
        }
    }
}

/// One task's place in the set: counted from spawn until it is dropped, even if the runtime drops
/// the task without polling it.
struct CountedTask {
    inner: Arc<CountedTaskSetInner>,
}

impl CountedTask {
    async fn run<F>(self, future: F)
    where
        F: Future<Output = ()>,
    {
        let inner = &self.inner;
        // The future is polled first, so the token is not polled on the poll that finishes it.
        tokio::select! {
            biased;
            result = AssertUnwindSafe(future).catch_unwind() => {
                if result.is_err() {
                    inner.panicked.fetch_add(1, Ordering::Relaxed);
                    tracing::error!("counted task panicked");
                }
            }
            () = inner.stop.cancelled() => {}
        }
    }
}

impl Drop for CountedTask {
    fn drop(&mut self) {
        if self.inner.active.fetch_sub(1, Ordering::SeqCst) == 1 && self.inner.draining.load(Ordering::SeqCst) {
            self.inner.idle.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::*;
    use crate::RuntimeContext;

    fn set_with_token() -> (TaskGroup, CountedTaskSet, CancellationToken) {
        let context = RuntimeContext::from_current("counted-task-set");
        let group = context.service_context("owner").task_group().clone();
        let stop = CancellationToken::new();
        let set = group.counted_task_set(stop.clone());
        (group, set, stop)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drain_waits_for_running_tasks_to_finish() {
        let (_group, set, stop) = set_with_token();
        let (release, released) = oneshot::channel::<()>();
        let (finished_tx, finished) = oneshot::channel::<()>();
        set.spawn(async move {
            let _ = released.await;
            let _ = finished_tx.send(());
        })
        .unwrap();
        assert_eq!(set.active(), 1);

        let drained = tokio::spawn(async move {
            let drained = set.drain_until(ShutdownDeadline::after(Duration::from_secs(5))).await;
            (drained, set.active())
        });
        tokio::task::yield_now().await;
        assert!(!drained.is_finished(), "the drain waits for the running task");
        release.send(()).unwrap();

        assert_eq!(drained.await.unwrap(), (true, 0));
        finished.await.expect("the task ran to completion");
        drop(stop);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drain_stops_tasks_still_running_at_the_deadline() {
        let (_group, set, stop) = set_with_token();
        let (dropped_tx, dropped) = oneshot::channel::<()>();
        struct NotifyOnDrop(Option<oneshot::Sender<()>>);
        impl Drop for NotifyOnDrop {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }
        set.spawn(async move {
            let _notify = NotifyOnDrop(Some(dropped_tx));
            std::future::pending::<()>().await;
        })
        .unwrap();

        let drained = set
            .drain_until(ShutdownDeadline::after(Duration::from_millis(50)))
            .await;

        assert!(!drained, "the task did not finish before the deadline");
        assert!(stop.is_cancelled(), "the late task is stopped through the token");
        assert_eq!(set.active(), 0);
        dropped.await.expect("the late task's future was dropped");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelling_the_token_stops_running_tasks() {
        let (_group, set, stop) = set_with_token();
        set.spawn(std::future::pending::<()>()).unwrap();
        stop.cancel();

        assert!(set.drain_until(ShutdownDeadline::after(Duration::from_secs(5))).await);
        assert!(set.spawn(async {}).is_err(), "a cancelled set takes no new task");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_closed_set_takes_no_new_task() {
        let (_group, set, _stop) = set_with_token();
        set.close();

        let error = set.spawn(async {}).expect_err("closed set");
        assert_eq!(error.kind(), crate::RuntimeErrorKind::Closed);
        assert_eq!(set.active(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_panicking_task_is_counted_and_leaves_the_others_running() {
        let (_group, set, _stop) = set_with_token();
        let (finished_tx, finished) = oneshot::channel::<()>();
        set.spawn(async { panic!("counted task test panic") }).unwrap();
        set.spawn(async move {
            tokio::task::yield_now().await;
            let _ = finished_tx.send(());
        })
        .unwrap();

        finished.await.expect("the other task finished");
        assert!(set.drain_until(ShutdownDeadline::after(Duration::from_secs(5))).await);
        assert_eq!(set.panicked(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_sets_tasks_run_until_the_group_shutdown_deadline_stops_them() {
        let (group, set, stop) = set_with_token();
        let (dropped_tx, dropped) = oneshot::channel::<()>();
        struct NotifyOnDrop(Option<oneshot::Sender<()>>);
        impl Drop for NotifyOnDrop {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }
        set.spawn(async move {
            let _notify = NotifyOnDrop(Some(dropped_tx));
            std::future::pending::<()>().await;
        })
        .unwrap();

        drop(set);
        assert!(
            !stop.is_cancelled(),
            "the group, not the set's owner, owns the task now"
        );

        let report = group.shutdown(Duration::from_millis(50)).await;

        assert!(stop.is_cancelled(), "the shutdown deadline stops the task");
        assert_eq!(report.aborted, 1, "{report:?}");
        tokio::time::timeout(Duration::from_secs(5), dropped)
            .await
            .expect("the task's future was dropped")
            .expect("drop notification");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn group_shutdown_waits_for_counted_tasks() {
        let (group, set, _stop) = set_with_token();
        let (release, released) = oneshot::channel::<()>();
        set.spawn(async move {
            let _ = released.await;
        })
        .unwrap();

        let shutdown = tokio::spawn({
            let group = group.clone();
            async move { group.shutdown(Duration::from_secs(5)).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!shutdown.is_finished(), "the group waits for its counted task");
        release.send(()).unwrap();

        let report = shutdown.await.unwrap();
        assert!(report.assert_no_task_leak().is_ok(), "{report:?}");
        drop(set);
    }
}
