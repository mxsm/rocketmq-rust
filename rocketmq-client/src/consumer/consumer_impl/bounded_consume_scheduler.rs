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

use std::fmt;
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use crate::ClientResult;
use futures::StreamExt;
use rocketmq_runtime::ChildServiceContext;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_util::time::DelayQueue;
use tracing::warn;

use crate::runtime::spawn_client_task_with_context;

struct ScheduledItem<T> {
    item: T,
    _queued_permit: OwnedSemaphorePermit,
}

/// A retry transfers the admitted item without acquiring another capacity slot.
pub(crate) enum ConsumeDisposition<T> {
    Complete,
    Retry { item: T, after: Duration },
}

type DelayedCommand<T> = (ScheduledItem<T>, Duration);

impl<T> ScheduledItem<T> {
    fn into_item(self) -> T {
        self.item
    }
}

/// An enqueue failure that returns ownership of the rejected consume request.
pub(crate) struct ConsumeScheduleError<T> {
    item: T,
}

impl<T> ConsumeScheduleError<T> {
    pub(crate) fn into_item(self) -> T {
        self.item
    }
}

impl<T> fmt::Debug for ConsumeScheduleError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumeScheduleError")
            .field("reason", &"scheduler is stopping")
            .finish()
    }
}

/// A lifecycle-owned consume scheduler with a fixed worker set and one shared
/// delayed-retry queue.
pub(crate) struct BoundedConsumeScheduler<T> {
    ready_tx: mpsc::Sender<ScheduledItem<T>>,
    ready_rx: StdMutex<Option<mpsc::Receiver<ScheduledItem<T>>>>,
    delayed_tx: mpsc::Sender<DelayedCommand<T>>,
    delayed_rx: StdMutex<Option<mpsc::Receiver<DelayedCommand<T>>>>,
    queued_slots: Arc<Semaphore>,
    #[cfg(any(test, feature = "test-support"))]
    capacity: usize,
    stopping: CancellationToken,
    force_stop: CancellationToken,
    tasks: TaskTracker,
    started: AtomicBool,
}

impl<T> BoundedConsumeScheduler<T>
where
    T: Send + 'static,
{
    pub(crate) fn new(capacity: usize) -> ClientResult<Self> {
        if capacity == 0 {
            return Err(crate::mq_client_err!(
                "consume scheduler capacity must be greater than 0"
            ));
        }
        let (ready_tx, ready_rx) = mpsc::channel(capacity);
        let (delayed_tx, delayed_rx) = mpsc::channel(capacity);
        Ok(Self {
            ready_tx,
            ready_rx: StdMutex::new(Some(ready_rx)),
            delayed_tx,
            delayed_rx: StdMutex::new(Some(delayed_rx)),
            queued_slots: Arc::new(Semaphore::new(capacity)),
            #[cfg(any(test, feature = "test-support"))]
            capacity,
            stopping: CancellationToken::new(),
            force_stop: CancellationToken::new(),
            tasks: TaskTracker::new(),
            started: AtomicBool::new(false),
        })
    }

    pub(crate) fn start<H, F>(
        &self,
        service_context: &ChildServiceContext,
        worker_count: usize,
        handler: H,
    ) -> ClientResult<()>
    where
        H: Fn(T) -> F + Send + Sync + Clone + 'static,
        F: Future<Output = ConsumeDisposition<T>> + Send + 'static,
    {
        if worker_count == 0 {
            return Err(crate::mq_client_err!(
                "consume scheduler worker count must be greater than 0"
            ));
        }
        if self.started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let ready_rx = self
            .ready_rx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| crate::mq_client_err!("consume scheduler ready receiver is unavailable"))?;
        let delayed_rx = self
            .delayed_rx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| crate::mq_client_err!("consume scheduler delayed receiver is unavailable"))?;
        let ready_rx = Arc::new(Mutex::new(ready_rx));

        self.spawn_delayed_dispatcher(service_context, delayed_rx)?;
        for _ in 0..worker_count {
            self.spawn_worker(service_context, Arc::clone(&ready_rx), handler.clone())?;
        }
        Ok(())
    }

    fn spawn_delayed_dispatcher(
        &self,
        service_context: &ChildServiceContext,
        mut delayed_rx: mpsc::Receiver<DelayedCommand<T>>,
    ) -> ClientResult<()> {
        let stopping = self.stopping.clone();
        let ready_tx = self.ready_tx.clone();
        let task = self.tasks.track_future(async move {
            let mut delayed = DelayQueue::new();
            loop {
                if delayed.is_empty() {
                    tokio::select! {
                        biased;
                        _ = stopping.cancelled() => break,
                        command = delayed_rx.recv() => match command {
                            Some((item, delay)) => { delayed.insert(item, delay); }
                            None => break,
                        }
                    }
                    continue;
                }

                tokio::select! {
                    biased;
                    _ = stopping.cancelled() => break,
                    expired = delayed.next() => {
                        if let Some(expired) = expired {
                            let item = expired.into_inner();
                            tokio::select! {
                                biased;
                                _ = stopping.cancelled() => break,
                                result = ready_tx.send(item) => {
                                    if result.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    },
                    command = delayed_rx.recv() => match command {
                        Some((item, delay)) => { delayed.insert(item, delay); }
                        None => break,
                    }
                }
            }
        });
        spawn_client_task_with_context(
            service_context,
            "rocketmq-client-consume-delay-scheduler",
            Box::pin(task),
        )
        .map(|_| ())
        .map_err(|error| crate::ClientError::service_source("consume_delay_scheduler", error))
    }

    fn spawn_worker<H, F>(
        &self,
        service_context: &ChildServiceContext,
        ready_rx: Arc<Mutex<mpsc::Receiver<ScheduledItem<T>>>>,
        handler: H,
    ) -> ClientResult<()>
    where
        H: Fn(T) -> F + Send + Sync + 'static,
        F: Future<Output = ConsumeDisposition<T>> + Send + 'static,
    {
        let stopping = self.stopping.clone();
        let force_stop = self.force_stop.clone();
        let delayed_tx = self.delayed_tx.clone();
        let task = self.tasks.track_future(async move {
            loop {
                let scheduled = tokio::select! {
                    biased;
                    _ = stopping.cancelled() => break,
                    item = async {
                        let mut receiver = ready_rx.lock().await;
                        receiver.recv().await
                    } => item,
                };
                let Some(scheduled) = scheduled else {
                    break;
                };
                let ScheduledItem { item, _queued_permit } = scheduled;
                let disposition = tokio::select! {
                    biased;
                    _ = force_stop.cancelled() => break,
                    disposition = handler(item) => disposition,
                };
                if let ConsumeDisposition::Retry { item, after } = disposition {
                    if stopping.is_cancelled() {
                        break;
                    }
                    // Every ready, running, and delayed item owns one of K permits.
                    // This transfer therefore always fits in the K-sized channel.
                    let scheduled = ScheduledItem { item, _queued_permit };
                    if let Err(error) = delayed_tx.try_send((scheduled, after)) {
                        let (_unfinished, _) = error.into_inner();
                        warn!("consume retry transfer rejected; unfinished messages remain uncommitted");
                        break;
                    }
                }
            }
        });
        spawn_client_task_with_context(service_context, "rocketmq-client-consume-worker", Box::pin(task))
            .map(|_| ())
            .map_err(|error| crate::ClientError::service_source("consume_worker", error))
    }

    pub(crate) async fn schedule(&self, item: T) -> Result<(), ConsumeScheduleError<T>> {
        self.schedule_on(item, None).await
    }

    pub(crate) async fn schedule_after(&self, item: T, delay: Duration) -> Result<(), ConsumeScheduleError<T>> {
        self.schedule_on(item, Some(delay)).await
    }

    async fn schedule_on(&self, item: T, delay: Option<Duration>) -> Result<(), ConsumeScheduleError<T>> {
        if self.stopping.is_cancelled() {
            return Err(ConsumeScheduleError { item });
        }
        let permit = tokio::select! {
            biased;
            _ = self.stopping.cancelled() => return Err(ConsumeScheduleError { item }),
            permit = Arc::clone(&self.queued_slots).acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => return Err(ConsumeScheduleError { item }),
            }
        };
        if self.stopping.is_cancelled() {
            return Err(ConsumeScheduleError { item });
        }
        let scheduled = ScheduledItem {
            item,
            _queued_permit: permit,
        };
        match delay {
            Some(delay) => self
                .delayed_tx
                .try_send((scheduled, delay))
                .map_err(|error| ConsumeScheduleError {
                    item: error.into_inner().0.into_item(),
                }),
            None => self.ready_tx.try_send(scheduled).map_err(|error| ConsumeScheduleError {
                item: error.into_inner().into_item(),
            }),
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn queued(&self) -> usize {
        self.capacity.saturating_sub(self.queued_slots.available_permits())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn task_count(&self) -> usize {
        self.tasks.len()
    }

    pub(crate) async fn shutdown(&self, timeout: Duration) -> bool {
        self.stopping.cancel();
        self.queued_slots.close();
        self.ready_rx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        self.delayed_rx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        self.tasks.close();
        if tokio::time::timeout(timeout, self.tasks.wait()).await.is_ok() {
            return true;
        }
        self.force_stop.cancel();
        let stopped = tokio::time::timeout(Duration::from_secs(1), self.tasks.wait())
            .await
            .is_ok();
        if !stopped {
            warn!("bounded consume scheduler did not stop after forced cancellation");
        }
        stopped
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use tokio::sync::Notify;

    fn update_peak(peak: &AtomicUsize, active: usize) {
        let mut observed = peak.load(Ordering::Acquire);
        while active > observed {
            match peak.compare_exchange_weak(observed, active, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return,
                Err(current) => observed = current,
            }
        }
    }

    async fn wait_for(counter: &AtomicUsize, expected: usize, changed: &Notify) {
        loop {
            let notified = changed.notified();
            if counter.load(Ordering::Acquire) >= expected {
                return;
            }
            notified.await;
        }
    }

    #[tokio::test]
    async fn fixed_workers_bound_execution_and_shutdown_cleanly() {
        let scheduler = BoundedConsumeScheduler::new(4).expect("scheduler config should be valid");
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let changed = Arc::new(Notify::new());
        let releases = Arc::new(Semaphore::new(0));

        scheduler
            .start(
                &crate::runtime::test_service_context("bounded-consume-scheduler-test"),
                2,
                {
                    let active = Arc::clone(&active);
                    let peak = Arc::clone(&peak);
                    let completed = Arc::clone(&completed);
                    let changed = Arc::clone(&changed);
                    let releases = Arc::clone(&releases);
                    move |_item: usize| {
                        let active = Arc::clone(&active);
                        let peak = Arc::clone(&peak);
                        let completed = Arc::clone(&completed);
                        let changed = Arc::clone(&changed);
                        let releases = Arc::clone(&releases);
                        async move {
                            let now_active = active.fetch_add(1, Ordering::AcqRel) + 1;
                            update_peak(&peak, now_active);
                            changed.notify_waiters();
                            let permit = releases.acquire().await.expect("release semaphore stays open");
                            permit.forget();
                            active.fetch_sub(1, Ordering::AcqRel);
                            completed.fetch_add(1, Ordering::AcqRel);
                            changed.notify_waiters();
                            ConsumeDisposition::Complete
                        }
                    }
                },
            )
            .expect("scheduler should start");

        scheduler.schedule(1).await.expect("first item should enqueue");
        scheduler.schedule(2).await.expect("second item should enqueue");
        scheduler.schedule(3).await.expect("third item should enqueue");
        wait_for(&active, 2, &changed).await;
        assert_eq!(peak.load(Ordering::Acquire), 2);
        assert_eq!(scheduler.task_count(), 3, "two workers plus one shared delay task");

        releases.add_permits(2);
        wait_for(&completed, 2, &changed).await;
        releases.add_permits(1);
        wait_for(&completed, 3, &changed).await;

        assert!(scheduler.shutdown(Duration::from_secs(1)).await);
        assert_eq!(scheduler.task_count(), 0);
        assert_eq!(scheduler.queued(), 0);
    }

    #[tokio::test]
    async fn delayed_retries_share_one_owned_scheduler_task() {
        let scheduler = BoundedConsumeScheduler::new(64).expect("scheduler config should be valid");
        let completed = Arc::new(AtomicUsize::new(0));
        let changed = Arc::new(Notify::new());
        scheduler
            .start(
                &crate::runtime::test_service_context("bounded-consume-delay-test"),
                2,
                {
                    let completed = Arc::clone(&completed);
                    let changed = Arc::clone(&changed);
                    move |_item: usize| {
                        let completed = Arc::clone(&completed);
                        let changed = Arc::clone(&changed);
                        async move {
                            completed.fetch_add(1, Ordering::AcqRel);
                            changed.notify_waiters();
                            ConsumeDisposition::Complete
                        }
                    }
                },
            )
            .expect("scheduler should start");

        for item in 0..64 {
            scheduler
                .schedule_after(item, Duration::from_millis(1))
                .await
                .expect("delayed item should enqueue");
        }
        assert_eq!(scheduler.task_count(), 3, "retry count must not create more tasks");
        tokio::time::timeout(Duration::from_secs(1), wait_for(&completed, 64, &changed))
            .await
            .expect("all delayed retries should complete");
        assert!(scheduler.shutdown(Duration::from_secs(1)).await);
    }

    #[tokio::test]
    async fn shutdown_releases_blocked_admission_and_returns_item_ownership() {
        let scheduler = BoundedConsumeScheduler::new(1).expect("scheduler config should be valid");
        let entered = Arc::new(Notify::new());
        let active = Arc::new(AtomicBool::new(false));
        scheduler
            .start(
                &crate::runtime::test_service_context("bounded-consume-backpressure-test"),
                1,
                {
                    let entered = Arc::clone(&entered);
                    let active = Arc::clone(&active);
                    move |_item: usize| {
                        let entered = Arc::clone(&entered);
                        let active = Arc::clone(&active);
                        async move {
                            active.store(true, Ordering::Release);
                            entered.notify_waiters();
                            futures::future::pending::<ConsumeDisposition<usize>>().await
                        }
                    }
                },
            )
            .expect("scheduler should start");

        scheduler.schedule(1).await.expect("active item should enqueue");
        loop {
            let notified = entered.notified();
            if active.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
        let blocked = scheduler.schedule(2);
        tokio::pin!(blocked);
        tokio::select! {
            result = &mut blocked => panic!("admission should wait while the queue is full: {result:?}"),
            () = tokio::task::yield_now() => {}
        }

        assert!(scheduler.shutdown(Duration::ZERO).await);
        let rejected = blocked.await.expect_err("shutdown must reject blocked admission");
        assert_eq!(rejected.into_item(), 2);
        assert_eq!(scheduler.queued(), 0);
    }
    #[tokio::test(start_paused = true)]
    async fn saturated_workers_retry_with_original_capacity_and_release_on_shutdown() {
        for workers in [1, 2] {
            let scheduler = BoundedConsumeScheduler::new(2).unwrap();
            let entered = Arc::new(AtomicUsize::new(0));
            let changed = Arc::new(Notify::new());
            let releases = Arc::new(Semaphore::new(0));
            let runs = Arc::new(AtomicUsize::new(0));
            let runtime = rocketmq_runtime::RuntimeContext::try_from_current("saturated-retry").unwrap();
            let context = runtime.service_context("saturated-retry-service").component("consume");
            scheduler
                .start(&context, workers, {
                    let entered = entered.clone();
                    let changed = changed.clone();
                    let releases = releases.clone();
                    let runs = runs.clone();
                    move |(item, attempt)| {
                        let entered = entered.clone();
                        let changed = changed.clone();
                        let releases = releases.clone();
                        let runs = runs.clone();
                        async move {
                            if attempt == 0 {
                                entered.fetch_add(1, Ordering::AcqRel);
                                changed.notify_waiters();
                                releases.acquire().await.unwrap().forget();
                            }
                            runs.fetch_add(1, Ordering::AcqRel);
                            changed.notify_waiters();
                            ConsumeDisposition::Retry {
                                item: (item, attempt + 1),
                                after: Duration::from_secs(5),
                            }
                        }
                    }
                })
                .unwrap();
            scheduler.schedule((0, 0)).await.unwrap();
            scheduler.schedule((1, 0)).await.unwrap();
            wait_for(&entered, workers, &changed).await;
            assert_eq!(scheduler.queued(), 2, "running items retain their permits");
            releases.add_permits(2);
            wait_for(&runs, 2, &changed).await;
            for expected in [4, 6, 8] {
                tokio::time::timeout(Duration::from_secs(6), wait_for(&runs, expected, &changed))
                    .await
                    .unwrap();
                assert_eq!(scheduler.queued(), 2, "retries must not grow admission");
            }
            assert!(scheduler.shutdown(Duration::from_secs(1)).await);
            assert_eq!(scheduler.queued(), 0);
            assert_eq!(scheduler.task_count(), 0);
        }
    }
}
