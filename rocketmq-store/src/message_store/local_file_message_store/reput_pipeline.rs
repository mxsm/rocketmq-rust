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

use std::mem::size_of;
use std::sync::Arc;

use futures_util::future::join_all;
use rocketmq_runtime::BudgetClass;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::ResourceBudget;
use tracing::warn;

use crate::base::commit_log_dispatcher::CommitLogDispatchExecution;
use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::runtime::StoreRuntimeScope;

use super::dispatch::CommitLogDispatchHandle;

const MIN_CLONE_BUDGET_BYTES: usize = 16 * 1024 * 1024;
const MAX_CLONE_BUDGET_BYTES: usize = 512 * 1024 * 1024;

/// Dispatches one CommitLog batch to independent required derived-state lanes.
///
/// The Reput reader still admits only one batch at a time to this coordinator. That preserves
/// strict batch order for every lane while allowing CQ, Index, RocksDB, Timer, and Tiered sinks
/// to process the current batch concurrently. The published frontier advances only after every
/// required lane completes.
#[derive(Clone)]
pub(super) struct ReputDispatchPipeline {
    dispatcher: CommitLogDispatchHandle,
    runtime_scope: StoreRuntimeScope,
    parallel_enabled: bool,
    clone_budget: ResourceBudget,
}

impl ReputDispatchPipeline {
    pub(super) fn new(
        dispatcher: CommitLogDispatchHandle,
        runtime_scope: &StoreRuntimeScope,
        parallel_enabled: bool,
        read_budget_bytes: usize,
    ) -> Self {
        let store_budget = runtime_scope.resource_budget();
        let capacity = read_budget_bytes
            .saturating_mul(16)
            .clamp(MIN_CLONE_BUDGET_BYTES, MAX_CLONE_BUDGET_BYTES)
            .min(store_budget.limit().capacity.bytes);
        let clone_budget = store_budget
            .child(
                "reput-required-lanes",
                BudgetLimit::new(1, capacity, FullPolicy::Reject),
            )
            .expect("Reput clone budget must fit the validated Store runtime budget");
        Self::with_clone_budget(dispatcher, runtime_scope, parallel_enabled, clone_budget)
    }

    fn with_clone_budget(
        dispatcher: CommitLogDispatchHandle,
        runtime_scope: &StoreRuntimeScope,
        parallel_enabled: bool,
        clone_budget: ResourceBudget,
    ) -> Self {
        Self {
            dispatcher,
            runtime_scope: runtime_scope.clone(),
            parallel_enabled,
            clone_budget,
        }
    }

    pub(super) async fn dispatch_batch(&self, dispatch_batch: &mut [DispatchRequest]) {
        let frontier = batch_end_offset(dispatch_batch);
        let dispatchers = self.dispatcher.snapshot();
        if !self.parallel_enabled
            || dispatchers.is_empty()
            || dispatchers
                .iter()
                .any(|dispatcher| !dispatcher.supports_parallel_dispatch())
        {
            self.dispatch_serial(dispatch_batch, frontier).await;
            return;
        }

        let retained_bytes = estimate_batch_bytes(dispatch_batch).saturating_mul(dispatchers.len().saturating_add(1));
        let Ok(_permit) = self.clone_budget.try_acquire(retained_bytes, BudgetClass::Data) else {
            warn!(
                retained_bytes,
                lanes = dispatchers.len(),
                "Reput required-lane clone budget is full; preserving delivery with serial dispatch"
            );
            self.dispatch_serial(dispatch_batch, frontier).await;
            return;
        };

        let source = Arc::new(dispatch_batch.to_vec());
        join_all(
            dispatchers
                .iter()
                .cloned()
                .map(|dispatcher| execute_lane(dispatcher, Arc::clone(&source), self.runtime_scope.clone())),
        )
        .await;
        if let Some(frontier) = frontier {
            self.dispatcher.publish_frontier(frontier);
        }
    }

    /// Publishes a scanner-verified mapped-file BLANK boundary in lane order.
    pub(super) async fn dispatch_commit_log_blank(&self, blank_start_offset: i64, next_file_offset: i64) {
        for dispatcher in self.dispatcher.snapshot().iter().cloned() {
            execute_blank_lane(
                dispatcher,
                blank_start_offset,
                next_file_offset,
                self.runtime_scope.clone(),
            )
            .await;
        }
        self.dispatcher.publish_frontier(next_file_offset);
    }

    async fn dispatch_serial(&self, dispatch_batch: &mut [DispatchRequest], frontier: Option<i64>) {
        self.dispatcher.dispatch_batch_async(dispatch_batch).await;
        if let Some(frontier) = frontier {
            self.dispatcher.publish_frontier(frontier);
        }
    }

    #[cfg(test)]
    fn published_frontier(&self) -> i64 {
        self.dispatcher.published_frontier()
    }
}

async fn execute_lane(
    dispatcher: Arc<dyn CommitLogDispatcher>,
    source: Arc<Vec<DispatchRequest>>,
    runtime_scope: StoreRuntimeScope,
) {
    match dispatcher.dispatch_execution() {
        CommitLogDispatchExecution::Inline => {
            let mut batch = source.as_ref().clone();
            dispatcher.dispatch_batch(&mut batch);
        }
        CommitLogDispatchExecution::Async => {
            let mut batch = source.as_ref().clone();
            dispatcher.dispatch_batch_async(&mut batch).await;
        }
        CommitLogDispatchExecution::Blocking => loop {
            let owned_dispatcher = Arc::clone(&dispatcher);
            let owned_source = Arc::clone(&source);
            match runtime_scope
                .spawn_io("reput-required-lane", move || {
                    let mut batch = owned_source.as_ref().clone();
                    owned_dispatcher.dispatch_batch(&mut batch);
                })
                .await
            {
                Ok(()) => break,
                Err(error) => {
                    warn!(%error, "managed Reput blocking lane rejected work; retrying without publishing");
                    tokio::task::yield_now().await;
                }
            }
        },
    }
}

async fn execute_blank_lane(
    dispatcher: Arc<dyn CommitLogDispatcher>,
    blank_start_offset: i64,
    next_file_offset: i64,
    runtime_scope: StoreRuntimeScope,
) {
    match dispatcher.dispatch_execution() {
        CommitLogDispatchExecution::Inline => {
            dispatcher.dispatch_commit_log_blank(blank_start_offset, next_file_offset);
        }
        CommitLogDispatchExecution::Async => {
            dispatcher
                .dispatch_commit_log_blank_async(blank_start_offset, next_file_offset)
                .await;
        }
        CommitLogDispatchExecution::Blocking => loop {
            let owned_dispatcher = Arc::clone(&dispatcher);
            match runtime_scope
                .spawn_io("reput-blank-required-lane", move || {
                    owned_dispatcher.dispatch_commit_log_blank(blank_start_offset, next_file_offset);
                })
                .await
            {
                Ok(()) => break,
                Err(error) => {
                    warn!(%error, "managed Reput BLANK lane rejected work; retrying without publishing");
                    tokio::task::yield_now().await;
                }
            }
        },
    }
}

fn batch_end_offset(dispatch_batch: &[DispatchRequest]) -> Option<i64> {
    dispatch_batch
        .iter()
        .filter(|request| request.success)
        .filter_map(|request| {
            if request.next_reput_from_offset > request.commit_log_offset {
                Some(request.next_reput_from_offset)
            } else {
                let size = if request.buffer_size >= 0 {
                    request.buffer_size
                } else {
                    request.msg_size
                };
                (size > 0).then(|| request.commit_log_offset.saturating_add(i64::from(size)))
            }
        })
        .max()
}

fn estimate_batch_bytes(dispatch_batch: &[DispatchRequest]) -> usize {
    dispatch_batch.iter().fold(0usize, |total, request| {
        let properties_bytes = request.properties_map.as_ref().map_or(0, |properties| {
            properties.iter().fold(0usize, |size, (key, value)| {
                size.saturating_add(key.len()).saturating_add(value.len())
            })
        });
        total
            .saturating_add(size_of::<DispatchRequest>())
            .saturating_add(request.topic.len())
            .saturating_add(request.keys.len())
            .saturating_add(request.uniq_key.as_ref().map_or(0, |value| value.len()))
            .saturating_add(request.bit_map.as_ref().map_or(0, Vec::len))
            .saturating_add(request.offset_id.as_ref().map_or(0, |value| value.len()))
            .saturating_add(properties_bytes)
    })
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    use parking_lot::Mutex;
    use rocketmq_runtime::BudgetLimit;
    use rocketmq_runtime::FullPolicy;
    use tokio::sync::mpsc;
    use tokio::sync::Semaphore;

    use super::*;
    use crate::message_store::local_file_message_store::dispatch::CommitLogDispatcherDefault;

    struct GateDispatcher {
        name: &'static str,
        supports_parallel: bool,
        started: mpsc::UnboundedSender<(&'static str, i64)>,
        release: Arc<Semaphore>,
        observed: Arc<Mutex<Vec<i64>>>,
    }

    impl CommitLogDispatcher for GateDispatcher {
        fn dispatch(&self, dispatch_request: &mut DispatchRequest) {
            self.observed.lock().push(dispatch_request.commit_log_offset);
        }

        fn supports_parallel_dispatch(&self) -> bool {
            self.supports_parallel
        }

        fn dispatch_execution(&self) -> CommitLogDispatchExecution {
            CommitLogDispatchExecution::Async
        }

        fn dispatch_batch_async<'a>(
            &'a self,
            dispatch_requests: &'a mut [DispatchRequest],
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(async move {
                let offset = dispatch_requests[0].commit_log_offset;
                self.observed.lock().push(offset);
                let _ = self.started.send((self.name, offset));
                let permit = self.release.acquire().await.expect("test gate must remain open");
                permit.forget();
            })
        }
    }

    struct RecordingDispatcher {
        observed: Arc<Mutex<Vec<i64>>>,
    }

    impl CommitLogDispatcher for RecordingDispatcher {
        fn dispatch(&self, dispatch_request: &mut DispatchRequest) {
            self.observed.lock().push(dispatch_request.commit_log_offset);
        }

        fn supports_parallel_dispatch(&self) -> bool {
            true
        }

        fn dispatch_execution(&self) -> CommitLogDispatchExecution {
            CommitLogDispatchExecution::Async
        }

        fn dispatch_batch_async<'a>(
            &'a self,
            dispatch_requests: &'a mut [DispatchRequest],
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(async move {
                self.observed
                    .lock()
                    .extend(dispatch_requests.iter().map(|request| request.commit_log_offset));
            })
        }
    }

    fn dispatch_request(offset: i64) -> DispatchRequest {
        DispatchRequest {
            commit_log_offset: offset,
            msg_size: 5,
            buffer_size: -1,
            success: true,
            ..DispatchRequest::default()
        }
    }

    fn gate_dispatcher(
        name: &'static str,
        supports_parallel: bool,
        started: mpsc::UnboundedSender<(&'static str, i64)>,
    ) -> (Arc<GateDispatcher>, Arc<Semaphore>, Arc<Mutex<Vec<i64>>>) {
        let release = Arc::new(Semaphore::new(0));
        let observed = Arc::new(Mutex::new(Vec::new()));
        (
            Arc::new(GateDispatcher {
                name,
                supports_parallel,
                started,
                release: Arc::clone(&release),
                observed: Arc::clone(&observed),
            }),
            release,
            observed,
        )
    }

    #[tokio::test]
    async fn required_lanes_run_concurrently_and_publish_after_every_lane() {
        let scope = crate::runtime::test_scope("reput-parallel-required-lanes-test");
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (first, first_release, _) = gate_dispatcher("first", true, started_tx.clone());
        let (second, second_release, _) = gate_dispatcher("second", true, started_tx);
        let dispatcher = CommitLogDispatcherDefault::with_dispatchers(vec![first, second]);
        let pipeline = ReputDispatchPipeline::new(dispatcher.handle(), &scope, true, 1024 * 1024);
        let task_pipeline = pipeline.clone();
        let task = tokio::spawn(async move {
            let mut batch = vec![dispatch_request(10)];
            task_pipeline.dispatch_batch(&mut batch).await;
        });

        let mut started = vec![
            started_rx.recv().await.expect("first lane must start").0,
            started_rx.recv().await.expect("second lane must start").0,
        ];
        started.sort_unstable();
        assert_eq!(started, vec!["first", "second"]);
        assert_eq!(pipeline.published_frontier(), -1);

        first_release.add_permits(1);
        tokio::task::yield_now().await;
        assert_eq!(pipeline.published_frontier(), -1);
        second_release.add_permits(1);
        task.await.expect("pipeline task must finish");
        assert_eq!(pipeline.published_frontier(), 15);
    }

    #[tokio::test]
    async fn every_lane_observes_batches_in_commit_log_order() {
        let scope = crate::runtime::test_scope("reput-lane-order-test");
        let first_observed = Arc::new(Mutex::new(Vec::new()));
        let second_observed = Arc::new(Mutex::new(Vec::new()));
        let dispatcher = CommitLogDispatcherDefault::with_dispatchers(vec![
            Arc::new(RecordingDispatcher {
                observed: Arc::clone(&first_observed),
            }),
            Arc::new(RecordingDispatcher {
                observed: Arc::clone(&second_observed),
            }),
        ]);
        let pipeline = ReputDispatchPipeline::new(dispatcher.handle(), &scope, true, 1024 * 1024);

        let mut first_batch = vec![dispatch_request(10), dispatch_request(15)];
        pipeline.dispatch_batch(&mut first_batch).await;
        let mut second_batch = vec![dispatch_request(20), dispatch_request(25)];
        pipeline.dispatch_batch(&mut second_batch).await;

        assert_eq!(*first_observed.lock(), vec![10, 15, 20, 25]);
        assert_eq!(*second_observed.lock(), vec![10, 15, 20, 25]);
        assert_eq!(pipeline.published_frontier(), 30);
    }

    #[tokio::test]
    async fn unknown_dispatcher_preserves_serial_semantics() {
        let scope = crate::runtime::test_scope("reput-serial-extension-test");
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (first, first_release, _) = gate_dispatcher("first", false, started_tx.clone());
        let (second, second_release, _) = gate_dispatcher("second", true, started_tx);
        let dispatcher = CommitLogDispatcherDefault::with_dispatchers(vec![first, second]);
        let pipeline = ReputDispatchPipeline::new(dispatcher.handle(), &scope, true, 1024 * 1024);
        let task_pipeline = pipeline.clone();
        let task = tokio::spawn(async move {
            let mut batch = vec![dispatch_request(30)];
            task_pipeline.dispatch_batch(&mut batch).await;
        });

        assert_eq!(started_rx.recv().await, Some(("first", 30)));
        tokio::task::yield_now().await;
        assert!(matches!(started_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
        first_release.add_permits(1);
        assert_eq!(started_rx.recv().await, Some(("second", 30)));
        second_release.add_permits(1);
        task.await.expect("pipeline task must finish");
        assert_eq!(pipeline.published_frontier(), 35);
    }

    #[tokio::test]
    async fn clone_budget_rejection_falls_back_without_dropping_lanes() {
        let scope = crate::runtime::test_scope("reput-clone-budget-test");
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (first, first_release, first_observed) = gate_dispatcher("first", true, started_tx.clone());
        let (second, second_release, second_observed) = gate_dispatcher("second", true, started_tx);
        let dispatcher = CommitLogDispatcherDefault::with_dispatchers(vec![first, second]);
        let clone_budget = scope
            .resource_budget()
            .child("reput-tiny-clone-budget", BudgetLimit::new(1, 1, FullPolicy::Reject))
            .expect("tiny test budget must fit Store budget");
        let pipeline = ReputDispatchPipeline::with_clone_budget(dispatcher.handle(), &scope, true, clone_budget);
        let task_pipeline = pipeline.clone();
        let task = tokio::spawn(async move {
            let mut batch = vec![dispatch_request(40)];
            task_pipeline.dispatch_batch(&mut batch).await;
        });

        assert_eq!(started_rx.recv().await, Some(("first", 40)));
        tokio::task::yield_now().await;
        assert!(matches!(started_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
        first_release.add_permits(1);
        assert_eq!(started_rx.recv().await, Some(("second", 40)));
        second_release.add_permits(1);
        task.await.expect("pipeline task must finish");

        assert_eq!(*first_observed.lock(), vec![40]);
        assert_eq!(*second_observed.lock(), vec![40]);
        assert_eq!(pipeline.published_frontier(), 45);
    }
}
