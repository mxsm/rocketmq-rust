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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::BlockingExecutorSnapshot;
use rocketmq_runtime::BlockingPoolPolicy;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::RuntimeResult;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use serde::Serialize;

static AUTH_SYNC_RUNTIME: OnceLock<Result<RuntimeOwner, String>> = OnceLock::new();
static AUTH_SYNC_BRIDGE_CALLS: AtomicU64 = AtomicU64::new(0);
static AUTH_SYNC_BRIDGE_MULTI_THREAD_BLOCK_IN_PLACE: AtomicU64 = AtomicU64::new(0);
static AUTH_SYNC_BRIDGE_CURRENT_THREAD_HANDOFFS: AtomicU64 = AtomicU64::new(0);
static AUTH_SYNC_BRIDGE_FALLBACK_RUNTIME_CALLS: AtomicU64 = AtomicU64::new(0);
static AUTH_SYNC_RUNTIME_ACQUIRES: AtomicU64 = AtomicU64::new(0);
static AUTH_SYNC_RUNTIME_CREATED: AtomicU64 = AtomicU64::new(0);
static AUTH_SYNC_RUNTIME_REUSED: AtomicU64 = AtomicU64::new(0);
static AUTH_BLOCKING_EXECUTOR_CREATED: AtomicU64 = AtomicU64::new(0);
static AUTH_BLOCKING_EXECUTOR_SHUTDOWN_REQUESTS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
pub(crate) static AUTH_RUNTIME_BRIDGE_TEST_LOCK: Mutex<()> = Mutex::new(());

#[cfg(test)]
pub(crate) fn auth_runtime_bridge_test_guard() -> std::sync::MutexGuard<'static, ()> {
    AUTH_RUNTIME_BRIDGE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub(crate) struct AuthSyncBridgeSnapshot {
    pub sync_bridge_calls: u64,
    pub multi_thread_block_in_place: u64,
    pub current_thread_handoffs: u64,
    pub fallback_runtime_calls: u64,
    pub shared_runtime_acquires: u64,
    pub shared_runtime_created: u64,
    pub shared_runtime_reused: u64,
    pub shared_runtime_available: bool,
    pub blocking_executor_creations: u64,
    pub blocking_executor_shutdown_requests: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AuthBlockingExecutor {
    inner: Arc<OnceLock<AuthBlockingExecutorState>>,
}

#[derive(Clone, Debug)]
struct AuthBlockingExecutorState {
    executor: BlockingExecutor,
    task_group: TaskGroup,
}

impl AuthBlockingExecutor {
    pub(crate) async fn spawn_io<F, R>(&self, name: &'static str, operation: F) -> RuntimeResult<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.executor()?.spawn_io(name, operation).await
    }

    pub(crate) fn snapshot(&self) -> Option<BlockingExecutorSnapshot> {
        self.inner.get().map(|state| state.executor.snapshot())
    }

    pub(crate) async fn shutdown_with_report(&self, timeout: Duration) -> Option<ShutdownReport> {
        AUTH_BLOCKING_EXECUTOR_SHUTDOWN_REQUESTS.fetch_add(1, Ordering::Relaxed);
        let state = self.inner.get()?.clone();
        let mut report = state.task_group.shutdown(timeout).await;
        report.merge_blocking(state.executor.snapshot());
        Some(report)
    }

    fn executor(&self) -> RuntimeResult<BlockingExecutor> {
        if let Some(state) = self.inner.get() {
            return Ok(state.executor.clone());
        }

        let handle = tokio::runtime::Handle::try_current().map_err(|_error| RuntimeError::NoCurrentRuntime)?;
        let group = TaskGroup::root("rocketmq-auth.blocking", RuntimeHandle::new(handle));
        let executor = BlockingExecutor::new(
            BlockingPoolPolicy {
                name: "rocketmq-auth.blocking".to_string(),
                ..BlockingPoolPolicy::default()
            },
            group.child("rocketmq-auth.blocking-reaper"),
        )?;
        let state = AuthBlockingExecutorState {
            executor: executor.clone(),
            task_group: group,
        };

        if self.inner.set(state).is_err() {
            return Ok(self
                .inner
                .get()
                .expect("auth blocking executor must be initialized")
                .executor
                .clone());
        }
        AUTH_BLOCKING_EXECUTOR_CREATED.fetch_add(1, Ordering::Relaxed);
        Ok(executor)
    }
}

pub(crate) fn block_on_sync_bridge<F, Fut, T, E, BuildError, ThreadPanic>(
    future: F,
    build_error: BuildError,
    thread_panic: ThreadPanic,
) -> Result<T, E>
where
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = Result<T, E>>,
    T: Send,
    E: Send,
    BuildError: FnOnce(String) -> E + Send,
    ThreadPanic: FnOnce() -> E + Send,
{
    AUTH_SYNC_BRIDGE_CALLS.fetch_add(1, Ordering::Relaxed);
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            AUTH_SYNC_BRIDGE_MULTI_THREAD_BLOCK_IN_PLACE.fetch_add(1, Ordering::Relaxed);
            tokio::task::block_in_place(|| handle.block_on(future()))
        }
        Ok(_) => std::thread::scope(|scope| {
            AUTH_SYNC_BRIDGE_CURRENT_THREAD_HANDOFFS.fetch_add(1, Ordering::Relaxed);
            scope
                .spawn(|| {
                    let runtime = auth_sync_runtime().map_err(build_error)?;
                    runtime.block_on(future())
                })
                .join()
                .map_err(|_| thread_panic())?
        }),
        Err(_) => {
            AUTH_SYNC_BRIDGE_FALLBACK_RUNTIME_CALLS.fetch_add(1, Ordering::Relaxed);
            let runtime = auth_sync_runtime().map_err(build_error)?;
            runtime.block_on(future())
        }
    }
}

pub(crate) fn auth_sync_bridge_snapshot() -> AuthSyncBridgeSnapshot {
    AuthSyncBridgeSnapshot {
        sync_bridge_calls: AUTH_SYNC_BRIDGE_CALLS.load(Ordering::Relaxed),
        multi_thread_block_in_place: AUTH_SYNC_BRIDGE_MULTI_THREAD_BLOCK_IN_PLACE.load(Ordering::Relaxed),
        current_thread_handoffs: AUTH_SYNC_BRIDGE_CURRENT_THREAD_HANDOFFS.load(Ordering::Relaxed),
        fallback_runtime_calls: AUTH_SYNC_BRIDGE_FALLBACK_RUNTIME_CALLS.load(Ordering::Relaxed),
        shared_runtime_acquires: AUTH_SYNC_RUNTIME_ACQUIRES.load(Ordering::Relaxed),
        shared_runtime_created: AUTH_SYNC_RUNTIME_CREATED.load(Ordering::Relaxed),
        shared_runtime_reused: AUTH_SYNC_RUNTIME_REUSED.load(Ordering::Relaxed),
        shared_runtime_available: AUTH_SYNC_RUNTIME.get().is_some(),
        blocking_executor_creations: AUTH_BLOCKING_EXECUTOR_CREATED.load(Ordering::Relaxed),
        blocking_executor_shutdown_requests: AUTH_BLOCKING_EXECUTOR_SHUTDOWN_REQUESTS.load(Ordering::Relaxed),
    }
}

#[cfg(test)]
pub(crate) fn reset_auth_sync_bridge_counters_for_tests() -> AuthSyncBridgeSnapshot {
    AUTH_SYNC_BRIDGE_CALLS.store(0, Ordering::Relaxed);
    AUTH_SYNC_BRIDGE_MULTI_THREAD_BLOCK_IN_PLACE.store(0, Ordering::Relaxed);
    AUTH_SYNC_BRIDGE_CURRENT_THREAD_HANDOFFS.store(0, Ordering::Relaxed);
    AUTH_SYNC_BRIDGE_FALLBACK_RUNTIME_CALLS.store(0, Ordering::Relaxed);
    AUTH_SYNC_RUNTIME_ACQUIRES.store(0, Ordering::Relaxed);
    AUTH_SYNC_RUNTIME_CREATED.store(0, Ordering::Relaxed);
    AUTH_SYNC_RUNTIME_REUSED.store(0, Ordering::Relaxed);
    AUTH_BLOCKING_EXECUTOR_CREATED.store(0, Ordering::Relaxed);
    AUTH_BLOCKING_EXECUTOR_SHUTDOWN_REQUESTS.store(0, Ordering::Relaxed);
    AuthSyncBridgeSnapshot {
        shared_runtime_available: AUTH_SYNC_RUNTIME.get().is_some(),
        ..AuthSyncBridgeSnapshot::default()
    }
}

fn auth_sync_runtime() -> Result<&'static RuntimeOwner, String> {
    AUTH_SYNC_RUNTIME_ACQUIRES.fetch_add(1, Ordering::Relaxed);
    let created_before = AUTH_SYNC_RUNTIME_CREATED.load(Ordering::Acquire);
    match AUTH_SYNC_RUNTIME.get_or_init(|| {
        AUTH_SYNC_RUNTIME_CREATED.fetch_add(1, Ordering::AcqRel);
        let parallelism = std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(2);
        let worker_threads = parallelism.clamp(2, 4);
        RuntimeOwner::new(RuntimeConfig {
            worker_threads,
            max_blocking_threads: worker_threads * 2,
            thread_name: "rocketmq-auth-sync".to_string(),
            shutdown_timeout: Duration::from_secs(10),
            ..RuntimeConfig::default()
        })
        .map_err(|error| error.to_string())
    }) {
        Ok(runtime) => {
            if created_before > 0 && AUTH_SYNC_RUNTIME_CREATED.load(Ordering::Acquire) == created_before {
                AUTH_SYNC_RUNTIME_REUSED.fetch_add(1, Ordering::Relaxed);
            }
            Ok(runtime)
        }
        Err(error) => {
            if created_before > 0 && AUTH_SYNC_RUNTIME_CREATED.load(Ordering::Acquire) == created_before {
                AUTH_SYNC_RUNTIME_REUSED.fetch_add(1, Ordering::Relaxed);
            }
            Err(error.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn auth_blocking_executor_snapshot_counts_creation_and_shutdown_request() {
        let executor = AuthBlockingExecutor::default();
        let before = auth_sync_bridge_snapshot();
        assert!(executor.snapshot().is_none());

        let value = executor
            .spawn_io("auth.blocking.counter", || 42usize)
            .await
            .expect("auth blocking task should complete");
        assert_eq!(value, 42);

        let active_snapshot = executor
            .snapshot()
            .expect("auth blocking executor snapshot should exist after first task");
        assert_eq!(active_snapshot.name, "rocketmq-auth.blocking");
        assert_eq!(active_snapshot.blocking_still_running, 0);

        let report = executor
            .shutdown_with_report(Duration::from_secs(1))
            .await
            .expect("initialized auth blocking executor should return shutdown report");
        assert_eq!(report.name, "rocketmq-auth.blocking");
        assert!(report.is_healthy(), "{}", report.to_json());

        let after = auth_sync_bridge_snapshot();
        assert!(
            after
                .blocking_executor_creations
                .saturating_sub(before.blocking_executor_creations)
                >= 1
        );
        assert!(
            after
                .blocking_executor_shutdown_requests
                .saturating_sub(before.blocking_executor_shutdown_requests)
                >= 1
        );
    }
}
