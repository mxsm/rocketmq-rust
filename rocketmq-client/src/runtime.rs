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
use std::io;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Mutex;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::BlockingPoolPolicy;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

static SHARED_FALLBACK: OnceLock<ClientSharedFallbackRegistry> = OnceLock::new();
static CLIENT_BLOCKING: OnceLock<BlockingExecutor> = OnceLock::new();

pub(crate) fn spawn_client_task<F>(task_name: &'static str, task: F) -> io::Result<JoinHandle<()>>
where
    F: Future<Output = ()> + Send + 'static,
{
    spawn_client_task_on(task_name, None, task)
}

pub(crate) fn spawn_client_task_on<F>(
    task_name: &'static str,
    executor: Option<&Handle>,
    task: F,
) -> io::Result<JoinHandle<()>>
where
    F: Future<Output = ()> + Send + 'static,
{
    if let Some(executor) = executor {
        return Ok(executor.spawn(task));
    }

    if let Ok(handle) = Handle::try_current() {
        return Ok(handle.spawn(task));
    }

    shared_fallback()?.spawn(task_name, task)
}

pub(crate) fn spawn_detached_client_task<F>(task_name: &'static str, task: F) -> io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    drop(spawn_client_task(task_name, task)?);
    Ok(())
}

pub(crate) async fn spawn_client_blocking_io<F, R>(task_name: &'static str, task: F) -> io::Result<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    client_blocking_executor()?
        .spawn_io(task_name, task)
        .await
        .map_err(io::Error::other)
}

pub(crate) fn spawn_delayed_client_action<F>(task_name: &'static str, delay: std::time::Duration, action: F)
where
    F: FnOnce() + Send + 'static,
{
    if delay.is_zero() {
        action();
        return;
    }

    if let Err(error) = spawn_detached_client_task(task_name, async move {
        tokio::time::sleep(delay).await;
        action();
    }) {
        tracing::error!(%error, task_name, "failed to spawn delayed client action");
    }
}

#[cfg(test)]
pub(crate) fn shared_fallback_snapshot() -> Option<ClientSharedFallbackSnapshot> {
    SHARED_FALLBACK.get().map(ClientSharedFallbackRegistry::snapshot)
}

pub(crate) fn shutdown_shared_fallback(timeout: Duration) -> io::Result<Option<ShutdownReport>> {
    match SHARED_FALLBACK.get() {
        Some(registry) => registry.shutdown_explicit(timeout),
        None => Ok(None),
    }
}

fn shared_fallback() -> io::Result<ClientSharedFallbackLease> {
    shared_fallback_registry().acquire()
}

fn shared_fallback_registry() -> &'static ClientSharedFallbackRegistry {
    SHARED_FALLBACK.get_or_init(ClientSharedFallbackRegistry::new)
}

fn client_blocking_executor() -> io::Result<BlockingExecutor> {
    if let Some(executor) = CLIENT_BLOCKING.get() {
        return Ok(executor.clone());
    }

    let handle = Handle::try_current().map_err(io::Error::other)?;
    let group = TaskGroup::root("rocketmq-client.blocking", RuntimeHandle::new(handle));
    let executor = BlockingExecutor::new(
        BlockingPoolPolicy {
            name: "rocketmq-client.blocking".to_string(),
            ..BlockingPoolPolicy::default()
        },
        group.child("rocketmq-client.blocking-reaper"),
    )
    .map_err(io::Error::other)?;

    if CLIENT_BLOCKING.set(executor.clone()).is_err() {
        return Ok(CLIENT_BLOCKING
            .get()
            .expect("client blocking executor must be initialized")
            .clone());
    }
    Ok(executor)
}

struct ClientSharedFallbackRegistry {
    state: Mutex<ClientSharedFallbackState>,
    next_generation: AtomicUsize,
    acquire_count: AtomicUsize,
    runtime_created: AtomicUsize,
    runtime_reused: AtomicUsize,
    submitted_tasks: AtomicUsize,
    active_tasks: AtomicUsize,
    active_leases: AtomicUsize,
    idle_shutdowns: AtomicUsize,
    explicit_shutdowns: AtomicUsize,
    idle_reaper_started: AtomicBool,
    idle_reaper_starts: AtomicUsize,
    idle_signal: Arc<(StdMutex<ClientFallbackIdleState>, Condvar)>,
}

#[derive(Default)]
struct ClientSharedFallbackState {
    runtime: Option<Arc<ClientSharedFallbackRuntime>>,
    explicit_shutdown: bool,
}

#[derive(Default)]
struct ClientFallbackIdleState {
    requested_generation: Option<usize>,
    deadline: Option<Instant>,
}

struct ClientSharedFallbackRuntime {
    owner: RuntimeOwner,
    generation: usize,
}

struct ClientSharedFallbackLease {
    registry: &'static ClientSharedFallbackRegistry,
    runtime: Arc<ClientSharedFallbackRuntime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ClientSharedFallbackSnapshot {
    pub acquire_count: usize,
    pub runtime_created: usize,
    pub runtime_reused: usize,
    pub submitted_tasks: usize,
    pub active_tasks: usize,
    pub active_leases: usize,
    pub runtime_generation: usize,
    pub runtime_available: bool,
    pub idle_shutdowns: usize,
    pub explicit_shutdowns: usize,
    pub idle_reaper_starts: usize,
}

impl ClientSharedFallbackRegistry {
    fn new() -> Self {
        Self {
            state: Mutex::new(ClientSharedFallbackState::default()),
            next_generation: AtomicUsize::new(0),
            acquire_count: AtomicUsize::new(0),
            runtime_created: AtomicUsize::new(0),
            runtime_reused: AtomicUsize::new(0),
            submitted_tasks: AtomicUsize::new(0),
            active_tasks: AtomicUsize::new(0),
            active_leases: AtomicUsize::new(0),
            idle_shutdowns: AtomicUsize::new(0),
            explicit_shutdowns: AtomicUsize::new(0),
            idle_reaper_started: AtomicBool::new(false),
            idle_reaper_starts: AtomicUsize::new(0),
            idle_signal: Arc::new((StdMutex::new(ClientFallbackIdleState::default()), Condvar::new())),
        }
    }

    fn acquire(&'static self) -> io::Result<ClientSharedFallbackLease> {
        self.acquire_count.fetch_add(1, Ordering::Relaxed);
        let runtime = {
            let mut state = self.state.lock();
            if state.explicit_shutdown {
                return Err(io::Error::other(
                    "client fallback runtime has been explicitly shut down",
                ));
            }

            let runtime = match &state.runtime {
                Some(runtime) => {
                    self.runtime_reused.fetch_add(1, Ordering::Relaxed);
                    runtime.clone()
                }
                None => {
                    let generation = self.next_generation.fetch_add(1, Ordering::AcqRel) + 1;
                    let runtime = Arc::new(ClientSharedFallbackRuntime::new(generation)?);
                    self.runtime_created.fetch_add(1, Ordering::Relaxed);
                    state.runtime = Some(runtime.clone());
                    runtime
                }
            };

            self.active_leases.fetch_add(1, Ordering::AcqRel);
            runtime
        };
        Ok(ClientSharedFallbackLease {
            registry: self,
            runtime,
        })
    }

    fn snapshot(&self) -> ClientSharedFallbackSnapshot {
        let state = self.state.lock();
        let runtime_generation = state
            .runtime
            .as_ref()
            .map(|runtime| runtime.generation)
            .unwrap_or_default();

        ClientSharedFallbackSnapshot {
            acquire_count: self.acquire_count.load(Ordering::Relaxed),
            runtime_created: self.runtime_created.load(Ordering::Relaxed),
            runtime_reused: self.runtime_reused.load(Ordering::Relaxed),
            submitted_tasks: self.submitted_tasks.load(Ordering::Relaxed),
            active_tasks: self.active_tasks.load(Ordering::Relaxed),
            active_leases: self.active_leases.load(Ordering::Relaxed),
            runtime_generation,
            runtime_available: state.runtime.is_some(),
            idle_shutdowns: self.idle_shutdowns.load(Ordering::Relaxed),
            explicit_shutdowns: self.explicit_shutdowns.load(Ordering::Relaxed),
            idle_reaper_starts: self.idle_reaper_starts.load(Ordering::Relaxed),
        }
    }

    fn shutdown_explicit(&'static self, timeout: Duration) -> io::Result<Option<ShutdownReport>> {
        let runtime = {
            let mut state = self.state.lock();
            state.explicit_shutdown = true;
            state.runtime.take()
        };

        runtime
            .map(|runtime| {
                self.explicit_shutdowns.fetch_add(1, Ordering::Relaxed);
                shutdown_runtime(runtime, timeout)
            })
            .transpose()
    }

    fn schedule_idle_shutdown(&'static self, generation: usize) {
        self.ensure_idle_reaper_started();

        let (lock, condvar) = &*self.idle_signal;
        let mut state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        state.requested_generation = Some(generation);
        state.deadline = Some(Instant::now() + fallback_idle_timeout());
        condvar.notify_one();
    }

    fn ensure_idle_reaper_started(&'static self) {
        if self
            .idle_reaper_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let registry = self;
        if let Err(error) = std::thread::Builder::new()
            .name("rocketmq-client-fallback-idle-reaper".to_string())
            .spawn(move || {
                registry.run_idle_reaper();
            })
        {
            self.idle_reaper_started.store(false, Ordering::Release);
            tracing::warn!(%error, "failed to start client fallback idle reaper");
        } else {
            self.idle_reaper_starts.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn run_idle_reaper(&'static self) {
        loop {
            let generation = self.wait_for_idle_deadline();
            self.shutdown_idle_if_current(generation);
        }
    }

    fn wait_for_idle_deadline(&self) -> usize {
        let (lock, condvar) = &*self.idle_signal;
        let mut state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        loop {
            let Some(deadline) = state.deadline else {
                state = condvar.wait(state).unwrap_or_else(|poisoned| poisoned.into_inner());
                continue;
            };

            let now = Instant::now();
            if now >= deadline {
                state.deadline = None;
                return state
                    .requested_generation
                    .take()
                    .expect("idle deadline requires a runtime generation");
            }

            let wait_duration = deadline.saturating_duration_since(now);
            let (next_state, _timeout) = condvar
                .wait_timeout(state, wait_duration)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next_state;
        }
    }

    fn shutdown_idle_if_current(&'static self, generation: usize) {
        let runtime = {
            let mut state = self.state.lock();
            if self.active_leases.load(Ordering::Acquire) != 0 {
                return;
            }

            let Some(runtime) = state.runtime.as_ref() else {
                return;
            };
            if runtime.generation != generation || state.explicit_shutdown {
                return;
            }
            state.runtime.take()
        };

        if let Some(runtime) = runtime {
            self.idle_shutdowns.fetch_add(1, Ordering::Relaxed);
            if let Err(error) = shutdown_runtime(runtime, fallback_shutdown_timeout()) {
                tracing::warn!(%error, "failed to shut down idle client fallback runtime");
            }
        }
    }
}

impl ClientSharedFallbackRuntime {
    fn new(generation: usize) -> io::Result<Self> {
        let parallelism = std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(2);
        let worker_threads = parallelism.clamp(2, 4);
        let config = RuntimeConfig {
            worker_threads,
            max_blocking_threads: worker_threads * 2,
            thread_name: "rocketmq-client-fallback".to_string(),
            ..RuntimeConfig::default()
        };
        let owner = RuntimeOwner::new(config).map_err(io::Error::other)?;
        Ok(Self { owner, generation })
    }
}

impl ClientSharedFallbackLease {
    fn spawn<F>(self, task_name: &'static str, task: F) -> io::Result<JoinHandle<()>>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let registry = self.registry;
        registry.submitted_tasks.fetch_add(1, Ordering::Relaxed);
        registry.active_tasks.fetch_add(1, Ordering::Relaxed);
        let runtime = self.runtime.clone();
        let result = runtime
            .owner
            .context()
            .root_group()
            .spawn_service_with_handle(task_name, async move {
                let _guard = ActiveTaskGuard {
                    registry,
                    task_name,
                    _lease: self,
                };
                task.await;
            });

        match result {
            Ok((_task_id, handle)) => Ok(handle),
            Err(error) => {
                registry.active_tasks.fetch_sub(1, Ordering::Relaxed);
                Err(io::Error::other(error))
            }
        }
    }
}

impl Drop for ClientSharedFallbackLease {
    fn drop(&mut self) {
        let previous = self.registry.active_leases.fetch_sub(1, Ordering::AcqRel);
        if previous == 1 {
            self.registry.schedule_idle_shutdown(self.runtime.generation);
        }
    }
}

struct ActiveTaskGuard {
    registry: &'static ClientSharedFallbackRegistry,
    task_name: &'static str,
    _lease: ClientSharedFallbackLease,
}

impl Drop for ActiveTaskGuard {
    fn drop(&mut self) {
        self.registry.active_tasks.fetch_sub(1, Ordering::Relaxed);
        tracing::debug!(task_name = self.task_name, "client fallback task finished");
    }
}

fn shutdown_runtime(runtime: Arc<ClientSharedFallbackRuntime>, timeout: Duration) -> io::Result<ShutdownReport> {
    match Arc::try_unwrap(runtime) {
        Ok(runtime) => runtime
            .owner
            .shutdown_runtime_blocking_with_timeout(timeout)
            .map_err(io::Error::other),
        Err(runtime) => {
            let report = runtime.owner.block_on(runtime.owner.context().shutdown_tasks(timeout));
            report.log_if_unhealthy();
            match Arc::try_unwrap(runtime) {
                Ok(runtime) => {
                    runtime
                        .owner
                        .shutdown_runtime_blocking_with_timeout(timeout)
                        .map_err(io::Error::other)?;
                }
                Err(_) => {
                    tracing::warn!(
                        "client fallback tasks were shut down but outstanding leases still hold the runtime owner"
                    );
                }
            }
            Ok(report)
        }
    }
}

fn fallback_shutdown_timeout() -> Duration {
    Duration::from_secs(5)
}

fn fallback_idle_timeout() -> Duration {
    #[cfg(test)]
    {
        Duration::from_millis(50)
    }
    #[cfg(not(test))]
    {
        Duration::from_secs(30)
    }
}

#[cfg(test)]
mod tests {
    use std::future;
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;

    #[test]
    fn shared_fallback_runtime_reuses_one_runtime_for_multiple_tasks() {
        let before = shared_fallback_snapshot()
            .map(|snapshot| snapshot.submitted_tasks)
            .unwrap_or_default();
        let (tx, rx) = mpsc::channel();

        for _ in 0..8 {
            let tx = tx.clone();
            spawn_detached_client_task("client-runtime-test", async move {
                tx.send(()).expect("test receiver should be alive");
            })
            .expect("fallback task should spawn");
        }
        drop(tx);

        for _ in 0..8 {
            rx.recv_timeout(Duration::from_secs(2))
                .expect("fallback task should complete");
        }

        let snapshot = shared_fallback_snapshot().expect("fallback runtime should exist");
        assert!(snapshot.submitted_tasks >= before + 8);
        assert!(snapshot.active_tasks <= snapshot.submitted_tasks);
    }

    #[test]
    fn fallback_registry_tracks_leases_and_rejects_after_explicit_shutdown() {
        let registry = Box::leak(Box::new(ClientSharedFallbackRegistry::new()));
        let lease = registry.acquire().expect("fallback runtime should be created");

        let snapshot = registry.snapshot();
        assert_eq!(snapshot.acquire_count, 1);
        assert_eq!(snapshot.runtime_created, 1);
        assert_eq!(snapshot.runtime_reused, 0);
        assert_eq!(snapshot.active_leases, 1);
        assert!(snapshot.runtime_available);
        assert_eq!(snapshot.runtime_generation, 1);

        drop(lease);
        assert_eq!(registry.snapshot().active_leases, 0);

        let report = registry
            .shutdown_explicit(Duration::from_secs(1))
            .expect("explicit shutdown should complete");
        assert!(report.expect("runtime should be shut down").is_healthy());
        let error = match registry.acquire() {
            Ok(_) => panic!("explicit shutdown must reject new fallback leases"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("explicitly shut down"));
        assert_eq!(registry.snapshot().explicit_shutdowns, 1);
    }

    #[test]
    fn fallback_spawned_task_is_tracked_by_runtime_context_shutdown() {
        let registry = Box::leak(Box::new(ClientSharedFallbackRegistry::new()));
        let lease = registry.acquire().expect("fallback runtime should be created");
        let runtime = lease.runtime.clone();
        let (tx, rx) = mpsc::channel();

        let handle = lease
            .spawn("tracked-fallback-task", async move {
                tx.send(()).expect("test receiver should be alive");
                future::pending::<()>().await;
            })
            .expect("fallback task should spawn");

        rx.recv_timeout(Duration::from_secs(2))
            .expect("fallback task should start");

        let report = runtime
            .owner
            .block_on(runtime.owner.context().shutdown_tasks(Duration::from_millis(20)));

        assert_eq!(report.aborted, 1, "{}", report.to_json());
        assert_eq!(report.leaked, 0, "{}", report.to_json());
        assert_eq!(registry.snapshot().active_tasks, 0);
        assert_eq!(registry.snapshot().active_leases, 0);
        drop(handle);
    }

    #[test]
    fn fallback_registry_explicit_shutdown_aborts_tracked_task() {
        let registry = Box::leak(Box::new(ClientSharedFallbackRegistry::new()));
        let lease = registry.acquire().expect("fallback runtime should be created");
        let (tx, rx) = mpsc::channel();

        let handle = lease
            .spawn("explicit-shutdown-task", async move {
                tx.send(()).expect("test receiver should be alive");
                future::pending::<()>().await;
            })
            .expect("fallback task should spawn");

        rx.recv_timeout(Duration::from_secs(2))
            .expect("fallback task should start");

        let report = registry
            .shutdown_explicit(Duration::from_millis(20))
            .expect("explicit shutdown should return a report")
            .expect("fallback runtime should exist");

        assert_eq!(report.aborted, 1, "{}", report.to_json());
        assert_eq!(report.leaked, 0, "{}", report.to_json());
        assert_eq!(report.timed_out, 1, "{}", report.to_json());
        assert_eq!(registry.snapshot().active_tasks, 0);
        assert_eq!(registry.snapshot().active_leases, 0);
        assert!(!registry.snapshot().runtime_available);
        drop(handle);
    }

    #[test]
    fn fallback_registry_idle_reaper_shuts_down_runtime_after_idle_timeout() {
        let registry = Box::leak(Box::new(ClientSharedFallbackRegistry::new()));
        let lease = registry.acquire().expect("fallback runtime should be created");
        assert!(registry.snapshot().runtime_available);

        drop(lease);

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let snapshot = registry.snapshot();
            if !snapshot.runtime_available {
                assert_eq!(snapshot.idle_shutdowns, 1);
                assert_eq!(snapshot.idle_reaper_starts, 1);
                break;
            }

            assert!(
                Instant::now() < deadline,
                "fallback runtime did not shut down after idle timeout"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn fallback_registry_reuses_single_idle_reaper_for_repeated_idle_requests() {
        let registry = Box::leak(Box::new(ClientSharedFallbackRegistry::new()));
        let first = registry.acquire().expect("fallback runtime should be created");
        drop(first);

        std::thread::sleep(Duration::from_millis(10));

        let second = registry.acquire().expect("fallback runtime should be reused");
        let snapshot = registry.snapshot();
        assert_eq!(snapshot.runtime_created, 1);
        assert_eq!(snapshot.runtime_reused, 1);
        assert_eq!(snapshot.idle_reaper_starts, 1);

        drop(second);

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let snapshot = registry.snapshot();
            if !snapshot.runtime_available {
                assert_eq!(snapshot.idle_shutdowns, 1);
                assert_eq!(snapshot.idle_reaper_starts, 1);
                break;
            }

            assert!(
                Instant::now() < deadline,
                "fallback runtime did not shut down after second idle request"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
