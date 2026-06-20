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
use std::sync::OnceLock;
use std::time::Duration;

use parking_lot::Mutex;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::BlockingPoolPolicy;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::RuntimeOwner;
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
    submitted_tasks: AtomicUsize,
    active_tasks: AtomicUsize,
    active_leases: AtomicUsize,
    idle_shutdowns: AtomicUsize,
    explicit_shutdowns: AtomicUsize,
    idle_check_scheduled: AtomicBool,
}

#[derive(Default)]
struct ClientSharedFallbackState {
    runtime: Option<Arc<ClientSharedFallbackRuntime>>,
    explicit_shutdown: bool,
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
    pub submitted_tasks: usize,
    pub active_tasks: usize,
    pub active_leases: usize,
    pub runtime_generation: usize,
    pub runtime_available: bool,
    pub idle_shutdowns: usize,
    pub explicit_shutdowns: usize,
}

impl ClientSharedFallbackRegistry {
    fn new() -> Self {
        Self {
            state: Mutex::new(ClientSharedFallbackState::default()),
            next_generation: AtomicUsize::new(0),
            submitted_tasks: AtomicUsize::new(0),
            active_tasks: AtomicUsize::new(0),
            active_leases: AtomicUsize::new(0),
            idle_shutdowns: AtomicUsize::new(0),
            explicit_shutdowns: AtomicUsize::new(0),
            idle_check_scheduled: AtomicBool::new(false),
        }
    }

    fn acquire(&'static self) -> io::Result<ClientSharedFallbackLease> {
        let runtime = {
            let mut state = self.state.lock();
            if state.explicit_shutdown {
                return Err(io::Error::other(
                    "client fallback runtime has been explicitly shut down",
                ));
            }

            match &state.runtime {
                Some(runtime) => runtime.clone(),
                None => {
                    let generation = self.next_generation.fetch_add(1, Ordering::AcqRel) + 1;
                    let runtime = Arc::new(ClientSharedFallbackRuntime::new(generation)?);
                    state.runtime = Some(runtime.clone());
                    runtime
                }
            }
        };

        self.active_leases.fetch_add(1, Ordering::AcqRel);
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
            submitted_tasks: self.submitted_tasks.load(Ordering::Relaxed),
            active_tasks: self.active_tasks.load(Ordering::Relaxed),
            active_leases: self.active_leases.load(Ordering::Relaxed),
            runtime_generation,
            runtime_available: state.runtime.is_some(),
            idle_shutdowns: self.idle_shutdowns.load(Ordering::Relaxed),
            explicit_shutdowns: self.explicit_shutdowns.load(Ordering::Relaxed),
        }
    }

    fn shutdown_explicit(&'static self) {
        let runtime = {
            let mut state = self.state.lock();
            state.explicit_shutdown = true;
            state.runtime.take()
        };

        if let Some(runtime) = runtime {
            self.explicit_shutdowns.fetch_add(1, Ordering::Relaxed);
            shutdown_runtime(runtime);
        }
    }

    fn schedule_idle_shutdown(&'static self, generation: usize) {
        if self
            .idle_check_scheduled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let registry = self;
        if let Err(error) = std::thread::Builder::new()
            .name("rocketmq-client-fallback-idle-shutdown".to_string())
            .spawn(move || {
                std::thread::sleep(fallback_idle_timeout());
                registry.idle_check_scheduled.store(false, Ordering::Release);
                registry.shutdown_idle_if_current(generation);
            })
        {
            self.idle_check_scheduled.store(false, Ordering::Release);
            tracing::warn!(%error, "failed to schedule client fallback idle shutdown");
        }
    }

    fn shutdown_idle_if_current(&'static self, generation: usize) {
        if self.active_leases.load(Ordering::Acquire) != 0 {
            return;
        }

        let runtime = {
            let mut state = self.state.lock();
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
            shutdown_runtime(runtime);
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
        self.registry.submitted_tasks.fetch_add(1, Ordering::Relaxed);
        self.registry.active_tasks.fetch_add(1, Ordering::Relaxed);
        let runtime = self.runtime.clone();
        Ok(runtime.owner.context().runtime().spawn(async move {
            let _guard = ActiveTaskGuard {
                registry: self.registry,
                task_name,
                _lease: self,
            };
            task.await;
        }))
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

fn shutdown_runtime(runtime: Arc<ClientSharedFallbackRuntime>) {
    if let Ok(runtime) = Arc::try_unwrap(runtime) {
        if let Err(error) = runtime.owner.shutdown_runtime_blocking() {
            tracing::warn!(%error, "failed to shut down client fallback runtime");
        }
    }
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
        assert_eq!(snapshot.active_leases, 1);
        assert!(snapshot.runtime_available);
        assert_eq!(snapshot.runtime_generation, 1);

        drop(lease);
        assert_eq!(registry.snapshot().active_leases, 0);

        registry.shutdown_explicit();
        let error = match registry.acquire() {
            Ok(_) => panic!("explicit shutdown must reject new fallback leases"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("explicitly shut down"));
        assert_eq!(registry.snapshot().explicit_shutdowns, 1);
    }
}
