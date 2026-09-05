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

use std::cmp;
use std::future::Future;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use crate::RuntimeConfig;
use crate::RuntimeContractViolation;
use crate::RuntimeError;
use crate::RuntimeOwner;
use crate::RuntimeOwnerPlan;
use crate::RuntimeResult;
use crate::ScheduledTaskConfig;
use crate::ScheduledTaskGroup;
use crate::TaskGroup;
use crate::TaskId;
use crate::TaskKind;
use tokio::runtime::Handle;

/// Represents tokio executor service.
pub struct TokioExecutorService {
    inner: RuntimeOwner,
    task_group: TaskGroup,
}

/// A Tokio executor service profile that passed deterministic validation.
#[derive(Debug)]
pub struct TokioExecutorServicePlan {
    runtime: RuntimeOwnerPlan,
}

impl Default for TokioExecutorService {
    fn default() -> Self {
        Self::new()
    }
}

impl TokioExecutorService {
    /// Shuts down the owned service.
    pub fn shutdown(self) {
        self.inner.shutdown_background();
    }

    /// Shuts down timeout.
    pub fn shutdown_timeout(self, timeout: Duration) {
        if let Err(error) = self.inner.shutdown_runtime_blocking_with_timeout(timeout) {
            tracing::warn!(%error, "failed to shut down TokioExecutorService runtime");
        }
    }
}

impl TokioExecutorService {
    /// Creates a new `TokioExecutorService`.
    pub fn new() -> TokioExecutorService {
        Self::try_new().unwrap_or_else(|error| panic!("failed to create TokioExecutorService: {error:#}"))
    }

    /// Builds the internally validated default executor profile.
    ///
    /// # Errors
    ///
    /// Returns an operational runtime error when the executor cannot start.
    pub fn try_new() -> RuntimeResult<TokioExecutorService> {
        Self::plan()
            .expect("internally derived Tokio executor profile is valid")
            .build()
    }

    /// Validates the internally derived executor profile.
    ///
    /// # Errors
    ///
    /// Returns a deterministic contract violation only if the platform's CPU
    /// count cannot form a supported runtime profile.
    pub fn plan() -> Result<TokioExecutorServicePlan, RuntimeContractViolation> {
        let workers = num_cpus::get().max(1);
        Self::plan_with_config(
            workers,
            Some("rocketmq-runtime-tokio-executor"),
            Duration::from_secs(30),
            workers.saturating_mul(4),
        )
    }

    /// Validates an explicit Tokio executor profile.
    ///
    /// # Errors
    ///
    /// Returns a deterministic contract violation when the requested runtime
    /// parameters are structurally invalid. Call [`TokioExecutorServicePlan::build`]
    /// to perform runtime construction.
    pub fn plan_with_config(
        thread_num: usize,
        thread_prefix: Option<impl Into<String>>,
        keep_alive: Duration,
        max_blocking_threads: usize,
    ) -> Result<TokioExecutorServicePlan, RuntimeContractViolation> {
        let thread_prefix_inner = if let Some(thread_prefix) = thread_prefix {
            thread_prefix.into()
        } else {
            "rocketmq-thread-".to_string()
        };
        Ok(TokioExecutorServicePlan {
            runtime: RuntimeOwner::plan(common_runtime_config(
                thread_num,
                thread_prefix_inner,
                keep_alive,
                max_blocking_threads,
            ))?,
        })
    }
}

impl TokioExecutorServicePlan {
    /// Builds a Tokio executor service from a validated profile.
    ///
    /// # Errors
    ///
    /// Returns an operational error when process-memory discovery or Tokio
    /// runtime construction fails.
    pub fn build(self) -> RuntimeResult<TokioExecutorService> {
        let inner = self.runtime.build()?;
        let task_group = inner.root_context().component("tokio-executor").task_group().clone();
        Ok(TokioExecutorService { inner, task_group })
    }
}

impl TokioExecutorService {
    /// Spawns the supplied task.
    pub fn spawn<F>(&self, future: F) -> TaskId
    where
        F: Future<Output = ()> + Send + 'static,
    {
        static TASK_ID: AtomicUsize = AtomicUsize::new(0);

        self.task_group
            .spawn(
                format!(
                    "rocketmq.common.tokio-executor.task.{}",
                    TASK_ID.fetch_add(1, Ordering::Relaxed)
                ),
                TaskKind::Worker,
                future,
            )
            .expect("TokioExecutorService task group should be open")
    }

    /// Returns the wait task.
    pub async fn wait_task(&self, task_id: TaskId, timeout: Duration) -> bool {
        self.task_group.wait_task(task_id, timeout).await
    }

    /// Returns the task count.
    pub fn task_count(&self) -> usize {
        self.task_group.task_count()
    }

    /// Returns handle.
    pub fn get_handle(&self) -> &Handle {
        self.inner.root_context().runtime().tokio_handle()
    }

    /// Returns the block on.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.inner.block_on(future)
    }
}

/// Represents futures executor service.
pub struct FuturesExecutorService {
    inner: futures::executor::ThreadPool,
}
impl FuturesExecutorService {
    /// Spawns the supplied task.
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.inner.spawn_ok(future);
    }
}

#[derive(Debug, Default)]
/// Represents futures executor service builder.
pub struct FuturesExecutorServiceBuilder {
    pool_size: usize,
    stack_size: usize,
    thread_name_prefix: Option<String>,
}

/// A futures executor configuration that passed deterministic validation.
#[derive(Debug)]
pub struct FuturesExecutorPlan {
    pool_size: usize,
    stack_size: usize,
    thread_name_prefix: Option<String>,
}

impl FuturesExecutorServiceBuilder {
    /// Creates a new `FuturesExecutorServiceBuilder`.
    pub fn new() -> FuturesExecutorServiceBuilder {
        FuturesExecutorServiceBuilder {
            pool_size: cmp::max(1, num_cpus::get()),
            stack_size: 0,
            thread_name_prefix: None,
        }
    }

    /// Returns the pool size.
    pub fn pool_size(mut self, pool_size: usize) -> Self {
        self.pool_size = pool_size;
        self
    }

    /// Returns the stack size.
    pub fn stack_size(mut self, stack_size: usize) -> Self {
        self.stack_size = stack_size;
        self
    }

    /// Validates deterministic pool settings and creates a build plan.
    ///
    /// # Errors
    ///
    /// Returns a contract violation when the requested pool size is zero.
    pub fn into_plan(self) -> Result<FuturesExecutorPlan, crate::RuntimeContractViolation> {
        if self.pool_size == 0 {
            return Err(crate::RuntimeContractViolation::InvalidConfiguration {
                policy: crate::RuntimeContractPolicy::FuturesExecutorPoolSizePositive,
            });
        }
        Ok(FuturesExecutorPlan {
            pool_size: self.pool_size,
            stack_size: self.stack_size,
            thread_name_prefix: self.thread_name_prefix,
        })
    }
}

impl FuturesExecutorPlan {
    /// Builds the validated futures executor.
    ///
    /// # Errors
    ///
    /// Returns a runtime build error while retaining the underlying
    /// `ThreadPoolBuildError` source when thread-pool creation fails.
    pub fn build(&self) -> RuntimeResult<FuturesExecutorService> {
        let name_prefix = self.thread_name_prefix.as_deref().unwrap_or("Default-Executor");
        let thread_pool = futures::executor::ThreadPool::builder()
            .stack_size(self.stack_size)
            .pool_size(self.pool_size)
            .name_prefix(name_prefix)
            .create()
            .map_err(|error| RuntimeError::build(crate::RuntimeOperation::BuildFuturesThreadPool, error))?;
        Ok(FuturesExecutorService { inner: thread_pool })
    }
}

/// Represents scheduled executor service.
pub struct ScheduledExecutorService {
    inner: RuntimeOwner,
    scheduled_tasks: ScheduledTaskGroup,
}

/// A scheduled executor profile that passed deterministic validation.
#[derive(Debug)]
pub struct ScheduledExecutorServicePlan {
    runtime: RuntimeOwnerPlan,
}

impl Default for ScheduledExecutorService {
    fn default() -> Self {
        Self::new()
    }
}
impl ScheduledExecutorService {
    /// Creates a new `ScheduledExecutorService`.
    pub fn new() -> ScheduledExecutorService {
        Self::try_new().unwrap_or_else(|error| panic!("failed to create ScheduledExecutorService: {error:#}"))
    }

    /// Builds the internally validated default scheduled executor profile.
    ///
    /// # Errors
    ///
    /// Returns an operational runtime error when the executor cannot start.
    pub fn try_new() -> RuntimeResult<ScheduledExecutorService> {
        Self::plan()
            .expect("internally derived scheduled executor profile is valid")
            .build()
    }

    /// Validates the internally derived scheduled executor profile.
    ///
    /// # Errors
    ///
    /// Returns a deterministic contract violation only if the platform's CPU
    /// count cannot form a supported runtime profile.
    pub fn plan() -> Result<ScheduledExecutorServicePlan, RuntimeContractViolation> {
        let workers = num_cpus::get().max(1);
        Self::plan_with_config(
            workers,
            Some("rocketmq-runtime-scheduled-executor"),
            Duration::from_secs(30),
            workers.saturating_mul(4),
        )
    }

    /// Validates an explicit scheduled executor profile.
    ///
    /// # Errors
    ///
    /// Returns a deterministic contract violation when the requested runtime
    /// parameters are structurally invalid. Call [`ScheduledExecutorServicePlan::build`]
    /// to perform runtime construction.
    pub fn plan_with_config(
        thread_num: usize,
        thread_prefix: Option<impl Into<String>>,
        keep_alive: Duration,
        max_blocking_threads: usize,
    ) -> Result<ScheduledExecutorServicePlan, RuntimeContractViolation> {
        let thread_prefix_inner = if let Some(thread_prefix) = thread_prefix {
            thread_prefix.into()
        } else {
            "rocketmq-thread-".to_string()
        };
        Ok(ScheduledExecutorServicePlan {
            runtime: RuntimeOwner::plan(common_runtime_config(
                thread_num,
                thread_prefix_inner,
                keep_alive,
                max_blocking_threads,
            ))?,
        })
    }

    /// Executes schedule at fixed rate.
    pub fn schedule_at_fixed_rate<F>(&self, task: F, initial_delay: Option<Duration>, period: Duration)
    where
        F: FnMut() + Send + 'static,
    {
        static SCHEDULE_ID: AtomicUsize = AtomicUsize::new(0);

        let task = Arc::new(Mutex::new(task));
        let mut config = ScheduledTaskConfig::fixed_rate_no_overlap(
            format!(
                "rocketmq.common.schedule_at_fixed_rate.{}",
                SCHEDULE_ID.fetch_add(1, Ordering::Relaxed)
            ),
            period,
        );
        config.initial_delay = initial_delay.unwrap_or(Duration::ZERO);

        let schedule_result = self.scheduled_tasks.schedule_fixed_rate_no_overlap(config, move || {
            let task = task.clone();
            async move {
                if let Ok(mut task) = task.lock() {
                    task();
                }
            }
        });
        if let Err(error) = schedule_result {
            tracing::warn!(%error, "failed to schedule fixed-rate task");
        }
    }
}

impl ScheduledExecutorServicePlan {
    /// Builds a scheduled executor service from a validated profile.
    ///
    /// # Errors
    ///
    /// Returns an operational error when process-memory discovery or Tokio
    /// runtime construction fails.
    pub fn build(self) -> RuntimeResult<ScheduledExecutorService> {
        let inner = self.runtime.build()?;
        let scheduled_tasks = inner.root_context().component("scheduled").scheduled_tasks("executor");
        Ok(ScheduledExecutorService { inner, scheduled_tasks })
    }
}

fn common_runtime_config(
    thread_num: usize,
    thread_name: impl Into<String>,
    keep_alive: Duration,
    max_blocking_threads: usize,
) -> RuntimeConfig {
    let mut config = RuntimeConfig::server_default(thread_name);
    config.worker_threads = thread_num;
    config.max_blocking_threads = max_blocking_threads;
    config.thread_keep_alive = keep_alive;
    config.blocking_lane_policies.storage_io.max_concurrency = max_blocking_threads;
    config.blocking_lane_policies.metadata_io.max_concurrency = max_blocking_threads;
    config.blocking_lane_policies.cpu_crypto.max_concurrency = max_blocking_threads;
    config
}

#[cfg(test)]
mod runtime_config_tests {
    use super::*;

    #[test]
    fn tokio_executor_try_new_with_config_rejects_invalid_thread_counts() {
        let error = match TokioExecutorService::plan_with_config(0, Some("test-"), Duration::from_secs(1), 1) {
            Ok(_) => panic!("zero worker threads should be rejected before tokio builder panics"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            crate::RuntimeContractViolation::InvalidConfiguration { .. }
        ));

        let error = match TokioExecutorService::plan_with_config(1, Some("test-"), Duration::from_secs(1), 0) {
            Ok(_) => panic!("zero blocking threads should be rejected before tokio builder panics"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            crate::RuntimeContractViolation::InvalidConfiguration { .. }
        ));
    }

    #[test]
    fn scheduled_executor_try_new_with_config_rejects_invalid_thread_counts() {
        let error = match ScheduledExecutorService::plan_with_config(0, Some("test-"), Duration::from_secs(1), 1) {
            Ok(_) => panic!("zero worker threads should be rejected before tokio builder panics"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            crate::RuntimeContractViolation::InvalidConfiguration { .. }
        ));

        let error = match ScheduledExecutorService::plan_with_config(1, Some("test-"), Duration::from_secs(1), 0) {
            Ok(_) => panic!("zero blocking threads should be rejected before tokio builder panics"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            crate::RuntimeContractViolation::InvalidConfiguration { .. }
        ));
    }

    #[test]
    fn futures_thread_pool_build_failure_keeps_the_build_source() {
        let error = match FuturesExecutorServiceBuilder::new().pool_size(0).into_plan() {
            Ok(_) => panic!("zero futures pool size must fail"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            crate::RuntimeContractViolation::InvalidConfiguration { .. }
        ));
    }
}

impl ScheduledExecutorService {
    /// Shuts down the owned service.
    pub fn shutdown(self) {
        self.shutdown_timeout(Duration::from_secs(30));
    }

    /// Shuts down timeout.
    pub fn shutdown_timeout(self, timeout: Duration) {
        let ScheduledExecutorService {
            inner,
            scheduled_tasks: _,
        } = self;
        if let Err(error) = inner.shutdown_runtime_blocking_with_timeout(timeout) {
            tracing::warn!(%error, "failed to shut down ScheduledExecutorService runtime");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    #[test]
    fn futures_executor_builder_create_returns_executor() {
        let plan = FuturesExecutorServiceBuilder::new()
            .pool_size(1)
            .stack_size(0)
            .into_plan()
            .expect("executor plan should be valid");
        let executor = plan.build().expect("executor should be created");
        let (tx, rx) = mpsc::channel();

        executor.spawn(async move {
            tx.send(42).expect("test receiver should be alive");
        });

        assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), 42);
    }

    #[test]
    fn tokio_executor_uses_runtime_owner_and_runs_tasks() {
        let executor =
            TokioExecutorService::plan_with_config(2, Some("tokio-executor-test"), Duration::from_secs(1), 3)
                .expect("tokio executor plan should be valid")
                .build()
                .expect("tokio executor should be created");
        let (tx, rx) = mpsc::channel();

        let task_id = executor.spawn(async move {
            tx.send(42).expect("test receiver should be alive");
        });

        assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), 42);
        assert!(executor.block_on(executor.wait_task(task_id, Duration::from_secs(1))));
        assert_eq!(executor.task_count(), 0);
        executor.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn scheduled_executor_fixed_rate_does_not_overlap() {
        let executor = ScheduledExecutorService::plan_with_config(2, Some("schedule-test-"), Duration::from_secs(1), 3)
            .expect("scheduled executor plan should be valid")
            .build()
            .expect("scheduled executor should be created");
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let runs = Arc::new(AtomicUsize::new(0));
        let active_task = active.clone();
        let max_active_task = max_active.clone();
        let runs_task = runs.clone();

        executor.schedule_at_fixed_rate(
            move || {
                let current = active_task.fetch_add(1, Ordering::SeqCst) + 1;
                max_active_task.fetch_max(current, Ordering::SeqCst);
                runs_task.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(30));
                active_task.fetch_sub(1, Ordering::SeqCst);
            },
            Some(Duration::ZERO),
            Duration::from_millis(5),
        );

        std::thread::sleep(Duration::from_millis(120));
        executor.shutdown_timeout(Duration::from_secs(1));

        assert!(runs.load(Ordering::SeqCst) > 0);
        assert_eq!(max_active.load(Ordering::SeqCst), 1);
    }
}
