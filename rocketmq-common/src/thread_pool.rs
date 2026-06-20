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

use anyhow::bail;
use anyhow::Context;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskId;
use rocketmq_runtime::TaskKind;
use tokio::runtime::Handle;

pub struct TokioExecutorService {
    inner: RuntimeOwner,
    task_group: TaskGroup,
}

impl Default for TokioExecutorService {
    fn default() -> Self {
        Self::new()
    }
}

impl TokioExecutorService {
    pub fn shutdown(self) {
        self.inner.shutdown_background();
    }

    pub fn shutdown_timeout(self, timeout: Duration) {
        if let Err(error) = self.inner.shutdown_runtime_blocking_with_timeout(timeout) {
            tracing::warn!(%error, "failed to shut down TokioExecutorService runtime");
        }
    }
}

impl TokioExecutorService {
    pub fn new() -> TokioExecutorService {
        Self::try_new().unwrap_or_else(|error| panic!("failed to create TokioExecutorService: {error:#}"))
    }

    pub fn try_new() -> anyhow::Result<TokioExecutorService> {
        Self::from_config(common_runtime_config(
            num_cpus::get(),
            "rocketmq-common-tokio-executor",
            Duration::from_secs(30),
            num_cpus::get().saturating_mul(4),
        ))
        .context("failed to create TokioExecutorService runtime")
    }

    pub fn new_with_config(
        thread_num: usize,
        thread_prefix: Option<impl Into<String>>,
        keep_alive: Duration,
        max_blocking_threads: usize,
    ) -> TokioExecutorService {
        Self::try_new_with_config(thread_num, thread_prefix, keep_alive, max_blocking_threads)
            .unwrap_or_else(|error| panic!("failed to create TokioExecutorService: {error:#}"))
    }

    pub fn try_new_with_config(
        thread_num: usize,
        thread_prefix: Option<impl Into<String>>,
        keep_alive: Duration,
        max_blocking_threads: usize,
    ) -> anyhow::Result<TokioExecutorService> {
        validate_runtime_config(thread_num, max_blocking_threads)?;
        let thread_prefix_inner = if let Some(thread_prefix) = thread_prefix {
            thread_prefix.into()
        } else {
            "rocketmq-thread-".to_string()
        };
        Self::from_config(common_runtime_config(
            thread_num,
            thread_prefix_inner,
            keep_alive,
            max_blocking_threads,
        ))
        .context("failed to create configured TokioExecutorService runtime")
    }

    fn from_config(config: RuntimeConfig) -> anyhow::Result<TokioExecutorService> {
        let inner = RuntimeOwner::new(config)?;
        let task_group = inner.context().root_group().child("tokio-executor");
        Ok(TokioExecutorService { inner, task_group })
    }
}

impl TokioExecutorService {
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

    pub async fn wait_task(&self, task_id: TaskId, timeout: Duration) -> bool {
        self.task_group.wait_task(task_id, timeout).await
    }

    pub fn task_count(&self) -> usize {
        self.task_group.task_count()
    }

    pub fn get_handle(&self) -> &Handle {
        self.inner.context().runtime().inner()
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.inner.block_on(future)
    }
}

pub struct FuturesExecutorService {
    inner: futures::executor::ThreadPool,
}
impl FuturesExecutorService {
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.inner.spawn_ok(future);
    }
}

#[derive(Debug, Default)]
pub struct FuturesExecutorServiceBuilder {
    pool_size: usize,
    stack_size: usize,
    thread_name_prefix: Option<String>,
}

impl FuturesExecutorServiceBuilder {
    pub fn new() -> FuturesExecutorServiceBuilder {
        FuturesExecutorServiceBuilder {
            pool_size: cmp::max(1, num_cpus::get()),
            stack_size: 0,
            thread_name_prefix: None,
        }
    }

    pub fn pool_size(mut self, pool_size: usize) -> Self {
        self.pool_size = pool_size;
        self
    }

    pub fn stack_size(mut self, stack_size: usize) -> Self {
        self.stack_size = stack_size;
        self
    }

    pub fn create(&mut self) -> anyhow::Result<FuturesExecutorService> {
        let name_prefix = self.thread_name_prefix.as_deref().unwrap_or("Default-Executor");
        let thread_pool = futures::executor::ThreadPool::builder()
            .stack_size(self.stack_size)
            .pool_size(self.pool_size)
            .name_prefix(name_prefix)
            .create()
            .context("failed to create futures executor thread pool")?;
        Ok(FuturesExecutorService { inner: thread_pool })
    }
}

pub struct ScheduledExecutorService {
    inner: RuntimeOwner,
    scheduled_tasks: ScheduledTaskGroup,
}

impl Default for ScheduledExecutorService {
    fn default() -> Self {
        Self::new()
    }
}
impl ScheduledExecutorService {
    pub fn new() -> ScheduledExecutorService {
        Self::try_new().unwrap_or_else(|error| panic!("failed to create ScheduledExecutorService: {error:#}"))
    }

    pub fn try_new() -> anyhow::Result<ScheduledExecutorService> {
        Self::from_config(common_runtime_config(
            num_cpus::get(),
            "rocketmq-common-scheduled-executor",
            Duration::from_secs(30),
            num_cpus::get().saturating_mul(4),
        ))
        .context("failed to create ScheduledExecutorService runtime")
    }

    pub fn new_with_config(
        thread_num: usize,
        thread_prefix: Option<impl Into<String>>,
        keep_alive: Duration,
        max_blocking_threads: usize,
    ) -> ScheduledExecutorService {
        Self::try_new_with_config(thread_num, thread_prefix, keep_alive, max_blocking_threads)
            .unwrap_or_else(|error| panic!("failed to create ScheduledExecutorService: {error:#}"))
    }

    pub fn try_new_with_config(
        thread_num: usize,
        thread_prefix: Option<impl Into<String>>,
        keep_alive: Duration,
        max_blocking_threads: usize,
    ) -> anyhow::Result<ScheduledExecutorService> {
        validate_runtime_config(thread_num, max_blocking_threads)?;
        let thread_prefix_inner = if let Some(thread_prefix) = thread_prefix {
            thread_prefix.into()
        } else {
            "rocketmq-thread-".to_string()
        };
        Self::from_config(common_runtime_config(
            thread_num,
            thread_prefix_inner,
            keep_alive,
            max_blocking_threads,
        ))
        .context("failed to create configured ScheduledExecutorService runtime")
    }

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

    fn from_config(config: RuntimeConfig) -> anyhow::Result<ScheduledExecutorService> {
        let inner = RuntimeOwner::new(config)?;
        let scheduled_tasks = ScheduledTaskGroup::new(inner.context().root_group().child("scheduled"));
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
    config.blocking_pool_policy.max_concurrency = max_blocking_threads;
    config
}

fn validate_runtime_config(thread_num: usize, max_blocking_threads: usize) -> anyhow::Result<()> {
    if thread_num == 0 {
        bail!("thread_num must be greater than zero");
    }
    if max_blocking_threads == 0 {
        bail!("max_blocking_threads must be greater than zero");
    }
    Ok(())
}

#[cfg(test)]
mod runtime_config_tests {
    use super::*;

    #[test]
    fn tokio_executor_try_new_with_config_rejects_invalid_thread_counts() {
        let error = match TokioExecutorService::try_new_with_config(0, Some("test-"), Duration::from_secs(1), 1) {
            Ok(_) => panic!("zero worker threads should be rejected before tokio builder panics"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("thread_num must be greater than zero"));

        let error = match TokioExecutorService::try_new_with_config(1, Some("test-"), Duration::from_secs(1), 0) {
            Ok(_) => panic!("zero blocking threads should be rejected before tokio builder panics"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("max_blocking_threads must be greater than zero"));
    }

    #[test]
    fn scheduled_executor_try_new_with_config_rejects_invalid_thread_counts() {
        let error = match ScheduledExecutorService::try_new_with_config(0, Some("test-"), Duration::from_secs(1), 1) {
            Ok(_) => panic!("zero worker threads should be rejected before tokio builder panics"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("thread_num must be greater than zero"));

        let error = match ScheduledExecutorService::try_new_with_config(1, Some("test-"), Duration::from_secs(1), 0) {
            Ok(_) => panic!("zero blocking threads should be rejected before tokio builder panics"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("max_blocking_threads must be greater than zero"));
    }
}

impl ScheduledExecutorService {
    pub fn shutdown(self) {
        self.shutdown_timeout(Duration::from_secs(30));
    }

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
        let mut builder = FuturesExecutorServiceBuilder::new().pool_size(1).stack_size(0);
        let executor = builder.create().expect("executor should be created");
        let (tx, rx) = mpsc::channel();

        executor.spawn(async move {
            tx.send(42).expect("test receiver should be alive");
        });

        assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), 42);
    }

    #[test]
    fn tokio_executor_uses_runtime_owner_and_runs_tasks() {
        let executor =
            TokioExecutorService::try_new_with_config(2, Some("tokio-executor-test"), Duration::from_secs(1), 2)
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
        let executor =
            ScheduledExecutorService::try_new_with_config(2, Some("schedule-test-"), Duration::from_secs(1), 2)
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
