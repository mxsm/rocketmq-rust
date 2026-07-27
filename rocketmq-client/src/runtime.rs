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
use std::sync::atomic::Ordering;
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
pub use rocketmq_observability::metrics::client::ClientMetrics;
pub use rocketmq_observability::TelemetryHandle;
use rocketmq_runtime::BudgetCapacity;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::ProcessMemoryLimit;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ResourceBudgetTree;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskControl;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskGroupLifecycleState;
use rocketmq_runtime::TaskId;

use crate::implementation::mq_client_manager::ClientPool;

/// Configuration for one explicitly owned RocketMQ client runtime.
#[derive(Clone, Debug)]
pub struct ClientRuntimeConfig {
    /// Maximum duration used by [`ClientRuntime::shutdown`].
    pub shutdown_timeout: Duration,
    /// Process/Pod hard memory limit. Zero selects automatic detection.
    pub process_memory_limit_bytes: u64,
    /// Share of the process hard limit managed by client resource budgets.
    pub managed_memory_numerator: u64,
    /// Denominator for [`Self::managed_memory_numerator`].
    pub managed_memory_denominator: u64,
}

impl Default for ClientRuntimeConfig {
    fn default() -> Self {
        Self {
            shutdown_timeout: Duration::from_secs(30),
            process_memory_limit_bytes: 0,
            managed_memory_numerator: 1,
            managed_memory_denominator: 4,
        }
    }
}

/// Explicit lifecycle owner for all RocketMQ clients in one application domain.
///
/// The caller must derive the supplied context from its application-owned
/// [`rocketmq_runtime::RuntimeOwner`]. The client never discovers the current
/// Tokio runtime and never creates a fallback runtime.
pub struct ClientRuntime {
    service_context: ChildServiceContext,
    telemetry_handle: TelemetryHandle,
    client_metrics: ClientMetrics,
    pool: ClientPool,
    resource_budget: ResourceBudget,
    config: ClientRuntimeConfig,
    shutdown: AtomicBool,
}

impl ClientRuntime {
    /// Creates a client runtime below a sealed application child context.
    ///
    /// # Panics
    ///
    /// Panics when the configured process memory limit or managed-memory
    /// fraction is invalid. Production composition should use
    /// [`Self::try_new`].
    pub fn new(
        service_context: ChildServiceContext,
        config: ClientRuntimeConfig,
        telemetry_handle: TelemetryHandle,
    ) -> Arc<Self> {
        Self::try_new(service_context, config, telemetry_handle).expect("client runtime resource budget must be valid")
    }

    /// Creates a client runtime with one shared process-derived resource root.
    pub fn try_new(
        service_context: ChildServiceContext,
        config: ClientRuntimeConfig,
        telemetry_handle: TelemetryHandle,
    ) -> RocketMQResult<Arc<Self>> {
        let resource_budget = build_client_resource_budget(&config)?;
        let client_metrics = ClientMetrics::from_handle(&telemetry_handle);
        let pool = ClientPool::new(
            service_context.child("pool"),
            resource_budget.clone(),
            telemetry_handle.clone(),
            client_metrics.clone(),
        );
        Ok(Arc::new(Self {
            service_context,
            telemetry_handle,
            client_metrics,
            pool,
            resource_budget,
            config,
            shutdown: AtomicBool::new(false),
        }))
    }

    /// Returns this runtime's isolated client-instance pool.
    pub fn pool(&self) -> &ClientPool {
        &self.pool
    }

    /// Returns the root client scope injected by the application.
    pub fn service_context(&self) -> &ChildServiceContext {
        &self.service_context
    }

    /// Returns the cloneable telemetry capability injected by the application.
    pub fn telemetry_handle(&self) -> &TelemetryHandle {
        &self.telemetry_handle
    }

    /// Returns this runtime's typed, lifecycle-gated client metric recorder.
    pub fn client_metrics(&self) -> &ClientMetrics {
        &self.client_metrics
    }

    /// Returns the shared parent budget for every client component owned by
    /// this runtime.
    pub fn resource_budget(&self) -> ResourceBudget {
        self.resource_budget.clone()
    }

    /// Creates a named descendant scope owned by this client runtime.
    pub fn child(&self, scope: impl Into<rocketmq_runtime::ScopeId>) -> ChildServiceContext {
        self.service_context.child(scope)
    }

    /// Stops all pooled clients and joins every task owned by this runtime.
    pub async fn shutdown(&self) -> ShutdownReport {
        self.shutdown_until(ShutdownDeadline::after(self.config.shutdown_timeout))
            .await
    }

    /// Stops all pooled clients using the caller's absolute shutdown deadline.
    pub async fn shutdown_until(&self, deadline: ShutdownDeadline) -> ShutdownReport {
        if !self.shutdown.swap(true, Ordering::AcqRel) {
            self.pool.shutdown_until(deadline).await;
        }
        self.service_context.task_group().shutdown_until(deadline).await
    }

    /// Returns whether shutdown admission has been closed.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
}

fn build_client_resource_budget(config: &ClientRuntimeConfig) -> RocketMQResult<ResourceBudget> {
    const CLIENT_ITEM_LIMIT: usize = 262_144;
    const CONTROL_RESERVE_COUNT: usize = 1_024;

    let process_limit = if config.process_memory_limit_bytes == 0 {
        ProcessMemoryLimit::detect()
    } else {
        ProcessMemoryLimit::configured(config.process_memory_limit_bytes)
    }
    .map_err(|error| RocketMQError::ConfigInvalidValue {
        key: "client.runtime.processMemoryLimitBytes",
        value: config.process_memory_limit_bytes.to_string(),
        reason: error.to_string(),
    })?;
    let managed_bytes = process_limit
        .fraction(config.managed_memory_numerator, config.managed_memory_denominator)
        .map_err(|error| RocketMQError::ConfigInvalidValue {
            key: "client.runtime.managedMemoryFraction",
            value: format!(
                "{}/{}",
                config.managed_memory_numerator, config.managed_memory_denominator
            ),
            reason: error.to_string(),
        })?;
    let managed_bytes = usize::try_from(managed_bytes).unwrap_or(usize::MAX).max(1);
    let control_bytes = (managed_bytes / 16).max(1);
    ResourceBudgetTree::new(
        "client",
        BudgetLimit::new(CLIENT_ITEM_LIMIT, managed_bytes, FullPolicy::Reject)
            .with_control_reserve(BudgetCapacity::new(CONTROL_RESERVE_COUNT, control_bytes)),
    )
    .map(|tree| tree.root())
    .map_err(|error| RocketMQError::ConfigInvalidValue {
        key: "client.runtime.resourceBudget",
        value: managed_bytes.to_string(),
        reason: error.to_string(),
    })
}

pub(crate) fn standalone_client_resource_budget() -> RocketMQResult<ResourceBudget> {
    build_client_resource_budget(&ClientRuntimeConfig::default())
}

#[cfg(test)]
static TEST_RUNTIME_OWNER: std::sync::LazyLock<rocketmq_runtime::RuntimeOwner> = std::sync::LazyLock::new(|| {
    rocketmq_runtime::RuntimeOwner::new(rocketmq_runtime::RuntimeConfig {
        thread_name: "rocketmq-client-unit-test".to_string(),
        ..Default::default()
    })
    .expect("client unit-test runtime owner should start")
});

#[cfg(test)]
pub(crate) fn test_client_runtime(scope: &'static str) -> Arc<ClientRuntime> {
    ClientRuntime::new(
        TEST_RUNTIME_OWNER.root_context().child(scope),
        ClientRuntimeConfig::default(),
        TelemetryHandle::noop(),
    )
}

#[cfg(test)]
pub(crate) fn test_service_context(scope: &'static str) -> ChildServiceContext {
    TEST_RUNTIME_OWNER.root_context().child(scope)
}

pub(crate) struct ClientRuntimeTaskHandle {
    task_group: TaskGroup,
    task_id: TaskId,
    completion_rx: Mutex<Option<std_mpsc::Receiver<()>>>,
}

impl ClientRuntimeTaskHandle {
    pub(crate) fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub(crate) fn task_count(&self) -> usize {
        self.task_group.task_count()
    }

    pub(crate) fn is_finished(&self) -> bool {
        let mut completion_rx = self.completion_rx.lock();
        let Some(receiver) = completion_rx.as_ref() else {
            return true;
        };

        match receiver.try_recv() {
            Ok(()) | Err(std_mpsc::TryRecvError::Disconnected) => {
                completion_rx.take();
                true
            }
            Err(std_mpsc::TryRecvError::Empty) => false,
        }
    }

    pub(crate) fn wait_finished(&self, timeout: Duration) -> bool {
        let mut completion_rx = self.completion_rx.lock();
        let Some(receiver) = completion_rx.take() else {
            return true;
        };

        match receiver.recv_timeout(timeout) {
            Ok(()) | Err(std_mpsc::RecvTimeoutError::Disconnected) => true,
            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                *completion_rx = Some(receiver);
                false
            }
        }
    }
}

pub(crate) fn spawn_client_task_with_context<F>(
    context: &ChildServiceContext,
    task_name: &'static str,
    task: F,
) -> io::Result<ClientRuntimeTaskHandle>
where
    F: Future<Output = ()> + Send + 'static,
{
    let task_group = context.task_group().child(task_name);
    let (completion_tx, completion_rx) = std_mpsc::channel();
    let task_id = task_group
        .spawn_service(task_name, async move {
            let _completion = ClientTrackedTaskCompletion::new(completion_tx);
            task.await;
        })
        .map_err(io::Error::other)?;

    Ok(ClientRuntimeTaskHandle {
        task_group,
        task_id,
        completion_rx: Mutex::new(Some(completion_rx)),
    })
}

pub(crate) fn spawn_detached_client_task_with_context<F>(
    context: &ChildServiceContext,
    task_name: &'static str,
    task: F,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    drop(spawn_client_task_with_context(context, task_name, task)?);
    Ok(())
}

pub(crate) struct ClientTrackedTaskHandle {
    task_group: TaskGroup,
    completion_rx: Mutex<Option<std_mpsc::Receiver<()>>>,
}

impl ClientTrackedTaskHandle {
    pub(crate) fn task_count(&self) -> usize {
        self.task_group.task_count()
    }

    pub(crate) fn is_finished(&self) -> bool {
        let mut completion_rx = self.completion_rx.lock();
        let Some(receiver) = completion_rx.as_ref() else {
            return true;
        };

        match receiver.try_recv() {
            Ok(()) | Err(std_mpsc::TryRecvError::Disconnected) => {
                completion_rx.take();
                true
            }
            Err(std_mpsc::TryRecvError::Empty) => false,
        }
    }

    pub(crate) async fn shutdown(self, timeout: Duration) -> ShutdownReport {
        self.task_group.shutdown(timeout).await
    }

    pub(crate) fn shutdown_blocking(self, timeout: Duration) -> (ShutdownReport, bool) {
        let completed = match self.completion_rx.into_inner() {
            Some(completion_rx) => match completion_rx.recv_timeout(timeout) {
                Ok(()) | Err(std_mpsc::RecvTimeoutError::Disconnected) => true,
                Err(std_mpsc::RecvTimeoutError::Timeout) => false,
            },
            None => true,
        };

        let report = self.task_group.shutdown_now();
        (report, completed)
    }

    pub(crate) fn shutdown_now(self) -> ShutdownReport {
        self.task_group.shutdown_now()
    }
}

pub(crate) fn spawn_client_tracked_task_with_context<F>(
    context: &ChildServiceContext,
    task_name: &'static str,
    task: F,
) -> io::Result<ClientTrackedTaskHandle>
where
    F: Future<Output = ()> + Send + 'static,
{
    let task_group = context.task_group().child(task_name);
    let (completion_tx, completion_rx) = std_mpsc::channel();
    task_group
        .spawn_service(task_name, async move {
            let _completion = ClientTrackedTaskCompletion::new(completion_tx);
            task.await;
        })
        .map_err(io::Error::other)?;

    Ok(ClientTrackedTaskHandle {
        task_group,
        completion_rx: Mutex::new(Some(completion_rx)),
    })
}

struct ClientTrackedTaskCompletion {
    completion_tx: Option<std_mpsc::Sender<()>>,
}

impl ClientTrackedTaskCompletion {
    fn new(completion_tx: std_mpsc::Sender<()>) -> Self {
        Self {
            completion_tx: Some(completion_tx),
        }
    }
}

impl Drop for ClientTrackedTaskCompletion {
    fn drop(&mut self) {
        if let Some(completion_tx) = self.completion_tx.take() {
            let _ = completion_tx.send(());
        }
    }
}

pub(crate) struct ClientScheduledTaskHandle {
    task_group: TaskGroup,
    scheduled_tasks: ScheduledTaskGroup,
}

impl ClientScheduledTaskHandle {
    pub(crate) fn is_running(&self) -> bool {
        self.task_group.lifecycle_state() == TaskGroupLifecycleState::Open && self.task_count() > 0
    }

    pub(crate) fn task_count(&self) -> usize {
        self.task_group.task_count() + self.scheduled_tasks.group().task_count()
    }

    pub(crate) fn schedule_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.scheduled_tasks.snapshot()
    }

    pub(crate) async fn shutdown(self, timeout: Duration) -> ShutdownReport {
        self.task_group.shutdown(timeout).await
    }

    pub(crate) fn shutdown_now(self) -> ShutdownReport {
        self.task_group.shutdown_now()
    }
}

pub(crate) fn schedule_client_fixed_delay_task_with_context<F, Fut>(
    context: &ChildServiceContext,
    task_name: &'static str,
    initial_delay: Duration,
    period: Duration,
    shutdown_timeout: Duration,
    task: F,
) -> io::Result<ClientScheduledTaskHandle>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let task_group = context.task_group().child(task_name);
    let scheduled_tasks = ScheduledTaskGroup::new(task_group.child("scheduled"));
    let mut config = ScheduledTaskConfig::fixed_delay(task_name, period);
    config.initial_delay = initial_delay;
    config.shutdown_timeout = shutdown_timeout;
    scheduled_tasks
        .schedule_fixed_delay(config, task)
        .map_err(io::Error::other)?;

    Ok(ClientScheduledTaskHandle {
        task_group,
        scheduled_tasks,
    })
}

pub(crate) fn schedule_client_fixed_delay_controlled_task_with_context<F, Fut>(
    context: &ChildServiceContext,
    task_name: &'static str,
    initial_delay: Duration,
    period: Duration,
    shutdown_timeout: Duration,
    task: F,
) -> io::Result<ClientScheduledTaskHandle>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ScheduledTaskControl> + Send + 'static,
{
    let task_group = context.task_group().child(task_name);
    let scheduled_tasks = ScheduledTaskGroup::new(task_group.child("scheduled"));
    let mut config = ScheduledTaskConfig::fixed_delay(task_name, period);
    config.initial_delay = initial_delay;
    config.shutdown_timeout = shutdown_timeout;
    scheduled_tasks
        .schedule_fixed_delay_controlled(config, task)
        .map_err(io::Error::other)?;

    Ok(ClientScheduledTaskHandle {
        task_group,
        scheduled_tasks,
    })
}

pub(crate) async fn spawn_client_blocking_io_with_context<F, R>(
    context: &ChildServiceContext,
    task_name: &'static str,
    task: F,
) -> io::Result<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    context
        .metadata_io()
        .spawn_io(task_name, task)
        .await
        .map_err(io::Error::other)
}

pub(crate) fn spawn_delayed_client_action_with_context<F>(
    context: &ChildServiceContext,
    task_name: &'static str,
    delay: Duration,
    action: F,
) where
    F: FnOnce() + Send + 'static,
{
    if delay.is_zero() {
        action();
        return;
    }

    if let Err(error) = spawn_detached_client_task_with_context(context, task_name, async move {
        tokio::time::sleep(delay).await;
        action();
    }) {
        tracing::error!(%error, task_name, "failed to spawn delayed client action");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    #[test]
    fn sibling_component_budgets_share_the_client_runtime_parent_limit() {
        let runtime = ClientRuntime::try_new(
            TEST_RUNTIME_OWNER.root_context().child("client-budget-parent-test"),
            ClientRuntimeConfig {
                process_memory_limit_bytes: 4_096,
                managed_memory_numerator: 1,
                managed_memory_denominator: 1,
                ..ClientRuntimeConfig::default()
            },
            TelemetryHandle::noop(),
        )
        .expect("client runtime budget");
        let parent = runtime.resource_budget();
        let first = parent
            .child("first-component", BudgetLimit::new(10, 4_096, FullPolicy::Reject))
            .expect("first child");
        let second = parent
            .child("second-component", BudgetLimit::new(10, 4_096, FullPolicy::Reject))
            .expect("second child");

        let _first_permit = first.try_acquire_data(3_000).expect("first reservation");

        assert!(second.try_acquire_data(1_000).is_err());
        assert_eq!(parent.snapshot().current_bytes, 3_000);
    }

    #[tokio::test]
    async fn explicit_context_owns_spawned_task() {
        let service_context = test_service_context("client-explicit-runtime-test");
        let completed = Arc::new(AtomicUsize::new(0));
        let completed_in_task = Arc::clone(&completed);

        let handle = spawn_client_task_with_context(&service_context, "owned-task", async move {
            completed_in_task.fetch_add(1, Ordering::Release);
        })
        .expect("task should spawn");

        assert!(handle.wait_finished(Duration::from_secs(1)));
        assert_eq!(completed.load(Ordering::Acquire), 1);
        assert!(handle.is_finished());
        assert_eq!(handle.task_count(), 0);
        let _ = handle.task_id();
        let report = service_context.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{report:?}");
    }
}
