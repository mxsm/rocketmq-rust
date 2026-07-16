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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use crate::RuntimeError;
use crate::RuntimeHandle;
use crate::RuntimeResult;
use crate::ShutdownReport;
use crate::TaskGroup;
use crate::TaskId;
use serde::Serialize;
use tokio::sync::Notify;
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::info;
use tracing::warn;

/// Service thread context that gets passed to the service
/// This contains all the control mechanisms
pub struct ServiceContext {
    /// Wait point for notifications
    wait_point: Arc<Notify>,
    /// Notification flag
    has_notified: Arc<AtomicBool>,
    /// Stop flag
    stopped: Arc<AtomicBool>,
}

impl ServiceContext {
    pub fn new(wait_point: Arc<Notify>, has_notified: Arc<AtomicBool>, stopped: Arc<AtomicBool>) -> Self {
        Self {
            wait_point,
            has_notified,
            stopped,
        }
    }

    /// Check if service is stopped
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    /// Wait for running with interval
    pub async fn wait_for_running(&self, interval: Duration) -> bool {
        // Check if already notified
        if self
            .has_notified
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return true; // Should call on_wait_end
        }

        // Entry to wait
        match timeout(interval, self.wait_point.notified()).await {
            Ok(_) => {
                // Notified
            }
            Err(_) => {
                // Timeout occurred - this is normal behavior
            }
        }
        // Reset notification flag
        self.has_notified.store(false, Ordering::Release);
        true // Should call on_wait_end
    }

    pub fn wakeup(&self) {
        if self
            .has_notified
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.wait_point.notify_one();
        }
    }
}

pub trait ServiceTask: Sync + Send {
    /// Get the service name
    fn get_service_name(&self) -> String;

    /// implement the service logic here
    fn run(&self, context: &ServiceContext) -> impl ::core::future::Future<Output = ()> + Send;

    /// override for custom behavior
    fn on_wait_end(&self) -> impl ::core::future::Future<Output = ()> + Send {
        async {
            // Default implementation does nothing
        }
    }

    /// Get join time for shutdown (default 90 seconds)
    fn get_join_time(&self) -> Duration {
        Duration::from_millis(90_000)
    }
}

/// Service thread implementation with lifecycle management
pub struct ServiceManager<T: ServiceTask + 'static> {
    /// The actual service implementation
    service: Arc<T>,

    /// Thread state management
    state: Arc<RwLock<ServiceLifecycle>>,

    /// Stop flag
    stopped: Arc<AtomicBool>,

    /// Started flag for restart capability
    started: Arc<AtomicBool>,

    /// Notification flag
    has_notified: Arc<AtomicBool>,

    /// Wait point for notifications
    wait_point: Arc<Notify>,

    /// Task handle for the running service
    task_handle: Arc<RwLock<Option<ServiceTaskHandle>>>,

    last_task_group_shutdown_report: Arc<RwLock<Option<ShutdownReport>>>,

    parent_task_group: Option<TaskGroup>,

    /// Whether this is a daemon service
    is_daemon: AtomicBool,
}

struct ServiceTaskHandle {
    task_id: TaskId,
    task_group: TaskGroup,
}

impl ServiceTaskHandle {
    fn new(task_id: TaskId, task_group: TaskGroup) -> Self {
        Self { task_id, task_group }
    }

    async fn shutdown(self, timeout_duration: Duration, interrupt: bool, service_name: &str) -> ShutdownReport {
        if interrupt {
            let aborted = self
                .task_group
                .abort_task_and_wait(self.task_id, timeout_duration)
                .await;
            if !aborted {
                warn!(
                    "Service thread {} interrupt did not finish before timeout",
                    service_name
                );
            }
            let report = self.task_group.shutdown(Duration::ZERO).await;
            if !report.is_healthy() {
                warn!(
                    "Service thread {} shutdown report is unhealthy: {}",
                    service_name,
                    report.to_json()
                );
            }
            return report;
        }

        let report = if self.task_group.wait_task(self.task_id, timeout_duration).await {
            self.task_group.shutdown(timeout_duration).await
        } else {
            warn!("Service thread {} shutdown timeout", service_name);
            self.task_group.shutdown(Duration::ZERO).await
        };
        if !report.is_healthy() {
            warn!(
                "Service thread {} shutdown report is unhealthy: {}",
                service_name,
                report.to_json()
            );
        }
        report
    }

    fn task_count(&self) -> usize {
        usize::from(self.task_group.contains_task(self.task_id))
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ServiceManagerLifecycleProbe {
    pub healthy: bool,
    pub task_count_before_shutdown: usize,
    pub task_count_after_shutdown: usize,
    pub task_group_count_before_shutdown: usize,
    pub task_group_count_after_shutdown: usize,
    pub task_group_completed: usize,
    pub task_group_cancelled: usize,
    pub task_group_aborted: usize,
    pub task_group_timed_out: usize,
    pub task_group_healthy: bool,
    pub shutdown_elapsed_us: u128,
}

fn spawn_service_task<F>(operation: &'static str, task_name: String, future: F) -> RuntimeResult<ServiceTaskHandle>
where
    F: Future<Output = ()> + Send + 'static,
{
    let handle = tokio::runtime::Handle::try_current().map_err(|error| RuntimeError::LifecycleOperation {
        operation,
        message: format!("requires a Tokio runtime: {error}"),
    })?;
    let task_group = TaskGroup::root("rocketmq.service-manager", RuntimeHandle::new(handle));
    spawn_service_task_with_group(operation, task_name, task_group, future)
}

fn spawn_service_task_with_task_group<F>(
    operation: &'static str,
    task_name: String,
    parent_task_group: TaskGroup,
    future: F,
) -> RuntimeResult<ServiceTaskHandle>
where
    F: Future<Output = ()> + Send + 'static,
{
    let task_group = parent_task_group.child("rocketmq.service-manager");
    spawn_service_task_with_group(operation, task_name, task_group, future)
}

fn spawn_service_task_with_group<F>(
    operation: &'static str,
    task_name: String,
    task_group: TaskGroup,
    future: F,
) -> RuntimeResult<ServiceTaskHandle>
where
    F: Future<Output = ()> + Send + 'static,
{
    let task_id = task_group
        .spawn_service(task_name, future)
        .map_err(|error| RuntimeError::LifecycleOperation {
            operation,
            message: format!("failed to spawn service task: {error}"),
        })?;
    Ok(ServiceTaskHandle::new(task_id, task_group))
}

impl<T: ServiceTask> AsRef<T> for ServiceManager<T> {
    fn as_ref(&self) -> &T {
        &self.service
    }
}

/// Service state enumeration
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ServiceLifecycle {
    NotStarted,
    Starting,
    Running,
    Stopping,
    Stopped,
}

impl<T: ServiceTask + 'static> ServiceManager<T> {
    /// Create new service thread implementation
    pub fn new(service: T) -> Self {
        Self::new_with_optional_task_group(Arc::new(service), None)
    }

    pub fn new_with_task_group(service: T, parent_task_group: TaskGroup) -> Self {
        Self::new_with_optional_task_group(Arc::new(service), Some(parent_task_group))
    }

    pub fn new_arc(service: Arc<T>) -> Self {
        Self::new_with_optional_task_group(service, None)
    }

    pub fn new_arc_with_task_group(service: Arc<T>, parent_task_group: TaskGroup) -> Self {
        Self::new_with_optional_task_group(service, Some(parent_task_group))
    }

    fn new_with_optional_task_group(service: Arc<T>, parent_task_group: Option<TaskGroup>) -> Self {
        Self {
            service,
            state: Arc::new(RwLock::new(ServiceLifecycle::NotStarted)),
            stopped: Arc::new(AtomicBool::new(false)),
            started: Arc::new(AtomicBool::new(false)),
            has_notified: Arc::new(AtomicBool::new(false)),
            wait_point: Arc::new(Notify::new()),
            task_handle: Arc::new(RwLock::new(None)),
            last_task_group_shutdown_report: Arc::new(RwLock::new(None)),
            parent_task_group,
            is_daemon: AtomicBool::new(false),
        }
    }

    /// Start the service thread
    pub async fn start(&self) -> RuntimeResult<()> {
        let service_name = self.service.get_service_name();

        info!(
            "Try to start service thread: {} started: {} current_state: {:?}",
            service_name,
            self.started.load(Ordering::Acquire),
            self.get_lifecycle_state().await
        );

        // Check if already started
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            warn!("Service thread {} is already started", service_name);
            return Ok(());
        }

        // Update state
        {
            let mut state = self.state.write().await;
            *state = ServiceLifecycle::Starting;
        }

        // Reset stopped flag
        self.stopped.store(false, Ordering::Release);

        // Clone necessary components for the task
        let service = self.service.clone();
        let state = self.state.clone();
        let stopped = self.stopped.clone();
        let started = self.started.clone();
        let has_notified = self.has_notified.clone();
        let wait_point = self.wait_point.clone();
        let task_handle = self.task_handle.clone();

        // Spawn the service task
        let future = async move {
            Self::run_internal(service, state, stopped, started, has_notified, wait_point).await;
        };
        let handle = match match self.parent_task_group.as_ref() {
            Some(parent_task_group) => spawn_service_task_with_task_group(
                "ServiceManager::start",
                service_name.clone(),
                parent_task_group.clone(),
                future,
            ),
            None => spawn_service_task("ServiceManager::start", service_name.clone(), future),
        } {
            Ok(handle) => handle,
            Err(error) => {
                self.started.store(false, Ordering::Release);
                self.has_notified.store(false, Ordering::Release);
                {
                    let mut state = self.state.write().await;
                    *state = ServiceLifecycle::NotStarted;
                }
                return Err(error);
            }
        };

        // Store the task handle
        {
            let mut handle_guard = task_handle.write().await;
            *handle_guard = Some(handle);
        }

        // Update state to running
        {
            let mut state = self.state.write().await;
            *state = ServiceLifecycle::Running;
        }

        info!(
            "Started service thread: {} started: {}",
            service_name,
            self.started.load(Ordering::Acquire)
        );

        Ok(())
    }

    /// Internal run method
    async fn run_internal(
        service: Arc<T>,
        state: Arc<RwLock<ServiceLifecycle>>,
        stopped: Arc<AtomicBool>,
        started: Arc<AtomicBool>,
        has_notified: Arc<AtomicBool>,
        wait_point: Arc<Notify>,
    ) {
        let service_name = service.get_service_name();
        info!("Service thread {} is running", service_name);

        // Set state to running
        {
            let mut state_guard = state.write().await;
            *state_guard = ServiceLifecycle::Running;
        }
        // Create context for the service
        let context = ServiceContext::new(wait_point.clone(), has_notified.clone(), stopped.clone());
        // Run the service
        service.run(&context).await;

        // Clean up after run completes
        started.store(false, Ordering::Release);
        stopped.store(true, Ordering::Release);
        has_notified.store(false, Ordering::Release);

        {
            let mut state_guard = state.write().await;
            *state_guard = ServiceLifecycle::Stopped;
        }

        info!("Service thread {} has stopped", service_name);
    }

    /// Shutdown the service
    pub async fn shutdown(&self) -> RuntimeResult<()> {
        self.shutdown_with_interrupt(false).await
    }

    /// Shutdown the service with optional interrupt
    pub async fn shutdown_with_interrupt(&self, interrupt: bool) -> RuntimeResult<()> {
        let service_name = self.service.get_service_name();

        info!(
            "Try to shutdown service thread: {} started: {} current_state: {:?}",
            service_name,
            self.started.load(Ordering::Acquire),
            self.get_lifecycle_state().await
        );

        // Check if not started
        if self
            .started
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            warn!("Service thread {} is not running", service_name);
            return Ok(());
        }

        // Update state
        {
            let mut state = self.state.write().await;
            *state = ServiceLifecycle::Stopping;
        }

        // Set stopped flag
        self.stopped.store(true, Ordering::Release);

        info!("Shutdown thread[{}] interrupt={}", service_name, interrupt);

        // Wake up if thread is waiting
        self.wakeup();

        let begin_time = Instant::now();

        // Wait for the task to complete
        let join_time = self.service.get_join_time();
        let result = if !self.is_daemon() {
            let handle = {
                let mut handle_guard = self.task_handle.write().await;
                handle_guard.take()
            };
            if let Some(handle) = handle {
                let report = handle.shutdown(join_time, interrupt, &service_name).await;
                *self.last_task_group_shutdown_report.write().await = Some(report);
                Ok(())
            } else {
                Ok(())
            }
        } else {
            Ok(())
        };

        let elapsed_time = begin_time.elapsed();
        info!(
            "Join thread[{}], elapsed time: {}ms, join time: {}ms",
            service_name,
            elapsed_time.as_millis(),
            join_time.as_millis()
        );

        // Update final state
        {
            let mut state = self.state.write().await;
            *state = ServiceLifecycle::Stopped;
        }

        result
    }

    /// Make the service stop (without waiting)
    pub fn make_stop(&self) {
        if !self.started.load(Ordering::Acquire) {
            return;
        }

        self.stopped.store(true, Ordering::Release);
        info!("Make stop thread[{}]", self.service.get_service_name());
    }

    /// Wake up the service thread
    pub fn wakeup(&self) {
        if self
            .has_notified
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.wait_point.notify_one();
        }
    }

    /// Wait for running with interval
    pub async fn wait_for_running(&self, interval: Duration) {
        // Check if already notified
        if self
            .has_notified
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.service.on_wait_end().await;
            return;
        }

        // Wait for notification or timeout
        let wait_result = timeout(interval, self.wait_point.notified()).await;

        // Reset notification flag
        self.has_notified.store(false, Ordering::Release);

        // Call on_wait_end regardless of how we were woken up
        self.service.on_wait_end().await;

        if wait_result.is_err() {
            // Timeout occurred - this is normal behavior
        }
    }

    /// Check if service is stopped
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    /// Check if service is daemon
    pub fn is_daemon(&self) -> bool {
        self.is_daemon.load(Ordering::Acquire)
    }

    /// Set daemon flag
    pub fn set_daemon(&self, daemon: bool) {
        self.is_daemon.store(daemon, Ordering::Release);
    }

    /// Get current service state
    pub async fn get_lifecycle_state(&self) -> ServiceLifecycle {
        *self.state.read().await
    }

    /// Check if service is started
    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }

    pub async fn task_count(&self) -> usize {
        self.task_handle
            .read()
            .await
            .as_ref()
            .map(ServiceTaskHandle::task_count)
            .unwrap_or_default()
    }

    pub async fn last_task_group_shutdown_report(&self) -> Option<ShutdownReport> {
        self.last_task_group_shutdown_report.read().await.clone()
    }
}

struct LifecycleProbeService;

impl ServiceTask for LifecycleProbeService {
    fn get_service_name(&self) -> String {
        "service-manager-lifecycle-probe".to_string()
    }

    async fn run(&self, context: &ServiceContext) {
        while !context.is_stopped() {
            context.wait_for_running(Duration::from_millis(1)).await;
        }
    }

    fn get_join_time(&self) -> Duration {
        Duration::from_secs(1)
    }
}

#[doc(hidden)]
pub async fn run_service_manager_lifecycle_probe() -> ServiceManagerLifecycleProbe {
    let manager = ServiceManager::new(LifecycleProbeService);
    let start_result = manager.start().await;
    let task_count_before_shutdown = manager.task_count().await;
    let task_group_count_before_shutdown = task_count_before_shutdown;

    let shutdown_started = Instant::now();
    let shutdown_result = manager.shutdown().await;
    let shutdown_elapsed_us = shutdown_started.elapsed().as_micros();
    let task_count_after_shutdown = manager.task_count().await;
    let report = manager.last_task_group_shutdown_report().await;
    let task_group_healthy = report.as_ref().is_some_and(ShutdownReport::is_healthy);
    let task_group_completed = report.as_ref().map(|report| report.completed).unwrap_or_default();
    let task_group_cancelled = report.as_ref().map(|report| report.cancelled).unwrap_or_default();
    let task_group_aborted = report.as_ref().map(|report| report.aborted).unwrap_or_default();
    let task_group_timed_out = report.as_ref().map(|report| report.timed_out).unwrap_or_default();
    let task_group_count_after_shutdown = manager.task_count().await;

    ServiceManagerLifecycleProbe {
        healthy: start_result.is_ok()
            && shutdown_result.is_ok()
            && task_count_before_shutdown == 1
            && task_count_after_shutdown == 0
            && task_group_healthy
            && task_group_completed == 1
            && task_group_cancelled == 0
            && task_group_timed_out == 0,
        task_count_before_shutdown,
        task_count_after_shutdown,
        task_group_count_before_shutdown,
        task_group_count_after_shutdown,
        task_group_completed,
        task_group_cancelled,
        task_group_aborted,
        task_group_timed_out,
        task_group_healthy,
        shutdown_elapsed_us,
    }
}

#[cfg(test)]
mod tests {
    use std::future;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use crate::RuntimeContext;
    use tokio::time::sleep;
    use tokio::time::Duration;

    use super::*;
    use crate::service_manager;

    /// Example implementation - Transaction Check Service
    pub struct ExampleTransactionCheckService {
        check_interval: Duration,
        transaction_timeout: Duration,
    }

    impl ExampleTransactionCheckService {
        pub fn new(check_interval: Duration, transaction_timeout: Duration) -> Self {
            Self {
                check_interval,
                transaction_timeout,
            }
        }
    }

    impl ServiceTask for ExampleTransactionCheckService {
        fn get_service_name(&self) -> String {
            "ExampleTransactionCheckService".to_string()
        }

        async fn run(&self, context: &ServiceContext) {
            info!("Start transaction check service thread!");

            while !context.is_stopped() {
                context.wait_for_running(self.check_interval).await;
            }

            info!("End transaction check service thread!");
        }

        async fn on_wait_end(&self) {
            let begin = Instant::now();
            info!("Begin to check prepare message, begin time: {:?}", begin);

            // Simulate transaction check work
            self.perform_transaction_check().await;

            let elapsed = begin.elapsed();
            info!("End to check prepare message, consumed time: {}ms", elapsed.as_millis());
        }
    }

    impl ExampleTransactionCheckService {
        async fn perform_transaction_check(&self) {
            // Simulate work
            sleep(Duration::from_millis(100)).await;
            info!(
                "Transaction check completed with timeout: {:?}",
                self.transaction_timeout
            );
        }
    }

    impl Clone for ExampleTransactionCheckService {
        fn clone(&self) -> Self {
            Self {
                check_interval: self.check_interval,
                transaction_timeout: self.transaction_timeout,
            }
        }
    }

    // Use the macro to add service thread functionality
    service_manager!(ExampleTransactionCheckService);

    #[derive(Clone)]
    struct TestService {
        name: String,
        work_duration: Duration,
    }

    impl TestService {
        fn new(name: String, work_duration: Duration) -> Self {
            Self { name, work_duration }
        }
    }

    impl ServiceTask for TestService {
        fn get_service_name(&self) -> String {
            self.name.clone()
        }

        async fn run(&self, context: &ServiceContext) {
            println!("TestService {} starting {}", self.name, context.is_stopped());

            let mut counter = 0;

            while !context.is_stopped() && counter < 5 {
                context.wait_for_running(Duration::from_millis(100)).await;
                println!("TestService {} running iteration {}", self.name, counter);
                counter += 1;
            }

            println!("TestService {} finished after {} iterations", self.name, counter);
        }

        async fn on_wait_end(&self) {
            println!("TestService {} performing work", self.name);
            sleep(self.work_duration).await;
            println!("TestService {} work completed", self.name);
        }
    }

    service_manager!(TestService);

    struct PendingService {
        dropped: Arc<AtomicBool>,
    }

    struct DropMarker {
        dropped: Arc<AtomicBool>,
    }

    impl Drop for DropMarker {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }

    impl ServiceTask for PendingService {
        fn get_service_name(&self) -> String {
            "pending-service".to_string()
        }

        async fn run(&self, _context: &ServiceContext) {
            let _marker = DropMarker {
                dropped: Arc::clone(&self.dropped),
            };
            future::pending::<()>().await;
        }

        fn get_join_time(&self) -> Duration {
            Duration::from_millis(10)
        }
    }

    #[test]
    fn spawn_service_task_without_tokio_runtime_returns_error() {
        let error = match spawn_service_task("test-service-start", "test-service".to_string(), async {}) {
            Ok(_) => panic!("spawning without a Tokio runtime should return an error"),
            Err(error) => error,
        };

        assert!(
            error.to_string().contains("requires a Tokio runtime"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn new_with_task_group_parents_service_task() {
        let context = RuntimeContext::from_current("service-manager-parent-test");
        let service_context = context.service_context("service-manager-service");
        let service = TestService::new("parented-service".to_string(), Duration::from_millis(10));
        let service_thread = ServiceManager::new_with_task_group(service, service_context.task_group().clone());

        service_thread.start().await.unwrap();
        service_thread.shutdown().await.unwrap();

        let report = service_context.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
        assert!(
            report
                .children
                .iter()
                .any(|child| child.name == "rocketmq.service-manager"),
            "{}",
            report.to_json()
        );
    }

    #[tokio::test]
    async fn test_service_lifecycle() {
        let service = TestService::new("test-service".to_string(), Duration::from_millis(50));
        let service_thread = service.create_service_task();

        // Test initial state
        assert_eq!(service_thread.get_lifecycle_state().await, ServiceLifecycle::NotStarted);
        assert!(!service_thread.is_started());
        assert!(!service_thread.is_stopped());

        // Test start
        service_thread.start().await.unwrap();
        assert_eq!(service_thread.get_lifecycle_state().await, ServiceLifecycle::Running);
        assert!(service_thread.is_started());
        assert!(!service_thread.is_stopped());

        // Let it run for a bit
        sleep(Duration::from_millis(300)).await;

        // Test wakeup
        service_thread.wakeup();
        sleep(Duration::from_millis(100)).await;

        // Test shutdown
        service_thread.shutdown().await.unwrap();
        assert_eq!(service_thread.get_lifecycle_state().await, ServiceLifecycle::Stopped);
        assert!(!service_thread.is_started());
        assert!(service_thread.is_stopped());
    }

    #[tokio::test]
    async fn test_daemon_service() {
        let service = TestService::new("daemon-service".to_string(), Duration::from_millis(10));
        let service_thread = service.create_service_task();

        // Set as daemon
        service_thread.set_daemon(true);
        assert!(service_thread.is_daemon());

        // Start and shutdown
        service_thread.start().await.unwrap();
        sleep(Duration::from_millis(100)).await;
        service_thread.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_start_attempts() {
        let service = TestService::new("multi-start-service".to_string(), Duration::from_millis(10));
        let service_thread = service.create_service_task();

        // First start should succeed
        service_thread.start().await.unwrap();
        assert!(service_thread.is_started());

        // Second start should be ignored
        service_thread.start().await.unwrap();
        assert!(service_thread.is_started());

        // Shutdown
        service_thread.shutdown().await.unwrap();
        assert!(!service_thread.is_started());
    }

    #[tokio::test]
    async fn shutdown_timeout_aborts_service_task() {
        let dropped = Arc::new(AtomicBool::new(false));
        let service_thread = ServiceManager::new(PendingService {
            dropped: Arc::clone(&dropped),
        });

        service_thread.start().await.unwrap();
        service_thread.shutdown().await.unwrap();

        assert!(
            dropped.load(Ordering::Acquire),
            "service future should be dropped after shutdown timeout aborts the task"
        );
        assert_eq!(service_thread.get_lifecycle_state().await, ServiceLifecycle::Stopped);
        assert!(!service_thread.is_started());
        assert!(service_thread.is_stopped());
        let report = service_thread
            .last_task_group_shutdown_report()
            .await
            .expect("service task group shutdown report should be recorded");
        assert!(!report.is_healthy(), "{}", report.to_json());
        assert_eq!(report.aborted, 1, "{}", report.to_json());
        assert_eq!(report.timed_out, 1, "{}", report.to_json());
    }

    #[tokio::test]
    async fn service_manager_lifecycle_probe_reports_clean_shutdown() {
        let probe = run_service_manager_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_before_shutdown, 1);
        assert_eq!(probe.task_count_after_shutdown, 0);
        assert_eq!(probe.task_group_count_before_shutdown, 1);
        assert_eq!(probe.task_group_count_after_shutdown, 0);
        assert_eq!(probe.task_group_completed, 1);
        assert_eq!(probe.task_group_cancelled, 0);
        assert_eq!(probe.task_group_aborted, 0);
        assert_eq!(probe.task_group_timed_out, 0);
        assert!(probe.task_group_healthy);
    }

    #[tokio::test]
    async fn test_make_stop() {
        let service = TestService::new("stop-service".to_string(), Duration::from_millis(10));
        let service_thread = service.create_service_task();

        service_thread.start().await.unwrap();
        sleep(Duration::from_millis(50)).await;

        // Make stop should set stopped flag
        service_thread.make_stop();
        assert!(service_thread.is_stopped());

        // Wait a bit for cleanup
        sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_example_transaction_service() {
        let service = ExampleTransactionCheckService::new(Duration::from_millis(100), Duration::from_millis(1000));
        let service_thread = service.create_service_task();

        service_thread.start().await.unwrap();
        sleep(Duration::from_millis(350)).await;
        service_thread.wakeup();
        sleep(Duration::from_millis(150)).await;
        service_thread.shutdown().await.unwrap();
    }
}
