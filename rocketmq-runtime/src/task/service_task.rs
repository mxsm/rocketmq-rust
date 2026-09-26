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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::time::timeout;
use tracing::info;
use tracing::warn;

use crate::config::DEFAULT_SHUTDOWN_TIMEOUT;
use crate::shutdown_deadline::ABORT_CONFIRMATION_TIMEOUT;
use crate::RuntimeError;
use crate::RuntimeResult;
use crate::ShutdownDeadline;
use crate::ShutdownReport;
use crate::TaskGroup;
use crate::TaskId;

/// Lifecycle state of a [`ServiceManager`] and the loop it runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceTaskState {
    /// The service has never been started.
    NotStarted,
    /// `start` accepted the request and is spawning the loop.
    Starting,
    /// The loop is running.
    Running,
    /// A stop was requested and the loop has not finished yet.
    Stopping,
    /// The loop finished, or shutdown completed.
    Stopped,
}

impl ServiceTaskState {
    const fn as_bits(self) -> u64 {
        match self {
            Self::NotStarted => 0,
            Self::Starting => 1,
            Self::Running => 2,
            Self::Stopping => 3,
            Self::Stopped => 4,
        }
    }

    const fn from_bits(bits: u64) -> Self {
        match bits {
            0 => Self::NotStarted,
            1 => Self::Starting,
            2 => Self::Running,
            3 => Self::Stopping,
            _ => Self::Stopped,
        }
    }
}

const STATE_BITS: u32 = 8;
const STATE_MASK: u64 = (1 << STATE_BITS) - 1;
const GENERATION_MASK: u64 = u64::MAX >> STATE_BITS;

fn pack(generation: u64, state: ServiceTaskState) -> u64 {
    (generation << STATE_BITS) | state.as_bits()
}

fn generation_of(word: u64) -> u64 {
    word >> STATE_BITS
}

fn state_of(word: u64) -> ServiceTaskState {
    ServiceTaskState::from_bits(word & STATE_MASK)
}

/// Control state shared by a [`ServiceManager`] and the loop it runs.
///
/// One atomic word holds the state together with the generation of the
/// current run. `start` begins a new generation, so a loop left over from an
/// earlier run that outlived its shutdown sees itself as stopped and cannot
/// overwrite the state of the run that replaced it.
#[derive(Debug)]
struct ServiceSignals {
    word: AtomicU64,
    has_notified: AtomicBool,
    wait_point: Notify,
}

impl ServiceSignals {
    fn new() -> Self {
        Self {
            word: AtomicU64::new(pack(0, ServiceTaskState::NotStarted)),
            has_notified: AtomicBool::new(false),
            wait_point: Notify::new(),
        }
    }

    fn state(&self) -> ServiceTaskState {
        state_of(self.word.load(Ordering::Acquire))
    }

    /// Starts a new generation from `NotStarted` or `Stopped`.
    ///
    /// Returns the replaced word and the new generation, or the current state
    /// when a run is still active.
    fn begin_start(&self) -> Result<(u64, u64), ServiceTaskState> {
        let mut word = self.word.load(Ordering::Acquire);
        loop {
            let state = state_of(word);
            if !matches!(state, ServiceTaskState::NotStarted | ServiceTaskState::Stopped) {
                return Err(state);
            }
            let generation = generation_of(word).wrapping_add(1) & GENERATION_MASK;
            match self.word.compare_exchange_weak(
                word,
                pack(generation, ServiceTaskState::Starting),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok((word, generation)),
                Err(actual) => word = actual,
            }
        }
    }

    /// Moves `generation` from `from` to `to`; does nothing if either changed.
    fn transition(&self, generation: u64, from: ServiceTaskState, to: ServiceTaskState) -> bool {
        self.word
            .compare_exchange(
                pack(generation, from),
                pack(generation, to),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Marks `generation` stopped unless a newer run already replaced it.
    fn finish(&self, generation: u64) -> bool {
        self.word
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |word| {
                (generation_of(word) == generation).then(|| pack(generation, ServiceTaskState::Stopped))
            })
            .is_ok()
    }

    /// Requests a stop of the current run and returns its state and generation.
    fn request_stop(&self) -> (ServiceTaskState, u64) {
        let mut word = self.word.load(Ordering::Acquire);
        loop {
            let state = state_of(word);
            let generation = generation_of(word);
            if !matches!(state, ServiceTaskState::Starting | ServiceTaskState::Running) {
                return (state, generation);
            }
            match self.word.compare_exchange_weak(
                word,
                pack(generation, ServiceTaskState::Stopping),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return (state, generation),
                Err(actual) => word = actual,
            }
        }
    }

    fn is_stopped_for(&self, generation: u64) -> bool {
        let word = self.word.load(Ordering::Acquire);
        generation_of(word) != generation
            || matches!(state_of(word), ServiceTaskState::Stopping | ServiceTaskState::Stopped)
    }

    fn wakeup(&self) {
        if self
            .has_notified
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.wait_point.notify_one();
        }
    }
}

/// Stops the run it belongs to when the loop returns or is aborted.
struct StopOnExit {
    signals: Arc<ServiceSignals>,
    generation: u64,
}

impl Drop for StopOnExit {
    fn drop(&mut self) {
        if self.signals.finish(self.generation) {
            self.signals.has_notified.store(false, Ordering::Release);
        }
    }
}

/// Control handle passed to [`ServiceTask::run`].
pub struct ServiceTaskContext {
    signals: Arc<ServiceSignals>,
    generation: u64,
}

impl ServiceTaskContext {
    /// Creates a context that is not stopped and has no pending wakeup.
    ///
    /// [`ServiceManager`] creates the context it passes to [`ServiceTask::run`];
    /// this constructor lets a test drive a service loop directly.
    pub fn new() -> Self {
        Self {
            signals: Arc::new(ServiceSignals::new()),
            generation: 0,
        }
    }

    /// Returns whether this run was asked to stop, or was replaced by a newer run.
    pub fn is_stopped(&self) -> bool {
        self.signals.is_stopped_for(self.generation)
    }

    /// Waits until woken up or until `interval` elapses.
    ///
    /// Returns at once when a wakeup is already pending. Always returns
    /// `true`, after which the loop does its `on_wait_end` work.
    pub async fn wait_for_running(&self, interval: Duration) -> bool {
        if self
            .signals
            .has_notified
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return true;
        }

        // A timeout is the normal way to leave the wait.
        let _ = timeout(interval, self.signals.wait_point.notified()).await;
        self.signals.has_notified.store(false, Ordering::Release);
        true
    }

    /// Wakes the loop if it is waiting, or makes its next wait return at once.
    pub fn wakeup(&self) {
        self.signals.wakeup();
    }
}

impl Default for ServiceTaskContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Defines service task behavior.
pub trait ServiceTask: Sync + Send {
    /// Get the service name
    fn get_service_name(&self) -> String;

    /// implement the service logic here
    fn run(&self, context: &ServiceTaskContext) -> impl ::core::future::Future<Output = ()> + Send;

    /// override for custom behavior
    fn on_wait_end(&self) -> impl ::core::future::Future<Output = ()> + Send {
        async {
            // Default implementation does nothing
        }
    }
}

/// Runs a [`ServiceTask`] loop as a service task of an owned task group.
///
/// Each run is spawned in a `rocketmq.service-manager` child of the task group
/// given at construction, so the owner's shutdown report and diagnostics
/// include it. A stopped manager can be started again.
///
/// Shutdown waits no longer than the earliest of the deadline passed to
/// [`Self::shutdown_until`] and the deadline installed on the parent task
/// group by an owner that is shutting down. When neither exists, it uses the
/// runtime's default shutdown budget.
pub struct ServiceManager<T: ServiceTask + 'static> {
    service: Arc<T>,
    signals: Arc<ServiceSignals>,
    task_handle: Mutex<Option<ServiceTaskHandle>>,
    last_task_group_shutdown_report: Mutex<Option<ShutdownReport>>,
    parent_task_group: TaskGroup,
}

struct ServiceTaskHandle {
    task_id: TaskId,
    task_group: TaskGroup,
}

impl ServiceTaskHandle {
    async fn shutdown(self, deadline: ShutdownDeadline, interrupt: bool, service_name: &str) -> ShutdownReport {
        let report = if interrupt {
            // The abort needs a window to confirm even after the deadline passed.
            let confirmation = deadline.remaining().max(ABORT_CONFIRMATION_TIMEOUT);
            if !self.task_group.abort_task_and_wait(self.task_id, confirmation).await {
                warn!(
                    "Service thread {} interrupt did not finish before timeout",
                    service_name
                );
            }
            self.task_group.shutdown(Duration::ZERO).await
        } else if self.task_group.wait_task(self.task_id, deadline.remaining()).await {
            self.task_group.shutdown_until(deadline).await
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
}

fn spawn_service_task<F>(
    parent_task_group: &TaskGroup,
    task_name: String,
    future: F,
) -> RuntimeResult<ServiceTaskHandle>
where
    F: Future<Output = ()> + Send + 'static,
{
    let task_group = parent_task_group.component("rocketmq.service-manager");
    let task_id = task_group
        .spawn_service(task_name, future)
        .map_err(|error| RuntimeError::within(crate::RuntimeOperation::SpawnServiceTask, error))?;
    Ok(ServiceTaskHandle { task_id, task_group })
}

impl<T: ServiceTask> AsRef<T> for ServiceManager<T> {
    fn as_ref(&self) -> &T {
        &self.service
    }
}

impl<T: ServiceTask + 'static> ServiceManager<T> {
    /// Creates a manager whose runs are owned by `parent_task_group`.
    pub fn new_with_task_group(service: T, parent_task_group: TaskGroup) -> Self {
        Self::new_arc_with_task_group(Arc::new(service), parent_task_group)
    }

    /// Creates a manager for a shared service whose runs are owned by
    /// `parent_task_group`.
    pub fn new_arc_with_task_group(service: Arc<T>, parent_task_group: TaskGroup) -> Self {
        Self {
            service,
            signals: Arc::new(ServiceSignals::new()),
            task_handle: Mutex::new(None),
            last_task_group_shutdown_report: Mutex::new(None),
            parent_task_group,
        }
    }

    /// Starts a run of the service loop.
    ///
    /// Starting a manager whose loop is still active only logs a warning.
    ///
    /// # Errors
    ///
    /// Returns an error if the parent task group no longer accepts work.
    pub async fn start(&self) -> RuntimeResult<()> {
        let service_name = self.service.get_service_name();
        let (replaced, generation) = match self.signals.begin_start() {
            Ok(started) => started,
            Err(state) => {
                warn!(
                    "Service thread {} is already started, current_state: {:?}",
                    service_name, state
                );
                return Ok(());
            }
        };
        info!("Try to start service thread: {}", service_name);

        let service = self.service.clone();
        let signals = self.signals.clone();
        // The service loop is created on the heap by the worker's first poll. An
        // inline loop would make this future as large as the service's state,
        // and unoptimized builds copy it at every hop down to the task group.
        let future = async move {
            Box::pin(Self::run_internal(service, signals, generation)).await;
        };
        let handle = match spawn_service_task(&self.parent_task_group, service_name.clone(), future) {
            Ok(handle) => handle,
            Err(error) => {
                // Nothing was spawned, so restore the state this start replaced.
                let _ = self.signals.word.compare_exchange(
                    pack(generation, ServiceTaskState::Starting),
                    replaced,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                return Err(error);
            }
        };

        // A run that ended on its own leaves its task group behind; release it.
        let previous = self.task_handle.lock().replace(handle);
        if let Some(previous) = previous {
            let _ = previous.task_group.shutdown_now();
        }
        self.signals
            .transition(generation, ServiceTaskState::Starting, ServiceTaskState::Running);

        info!("Started service thread: {}", service_name);
        Ok(())
    }

    async fn run_internal(service: Arc<T>, signals: Arc<ServiceSignals>, generation: u64) {
        let service_name = service.get_service_name();
        let _stop_on_exit = StopOnExit {
            signals: signals.clone(),
            generation,
        };
        info!("Service thread {} is running", service_name);
        signals.transition(generation, ServiceTaskState::Starting, ServiceTaskState::Running);

        let context = ServiceTaskContext { signals, generation };
        service.run(&context).await;

        info!("Service thread {} has stopped", service_name);
    }

    /// Stops the service and waits for its loop.
    ///
    /// The wait ends at the deadline installed on the parent task group, or
    /// after the runtime's default shutdown budget when there is none. A loop
    /// still running then is aborted.
    ///
    /// # Errors
    ///
    /// Currently always succeeds; the result is kept for callers that
    /// propagate shutdown failures.
    pub async fn shutdown(&self) -> RuntimeResult<()> {
        self.shutdown_inner(false, None).await
    }

    /// Stops the service like [`Self::shutdown`], aborting the loop at once
    /// when `interrupt` is set.
    ///
    /// # Errors
    ///
    /// Currently always succeeds; the result is kept for callers that
    /// propagate shutdown failures.
    pub async fn shutdown_with_interrupt(&self, interrupt: bool) -> RuntimeResult<()> {
        self.shutdown_inner(interrupt, None).await
    }

    /// Stops the service and waits for its loop no later than `deadline`.
    ///
    /// # Errors
    ///
    /// Currently always succeeds; the result is kept for callers that
    /// propagate shutdown failures.
    pub async fn shutdown_until(&self, deadline: ShutdownDeadline) -> RuntimeResult<()> {
        self.shutdown_inner(false, Some(deadline)).await
    }

    /// Stops the service no later than `deadline`, aborting the loop at once
    /// when `interrupt` is set.
    ///
    /// # Errors
    ///
    /// Currently always succeeds; the result is kept for callers that
    /// propagate shutdown failures.
    pub async fn shutdown_with_interrupt_until(
        &self,
        interrupt: bool,
        deadline: ShutdownDeadline,
    ) -> RuntimeResult<()> {
        self.shutdown_inner(interrupt, Some(deadline)).await
    }

    async fn shutdown_inner(&self, interrupt: bool, requested: Option<ShutdownDeadline>) -> RuntimeResult<()> {
        let service_name = self.service.get_service_name();
        let handle = self.task_handle.lock().take();
        let (state, generation) = self.signals.request_stop();
        if handle.is_none()
            && !matches!(
                state,
                ServiceTaskState::Starting | ServiceTaskState::Running | ServiceTaskState::Stopping
            )
        {
            warn!("Service thread {} is not running", service_name);
            return Ok(());
        }

        info!("Shutdown thread[{}] interrupt={}", service_name, interrupt);
        self.signals.wakeup();

        let deadline = self.shutdown_deadline(requested);
        let begin_time = Instant::now();
        if let Some(handle) = handle {
            let report = handle.shutdown(deadline, interrupt, &service_name).await;
            *self.last_task_group_shutdown_report.lock() = Some(report);
        }
        info!(
            "Join thread[{}], elapsed time: {}ms",
            service_name,
            begin_time.elapsed().as_millis()
        );

        self.signals.finish(generation);
        Ok(())
    }

    fn shutdown_deadline(&self, requested: Option<ShutdownDeadline>) -> ShutdownDeadline {
        match (requested, self.parent_task_group.shutdown_deadline()) {
            (Some(requested), Some(installed)) => requested.earliest(installed),
            (Some(deadline), None) | (None, Some(deadline)) => deadline,
            (None, None) => ShutdownDeadline::after(DEFAULT_SHUTDOWN_TIMEOUT),
        }
    }

    /// Asks the loop to stop without waiting for it.
    pub fn make_stop(&self) {
        let (state, _) = self.signals.request_stop();
        if matches!(state, ServiceTaskState::Starting | ServiceTaskState::Running) {
            info!("Make stop thread[{}]", self.service.get_service_name());
        }
    }

    /// Wakes the loop if it is waiting, or makes its next wait return at once.
    pub fn wakeup(&self) {
        self.signals.wakeup();
    }

    /// Returns the current lifecycle state.
    pub fn get_lifecycle_state(&self) -> ServiceTaskState {
        self.signals.state()
    }

    /// Returns the shutdown report of the task group used by the last stopped run.
    pub fn last_task_group_shutdown_report(&self) -> Option<ShutdownReport> {
        self.last_task_group_shutdown_report.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use std::future;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use tokio::sync::Notify;
    use tokio::time::Duration;

    use super::*;
    use crate::RuntimeContext;

    /// Counts loop iterations and signals each one.
    struct CountingService {
        iterations: Arc<AtomicUsize>,
        iterated: Arc<Notify>,
    }

    impl ServiceTask for CountingService {
        fn get_service_name(&self) -> String {
            "counting-service".to_string()
        }

        async fn run(&self, context: &ServiceTaskContext) {
            while !context.is_stopped() {
                context.wait_for_running(Duration::from_secs(60)).await;
                self.on_wait_end().await;
            }
        }

        async fn on_wait_end(&self) {
            self.iterations.fetch_add(1, Ordering::AcqRel);
            self.iterated.notify_one();
        }
    }

    struct PendingService {
        entered: Arc<Notify>,
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

        async fn run(&self, _context: &ServiceTaskContext) {
            let _marker = DropMarker {
                dropped: Arc::clone(&self.dropped),
            };
            self.entered.notify_one();
            future::pending::<()>().await;
        }
    }

    fn counting_service() -> (CountingService, Arc<AtomicUsize>, Arc<Notify>) {
        let iterations = Arc::new(AtomicUsize::new(0));
        let iterated = Arc::new(Notify::new());
        (
            CountingService {
                iterations: iterations.clone(),
                iterated: iterated.clone(),
            },
            iterations,
            iterated,
        )
    }

    /// Returns a service that never stops on its own, with its entry signal
    /// and a flag set when its loop future is dropped.
    fn pending_service() -> (PendingService, Arc<Notify>, Arc<AtomicBool>) {
        let entered = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        (
            PendingService {
                entered: entered.clone(),
                dropped: dropped.clone(),
            },
            entered,
            dropped,
        )
    }

    #[tokio::test]
    async fn service_runs_under_its_parent_task_group() {
        let context = RuntimeContext::from_current("service-manager-parent-test");
        let service_context = context.service_context("service-manager-service");
        let (service, _, _) = counting_service();
        let manager = ServiceManager::new_with_task_group(service, service_context.task_group().clone());

        manager.start().await.unwrap();
        assert_eq!(service_context.task_group().component_count(), 1);
        manager.shutdown().await.unwrap();
        let child_report = manager
            .last_task_group_shutdown_report()
            .expect("service manager shutdown report should exist");
        assert_eq!(child_report.name, "rocketmq.service-manager");
        assert!(child_report.is_healthy(), "{}", child_report.to_json());

        let report = service_context.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
        assert!(report.children.is_empty(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn lifecycle_moves_through_running_and_stopped() {
        let context = RuntimeContext::from_current("service-manager-lifecycle-test");
        let service_context = context.service_context("service-manager-lifecycle");
        let (service, iterations, iterated) = counting_service();
        let manager = ServiceManager::new_with_task_group(service, service_context.task_group().clone());
        assert_eq!(manager.get_lifecycle_state(), ServiceTaskState::NotStarted);

        manager.start().await.unwrap();
        assert_eq!(manager.get_lifecycle_state(), ServiceTaskState::Running);

        let iteration = iterated.notified();
        manager.wakeup();
        iteration.await;
        assert!(iterations.load(Ordering::Acquire) >= 1);

        manager.shutdown().await.unwrap();
        assert_eq!(manager.get_lifecycle_state(), ServiceTaskState::Stopped);
    }

    #[tokio::test]
    async fn second_start_while_running_is_ignored() {
        let context = RuntimeContext::from_current("service-manager-multi-start-test");
        let service_context = context.service_context("service-manager-multi-start");
        let (service, _, _) = counting_service();
        let manager = ServiceManager::new_with_task_group(service, service_context.task_group().clone());

        manager.start().await.unwrap();
        manager.start().await.unwrap();
        assert_eq!(service_context.task_group().component_count(), 1);
        assert_eq!(manager.get_lifecycle_state(), ServiceTaskState::Running);

        manager.shutdown().await.unwrap();
        assert_eq!(manager.get_lifecycle_state(), ServiceTaskState::Stopped);
    }

    #[tokio::test]
    async fn stopped_manager_can_start_again() {
        let context = RuntimeContext::from_current("service-manager-restart-test");
        let service_context = context.service_context("service-manager-restart");
        let (service, _, _) = counting_service();
        let manager = ServiceManager::new_with_task_group(service, service_context.task_group().clone());

        manager.start().await.unwrap();
        manager.shutdown().await.unwrap();
        manager.start().await.unwrap();
        assert_eq!(manager.get_lifecycle_state(), ServiceTaskState::Running);
        manager.shutdown().await.unwrap();
        assert_eq!(manager.get_lifecycle_state(), ServiceTaskState::Stopped);

        let report = service_context.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn make_stop_ends_the_loop_without_shutdown() {
        let context = RuntimeContext::from_current("service-manager-make-stop-test");
        let service_context = context.service_context("service-manager-make-stop");
        let (service, _, _) = counting_service();
        let manager = ServiceManager::new_with_task_group(service, service_context.task_group().clone());

        manager.start().await.unwrap();
        manager.make_stop();
        assert_eq!(manager.get_lifecycle_state(), ServiceTaskState::Stopping);
        manager.wakeup();

        let report = service_context.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
        assert_eq!(manager.get_lifecycle_state(), ServiceTaskState::Stopped);
    }

    #[tokio::test]
    async fn shutdown_deadline_aborts_a_loop_that_does_not_stop() {
        let context = RuntimeContext::from_current("service-manager-deadline-test");
        let service_context = context.service_context("service-manager-deadline");
        let (service, entered, dropped) = pending_service();
        let manager = ServiceManager::new_with_task_group(service, service_context.task_group().clone());

        manager.start().await.unwrap();
        entered.notified().await;
        let started = std::time::Instant::now();
        manager
            .shutdown_until(ShutdownDeadline::after(Duration::from_millis(20)))
            .await
            .unwrap();

        assert!(
            started.elapsed() < Duration::from_secs(5),
            "shutdown should stop at the deadline instead of a fixed join time"
        );
        assert!(
            dropped.load(Ordering::Acquire),
            "service future should be dropped after the deadline aborts the task"
        );
        assert_eq!(manager.get_lifecycle_state(), ServiceTaskState::Stopped);
        let report = manager
            .last_task_group_shutdown_report()
            .expect("service task group shutdown report should be recorded");
        assert!(!report.is_healthy(), "{}", report.to_json());
        assert_eq!(report.aborted, 1, "{}", report.to_json());
        assert_eq!(report.timed_out, 1, "{}", report.to_json());
    }

    #[tokio::test]
    async fn shutdown_follows_the_deadline_installed_by_the_parent_owner() {
        let context = RuntimeContext::from_current("service-manager-installed-deadline-test");
        let service_context = context.service_context("service-manager-installed-deadline");
        let (service, entered, dropped) = pending_service();
        let manager = ServiceManager::new_with_task_group(service, service_context.task_group().clone());
        manager.start().await.unwrap();
        entered.notified().await;

        let started = std::time::Instant::now();
        // The owner installs its deadline when the shutdown call is made, so
        // the manager's unbounded shutdown below inherits it.
        let owner_shutdown = service_context.task_group().shutdown(Duration::from_millis(20));
        let (report, stopped) = tokio::join!(owner_shutdown, manager.shutdown());
        stopped.unwrap();

        assert!(
            started.elapsed() < Duration::from_secs(5),
            "an unbounded shutdown call should inherit the owner's deadline"
        );
        assert!(dropped.load(Ordering::Acquire));
        assert!(!report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn interrupt_aborts_the_loop_at_once() {
        let context = RuntimeContext::from_current("service-manager-interrupt-test");
        let service_context = context.service_context("service-manager-interrupt");
        let (service, entered, dropped) = pending_service();
        let manager = ServiceManager::new_with_task_group(service, service_context.task_group().clone());

        manager.start().await.unwrap();
        entered.notified().await;
        manager.shutdown_with_interrupt(true).await.unwrap();

        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(manager.get_lifecycle_state(), ServiceTaskState::Stopped);
    }

    #[test]
    fn a_replaced_run_sees_itself_stopped() {
        let signals = Arc::new(ServiceSignals::new());
        let (_, first) = signals.begin_start().expect("first run starts");
        let old = ServiceTaskContext {
            signals: signals.clone(),
            generation: first,
        };
        signals.transition(first, ServiceTaskState::Starting, ServiceTaskState::Running);
        assert!(!old.is_stopped());

        signals.request_stop();
        assert!(signals.finish(first));
        let (_, second) = signals.begin_start().expect("second run starts");
        signals.transition(second, ServiceTaskState::Starting, ServiceTaskState::Running);

        assert!(old.is_stopped(), "a loop from the first run must not keep running");
        assert!(!signals.finish(first), "the first run must not stop the second");
        assert_eq!(signals.state(), ServiceTaskState::Running);
    }

    #[tokio::test]
    async fn standalone_context_returns_at_once_for_a_pending_wakeup() {
        let context = ServiceTaskContext::new();
        assert!(!context.is_stopped());
        context.wakeup();
        assert!(context.wait_for_running(Duration::from_secs(60)).await);
    }
}
