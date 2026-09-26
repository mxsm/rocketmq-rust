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
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use parking_lot::Mutex;
use serde::Serialize;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::error::RuntimeError;
use crate::error::RuntimeResult;
use crate::operation::OperationContext;
use crate::shutdown_report::ShutdownReport;
use crate::task_group::TaskGroup;
use crate::task_group::TaskId;
use crate::task_group::TaskKind;

/// Identifies the schedule mode state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ScheduleMode {
    /// Waits `period` after each run completes.
    FixedDelay,
    /// Ticks every `period`; runs never overlap.
    FixedRateNoOverlap,
    /// Ticks every `period`; runs overlap up to the policy's bound.
    FixedRateAllowOverlap,
}

/// Maximum concurrent runs admitted for one bounded schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ScheduledTaskConcurrency {
    /// At most one run is active.
    Serial,
    /// At most `max_runs` runs are active.
    Bounded(NonZeroUsize),
}

/// Policy for ticks that arrive while all run slots are occupied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum MissedTickPolicy {
    /// Discard the missed tick.
    Skip,
    /// Retain at most one pending intent and run it when a slot becomes free.
    CoalesceLatest,
    /// Retain at most `max_pending` intents and run them as slots become free.
    BoundedCatchUp(NonZeroUsize),
}

/// Bounded execution policy for one scheduled task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ScheduledExecutionPolicy {
    /// Maximum concurrent runs.
    pub concurrency: ScheduledTaskConcurrency,
    /// Behavior for missed ticks.
    pub missed_ticks: MissedTickPolicy,
}

impl ScheduledExecutionPolicy {
    /// Creates a serial policy.
    #[must_use]
    pub const fn serial(missed_ticks: MissedTickPolicy) -> Self {
        Self {
            concurrency: ScheduledTaskConcurrency::Serial,
            missed_ticks,
        }
    }

    /// Creates a bounded-overlap policy.
    #[must_use]
    pub const fn bounded(concurrency: NonZeroUsize, missed_ticks: MissedTickPolicy) -> Self {
        Self {
            concurrency: ScheduledTaskConcurrency::Bounded(concurrency),
            missed_ticks,
        }
    }
}

impl Default for ScheduledExecutionPolicy {
    /// One run at a time; a tick that arrives while it runs is skipped.
    fn default() -> Self {
        Self::serial(MissedTickPolicy::Skip)
    }
}

/// Identifies the scheduled task control state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ScheduledTaskControl {
    /// Represents the continue case.
    Continue,
    /// Represents the stop case.
    Stop,
}

/// Describes the result of registering a scheduled task.
///
/// A duplicate schedule name is a normal outcome. The existing registration,
/// its driver, and its metrics remain unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduledTaskRegistrationOutcome {
    /// The task was registered and its driver was started.
    Scheduled(TaskId),
    /// A task with the requested name was already registered.
    AlreadyPresent,
}

/// Name, cadence and run limit of one schedule.
///
/// The mode chosen by the constructor is the only input that decides how
/// runs are timed and whether they may overlap. The policy passed at
/// registration must agree with it.
#[derive(Debug, Clone)]
pub struct ScheduledTaskConfig {
    /// The name value.
    pub name: String,
    /// Delay before the first run; zero means the first run is immediately due.
    pub initial_delay: Duration,
    /// Delay between completions or fixed-rate tick interval, according to mode.
    pub period: Duration,
    /// The mode value.
    pub mode: ScheduleMode,
    /// Optional limit on each asynchronous run, starting when it is polled.
    ///
    /// Expiry drops the run future; it cannot interrupt blocking work already
    /// started by that future.
    pub max_run_time: Option<Duration>,
}

impl ScheduledTaskConfig {
    /// Creates a schedule that waits `period` after each run completes.
    pub fn fixed_delay(name: impl Into<String>, period: Duration) -> Self {
        Self {
            name: name.into(),
            initial_delay: Duration::ZERO,
            period,
            mode: ScheduleMode::FixedDelay,
            max_run_time: None,
        }
    }

    /// Creates a schedule that ticks every `period` and never overlaps runs.
    pub fn fixed_rate_no_overlap(name: impl Into<String>, period: Duration) -> Self {
        Self {
            mode: ScheduleMode::FixedRateNoOverlap,
            ..Self::fixed_delay(name, period)
        }
    }

    /// Creates a schedule that ticks every `period` and overlaps runs up to
    /// the bound of its policy.
    pub fn fixed_rate(name: impl Into<String>, period: Duration) -> Self {
        Self {
            mode: ScheduleMode::FixedRateAllowOverlap,
            ..Self::fixed_delay(name, period)
        }
    }

    /// Sets the delay before the first run.
    #[must_use]
    pub fn with_initial_delay(mut self, initial_delay: Duration) -> Self {
        self.initial_delay = initial_delay;
        self
    }

    /// Checks the configuration against `policy` and returns the run limit.
    fn run_limit(&self, policy: ScheduledExecutionPolicy) -> RuntimeResult<usize> {
        if self.period.is_zero() {
            return Err(RuntimeError::unsupported(
                crate::RuntimeOperation::RegisterScheduledTask,
            ));
        }
        match (self.mode, policy.concurrency) {
            (ScheduleMode::FixedDelay | ScheduleMode::FixedRateNoOverlap, ScheduledTaskConcurrency::Serial) => Ok(1),
            (ScheduleMode::FixedRateAllowOverlap, ScheduledTaskConcurrency::Bounded(max_runs)) => Ok(max_runs.get()),
            (ScheduleMode::FixedDelay | ScheduleMode::FixedRateNoOverlap, ScheduledTaskConcurrency::Bounded(_))
            | (ScheduleMode::FixedRateAllowOverlap, ScheduledTaskConcurrency::Serial) => Err(
                RuntimeError::unsupported(crate::RuntimeOperation::RegisterScheduledTask),
            ),
        }
    }
}

/// Represents scheduled task group.
#[derive(Debug, Clone)]
pub struct ScheduledTaskGroup {
    group: TaskGroup,
    schedules: Arc<DashMap<Arc<str>, Arc<ScheduledTaskMetrics>>>,
}

/// Reads an explicitly selected set of schedules without retaining their owner.
///
/// Lookup cost is bounded by the names supplied at construction. A selected
/// schedule may be registered later; missing registrations are omitted.
#[derive(Debug, Clone)]
pub struct ScheduledTaskObserver {
    schedules: std::sync::Weak<DashMap<Arc<str>, Arc<ScheduledTaskMetrics>>>,
    names: Arc<[Arc<str>]>,
}

impl ScheduledTaskObserver {
    /// Reads the selected registrations using only in-memory metric state.
    pub fn snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        let Some(schedules) = self.schedules.upgrade() else {
            return Vec::new();
        };
        self.names
            .iter()
            .filter_map(|name| schedules.get(name).map(|entry| entry.value().snapshot()))
            .collect()
    }
}

#[derive(Debug)]
struct ScheduledTaskMetrics {
    config: ScheduledTaskConfig,
    max_concurrency: usize,
    pending_runs: AtomicU64,
    completion: Notify,
    active_runs: AtomicU64,
    runs: AtomicU64,
    skips: AtomicU64,
    overlaps: AtomicU64,
    failures: AtomicU64,
    last_drift_ms: AtomicU64,
    last_elapsed_ms: AtomicU64,
    max_elapsed_ms: AtomicU64,
}

/// Represents scheduled task snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct ScheduledTaskSnapshot {
    /// The name value.
    pub name: String,
    /// The mode value.
    pub mode: ScheduleMode,
    /// Whether running.
    pub running: bool,
    /// The active runs value.
    pub active_runs: u64,
    /// Runs that returned normally, including a controlled stop.
    pub runs: u64,
    /// The skips value.
    pub skips: u64,
    /// The overlaps value.
    pub overlaps: u64,
    /// Reserved runs that timed out, panicked, were cancelled, or could not start.
    pub failures: u64,
    /// The last drift duration in milliseconds.
    pub last_drift_ms: u64,
    /// The last elapsed duration in milliseconds.
    pub last_elapsed_ms: u64,
    /// The max elapsed duration in milliseconds.
    pub max_elapsed_ms: u64,
}

/// Where a schedule's driver and runs are registered.
#[derive(Clone, Copy)]
enum ScheduleBinding<'a> {
    /// The fixed component owner; its cancellation ends the schedule.
    Group,
    /// A bounded operation under the component owner.
    Operation(&'a OperationContext),
}

impl ScheduleBinding<'_> {
    fn cancellation_token(self, group: &TaskGroup) -> CancellationToken {
        match self {
            Self::Group => group.cancellation_token(),
            Self::Operation(operation) => operation.cancellation_token(),
        }
    }

    fn run_spawner(self, group: &TaskGroup) -> RunSpawner {
        RunSpawner {
            group: group.clone(),
            operation: match self {
                Self::Group => None,
                Self::Operation(operation) => Some(operation.with_task_kind(TaskKind::ScheduledRun)),
            },
        }
    }
}

/// Spawns the runs of one fixed-rate schedule.
struct RunSpawner {
    group: TaskGroup,
    operation: Option<OperationContext>,
}

impl RunSpawner {
    fn spawn<F>(&self, name: String, future: F) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        match &self.operation {
            Some(operation) => self.group.spawn_operation(operation, name, future),
            None => self.group.spawn(name, TaskKind::ScheduledRun, future),
        }
    }
}

/// One run of a registered task.
type ScheduledRun = Pin<Box<dyn Future<Output = ScheduledTaskControl> + Send + 'static>>;

/// A registered task after type erasure; each call starts the next run.
///
/// Erasing the task where it is registered compiles the drivers, the run
/// adapter and the task-group submission they use once in this crate instead
/// of once per registration site, and keeps a large closure or run future out
/// of the driver's own state.
type ScheduledTaskFn = Box<dyn FnMut() -> ScheduledRun + Send + 'static>;

/// Erases a task whose runs return nothing; every run continues the schedule.
fn erase_task<F, Fut>(mut task: F) -> ScheduledTaskFn
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Box::new(move || {
        let run = task();
        Box::pin(async move {
            run.await;
            ScheduledTaskControl::Continue
        })
    })
}

/// Erases a task whose runs decide whether the schedule continues.
fn erase_controlled_task<F, Fut>(mut task: F) -> ScheduledTaskFn
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ScheduledTaskControl> + Send + 'static,
{
    Box::new(move || Box::pin(task()))
}

impl ScheduledTaskGroup {
    /// Creates a new `ScheduledTaskGroup`.
    pub fn new(group: TaskGroup) -> Self {
        Self {
            group,
            // Registrations are rare, so a few shards suffice.
            schedules: Arc::new(DashMap::with_shard_amount(4)),
        }
    }

    /// Returns the group.
    pub fn group(&self) -> &TaskGroup {
        &self.group
    }

    /// Schedules `task` with the cadence of `config` and the run limits of `policy`.
    ///
    /// The driver and runs belong to this group and stop when it is cancelled.
    /// A fixed-delay schedule runs `task` on its driver; a fixed-rate schedule
    /// starts each run as its own task once a run slot is free. A duplicate
    /// name leaves the existing registration unchanged.
    ///
    /// # Errors
    ///
    /// Returns an unsupported error when the period is zero or `policy`
    /// contradicts the mode of `config`: fixed-delay and non-overlapping
    /// schedules need a serial policy, an overlapping schedule needs a bounded
    /// one. Returns an operational error when the driver cannot be spawned.
    pub fn schedule<F, Fut>(
        &self,
        config: ScheduledTaskConfig,
        policy: ScheduledExecutionPolicy,
        task: F,
    ) -> RuntimeResult<ScheduledTaskRegistrationOutcome>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.schedule_erased(ScheduleBinding::Group, config, policy, erase_task(task))
    }

    /// Schedules `task` like [`Self::schedule`] as part of a bounded operation.
    ///
    /// The driver and runs are registered with this group's fixed component
    /// owner and stop when the operation is cancelled or reaches its deadline.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::schedule`].
    pub fn schedule_operation<F, Fut>(
        &self,
        operation: &OperationContext,
        config: ScheduledTaskConfig,
        policy: ScheduledExecutionPolicy,
        task: F,
    ) -> RuntimeResult<ScheduledTaskRegistrationOutcome>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.schedule_erased(ScheduleBinding::Operation(operation), config, policy, erase_task(task))
    }

    /// Schedules fixed-delay work that can end its own schedule.
    ///
    /// The schedule stops after a run returns [`ScheduledTaskControl::Stop`].
    ///
    /// # Errors
    ///
    /// Returns an unsupported error when the period is zero or `config` is not
    /// a fixed-delay schedule, and an operational error when the driver cannot
    /// be spawned.
    pub fn schedule_controlled<F, Fut>(
        &self,
        config: ScheduledTaskConfig,
        task: F,
    ) -> RuntimeResult<ScheduledTaskRegistrationOutcome>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ScheduledTaskControl> + Send + 'static,
    {
        if config.mode != ScheduleMode::FixedDelay {
            return Err(RuntimeError::unsupported(
                crate::RuntimeOperation::RegisterScheduledTask,
            ));
        }
        config.run_limit(ScheduledExecutionPolicy::default())?;
        self.register_fixed_delay(ScheduleBinding::Group, config, erase_controlled_task(task))
    }

    fn schedule_erased(
        &self,
        binding: ScheduleBinding<'_>,
        config: ScheduledTaskConfig,
        policy: ScheduledExecutionPolicy,
        task: ScheduledTaskFn,
    ) -> RuntimeResult<ScheduledTaskRegistrationOutcome> {
        let max_concurrency = config.run_limit(policy)?;
        if config.mode == ScheduleMode::FixedDelay {
            return self.register_fixed_delay(binding, config, task);
        }
        self.register_fixed_rate(binding, config, max_concurrency, policy.missed_ticks, task)
    }

    fn register_fixed_delay(
        &self,
        binding: ScheduleBinding<'_>,
        config: ScheduledTaskConfig,
        task: ScheduledTaskFn,
    ) -> RuntimeResult<ScheduledTaskRegistrationOutcome> {
        let name: Arc<str> = Arc::from(config.name.as_str());
        let Some(metrics) = self.register(name.clone(), config.clone(), 1) else {
            return Ok(ScheduledTaskRegistrationOutcome::AlreadyPresent);
        };
        let token = binding.cancellation_token(&self.group);
        let driver = async move {
            fixed_delay_driver(token, config, metrics, task).await;
        };
        self.spawn_driver(binding, name, driver)
    }

    fn register_fixed_rate(
        &self,
        binding: ScheduleBinding<'_>,
        config: ScheduledTaskConfig,
        max_concurrency: usize,
        missed_ticks: MissedTickPolicy,
        task: ScheduledTaskFn,
    ) -> RuntimeResult<ScheduledTaskRegistrationOutcome> {
        let name: Arc<str> = Arc::from(config.name.as_str());
        let Some(metrics) = self.register(name.clone(), config.clone(), max_concurrency) else {
            return Ok(ScheduledTaskRegistrationOutcome::AlreadyPresent);
        };
        let token = binding.cancellation_token(&self.group);
        let runs = binding.run_spawner(&self.group);
        // Runs only lock the closure to create their future, never while it runs.
        let task = Arc::new(Mutex::new(task));
        let run_name: Arc<str> = Arc::from(format!("scheduled-run:{name}"));
        let driver = async move {
            fixed_rate_driver(token, config, metrics, missed_ticks, runs, task, run_name).await;
        };
        self.spawn_driver(binding, name, driver)
    }

    fn spawn_driver<F>(
        &self,
        binding: ScheduleBinding<'_>,
        name: Arc<str>,
        driver: F,
    ) -> RuntimeResult<ScheduledTaskRegistrationOutcome>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let driver_name = format!("scheduled-driver:{name}");
        let spawn_result = match binding {
            ScheduleBinding::Group => self.group.spawn(driver_name, TaskKind::ScheduledDriver, driver),
            ScheduleBinding::Operation(operation) => self.group.spawn_operation(
                &operation.with_task_kind(TaskKind::ScheduledDriver),
                driver_name,
                driver,
            ),
        };
        if spawn_result.is_err() {
            self.schedules.remove(&name);
        }
        spawn_result.map(ScheduledTaskRegistrationOutcome::Scheduled)
    }

    /// Returns the snapshot.
    pub fn snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.schedules.iter().map(|entry| entry.value().snapshot()).collect()
    }

    /// Observes a fixed selection of schedule names without keeping tasks alive.
    ///
    /// Duplicate names are removed. Callers should select component-level jobs,
    /// not names derived from requests or resources.
    pub fn observer(&self, names: &[&str]) -> ScheduledTaskObserver {
        let mut names: Vec<Arc<str>> = names.iter().map(|name| Arc::from(*name)).collect();
        names.sort_unstable();
        names.dedup();
        ScheduledTaskObserver {
            schedules: Arc::downgrade(&self.schedules),
            names: names.into(),
        }
    }

    /// Clears completed schedule registrations so a fixed component owner can
    /// start a new operation generation with the same schedule names.
    ///
    /// # Errors
    ///
    /// Returns an error while the component group still has active tasks.
    pub fn clear_completed(&self) -> RuntimeResult<()> {
        if self.group.task_count() != 0 {
            return Err(RuntimeError::context_unavailable(
                crate::RuntimeOperation::ClearCompletedSchedules,
            ));
        }
        self.schedules.clear();
        Ok(())
    }

    /// Shuts down the owned service.
    pub async fn shutdown(&self, timeout: Duration) -> ShutdownReport {
        self.group.shutdown(timeout).await
    }

    /// Shuts down the owned service using one absolute deadline.
    pub async fn shutdown_until(&self, deadline: crate::ShutdownDeadline) -> ShutdownReport {
        self.group.shutdown_until(deadline).await
    }

    fn register(
        &self,
        name: Arc<str>,
        config: ScheduledTaskConfig,
        max_concurrency: usize,
    ) -> Option<Arc<ScheduledTaskMetrics>> {
        let metrics = Arc::new(ScheduledTaskMetrics {
            config,
            max_concurrency,
            pending_runs: AtomicU64::new(0),
            completion: Notify::new(),
            active_runs: AtomicU64::new(0),
            runs: AtomicU64::new(0),
            skips: AtomicU64::new(0),
            overlaps: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            last_drift_ms: AtomicU64::new(0),
            last_elapsed_ms: AtomicU64::new(0),
            max_elapsed_ms: AtomicU64::new(0),
        });

        match self.schedules.entry(name.clone()) {
            Entry::Occupied(_) => None,
            Entry::Vacant(entry) => {
                entry.insert(metrics.clone());
                Some(metrics)
            }
        }
    }
}

impl ScheduledTaskMetrics {
    fn try_reserve_run(self: &Arc<Self>) -> Option<ScheduledRunGuard> {
        let mut active = self.active_runs.load(Ordering::Acquire);
        loop {
            if active >= self.max_concurrency as u64 {
                return None;
            }
            match self
                .active_runs
                .compare_exchange_weak(active, active + 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    if active > 0 {
                        self.overlaps.fetch_add(1, Ordering::Relaxed);
                    }
                    return Some(ScheduledRunGuard::reserved(self.clone()));
                }
                Err(observed) => active = observed,
            }
        }
    }

    fn take_pending_run(&self) -> bool {
        self.pending_runs
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| pending.checked_sub(1))
            .is_ok()
    }

    fn queue_missed_ticks(&self, missed: u64, policy: MissedTickPolicy) {
        if missed == 0 {
            return;
        }
        let skipped = match policy {
            MissedTickPolicy::Skip => missed,
            MissedTickPolicy::CoalesceLatest => {
                let previously_pending = self.pending_runs.swap(1, Ordering::AcqRel);
                if previously_pending == 0 {
                    missed.saturating_sub(1)
                } else {
                    missed
                }
            }
            MissedTickPolicy::BoundedCatchUp(limit) => {
                let limit = limit.get() as u64;
                let mut added = 0;
                while added < missed {
                    let pending = self.pending_runs.load(Ordering::Acquire);
                    if pending >= limit {
                        break;
                    }
                    if self
                        .pending_runs
                        .compare_exchange_weak(pending, pending + 1, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        added += 1;
                    }
                }
                missed.saturating_sub(added)
            }
        };
        self.skips.fetch_add(skipped, Ordering::Relaxed);
    }

    fn begin_serial_run(self: &Arc<Self>, expected_at: Instant) -> ScheduledRunGuard {
        self.active_runs.fetch_add(1, Ordering::AcqRel);
        self.record_drift(expected_at);
        ScheduledRunGuard::reserved(self.clone())
    }

    fn record_drift(&self, expected_at: Instant) {
        let drift_ms = Instant::now().saturating_duration_since(expected_at).as_millis() as u64;
        self.last_drift_ms.store(drift_ms, Ordering::Relaxed);
    }

    fn snapshot(&self) -> ScheduledTaskSnapshot {
        let active_runs = self.active_runs.load(Ordering::Acquire);
        ScheduledTaskSnapshot {
            name: self.config.name.clone(),
            mode: self.config.mode,
            running: active_runs > 0,
            active_runs,
            runs: self.runs.load(Ordering::Relaxed),
            skips: self.skips.load(Ordering::Relaxed),
            overlaps: self.overlaps.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            last_drift_ms: self.last_drift_ms.load(Ordering::Relaxed),
            last_elapsed_ms: self.last_elapsed_ms.load(Ordering::Relaxed),
            max_elapsed_ms: self.max_elapsed_ms.load(Ordering::Relaxed),
        }
    }
}

// Reservation owns settlement even before a submitted future is first polled.
// Drop covers construction/poll/destructor panics, rejection, and cancellation.
struct ScheduledRunGuard {
    metrics: Arc<ScheduledTaskMetrics>,
    started_at: Option<Instant>,
    outcome: ScheduledRunOutcome,
}

#[derive(Clone, Copy)]
enum ScheduledRunOutcome {
    Completed,
    TimedOut,
    Panicked,
    Cancelled,
    RejectedBeforeStart,
}

impl ScheduledRunGuard {
    fn reserved(metrics: Arc<ScheduledTaskMetrics>) -> Self {
        Self {
            metrics,
            started_at: None,
            outcome: ScheduledRunOutcome::RejectedBeforeStart,
        }
    }

    fn start(&mut self) {
        self.started_at = Some(Instant::now());
        self.outcome = ScheduledRunOutcome::Cancelled;
    }

    fn finish(mut self, timed_out: bool) {
        self.outcome = if timed_out {
            ScheduledRunOutcome::TimedOut
        } else {
            ScheduledRunOutcome::Completed
        };
    }
}

impl Drop for ScheduledRunGuard {
    fn drop(&mut self) {
        if let Some(started_at) = self.started_at {
            let elapsed_ms = started_at.elapsed().as_millis() as u64;
            self.metrics.last_elapsed_ms.store(elapsed_ms, Ordering::Relaxed);
            self.metrics.max_elapsed_ms.fetch_max(elapsed_ms, Ordering::Relaxed);
        }
        let outcome = if std::thread::panicking() {
            ScheduledRunOutcome::Panicked
        } else {
            self.outcome
        };
        match outcome {
            ScheduledRunOutcome::Completed => {
                self.metrics.runs.fetch_add(1, Ordering::Relaxed);
            }
            ScheduledRunOutcome::TimedOut
            | ScheduledRunOutcome::Panicked
            | ScheduledRunOutcome::Cancelled
            | ScheduledRunOutcome::RejectedBeforeStart => {
                self.metrics.failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.metrics.active_runs.fetch_sub(1, Ordering::AcqRel);
        self.metrics.completion.notify_one();
    }
}

/// Starts one reserved run; returns `false` if the owner rejected it.
fn spawn_bounded_run(
    runs: &RunSpawner,
    mut run: ScheduledRunGuard,
    task: &Arc<Mutex<ScheduledTaskFn>>,
    name: &Arc<str>,
    max_run_time: Option<Duration>,
) -> bool {
    let task = task.clone();
    runs.spawn(name.to_string(), async move {
        run.start();
        let future = {
            let mut task = task.lock();
            (task)()
        };
        // Only a controlled fixed-delay schedule can stop itself.
        let (_, timed_out) = run_with_optional_timeout(future, max_run_time).await;
        run.finish(timed_out);
    })
    .is_ok()
}

async fn fixed_rate_driver(
    token: CancellationToken,
    config: ScheduledTaskConfig,
    metrics: Arc<ScheduledTaskMetrics>,
    missed_ticks: MissedTickPolicy,
    runs: RunSpawner,
    task: Arc<Mutex<ScheduledTaskFn>>,
    run_name: Arc<str>,
) {
    let period = config.period;
    let max_run_time = config.max_run_time;
    let mut expected_tick = Instant::now() + config.initial_delay;
    loop {
        if token.is_cancelled() {
            return;
        }
        tokio::select! {
            biased;
            _ = token.cancelled() => return,
            _ = metrics.completion.notified() => {}
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(expected_tick)) => {
                let now = Instant::now();
                metrics.record_drift(expected_tick);
                let overdue = now.saturating_duration_since(expected_tick);
                let total_missed = 1u64.saturating_add(
                    u64::try_from(overdue.as_nanos() / period.as_nanos()).unwrap_or(u64::MAX),
                );
                let mut remaining = total_missed;
                while remaining > 0 {
                    let Some(run) = metrics.try_reserve_run() else { break; };
                    if !spawn_bounded_run(&runs, run, &task, &run_name, max_run_time) {
                        break;
                    }
                    remaining -= 1;
                }
                metrics.queue_missed_ticks(remaining, missed_ticks);
                let advance = period.saturating_mul(u32::try_from(total_missed).unwrap_or(u32::MAX));
                expected_tick = expected_tick.checked_add(advance).unwrap_or(now);
            }
        }
        while metrics.pending_runs.load(Ordering::Acquire) > 0 {
            let Some(run) = metrics.try_reserve_run() else {
                break;
            };
            if !metrics.take_pending_run() {
                break;
            }
            if !spawn_bounded_run(&runs, run, &task, &run_name, max_run_time) {
                break;
            }
        }
    }
}

/// Awaits `run` within `max_run_time`; returns its control and whether it timed out.
async fn run_with_optional_timeout(run: ScheduledRun, max_run_time: Option<Duration>) -> (ScheduledTaskControl, bool) {
    if let Some(timeout) = max_run_time {
        match tokio::time::timeout(timeout, run).await {
            Ok(control) => (control, false),
            Err(_) => (ScheduledTaskControl::Continue, true),
        }
    } else {
        (run.await, false)
    }
}

// Group and operation schedules share timing and settlement; their driver
// registration keeps the distinct component or operation cancellation boundary.
async fn fixed_delay_driver(
    token: CancellationToken,
    config: ScheduledTaskConfig,
    metrics: Arc<ScheduledTaskMetrics>,
    mut task: ScheduledTaskFn,
) {
    if !sleep_or_cancel(&token, config.initial_delay).await {
        return;
    }

    loop {
        if token.is_cancelled() {
            return;
        }

        let started_at = Instant::now();
        let mut run = metrics.begin_serial_run(started_at);
        run.start();
        let (control, timed_out) = run_with_optional_timeout(task(), config.max_run_time).await;
        run.finish(timed_out);
        if control == ScheduledTaskControl::Stop {
            return;
        }

        if !sleep_or_cancel(&token, config.period).await {
            return;
        }
    }
}

async fn sleep_or_cancel(token: &CancellationToken, duration: Duration) -> bool {
    if duration.is_zero() {
        return !token.is_cancelled();
    }

    tokio::select! {
        _ = token.cancelled() => false,
        _ = tokio::time::sleep(duration) => true,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::Semaphore;

    use super::*;
    use crate::RuntimeContext;

    fn panicking_run(construction: bool) -> impl Future<Output = ()> + Send {
        assert!(!construction, "injected run construction panic");
        async { panic!("injected run poll panic") }
    }

    fn panicking_controlled_run(construction: bool) -> impl Future<Output = ScheduledTaskControl> + Send {
        assert!(!construction, "injected controlled construction panic");
        async { panic!("injected controlled poll panic") }
    }

    fn two() -> NonZeroUsize {
        NonZeroUsize::new(2).unwrap()
    }

    #[tokio::test]
    async fn every_schedule_entry_settles_construction_and_poll_panics() {
        for construction in [false, true] {
            for entry in 0..6 {
                let context = RuntimeContext::from_current("scheduled-panic");
                let scheduled = ScheduledTaskGroup::new(context.root_group().clone());
                let operation = OperationContext::without_deadline(TaskKind::ScheduledDriver);
                let delay = ScheduledTaskConfig::fixed_delay("panic", Duration::from_secs(60));
                let serial = ScheduledTaskConfig::fixed_rate_no_overlap("panic", Duration::from_secs(60));
                let overlapping = ScheduledTaskConfig::fixed_rate("panic", Duration::from_secs(60));
                let result = match entry {
                    0 => scheduled.schedule(delay, ScheduledExecutionPolicy::default(), move || {
                        panicking_run(construction)
                    }),
                    1 => scheduled.schedule(serial, ScheduledExecutionPolicy::default(), move || {
                        panicking_run(construction)
                    }),
                    2 => scheduled.schedule(
                        overlapping,
                        ScheduledExecutionPolicy::bounded(two(), MissedTickPolicy::Skip),
                        move || panicking_run(construction),
                    ),
                    3 => scheduled.schedule_controlled(delay, move || panicking_controlled_run(construction)),
                    4 => scheduled.schedule_operation(
                        &operation,
                        delay,
                        ScheduledExecutionPolicy::default(),
                        move || panicking_run(construction),
                    ),
                    _ => scheduled.schedule_operation(
                        &operation,
                        serial,
                        ScheduledExecutionPolicy::default(),
                        move || panicking_run(construction),
                    ),
                };
                result.unwrap();
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        let snapshot = &scheduled.snapshot()[0];
                        if snapshot.failures == 1 && snapshot.active_runs == 0 {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "entry {entry}, construction {construction} did not settle: {:?}",
                        scheduled.snapshot()
                    )
                });
                let snapshot = &scheduled.snapshot()[0];
                assert_eq!(snapshot.active_runs, 0, "entry {entry}, construction {construction}");
                assert!(!snapshot.running);
                assert_eq!(snapshot.failures, 1);
                assert_eq!(snapshot.runs, 0);
                let report = context.shutdown_tasks(Duration::from_secs(1)).await;
                assert_eq!(report.panicked, 1);
                assert_eq!(report.leaked, 0);
            }
        }
    }

    #[tokio::test]
    async fn a_policy_that_contradicts_the_mode_is_rejected() {
        let context = RuntimeContext::from_current("scheduled-policy-contract");
        let scheduled = ScheduledTaskGroup::new(context.root_group().clone());
        let period = Duration::from_secs(60);
        let bounded = ScheduledExecutionPolicy::bounded(two(), MissedTickPolicy::Skip);
        let serial = ScheduledExecutionPolicy::default();

        for (config, policy) in [
            (ScheduledTaskConfig::fixed_delay("delay-bounded", period), bounded),
            (
                ScheduledTaskConfig::fixed_rate_no_overlap("no-overlap-bounded", period),
                bounded,
            ),
            (ScheduledTaskConfig::fixed_rate("overlap-serial", period), serial),
            (ScheduledTaskConfig::fixed_delay("zero-period", Duration::ZERO), serial),
        ] {
            let name = config.name.clone();
            let error = scheduled.schedule(config, policy, || async {}).unwrap_err();
            assert_eq!(
                error.condition(),
                rocketmq_error::CanonicalCondition::Unimplemented,
                "{name}: {error}"
            );
        }
        let error = scheduled
            .schedule_controlled(
                ScheduledTaskConfig::fixed_rate_no_overlap("controlled-rate", period),
                || async { ScheduledTaskControl::Stop },
            )
            .unwrap_err();
        assert_eq!(error.condition(), rocketmq_error::CanonicalCondition::Unimplemented);
        assert!(scheduled.snapshot().is_empty());
        assert!(context.shutdown_tasks(Duration::from_secs(1)).await.is_healthy());
    }

    #[tokio::test]
    async fn a_reserved_run_settles_if_rejected_or_aborted_before_first_poll() {
        let context = RuntimeContext::from_current("run-guard");
        let scheduled = ScheduledTaskGroup::new(context.root_group().clone());
        let name: Arc<str> = Arc::from("guard");
        let metrics = scheduled
            .register(
                name.clone(),
                ScheduledTaskConfig::fixed_rate_no_overlap("guard", Duration::from_secs(1)),
                1,
            )
            .unwrap();
        let run = metrics.try_reserve_run().unwrap();
        let (id, handle) = scheduled
            .group
            .spawn_with_handle("never-polled", TaskKind::ScheduledRun, async move {
                let mut run = run;
                run.start();
                std::future::pending::<()>().await;
                run.finish(false);
            })
            .unwrap();
        handle.abort();
        assert!(handle.await.unwrap_err().is_cancelled());
        assert!(scheduled.group.wait_task(id, Duration::ZERO).await);
        assert_eq!(metrics.snapshot().active_runs, 0);
        assert_eq!(metrics.snapshot().failures, 1);
        assert!(context.shutdown_tasks(Duration::from_secs(1)).await.is_healthy());
        let run = metrics.try_reserve_run().unwrap();
        let runs = ScheduleBinding::Group.run_spawner(&scheduled.group);
        assert!(!spawn_bounded_run(
            &runs,
            run,
            &Arc::new(Mutex::new(erase_task(|| async {}))),
            &name,
            None
        ));
        assert_eq!(metrics.snapshot().active_runs, 0);
        assert_eq!(metrics.snapshot().failures, 2);
    }

    #[tokio::test]
    async fn operation_cancellation_settles_an_active_scheduled_run() {
        let context = RuntimeContext::from_current("scheduled-operation-cancel");
        let scheduled = ScheduledTaskGroup::new(context.root_group().clone());
        let operation = OperationContext::without_deadline(TaskKind::ScheduledDriver);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut started = Some(started_tx);
        scheduled
            .schedule_operation(
                &operation,
                ScheduledTaskConfig::fixed_delay("cancel", Duration::from_secs(1)),
                ScheduledExecutionPolicy::default(),
                move || {
                    let started = started.take();
                    async move {
                        if let Some(started) = started {
                            let _ = started.send(());
                        }
                        std::future::pending::<()>().await;
                    }
                },
            )
            .unwrap();
        started_rx.await.unwrap();
        assert!(operation
            .cancel_and_wait(&scheduled.group, Duration::from_secs(1))
            .await
            .unwrap());
        assert_eq!(scheduled.snapshot()[0].active_runs, 0);
        assert_eq!(scheduled.snapshot()[0].failures, 1);
        assert_eq!(scheduled.snapshot()[0].runs, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn fixed_delay_waits_for_the_first_delay_and_then_for_each_completion() {
        for operation_owned in [false, true] {
            let context = RuntimeContext::from_current("fixed-delay-contract");
            let scheduled = ScheduledTaskGroup::new(context.root_group().clone());
            let operation = OperationContext::without_deadline(TaskKind::ScheduledDriver);
            let config = ScheduledTaskConfig::fixed_delay("serial", Duration::from_secs(3))
                .with_initial_delay(Duration::from_secs(2));
            let release = Arc::new(Semaphore::new(0));
            let task_release = release.clone();
            let (starts, mut started) = tokio::sync::mpsc::unbounded_channel();
            let mut count = 0;
            let task = move || {
                count += 1;
                let count = count;
                let starts = starts.clone();
                let release = task_release.clone();
                async move {
                    starts.send((count, tokio::time::Instant::now())).unwrap();
                    if count == 1 {
                        release.acquire().await.unwrap().forget();
                    }
                }
            };
            let begin = tokio::time::Instant::now();
            let result = if operation_owned {
                scheduled.schedule_operation(&operation, config.clone(), ScheduledExecutionPolicy::default(), task)
            } else {
                scheduled.schedule(config.clone(), ScheduledExecutionPolicy::default(), task)
            };
            assert!(matches!(
                result.unwrap(),
                ScheduledTaskRegistrationOutcome::Scheduled(_)
            ));
            assert_eq!(
                scheduled
                    .schedule(config, ScheduledExecutionPolicy::default(), || async {
                        panic!("duplicate ran")
                    })
                    .unwrap(),
                ScheduledTaskRegistrationOutcome::AlreadyPresent,
            );
            let (count, first) = started.recv().await.unwrap();
            assert_eq!(count, 1);
            assert_eq!(first - begin, Duration::from_secs(2));
            tokio::time::advance(Duration::from_secs(10)).await;
            assert!(started.try_recv().is_err());
            assert_eq!(scheduled.snapshot()[0].active_runs, 1);
            let completion = tokio::time::Instant::now();
            release.add_permits(1);
            let (count, second) = started.recv().await.unwrap();
            assert_eq!(count, 2);
            assert_eq!(second - completion, Duration::from_secs(3));
            assert!(context.shutdown_tasks(Duration::from_secs(1)).await.is_healthy());
            let snapshot = &scheduled.snapshot()[0];
            assert_eq!(snapshot.runs, 2);
            assert_eq!(snapshot.active_runs, 0);
            assert_eq!(snapshot.failures, 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_controlled_schedule_ends_after_a_stop() {
        let context = RuntimeContext::from_current("scheduled-controlled-stop");
        let scheduled = ScheduledTaskGroup::new(context.root_group().clone());
        let calls = Arc::new(AtomicUsize::new(0));
        let task_calls = calls.clone();
        scheduled
            .schedule_controlled(
                ScheduledTaskConfig::fixed_delay("self-stopping", Duration::from_secs(1)),
                move || {
                    let call = task_calls.fetch_add(1, Ordering::AcqRel) + 1;
                    async move {
                        if call == 2 {
                            ScheduledTaskControl::Stop
                        } else {
                            ScheduledTaskControl::Continue
                        }
                    }
                },
            )
            .unwrap();

        for _ in 0..5 {
            for _ in 0..20 {
                tokio::task::yield_now().await;
            }
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::Acquire), 2);
        let snapshot = &scheduled.snapshot()[0];
        assert_eq!(snapshot.runs, 2);
        assert_eq!(snapshot.active_runs, 0);
        assert_eq!(scheduled.group().task_count(), 0, "the driver ends after a stop");
        assert!(context.shutdown_tasks(Duration::from_secs(1)).await.is_healthy());
    }

    #[tokio::test(start_paused = true)]
    async fn a_timed_out_run_releases_its_slot_once_before_the_next_controlled_run() {
        let context = RuntimeContext::from_current("scheduled-timeout");
        let scheduled = ScheduledTaskGroup::new(context.root_group().clone());
        let (second_tx, second_rx) = tokio::sync::oneshot::channel();
        let mut second_tx = Some(second_tx);
        let mut calls = 0;
        let mut config = ScheduledTaskConfig::fixed_delay("timeout", Duration::from_secs(1));
        config.max_run_time = Some(Duration::from_secs(1));
        scheduled
            .schedule_controlled(config, move || {
                calls += 1;
                let second = if calls == 2 { second_tx.take() } else { None };
                async move {
                    if let Some(second) = second {
                        let _ = second.send(());
                        ScheduledTaskControl::Stop
                    } else {
                        std::future::pending().await
                    }
                }
            })
            .unwrap();
        second_rx.await.unwrap();
        let report = context.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy());
        let snapshot = &scheduled.snapshot()[0];
        assert_eq!(snapshot.active_runs, 0);
        assert_eq!(snapshot.runs, 1);
        assert_eq!(snapshot.failures, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn coalesced_serial_schedule_never_exceeds_one_active_and_one_pending_run() {
        let context = RuntimeContext::try_from_current("bounded-schedule").unwrap();
        let scheduled = ScheduledTaskGroup::new(context.root_group().clone());
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));

        let task_active = active.clone();
        let task_max_active = max_active.clone();
        let task_calls = calls.clone();
        let task_release = release.clone();
        scheduled
            .schedule(
                ScheduledTaskConfig::fixed_rate_no_overlap("bounded-coalesce", Duration::from_secs(1)),
                ScheduledExecutionPolicy::serial(MissedTickPolicy::CoalesceLatest),
                move || {
                    let task_active = task_active.clone();
                    let task_max_active = task_max_active.clone();
                    let task_calls = task_calls.clone();
                    let task_release = task_release.clone();
                    async move {
                        let current = task_active.fetch_add(1, Ordering::AcqRel) + 1;
                        task_max_active.fetch_max(current, Ordering::AcqRel);
                        task_calls.fetch_add(1, Ordering::AcqRel);
                        let permit = task_release.acquire().await.unwrap();
                        drop(permit);
                        task_active.fetch_sub(1, Ordering::AcqRel);
                    }
                },
            )
            .unwrap();

        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::Acquire), 1);

        tokio::time::advance(Duration::from_secs(5)).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        let snapshot = scheduled
            .snapshot()
            .into_iter()
            .find(|snapshot| snapshot.name == "bounded-coalesce")
            .unwrap();
        assert_eq!(snapshot.active_runs, 1);
        assert!(snapshot.skips >= 4);
        assert_eq!(calls.load(Ordering::Acquire), 1);

        release.add_permits(1);
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::Acquire), 2);
        assert_eq!(max_active.load(Ordering::Acquire), 1);

        release.add_permits(1);
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(active.load(Ordering::Acquire), 0);
        assert!(context.shutdown_tasks(Duration::from_secs(1)).await.is_healthy());
    }

    #[tokio::test(start_paused = true)]
    async fn overlapping_runs_stay_within_the_policy_bound() {
        let context = RuntimeContext::from_current("scheduled-overlap-bound");
        let scheduled = ScheduledTaskGroup::new(context.root_group().clone());
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));

        let task_active = active.clone();
        let task_max_active = max_active.clone();
        let task_release = release.clone();
        scheduled
            .schedule(
                ScheduledTaskConfig::fixed_rate("overlap", Duration::from_secs(1)),
                ScheduledExecutionPolicy::bounded(two(), MissedTickPolicy::Skip),
                move || {
                    let task_active = task_active.clone();
                    let task_max_active = task_max_active.clone();
                    let task_release = task_release.clone();
                    async move {
                        let current = task_active.fetch_add(1, Ordering::AcqRel) + 1;
                        task_max_active.fetch_max(current, Ordering::AcqRel);
                        drop(task_release.acquire().await.unwrap());
                        task_active.fetch_sub(1, Ordering::AcqRel);
                    }
                },
            )
            .unwrap();

        for _ in 0..5 {
            tokio::time::advance(Duration::from_secs(1)).await;
            for _ in 0..20 {
                tokio::task::yield_now().await;
            }
        }
        let snapshot = &scheduled.snapshot()[0];
        assert_eq!(max_active.load(Ordering::Acquire), 2);
        assert_eq!(snapshot.active_runs, 2);
        assert_eq!(snapshot.overlaps, 1);
        assert!(snapshot.skips >= 3, "{snapshot:?}");

        release.add_permits(2);
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert!(context.shutdown_tasks(Duration::from_secs(1)).await.is_healthy());
    }
}
