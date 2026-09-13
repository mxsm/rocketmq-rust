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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
/// Identifies the schedule mode state.
pub enum ScheduleMode {
    /// Represents the fixed delay case.
    FixedDelay,
    /// Represents the fixed rate no overlap case.
    FixedRateNoOverlap,
    /// Represents the fixed rate allow overlap case.
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

    fn max_concurrency(self) -> usize {
        match self.concurrency {
            ScheduledTaskConcurrency::Serial => 1,
            ScheduledTaskConcurrency::Bounded(concurrency) => concurrency.get(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
/// Identifies the scheduled task control state.
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

#[derive(Debug, Clone)]
/// Represents scheduled task config.
pub struct ScheduledTaskConfig {
    /// The name value.
    pub name: String,
    /// The initial delay value.
    pub initial_delay: Duration,
    /// The period value.
    pub period: Duration,
    /// The mode value.
    pub mode: ScheduleMode,
    /// The max run time value.
    pub max_run_time: Option<Duration>,
    /// The shutdown timeout value.
    pub shutdown_timeout: Duration,
}

impl ScheduledTaskConfig {
    /// Creates the fixed delay value.
    pub fn fixed_delay(name: impl Into<String>, period: Duration) -> Self {
        Self {
            name: name.into(),
            initial_delay: Duration::ZERO,
            period,
            mode: ScheduleMode::FixedDelay,
            max_run_time: None,
            shutdown_timeout: Duration::from_secs(30),
        }
    }

    /// Creates the fixed rate no overlap value.
    pub fn fixed_rate_no_overlap(name: impl Into<String>, period: Duration) -> Self {
        Self {
            mode: ScheduleMode::FixedRateNoOverlap,
            ..Self::fixed_delay(name, period)
        }
    }

    /// Creates the fixed rate value.
    pub fn fixed_rate(name: impl Into<String>, period: Duration) -> Self {
        Self {
            mode: ScheduleMode::FixedRateAllowOverlap,
            ..Self::fixed_delay(name, period)
        }
    }
}

#[derive(Debug, Clone)]
/// Represents scheduled task group.
pub struct ScheduledTaskGroup {
    group: TaskGroup,
    schedules: Arc<DashMap<Arc<str>, Arc<ScheduledTaskMetrics>>>,
}

#[derive(Debug)]
struct ScheduledTaskMetrics {
    config: ScheduledTaskConfig,
    max_concurrency: usize,
    pending_runs: AtomicU64,
    completion: Notify,
    running: AtomicBool,
    active_runs: AtomicU64,
    runs: AtomicU64,
    skips: AtomicU64,
    overlaps: AtomicU64,
    failures: AtomicU64,
    last_drift_ms: AtomicU64,
    last_elapsed_ms: AtomicU64,
    max_elapsed_ms: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
/// Represents scheduled task snapshot.
pub struct ScheduledTaskSnapshot {
    /// The name value.
    pub name: String,
    /// The mode value.
    pub mode: ScheduleMode,
    /// Whether running.
    pub running: bool,
    /// The active runs value.
    pub active_runs: u64,
    /// The runs value.
    pub runs: u64,
    /// The skips value.
    pub skips: u64,
    /// The overlaps value.
    pub overlaps: u64,
    /// The failures value.
    pub failures: u64,
    /// The last drift duration in milliseconds.
    pub last_drift_ms: u64,
    /// The last elapsed duration in milliseconds.
    pub last_elapsed_ms: u64,
    /// The max elapsed duration in milliseconds.
    pub max_elapsed_ms: u64,
}

impl ScheduledTaskGroup {
    /// Creates a new `ScheduledTaskGroup`.
    pub fn new(group: TaskGroup) -> Self {
        Self {
            group,
            schedules: Arc::new(DashMap::new()),
        }
    }

    /// Returns the group.
    pub fn group(&self) -> &TaskGroup {
        &self.group
    }

    /// Returns the schedule fixed delay.
    ///
    /// # Errors
    ///
    /// Returns an operational error when the task driver cannot be spawned.
    pub fn schedule_fixed_delay<F, Fut>(
        &self,
        config: ScheduledTaskConfig,
        mut task: F,
    ) -> RuntimeResult<ScheduledTaskRegistrationOutcome>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.schedule_fixed_delay_controlled(config, move || {
            let future = task();
            async move {
                future.await;
                ScheduledTaskControl::Continue
            }
        })
    }

    /// Schedules fixed-delay work as part of a bounded operation.
    ///
    /// The driver is registered directly with this group's fixed component
    /// owner and stops when the operation is cancelled or reaches its
    /// deadline.
    ///
    /// # Errors
    ///
    /// Returns an operational error when the bounded task driver cannot be
    /// spawned.
    pub fn schedule_fixed_delay_operation<F, Fut>(
        &self,
        operation: &OperationContext,
        config: ScheduledTaskConfig,
        mut task: F,
    ) -> RuntimeResult<ScheduledTaskRegistrationOutcome>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.schedule_fixed_delay_controlled_operation(operation, config, move || {
            let future = task();
            async move {
                future.await;
                ScheduledTaskControl::Continue
            }
        })
    }

    /// Schedules controlled fixed-delay work as part of a bounded operation.
    ///
    /// # Errors
    ///
    /// Returns an operational error when the bounded task driver cannot be
    /// spawned.
    pub fn schedule_fixed_delay_controlled_operation<F, Fut>(
        &self,
        operation: &OperationContext,
        mut config: ScheduledTaskConfig,
        mut task: F,
    ) -> RuntimeResult<ScheduledTaskRegistrationOutcome>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ScheduledTaskControl> + Send + 'static,
    {
        config.mode = ScheduleMode::FixedDelay;
        let name: Arc<str> = Arc::from(config.name.as_str());
        let name_for_cleanup = name.clone();
        let Some(metrics) = self.register(name.clone(), config.clone(), 1) else {
            return Ok(ScheduledTaskRegistrationOutcome::AlreadyPresent);
        };
        let token = operation.cancellation_token();
        let driver = operation.with_task_kind(TaskKind::ScheduledDriver);
        let spawn_result = self
            .group
            .spawn_operation(&driver, format!("scheduled-driver:{name}"), async move {
                if !sleep_or_cancel(&token, config.initial_delay).await {
                    return;
                }

                loop {
                    if token.is_cancelled() {
                        return;
                    }

                    let started_at = Instant::now();
                    metrics.begin_serial_run(started_at);
                    let (control, timed_out) = run_controlled_with_optional_timeout(task(), config.max_run_time).await;
                    metrics.finish_run(started_at, timed_out);
                    if control == ScheduledTaskControl::Stop {
                        return;
                    }

                    if !sleep_or_cancel(&token, config.period).await {
                        return;
                    }
                }
            });
        if spawn_result.is_err() {
            self.schedules.remove(&name_for_cleanup);
        }
        spawn_result.map(ScheduledTaskRegistrationOutcome::Scheduled)
    }

    /// Schedules fixed-rate, non-overlapping work as part of a bounded operation.
    ///
    /// # Errors
    ///
    /// Returns an operational error when the bounded task driver cannot be
    /// spawned.
    pub fn schedule_fixed_rate_no_overlap_operation<F, Fut>(
        &self,
        operation: &OperationContext,
        mut config: ScheduledTaskConfig,
        task: F,
    ) -> RuntimeResult<ScheduledTaskRegistrationOutcome>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        config.mode = ScheduleMode::FixedRateNoOverlap;
        let name: Arc<str> = Arc::from(config.name.as_str());
        let name_for_cleanup = name.clone();
        let Some(metrics) = self.register(name.clone(), config.clone(), 1) else {
            return Ok(ScheduledTaskRegistrationOutcome::AlreadyPresent);
        };
        let token = operation.cancellation_token();
        let driver = operation.with_task_kind(TaskKind::ScheduledDriver);
        let run_operation = operation.with_task_kind(TaskKind::ScheduledRun);
        let run_group = self.group.clone();
        let task = Arc::new(task);

        let spawn_result = self
            .group
            .spawn_operation(&driver, format!("scheduled-driver:{name}"), async move {
                if !sleep_or_cancel(&token, config.initial_delay).await {
                    return;
                }

                let mut expected_tick = Instant::now();
                loop {
                    if token.is_cancelled() {
                        return;
                    }

                    if !metrics.try_begin_no_overlap_run(expected_tick) {
                        expected_tick = next_expected_tick(expected_tick, config.period);
                    } else {
                        let run_name = format!("scheduled-run:{name}");
                        let run_metrics = metrics.clone();
                        let run_task = task.clone();
                        let max_run_time = config.max_run_time;
                        let spawn_result = run_group.spawn_operation(&run_operation, run_name, async move {
                            let started_at = Instant::now();
                            let timed_out = run_with_optional_timeout(run_task(), max_run_time).await;
                            run_metrics.finish_run(started_at, timed_out);
                        });
                        if spawn_result.is_err() {
                            metrics.rollback_started_run();
                        }
                        expected_tick = next_expected_tick(expected_tick, config.period);
                    }

                    if !sleep_or_cancel(&token, config.period).await {
                        return;
                    }
                }
            });
        if spawn_result.is_err() {
            self.schedules.remove(&name_for_cleanup);
        }
        spawn_result.map(ScheduledTaskRegistrationOutcome::Scheduled)
    }

    /// Returns the schedule fixed delay controlled.
    ///
    /// # Errors
    ///
    /// Returns an operational error when the task driver cannot be spawned.
    pub fn schedule_fixed_delay_controlled<F, Fut>(
        &self,
        mut config: ScheduledTaskConfig,
        mut task: F,
    ) -> RuntimeResult<ScheduledTaskRegistrationOutcome>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ScheduledTaskControl> + Send + 'static,
    {
        config.mode = ScheduleMode::FixedDelay;
        let name: Arc<str> = Arc::from(config.name.as_str());
        let name_for_cleanup = name.clone();
        let Some(metrics) = self.register(name.clone(), config.clone(), 1) else {
            return Ok(ScheduledTaskRegistrationOutcome::AlreadyPresent);
        };
        let token = self.group.cancellation_token();
        let spawn_result = self.group.spawn(
            format!("scheduled-driver:{name}"),
            TaskKind::ScheduledDriver,
            async move {
                if !sleep_or_cancel(&token, config.initial_delay).await {
                    return;
                }

                loop {
                    if token.is_cancelled() {
                        return;
                    }

                    let started_at = Instant::now();
                    metrics.begin_serial_run(started_at);
                    let (control, timed_out) = run_controlled_with_optional_timeout(task(), config.max_run_time).await;
                    metrics.finish_run(started_at, timed_out);
                    if control == ScheduledTaskControl::Stop {
                        return;
                    }

                    if !sleep_or_cancel(&token, config.period).await {
                        return;
                    }
                }
            },
        );
        if spawn_result.is_err() {
            self.schedules.remove(&name_for_cleanup);
        }
        spawn_result.map(ScheduledTaskRegistrationOutcome::Scheduled)
    }

    /// Returns the schedule fixed rate no overlap.
    ///
    /// # Errors
    ///
    /// Returns an operational error when the task driver cannot be spawned.
    pub fn schedule_fixed_rate_no_overlap<F, Fut>(
        &self,
        mut config: ScheduledTaskConfig,
        task: F,
    ) -> RuntimeResult<ScheduledTaskRegistrationOutcome>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        config.mode = ScheduleMode::FixedRateNoOverlap;
        let name: Arc<str> = Arc::from(config.name.as_str());
        let name_for_cleanup = name.clone();
        let Some(metrics) = self.register(name.clone(), config.clone(), 1) else {
            return Ok(ScheduledTaskRegistrationOutcome::AlreadyPresent);
        };
        let token = self.group.cancellation_token();
        let run_group = self.group.clone();
        let task = Arc::new(task);

        let spawn_result = self.group.spawn(
            format!("scheduled-driver:{name}"),
            TaskKind::ScheduledDriver,
            async move {
                if !sleep_or_cancel(&token, config.initial_delay).await {
                    return;
                }

                let mut expected_tick = Instant::now();
                loop {
                    if token.is_cancelled() {
                        return;
                    }

                    if !metrics.try_begin_no_overlap_run(expected_tick) {
                        expected_tick = next_expected_tick(expected_tick, config.period);
                    } else {
                        let run_name = format!("scheduled-run:{name}");
                        let run_metrics = metrics.clone();
                        let run_task = task.clone();
                        let max_run_time = config.max_run_time;
                        let spawn_result = run_group.spawn(run_name, TaskKind::ScheduledRun, async move {
                            let started_at = Instant::now();
                            let timed_out = run_with_optional_timeout(run_task(), max_run_time).await;
                            run_metrics.finish_run(started_at, timed_out);
                        });
                        if spawn_result.is_err() {
                            metrics.rollback_started_run();
                        }
                        expected_tick = next_expected_tick(expected_tick, config.period);
                    }

                    if !sleep_or_cancel(&token, config.period).await {
                        return;
                    }
                }
            },
        );
        if spawn_result.is_err() {
            self.schedules.remove(&name_for_cleanup);
        }
        spawn_result.map(ScheduledTaskRegistrationOutcome::Scheduled)
    }

    /// Returns the schedule fixed rate.
    ///
    /// # Errors
    ///
    /// Returns an operational error when the task driver cannot be spawned.
    pub fn schedule_fixed_rate<F, Fut>(
        &self,
        mut config: ScheduledTaskConfig,
        task: F,
    ) -> RuntimeResult<ScheduledTaskRegistrationOutcome>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        config.mode = ScheduleMode::FixedRateAllowOverlap;
        let name: Arc<str> = Arc::from(config.name.as_str());
        let name_for_cleanup = name.clone();
        let Some(metrics) = self.register(name.clone(), config.clone(), 1) else {
            return Ok(ScheduledTaskRegistrationOutcome::AlreadyPresent);
        };
        let token = self.group.cancellation_token();
        let run_group = self.group.clone();
        let task = Arc::new(task);

        let spawn_result = self.group.spawn(
            format!("scheduled-driver:{name}"),
            TaskKind::ScheduledDriver,
            async move {
                if !sleep_or_cancel(&token, config.initial_delay).await {
                    return;
                }

                let mut expected_tick = Instant::now();
                loop {
                    if token.is_cancelled() {
                        return;
                    }

                    metrics.begin_overlapping_run(expected_tick);
                    let run_name = format!("scheduled-run:{name}");
                    let run_metrics = metrics.clone();
                    let run_task = task.clone();
                    let max_run_time = config.max_run_time;
                    let spawn_result = run_group.spawn(run_name, TaskKind::ScheduledRun, async move {
                        let started_at = Instant::now();
                        let timed_out = run_with_optional_timeout(run_task(), max_run_time).await;
                        run_metrics.finish_run(started_at, timed_out);
                    });
                    if spawn_result.is_err() {
                        metrics.rollback_started_run();
                    }

                    expected_tick = next_expected_tick(expected_tick, config.period);
                    if !sleep_or_cancel(&token, config.period).await {
                        return;
                    }
                }
            },
        );
        if spawn_result.is_err() {
            self.schedules.remove(&name_for_cleanup);
        }
        spawn_result.map(ScheduledTaskRegistrationOutcome::Scheduled)
    }

    /// Returns the schedule fixed rate allow overlap.
    ///
    /// # Errors
    ///
    /// Returns an operational error when the task driver cannot be spawned.
    pub fn schedule_fixed_rate_allow_overlap<F, Fut>(
        &self,
        config: ScheduledTaskConfig,
        task: F,
    ) -> RuntimeResult<ScheduledTaskRegistrationOutcome>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.schedule_fixed_rate(config, task)
    }

    /// Schedules one task with explicit concurrency and missed-tick limits.
    ///
    /// A run slot is acquired before a run task is created. Fixed-delay
    /// schedules remain serial; fixed-rate schedules use the supplied
    /// concurrency and missed-tick policy.
    ///
    /// # Errors
    ///
    /// Returns an operational error when the period is zero or the driver
    /// cannot be registered.
    pub fn schedule_bounded<F, Fut>(
        &self,
        mut config: ScheduledTaskConfig,
        policy: ScheduledExecutionPolicy,
        task: F,
    ) -> RuntimeResult<ScheduledTaskRegistrationOutcome>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if config.period.is_zero() {
            return Err(RuntimeError::unsupported(
                crate::RuntimeOperation::RegisterScheduledTask,
            ));
        }
        let max_concurrency = if matches!(config.mode, ScheduleMode::FixedDelay | ScheduleMode::FixedRateNoOverlap) {
            1
        } else {
            policy.max_concurrency()
        };
        config.mode = if config.mode == ScheduleMode::FixedDelay {
            ScheduleMode::FixedDelay
        } else if max_concurrency == 1 {
            ScheduleMode::FixedRateNoOverlap
        } else {
            ScheduleMode::FixedRateAllowOverlap
        };

        let name: Arc<str> = Arc::from(config.name.as_str());
        let name_for_cleanup = name.clone();
        let Some(metrics) = self.register(name.clone(), config.clone(), max_concurrency) else {
            return Ok(ScheduledTaskRegistrationOutcome::AlreadyPresent);
        };
        let token = self.group.cancellation_token();
        let run_group = self.group.clone();
        let task = Arc::new(task);
        let max_run_time = config.max_run_time;
        let period = config.period;
        let initial_delay = config.initial_delay;

        let spawn_result = if config.mode == ScheduleMode::FixedDelay {
            self.group.spawn(
                format!("scheduled-driver:{name}"),
                TaskKind::ScheduledDriver,
                async move {
                    if !sleep_or_cancel(&token, initial_delay).await {
                        return;
                    }
                    loop {
                        if token.is_cancelled() {
                            return;
                        }
                        let started_at = Instant::now();
                        metrics.begin_serial_run(started_at);
                        let timed_out = run_with_optional_timeout(task(), max_run_time).await;
                        metrics.finish_run(started_at, timed_out);
                        if !sleep_or_cancel(&token, period).await {
                            return;
                        }
                    }
                },
            )
        } else {
            self.group.spawn(
                format!("scheduled-driver:{name}"),
                TaskKind::ScheduledDriver,
                async move {
                    let mut expected_tick = Instant::now() + initial_delay;
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
                                let overdue = now.saturating_duration_since(expected_tick);
                                let total_missed = 1u64.saturating_add(
                                    u64::try_from(overdue.as_nanos() / period.as_nanos()).unwrap_or(u64::MAX),
                                );
                                let mut remaining = total_missed;
                                while remaining > 0 && metrics.try_reserve_run() {
                                    if !spawn_bounded_run(
                                        &run_group,
                                        &metrics,
                                        &task,
                                        &name,
                                        max_run_time,
                                    ) {
                                        metrics.rollback_started_run();
                                        break;
                                    }
                                    remaining -= 1;
                                }
                                metrics.queue_missed_ticks(remaining, policy.missed_ticks);
                                let advance = period.saturating_mul(
                                    u32::try_from(total_missed).unwrap_or(u32::MAX),
                                );
                                expected_tick = expected_tick.checked_add(advance).unwrap_or(now);
                            }
                        }
                        while metrics.pending_runs.load(Ordering::Acquire) > 0 && metrics.try_reserve_run() {
                            if !metrics.take_pending_run() {
                                metrics.rollback_started_run();
                                break;
                            }
                            if !spawn_bounded_run(&run_group, &metrics, &task, &name, max_run_time) {
                                metrics.rollback_started_run();
                                break;
                            }
                        }
                    }
                },
            )
        };
        if spawn_result.is_err() {
            self.schedules.remove(&name_for_cleanup);
        }
        spawn_result.map(ScheduledTaskRegistrationOutcome::Scheduled)
    }

    /// Returns the snapshot.
    pub fn snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.schedules.iter().map(|entry| entry.value().snapshot()).collect()
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
            running: AtomicBool::new(false),
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
    fn try_reserve_run(&self) -> bool {
        let mut active = self.active_runs.load(Ordering::Acquire);
        loop {
            if active >= self.max_concurrency as u64 {
                return false;
            }
            match self
                .active_runs
                .compare_exchange_weak(active, active + 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    self.running.store(true, Ordering::Release);
                    return true;
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

    fn begin_serial_run(&self, expected_at: Instant) {
        self.active_runs.fetch_add(1, Ordering::AcqRel);
        self.running.store(true, Ordering::Release);
        self.record_drift(expected_at);
    }

    fn try_begin_no_overlap_run(&self, expected_at: Instant) -> bool {
        if self.running.swap(true, Ordering::AcqRel) {
            self.skips.fetch_add(1, Ordering::Relaxed);
            false
        } else {
            self.active_runs.fetch_add(1, Ordering::AcqRel);
            self.record_drift(expected_at);
            true
        }
    }

    fn begin_overlapping_run(&self, expected_at: Instant) {
        let previous_runs = self.active_runs.fetch_add(1, Ordering::AcqRel);
        if previous_runs > 0 {
            self.overlaps.fetch_add(1, Ordering::Relaxed);
        }
        self.running.store(true, Ordering::Release);
        self.record_drift(expected_at);
    }

    fn finish_run(&self, started_at: Instant, timed_out: bool) {
        let elapsed_ms = started_at.elapsed().as_millis() as u64;
        self.last_elapsed_ms.store(elapsed_ms, Ordering::Relaxed);
        self.max_elapsed_ms.fetch_max(elapsed_ms, Ordering::Relaxed);
        if timed_out {
            self.failures.fetch_add(1, Ordering::Relaxed);
        } else {
            self.runs.fetch_add(1, Ordering::Relaxed);
        }
        self.finish_active_run();
        self.completion.notify_one();
    }

    fn rollback_started_run(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
        self.finish_active_run();
        self.completion.notify_one();
    }

    fn finish_active_run(&self) {
        if self.active_runs.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.running.store(false, Ordering::Release);
        }
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

fn spawn_bounded_run<F, Fut>(
    group: &TaskGroup,
    metrics: &Arc<ScheduledTaskMetrics>,
    task: &Arc<F>,
    name: &Arc<str>,
    max_run_time: Option<Duration>,
) -> bool
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let run_metrics = metrics.clone();
    let run_task = task.clone();
    group
        .spawn(format!("scheduled-run:{name}"), TaskKind::ScheduledRun, async move {
            let started_at = Instant::now();
            let timed_out = run_with_optional_timeout(run_task(), max_run_time).await;
            run_metrics.finish_run(started_at, timed_out);
        })
        .is_ok()
}

fn next_expected_tick(current: Instant, period: Duration) -> Instant {
    current.checked_add(period).unwrap_or_else(Instant::now)
}

async fn run_with_optional_timeout<Fut>(future: Fut, max_run_time: Option<Duration>) -> bool
where
    Fut: Future<Output = ()> + Send,
{
    if let Some(timeout) = max_run_time {
        tokio::time::timeout(timeout, future).await.is_err()
    } else {
        future.await;
        false
    }
}

async fn run_controlled_with_optional_timeout<Fut>(
    future: Fut,
    max_run_time: Option<Duration>,
) -> (ScheduledTaskControl, bool)
where
    Fut: Future<Output = ScheduledTaskControl> + Send,
{
    if let Some(timeout) = max_run_time {
        match tokio::time::timeout(timeout, future).await {
            Ok(control) => (control, false),
            Err(_) => (ScheduledTaskControl::Continue, true),
        }
    } else {
        (future.await, false)
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

    #[tokio::test(start_paused = true)]
    async fn coalesced_bounded_schedule_never_exceeds_one_active_and_one_pending_run() {
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
            .schedule_bounded(
                ScheduledTaskConfig::fixed_rate("bounded-coalesce", Duration::from_secs(1)),
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
}
