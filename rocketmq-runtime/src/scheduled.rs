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

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::error::RuntimeError;
use crate::error::RuntimeResult;
use crate::shutdown_report::ShutdownReport;
use crate::task_group::TaskGroup;
use crate::task_group::TaskId;
use crate::task_group::TaskKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ScheduleMode {
    FixedDelay,
    FixedRateNoOverlap,
    FixedRateAllowOverlap,
}

#[derive(Debug, Clone)]
pub struct ScheduledTaskConfig {
    pub name: String,
    pub initial_delay: Duration,
    pub period: Duration,
    pub mode: ScheduleMode,
    pub max_run_time: Option<Duration>,
    pub shutdown_timeout: Duration,
}

impl ScheduledTaskConfig {
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

    pub fn fixed_rate_no_overlap(name: impl Into<String>, period: Duration) -> Self {
        Self {
            mode: ScheduleMode::FixedRateNoOverlap,
            ..Self::fixed_delay(name, period)
        }
    }

    pub fn fixed_rate(name: impl Into<String>, period: Duration) -> Self {
        Self {
            mode: ScheduleMode::FixedRateAllowOverlap,
            ..Self::fixed_delay(name, period)
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScheduledTaskGroup {
    group: TaskGroup,
    schedules: Arc<DashMap<Arc<str>, Arc<ScheduledTaskMetrics>>>,
}

#[derive(Debug)]
struct ScheduledTaskMetrics {
    config: ScheduledTaskConfig,
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
pub struct ScheduledTaskSnapshot {
    pub name: String,
    pub mode: ScheduleMode,
    pub running: bool,
    pub active_runs: u64,
    pub runs: u64,
    pub skips: u64,
    pub overlaps: u64,
    pub failures: u64,
    pub last_drift_ms: u64,
    pub last_elapsed_ms: u64,
    pub max_elapsed_ms: u64,
}

impl ScheduledTaskGroup {
    pub fn new(group: TaskGroup) -> Self {
        Self {
            group,
            schedules: Arc::new(DashMap::new()),
        }
    }

    pub fn group(&self) -> &TaskGroup {
        &self.group
    }

    pub fn schedule_fixed_delay<F, Fut>(&self, mut config: ScheduledTaskConfig, mut task: F) -> RuntimeResult<TaskId>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        config.mode = ScheduleMode::FixedDelay;
        let name: Arc<str> = Arc::from(config.name.as_str());
        let name_for_cleanup = name.clone();
        let metrics = self.register(name.clone(), config.clone())?;
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
                    let timed_out = run_with_optional_timeout(task(), config.max_run_time).await;
                    metrics.finish_run(started_at, timed_out);

                    if !sleep_or_cancel(&token, config.period).await {
                        return;
                    }
                }
            },
        );
        if spawn_result.is_err() {
            self.schedules.remove(&name_for_cleanup);
        }
        spawn_result
    }

    pub fn schedule_fixed_rate_no_overlap<F, Fut>(
        &self,
        mut config: ScheduledTaskConfig,
        task: F,
    ) -> RuntimeResult<TaskId>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        config.mode = ScheduleMode::FixedRateNoOverlap;
        let name: Arc<str> = Arc::from(config.name.as_str());
        let name_for_cleanup = name.clone();
        let metrics = self.register(name.clone(), config.clone())?;
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
        spawn_result
    }

    pub fn schedule_fixed_rate<F, Fut>(&self, mut config: ScheduledTaskConfig, task: F) -> RuntimeResult<TaskId>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        config.mode = ScheduleMode::FixedRateAllowOverlap;
        let name: Arc<str> = Arc::from(config.name.as_str());
        let name_for_cleanup = name.clone();
        let metrics = self.register(name.clone(), config.clone())?;
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
        spawn_result
    }

    pub fn schedule_fixed_rate_allow_overlap<F, Fut>(
        &self,
        config: ScheduledTaskConfig,
        task: F,
    ) -> RuntimeResult<TaskId>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.schedule_fixed_rate(config, task)
    }

    pub fn snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.schedules.iter().map(|entry| entry.value().snapshot()).collect()
    }

    pub async fn shutdown(&self, timeout: Duration) -> ShutdownReport {
        self.group.shutdown(timeout).await
    }

    fn register(&self, name: Arc<str>, config: ScheduledTaskConfig) -> RuntimeResult<Arc<ScheduledTaskMetrics>> {
        let metrics = Arc::new(ScheduledTaskMetrics {
            config,
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
            Entry::Occupied(_) => Err(RuntimeError::ScheduledTaskExists { name }),
            Entry::Vacant(entry) => {
                entry.insert(metrics.clone());
                Ok(metrics)
            }
        }
    }
}

impl ScheduledTaskMetrics {
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
    }

    fn rollback_started_run(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
        self.finish_active_run();
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

async fn sleep_or_cancel(token: &CancellationToken, duration: Duration) -> bool {
    if duration.is_zero() {
        return !token.is_cancelled();
    }

    tokio::select! {
        _ = token.cancelled() => false,
        _ = tokio::time::sleep(duration) => true,
    }
}
