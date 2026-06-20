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

use dashmap::DashMap;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::error::RuntimeError;
use crate::error::RuntimeResult;
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
    runs: AtomicU64,
    skips: AtomicU64,
    failures: AtomicU64,
    last_elapsed_ms: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScheduledTaskSnapshot {
    pub name: String,
    pub mode: ScheduleMode,
    pub running: bool,
    pub runs: u64,
    pub skips: u64,
    pub failures: u64,
    pub last_elapsed_ms: u64,
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
        let metrics = self.register(name.clone(), config.clone())?;
        let token = self.group.cancellation_token();
        self.group.spawn(
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
                    metrics.running.store(true, Ordering::Release);
                    let timed_out = run_with_optional_timeout(task(), config.max_run_time).await;
                    metrics.running.store(false, Ordering::Release);
                    metrics
                        .last_elapsed_ms
                        .store(started_at.elapsed().as_millis() as u64, Ordering::Relaxed);
                    if timed_out {
                        metrics.failures.fetch_add(1, Ordering::Relaxed);
                    } else {
                        metrics.runs.fetch_add(1, Ordering::Relaxed);
                    }

                    if !sleep_or_cancel(&token, config.period).await {
                        return;
                    }
                }
            },
        )
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
        let metrics = self.register(name.clone(), config.clone())?;
        let token = self.group.cancellation_token();
        let run_group = self.group.clone();
        let task = Arc::new(task);

        self.group.spawn(
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

                    if metrics.running.swap(true, Ordering::AcqRel) {
                        metrics.skips.fetch_add(1, Ordering::Relaxed);
                    } else {
                        let run_name = format!("scheduled-run:{name}");
                        let run_metrics = metrics.clone();
                        let run_task = task.clone();
                        let max_run_time = config.max_run_time;
                        let _ = run_group.spawn(run_name, TaskKind::ScheduledRun, async move {
                            let started_at = Instant::now();
                            let timed_out = run_with_optional_timeout(run_task(), max_run_time).await;
                            run_metrics.running.store(false, Ordering::Release);
                            run_metrics
                                .last_elapsed_ms
                                .store(started_at.elapsed().as_millis() as u64, Ordering::Relaxed);
                            if timed_out {
                                run_metrics.failures.fetch_add(1, Ordering::Relaxed);
                            } else {
                                run_metrics.runs.fetch_add(1, Ordering::Relaxed);
                            }
                        });
                    }

                    if !sleep_or_cancel(&token, config.period).await {
                        return;
                    }
                }
            },
        )
    }

    pub fn snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.schedules.iter().map(|entry| entry.value().snapshot()).collect()
    }

    fn register(&self, name: Arc<str>, config: ScheduledTaskConfig) -> RuntimeResult<Arc<ScheduledTaskMetrics>> {
        if self.schedules.contains_key(&name) {
            return Err(RuntimeError::ScheduledTaskExists { name });
        }

        let metrics = Arc::new(ScheduledTaskMetrics {
            config,
            running: AtomicBool::new(false),
            runs: AtomicU64::new(0),
            skips: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            last_elapsed_ms: AtomicU64::new(0),
        });
        self.schedules.insert(name, metrics.clone());
        Ok(metrics)
    }
}

impl ScheduledTaskMetrics {
    fn snapshot(&self) -> ScheduledTaskSnapshot {
        ScheduledTaskSnapshot {
            name: self.config.name.clone(),
            mode: self.config.mode,
            running: self.running.load(Ordering::Acquire),
            runs: self.runs.load(Ordering::Relaxed),
            skips: self.skips.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            last_elapsed_ms: self.last_elapsed_ms.load(Ordering::Relaxed),
        }
    }
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
