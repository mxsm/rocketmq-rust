// Copyright 2026 The RocketMQ Rust Authors
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

//! Store-runtime ownership for managed mapped-file retirement batches.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::TaskGroup;
use rocketmq_store_local::mapped_file::ManagedLifecycleRuntime;
use rocketmq_store_local::mapped_file::ManagedRetirementBatchReport;
use rocketmq_store_local::mapped_file::ManagedRetirementStage;

use crate::runtime::StoreRuntimeScope;
use crate::store::running_flags::RunningFlags;
use crate::store_error::StoreComponent;
use crate::store_error::StoreError;
use crate::store_error::StoreErrorKind;
use crate::store_error::StoreOperation;

const DEFAULT_BATCH_SIZE: usize = 64;
const DEFAULT_PERIOD: Duration = Duration::from_millis(250);
const DEFAULT_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct RetirementServiceBatch {
    pub(super) attempted: usize,
    pub(super) completed: usize,
    pub(super) pending_tickets: usize,
    pub(super) tombstone_backlog: usize,
    pub(super) oldest_pending_age: Duration,
    pub(super) last_failure_stage: Option<ManagedRetirementStage>,
    pub(super) recovery_required: bool,
}

impl From<ManagedRetirementBatchReport> for RetirementServiceBatch {
    fn from(report: ManagedRetirementBatchReport) -> Self {
        Self {
            attempted: report.attempted(),
            completed: report.completed(),
            pending_tickets: report.pending_tickets(),
            tombstone_backlog: report.tombstone_backlog(),
            oldest_pending_age: report.oldest_pending_age(),
            last_failure_stage: report.last_failure_stage(),
            recovery_required: report.recovery_required(),
        }
    }
}

pub(super) trait RetirementBatchDriver: Clone + Send + Sync + 'static {
    fn begin_shutdown(&self);
    fn drive_batch(&self, max_actions: usize) -> RetirementServiceBatch;
    fn drive_drain_batch(&self, max_actions: usize) -> RetirementServiceBatch;
    fn snapshot(&self) -> RetirementServiceBatch;
}

impl RetirementBatchDriver for ManagedLifecycleRuntime {
    fn begin_shutdown(&self) {
        Self::begin_shutdown(self);
    }

    fn drive_batch(&self, max_actions: usize) -> RetirementServiceBatch {
        Self::drive_batch(self, max_actions).into()
    }

    fn drive_drain_batch(&self, max_actions: usize) -> RetirementServiceBatch {
        Self::drive_drain_batch(self, max_actions).into()
    }

    fn snapshot(&self) -> RetirementServiceBatch {
        Self::snapshot(self).into()
    }
}

#[derive(Debug, Clone, Copy)]
struct RetirementServiceConfig {
    batch_size: usize,
    period: Duration,
    task_shutdown_timeout: Duration,
    drain_timeout: Duration,
}

impl Default for RetirementServiceConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            period: DEFAULT_PERIOD,
            task_shutdown_timeout: DEFAULT_TASK_SHUTDOWN_TIMEOUT,
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
        }
    }
}

pub(super) struct MappedFileRetirementService<D = ManagedLifecycleRuntime> {
    driver: D,
    runtime_scope: StoreRuntimeScope,
    running_flags: Arc<RunningFlags>,
    accepting: Arc<AtomicBool>,
    latest_report: Arc<Mutex<RetirementServiceBatch>>,
    task_group: Option<TaskGroup>,
    scheduled_tasks: Option<ScheduledTaskGroup>,
    config: RetirementServiceConfig,
}

impl MappedFileRetirementService<ManagedLifecycleRuntime> {
    pub(super) fn new(
        runtime: ManagedLifecycleRuntime,
        runtime_scope: StoreRuntimeScope,
        running_flags: Arc<RunningFlags>,
    ) -> Self {
        Self::with_config(
            runtime,
            runtime_scope,
            running_flags,
            RetirementServiceConfig::default(),
        )
    }
}

impl<D: RetirementBatchDriver> MappedFileRetirementService<D> {
    fn with_config(
        driver: D,
        runtime_scope: StoreRuntimeScope,
        running_flags: Arc<RunningFlags>,
        config: RetirementServiceConfig,
    ) -> Self {
        let initial = driver.snapshot();
        Self {
            driver,
            runtime_scope,
            running_flags,
            accepting: Arc::new(AtomicBool::new(false)),
            latest_report: Arc::new(Mutex::new(initial)),
            task_group: None,
            scheduled_tasks: None,
            config,
        }
    }

    pub(super) fn start(&mut self) -> Result<(), StoreError> {
        if self.task_group.is_some() {
            return Ok(());
        }
        let task_group = self.runtime_scope.task_group("rocketmq-store.mapped-file-retirement");
        let scheduled_tasks = ScheduledTaskGroup::new(task_group.clone());
        self.accepting.store(true, Ordering::Release);

        let driver = self.driver.clone();
        let runtime_scope = self.runtime_scope.clone();
        let running_flags = Arc::clone(&self.running_flags);
        let accepting = Arc::clone(&self.accepting);
        let latest_report = Arc::clone(&self.latest_report);
        let batch_size = self.config.batch_size;
        if let Err(source) = scheduled_tasks.schedule_fixed_delay(
            ScheduledTaskConfig::fixed_delay("mapped-file-retirement-reaper", self.config.period),
            move || {
                let driver = driver.clone();
                let runtime_scope = runtime_scope.clone();
                let running_flags = Arc::clone(&running_flags);
                let accepting = Arc::clone(&accepting);
                let latest_report = Arc::clone(&latest_report);
                async move {
                    if !accepting.load(Ordering::Acquire) {
                        return;
                    }
                    let blocking_driver = driver.clone();
                    match runtime_scope
                        .spawn_io("mapped-file-retirement-batch", move || {
                            blocking_driver.drive_batch(batch_size)
                        })
                        .await
                    {
                        Ok(report) => {
                            if report.recovery_required {
                                accepting.store(false, Ordering::Release);
                                driver.begin_shutdown();
                                running_flags.get_and_make_not_writeable();
                            }
                            *latest_report.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = report;
                        }
                        Err(error) => {
                            accepting.store(false, Ordering::Release);
                            driver.begin_shutdown();
                            running_flags.get_and_make_not_writeable();
                            tracing::error!(error = %error, "mapped-file retirement blocking batch failed");
                        }
                    }
                }
            },
        ) {
            self.accepting.store(false, Ordering::Release);
            task_group.cancel();
            return Err(StoreError::new(StoreErrorKind::Unavailable, StoreOperation::Start)
                .in_component(StoreComponent::MappedFile)
                .with_detail("failed to schedule the managed mapped-file retirement service")
                .with_source(source));
        }

        self.task_group = Some(task_group);
        self.scheduled_tasks = Some(scheduled_tasks);
        Ok(())
    }

    pub(super) async fn cancel_drain_and_await(&mut self) -> Result<RetirementServiceBatch, StoreError> {
        self.accepting.store(false, Ordering::Release);
        self.driver.begin_shutdown();
        self.scheduled_tasks.take();

        let mut first_error = None;
        if let Some(task_group) = self.task_group.take() {
            let report = task_group.shutdown(self.config.task_shutdown_timeout).await;
            if let Err(error) = crate::runtime::shutdown_report_result("mapped-file retirement service", report) {
                first_error = Some(
                    StoreError::new(StoreErrorKind::Timeout, StoreOperation::Shutdown)
                        .in_component(StoreComponent::MappedFile)
                        .with_detail("managed mapped-file retirement task group did not drain")
                        .with_source(error),
                );
            }
        }

        let deadline = ShutdownDeadline::after(self.config.drain_timeout);
        let batch_size = self.config.batch_size;
        let mut latest = self.driver.snapshot();
        while latest.pending_tickets > 0 && !latest.recovery_required && !deadline.is_expired() {
            let driver = self.driver.clone();
            match self
                .runtime_scope
                .spawn_io_until("mapped-file-retirement-drain", deadline, move || {
                    driver.drive_drain_batch(batch_size)
                })
                .await
            {
                Ok(report) => {
                    let made_progress = report.completed > 0 || report.pending_tickets < latest.pending_tickets;
                    latest = report;
                    if latest.attempted == 0 || !made_progress {
                        break;
                    }
                }
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(
                            StoreError::new(StoreErrorKind::Timeout, StoreOperation::Shutdown)
                                .in_component(StoreComponent::MappedFile)
                                .with_detail("managed mapped-file retirement drain failed")
                                .with_source(error),
                        );
                    }
                    break;
                }
            }
        }
        *self
            .latest_report
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = latest;
        match first_error {
            Some(error) => Err(error),
            None => Ok(latest),
        }
    }

    pub(super) fn snapshot(&self) -> RetirementServiceBatch {
        *self
            .latest_report
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests;
