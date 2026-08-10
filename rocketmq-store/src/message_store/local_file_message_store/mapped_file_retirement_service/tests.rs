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

use std::sync::atomic::AtomicUsize;

use tokio::sync::Semaphore;

use super::*;

#[derive(Clone)]
struct FakeDriver {
    pending: Arc<AtomicUsize>,
    scheduled_calls: Arc<AtomicUsize>,
    drain_calls: Arc<AtomicUsize>,
    drain_batch_limit: Arc<AtomicUsize>,
    shutdown_calls: Arc<AtomicUsize>,
    recovery_required: Arc<AtomicBool>,
    entered: Arc<Semaphore>,
}

impl FakeDriver {
    fn new(pending: usize) -> Self {
        Self {
            pending: Arc::new(AtomicUsize::new(pending)),
            scheduled_calls: Arc::new(AtomicUsize::new(0)),
            drain_calls: Arc::new(AtomicUsize::new(0)),
            drain_batch_limit: Arc::new(AtomicUsize::new(0)),
            shutdown_calls: Arc::new(AtomicUsize::new(0)),
            recovery_required: Arc::new(AtomicBool::new(false)),
            entered: Arc::new(Semaphore::new(0)),
        }
    }

    fn report_after_one(&self) -> RetirementServiceBatch {
        let previous = self
            .pending
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| pending.checked_sub(1))
            .unwrap_or_default();
        RetirementServiceBatch {
            attempted: usize::from(previous > 0),
            completed: usize::from(previous > 0),
            pending_tickets: previous.saturating_sub(1),
            recovery_required: self.recovery_required.load(Ordering::Acquire),
            ..RetirementServiceBatch::default()
        }
    }
}

impl RetirementBatchDriver for FakeDriver {
    fn begin_shutdown(&self) {
        self.shutdown_calls.fetch_add(1, Ordering::AcqRel);
    }

    fn drive_batch(&self, _max_actions: usize) -> RetirementServiceBatch {
        self.scheduled_calls.fetch_add(1, Ordering::AcqRel);
        let report = self.report_after_one();
        self.entered.add_permits(1);
        report
    }

    fn drive_drain_batch(&self, max_actions: usize) -> RetirementServiceBatch {
        self.drain_calls.fetch_add(1, Ordering::AcqRel);
        self.drain_batch_limit.store(max_actions, Ordering::Release);
        self.report_after_one()
    }

    fn snapshot(&self) -> RetirementServiceBatch {
        RetirementServiceBatch {
            pending_tickets: self.pending.load(Ordering::Acquire),
            recovery_required: self.recovery_required.load(Ordering::Acquire),
            ..RetirementServiceBatch::default()
        }
    }
}

fn test_config() -> RetirementServiceConfig {
    RetirementServiceConfig {
        batch_size: 1,
        period: Duration::from_secs(60),
        task_shutdown_timeout: Duration::from_secs(1),
        drain_timeout: Duration::from_secs(1),
    }
}

#[tokio::test]
async fn start_cancel_drain_and_await_uses_the_owned_runtime_boundaries() {
    let driver = FakeDriver::new(2);
    let scope = crate::runtime::test_scope("mapped-file-retirement-service-lifecycle");
    let running_flags = Arc::new(RunningFlags::new());
    let mut service = MappedFileRetirementService::with_config(
        driver.clone(),
        scope.clone(),
        Arc::clone(&running_flags),
        test_config(),
    );

    service.start().expect("service starts exactly once");
    tokio::time::timeout(Duration::from_secs(2), driver.entered.acquire())
        .await
        .expect("scheduled batch enters")
        .expect("semaphore remains open")
        .forget();

    let report = service
        .cancel_drain_and_await()
        .await
        .expect("service cancels, drains, and awaits");

    assert_eq!(report.pending_tickets, 0);
    assert_eq!(driver.scheduled_calls.load(Ordering::Acquire), 1);
    assert_eq!(driver.drain_calls.load(Ordering::Acquire), 1);
    assert_eq!(driver.drain_batch_limit.load(Ordering::Acquire), 1);
    assert_eq!(driver.shutdown_calls.load(Ordering::Acquire), 1);
    assert!(running_flags.is_writeable());
    assert_eq!(scope.blocking_snapshot().blocking_still_running, 0);
    assert!(service.task_group.is_none());
    assert!(service.scheduled_tasks.is_none());
}

#[tokio::test]
async fn replay_required_result_stops_admission_and_marks_store_not_writeable() {
    let driver = FakeDriver::new(2);
    driver.recovery_required.store(true, Ordering::Release);
    let scope = crate::runtime::test_scope("mapped-file-retirement-service-recovery-fence");
    let running_flags = Arc::new(RunningFlags::new());
    let mut service =
        MappedFileRetirementService::with_config(driver.clone(), scope, Arc::clone(&running_flags), test_config());

    service.start().expect("service starts exactly once");
    tokio::time::timeout(Duration::from_secs(2), driver.entered.acquire())
        .await
        .expect("scheduled batch enters")
        .expect("semaphore remains open")
        .forget();

    let report = service
        .cancel_drain_and_await()
        .await
        .expect("recovery-fenced service still awaits its owned tasks");
    assert!(report.recovery_required);
    assert_eq!(report.pending_tickets, 1);
    assert!(!running_flags.is_writeable());
    assert_eq!(driver.drain_calls.load(Ordering::Acquire), 0);
    assert!(driver.shutdown_calls.load(Ordering::Acquire) >= 1);
}
