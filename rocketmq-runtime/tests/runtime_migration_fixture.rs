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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ScheduleMode;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ShutdownReport;
use tokio::sync::Notify;

fn migrated_initial_delay(initial_delay: Option<Duration>) -> Duration {
    initial_delay.unwrap_or(Duration::ZERO)
}

#[test]
fn migration_fixture_owns_scheduled_child_work_and_reports_shutdown() {
    let owner = RuntimeOwner::new(RuntimeConfig::for_parallelism("runtime-migration-fixture", 1))
        .expect("runtime owner should start");
    let service: ChildServiceContext = owner.root_context().component("migration-fixture");
    let scheduled = service.scheduled_tasks("migration-fixture-schedules");
    let runs = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(Notify::new());
    let mut config = ScheduledTaskConfig::fixed_rate_no_overlap("migration-fixture.refresh", Duration::from_secs(60));
    config.initial_delay = migrated_initial_delay(Some(Duration::ZERO));
    assert_eq!(config.initial_delay, Duration::ZERO);

    scheduled
        .schedule_fixed_rate_no_overlap(config, {
            let runs = Arc::clone(&runs);
            let completed = Arc::clone(&completed);
            move || {
                let runs = Arc::clone(&runs);
                let completed = Arc::clone(&completed);
                async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    completed.notify_one();
                }
            }
        })
        .expect("scheduled task should register");

    let report: ShutdownReport = owner.block_on(async {
        completed.notified().await;
        owner.shutdown_tasks().await
    });

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let snapshot = scheduled.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].mode, ScheduleMode::FixedRateNoOverlap);
    assert!(report.is_healthy(), "{}", report.to_json());
}
