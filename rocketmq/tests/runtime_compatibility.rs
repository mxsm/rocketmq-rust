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

use std::time::Duration;

use rocketmq_runtime::schedule::simple_scheduler::ScheduleMode;
use rocketmq_runtime::schedule::simple_scheduler::ScheduledTaskManager;
use rocketmq_runtime::task::service_task::ServiceContext;
use rocketmq_runtime::task::service_task::ServiceTask;
use rocketmq_runtime::task::ServiceManager;

struct ProbeService;

impl ServiceTask for ProbeService {
    fn get_service_name(&self) -> String {
        "runtime-compatibility-probe".to_string()
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

#[tokio::test]
async fn canonical_and_legacy_shutdown_paths_share_type_identity() {
    let (canonical, sender) = rocketmq_runtime::Shutdown::new(1);
    let mut legacy: rocketmq_rust::Shutdown<()> = canonical;

    sender.send(()).expect("shutdown receiver should remain connected");
    legacy.recv().await;

    assert!(legacy.is_shutdown());
}

#[tokio::test]
async fn canonical_scheduler_is_usable_through_legacy_path() {
    let canonical = ScheduledTaskManager::new_legacy_compatibility();
    let legacy: rocketmq_rust::schedule::simple_scheduler::ScheduledTaskManager = canonical;
    let task_id = legacy
        .add_scheduled_task(
            ScheduleMode::FixedDelay,
            Duration::ZERO,
            Duration::from_secs(60),
            |_token| async { Ok(()) },
        )
        .expect("compatibility scheduler should start a task");

    assert_eq!(legacy.task_count(), 1);
    legacy.cancel_task(task_id);
    assert_eq!(legacy.task_count(), 0);
}

#[tokio::test]
async fn canonical_service_manager_is_usable_through_legacy_path() {
    let canonical = ServiceManager::new(ProbeService);
    let legacy: rocketmq_rust::task::ServiceManager<ProbeService> = canonical;

    legacy.start().await.expect("service manager should start");
    assert_eq!(legacy.task_count().await, 1);
    legacy.shutdown().await.expect("service manager should stop");
    assert_eq!(legacy.task_count().await, 0);
}
