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

use std::process::Command;
use std::time::Duration;

use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::TaskKind;

const CHILD_MODE: &str = "ROCKETMQ_RUNTIME_STACK_TEST_CHILD";
const STACK_SIZE: usize = 1024 * 1024;

#[test]
fn large_tasks_submit_on_one_mib_stack() {
    if std::env::var_os(CHILD_MODE).is_none() {
        // Stack overflow aborts the process; isolate it from the rest of the test suite.
        let output = Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", "large_tasks_submit_on_one_mib_stack", "--nocapture"])
            .env(CHILD_MODE, "1")
            .output()
            .expect("run stack probe subprocess");
        assert!(
            output.status.success(),
            "stack probe failed: {}\nstdout: {}\nstderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    std::thread::Builder::new()
        .name("runtime-submission-stack-probe".to_owned())
        .stack_size(STACK_SIZE)
        .spawn(run_stack_probe)
        .expect("start small-stack thread")
        .join()
        .expect("small-stack thread completed");
}

fn run_stack_probe() {
    let owner = RuntimeOwner::plan(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .expect("valid runtime configuration")
    .build()
    .expect("runtime owner");
    let context = owner.root_context().component("stack-probe");
    let group = context.task_group();

    let (_, handle) = group
        .spawn_with_handle("large-task", TaskKind::Service, large_task())
        .expect("submit large task");
    owner.block_on(handle).expect("large task completed");

    let schedules = ScheduledTaskGroup::new(group.clone());
    let (completed, received) = std::sync::mpsc::sync_channel(1);
    schedules
        .schedule_fixed_delay(
            ScheduledTaskConfig::fixed_delay("large-scheduled-task", Duration::from_secs(60)),
            move || {
                let completed = completed.clone();
                async move {
                    large_task().await;
                    completed.send(()).expect("report scheduled completion");
                }
            },
        )
        .expect("submit large scheduled task");
    received
        .recv_timeout(Duration::from_secs(10))
        .expect("large scheduled task completed");

    let report = owner.block_on(owner.shutdown_tasks());
    assert!(report.is_healthy(), "{}", report.to_json());
    owner.shutdown_runtime_blocking().expect("runtime stopped");
}

async fn large_task() {
    let payload = [7_u8; 16 * 1024];
    tokio::task::yield_now().await;
    assert_eq!(std::hint::black_box(payload)[0], 7);
}
