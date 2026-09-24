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

use rocketmq_runtime::MissedTickPolicy;
use rocketmq_runtime::OperationContext;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ScheduledExecutionPolicy;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskControl;
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

fn isolated_stack_probe(name: &str, probe: fn()) {
    if std::env::var_os(CHILD_MODE).is_none() {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture"])
            .env(CHILD_MODE, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{name}: {}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    std::thread::Builder::new()
        .name(name.into())
        .stack_size(STACK_SIZE)
        .spawn(probe)
        .unwrap()
        .join()
        .unwrap();
}

fn small_stack_owner() -> RuntimeOwner {
    RuntimeOwner::plan(RuntimeConfig {
        worker_threads: 2,
        thread_stack_size: Some(STACK_SIZE),
        ..RuntimeConfig::default()
    })
    .unwrap()
    .build()
    .unwrap()
}

async fn payload_task<const N: usize>() {
    let payload = [7_u8; N];
    tokio::task::yield_now().await;
    assert_eq!(std::hint::black_box(&payload)[0], 7);
}

#[test]
fn large_operations_fit_on_one_mib_stack() {
    isolated_stack_probe("large_operations_fit_on_one_mib_stack", || {
        let owner = small_stack_owner();
        let context = owner.root_context().component("operation-stack");
        let operation = OperationContext::without_deadline(TaskKind::Worker);
        let id = context
            .task_group()
            .spawn_operation(&operation, "large", payload_task::<65536>())
            .unwrap();
        assert!(owner.block_on(context.task_group().wait_task(id, Duration::from_secs(5))));
        let id = context
            .task_group()
            .spawn_draining_operation(&operation, "large-drain", payload_task::<65536>())
            .unwrap();
        assert!(owner.block_on(context.task_group().wait_task(id, Duration::from_secs(5))));
        let id = context
            .task_group()
            .spawn_cancellable_service("large-cancellable", payload_task::<65536>())
            .unwrap();
        assert!(owner.block_on(context.task_group().wait_task(id, Duration::from_secs(5))));
        assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
    });
}

#[test]
fn scheduled_runs_fit_on_one_mib_worker_stack() {
    isolated_stack_probe("scheduled_runs_fit_on_one_mib_worker_stack", || {
        let owner = small_stack_owner();
        let context = owner.root_context().component("schedule-stack");
        let schedules = ScheduledTaskGroup::new(context.task_group().clone());
        let (sent, received) = std::sync::mpsc::sync_channel(1);
        schedules
            .schedule_fixed_delay(
                ScheduledTaskConfig::fixed_delay("large-run", Duration::from_secs(60)),
                move || {
                    let sent = sent.clone();
                    async move {
                        payload_task::<32768>().await;
                        sent.send(()).unwrap();
                    }
                },
            )
            .unwrap();
        received.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
    });
}

#[test]
fn blocking_captures_fit_on_one_mib_stack() {
    isolated_stack_probe("blocking_captures_fit_on_one_mib_stack", || {
        let owner = small_stack_owner();
        let context = owner.root_context().component("blocking-stack");
        let payload = [7_u8; 65536];
        let future = context
            .storage_io()
            .spawn_io("large-capture", move || std::hint::black_box(&payload)[0]);
        // Prevent large captures from inflating the caller's own async state.
        assert!(std::mem::size_of_val(&future) < 4096);
        let result = owner.block_on(future);
        assert_eq!(result.unwrap(), 7);
        assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
    });
}

#[test]
fn blocking_results_fit_on_one_mib_stack() {
    isolated_stack_probe("blocking_results_fit_on_one_mib_stack", || {
        let owner = small_stack_owner();
        let context = owner.root_context().component("blocking-result-stack");
        let result = owner
            .block_on(context.storage_io().spawn_io("large-result", || [7_u8; 32768]))
            .unwrap();
        assert_eq!(std::hint::black_box(&result)[0], 7);
        assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
    });
}

#[test]
fn deep_task_tree_shuts_down_on_one_mib_stack() {
    isolated_stack_probe("deep_task_tree_shuts_down_on_one_mib_stack", || {
        let owner = small_stack_owner();
        let root = owner.root_context().component("tree");
        let mut leaf = root.clone();
        for _ in 0..512 {
            leaf = leaf.component("child");
        }
        let report = owner.block_on(root.task_group().shutdown(Duration::from_secs(5)));
        assert!(report.is_healthy());
        let mut node = &report;
        let mut depth = 0;
        while let Some(child) = node.children.first() {
            depth += 1;
            node = child;
        }
        assert_eq!(depth, 512);
        drop(leaf);
        assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
    });
}

#[test]
fn deep_idle_ancestors_drop_on_one_mib_stack() {
    isolated_stack_probe("deep_idle_ancestors_drop_on_one_mib_stack", || {
        let owner = small_stack_owner();
        let root = owner.root_context().component("tree");
        let mut leaf = root.clone();
        for _ in 0..4096 {
            leaf = leaf.component("child");
        }
        drop(leaf);
        assert_eq!(root.task_group().component_count(), 0);
        assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
    });
}

#[test]
fn deep_immediate_shutdown_fits_on_one_mib_stack() {
    isolated_stack_probe("deep_immediate_shutdown_fits_on_one_mib_stack", || {
        let owner = small_stack_owner();
        let root = owner.root_context().component("tree");
        let mut leaf = root.clone();
        for _ in 0..1024 {
            leaf = leaf.component("child");
        }
        let report = root.task_group().shutdown_now();
        assert!(report.is_healthy());
        drop(leaf);
        assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
    });
}

#[test]
fn large_scheduler_factories_fit_on_one_mib_stack() {
    isolated_stack_probe("large_scheduler_factories_fit_on_one_mib_stack", || {
        let owner = small_stack_owner();
        let context = owner.root_context().component("factory-stack");
        let schedules = ScheduledTaskGroup::new(context.task_group().clone());
        let payload = [7_u8; 65536];
        let (sent, received) = std::sync::mpsc::sync_channel(1);
        schedules
            .schedule_fixed_delay(
                ScheduledTaskConfig::fixed_delay("large-factory", Duration::from_secs(60)),
                move || {
                    let value = std::hint::black_box(&payload)[0];
                    let sent = sent.clone();
                    async move {
                        sent.send(value).unwrap();
                    }
                },
            )
            .unwrap();
        assert_eq!(received.recv_timeout(Duration::from_secs(5)).unwrap(), 7);
        assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
    });
}

#[test]
fn scheduling_adapters_fit_on_one_mib_worker_stack() {
    isolated_stack_probe("scheduling_adapters_fit_on_one_mib_worker_stack", || {
        let owner = small_stack_owner();
        let context = owner.root_context().component("schedule-adapter-stack");
        let schedules = ScheduledTaskGroup::new(context.task_group().clone());
        let operation = OperationContext::without_deadline(TaskKind::Worker);

        macro_rules! check_schedule {
            ($method:ident, $result:expr $(, $prefix:expr)*) => {{
                let (sent, received) = std::sync::mpsc::sync_channel(1);
                let mut config = ScheduledTaskConfig::fixed_delay(stringify!($method), Duration::from_secs(60));
                config.max_run_time = Some(Duration::from_secs(5));
                schedules.$method($($prefix,)* config, move || {
                    let sent = sent.clone();
                    async move {
                        payload_task::<32768>().await;
                        sent.send(()).unwrap();
                        $result
                    }
                }).unwrap();
                received.recv_timeout(Duration::from_secs(5)).unwrap();
            }};
        }

        check_schedule!(schedule_fixed_delay, ());
        check_schedule!(schedule_fixed_delay_operation, (), &operation);
        check_schedule!(schedule_fixed_delay_controlled, ScheduledTaskControl::Stop);
        check_schedule!(
            schedule_fixed_delay_controlled_operation,
            ScheduledTaskControl::Stop,
            &operation
        );
        check_schedule!(schedule_fixed_rate_no_overlap_operation, (), &operation);
        check_schedule!(schedule_fixed_rate_no_overlap, ());
        check_schedule!(schedule_fixed_rate, ());
        check_schedule!(schedule_fixed_rate_allow_overlap, ());

        // Exercise both boxing decisions together: a large factory and a large run.
        let payload = [7_u8; 65536];
        let (sent, received) = std::sync::mpsc::sync_channel(1);
        let mut config = ScheduledTaskConfig::fixed_rate("bounded", Duration::from_secs(60));
        config.max_run_time = Some(Duration::from_secs(5));
        schedules
            .schedule_bounded(
                config,
                ScheduledExecutionPolicy::serial(MissedTickPolicy::Skip),
                move || {
                    let value = std::hint::black_box(&payload)[0];
                    let sent = sent.clone();
                    async move {
                        payload_task::<32768>().await;
                        sent.send(value).unwrap();
                    }
                },
            )
            .unwrap();
        assert_eq!(received.recv_timeout(Duration::from_secs(5)).unwrap(), 7);
        assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
    });
}
