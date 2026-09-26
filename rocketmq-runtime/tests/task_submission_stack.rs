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

use std::num::NonZeroUsize;
use std::process::Command;
use std::sync::mpsc::SyncSender;
use std::time::Duration;

use rocketmq_runtime::task::service_task::ServiceTask;
use rocketmq_runtime::task::service_task::ServiceTaskContext;
use rocketmq_runtime::task::ServiceManager;
use rocketmq_runtime::MissedTickPolicy;
use rocketmq_runtime::OperationContext;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ScheduledExecutionPolicy;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskControl;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskKind;

const CHILD_MODE: &str = "ROCKETMQ_RUNTIME_STACK_TEST_CHILD";
const STACK_SIZE: usize = 1024 * 1024;
// A submitting thread whose stack is already mostly used, such as a Broker
// entrypoint starting services deep inside its startup future.
const SUBMITTER_STACK_SIZE: usize = 256 * 1024;
// Below the release inline boundary, like the Broker transaction check loop.
const MID_SIZE_PAYLOAD: usize = 12 * 1024;

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
        .schedule(
            ScheduledTaskConfig::fixed_delay("large-scheduled-task", Duration::from_secs(60)),
            ScheduledExecutionPolicy::default(),
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
    isolated_stack_probe_on(name, STACK_SIZE, probe);
}

fn isolated_stack_probe_on(name: &str, stack_size: usize, probe: fn()) {
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
        .stack_size(stack_size)
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

struct PayloadService<const N: usize> {
    running: SyncSender<()>,
}

impl<const N: usize> ServiceTask for PayloadService<N> {
    fn get_service_name(&self) -> String {
        "payload-service".to_owned()
    }

    async fn run(&self, context: &ServiceTaskContext) {
        let payload = [7_u8; N];
        self.running.send(()).unwrap();
        while !context.is_stopped() {
            context.wait_for_running(Duration::from_millis(10)).await;
        }
        assert_eq!(std::hint::black_box(&payload)[0], 7);
    }
}

fn start_and_stop_payload_service<const N: usize>(owner: &RuntimeOwner, group: &TaskGroup) {
    let (running, started) = std::sync::mpsc::sync_channel(1);
    let service = ServiceManager::new_with_task_group(PayloadService::<N> { running }, group.clone());
    owner.block_on(service.start()).unwrap();
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    owner.block_on(service.shutdown()).unwrap();
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
fn service_and_task_submission_fit_on_a_small_submitter_stack() {
    isolated_stack_probe_on(
        "service_and_task_submission_fit_on_a_small_submitter_stack",
        SUBMITTER_STACK_SIZE,
        || {
            let owner = small_stack_owner();
            let context = owner.root_context().component("submitter-stack");
            let group = context.task_group();

            // Unoptimized builds copy an inline future into every wrapper frame.
            let id = group
                .spawn("mid-size", TaskKind::Worker, payload_task::<MID_SIZE_PAYLOAD>())
                .unwrap();
            assert!(owner.block_on(group.wait_task(id, Duration::from_secs(5))));

            // A service loop is built by the worker, not moved down from the caller.
            start_and_stop_payload_service::<MID_SIZE_PAYLOAD>(&owner, group);
            start_and_stop_payload_service::<65536>(&owner, group);
            assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
        },
    );
}

#[test]
fn scheduled_runs_fit_on_one_mib_worker_stack() {
    isolated_stack_probe("scheduled_runs_fit_on_one_mib_worker_stack", || {
        let owner = small_stack_owner();
        let context = owner.root_context().component("schedule-stack");
        let schedules = ScheduledTaskGroup::new(context.task_group().clone());
        let (sent, received) = std::sync::mpsc::sync_channel(1);
        schedules
            .schedule(
                ScheduledTaskConfig::fixed_delay("large-run", Duration::from_secs(60)),
                ScheduledExecutionPolicy::default(),
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
            .schedule(
                ScheduledTaskConfig::fixed_delay("large-factory", Duration::from_secs(60)),
                ScheduledExecutionPolicy::default(),
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
            ($name:literal, $mode:ident, |$config:ident, $task:ident| $register:expr, $result:expr) => {{
                let (sent, received) = std::sync::mpsc::sync_channel(1);
                let mut $config = ScheduledTaskConfig::$mode($name, Duration::from_secs(60));
                $config.max_run_time = Some(Duration::from_secs(5));
                let $task = move || {
                    let sent = sent.clone();
                    async move {
                        payload_task::<32768>().await;
                        sent.send(()).unwrap();
                        $result
                    }
                };
                $register.unwrap();
                received.recv_timeout(Duration::from_secs(5)).unwrap();
            }};
        }

        let serial = ScheduledExecutionPolicy::default();
        let overlapping = ScheduledExecutionPolicy::bounded(NonZeroUsize::new(2).unwrap(), MissedTickPolicy::Skip);
        check_schedule!(
            "delay",
            fixed_delay,
            |config, task| schedules.schedule(config, serial, task),
            ()
        );
        check_schedule!(
            "delay-operation",
            fixed_delay,
            |config, task| schedules.schedule_operation(&operation, config, serial, task),
            ()
        );
        check_schedule!(
            "controlled",
            fixed_delay,
            |config, task| schedules.schedule_controlled(config, task),
            ScheduledTaskControl::Stop
        );
        check_schedule!(
            "no-overlap",
            fixed_rate_no_overlap,
            |config, task| schedules.schedule(config, serial, task),
            ()
        );
        check_schedule!(
            "no-overlap-operation",
            fixed_rate_no_overlap,
            |config, task| schedules.schedule_operation(&operation, config, serial, task),
            ()
        );
        check_schedule!(
            "overlap",
            fixed_rate,
            |config, task| schedules.schedule(config, overlapping, task),
            ()
        );

        // Exercise both boxing decisions together: a large factory and a large run.
        let payload = [7_u8; 65536];
        let (sent, received) = std::sync::mpsc::sync_channel(1);
        let mut config = ScheduledTaskConfig::fixed_rate_no_overlap("bounded", Duration::from_secs(60));
        config.max_run_time = Some(Duration::from_secs(5));
        schedules
            .schedule(config, serial, move || {
                let value = std::hint::black_box(&payload)[0];
                let sent = sent.clone();
                async move {
                    payload_task::<32768>().await;
                    sent.send(value).unwrap();
                }
            })
            .unwrap();
        assert_eq!(received.recv_timeout(Duration::from_secs(5)).unwrap(), 7);
        assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
    });
}
