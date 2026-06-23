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

use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_rust::schedule::executor::ExecutorConfig;
use rocketmq_rust::schedule::executor::TaskExecutor;
use rocketmq_rust::schedule::simple_scheduler::ScheduleMode;
use rocketmq_rust::schedule::simple_scheduler::ScheduledTaskManager;
use rocketmq_rust::schedule::Task;
use rocketmq_rust::schedule::TaskResult;
use rocketmq_rust::SchedulerConfig;
use rocketmq_rust::TaskScheduler;
use serde::Serialize;

#[derive(Debug, Serialize)]
struct SchedulerLifecycleProbe {
    max_scheduler_threads: usize,
    enable_persistence: bool,
    task_count_before_shutdown: usize,
    task_count_after_shutdown: usize,
    shutdown_elapsed_us: u128,
    completed: usize,
    cancelled: usize,
    aborted: usize,
    panicked: usize,
    timed_out: usize,
    leaked: usize,
    healthy: bool,
}

#[derive(Debug, Serialize)]
struct ScheduledTaskManagerProbe {
    task_count: usize,
    runs: usize,
    task_count_before_shutdown: usize,
    driver_task_count_before_shutdown: usize,
    task_count_after_shutdown: usize,
    driver_task_count_after_shutdown: usize,
    shutdown_elapsed_us: u128,
    report_task_count: usize,
    completed: usize,
    aborted: usize,
    panicked: usize,
    timed_out: usize,
    task_group_healthy: bool,
    healthy: bool,
}

#[derive(Debug, Serialize)]
struct TaskExecutorProbe {
    task_count: usize,
    started: usize,
    dropped: usize,
    running_task_count_before_shutdown: usize,
    task_group_count_before_shutdown: usize,
    running_task_count_after_shutdown: usize,
    task_group_count_after_shutdown: usize,
    cancelled_by_shutdown: usize,
    shutdown_elapsed_us: u128,
    task_group_aborted: usize,
    task_group_healthy: bool,
    healthy: bool,
}

struct DropCounter(Arc<AtomicUsize>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Release);
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq crate should live below workspace root")
        .to_path_buf()
}

fn benchmark_artifact_dir() -> PathBuf {
    workspace_root().join("target/runtime-baseline/prototype")
}

fn run_scheduler_lifecycle_probe(max_scheduler_threads: usize, enable_persistence: bool) -> SchedulerLifecycleProbe {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .thread_name("rocketmq-scheduler-lifecycle-bench")
        .enable_all()
        .build()
        .expect("scheduler lifecycle benchmark runtime should start");

    runtime.block_on(async move {
        let scheduler = TaskScheduler::new(SchedulerConfig {
            check_interval: Duration::from_secs(60),
            persistence_interval: Duration::from_secs(60),
            max_scheduler_threads,
            enable_persistence,
            ..SchedulerConfig::default()
        });

        scheduler.start().await.expect("scheduler should start");
        let task_count_before_shutdown = scheduler.scheduler_task_count().await;
        tokio::task::yield_now().await;

        let started_at = Instant::now();
        scheduler.stop().await.expect("scheduler should stop");
        let shutdown_elapsed_us = started_at.elapsed().as_micros();
        let report = scheduler
            .last_scheduler_shutdown_report()
            .await
            .expect("scheduler shutdown report should exist");
        let task_count_after_shutdown = scheduler.scheduler_task_count().await;

        assert_eq!(
            task_count_before_shutdown,
            max_scheduler_threads + usize::from(enable_persistence),
            "{}",
            report.to_json()
        );
        assert_eq!(task_count_after_shutdown, 0, "{}", report.to_json());
        assert!(report.is_healthy(), "{}", report.to_json());

        SchedulerLifecycleProbe {
            max_scheduler_threads,
            enable_persistence,
            task_count_before_shutdown,
            task_count_after_shutdown,
            shutdown_elapsed_us,
            completed: report.completed,
            cancelled: report.cancelled,
            aborted: report.aborted,
            panicked: report.panicked,
            timed_out: report.timed_out,
            leaked: report.leaked,
            healthy: report.is_healthy(),
        }
    })
}

fn write_scheduler_report_artifact() {
    let cases = [
        run_scheduler_lifecycle_probe(1, false),
        run_scheduler_lifecycle_probe(2, true),
    ];
    assert!(cases.iter().all(|case| case.healthy), "{cases:?}");

    let output_dir = benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "rocketmq_scheduler_lifecycle",
        "generated_at_unix_ms": generated_at_unix_ms,
        "cases": cases,
    });
    let path = output_dir.join("rocketmq-scheduler-lifecycle-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("scheduler lifecycle artifact should serialize"),
    )
    .expect("scheduler lifecycle artifact should be written");
}

fn run_scheduled_task_manager_probe(task_count: usize) -> ScheduledTaskManagerProbe {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .thread_name("rocketmq-scheduled-task-manager-bench")
        .enable_all()
        .build()
        .expect("scheduled task manager benchmark runtime should start");

    runtime.block_on(async move {
        let manager = ScheduledTaskManager::new_legacy_compatibility();
        let runs = Arc::new(AtomicUsize::new(0));
        for _ in 0..task_count {
            let runs = runs.clone();
            manager
                .add_scheduled_task(
                    ScheduleMode::FixedDelay,
                    Duration::ZERO,
                    Duration::from_secs(60),
                    move |_token| {
                        let runs = runs.clone();
                        async move {
                            runs.fetch_add(1, Ordering::Relaxed);
                            Ok(())
                        }
                    },
                )
                .expect("scheduled task manager driver should start");
        }

        tokio::time::timeout(Duration::from_secs(1), async {
            while runs.load(Ordering::Acquire) < task_count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all scheduled task manager probes should run once");

        let task_count_before_shutdown = manager.task_count();
        let driver_task_count_before_shutdown = manager.driver_task_count();
        let started_at = Instant::now();
        let report = manager.shutdown_all(Duration::from_secs(1)).await;
        let shutdown_elapsed_us = started_at.elapsed().as_micros();
        let task_group_report = manager
            .last_task_group_shutdown_report()
            .expect("scheduled task manager task group report should exist");

        assert_eq!(task_count_before_shutdown, task_count);
        assert_eq!(driver_task_count_before_shutdown, task_count);
        assert_eq!(manager.task_count(), 0);
        assert_eq!(manager.driver_task_count(), 0);
        assert!(report.is_healthy(), "{report:?}");
        assert!(task_group_report.is_healthy(), "{}", task_group_report.to_json());

        ScheduledTaskManagerProbe {
            task_count,
            runs: runs.load(Ordering::Acquire),
            task_count_before_shutdown,
            driver_task_count_before_shutdown,
            task_count_after_shutdown: manager.task_count(),
            driver_task_count_after_shutdown: manager.driver_task_count(),
            shutdown_elapsed_us,
            report_task_count: report.task_count,
            completed: report.completed,
            aborted: report.aborted,
            panicked: report.panicked,
            timed_out: report.timed_out,
            task_group_healthy: task_group_report.is_healthy(),
            healthy: report.is_healthy() && task_group_report.is_healthy(),
        }
    })
}

fn write_scheduled_task_manager_report_artifact() {
    let cases = [run_scheduled_task_manager_probe(1), run_scheduled_task_manager_probe(8)];
    assert!(cases.iter().all(|case| case.healthy), "{cases:?}");

    let output_dir = benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "rocketmq_scheduled_task_manager_lifecycle",
        "generated_at_unix_ms": generated_at_unix_ms,
        "cases": cases,
    });
    let path = output_dir.join("rocketmq-scheduled-task-manager-lifecycle-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("scheduled task manager artifact should serialize"),
    )
    .expect("scheduled task manager artifact should be written");
}

fn run_task_executor_probe(task_count: usize) -> TaskExecutorProbe {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .thread_name("rocketmq-task-executor-bench")
        .enable_all()
        .build()
        .expect("task executor benchmark runtime should start");

    runtime.block_on(async move {
        let executor = TaskExecutor::new(ExecutorConfig {
            max_concurrent_tasks: task_count.max(1),
            default_timeout: Duration::from_secs(60),
            enable_metrics: true,
        });
        let started = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));

        for index in 0..task_count {
            let started = started.clone();
            let dropped = dropped.clone();
            let task = Arc::new(Task::new(
                format!("executor-pending-task-{index}"),
                format!("executor-pending-task-{index}"),
                move |_context| {
                    let started = started.clone();
                    let dropped = dropped.clone();
                    async move {
                        let _drop_counter = DropCounter(dropped);
                        started.fetch_add(1, Ordering::Release);
                        std::future::pending::<TaskResult>().await
                    }
                },
            ));
            executor
                .execute_task(task, SystemTime::now())
                .await
                .expect("task executor benchmark task should start");
        }

        tokio::time::timeout(Duration::from_secs(1), async {
            while started.load(Ordering::Acquire) < task_count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all task executor benchmark tasks should start");

        let running_task_count_before_shutdown = executor.running_task_count().await;
        let task_group_count_before_shutdown = executor.task_group_task_count().await;
        let started_at = Instant::now();
        let cancelled_by_shutdown = executor.shutdown_all(Duration::from_secs(1)).await;
        let shutdown_elapsed_us = started_at.elapsed().as_micros();
        let report = executor
            .last_task_group_shutdown_report()
            .await
            .expect("task executor task group report should exist");
        let running_task_count_after_shutdown = executor.running_task_count().await;
        let task_group_count_after_shutdown = executor.task_group_task_count().await;
        let dropped = dropped.load(Ordering::Acquire);

        assert_eq!(running_task_count_before_shutdown, task_count);
        assert_eq!(task_group_count_before_shutdown, task_count);
        assert_eq!(cancelled_by_shutdown, task_count);
        assert_eq!(running_task_count_after_shutdown, 0);
        assert_eq!(task_group_count_after_shutdown, 0);
        assert_eq!(dropped, task_count);
        assert!(report.is_healthy(), "{}", report.to_json());
        assert_eq!(report.aborted, task_count, "{}", report.to_json());

        TaskExecutorProbe {
            task_count,
            started: started.load(Ordering::Acquire),
            dropped,
            running_task_count_before_shutdown,
            task_group_count_before_shutdown,
            running_task_count_after_shutdown,
            task_group_count_after_shutdown,
            cancelled_by_shutdown,
            shutdown_elapsed_us,
            task_group_aborted: report.aborted,
            task_group_healthy: report.is_healthy(),
            healthy: report.is_healthy(),
        }
    })
}

fn write_task_executor_report_artifact() {
    let cases = [run_task_executor_probe(1), run_task_executor_probe(8)];
    assert!(cases.iter().all(|case| case.healthy), "{cases:?}");

    let output_dir = benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "rocketmq_task_executor_lifecycle",
        "generated_at_unix_ms": generated_at_unix_ms,
        "cases": cases,
    });
    let path = output_dir.join("rocketmq-task-executor-lifecycle-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("task executor artifact should serialize"),
    )
    .expect("task executor artifact should be written");
}

fn bench_scheduler_lifecycle(criterion: &mut Criterion) {
    write_scheduler_report_artifact();

    let mut group = criterion.benchmark_group("rocketmq_scheduler_lifecycle");
    for (max_scheduler_threads, enable_persistence) in [(1usize, false), (2, true)] {
        group.bench_with_input(
            BenchmarkId::new(
                "task_group_shutdown",
                format!("threads={max_scheduler_threads},persistence={enable_persistence}"),
            ),
            &(max_scheduler_threads, enable_persistence),
            |bencher, (max_scheduler_threads, enable_persistence)| {
                bencher.iter(|| {
                    let output = run_scheduler_lifecycle_probe(
                        black_box(*max_scheduler_threads),
                        black_box(*enable_persistence),
                    );
                    assert!(output.healthy, "{output:?}");
                    black_box(output.task_count_before_shutdown);
                    black_box(output.task_count_after_shutdown);
                    black_box(output.shutdown_elapsed_us);
                });
            },
        );
    }
    group.finish();
}

fn bench_scheduled_task_manager_lifecycle(criterion: &mut Criterion) {
    write_scheduled_task_manager_report_artifact();

    let mut group = criterion.benchmark_group("rocketmq_scheduled_task_manager_lifecycle");
    for task_count in [1usize, 8] {
        group.bench_with_input(
            BenchmarkId::new("task_group_driver_shutdown", task_count),
            &task_count,
            |bencher, task_count| {
                bencher.iter(|| {
                    let output = run_scheduled_task_manager_probe(black_box(*task_count));
                    assert!(output.healthy, "{output:?}");
                    black_box(output.driver_task_count_before_shutdown);
                    black_box(output.driver_task_count_after_shutdown);
                    black_box(output.shutdown_elapsed_us);
                });
            },
        );
    }
    group.finish();
}

fn bench_task_executor_lifecycle(criterion: &mut Criterion) {
    write_task_executor_report_artifact();

    let mut group = criterion.benchmark_group("rocketmq_task_executor_lifecycle");
    for task_count in [1usize, 8] {
        group.bench_with_input(
            BenchmarkId::new("task_group_execution_shutdown", task_count),
            &task_count,
            |bencher, task_count| {
                bencher.iter(|| {
                    let output = run_task_executor_probe(black_box(*task_count));
                    assert!(output.healthy, "{output:?}");
                    black_box(output.running_task_count_before_shutdown);
                    black_box(output.task_group_count_after_shutdown);
                    black_box(output.shutdown_elapsed_us);
                });
            },
        );
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1));
    targets = bench_scheduler_lifecycle, bench_scheduled_task_manager_lifecycle, bench_task_executor_lifecycle
}
criterion_main!(benches);
