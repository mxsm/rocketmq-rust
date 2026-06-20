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
use rocketmq_common::ScheduledExecutorService;
use rocketmq_common::TokioExecutorService;
use serde::Serialize;

#[derive(Debug, Serialize)]
struct TokioExecutorProbe {
    task_count: usize,
    completed: usize,
    spawn_wait_elapsed_us: u128,
    shutdown_elapsed_us: u128,
    healthy: bool,
}

#[derive(Debug, Serialize)]
struct ScheduledExecutorProbe {
    runs: usize,
    max_active: usize,
    shutdown_elapsed_us: u128,
    healthy: bool,
}

#[derive(Debug, Serialize)]
struct CommonExecutorLifecycleProbe {
    tokio_executor: TokioExecutorProbe,
    scheduled_executor: ScheduledExecutorProbe,
    healthy: bool,
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-common should live below workspace root")
        .to_path_buf()
}

fn benchmark_artifact_dir() -> PathBuf {
    workspace_root().join("target/runtime-baseline/prototype")
}

fn run_tokio_executor_probe(task_count: usize) -> TokioExecutorProbe {
    let executor =
        TokioExecutorService::try_new_with_config(2, Some("rocketmq-common-tokio-bench"), Duration::from_secs(1), 4)
            .expect("tokio executor benchmark runtime should start");

    let completed = Arc::new(AtomicUsize::new(0));
    let started_at = Instant::now();
    let mut task_ids = Vec::with_capacity(task_count);
    for _ in 0..task_count {
        let completed = completed.clone();
        task_ids.push(executor.spawn(async move {
            completed.fetch_add(1, Ordering::Relaxed);
        }));
    }
    executor.block_on(async {
        for task_id in task_ids {
            assert!(
                executor.wait_task(task_id, Duration::from_secs(1)).await,
                "tokio executor task should finish"
            );
        }
    });
    let spawn_wait_elapsed_us = started_at.elapsed().as_micros();
    let completed = completed.load(Ordering::Relaxed);

    let shutdown_started_at = Instant::now();
    executor.shutdown_timeout(Duration::from_secs(1));
    let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();

    TokioExecutorProbe {
        task_count,
        completed,
        spawn_wait_elapsed_us,
        shutdown_elapsed_us,
        healthy: completed == task_count,
    }
}

fn run_scheduled_executor_probe(run_duration: Duration) -> ScheduledExecutorProbe {
    let executor = ScheduledExecutorService::try_new_with_config(
        2,
        Some("rocketmq-common-scheduled-bench"),
        Duration::from_secs(1),
        4,
    )
    .expect("scheduled executor benchmark runtime should start");
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let runs = Arc::new(AtomicUsize::new(0));

    let active_task = active.clone();
    let max_active_task = max_active.clone();
    let runs_task = runs.clone();
    executor.schedule_at_fixed_rate(
        move || {
            let current = active_task.fetch_add(1, Ordering::SeqCst) + 1;
            max_active_task.fetch_max(current, Ordering::SeqCst);
            runs_task.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(5));
            active_task.fetch_sub(1, Ordering::SeqCst);
        },
        Some(Duration::ZERO),
        Duration::from_millis(1),
    );

    std::thread::sleep(run_duration);
    let runs = runs.load(Ordering::SeqCst);
    let max_active = max_active.load(Ordering::SeqCst);

    let shutdown_started_at = Instant::now();
    executor.shutdown_timeout(Duration::from_secs(1));
    let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();

    ScheduledExecutorProbe {
        runs,
        max_active,
        shutdown_elapsed_us,
        healthy: runs > 0 && max_active == 1,
    }
}

fn run_common_executor_lifecycle_probe(task_count: usize) -> CommonExecutorLifecycleProbe {
    let tokio_executor = run_tokio_executor_probe(task_count);
    let scheduled_executor = run_scheduled_executor_probe(Duration::from_millis(40));
    let healthy = tokio_executor.healthy && scheduled_executor.healthy;
    CommonExecutorLifecycleProbe {
        tokio_executor,
        scheduled_executor,
        healthy,
    }
}

fn write_common_executor_report_artifact() {
    let output = run_common_executor_lifecycle_probe(128);
    assert!(output.healthy, "{output:?}");
    let output_dir = benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "common_executor_lifecycle",
        "generated_at_unix_ms": generated_at_unix_ms,
        "probe": output,
    });
    let path = output_dir.join("common-executor-lifecycle-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("common executor benchmark artifact should serialize"),
    )
    .expect("common executor benchmark artifact should be written");
}

fn bench_common_executor_lifecycle(criterion: &mut Criterion) {
    write_common_executor_report_artifact();

    let mut group = criterion.benchmark_group("common_executor_lifecycle");
    for task_count in [32usize, 128] {
        group.bench_with_input(
            BenchmarkId::new("tokio_executor_spawn_wait_shutdown", task_count),
            &task_count,
            |bencher, task_count| {
                bencher.iter(|| {
                    let output = run_tokio_executor_probe(black_box(*task_count));
                    assert!(output.healthy, "{output:?}");
                    black_box(output.spawn_wait_elapsed_us);
                    black_box(output.shutdown_elapsed_us);
                });
            },
        );
    }
    group.bench_function("scheduled_executor_no_overlap_shutdown", |bencher| {
        bencher.iter(|| {
            let output = run_scheduled_executor_probe(Duration::from_millis(black_box(20)));
            assert!(output.healthy, "{output:?}");
            black_box(output.runs);
            black_box(output.shutdown_elapsed_us);
        });
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1));
    targets = bench_common_executor_lifecycle
}
criterion_main!(benches);
