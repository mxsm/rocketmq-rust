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
use futures::future::join_all;
use rocketmq_runtime::BlockingExecutorSnapshot;
use rocketmq_runtime::BlockingPoolPolicy;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ShutdownReport;
use tokio::sync::oneshot;

#[derive(Debug)]
struct CompletedBlockingOutput {
    task_count: usize,
    elapsed: Duration,
    max_active: usize,
    snapshot: BlockingExecutorSnapshot,
    report: ShutdownReport,
}

#[derive(Debug)]
struct TimeoutBlockingOutput {
    timed_out_snapshot: BlockingExecutorSnapshot,
    cleaned_snapshot: BlockingExecutorSnapshot,
    report: ShutdownReport,
}

fn runtime_config(policy: BlockingPoolPolicy) -> RuntimeConfig {
    let max_blocking_threads = (policy.max_concurrency * 2).max(2);
    RuntimeConfig {
        worker_threads: 2,
        max_blocking_threads,
        shutdown_timeout: Duration::from_secs(5),
        thread_name: "rocketmq-blocking-bench".to_string(),
        blocking_pool_policy: policy,
        ..RuntimeConfig::default()
    }
}

fn run_completed_blocking(
    task_count: usize,
    max_concurrency: usize,
    work_duration: Duration,
) -> CompletedBlockingOutput {
    let policy = BlockingPoolPolicy {
        name: "blocking-bench-completed".to_string(),
        max_concurrency,
        queue_timeout: Duration::from_secs(2),
        task_timeout: Duration::from_secs(2),
        warn_after: Duration::from_secs(10),
    };
    let owner = RuntimeOwner::new(runtime_config(policy)).expect("runtime owner should start");
    let context = owner.context().clone();

    owner.block_on(async move {
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let started_at = Instant::now();
        let executor = context.blocking().clone();
        let runs = (0..task_count)
            .map(|task_index| {
                let executor = executor.clone();
                let active = active.clone();
                let max_active = max_active.clone();
                async move {
                    executor
                        .spawn_io(format!("short-io-{task_index}"), move || {
                            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                            max_active.fetch_max(current, Ordering::SeqCst);
                            std::thread::sleep(work_duration);
                            active.fetch_sub(1, Ordering::SeqCst);
                            task_index
                        })
                        .await
                }
            })
            .collect::<Vec<_>>();

        let results = join_all(runs).await;
        for result in results {
            result.expect("short blocking task should complete");
        }
        let elapsed = started_at.elapsed();
        let snapshot = executor.snapshot();
        assert_eq!(snapshot.blocking_still_running, 0, "{snapshot:?}");
        assert!(snapshot.tasks.is_empty(), "{snapshot:?}");
        assert!(max_active.load(Ordering::SeqCst) <= max_concurrency);

        let report = context.shutdown_tasks(Duration::from_secs(5)).await;
        assert!(report.is_healthy(), "{}", report.to_json());

        CompletedBlockingOutput {
            task_count,
            elapsed,
            max_active: max_active.load(Ordering::SeqCst),
            snapshot,
            report,
        }
    })
}

fn run_timeout_still_running() -> TimeoutBlockingOutput {
    let policy = BlockingPoolPolicy {
        name: "blocking-bench-timeout".to_string(),
        max_concurrency: 1,
        queue_timeout: Duration::from_secs(2),
        task_timeout: Duration::from_millis(20),
        warn_after: Duration::from_secs(10),
    };
    let owner = RuntimeOwner::new(runtime_config(policy)).expect("runtime owner should start");
    let context = owner.context().clone();

    owner.block_on(async move {
        let executor = context.blocking().clone();
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let run = {
            let executor = executor.clone();
            tokio::spawn(async move {
                executor
                    .spawn_io("timeout-io", move || {
                        let _ = started_tx.send(());
                        let _ = release_rx.recv();
                    })
                    .await
            })
        };

        started_rx.await.expect("blocking task should start");
        let error = run
            .await
            .expect("blocking caller task should join")
            .expect_err("blocking task should time out while closure still runs");
        assert!(matches!(error, RuntimeError::BlockingTaskTimeoutStillRunning { .. }));

        let timed_out_snapshot = executor.snapshot();
        assert_eq!(timed_out_snapshot.timed_out_still_running, 1, "{timed_out_snapshot:?}");
        assert_eq!(timed_out_snapshot.blocking_still_running, 1, "{timed_out_snapshot:?}");

        release_tx.send(()).expect("release signal should reach blocking task");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if executor.blocking_still_running() == 0 && executor.snapshot().tasks.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("blocking reaper should remove late-exiting task");

        let cleaned_snapshot = executor.snapshot();
        assert_eq!(cleaned_snapshot.blocking_still_running, 0, "{cleaned_snapshot:?}");
        assert!(cleaned_snapshot.tasks.is_empty(), "{cleaned_snapshot:?}");

        let report = context.shutdown_tasks(Duration::from_secs(5)).await;
        assert!(report.is_healthy(), "{}", report.to_json());

        TimeoutBlockingOutput {
            timed_out_snapshot,
            cleaned_snapshot,
            report,
        }
    })
}

fn write_blocking_report_artifact() {
    let completed = run_completed_blocking(32, 4, Duration::from_millis(2));
    let timeout = run_timeout_still_running();
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-runtime should live below workspace root")
        .to_path_buf();
    let output_dir = workspace_root.join("target/runtime-baseline/prototype");
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "blocking_executor",
        "generated_at_unix_ms": generated_at_unix_ms,
        "completed": {
            "task_count": completed.task_count,
            "elapsed_ms": completed.elapsed.as_millis(),
            "max_active": completed.max_active,
            "snapshot": completed.snapshot,
            "healthy": completed.report.is_healthy(),
            "shutdown_report": completed.report,
        },
        "timeout_still_running": {
            "timed_out_snapshot": timeout.timed_out_snapshot,
            "cleaned_snapshot": timeout.cleaned_snapshot,
            "healthy": timeout.report.is_healthy(),
            "shutdown_report": timeout.report,
        },
    });
    let path = output_dir.join("blocking-executor-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("blocking benchmark artifact should serialize"),
    )
    .expect("blocking benchmark artifact should be written");
}

fn bench_blocking_executor(criterion: &mut Criterion) {
    write_blocking_report_artifact();

    let mut group = criterion.benchmark_group("blocking_executor");
    for task_count in [8usize, 32] {
        group.bench_with_input(
            BenchmarkId::new("spawn_io_completed", task_count),
            &task_count,
            |bencher, task_count| {
                bencher.iter(|| {
                    let output = run_completed_blocking(black_box(*task_count), 4, Duration::from_millis(1));
                    black_box(output.elapsed);
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
    targets = bench_blocking_executor
}
criterion_main!(benches);
