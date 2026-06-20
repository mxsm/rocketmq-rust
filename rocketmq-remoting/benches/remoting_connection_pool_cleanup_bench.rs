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
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use rocketmq_remoting::clients::connection_pool::ConnectionPool;
use rocketmq_runtime::ShutdownReport;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
struct RemotingConnectionPoolCleanupProbe {
    task_count_before_shutdown: usize,
    task_count_after_shutdown: usize,
    scheduled_runs: u64,
    scheduled_skips: u64,
    scheduled_overlaps: u64,
    scheduled_failures: u64,
    shutdown_elapsed_us: u128,
    shutdown_report: ShutdownReport,
    healthy: bool,
}

fn run_cleanup_probe() -> RemotingConnectionPoolCleanupProbe {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .thread_name("rocketmq-remoting-connection-pool-cleanup-bench")
        .enable_all()
        .build()
        .expect("remoting connection pool cleanup benchmark runtime should start");

    runtime.block_on(async {
        let pool = ConnectionPool::<()>::new(100, Duration::from_millis(1));
        let cleanup_task = pool.start_cleanup_task(Duration::from_millis(1));

        let mut snapshots = cleanup_task.schedule_snapshot();
        for _ in 0..50 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            snapshots = cleanup_task.schedule_snapshot();
        }

        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
        let task_count_before_shutdown = cleanup_task.task_count();
        let shutdown_started_at = Instant::now();
        let shutdown_report = cleanup_task.shutdown(Duration::from_secs(1)).await;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = cleanup_task.task_count();
        let healthy = scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && task_count_before_shutdown > 0
            && task_count_after_shutdown == 0
            && shutdown_report.is_healthy();

        RemotingConnectionPoolCleanupProbe {
            task_count_before_shutdown,
            task_count_after_shutdown,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            shutdown_elapsed_us,
            shutdown_report,
            healthy,
        }
    })
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-remoting should live below workspace root")
        .to_path_buf()
}

fn benchmark_artifact_dir() -> PathBuf {
    workspace_root().join("target/runtime-baseline/prototype")
}

fn write_remoting_connection_pool_cleanup_report_artifact() {
    let output = run_cleanup_probe();
    assert!(output.healthy, "{output:?}");
    let output_dir = benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "remoting_connection_pool_cleanup",
        "generated_at_unix_ms": generated_at_unix_ms,
        "probe": output,
    });
    let path = output_dir.join("remoting-connection-pool-cleanup-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload)
            .expect("remoting connection pool cleanup benchmark artifact should serialize"),
    )
    .expect("remoting connection pool cleanup benchmark artifact should be written");
}

fn bench_remoting_connection_pool_cleanup(criterion: &mut Criterion) {
    write_remoting_connection_pool_cleanup_report_artifact();

    criterion.bench_function("remoting_connection_pool_cleanup/fixed_delay_shutdown", |bencher| {
        bencher.iter(|| {
            let output = run_cleanup_probe();
            assert!(output.healthy, "{output:?}");
            black_box(output.scheduled_runs);
            black_box(output.scheduled_skips);
            black_box(output.scheduled_overlaps);
            black_box(output.shutdown_elapsed_us);
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1));
    targets = bench_remoting_connection_pool_cleanup
}
criterion_main!(benches);
