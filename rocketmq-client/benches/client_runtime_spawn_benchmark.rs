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
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_client_rust::client_runtime_fallback_snapshot;
use rocketmq_client_rust::reset_client_runtime_fallback_for_diagnostics;
use rocketmq_client_rust::spawn_client_runtime_probe_task;
use rocketmq_client_rust::ClientRuntimeTaskHandle;
use rocketmq_client_rust::ClientSharedFallbackSnapshot;
use rocketmq_runtime::ShutdownReport;

#[derive(Debug)]
struct ClientRuntimeSpawnOutput {
    task_count: usize,
    elapsed: Duration,
    snapshot: ClientSharedFallbackSnapshot,
    shutdown_report: Option<ShutdownReport>,
    acquire_count_delta: usize,
    runtime_created_delta: usize,
    runtime_reused_delta: usize,
    submitted_tasks_delta: usize,
}

fn run_fallback_spawn(task_count: usize) -> ClientRuntimeSpawnOutput {
    reset_client_runtime_fallback_for_diagnostics(Duration::from_secs(5))
        .expect("client fallback runtime should reset before benchmark");
    let before = client_runtime_fallback_snapshot();
    let before_acquire_count = before.map(|snapshot| snapshot.acquire_count).unwrap_or_default();
    let before_runtime_created = before.map(|snapshot| snapshot.runtime_created).unwrap_or_default();
    let before_runtime_reused = before.map(|snapshot| snapshot.runtime_reused).unwrap_or_default();
    let before_submitted_tasks = before.map(|snapshot| snapshot.submitted_tasks).unwrap_or_default();

    let (tx, rx) = mpsc::channel();
    let started_at = Instant::now();
    let mut task_handles = Vec::with_capacity(task_count);
    for task_index in 0..task_count {
        let tx = tx.clone();
        let task_handle = spawn_client_runtime_probe_task("rocketmq-client-runtime-spawn-bench", async move {
            tx.send(task_index).expect("benchmark receiver should stay alive");
        })
        .expect("client fallback task should spawn");
        task_handles.push(task_handle);
    }
    drop(tx);

    for _ in 0..task_count {
        rx.recv_timeout(Duration::from_secs(5))
            .expect("client fallback task should complete");
    }
    wait_for_tasks_finished(&task_handles);
    wait_for_fallback_idle();
    let elapsed = started_at.elapsed();
    let snapshot = client_runtime_fallback_snapshot().expect("client fallback runtime should exist after spawn");

    assert_eq!(snapshot.active_tasks, 0, "{snapshot:?}");
    assert_eq!(snapshot.active_leases, 0, "{snapshot:?}");
    assert_eq!(snapshot.runtime_created - before_runtime_created, 1, "{snapshot:?}");
    assert_eq!(
        snapshot.runtime_reused - before_runtime_reused,
        task_count.saturating_sub(1),
        "{snapshot:?}"
    );

    let shutdown_report = reset_client_runtime_fallback_for_diagnostics(Duration::from_secs(5))
        .expect("client fallback runtime should reset after benchmark");
    if let Some(report) = &shutdown_report {
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    ClientRuntimeSpawnOutput {
        task_count,
        elapsed,
        snapshot,
        shutdown_report,
        acquire_count_delta: snapshot.acquire_count - before_acquire_count,
        runtime_created_delta: snapshot.runtime_created - before_runtime_created,
        runtime_reused_delta: snapshot.runtime_reused - before_runtime_reused,
        submitted_tasks_delta: snapshot.submitted_tasks - before_submitted_tasks,
    }
}

fn wait_for_fallback_idle() {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let snapshot = client_runtime_fallback_snapshot().expect("client fallback runtime should exist");
        if snapshot.active_tasks == 0 && snapshot.active_leases == 0 {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "client fallback runtime did not become idle: {snapshot:?}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn wait_for_tasks_finished(task_handles: &[ClientRuntimeTaskHandle]) {
    for task_handle in task_handles {
        assert!(
            task_handle.wait_finished(Duration::from_secs(5)),
            "client fallback task did not finish"
        );
    }
}

fn percentile_duration_micros(samples: &[Duration], percentile: usize) -> u128 {
    assert!(!samples.is_empty(), "percentile requires at least one sample");
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len() - 1) * percentile) / 100;
    sorted[index].as_micros()
}

fn snapshot_json(snapshot: ClientSharedFallbackSnapshot) -> serde_json::Value {
    serde_json::json!({
        "state": format!("{:?}", snapshot.state),
        "acquire_count": snapshot.acquire_count,
        "runtime_created": snapshot.runtime_created,
        "runtime_reused": snapshot.runtime_reused,
        "submitted_tasks": snapshot.submitted_tasks,
        "active_tasks": snapshot.active_tasks,
        "active_leases": snapshot.active_leases,
        "runtime_generation": snapshot.runtime_generation,
        "runtime_available": snapshot.runtime_available,
        "idle_shutdowns": snapshot.idle_shutdowns,
        "explicit_shutdowns": snapshot.explicit_shutdowns,
        "idle_reaper_starts": snapshot.idle_reaper_starts,
    })
}

fn write_client_runtime_report_artifact() {
    let sample_count = 10;
    let outputs = (0..sample_count).map(|_| run_fallback_spawn(128)).collect::<Vec<_>>();
    let elapsed_samples = outputs.iter().map(|output| output.elapsed).collect::<Vec<_>>();
    let output = outputs.last().expect("runtime report should have at least one sample");
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-client should live below workspace root")
        .to_path_buf();
    let output_dir = workspace_root.join("target/runtime-baseline/prototype");
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "client_runtime_spawn",
        "generated_at_unix_ms": generated_at_unix_ms,
        "fallback_spawn": {
            "task_count": output.task_count,
            "sample_count": sample_count,
            "elapsed_ms": output.elapsed.as_millis(),
            "elapsed_us": output.elapsed.as_micros(),
            "elapsed_p50_us": percentile_duration_micros(&elapsed_samples, 50),
            "elapsed_p95_us": percentile_duration_micros(&elapsed_samples, 95),
            "elapsed_p99_us": percentile_duration_micros(&elapsed_samples, 99),
            "acquire_count_delta": output.acquire_count_delta,
            "runtime_created_delta": output.runtime_created_delta,
            "runtime_reused_delta": output.runtime_reused_delta,
            "submitted_tasks_delta": output.submitted_tasks_delta,
            "snapshot": snapshot_json(output.snapshot),
            "shutdown_report": output.shutdown_report,
            "healthy": output
                .shutdown_report
                .as_ref()
                .map(ShutdownReport::is_healthy)
                .unwrap_or(true),
        },
    });
    let path = output_dir.join("client-runtime-spawn-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("client runtime benchmark artifact should serialize"),
    )
    .expect("client runtime benchmark artifact should be written");
}

fn bench_client_runtime_spawn(criterion: &mut Criterion) {
    write_client_runtime_report_artifact();

    let mut group = criterion.benchmark_group("client_runtime_spawn");
    for task_count in [32usize, 128] {
        group.bench_with_input(
            BenchmarkId::new("fallback_spawn", task_count),
            &task_count,
            |bencher, task_count| {
                bencher.iter(|| {
                    let output = run_fallback_spawn(black_box(*task_count));
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
    targets = bench_client_runtime_spawn
}
criterion_main!(benches);
