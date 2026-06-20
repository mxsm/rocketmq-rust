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
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ShutdownReport;

fn runtime_config() -> RuntimeConfig {
    RuntimeConfig {
        worker_threads: 2,
        max_blocking_threads: 2,
        shutdown_timeout: Duration::from_secs(5),
        thread_name: "rocketmq-runtime-bench".to_string(),
        ..RuntimeConfig::default()
    }
}

fn run_spawn_shutdown(task_count: usize) -> ShutdownReport {
    let owner = RuntimeOwner::new(runtime_config()).expect("runtime owner should start");
    let context = owner.context().clone();

    let report = owner.block_on(async move {
        let service = context.service_context(format!("bench.task-group.{task_count}"));
        for task_index in 0..task_count {
            service
                .spawn_service(format!("short-task-{task_index}"), async {})
                .expect("short benchmark task should spawn");
            if task_index % 256 == 0 {
                tokio::task::yield_now().await;
            }
        }

        context.shutdown_tasks(Duration::from_secs(5)).await
    });

    assert!(report.is_healthy(), "{}", report.to_json());
    assert!(report.remaining_tasks.is_empty(), "{}", report.to_json());
    report
}

fn write_shutdown_report_artifact() {
    let task_count = 10_000;
    let report = run_spawn_shutdown(task_count);
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
        "case": "task_group_lifecycle",
        "generated_at_unix_ms": generated_at_unix_ms,
        "task_count": task_count,
        "healthy": report.is_healthy(),
        "shutdown_report": report,
    });
    let path = output_dir.join("task-group-lifecycle-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("runtime benchmark artifact should serialize"),
    )
    .expect("runtime benchmark artifact should be written");
}

fn bench_task_group_lifecycle(criterion: &mut Criterion) {
    write_shutdown_report_artifact();

    let mut group = criterion.benchmark_group("task_group_lifecycle");
    for task_count in [128usize, 1024] {
        group.bench_with_input(
            BenchmarkId::new("spawn_shutdown_short_tasks", task_count),
            &task_count,
            |bencher, task_count| {
                bencher.iter(|| {
                    let report = run_spawn_shutdown(black_box(*task_count));
                    black_box(report.is_healthy());
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
    targets = bench_task_group_lifecycle
}
criterion_main!(benches);
