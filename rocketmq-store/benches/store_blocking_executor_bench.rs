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
use rocketmq_store::bench_support::run_store_blocking_io_probe;
use rocketmq_store::bench_support::StoreBlockingIoProbe;

fn run_store_blocking_probe(task_count: usize, work_duration: Duration) -> StoreBlockingIoProbe {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(64)
        .thread_name("rocketmq-store-blocking-bench")
        .enable_all()
        .build()
        .expect("store blocking benchmark runtime should start");

    runtime.block_on(run_store_blocking_io_probe(task_count, work_duration))
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-store should live below workspace root")
        .to_path_buf()
}

fn benchmark_artifact_dir() -> PathBuf {
    workspace_root().join("target/runtime-baseline/prototype")
}

fn write_store_blocking_report_artifact() {
    let output = run_store_blocking_probe(96, Duration::from_millis(2));
    assert!(output.healthy, "{output:?}");
    let output_dir = benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "store_blocking_executor",
        "generated_at_unix_ms": generated_at_unix_ms,
        "probe": output,
    });
    let path = output_dir.join("store-blocking-executor-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("store blocking benchmark artifact should serialize"),
    )
    .expect("store blocking benchmark artifact should be written");
}

fn bench_store_blocking_executor(criterion: &mut Criterion) {
    write_store_blocking_report_artifact();

    let mut group = criterion.benchmark_group("store_blocking_executor");
    for task_count in [32usize, 96] {
        group.bench_with_input(
            BenchmarkId::new("spawn_io_queue_wait", task_count),
            &task_count,
            |bencher, task_count| {
                bencher.iter(|| {
                    let output = run_store_blocking_probe(black_box(*task_count), Duration::from_millis(1));
                    assert!(output.healthy, "{output:?}");
                    black_box(output.elapsed_us);
                    black_box(output.queue_wait_p99_us);
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
    targets = bench_store_blocking_executor
}
criterion_main!(benches);
