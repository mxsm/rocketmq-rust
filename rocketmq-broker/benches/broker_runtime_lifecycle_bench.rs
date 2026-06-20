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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_broker::bench_support::run_broker_runtime_lifecycle_probe;
use rocketmq_broker::bench_support::BrokerRuntimeLifecycleProbe;

static NEXT_BENCH_ROOT_ID: AtomicU64 = AtomicU64::new(0);

fn run_lifecycle_probe(pending_task_count: usize) -> BrokerRuntimeLifecycleProbe {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .thread_name("rocketmq-broker-lifecycle-bench")
        .enable_all()
        .build()
        .expect("broker lifecycle benchmark runtime should start");
    let root = unique_benchmark_root();
    runtime.block_on(run_broker_runtime_lifecycle_probe(root, pending_task_count))
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-broker should live below workspace root")
        .to_path_buf()
}

fn benchmark_artifact_dir() -> PathBuf {
    workspace_root().join("target/runtime-baseline/prototype")
}

fn unique_benchmark_root() -> PathBuf {
    let id = NEXT_BENCH_ROOT_ID.fetch_add(1, Ordering::Relaxed);
    benchmark_artifact_dir().join(format!("broker-runtime-lifecycle-root-{}-{id}", std::process::id()))
}

fn write_broker_lifecycle_report_artifact() {
    let output = run_lifecycle_probe(8);
    assert!(output.healthy, "{output:?}");
    let output_dir = benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "broker_runtime_lifecycle",
        "generated_at_unix_ms": generated_at_unix_ms,
        "probe": output,
    });
    let path = output_dir.join("broker-runtime-lifecycle-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("broker lifecycle benchmark artifact should serialize"),
    )
    .expect("broker lifecycle benchmark artifact should be written");
}

fn bench_broker_runtime_lifecycle(criterion: &mut Criterion) {
    write_broker_lifecycle_report_artifact();

    let mut group = criterion.benchmark_group("broker_runtime_lifecycle");
    for pending_task_count in [1usize, 8] {
        group.bench_with_input(
            BenchmarkId::new("scheduled_shutdown_and_basic_service", pending_task_count),
            &pending_task_count,
            |bencher, pending_task_count| {
                bencher.iter(|| {
                    let output = run_lifecycle_probe(black_box(*pending_task_count));
                    assert!(output.healthy, "{output:?}");
                    black_box(output.scheduled_shutdown_elapsed_us);
                    black_box(output.basic_shutdown_elapsed_us);
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
    targets = bench_broker_runtime_lifecycle
}
criterion_main!(benches);
