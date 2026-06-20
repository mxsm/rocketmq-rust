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
use rocketmq_proxy::bench_support::run_proxy_housekeeping_lifecycle_probe;
use rocketmq_proxy::bench_support::ProxyHousekeepingLifecycleProbe;

fn run_housekeeping_probe(shutdown_delay: Duration) -> ProxyHousekeepingLifecycleProbe {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .thread_name("rocketmq-proxy-housekeeping-bench")
        .enable_all()
        .build()
        .expect("proxy housekeeping benchmark runtime should start");

    runtime.block_on(run_proxy_housekeeping_lifecycle_probe(shutdown_delay))
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-proxy should live below workspace root")
        .to_path_buf()
}

fn benchmark_artifact_dir() -> PathBuf {
    workspace_root().join("target/runtime-baseline/prototype")
}

fn write_proxy_housekeeping_report_artifact() {
    let output = run_housekeeping_probe(Duration::from_millis(5));
    assert!(output.healthy, "{output:?}");
    let output_dir = benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "proxy_housekeeping_lifecycle",
        "generated_at_unix_ms": generated_at_unix_ms,
        "probe": output,
    });
    let path = output_dir.join("proxy-housekeeping-lifecycle-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("proxy housekeeping benchmark artifact should serialize"),
    )
    .expect("proxy housekeeping benchmark artifact should be written");
}

fn bench_proxy_housekeeping_lifecycle(criterion: &mut Criterion) {
    write_proxy_housekeeping_report_artifact();

    let mut group = criterion.benchmark_group("proxy_housekeeping_lifecycle");
    for shutdown_delay_ms in [5u64, 20] {
        group.bench_with_input(
            BenchmarkId::new("shutdown_drain", shutdown_delay_ms),
            &shutdown_delay_ms,
            |bencher, shutdown_delay_ms| {
                bencher.iter(|| {
                    let output = run_housekeeping_probe(Duration::from_millis(black_box(*shutdown_delay_ms)));
                    assert!(output.healthy, "{output:?}");
                    black_box(output.shutdown_elapsed_us);
                    black_box(output.scheduled_runs);
                    black_box(output.scheduled_skips);
                    black_box(output.scheduled_overlaps);
                    black_box(output.task_count_after_shutdown);
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
    targets = bench_proxy_housekeeping_lifecycle
}
criterion_main!(benches);
