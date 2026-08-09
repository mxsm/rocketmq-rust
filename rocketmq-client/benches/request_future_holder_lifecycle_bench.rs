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

#![recursion_limit = "256"]

#[path = "support/mod.rs"]
mod support;

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
use rocketmq_client_rust::test_support::run_request_future_holder_lifecycle_probe;
use rocketmq_client_rust::test_support::run_request_future_holder_scan_probe;
use rocketmq_client_rust::test_support::RequestFutureHolderLifecycleProbe;

fn run_lifecycle_probe() -> RequestFutureHolderLifecycleProbe {
    let runtime = support::BenchClientRuntime::new("request-future-holder");
    let output = runtime.block_on(run_request_future_holder_lifecycle_probe(
        runtime.component("request-futures"),
    ));
    runtime.shutdown();
    output
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-client should live below workspace root")
        .to_path_buf()
}

fn benchmark_artifact_dir() -> PathBuf {
    workspace_root().join("target/runtime-baseline/prototype")
}

fn write_request_future_holder_report_artifact() {
    let output = run_lifecycle_probe();
    assert!(output.healthy, "{output:?}");
    let output_dir = benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "client_request_future_holder_lifecycle",
        "generated_at_unix_ms": generated_at_unix_ms,
        "probe": output,
    });
    let path = output_dir.join("client-request-future-holder-lifecycle-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("request future holder benchmark artifact should serialize"),
    )
    .expect("request future holder benchmark artifact should be written");
}

fn bench_request_future_holder_lifecycle(criterion: &mut Criterion) {
    write_request_future_holder_report_artifact();

    criterion.bench_function(
        "client_request_future_holder_lifecycle/fixed_delay_shutdown",
        |bencher| {
            bencher.iter(|| {
                let output = run_lifecycle_probe();
                assert!(output.healthy, "{output:?}");
                black_box(output.scheduled_runs);
                black_box(output.scheduled_skips);
                black_box(output.scheduled_overlaps);
                black_box(output.shutdown_elapsed_us);
            });
        },
    );

    let mut group = criterion.benchmark_group("client_request_future_holder_lifecycle/deadline_scan");
    group.sample_size(10);
    let scan_runtime = support::BenchClientRuntime::new("request-future-scan");

    for pending_requests in [1_000usize, 10_000, 100_000] {
        for expired_percent in [1usize, 10, 100] {
            group.bench_with_input(
                BenchmarkId::new(format!("expired_{expired_percent}pct"), pending_requests),
                &(pending_requests, expired_percent),
                |bencher, &(pending_requests, expired_percent)| {
                    bencher.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let output = scan_runtime.block_on(run_request_future_holder_scan_probe(
                                scan_runtime.component("scan"),
                                pending_requests,
                                expired_percent,
                            ));
                            assert_eq!(output.pending_requests, pending_requests);
                            assert_eq!(output.callbacks, output.expired_requests);
                            assert_eq!(output.remaining_requests, pending_requests - output.expired_requests);
                            black_box(output.scan_elapsed_us);
                            let scan_elapsed_us = output.scan_elapsed_us.min(u128::from(u64::MAX)) as u64;
                            total += Duration::from_micros(scan_elapsed_us);
                        }
                        total
                    });
                },
            );
        }
    }
    group.finish();
    scan_runtime.shutdown();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1));
    targets = bench_request_future_holder_lifecycle
}
criterion_main!(benches);
