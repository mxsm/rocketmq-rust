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
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_common::common::future::CompletableFuture;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use serde::Serialize;

#[derive(Debug, Serialize)]
struct CompletableFutureProbe {
    future_count: usize,
    outside_runtime_completed: usize,
    inside_runtime_completed: usize,
    outside_runtime_elapsed_us: u128,
    inside_runtime_elapsed_us: u128,
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

fn run_outside_runtime_probe(future_count: usize) -> (usize, u128) {
    let started_at = Instant::now();
    let mut completed = 0;
    for value in 0..future_count {
        let future = CompletableFuture::new();
        let sender = future.get_sender();
        sender
            .blocking_send(value)
            .expect("completable future should accept blocking send without runtime");
        let result = futures::executor::block_on(future);
        if result == Some(value) {
            completed += 1;
        }
    }
    (completed, started_at.elapsed().as_micros())
}

fn run_inside_runtime_probe(future_count: usize) -> (usize, u128) {
    let owner = RuntimeOwner::new(RuntimeConfig {
        worker_threads: 2,
        max_blocking_threads: 2,
        thread_name: "rocketmq-common-completable-future-bench".to_string(),
        shutdown_timeout: Duration::from_secs(1),
        ..RuntimeConfig::default()
    })
    .expect("completable future benchmark runtime should start");

    let started_at = Instant::now();
    let completed = owner.block_on(async move {
        let mut completed = 0;
        for value in 0..future_count {
            let future = CompletableFuture::new();
            let sender = future.get_sender();
            sender
                .send(value)
                .await
                .expect("completable future should accept async send");
            let result = future.await;
            if result == Some(value) {
                completed += 1;
            }
        }
        completed
    });
    let elapsed = started_at.elapsed().as_micros();
    owner
        .shutdown_runtime_blocking()
        .expect("benchmark runtime should shut down");
    (completed, elapsed)
}

fn run_completable_future_probe(future_count: usize) -> CompletableFutureProbe {
    let (outside_runtime_completed, outside_runtime_elapsed_us) = run_outside_runtime_probe(future_count);
    let (inside_runtime_completed, inside_runtime_elapsed_us) = run_inside_runtime_probe(future_count);
    CompletableFutureProbe {
        future_count,
        outside_runtime_completed,
        inside_runtime_completed,
        outside_runtime_elapsed_us,
        inside_runtime_elapsed_us,
        healthy: outside_runtime_completed == future_count && inside_runtime_completed == future_count,
    }
}

fn write_completable_future_report_artifact() {
    let output = run_completable_future_probe(512);
    assert!(output.healthy, "{output:?}");
    let output_dir = benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "common_completable_future",
        "generated_at_unix_ms": generated_at_unix_ms,
        "probe": output,
    });
    let path = output_dir.join("common-completable-future-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("completable future benchmark artifact should serialize"),
    )
    .expect("completable future benchmark artifact should be written");
}

fn bench_common_completable_future(criterion: &mut Criterion) {
    write_completable_future_report_artifact();

    let mut group = criterion.benchmark_group("common_completable_future");
    for future_count in [128usize, 512] {
        group.bench_with_input(
            BenchmarkId::new("outside_runtime_blocking_send_poll", future_count),
            &future_count,
            |bencher, future_count| {
                bencher.iter(|| {
                    let (completed, elapsed_us) = run_outside_runtime_probe(black_box(*future_count));
                    assert_eq!(completed, *future_count);
                    black_box(elapsed_us);
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("inside_runtime_async_send_poll", future_count),
            &future_count,
            |bencher, future_count| {
                bencher.iter(|| {
                    let (completed, elapsed_us) = run_inside_runtime_probe(black_box(*future_count));
                    assert_eq!(completed, *future_count);
                    black_box(elapsed_us);
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
    targets = bench_common_completable_future
}
criterion_main!(benches);
