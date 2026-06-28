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
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_store::bench_support::run_ha_bytes_vectored_benchmark_report;
use rocketmq_store::bench_support::HaTransferBenchmarkReport;

const BODY_SIZE: usize = 64 * 1024;

fn run_report(body_size: usize) -> HaTransferBenchmarkReport {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("HA transfer benchmark runtime should start");

    runtime.block_on(run_ha_bytes_vectored_benchmark_report(body_size))
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

fn write_ha_transfer_report_artifact() {
    let report = run_report(BODY_SIZE);
    assert!(report.frames_match, "{report:?}");
    assert!(report.ack_offsets_match, "{report:?}");
    assert_eq!(report.syscall_reduction_percent, 50, "{report:?}");

    let output_dir = benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("HA transfer benchmark artifact directory should be created");
    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "ha_transfer_bytes_vs_vectored",
        "generated_at_unix_ms": generated_at_unix_ms,
        "report": report,
    });
    let path = output_dir.join("ha-transfer-bytes-vectored-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("HA transfer benchmark artifact should serialize"),
    )
    .expect("HA transfer benchmark artifact should be written");
}

fn bench_ha_transfer(criterion: &mut Criterion) {
    write_ha_transfer_report_artifact();

    let mut group = criterion.benchmark_group("ha_transfer");
    group.throughput(Throughput::Bytes(BODY_SIZE as u64));
    group.bench_function("bytes_vs_vectored_report_64KiB", |bencher| {
        bencher.iter(|| {
            let report = run_report(black_box(BODY_SIZE));
            assert!(report.frames_match, "{report:?}");
            assert!(report.ack_offsets_match, "{report:?}");
            black_box(report.syscall_reduction_percent);
            black_box(report.bytes_baseline.ack_latency_nanos);
            black_box(report.vectored_optimized.ack_latency_nanos);
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
    targets = bench_ha_transfer
}
criterion_main!(benches);
