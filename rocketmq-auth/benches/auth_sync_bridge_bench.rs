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
use rocketmq_auth::bench_support::run_auth_sync_bridge_current_thread_probe;
use rocketmq_auth::bench_support::run_auth_sync_bridge_multi_thread_probe;
use rocketmq_auth::bench_support::run_auth_sync_bridge_no_runtime_probe;
use rocketmq_auth::bench_support::AuthSyncBridgeProbe;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-auth should live below workspace root")
        .to_path_buf()
}

fn benchmark_artifact_dir() -> PathBuf {
    workspace_root().join("target/runtime-baseline/prototype")
}

fn write_auth_sync_bridge_report_artifact() {
    let no_runtime = run_auth_sync_bridge_no_runtime_probe(64);
    let current_thread = run_auth_sync_bridge_current_thread_probe(32);
    let multi_thread = run_auth_sync_bridge_multi_thread_probe(64);

    assert!(no_runtime.healthy, "{no_runtime:?}");
    assert!(current_thread.healthy, "{current_thread:?}");
    assert!(multi_thread.healthy, "{multi_thread:?}");

    let output_dir = benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "auth_sync_bridge",
        "generated_at_unix_ms": generated_at_unix_ms,
        "no_runtime": no_runtime,
        "current_thread": current_thread,
        "multi_thread": multi_thread,
    });
    let path = output_dir.join("auth-sync-bridge-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("auth sync bridge benchmark artifact should serialize"),
    )
    .expect("auth sync bridge benchmark artifact should be written");
}

fn bench_auth_sync_bridge(criterion: &mut Criterion) {
    write_auth_sync_bridge_report_artifact();

    let mut group = criterion.benchmark_group("auth_sync_bridge");
    for call_count in [16usize, 64] {
        group.bench_with_input(
            BenchmarkId::new("no_runtime_shared_fallback", call_count),
            &call_count,
            |bencher, call_count| {
                bencher.iter(|| {
                    let output: AuthSyncBridgeProbe = run_auth_sync_bridge_no_runtime_probe(black_box(*call_count));
                    assert!(output.healthy, "{output:?}");
                    black_box(output.elapsed_us);
                    black_box(output.delta.shared_runtime_created);
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("multi_thread_block_in_place", call_count),
            &call_count,
            |bencher, call_count| {
                bencher.iter(|| {
                    let output = run_auth_sync_bridge_multi_thread_probe(black_box(*call_count));
                    assert!(output.healthy, "{output:?}");
                    black_box(output.elapsed_us);
                    black_box(output.delta.multi_thread_block_in_place);
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
    targets = bench_auth_sync_bridge
}
criterion_main!(benches);
