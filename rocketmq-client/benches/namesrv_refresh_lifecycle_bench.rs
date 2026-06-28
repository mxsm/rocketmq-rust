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
use rocketmq_client_rust::run_namesrv_refresh_lifecycle_probe;
use rocketmq_client_rust::run_route_refresh_shard_probe;
use rocketmq_client_rust::NamesrvRefreshLifecycleProbe;
use rocketmq_client_rust::RouteRefreshShardProbe;

fn run_lifecycle_probe() -> NamesrvRefreshLifecycleProbe {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .thread_name("rocketmq-client-namesrv-refresh-bench")
        .enable_all()
        .build()
        .expect("namesrv refresh benchmark runtime should start");

    runtime.block_on(run_namesrv_refresh_lifecycle_probe())
}

fn run_route_refresh_probe(topic_count: usize) -> RouteRefreshShardProbe {
    run_route_refresh_shard_probe(topic_count)
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

fn write_namesrv_refresh_report_artifact() {
    let output = run_lifecycle_probe();
    assert!(output.healthy, "{output:?}");
    let route_refresh_profiles = [10, 100, 1_000]
        .into_iter()
        .map(run_route_refresh_probe)
        .collect::<Vec<_>>();
    let output_dir = benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "client_namesrv_refresh_lifecycle",
        "generated_at_unix_ms": generated_at_unix_ms,
        "probe": output,
        "route_refresh_profiles": route_refresh_profiles,
    });
    let path = output_dir.join("client-namesrv-refresh-lifecycle-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("namesrv refresh artifact should serialize"),
    )
    .expect("namesrv refresh artifact should be written");
}

fn bench_namesrv_refresh_lifecycle(criterion: &mut Criterion) {
    write_namesrv_refresh_report_artifact();

    criterion.bench_function("client_namesrv_refresh_lifecycle/task_group_shutdown", |bencher| {
        bencher.iter(|| {
            let output = run_lifecycle_probe();
            assert!(output.healthy, "{output:?}");
            black_box(output.task_count_before_shutdown);
            black_box(output.task_count_after_shutdown);
            black_box(output.shutdown_elapsed_us);
        });
    });

    for topic_count in [10, 100, 1_000] {
        let bench_name = format!("client_route_refresh_shard/{topic_count}_topics");
        criterion.bench_function(&bench_name, |bencher| {
            bencher.iter(|| {
                let output = run_route_refresh_probe(topic_count);
                assert_eq!(output.topic_count, topic_count);
                assert!(output.selected_topics <= output.batch_size);
                black_box(output.selected_topics);
                black_box(output.skipped_topics);
                black_box(output.peak_topic_reduction_percent);
            });
        });
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1));
    targets = bench_namesrv_refresh_lifecycle
}
criterion_main!(benches);
