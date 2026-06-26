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
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_namesrv::bootstrap::Builder;
use rocketmq_namesrv::bootstrap::NameServerShutdownReport;

#[derive(Debug)]
struct NameSrvShutdownOutput {
    elapsed: Duration,
    report: NameServerShutdownReport,
}

fn run_namesrv_shutdown_drain(shutdown_after: Duration) -> NameSrvShutdownOutput {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .thread_name("rocketmq-namesrv-shutdown-bench")
        .enable_all()
        .build()
        .expect("namesrv benchmark runtime should start");

    runtime.block_on(async move {
        let artifact_dir = benchmark_artifact_dir();
        let bootstrap = Builder::new()
            .set_name_server_config(namesrv_config(&artifact_dir))
            .set_server_config(server_config())
            .build();

        let started_at = Instant::now();
        let report = bootstrap
            .boot_with_shutdown_report(async move {
                tokio::time::sleep(shutdown_after).await;
            })
            .await
            .expect("namesrv benchmark bootstrap should drain shutdown");
        let elapsed = started_at.elapsed();
        assert!(report.is_healthy(), "{report:?}");

        NameSrvShutdownOutput { elapsed, report }
    })
}

fn namesrv_config(artifact_dir: &Path) -> NamesrvConfig {
    NamesrvConfig {
        kv_config_path: artifact_dir
            .join("namesrv-bench-kv.json")
            .to_string_lossy()
            .into_owned(),
        config_store_path: artifact_dir
            .join("namesrv-bench.properties")
            .to_string_lossy()
            .into_owned(),
        scan_not_active_broker_interval: 10,
        enable_controller_in_namesrv: false,
        cluster_test: false,
        use_route_info_manager_v2: true,
        ..NamesrvConfig::default()
    }
}

fn server_config() -> ServerConfig {
    ServerConfig {
        listen_port: 0,
        bind_address: "127.0.0.1".to_string(),
        ..ServerConfig::default()
    }
}

fn benchmark_artifact_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-namesrv should live below workspace root")
        .join("target/runtime-baseline/prototype")
}

fn write_namesrv_shutdown_report_artifact() {
    let output = run_namesrv_shutdown_drain(Duration::from_millis(25));
    let output_dir = benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "namesrv_shutdown_drain",
        "generated_at_unix_ms": generated_at_unix_ms,
        "shutdown_after_ms": 25,
        "elapsed_ms": output.elapsed.as_millis(),
        "elapsed_us": output.elapsed.as_micros(),
        "healthy": output.report.is_healthy(),
        "in_flight_completed": output.report.in_flight.completed,
        "in_flight_remaining": output.report.in_flight.remaining,
        "in_flight_timed_out": output.report.in_flight.timed_out,
        "in_flight_timeout_ms": output.report.in_flight.timeout_ms,
        "in_flight_drain_elapsed_ms": output.report.in_flight.elapsed_ms,
        "remoting_client_present": output.report.remoting_client.is_some(),
        "remoting_client_healthy": output
            .report
            .remoting_client
            .as_ref()
            .is_some_and(|report| report.is_healthy()),
        "shutdown_report": output.report,
    });
    let path = output_dir.join("namesrv-shutdown-drain-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("namesrv shutdown benchmark artifact should serialize"),
    )
    .expect("namesrv shutdown benchmark artifact should be written");
}

fn bench_namesrv_shutdown_drain(criterion: &mut Criterion) {
    write_namesrv_shutdown_report_artifact();

    criterion.bench_function("namesrv_shutdown_drain/bootstrap_shutdown", |bencher| {
        bencher.iter(|| {
            let output = run_namesrv_shutdown_drain(black_box(Duration::from_millis(25)));
            black_box(output.elapsed);
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1));
    targets = bench_namesrv_shutdown_drain
}
criterion_main!(benches);
