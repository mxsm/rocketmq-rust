// Copyright 2026 The RocketMQ Rust Authors
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
use std::process::Command;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use anyhow::Result;
use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use serde_json::json;
use sha2::Digest;
use sha2::Sha256;

#[path = "support/namesrv_harness.rs"]
mod namesrv_harness;

use namesrv_harness::load_workload_manifest;
use namesrv_harness::network::OwnedNameServer;
use namesrv_harness::network::RouteBenchHarness;
use namesrv_harness::network::RouteTraceMetrics;
use namesrv_harness::validate_workload_manifest;
use namesrv_harness::RouteWorkloadProfile;

fn bench_namesrv_route_e2e(criterion: &mut Criterion) {
    if let Err(error) = run_benchmark(criterion) {
        panic!("NameServer route E2E benchmark failed: {error:#}");
    }
}

fn run_benchmark(criterion: &mut Criterion) -> Result<()> {
    let manifest_path = std::env::var_os("NAMESRV_BENCH_MANIFEST")
        .map(PathBuf::from)
        .unwrap_or_else(default_manifest_path);
    let manifest = load_workload_manifest(&manifest_path)?;
    validate_workload_manifest(&manifest)?;
    let profile_name = std::env::var("NAMESRV_BENCH_WORKLOAD").unwrap_or_else(|_| "smoke".to_string());
    let profile = manifest
        .profile(&profile_name)
        .with_context(|| format!("route workload profile '{profile_name}' does not exist"))?
        .clone();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(profile.connections.clamp(2, 16))
        .max_blocking_threads(4)
        .thread_name("namesrv-route-e2e-bench")
        .enable_all()
        .build()
        .context("build NameServer route benchmark runtime")?;

    let external_endpoint = std::env::var("NAMESRV_BENCH_ENDPOINT").ok();
    let mut owned_server = if external_endpoint.is_none() {
        Some(runtime.block_on(OwnedNameServer::start())?)
    } else {
        None
    };
    let endpoint = external_endpoint.map(CheetahString::from_string).unwrap_or_else(|| {
        owned_server
            .as_ref()
            .expect("owned server should exist")
            .endpoint
            .clone()
    });
    let mut harness = runtime.block_on(RouteBenchHarness::connect(endpoint.clone(), profile.connections))?;
    runtime.block_on(harness.prepare_topology(&profile))?;

    let warmup = RouteWorkloadProfile {
        name: format!("{}-warmup", profile.name),
        operations: profile.operations.clamp(100, 1_000),
        ..profile.clone()
    };
    let warmup_metrics = runtime.block_on(harness.run_trace(&warmup, manifest.seed ^ 0xa5a5_a5a5))?;
    if warmup_metrics.errors != 0 {
        anyhow::bail!("route benchmark warmup returned {} errors", warmup_metrics.errors);
    }
    let metrics = runtime.block_on(harness.run_trace(&profile, manifest.seed))?;
    if metrics.errors != 0 {
        anyhow::bail!("route benchmark trace returned {} errors", metrics.errors);
    }
    write_artifacts(&manifest_path, &profile, manifest.seed, &endpoint, &metrics)?;

    let request_index = AtomicUsize::new(0);
    criterion.bench_function(&format!("namesrv_route_e2e/{profile_name}"), |bencher| {
        bencher.iter(|| {
            let topic_index = request_index.fetch_add(1, Ordering::Relaxed);
            runtime.block_on(async {
                let response_bytes = harness
                    .request_topic(topic_index)
                    .await
                    .expect("criterion TCP route request should succeed");
                black_box(response_bytes);
            });
        });
    });

    runtime.block_on(harness.shutdown())?;
    if let Some(server) = owned_server.take() {
        runtime.block_on(server.shutdown())?;
    }
    Ok(())
}

fn write_artifacts(
    manifest_path: &Path,
    profile: &RouteWorkloadProfile,
    seed: u64,
    endpoint: &CheetahString,
    metrics: &RouteTraceMetrics,
) -> Result<()> {
    let server = std::env::var("NAMESRV_BENCH_SERVER").unwrap_or_else(|_| "rust-in-process".to_string());
    let label = std::env::var("NAMESRV_BENCH_PROFILE").unwrap_or_else(|_| "local".to_string());
    let output_dir = std::env::var_os("NAMESRV_BENCH_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| default_output_dir(&server, &label, &profile.name));
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("create route benchmark output directory {}", output_dir.display()))?;
    let manifest_bytes = fs::read(manifest_path)
        .with_context(|| format!("read route benchmark manifest {}", manifest_path.display()))?;
    let fixture_sha256 = Sha256::digest(&manifest_bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let commit = git_output(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis();
    let artifact = json!({
        "schemaVersion": 1,
        "generatedAtUnixMillis": generated_at_unix_ms,
        "server": server,
        "profile": label,
        "workload": profile,
        "seed": seed,
        "fixtureSha256": fixture_sha256,
        "rustCommit": commit,
        "javaVersion": std::env::var("NAMESRV_BENCH_JAVA_VERSION").ok(),
        "endpoint": endpoint,
        "releaseProfile": true,
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "logicalCpus": std::thread::available_parallelism().map(usize::from).ok(),
        "allocationBytesPerOperation": null,
        "metrics": metrics,
    });
    fs::write(
        output_dir.join("route-benchmark.json"),
        serde_json::to_vec_pretty(&artifact).context("serialize route benchmark JSON artifact")?,
    )
    .context("write route benchmark JSON artifact")?;
    let csv = format!(
        "server,profile,workload,operations,route_reads,registration_writes,errors,duration_ms,qps,p50_us,p95_us,p99_us,p999_us,response_bytes,allocation_bytes_per_op\n{},{},{},{},{},{},{},{},{:.3},{},{},{},{},{},N/A\n",
        server,
        label,
        profile.name,
        metrics.operations,
        metrics.route_reads,
        metrics.registration_writes,
        metrics.errors,
        metrics.duration_millis,
        metrics.qps,
        metrics.p50_micros,
        metrics.p95_micros,
        metrics.p99_micros,
        metrics.p999_micros,
        metrics.response_bytes,
    );
    fs::write(output_dir.join("route-benchmark.csv"), csv).context("write route benchmark CSV artifact")?;
    Ok(())
}

fn default_manifest_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("benches/fixtures/route_workloads.json")
}

fn default_output_dir(server: &str, profile: &str, workload: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-namesrv is below the workspace root")
        .join(format!("target/namesrv-bench/{server}-{profile}-{workload}"))
}

fn git_output(arguments: &[&str]) -> Option<String> {
    let output = Command::new("git").args(arguments).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(2));
    targets = bench_namesrv_route_e2e
}
criterion_main!(benches);
