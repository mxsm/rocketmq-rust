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

//! Retained-metadata load: coalescing on one hot resource, throughput across
//! distinct resources, and a hot resource that competes with a cold one.
//!
//! The competition scenario is a correctness claim as much as a measurement: a
//! continuously resubmitted hot resource must not keep a cold resource from
//! reaching durability, which the runtime pins with its per-resource ordering
//! contract. The scenario asserts that before it returns a duration, so a change
//! that starves the cold resource fails instead of producing a faster number.

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
use rocketmq_runtime::MetadataDeadline;
use rocketmq_runtime::MetadataIoConfig;
use rocketmq_runtime::MetadataIoSnapshot;
use rocketmq_runtime::MetadataWriteRequest;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use tempfile::TempDir;

#[derive(Debug)]
struct MetadataOutput {
    resources: usize,
    submitted: usize,
    elapsed: Duration,
    durable: usize,
    snapshot: MetadataIoSnapshot,
    retained_target_files: usize,
}

fn runtime_config() -> RuntimeConfig {
    RuntimeConfig {
        worker_threads: 2,
        max_blocking_threads: 4,
        shutdown_timeout: Duration::from_secs(5),
        thread_name: "rocketmq-metadata-bench".to_string(),
        ..RuntimeConfig::default()
    }
}

fn metadata_config(max_pending_bytes: usize) -> MetadataIoConfig {
    MetadataIoConfig {
        max_pending_bytes,
        ..MetadataIoConfig::default()
    }
}

/// Submits `generation_count` generations for one resource and waits for the
/// last admitted generation, which is the coalescing case.
fn run_hot_resource(root: &TempDir, resource: &str, generation_count: usize) -> MetadataOutput {
    let owner = RuntimeOwner::plan(runtime_config())
        .expect("test runtime configuration is valid")
        .build()
        .expect("runtime owner should start");
    let component = owner.root_context().component("bench.metadata.hot");
    let actor = metadata_config(64 * 1024 * 1024)
        .into_plan()
        .expect("the benchmark metadata configuration is valid")
        .start(&component)
        .expect("the metadata actor should start");
    let deadline = MetadataDeadline::after(Duration::from_secs(30));
    let target = root.path().join(format!("{resource}.json"));

    let (elapsed, durable) = owner.block_on(async {
        let started_at = Instant::now();
        let mut durable = 0usize;
        for generation in 1..=generation_count {
            let request = MetadataWriteRequest::new(
                resource,
                generation as u64,
                target.clone(),
                format!("snapshot-{generation}").into_bytes(),
            );
            let observation = actor
                .submit_observed(request, deadline)
                .await
                .expect("the actor admits the benchmark generation");
            let settled_cleanly = !observation.requires_reconciliation() && observation.settled().is_some();
            durable += usize::from(settled_cleanly);
        }
        (started_at.elapsed(), durable)
    });

    let snapshot = actor.snapshot();
    assert_eq!(snapshot.pending_operations, 0, "{snapshot:?}");
    let drain = owner.block_on(actor.shutdown_until(deadline));
    assert!(!drain.timed_out, "{drain:?}");
    let report = owner.shutdown_runtime_blocking().expect("the owner should shut down");
    assert!(report.is_healthy(), "{}", report.to_json());
    MetadataOutput {
        resources: 1,
        submitted: generation_count,
        elapsed,
        durable,
        snapshot,
        retained_target_files: fs::read_dir(root.path()).expect("benchmark root is readable").count(),
    }
}

/// Submits one generation per resource, so every write owns its target and the
/// actor cannot coalesce any of them.
fn run_distinct_resources(root: &TempDir, resource_count: usize) -> MetadataOutput {
    let owner = RuntimeOwner::plan(runtime_config())
        .expect("test runtime configuration is valid")
        .build()
        .expect("runtime owner should start");
    let component = owner.root_context().component("bench.metadata.distinct");
    let actor = metadata_config(256 * 1024 * 1024)
        .into_plan()
        .expect("the benchmark metadata configuration is valid")
        .start(&component)
        .expect("the metadata actor should start");
    let deadline = MetadataDeadline::after(Duration::from_secs(30));

    let (elapsed, durable) = owner.block_on(async {
        let started_at = Instant::now();
        let mut durable = 0usize;
        for resource_index in 0..resource_count {
            let resource = format!("resource-{resource_index}");
            let request = MetadataWriteRequest::new(
                resource.as_str(),
                1_u64,
                root.path().join(format!("{resource}.json")),
                format!("snapshot-{resource_index}").into_bytes(),
            );
            let observation = actor
                .submit_observed(request, deadline)
                .await
                .expect("the actor admits the benchmark generation");
            let settled_cleanly = !observation.requires_reconciliation() && observation.settled().is_some();
            durable += usize::from(settled_cleanly);
        }
        (started_at.elapsed(), durable)
    });

    let snapshot = actor.snapshot();
    assert_eq!(snapshot.pending_operations, 0, "{snapshot:?}");
    let drain = owner.block_on(actor.shutdown_until(deadline));
    assert!(!drain.timed_out, "{drain:?}");
    let report = owner.shutdown_runtime_blocking().expect("the owner should shut down");
    assert!(report.is_healthy(), "{}", report.to_json());
    MetadataOutput {
        resources: resource_count,
        submitted: resource_count,
        elapsed,
        durable,
        snapshot,
        retained_target_files: fs::read_dir(root.path()).expect("benchmark root is readable").count(),
    }
}

/// Submits one cold resource and then a continuous hot stream for a different
/// resource, and asserts that both reach durability.
fn run_hot_and_cold_competition(root: &TempDir, hot_generations: usize) -> Duration {
    let owner = RuntimeOwner::plan(runtime_config())
        .expect("test runtime configuration is valid")
        .build()
        .expect("runtime owner should start");
    let component = owner.root_context().component("bench.metadata.competition");
    let actor = metadata_config(64 * 1024 * 1024)
        .into_plan()
        .expect("the benchmark metadata configuration is valid")
        .start(&component)
        .expect("the metadata actor should start");
    let deadline = MetadataDeadline::after(Duration::from_secs(30));

    let (elapsed, hot_durable) = owner.block_on(async {
        // The cold resource is admitted first. Every following generation is a
        // different resource, so the hot stream must not hold it back.
        let cold = actor
            .submit_observed(
                MetadataWriteRequest::new("cold", 1_u64, root.path().join("cold.json"), b"cold-snapshot".to_vec()),
                deadline,
            )
            .await
            .expect("the actor admits the cold generation");
        assert!(
            !cold.requires_reconciliation() && cold.settled().is_some(),
            "the cold resource must reach durability"
        );

        let started_at = Instant::now();
        let mut hot_durable = 0usize;
        for generation in 1..=hot_generations {
            let observation = actor
                .submit_observed(
                    MetadataWriteRequest::new(
                        "hot",
                        generation as u64,
                        root.path().join("hot.json"),
                        format!("hot-snapshot-{generation}").into_bytes(),
                    ),
                    deadline,
                )
                .await
                .expect("the actor admits the hot generation");
            let settled_cleanly = !observation.requires_reconciliation() && observation.settled().is_some();
            hot_durable += usize::from(settled_cleanly);
        }
        (started_at.elapsed(), hot_durable)
    });

    assert_eq!(hot_durable, hot_generations, "every hot generation must settle");
    let snapshot = actor.snapshot();
    assert_eq!(snapshot.pending_operations, 0, "{snapshot:?}");
    let drain = owner.block_on(actor.shutdown_until(deadline));
    assert!(!drain.timed_out, "{drain:?}");
    let report = owner.shutdown_runtime_blocking().expect("the owner should shut down");
    assert!(report.is_healthy(), "{}", report.to_json());
    elapsed
}

fn write_metadata_report_artifact() {
    let root = TempDir::new().expect("benchmark directory should be created");
    let hot = run_hot_resource(&root, "hot", 64);
    let distinct = run_distinct_resources(&root, 32);
    let competition = run_hot_and_cold_competition(&root, 64);

    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-runtime should live below workspace root")
        .to_path_buf();
    let output_dir = workspace_root.join("target/runtime-baseline/prototype");
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "metadata_io",
        "generated_at_unix_ms": generated_at_unix_ms,
        "hot_resource": {
            "submitted": hot.submitted,
            "elapsed_ms": hot.elapsed.as_millis(),
            "durable_observed": hot.durable,
            "retained_target_files": hot.retained_target_files,
            "pending_bytes": hot.snapshot.pending_bytes,
            "pending_operations": hot.snapshot.pending_operations,
        },
        "distinct_resources": {
            "resources": distinct.resources,
            "elapsed_ms": distinct.elapsed.as_millis(),
            "durable_observed": distinct.durable,
            "retained_target_files": distinct.retained_target_files,
            "pending_bytes": distinct.snapshot.pending_bytes,
            "pending_operations": distinct.snapshot.pending_operations,
        },
        "hot_and_cold_competition_ms": competition.as_millis(),
    });
    let path = output_dir.join("metadata-io-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("metadata benchmark artifact should serialize"),
    )
    .expect("metadata benchmark artifact should be written");
}

fn bench_metadata_io(criterion: &mut Criterion) {
    write_metadata_report_artifact();

    let mut group = criterion.benchmark_group("metadata_io");
    for generation_count in [16usize, 64] {
        let root = TempDir::new().expect("benchmark directory should be created");
        group.bench_with_input(
            BenchmarkId::new("hot_resource_coalesced", generation_count),
            &generation_count,
            |bencher, generation_count| {
                bencher.iter(|| {
                    let output = run_hot_resource(&root, "hot", black_box(*generation_count));
                    black_box(output.elapsed);
                });
            },
        );
    }
    for resource_count in [8usize, 32] {
        let root = TempDir::new().expect("benchmark directory should be created");
        group.bench_with_input(
            BenchmarkId::new("distinct_resources", resource_count),
            &resource_count,
            |bencher, resource_count| {
                bencher.iter(|| {
                    let output = run_distinct_resources(&root, black_box(*resource_count));
                    black_box(output.elapsed);
                });
            },
        );
    }
    let root = TempDir::new().expect("benchmark directory should be created");
    group.bench_with_input(
        BenchmarkId::new("hot_and_cold_competition", 64usize),
        &64usize,
        |bencher, hot_generations| {
            bencher.iter(|| {
                black_box(run_hot_and_cold_competition(&root, black_box(*hot_generations)));
            });
        },
    );
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1));
    targets = bench_metadata_io
}
criterion_main!(benches);
