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

//! Sampling cost of the sanitized diagnostics views under task and group load.
//!
//! The bounded aggregates are what a routine sampler reads, so the measurement
//! separates an aggregate-only sample from an explicitly requested detail list
//! and from the V1 view, which has no detail path at all. Every scenario also
//! asserts the declared budgets, so a sample that silently scanned the whole
//! tree instead of the requested share would fail rather than look fast.

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
use rocketmq_runtime::RuntimeComponent;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeDiagnosticsInputs;
use rocketmq_runtime::RuntimeDiagnosticsScope;
use rocketmq_runtime::RuntimeDiagnosticsViewOptionsV2;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskKind;

#[derive(Debug)]
struct DiagnosticsOutput {
    sampled_tasks: usize,
    sampled_groups: usize,
    aggregate_elapsed: Duration,
    detail_elapsed: Duration,
    versioned_elapsed: Duration,
    detail_entries: usize,
    details_scanned: usize,
    scope: RuntimeDiagnosticsScope,
    truncated: bool,
    report: ShutdownReport,
}

fn runtime_config() -> RuntimeConfig {
    RuntimeConfig {
        worker_threads: 2,
        max_blocking_threads: 3,
        shutdown_timeout: Duration::from_secs(5),
        thread_name: "rocketmq-diagnostics-bench".to_string(),
        ..RuntimeConfig::default()
    }
}

fn run_diagnostics_sample(task_count: usize, group_count: usize, detail_entries: usize) -> DiagnosticsOutput {
    let owner = RuntimeOwner::plan(runtime_config())
        .expect("test runtime configuration is valid")
        .build()
        .expect("runtime owner should start");
    let root = owner.root_context().component("bench.diagnostics-root");

    owner.block_on(async move {
        // Tasks wait on the component token so the scenario shuts down cleanly
        // and each iteration starts from the same population.
        for group_index in 0..group_count {
            let component = root.component(
                rocketmq_runtime::ScopeId::try_new(format!("bench.diagnostics.{group_index}"))
                    .expect("the benchmark scope has a fixed nonblank prefix"),
            );
            let cancellation = component.task_group().cancellation_token();
            for task_index in 0..task_count / group_count {
                component
                    .spawn(
                        format!("diagnostics-task-{group_index}-{task_index}"),
                        TaskKind::Worker,
                        {
                            let cancellation = cancellation.clone();
                            async move { cancellation.cancelled().await }
                        },
                    )
                    .expect("benchmark task should spawn");
            }
        }
        // A group counts only its own registry, so the wait uses the subtree
        // count the view reports: that is the population this scenario samples,
        // which makes the wait prove the sampled population is complete.
        while root
            .diagnostics_view_v2(RuntimeComponent::Broker, RuntimeDiagnosticsInputs::default())
            .tasks
            .task_count
            < task_count
        {
            tokio::task::yield_now().await;
        }

        let sampled_at = Instant::now();
        let aggregate = root.diagnostics_view_v2(RuntimeComponent::Broker, RuntimeDiagnosticsInputs::default());
        let aggregate_elapsed = sampled_at.elapsed();
        assert_eq!(aggregate.tasks.task_count, task_count);
        assert_eq!(aggregate.tasks.scope, RuntimeDiagnosticsScope::Subtree);
        assert_eq!(
            aggregate.details.len(),
            0,
            "an aggregate sample must not collect details"
        );

        let detailed_at = Instant::now();
        let detailed = root.diagnostics_view_v2_with_options(
            RuntimeComponent::Broker,
            RuntimeDiagnosticsInputs::default(),
            RuntimeDiagnosticsViewOptionsV2 {
                max_detail_entries: detail_entries,
                detail_scan_budget: task_count,
                ..RuntimeDiagnosticsViewOptionsV2::default()
            },
        );
        let detail_elapsed = detailed_at.elapsed();
        assert_eq!(detailed.details.len(), detail_entries);
        assert_eq!(detailed.details_scanned, task_count);
        assert_eq!(detailed.detail_scan_budget, task_count);

        let versioned_at = Instant::now();
        let versioned = root.diagnostics_view_v1(RuntimeComponent::Broker);
        let versioned_elapsed = versioned_at.elapsed();
        assert_eq!(versioned.task_count, task_count);

        let report = root.task_group().shutdown(Duration::from_secs(5)).await;

        DiagnosticsOutput {
            sampled_tasks: aggregate.tasks.task_count,
            sampled_groups: aggregate.tasks.task_group_count,
            aggregate_elapsed,
            detail_elapsed,
            versioned_elapsed,
            detail_entries: detailed.details.len(),
            details_scanned: detailed.details_scanned,
            scope: aggregate.tasks.scope,
            truncated: aggregate.truncated,
            report,
        }
    })
}

fn write_diagnostics_report_artifact() {
    let output = run_diagnostics_sample(512, 8, 64);
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
        "case": "runtime_diagnostics",
        "generated_at_unix_ms": generated_at_unix_ms,
        "aggregate_sample_ms": output.aggregate_elapsed.as_secs_f64() * 1000.0,
        "detail_sample_ms": output.detail_elapsed.as_secs_f64() * 1000.0,
        "versioned_sample_ms": output.versioned_elapsed.as_secs_f64() * 1000.0,
        "sampled_tasks": output.sampled_tasks,
        "sampled_groups": output.sampled_groups,
        "detail_entries": output.detail_entries,
        "details_scanned": output.details_scanned,
        "scope": format!("{:?}", output.scope),
        "truncated": output.truncated,
        "healthy": output.report.is_healthy(),
        "shutdown_report": output.report,
    });
    let path = output_dir.join("runtime-diagnostics-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("diagnostics benchmark artifact should serialize"),
    )
    .expect("diagnostics benchmark artifact should be written");
}

fn bench_runtime_diagnostics(criterion: &mut Criterion) {
    write_diagnostics_report_artifact();

    let mut group = criterion.benchmark_group("runtime_diagnostics");
    for (task_count, group_count) in [(128usize, 4usize), (1024, 8)] {
        group.bench_with_input(
            BenchmarkId::new("aggregate_scope_sample", task_count),
            &(task_count, group_count),
            |bencher, (task_count, group_count)| {
                bencher.iter(|| {
                    let output = run_diagnostics_sample(black_box(*task_count), black_box(*group_count), 0);
                    black_box(output.aggregate_elapsed);
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("bounded_detail_sample", task_count),
            &(task_count, group_count),
            |bencher, (task_count, group_count)| {
                bencher.iter(|| {
                    let output = run_diagnostics_sample(black_box(*task_count), black_box(*group_count), 64);
                    black_box(output.detail_elapsed);
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
    targets = bench_runtime_diagnostics
}
criterion_main!(benches);
