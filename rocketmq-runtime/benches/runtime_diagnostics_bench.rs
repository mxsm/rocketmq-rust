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
//! Aggregate age statistics still scan the complete population; only detail
//! collection obeys the requested scan budget.

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

#[path = "support/allocation.rs"]
mod allocation;

#[global_allocator]
static ALLOCATOR: allocation::CountingAllocator = allocation::CountingAllocator;

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
    RuntimeConfig::for_parallelism("rocketmq-diagnostics-bench", 2)
        .with_max_blocking_threads(3)
        .expect("benchmark blocking lanes fit the global limit")
}

struct DiagnosticsFixture {
    owner: RuntimeOwner,
    root: rocketmq_runtime::ChildServiceContext,
    task_count: usize,
}

impl DiagnosticsFixture {
    fn new(task_count: usize, group_count: usize) -> Self {
        assert!(group_count > 0 && task_count >= group_count);
        let owner = RuntimeOwner::plan(runtime_config()).unwrap().build().unwrap();
        let root = owner.root_context().component("bench.diagnostics-root");
        for group_index in 0..group_count {
            let component =
                root.component(rocketmq_runtime::ScopeId::try_new(format!("bench.diagnostics.{group_index}")).unwrap());
            for task_index in (group_index..task_count).step_by(group_count) {
                let cancellation = component.task_group().cancellation_token();
                component
                    .spawn(format!("diagnostics-task-{task_index}"), TaskKind::Worker, async move {
                        cancellation.cancelled().await
                    })
                    .unwrap();
            }
        }
        let fixture = Self {
            owner,
            root,
            task_count,
        };
        assert_eq!(fixture.aggregate().tasks.task_count, task_count);
        fixture
    }

    fn aggregate(&self) -> rocketmq_runtime::RuntimeDiagnosticsViewV2 {
        self.root
            .diagnostics_view_v2(RuntimeComponent::Broker, RuntimeDiagnosticsInputs::default())
    }

    fn detailed(&self, detail_entries: usize) -> rocketmq_runtime::RuntimeDiagnosticsViewV2 {
        self.root.diagnostics_view_v2_with_options(
            RuntimeComponent::Broker,
            RuntimeDiagnosticsInputs::default(),
            RuntimeDiagnosticsViewOptionsV2 {
                max_detail_entries: detail_entries,
                detail_scan_budget: self.task_count,
                ..Default::default()
            },
        )
    }

    fn finish(self) -> ShutdownReport {
        let report = self
            .owner
            .block_on(self.root.task_group().shutdown(Duration::from_secs(10)));
        assert!(report.is_healthy(), "{}", report.to_json());
        report
    }
}

fn run_diagnostics_sample(task_count: usize, group_count: usize, detail_entries: usize) -> DiagnosticsOutput {
    let fixture = DiagnosticsFixture::new(task_count, group_count);
    let sampled_at = Instant::now();
    let aggregate = fixture.aggregate();
    let aggregate_elapsed = sampled_at.elapsed();
    assert_eq!(aggregate.tasks.task_count, task_count);
    assert_eq!(aggregate.tasks.scope, RuntimeDiagnosticsScope::Subtree);
    assert!(aggregate.details.is_empty());

    let detailed_at = Instant::now();
    let detailed = fixture.detailed(detail_entries);
    let detail_elapsed = detailed_at.elapsed();
    assert_eq!(detailed.details.len(), detail_entries);
    assert_eq!(detailed.details_scanned, task_count);

    let versioned_at = Instant::now();
    let versioned = fixture.root.diagnostics_view_v1(RuntimeComponent::Broker);
    let versioned_elapsed = versioned_at.elapsed();
    assert_eq!(versioned.task_count, task_count);
    let report = fixture.finish();
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
}

fn write_diagnostics_report_artifact() {
    let output = run_diagnostics_sample(512, 8, 64);
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-runtime should live below workspace root")
        .to_path_buf();
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root.join("target"));
    let output_dir = target_dir.join("runtime-measurements");
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

fn write_sampling_measurements() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let mut rows = Vec::new();
    for tasks in [1_000, 10_000, 100_000] {
        let fixture = DiagnosticsFixture::new(tasks, 8);
        for details in [0, 64] {
            for _ in 0..5 {
                black_box(fixture.detailed(details));
            }
            let mut durations = Vec::with_capacity(101);
            let mut allocations = Vec::with_capacity(101);
            for _ in 0..101 {
                let start = Instant::now();
                let (view, counts) = allocation::measure(|| fixture.detailed(details));
                durations.push(start.elapsed().as_nanos() as u64);
                allocations.push(counts);
                assert_eq!(view.tasks.task_count, tasks);
                assert_eq!(view.details.len(), details);
                black_box(view);
            }
            rows.push(serde_json::json!({"tasks": tasks, "groups": 8, "details": details,
                "sample_ns": durations, "alloc_calls_and_requested_bytes": allocations}));
        }
        fixture.finish();
    }
    let mut submissions = Vec::new();
    for repetition in 0..3 {
        for sampler in [false, true] {
            let fixture = DiagnosticsFixture::new(10_000, 8);
            let work = fixture.root.component("submission-probe");
            let stop = AtomicBool::new(false);
            let samples = AtomicUsize::new(0);
            let observed = fixture.root.clone();
            let mut submit_ns = Vec::with_capacity(5_000);
            let mut completion_ns = Vec::with_capacity(5_000);
            let measured = Instant::now();
            std::thread::scope(|scope| {
                if sampler {
                    let stop = &stop;
                    let samples = &samples;
                    scope.spawn(move || {
                        while !stop.load(Ordering::Acquire) {
                            black_box(
                                observed
                                    .diagnostics_view_v2(RuntimeComponent::Broker, RuntimeDiagnosticsInputs::default()),
                            );
                            samples.fetch_add(1, Ordering::Relaxed);
                            std::thread::sleep(Duration::from_millis(10));
                        }
                    });
                }
                fixture.owner.block_on(async {
                    for _ in 0..5_000 {
                        let start = Instant::now();
                        let id = work.spawn("submission-probe", TaskKind::Worker, async {}).unwrap();
                        submit_ns.push(start.elapsed().as_nanos() as u64);
                        assert!(work.task_group().wait_task(id, Duration::from_secs(10)).await);
                        completion_ns.push(start.elapsed().as_nanos() as u64);
                    }
                });
                stop.store(true, Ordering::Release);
            });
            let elapsed = measured.elapsed();
            assert_eq!(work.task_group().task_count(), 0);
            submissions.push(serde_json::json!({"repetition": repetition, "sampler": sampler,
                "tasks": 10_000, "groups": 8, "submitters": 1, "rejected": 0,
                "sample_delay_ms": 10, "actual_samples": samples.load(Ordering::Relaxed),
                "window_ns": elapsed.as_nanos() as u64, "submit_ns": submit_ns,
                "completion_ns": completion_ns}));
            fixture.finish();
        }
    }
    let output = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target"))
        .join("runtime-measurements");
    fs::create_dir_all(&output).unwrap();
    fs::write(output.join("sampling-costs.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "allocation_scope": "sampling thread only; successful alloc/realloc calls and requested bytes, not peak/live bytes",
        "timing_scope": "sample plus allocation counter; excludes setup/drop/teardown",
        "samples": rows, "submissions": submissions,
    })).unwrap()).unwrap();
}

fn bench_runtime_diagnostics(criterion: &mut Criterion) {
    write_diagnostics_report_artifact();
    if std::env::var_os("ROCKETMQ_MEASURE_SAMPLING").is_some() {
        write_sampling_measurements();
    }

    let mut group = criterion.benchmark_group("runtime_diagnostics");
    // Runtime construction, population, assertions and shutdown stay outside
    // Criterion's timed iterations. Only one sample is measured per iteration.
    for task_count in [1_000usize, 10_000, 100_000] {
        group.bench_with_input(
            BenchmarkId::new("aggregate_tasks", task_count),
            &task_count,
            |bencher, count| {
                let fixture = DiagnosticsFixture::new(*count, 8);
                bencher.iter(|| black_box(fixture.aggregate()));
                fixture.finish();
            },
        );
        group.bench_with_input(
            BenchmarkId::new("detail_tasks", task_count),
            &task_count,
            |bencher, count| {
                let fixture = DiagnosticsFixture::new(*count, 8);
                bencher.iter(|| black_box(fixture.detailed(64)));
                fixture.finish();
            },
        );
        group.bench_with_input(
            BenchmarkId::new("v1_tasks", task_count),
            &task_count,
            |bencher, count| {
                let fixture = DiagnosticsFixture::new(*count, 8);
                bencher.iter(|| black_box(fixture.root.diagnostics_view_v1(RuntimeComponent::Broker)));
                fixture.finish();
            },
        );
    }
    for groups in [1usize, 8, 64] {
        group.bench_with_input(
            BenchmarkId::new("aggregate_groups", groups),
            &groups,
            |bencher, count| {
                let fixture = DiagnosticsFixture::new(10_000, *count);
                bencher.iter(|| black_box(fixture.aggregate()));
                fixture.finish();
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
