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

//! Baseline for the runtime convergence work on submission, budgets, idle
//! memory and blocking admission.
//!
//! Each scenario records raw throughput, latency percentiles and allocation
//! counts so later changes can be compared against the same environment. The
//! JSON artifact is written to `target/runtime-baseline/convergence/`, named
//! by `ROCKETMQ_BENCH_LABEL` (default `local`). No threshold is asserted.
//!
//! ```text
//! ROCKETMQ_BENCH_LABEL=before cargo bench -p rocketmq-runtime --bench runtime_convergence_bench
//! ```

use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::OperationContext;
use rocketmq_runtime::ResourceBudgetTree;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ScopeId;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskKind;
use serde_json::json;
use serde_json::Value;

#[path = "support/allocation.rs"]
mod allocation;

#[global_allocator]
static ALLOCATOR: allocation::CountingAllocator = allocation::CountingAllocator;

const THREAD_COUNTS: [usize; 3] = [1, 4, 8];
const OPERATIONS_PER_THREAD: usize = 20_000;
const PERMITS_PER_THREAD: usize = 200_000;
const IDLE_SAMPLES: usize = 1_000;
const BLOCKING_TASKS: usize = 48;

fn owner() -> RuntimeOwner {
    let mut config = RuntimeConfig::for_parallelism("rocketmq-convergence-bench", 4);
    config.worker_threads = 4;
    config.blocking_lane_policies.storage_io.max_concurrency = 2;
    config.blocking_lane_policies.storage_io.max_queue_depth = 256;
    RuntimeOwner::plan(config)
        .expect("benchmark runtime configuration is valid")
        .build()
        .expect("benchmark runtime starts")
}

/// Latency percentiles in nanoseconds over one sample set.
fn percentiles(samples: &mut [u64]) -> Value {
    samples.sort_unstable();
    let at = |fraction: f64| {
        let index = ((samples.len() as f64 - 1.0) * fraction).round() as usize;
        samples[index]
    };
    json!({
        "samples": samples.len(),
        "p50_ns": at(0.50),
        "p90_ns": at(0.90),
        "p99_ns": at(0.99),
        "p999_ns": at(0.999),
        "max_ns": samples[samples.len() - 1],
    })
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[derive(Clone, Copy)]
enum GroupLayout {
    Shared,
    PerConnection,
}

impl GroupLayout {
    fn as_str(self) -> &'static str {
        match self {
            Self::Shared => "shared_group",
            Self::PerConnection => "group_per_connection",
        }
    }
}

/// Item 1: N threads submit draining operations into one group, or each into
/// its own connection group.
fn operation_submission(layout: GroupLayout, threads: usize) -> Value {
    let owner = owner();
    let service = owner.root_context().component("bench.operations");
    let shared = service.task_group().clone();
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut workers = Vec::with_capacity(threads);
    for thread in 0..threads {
        let group = match layout {
            GroupLayout::Shared => shared.clone(),
            GroupLayout::PerConnection => shared
                .try_child(format!("connection-{thread}"))
                .expect("the bench group admits connection groups"),
        };
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            let operation = OperationContext::without_deadline(TaskKind::Worker);
            let mut latencies = Vec::with_capacity(OPERATIONS_PER_THREAD);
            barrier.wait();
            for _ in 0..OPERATIONS_PER_THREAD {
                let started = Instant::now();
                group
                    .spawn_draining_operation(&operation, "bench.operation", async {})
                    .expect("the bench group admits operations");
                latencies.push(elapsed_ns(started));
            }
            operation.close_admission();
            (group, operation, latencies)
        }));
    }
    barrier.wait();
    let started = Instant::now();
    let results: Vec<(TaskGroup, OperationContext, Vec<u64>)> = workers
        .into_iter()
        .map(|worker| worker.join().expect("submission thread"))
        .collect();
    let submit_elapsed = started.elapsed();
    owner.block_on(async {
        for (group, operation, _) in &results {
            assert!(operation
                .wait(group, Duration::from_secs(60))
                .await
                .expect("operation keeps its owner"));
        }
    });
    let drained_elapsed = started.elapsed();
    let mut latencies: Vec<u64> = results.into_iter().flat_map(|(_, _, latencies)| latencies).collect();
    let total = threads * OPERATIONS_PER_THREAD;
    let report = owner.block_on(async { service.task_group().shutdown(Duration::from_secs(30)).await });
    assert!(report.is_healthy(), "{}", report.to_json());
    json!({
        "layout": layout.as_str(),
        "threads": threads,
        "operations": total,
        "submit_elapsed_ms": submit_elapsed.as_secs_f64() * 1e3,
        "drained_elapsed_ms": drained_elapsed.as_secs_f64() * 1e3,
        "submit_per_second": total as f64 / submit_elapsed.as_secs_f64(),
        "latency": percentiles(&mut latencies),
    })
}

/// Item 2: N threads acquire and release permits of their own connection
/// budget under one shared process root.
fn budget_permits(threads: usize) -> Value {
    let tree = ResourceBudgetTree::new(
        "bench-process",
        BudgetLimit::new(1_000_000, usize::MAX / 4, FullPolicy::Reject),
    )
    .expect("valid bench root budget");
    let root = tree.root();
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut workers = Vec::with_capacity(threads);
    for thread in 0..threads {
        let connection = root
            .child(
                format!("connection-{thread}"),
                BudgetLimit::new(1_024, 1 << 30, FullPolicy::Reject),
            )
            .expect("valid bench connection budget");
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            let mut latencies = Vec::with_capacity(PERMITS_PER_THREAD);
            barrier.wait();
            for _ in 0..PERMITS_PER_THREAD {
                let started = Instant::now();
                let permit = connection.try_acquire_data(64).expect("bench budget has capacity");
                drop(black_box(permit));
                latencies.push(elapsed_ns(started));
            }
            latencies
        }));
    }
    barrier.wait();
    let started = Instant::now();
    let mut latencies: Vec<u64> = workers
        .into_iter()
        .flat_map(|worker| worker.join().expect("permit thread"))
        .collect();
    let elapsed = started.elapsed();
    let total = threads * PERMITS_PER_THREAD;
    let snapshot = root.snapshot();
    assert_eq!(snapshot.current_count, 0);
    json!({
        "threads": threads,
        "permits": total,
        "elapsed_ms": elapsed.as_secs_f64() * 1e3,
        "acquire_release_per_second": total as f64 / elapsed.as_secs_f64(),
        "latency": percentiles(&mut latencies),
    })
}

/// Item 3: allocations made to create an idle connection group and an
/// operation context.
fn idle_memory() -> Value {
    let owner = owner();
    let service = owner.root_context().component("bench.idle");
    let parent = service.task_group().clone();
    let mut groups = Vec::with_capacity(IDLE_SAMPLES);
    let mut group_calls = 0;
    let mut group_bytes = 0;
    for index in 0..IDLE_SAMPLES {
        let name: Arc<str> = Arc::from(format!("session-{index}"));
        let (group, (calls, bytes)) = allocation::measure(|| parent.try_child(name).expect("idle group"));
        group_calls += calls;
        group_bytes += bytes;
        groups.push(group);
    }
    let mut contexts = Vec::with_capacity(IDLE_SAMPLES);
    let mut context_calls = 0;
    let mut context_bytes = 0;
    for _ in 0..IDLE_SAMPLES {
        let (context, (calls, bytes)) = allocation::measure(|| OperationContext::without_deadline(TaskKind::Worker));
        context_calls += calls;
        context_bytes += bytes;
        contexts.push(context);
    }
    let mut services = Vec::with_capacity(IDLE_SAMPLES);
    let mut service_calls = 0;
    let mut service_bytes = 0;
    for index in 0..IDLE_SAMPLES {
        let scope = ScopeId::try_new(format!("session-context-{index}")).expect("nonblank scope");
        let (child, (calls, bytes)) = allocation::measure(|| service.component(scope));
        service_calls += calls;
        service_bytes += bytes;
        services.push(child);
    }
    drop((groups, contexts, services));
    let report = owner.block_on(async { service.task_group().shutdown(Duration::from_secs(10)).await });
    assert!(report.is_healthy(), "{}", report.to_json());
    let per = |total: u64| total as f64 / IDLE_SAMPLES as f64;
    json!({
        "samples": IDLE_SAMPLES,
        "task_group": { "allocations": per(group_calls), "bytes": per(group_bytes) },
        "operation_context": { "allocations": per(context_calls), "bytes": per(context_bytes) },
        "child_service_context": { "allocations": per(service_calls), "bytes": per(service_bytes) },
    })
}

/// Item 4: start order of contended blocking work against its submission order.
fn blocking_admission_order() -> Value {
    let owner = owner();
    let service = owner.root_context().component("bench.blocking");
    let started_order = Arc::new(Mutex::new(Vec::with_capacity(BLOCKING_TASKS)));
    let started = Instant::now();
    owner.block_on(async {
        let storage = service.storage_io();
        let submissions = (0..BLOCKING_TASKS).map(|index| {
            let started_order = Arc::clone(&started_order);
            storage.spawn_io("bench.blocking", move || {
                started_order
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(index);
                std::thread::sleep(Duration::from_millis(2));
            })
        });
        // join_all polls the submissions in order, so admission sees them in index order.
        for result in futures::future::join_all(submissions).await {
            result.expect("blocking work is admitted");
        }
    });
    let elapsed = started.elapsed();
    let order = started_order
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let inversions = order
        .iter()
        .enumerate()
        .map(|(position, index)| order[position + 1..].iter().filter(|later| *later < index).count())
        .sum::<usize>();
    let report = owner.block_on(async { service.task_group().shutdown(Duration::from_secs(10)).await });
    assert!(report.is_healthy(), "{}", report.to_json());
    json!({
        "tasks": BLOCKING_TASKS,
        "lane_concurrency": 2,
        "elapsed_ms": elapsed.as_secs_f64() * 1e3,
        "start_order_inversions": inversions,
    })
}

fn environment() -> Value {
    json!({
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "available_parallelism": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
        "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        "runtime_worker_threads": 4,
    })
}

fn main() {
    // Cargo passes `--bench` to harness-less benches; `cargo test --benches`
    // runs them without it, and only the build is needed there.
    if !std::env::args().any(|argument| argument == "--bench") {
        return;
    }
    let label = std::env::var("ROCKETMQ_BENCH_LABEL").unwrap_or_else(|_| "local".to_string());
    let mut operations = Vec::new();
    for layout in [GroupLayout::Shared, GroupLayout::PerConnection] {
        for threads in THREAD_COUNTS {
            let result = operation_submission(layout, threads);
            println!("operations {result}");
            operations.push(result);
        }
    }
    let mut permits = Vec::new();
    for threads in THREAD_COUNTS {
        let result = budget_permits(threads);
        println!("permits {result}");
        permits.push(result);
    }
    let memory = idle_memory();
    println!("idle_memory {memory}");
    let blocking = blocking_admission_order();
    println!("blocking {blocking}");

    let payload = json!({
        "label": label,
        "generated_at_unix_ms": SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis())
            .unwrap_or_default(),
        "environment": environment(),
        "operation_submission": operations,
        "budget_permits": permits,
        "idle_memory": memory,
        "blocking_admission": blocking,
    });
    let output_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-runtime lives below the workspace root")
        .join("target/runtime-baseline/convergence");
    fs::create_dir_all(&output_dir).expect("benchmark artifact directory");
    let path = output_dir.join(format!("{label}.json"));
    fs::write(
        &path,
        serde_json::to_vec_pretty(&payload).expect("serializable payload"),
    )
    .expect("benchmark artifact is written");
    println!("wrote {}", path.display());
}
