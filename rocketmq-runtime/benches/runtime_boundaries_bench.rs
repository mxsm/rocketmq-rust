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

//! Workload evidence for admission, retained memory, and bounded diagnostics.
//! Synthetic blocking delays measure queue isolation, not disk throughput.

use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Condvar, Mutex,
};
use std::time::{Duration, Instant};

use rocketmq_runtime::{
    BlockingExecutor, BlockingLane, BlockingPoolPolicy, MetadataDeadline, MetadataFileSystem, MetadataIoConfig,
    MetadataIoReceipt, MetadataWriteRequest, MetadataWriteSubmissionStatus, ProcessMemoryLimit, RuntimeComponent,
    RuntimeConfig, RuntimeDiagnosticsInputs, RuntimeErrorKind, RuntimeOwner, RuntimeResult, ShutdownDeadline,
};
use serde_json::{json, Value};

#[path = "support/allocation.rs"]
mod allocation;

#[global_allocator]
static ALLOCATOR: allocation::CountingAllocator = allocation::CountingAllocator;

fn owner() -> RuntimeOwner {
    let mut config = RuntimeConfig::for_parallelism("runtime-boundaries", 2)
        .with_max_blocking_threads(3)
        .unwrap();
    config.blocking_lane_policies.storage_io.max_queue_depth = 32;
    config.blocking_lane_policies.metadata_io.max_queue_depth = 32;
    config.blocking_lane_policies.cpu_crypto.max_queue_depth = 32;
    RuntimeOwner::plan(config)
        .unwrap()
        .with_memory_limit(ProcessMemoryLimit::configured(1024 * 1024).unwrap())
        .build()
        .unwrap()
}

fn summary(samples: &[u64]) -> Value {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let percentile = |q: usize| sorted[(sorted.len() - 1) * q / 100];
    json!({"samples": sorted.len(), "p50_ns": percentile(50), "p95_ns": percentile(95),
        "p99_ns": percentile(99), "max_ns": sorted.last().unwrap()})
}

fn mixed_lanes() -> Value {
    let owner = owner();
    let service = owner.root_context().component("mixed");
    let lanes = [
        BlockingLane::StorageIo,
        BlockingLane::MetadataIo,
        BlockingLane::CpuCrypto,
    ];
    let start = Instant::now();
    let rows = owner.block_on(async {
        let work = lanes.into_iter().flat_map(|lane| {
            let executor = service.blocking(lane).clone();
            (0..32).map(move |_| {
                let executor = executor.clone();
                async move {
                    let submitted = Instant::now();
                    let queue = executor
                        .spawn_io("synthetic-one-ms", move || {
                            let queue = submitted.elapsed();
                            std::thread::sleep(Duration::from_millis(1));
                            queue
                        })
                        .await
                        .unwrap();
                    (lane, queue.as_nanos() as u64, submitted.elapsed().as_nanos() as u64)
                }
            })
        });
        futures::future::join_all(work).await
    });
    let elapsed = start.elapsed();
    let rows = lanes
        .into_iter()
        .map(|lane| {
            let queue = rows
                .iter()
                .filter(|row| row.0 == lane)
                .map(|row| row.1)
                .collect::<Vec<_>>();
            let total = rows
                .iter()
                .filter(|row| row.0 == lane)
                .map(|row| row.2)
                .collect::<Vec<_>>();
            assert_eq!(service.blocking(lane).snapshot().global_running, 0);
            json!({"lane": format!("{lane:?}"), "queue": summary(&queue), "completion": summary(&total)})
        })
        .collect::<Vec<_>>();
    let shutdown = Instant::now();
    let report = owner.shutdown_runtime_blocking().unwrap();
    assert!(report.is_healthy());
    json!({"workload": "96 concurrent submissions; synthetic 1 ms blocking closures; global capacity 3",
        "elapsed_ns": elapsed.as_nanos() as u64, "throughput_per_second": 96.0 / elapsed.as_secs_f64(),
        "rejected": 0, "lanes": rows, "shutdown_ns": shutdown.elapsed().as_nanos() as u64,
        "blocking_still_running": report.blocking_still_running})
}

#[derive(Debug)]
struct ControlledFileSystem {
    delay: Option<Duration>,
    released: Mutex<bool>,
    wake: Condvar,
    started: AtomicUsize,
}

impl ControlledFileSystem {
    fn new(delay: Option<Duration>) -> Self {
        Self {
            delay,
            released: Mutex::new(false),
            wake: Condvar::new(),
            started: AtomicUsize::new(0),
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }

    async fn wait_started(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while self.started.load(Ordering::Acquire) < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}

impl MetadataFileSystem for ControlledFileSystem {
    fn persist_atomic(&self, target: &Path, _bytes: &[u8]) -> RuntimeResult<()> {
        if target == Path::new("slow.json") {
            self.started.fetch_add(1, Ordering::Release);
            if let Some(delay) = self.delay {
                std::thread::sleep(delay);
            } else {
                let guard = self.released.lock().unwrap();
                let (guard, timeout) = self
                    .wake
                    .wait_timeout_while(guard, Duration::from_secs(10), |released| !*released)
                    .unwrap();
                assert!(*guard && !timeout.timed_out());
            }
        }
        Ok(())
    }
}

struct ReleaseOnDrop(Arc<ControlledFileSystem>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

fn accepted(outcome: MetadataWriteSubmissionStatus) -> MetadataIoReceipt {
    match outcome {
        MetadataWriteSubmissionStatus::Accepted(receipt) => receipt,
        MetadataWriteSubmissionStatus::TargetConflict(_) => panic!("unique benchmark targets"),
    }
}

fn rss() -> u64 {
    let pid = sysinfo::get_current_pid().unwrap();
    let mut system = sysinfo::System::new();
    system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
    system.process(pid).unwrap().memory()
}

fn metadata_memory() -> Value {
    let owner = owner();
    let service = owner.root_context().component("metadata");
    let filesystem = Arc::new(ControlledFileSystem::new(None));
    let _release = ReleaseOnDrop(filesystem.clone());
    let first = MetadataIoConfig::default()
        .into_plan()
        .unwrap()
        .start_with_file_system(&service, filesystem.clone())
        .unwrap();
    let second = MetadataIoConfig::default()
        .into_plan()
        .unwrap()
        .start_with_file_system(&service, filesystem.clone())
        .unwrap();
    let rss_before = rss();
    let result = owner.block_on(async {
        let deadline = MetadataDeadline::after(Duration::from_secs(10));
        let held = accepted(
            first
                .submit(
                    MetadataWriteRequest::new("slow", 1_u64, "slow.json", vec![1; 768 * 1024]),
                    deadline,
                )
                .unwrap(),
        );
        filesystem.wait_started(1).await;
        let denied = second
            .submit(
                MetadataWriteRequest::new("fast", 1_u64, "fast.json", vec![2; 768 * 1024]),
                deadline,
            )
            .unwrap_err();
        assert_eq!(denied.kind(), RuntimeErrorKind::CapacityExhausted);
        let old = accepted(
            first
                .submit(
                    MetadataWriteRequest::new("slow", 2_u64, "slow.json", vec![2; 256 * 1024]),
                    deadline,
                )
                .unwrap(),
        );
        let latest = accepted(
            first
                .submit(
                    MetadataWriteRequest::new("slow", 3_u64, "slow.json", vec![3; 256 * 1024]),
                    deadline,
                )
                .unwrap(),
        );
        let charge = service.process_budget().snapshot().current_bytes;
        assert_eq!(charge, 1024 * 1024);
        let during = rss();
        filesystem.release();
        for receipt in [held, old, latest] {
            receipt.wait_until(deadline).await.unwrap();
        }
        assert!(!first.shutdown_until(deadline).await.timed_out);
        assert!(!second.shutdown_until(deadline).await.timed_out);
        assert_eq!(service.process_budget().snapshot().current_bytes, 0);
        json!({"filesystem": "injected; no disk writes", "managed_limit_bytes": 1024 * 1024,
            "retained_bytes_at_capacity": charge, "retained_bytes_after_drain": 0,
            "cross_actor_rejections": 1, "replacement_at_capacity": "accepted",
            "rss_before_bytes": rss_before, "rss_during_bytes": during, "rss_after_bytes": rss()})
    });
    assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
    result
}

fn slow_target(concurrency: usize) -> Value {
    let owner = owner();
    let service = owner.root_context().component("slow-target");
    let filesystem = Arc::new(ControlledFileSystem::new(Some(Duration::from_millis(20))));
    let actor = MetadataIoConfig::default()
        .into_plan()
        .unwrap()
        .with_max_concurrent_writes(std::num::NonZeroUsize::new(concurrency).unwrap())
        .start_with_file_system(&service, filesystem.clone())
        .unwrap();
    let samples = owner.block_on(async {
        let mut samples = Vec::new();
        for index in 0..21 {
            let deadline = MetadataDeadline::after(Duration::from_secs(5));
            let slow = accepted(actor.submit_next("slow", "slow.json", vec![1; 64], deadline).unwrap());
            filesystem.wait_started(index + 1).await;
            let start = Instant::now();
            accepted(actor.submit_next("fast", "fast.json", vec![2; 64], deadline).unwrap())
                .wait_until(deadline)
                .await
                .unwrap();
            samples.push(start.elapsed().as_nanos() as u64);
            slow.wait_until(deadline).await.unwrap();
        }
        assert!(
            !actor
                .shutdown_until(MetadataDeadline::after(Duration::from_secs(5)))
                .await
                .timed_out
        );
        samples
    });
    assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
    json!({"filesystem": "injected 20 ms slow target; immediate fast target", "concurrency": concurrency,
        "fast_target_completion": summary(&samples)})
}

fn diagnostics(groups: usize) -> Value {
    let owner = owner();
    let service = owner.root_context().component("diagnostics");
    let children = (0..groups)
        .map(|index| service.task_group().try_child(format!("child-{index}")).unwrap())
        .collect::<Vec<_>>();
    let mut timings = Vec::new();
    let mut aggregate_timings = Vec::new();
    let mut allocated_bytes = Vec::new();
    for _ in 0..31 {
        let start = Instant::now();
        let (sample, (_, bytes)) = allocation::measure(|| service.task_group().diagnostics_task_details(8, 8));
        timings.push(start.elapsed().as_nanos() as u64);
        allocated_bytes.push(bytes);
        assert_eq!(sample.group_entries_scanned, 8);
        assert!(sample.truncated);
        let start = Instant::now();
        let aggregate = service.diagnostics_view_v2(RuntimeComponent::Broker, RuntimeDiagnosticsInputs::default());
        aggregate_timings.push(start.elapsed().as_nanos() as u64);
        assert_eq!(aggregate.tasks.task_group_count, groups + 1);
    }
    drop(children);
    assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
    json!({"empty_groups": groups, "scan_budget": 8, "detail_only": summary(&timings),
        "aggregate": summary(&aggregate_timings), "max_detail_allocation_bytes": allocated_bytes.into_iter().max().unwrap(),
        "allocation_scope": "sampling thread requested allocations; excludes setup, aggregates and teardown"})
}

fn task_diagnostics(tasks: usize) -> Value {
    let owner = owner();
    let service = owner.root_context().component("task-diagnostics");
    let (ready, mut readiness) = tokio::sync::mpsc::unbounded_channel();
    for _ in 0..tasks {
        let ready = ready.clone();
        let cancellation = service.task_group().cancellation_token();
        service
            .spawn_service("held", async move {
                ready.send(()).unwrap();
                cancellation.cancelled().await;
            })
            .unwrap();
    }
    owner.block_on(async {
        for _ in 0..tasks {
            readiness.recv().await.unwrap();
        }
    });
    let mut detail_times = Vec::new();
    let mut aggregate_times = Vec::new();
    let mut allocated = Vec::new();
    for _ in 0..31 {
        let start = Instant::now();
        let (details, (_, bytes)) = allocation::measure(|| service.task_group().diagnostics_task_details(64, 16));
        detail_times.push(start.elapsed().as_nanos() as u64);
        allocated.push(bytes);
        assert_eq!(details.tasks_scanned, 64);
        assert_eq!(details.details.len(), 16);
        assert!(details.truncated);
        let start = Instant::now();
        let aggregate = service.diagnostics_view_v2(RuntimeComponent::Broker, RuntimeDiagnosticsInputs::default());
        aggregate_times.push(start.elapsed().as_nanos() as u64);
        assert_eq!(aggregate.tasks.task_count, tasks);
    }
    assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
    json!({"tasks": tasks, "scan_budget": 64, "output_budget": 16, "detail_only": summary(&detail_times),
        "aggregate": summary(&aggregate_times), "max_detail_allocation_bytes": allocated.into_iter().max().unwrap()})
}

fn isolated_shutdown() -> Value {
    let owner = owner();
    let service = owner.root_context().component("shutdown");
    let executor = BlockingExecutor::new_isolated(BlockingPoolPolicy::default(), service.task_group().clone()).unwrap();
    let result = owner.block_on(async {
        let (release, held) = std::sync::mpsc::channel();
        let (started, ready) = tokio::sync::oneshot::channel();
        let mut observer = Box::pin(executor.spawn_io("held", move || {
            started.send(()).unwrap();
            held.recv_timeout(Duration::from_secs(10)).unwrap();
        }));
        assert!(futures::poll!(observer.as_mut()).is_pending());
        ready.await.unwrap();
        drop(observer);
        let start = Instant::now();
        let timed_out = executor.shutdown_until(ShutdownDeadline::after(Duration::ZERO)).await.unwrap();
        let timeout_ns = start.elapsed().as_nanos() as u64;
        assert!(!timed_out.completed);
        assert_eq!(timed_out.pending_operations, 1);
        release.send(()).unwrap();
        let start = Instant::now();
        let settled = executor.shutdown_until(ShutdownDeadline::after(Duration::from_secs(5))).await.unwrap();
        assert!(settled.completed);
        json!({"timeout_observation_ns": timeout_ns, "unconfirmed_operations": timed_out.pending_operations,
            "confirmation_after_release_ns": start.elapsed().as_nanos() as u64, "remaining_after_release": settled.pending_operations})
    });
    assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
    result
}

fn main() {
    if !std::env::args().any(|argument| argument == "--bench") {
        return;
    }
    let results = json!({"os": std::env::consts::OS, "arch": std::env::consts::ARCH,
        "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        "available_parallelism": std::thread::available_parallelism().unwrap().get(),
        "percentiles": "sorted sample index floor((n - 1) * q / 100); descriptive, not production SLOs",
        "mixed_lanes": mixed_lanes(), "metadata_memory": metadata_memory(),
        "slow_target": [slow_target(1), slow_target(2)],
        "diagnostics": [diagnostics(128), diagnostics(4096), diagnostics(16384)],
        "task_diagnostics": [task_diagnostics(1000), task_diagnostics(10000), task_diagnostics(100000)],
        "isolated_shutdown": isolated_shutdown()});
    let output = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target"))
        .join("runtime-measurements");
    std::fs::create_dir_all(&output).unwrap();
    let path = output.join("runtime-boundaries.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&results).unwrap()).unwrap();
    println!("{results}");
    println!("wrote {}", path.display());
}
