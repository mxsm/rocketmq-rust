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

//! Measures admitted metadata work separately from fixture setup and shutdown.
//! A gate holds the first real filesystem write while hot and cold work queues.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rocketmq_runtime::*;
use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[derive(Debug, Default)]
struct GatedFileSystem {
    first: AtomicBool,
    released: Mutex<bool>,
    wake: Condvar,
    started: tokio::sync::Notify,
    writes: Mutex<Vec<Vec<u8>>>,
}
impl GatedFileSystem {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}
impl MetadataFileSystem for GatedFileSystem {
    fn persist_atomic(&self, target: &Path, bytes: &[u8]) -> RuntimeResult<()> {
        if !self.first.swap(true, Ordering::AcqRel) {
            self.started.notify_one();
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.wake.wait(released).unwrap();
            }
        }
        LocalMetadataFileSystem.persist_atomic(target, bytes)?;
        self.writes.lock().unwrap().push(bytes.to_vec());
        Ok(())
    }
}
struct ReleaseOnDrop(Arc<GatedFileSystem>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct Fixture {
    owner: RuntimeOwner,
    actor: MetadataIoActor,
    files: TempDir,
    filesystem: Arc<GatedFileSystem>,
}
impl Fixture {
    fn new() -> Self {
        let owner = RuntimeOwner::plan(RuntimeConfig::for_parallelism("metadata-bench", 2))
            .unwrap()
            .build()
            .unwrap();
        let parent = owner.root_context().component("metadata");
        let filesystem = Arc::new(GatedFileSystem::default());
        let actor = MetadataIoConfig::default()
            .into_plan()
            .unwrap()
            .start_with_file_system(&parent, filesystem.clone())
            .unwrap();
        Self {
            owner,
            actor,
            files: TempDir::new().unwrap(),
            filesystem,
        }
    }
    fn submit(&self, resource: &str, generation: u64) -> MetadataIoReceipt {
        let request = MetadataWriteRequest::new(
            resource,
            generation,
            self.files.path().join(resource),
            format!("{resource}-{generation}").into_bytes(),
        );
        match self
            .actor
            .submit(request, MetadataDeadline::after(Duration::from_secs(30)))
            .unwrap()
        {
            MetadataIoAdmissionOutcome::Accepted(receipt) => receipt,
            MetadataIoAdmissionOutcome::TargetConflict(_) => panic!("distinct or same-resource targets"),
        }
    }
    fn finish(self) {
        assert_eq!(self.actor.snapshot().pending_operations, 0);
        assert_eq!(self.actor.snapshot().pending_bytes, 0);
        assert!(
            !self
                .owner
                .block_on(
                    self.actor
                        .shutdown_until(MetadataDeadline::after(Duration::from_secs(5)))
                )
                .timed_out
        );
        assert!(self
            .owner
            .shutdown_runtime_blocking_with_timeout(Duration::from_secs(5))
            .unwrap()
            .is_healthy());
    }
}
#[derive(Clone, Copy, Debug)]
enum Scenario {
    Coalesced,
    HotAndCold,
    Distinct,
}
fn run(scenario: Scenario, count: usize) -> (Duration, serde_json::Value) {
    let fixture = Fixture::new();
    let release = ReleaseOnDrop(fixture.filesystem.clone());
    let (elapsed, submitted) = fixture.owner.block_on(async {
        let deadline = MetadataDeadline::after(Duration::from_secs(30));
        let first = fixture.submit("hot", 1);
        fixture.filesystem.started.notified().await;
        let mut receipts = Vec::with_capacity(count + 1);
        receipts.push(first);
        let start = Instant::now();
        match scenario {
            Scenario::Coalesced | Scenario::HotAndCold => {
                if matches!(scenario, Scenario::HotAndCold) {
                    receipts.push(fixture.submit("cold", 1));
                }
                for generation in 2..=count {
                    receipts.push(fixture.submit("hot", generation as u64));
                }
                assert_eq!(
                    fixture.actor.snapshot().pending_operations,
                    if matches!(scenario, Scenario::HotAndCold) { 3 } else { 2 }
                );
            }
            Scenario::Distinct => {
                for index in 1..count {
                    receipts.push(fixture.submit(&format!("other-{index}"), 1));
                }
                assert_eq!(fixture.actor.snapshot().pending_operations, count);
            }
        }
        let submitted = receipts.len();
        fixture.filesystem.release();
        for receipt in receipts {
            receipt.wait_until(deadline).await.unwrap();
        }
        (start.elapsed(), submitted)
    });
    drop(release);
    let writes = fixture.filesystem.writes.lock().unwrap().clone();
    match scenario {
        Scenario::Coalesced => assert_eq!(writes, vec![b"hot-1".to_vec(), format!("hot-{count}").into_bytes()]),
        Scenario::HotAndCold => assert_eq!(
            writes,
            vec![
                b"hot-1".to_vec(),
                b"cold-1".to_vec(),
                format!("hot-{count}").into_bytes()
            ]
        ),
        Scenario::Distinct => assert_eq!(writes.len(), count),
    }
    assert_eq!(
        fs::read(fixture.files.path().join("hot")).unwrap(),
        if matches!(scenario, Scenario::Distinct) {
            b"hot-1".to_vec()
        } else {
            format!("hot-{count}").into_bytes()
        }
    );
    let row = serde_json::json!({"scenario": format!("{scenario:?}"), "submitted": submitted,
        "writes": writes.len(), "rejected": 0, "elapsed_ns": elapsed.as_nanos() as u64,
        "pending_operations": fixture.actor.snapshot().pending_operations,
        "pending_bytes": fixture.actor.snapshot().pending_bytes});
    fixture.finish();
    (elapsed, row)
}
fn bench_metadata_io(criterion: &mut Criterion) {
    let cases = [
        (Scenario::Coalesced, 16),
        (Scenario::Coalesced, 64),
        (Scenario::Distinct, 8),
        (Scenario::Distinct, 32),
        (Scenario::HotAndCold, 64),
    ];
    let mut rows = Vec::new();
    for (scenario, count) in cases {
        for _ in 0..21 {
            rows.push(run(scenario, count).1);
        }
    }
    let output = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target"))
        .join("runtime-measurements");
    fs::create_dir_all(&output).unwrap();
    fs::write(output.join("metadata-io-report.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "window": "queued submission, gate release and real filesystem durability; first gate arrival/setup/shutdown excluded",
        "repetitions": 21, "rows": rows,
    })).unwrap()).unwrap();
    let mut group = criterion.benchmark_group("metadata_io");
    for (scenario, count) in cases {
        group.bench_with_input(
            BenchmarkId::new(format!("{scenario:?}"), count),
            &count,
            |bencher, count| {
                // Return only the explicitly measured work duration to Criterion.
                bencher.iter_custom(|iterations| (0..iterations).map(|_| black_box(run(scenario, *count).0)).sum());
            },
        );
    }
    group.finish();
}
criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10)
        .warm_up_time(Duration::from_millis(500)).measurement_time(Duration::from_secs(1));
    targets = bench_metadata_io
}
criterion_main!(benches);
