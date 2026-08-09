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

#![recursion_limit = "256"]

#[path = "support/mod.rs"]
mod support;

use std::hint::black_box;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;

#[derive(Debug)]
struct ClientRuntimeSpawnOutput {
    task_count: usize,
    retained_child_groups: usize,
    elapsed: Duration,
}

fn run_explicit_runtime_spawn(task_count: usize) -> ClientRuntimeSpawnOutput {
    let runtime = support::BenchClientRuntime::new("explicit-spawn");
    let spawn_context = runtime.component("spawn");
    let task_group = spawn_context.task_group().clone();
    let baseline_children = task_group.component_count();
    let (tx, rx) = mpsc::channel();
    let started_at = Instant::now();

    for task_index in 0..task_count {
        let tx = tx.clone();
        task_group
            .spawn_service("explicit-client-runtime-spawn", async move {
                tx.send(task_index).expect("benchmark receiver should stay alive");
            })
            .expect("explicit client runtime task should spawn");
    }
    drop(tx);

    for _ in 0..task_count {
        rx.recv_timeout(Duration::from_secs(5))
            .expect("explicit client runtime task should complete");
    }
    let elapsed = started_at.elapsed();
    let retained_child_groups = task_group.component_count() - baseline_children;
    runtime.shutdown();

    ClientRuntimeSpawnOutput {
        task_count,
        retained_child_groups,
        elapsed,
    }
}

fn bench_client_runtime_spawn(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("client_runtime_spawn");
    for task_count in [32usize, 128] {
        group.bench_with_input(
            BenchmarkId::new("explicit_owned_runtime", task_count),
            &task_count,
            |bencher, task_count| {
                bencher.iter(|| {
                    let output = run_explicit_runtime_spawn(black_box(*task_count));
                    assert_eq!(output.task_count, *task_count);
                    assert_eq!(output.retained_child_groups, 0);
                    black_box(output.elapsed);
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
    targets = bench_client_runtime_spawn
}
criterion_main!(benches);
