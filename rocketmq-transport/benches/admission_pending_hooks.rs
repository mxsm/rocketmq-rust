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

use std::hint::black_box;
use std::time::Duration;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_transport::benchmark_support::AdmissionHotPathHarness;
use rocketmq_transport::benchmark_support::HookHotPathHarness;
use rocketmq_transport::benchmark_support::PendingHotPathHarness;

fn benchmark_admission_pending_hooks(c: &mut Criterion) {
    let admission = AdmissionHotPathHarness::new();
    let mut admission_group = c.benchmark_group("admission_scope");
    admission_group.sample_size(10);
    admission_group.warm_up_time(Duration::from_secs(1));
    admission_group.measurement_time(Duration::from_secs(2));
    admission_group.bench_function("registry_lookup", |benchmark| {
        benchmark.iter(|| admission.registry_lookup_acquire_release(black_box(128)));
    });
    admission_group.bench_function("prepared_handle", |benchmark| {
        benchmark.iter(|| admission.prepared_acquire_release(black_box(128)));
    });
    admission_group.finish();

    let pending = PendingHotPathHarness::new();
    let mut pending_group = c.benchmark_group("pending_completion");
    pending_group.sample_size(10);
    pending_group.warm_up_time(Duration::from_secs(1));
    pending_group.measurement_time(Duration::from_secs(2));
    pending_group.bench_function("legacy_box_mutex", |benchmark| {
        benchmark.iter(|| pending.boxed_mutex_completion());
    });
    pending_group.bench_function("concrete_oneshot", |benchmark| {
        benchmark.iter(|| pending.concrete_oneshot_completion());
    });
    pending_group.bench_function("concrete_table_completion", |benchmark| {
        benchmark.iter(|| pending.concrete_register_complete());
    });
    pending_group.finish();

    let mut hooks_group = c.benchmark_group("hook_snapshot");
    hooks_group.sample_size(10);
    hooks_group.warm_up_time(Duration::from_secs(1));
    hooks_group.measurement_time(Duration::from_secs(2));
    for hook_count in [0, 1, 8, 32] {
        let hooks = HookHotPathHarness::new(hook_count);
        hooks_group.bench_with_input(
            BenchmarkId::new("legacy_vec_clone", hook_count),
            &hooks,
            |benchmark, hooks| benchmark.iter(|| black_box(hooks.clone_legacy())),
        );
        hooks_group.bench_with_input(
            BenchmarkId::new("arc_swap_snapshot", hook_count),
            &hooks,
            |benchmark, hooks| benchmark.iter(|| black_box(hooks.load_snapshot())),
        );
    }
    hooks_group.finish();
}

criterion_group!(benches, benchmark_admission_pending_hooks);
criterion_main!(benches);
