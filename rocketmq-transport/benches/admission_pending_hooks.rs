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

use std::collections::HashMap;
use std::hint::black_box;

use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_protocol::RemotingCommand;
use rocketmq_transport::benchmark_support::AdmissionHotPathHarness;
use rocketmq_transport::benchmark_support::HookHotPathHarness;
use rocketmq_transport::benchmark_support::PendingHotPathHarness;

#[path = "support/criterion_profile.rs"]
mod criterion_profile;

use criterion_profile::apply_remoting_command_baseline_profile;

fn benchmark_admission_pending_hooks(c: &mut Criterion) {
    let admission = AdmissionHotPathHarness::new();
    let mut admission_group = c.benchmark_group("admission_scope");
    apply_remoting_command_baseline_profile(&mut admission_group);
    admission_group.bench_function("registry_lookup", |benchmark| {
        benchmark.iter(|| admission.registry_lookup_acquire_release(black_box(128)));
    });
    admission_group.bench_function("prepared_handle", |benchmark| {
        benchmark.iter(|| admission.prepared_acquire_release(black_box(128)));
    });
    admission_group.finish();

    let pending = PendingHotPathHarness::new();
    let mut pending_group = c.benchmark_group("pending_completion");
    apply_remoting_command_baseline_profile(&mut pending_group);
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
    apply_remoting_command_baseline_profile(&mut hooks_group);
    for hook_count in [0, 1, 4] {
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

    let mut logical_request = c.benchmark_group("hook_logical_request");
    apply_remoting_command_baseline_profile(&mut logical_request);
    for hook_count in [0, 1, 4] {
        let hooks = HookHotPathHarness::new(hook_count);
        for ext_fields in [0, 8, 32, 128] {
            let fields = (0..ext_fields)
                .map(|index| {
                    (
                        CheetahString::from_string(format!("benchmarkKey{index:03}")),
                        CheetahString::from_string(format!("benchmark-value-{index:03}")),
                    )
                })
                .collect::<HashMap<_, _>>();
            let request = if fields.is_empty() {
                RemotingCommand::create_remoting_command(10)
            } else {
                RemotingCommand::create_remoting_command(10).set_ext_fields(fields)
            };
            logical_request.bench_with_input(
                BenchmarkId::new(format!("hooks-{hook_count}"), format!("ext-{ext_fields}")),
                &request,
                |benchmark, request| {
                    benchmark.iter(|| {
                        let request = request.clone();
                        let hook_count = hooks.load_snapshot();
                        black_box((request, hook_count));
                    });
                },
            );
        }
    }
    logical_request.finish();
}

criterion_group!(benches, benchmark_admission_pending_hooks);
criterion_main!(benches);
