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

use std::hint::black_box;
use std::time::Duration;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_proxy_cluster::bench_support::run_cluster_admission_probe;
use rocketmq_proxy_cluster::bench_support::ClusterAdmissionPattern;

fn bench_cluster_executor(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("proxy_cluster_keyed_admission");
    for command_count in [256usize, 1_024] {
        for pattern in [ClusterAdmissionPattern::SameKey, ClusterAdmissionPattern::DistinctKeys] {
            group.bench_with_input(
                BenchmarkId::new(format!("{pattern:?}"), command_count),
                &(pattern, command_count),
                |bencher, &(pattern, command_count)| {
                    bencher.iter(|| {
                        let probe = run_cluster_admission_probe(black_box(command_count), pattern)
                            .expect("proxy cluster admission benchmark should complete");
                        assert_eq!(probe.drained_count, probe.command_count);
                        assert_eq!(probe.diagnostics.active_keys, 0);
                        assert_eq!(probe.diagnostics.queued_and_active, 0);
                        black_box(probe);
                    });
                },
            );
        }
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1));
    targets = bench_cluster_executor
}
criterion_main!(benches);
