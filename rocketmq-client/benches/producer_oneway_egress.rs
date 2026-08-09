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

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use rocketmq_runtime::BudgetClass;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::ResourceBudgetTree;
use std::hint::black_box;

const PAYLOAD_BYTES: usize = 1024;

fn benchmark_oneway_reservation_transfer(criterion: &mut Criterion) {
    let root = ResourceBudgetTree::new(
        "process",
        BudgetLimit::new(65_536, 256 * 1024 * 1024, FullPolicy::Reject),
    )
    .expect("process budget")
    .root();
    let producer = root
        .child(
            "producer-egress",
            BudgetLimit::new(16_384, 64 * 1024 * 1024, FullPolicy::Reject),
        )
        .expect("producer budget");
    let transport = root
        .child(
            "transport-writer",
            BudgetLimit::new(16_384, 64 * 1024 * 1024, FullPolicy::Reject),
        )
        .expect("transport budget");

    criterion.bench_function("producer_oneway_egress/rebind_1kib", |bencher| {
        bencher.iter(|| {
            let mut permit = producer
                .try_acquire(black_box(PAYLOAD_BYTES), BudgetClass::Data)
                .expect("producer admission");
            permit.try_rebind(black_box(&transport)).expect("writer rebind");
            black_box(permit);
        });
    });
}

criterion_group!(benches, benchmark_oneway_reservation_transfer);
criterion_main!(benches);
