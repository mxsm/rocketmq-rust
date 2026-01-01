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

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_common::common::constant::PermName;

fn bench_perm2string(c: &mut Criterion) {
    let mut group = c.benchmark_group("perm2string");

    // Test with different permission combinations
    let test_cases = vec![
        (0, "no_permissions"),
        (PermName::PERM_READ, "read_only"),
        (PermName::PERM_WRITE, "write_only"),
        (PermName::PERM_INHERIT, "inherit_only"),
        (PermName::PERM_READ | PermName::PERM_WRITE, "read_write"),
        (
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_INHERIT,
            "all_permissions",
        ),
        (
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_PRIORITY,
            "with_priority",
        ),
        (
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_INHERIT | PermName::PERM_PRIORITY,
            "all_flags",
        ),
    ];

    for (perm, name) in test_cases {
        group.bench_with_input(BenchmarkId::new("v1", name), &perm, |b, &p| {
            b.iter(|| PermName::perm2string(black_box(p)))
        });
    }

    group.finish();
}

fn bench_perm_to_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("perm_to_string");

    let test_cases = vec![
        (0, "no_permissions"),
        (PermName::PERM_READ, "read_only"),
        (PermName::PERM_WRITE, "write_only"),
        (PermName::PERM_INHERIT, "inherit_only"),
        (PermName::PERM_READ | PermName::PERM_WRITE, "read_write"),
        (
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_INHERIT,
            "all_permissions",
        ),
        (
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_PRIORITY,
            "with_priority",
        ),
        (
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_INHERIT | PermName::PERM_PRIORITY,
            "all_flags",
        ),
    ];

    for (perm, name) in test_cases {
        group.bench_with_input(BenchmarkId::new("v2", name), &perm, |b, &p| {
            b.iter(|| PermName::perm_to_string(black_box(p)))
        });
    }

    group.finish();
}

criterion_group!(benches, bench_perm2string, bench_perm_to_string,);
criterion_main!(benches);
