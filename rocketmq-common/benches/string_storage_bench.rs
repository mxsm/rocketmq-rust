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

use std::collections::HashMap;
use std::hint::black_box;

use cheetah_string::CheetahStr;
use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;

const SAMPLES: &[&str] = &[
    "T8",
    "BenchmarkTopic",
    "topic-01234567890123",
    "topic-012345678901234",
    "broker-a-long-name-with-zone-and-cluster-001",
    "AC14000100002A9F0000000000010001",
];

fn bench_string_construct(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_storage_construct");

    for sample in SAMPLES {
        group.throughput(Throughput::Bytes(sample.len() as u64));

        group.bench_with_input(BenchmarkId::new("String", sample.len()), sample, |b, value| {
            b.iter(|| String::from(black_box(*value)))
        });

        group.bench_with_input(BenchmarkId::new("CheetahString", sample.len()), sample, |b, value| {
            b.iter(|| CheetahString::from_slice(black_box(value)))
        });

        group.bench_with_input(BenchmarkId::new("CheetahStr", sample.len()), sample, |b, value| {
            b.iter(|| CheetahStr::from_slice(black_box(value)))
        });
    }

    group.finish();
}

fn bench_string_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_storage_clone");

    for sample in SAMPLES {
        let string_value = String::from(*sample);
        let cheetah_value = CheetahString::from_slice(sample);
        let cheetah_shared = CheetahStr::from_slice(sample);

        group.throughput(Throughput::Bytes(sample.len() as u64));

        group.bench_with_input(BenchmarkId::new("String", sample.len()), &string_value, |b, value| {
            b.iter(|| black_box(value.clone()))
        });

        group.bench_with_input(
            BenchmarkId::new("CheetahString", sample.len()),
            &cheetah_value,
            |b, value| b.iter(|| black_box(value.clone())),
        );

        group.bench_with_input(
            BenchmarkId::new("CheetahStr", sample.len()),
            &cheetah_shared,
            |b, value| b.iter(|| black_box(value.clone())),
        );
    }

    group.finish();
}

fn bench_cheetah_hashmap_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_storage_hashmap_lookup");

    for size in [128usize, 1_024, 16_384] {
        let mut cheetah_map = HashMap::with_capacity(size);
        let mut string_map = HashMap::with_capacity(size);

        for index in 0..size {
            let key = format!("topic-{index:06}");
            cheetah_map.insert(CheetahString::from_slice(key.as_str()), index);
            string_map.insert(key, index);
        }

        let hit = format!("topic-{:06}", size / 2);
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("CheetahString_get_str", size), &hit, |b, hit| {
            b.iter(|| black_box(cheetah_map.get(black_box(hit.as_str()))))
        });

        group.bench_with_input(BenchmarkId::new("String_get_str", size), &hit, |b, hit| {
            b.iter(|| black_box(string_map.get(black_box(hit.as_str()))))
        });

        group.bench_with_input(BenchmarkId::new("CheetahString_temp_key", size), &hit, |b, hit| {
            b.iter(|| {
                let key = CheetahString::from_slice(black_box(hit.as_str()));
                black_box(cheetah_map.get(&key))
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_string_construct,
    bench_string_clone,
    bench_cheetah_hashmap_lookup
);
criterion_main!(benches);
