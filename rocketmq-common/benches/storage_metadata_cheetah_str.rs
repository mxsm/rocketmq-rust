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
use std::net::SocketAddr;

use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use rocketmq_common::common::message::storage_metadata::StorageMetadata;

fn sample_storage_metadata() -> StorageMetadata {
    let addr: SocketAddr = "192.168.1.100:10911".parse().expect("sample address should parse");
    StorageMetadata::new(
        CheetahString::from_string(format!("benchmark-broker-{}", "x".repeat(96))),
        3,
        1024,
        4096,
        1_720_000_000_123,
        addr,
        256,
    )
}

fn bench_storage_metadata_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_metadata");
    let metadata = sample_storage_metadata();

    group.bench_function("clone", |b| b.iter(|| black_box(metadata.clone())));

    group.finish();
}

criterion_group!(benches, bench_storage_metadata_clone);
criterion_main!(benches);
