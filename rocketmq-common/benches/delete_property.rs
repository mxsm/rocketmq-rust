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

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_common::MessageUtils::build_message_id;
use rocketmq_common::MessageUtils::delete_property;
use rocketmq_common::MessageUtils::delete_property_v2;

fn bench_delete_property(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete_property");

    // Small property string
    let small = "key1\u{0001}value1\u{0002}key2\u{0001}value2\u{0002}key3\u{0001}value3";
    group.bench_function("small_string", |b| {
        b.iter(|| delete_property(black_box(small), black_box("key2")))
    });

    // Medium property string
    let medium = (0..10)
        .map(|i| format!("key{}\u{0001}value{}\u{0002}", i, i))
        .collect::<String>();
    group.bench_function("medium_string", |b| {
        b.iter(|| delete_property(black_box(&medium), black_box("key5")))
    });

    // Large property string
    let large = (0..100)
        .map(|i| format!("key{}\u{0001}value{}\u{0002}", i, i))
        .collect::<String>();
    group.bench_function("large_string", |b| {
        b.iter(|| delete_property(black_box(&large), black_box("key50")))
    });

    // Non-existent key
    group.bench_function("non_existent_key", |b| {
        b.iter(|| delete_property(black_box(small), black_box("nonexistent")))
    });

    group.finish();
}

fn bench_delete_property_v2(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete_property_v2");

    let small = "key1\u{0001}value1\u{0002}key2\u{0001}value2\u{0002}key3\u{0001}value3";
    group.bench_function("small_string", |b| {
        b.iter(|| delete_property_v2(black_box(small), black_box("key2")))
    });

    let medium = (0..10)
        .map(|i| format!("key{}\u{0001}value{}\u{0002}", i, i))
        .collect::<String>();
    group.bench_function("medium_string", |b| {
        b.iter(|| delete_property_v2(black_box(&medium), black_box("key5")))
    });

    let large = (0..100)
        .map(|i| format!("key{}\u{0001}value{}\u{0002}", i, i))
        .collect::<String>();
    group.bench_function("large_string", |b| {
        b.iter(|| delete_property_v2(black_box(&large), black_box("key50")))
    });

    group.finish();
}

fn bench_delete_property_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete_property_comparison");

    let test_data = (0..50)
        .map(|i| format!("key{}\u{0001}value{}\u{0002}", i, i))
        .collect::<String>();

    group.bench_with_input(BenchmarkId::new("v1", "50_keys"), &test_data, |b, data| {
        b.iter(|| delete_property(black_box(data), black_box("key25")))
    });

    group.bench_with_input(BenchmarkId::new("v2", "50_keys"), &test_data, |b, data| {
        b.iter(|| delete_property_v2(black_box(data), black_box("key25")))
    });

    group.finish();
}

fn bench_build_message_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("build_message_id");

    let addr_v4: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    group.bench_function("ipv4", |b| {
        b.iter(|| build_message_id(black_box(addr_v4), black_box(123456789)))
    });

    let addr_v6: SocketAddr = "[::1]:8080".parse().unwrap();
    group.bench_function("ipv6", |b| {
        b.iter(|| build_message_id(black_box(addr_v6), black_box(123456789)))
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_delete_property,
    bench_delete_property_v2,
    bench_build_message_id,
    bench_delete_property_comparison
);
criterion_main!(benches);
