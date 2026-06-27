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

use cheetah_string::CheetahBuilder;
use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_client_rust::utils::message_util::MessageUtil;
use rocketmq_common::common::message::message_decoder::NAME_VALUE_SEPARATOR;
use rocketmq_common::common::message::message_decoder::PROPERTY_SEPARATOR;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::MessageDecoder;

// ============================================================================
// Helper functions to create test messages
// ============================================================================

fn create_test_message_with_all_properties() -> Message {
    let mut msg = Message::default();
    MessageAccessor::put_property(
        &mut msg,
        CheetahString::from_static_str(MessageConst::PROPERTY_CLUSTER),
        CheetahString::from_static_str("DefaultCluster"),
    );
    MessageAccessor::put_property(
        &mut msg,
        CheetahString::from_static_str(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT),
        CheetahString::from_static_str("client-123"),
    );
    MessageAccessor::put_property(
        &mut msg,
        CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID),
        CheetahString::from_static_str("correlation-456"),
    );
    MessageAccessor::put_property(
        &mut msg,
        CheetahString::from_static_str(MessageConst::PROPERTY_MESSAGE_TTL),
        CheetahString::from_static_str("60000"),
    );
    msg
}

fn create_test_message_without_cluster() -> Message {
    let mut msg = Message::default();
    MessageAccessor::put_property(
        &mut msg,
        CheetahString::from_static_str(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT),
        CheetahString::from_static_str("client-123"),
    );
    msg
}

fn create_test_message_minimal() -> Message {
    let mut msg = Message::default();
    MessageAccessor::put_property(
        &mut msg,
        CheetahString::from_static_str(MessageConst::PROPERTY_CLUSTER),
        CheetahString::from_static_str("DefaultCluster"),
    );
    msg
}

fn create_properties(count: usize) -> HashMap<CheetahString, CheetahString> {
    (0..count)
        .map(|index| {
            (
                CheetahString::from_string(format!("property-key-{index}")),
                CheetahString::from_string(format!("property-value-{index}")),
            )
        })
        .collect()
}

fn legacy_message_properties_to_string(properties: &HashMap<CheetahString, CheetahString>) -> CheetahString {
    let mut len = 0;
    for (name, value) in properties.iter() {
        len += name.len();
        len += value.len();
        len += 2;
    }

    let mut builder = CheetahBuilder::with_capacity(len);
    for (name, value) in properties.iter() {
        builder.push_str(name.as_str());
        builder.push(NAME_VALUE_SEPARATOR);
        builder.push_str(value.as_str());
        builder.push(PROPERTY_SEPARATOR);
    }
    builder.finish_string()
}

// ============================================================================
// Benchmark: create_reply_message - Success Path (All Properties)
// ============================================================================

fn bench_create_reply_message_success_full(c: &mut Criterion) {
    let mut group = c.benchmark_group("create_reply_message_success_full");
    group.throughput(Throughput::Elements(1));

    let request_message = create_test_message_with_all_properties();
    let body = b"test response body";

    group.bench_function("with_all_properties", |b| {
        b.iter(|| {
            let result = MessageUtil::create_reply_message(&request_message, body);
            assert!(result.is_ok());
            black_box(result)
        });
    });

    group.finish();
}

// ============================================================================
// Benchmark: create_reply_message - Success Path (Minimal Properties)
// ============================================================================

fn bench_create_reply_message_success_minimal(c: &mut Criterion) {
    let mut group = c.benchmark_group("create_reply_message_success_minimal");
    group.throughput(Throughput::Elements(1));

    let request_message = create_test_message_minimal();
    let body = b"test response body";

    group.bench_function("with_minimal_properties", |b| {
        b.iter(|| {
            let result = MessageUtil::create_reply_message(&request_message, body);
            assert!(result.is_ok());
            black_box(result)
        });
    });

    group.finish();
}

// ============================================================================
// Benchmark: create_reply_message - Failure Path (Missing Cluster)
// ============================================================================

fn bench_create_reply_message_failure(c: &mut Criterion) {
    let mut group = c.benchmark_group("create_reply_message_failure");
    group.throughput(Throughput::Elements(1));

    let request_message = create_test_message_without_cluster();
    let body = b"test response body";

    group.bench_function("missing_cluster_early_return", |b| {
        b.iter(|| {
            let result = MessageUtil::create_reply_message(&request_message, body);
            assert!(result.is_err());
            black_box(result)
        });
    });

    group.finish();
}

// ============================================================================
// Benchmark: get_reply_to_client
// ============================================================================

fn bench_get_reply_to_client(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_reply_to_client");
    group.throughput(Throughput::Elements(1));

    let message_with_property = create_test_message_with_all_properties();
    let message_without_property = create_test_message_minimal();

    group.bench_function("with_property", |b| {
        b.iter(|| {
            let result = MessageUtil::get_reply_to_client(&message_with_property);
            assert!(result.is_some());
            black_box(result)
        });
    });

    group.bench_function("without_property", |b| {
        b.iter(|| {
            let result = MessageUtil::get_reply_to_client(&message_without_property);
            assert!(result.is_none());
            black_box(result)
        });
    });

    group.finish();
}

// ============================================================================
// Benchmark: Throughput Test - High Concurrency Simulation
// ============================================================================

fn bench_high_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("high_throughput");

    for batch_size in [10, 100, 1000, 10000] {
        group.throughput(Throughput::Elements(batch_size));

        let request_message = create_test_message_with_all_properties();
        let body = b"test response body";

        group.bench_with_input(BenchmarkId::from_parameter(batch_size), &batch_size, |b, &size| {
            b.iter(|| {
                for _ in 0..size {
                    let _ = black_box(MessageUtil::create_reply_message(&request_message, body));
                }
            });
        });
    }

    group.finish();
}

// ============================================================================
// Benchmark: Memory Allocation Comparison
// ============================================================================

fn bench_memory_pattern(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_pattern");
    group.throughput(Throughput::Elements(1000));

    let request_message = create_test_message_with_all_properties();
    let body = b"test response body";

    group.bench_function("repeated_calls", |b| {
        b.iter(|| {
            // Simulate high-frequency calls in production
            for _ in 0..1000 {
                let _ = black_box(MessageUtil::create_reply_message(&request_message, body));
            }
        });
    });

    group.finish();
}

// ============================================================================
// Benchmark: Static String Cache Hit Rate
// ============================================================================

fn bench_static_cache_effectiveness(c: &mut Criterion) {
    let mut group = c.benchmark_group("static_cache_effectiveness");

    // Create messages with different property combinations
    let messages = vec![
        create_test_message_with_all_properties(),
        create_test_message_minimal(),
        create_test_message_with_all_properties(),
        create_test_message_minimal(),
    ];

    let body = b"test response body";

    group.bench_function("mixed_property_patterns", |b| {
        b.iter(|| {
            for msg in &messages {
                let _ = black_box(MessageUtil::create_reply_message(msg, body));
            }
        });
    });

    group.finish();
}

// ============================================================================
// Benchmark: message_properties_to_string
// ============================================================================

fn bench_message_properties_to_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_properties_to_string");

    for property_count in [0usize, 1, 4, 16] {
        group.throughput(Throughput::Elements(property_count.max(1) as u64));
        let properties = create_properties(property_count);
        group.bench_with_input(
            BenchmarkId::new("legacy", property_count),
            &properties,
            |b, properties| {
                b.iter(|| black_box(legacy_message_properties_to_string(black_box(properties))));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("optimized", property_count),
            &properties,
            |b, properties| {
                b.iter(|| black_box(MessageDecoder::message_properties_to_string(black_box(properties))));
            },
        );
    }

    group.finish();
}

// ============================================================================
// Register all benchmarks
// ============================================================================

criterion_group!(
    benches,
    bench_create_reply_message_success_full,
    bench_create_reply_message_success_minimal,
    bench_create_reply_message_failure,
    bench_get_reply_to_client,
    bench_high_throughput,
    bench_memory_pattern,
    bench_static_cache_effectiveness,
    bench_message_properties_to_string,
);

criterion_main!(benches);
