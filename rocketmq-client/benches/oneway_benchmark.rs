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

//! Performance benchmarks for send_oneway optimization (P0 + P1)
//!
//! Run with:
//! ```bash
//! cargo bench --bench oneway_benchmark
//! ```

use bytes::Bytes;
use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_single::Message;

/// Benchmark single send_oneway call latency
///
/// Target: < 10μs per call
/// Measures: Time from call to return (not including background send)
fn bench_send_oneway_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("send_oneway_latency", |b| {
        b.to_async(&rt).iter(|| async {
            let mut producer = DefaultMQProducer::builder()
                .producer_group("bench_group".to_string())
                .name_server_addr("127.0.0.1:9876".to_string())
                .build();

            let msg = Message::builder()
                .topic(CheetahString::from_static_str("BenchTopic"))
                .tags(CheetahString::from_static_str("BenchTag"))
                .body(b"BenchBody".to_vec())
                .build()
                .unwrap();

            let _ = producer.send_oneway(msg).await;
        });
    });
}

/// Benchmark send_oneway_batch throughput
///
/// Target: 100K+ messages/second
/// Measures: Messages spawned per second
///
/// Note: send_oneway_batch is on DefaultMQProducerImpl, not directly accessible
/// from DefaultMQProducer in benches. This test is currently disabled.
#[allow(dead_code)]
fn bench_send_oneway_batch_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_throughput");
    let rt = tokio::runtime::Runtime::new().unwrap();

    for batch_size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::new("messages", batch_size), batch_size, |b, &size| {
            let _messages: Vec<Message> = (0..size)
                .map(|i| {
                    Message::builder()
                        .topic(CheetahString::from_static_str("BenchTopic"))
                        .tags(CheetahString::from_string(format!("Tag{}", i)))
                        .body(format!("Body{}", i).as_bytes().to_vec())
                        .build()
                        .unwrap()
                })
                .collect();

            b.to_async(&rt).iter(|| async {
                let producer = DefaultMQProducer::builder()
                    .producer_group("bench_group".to_string())
                    .name_server_addr("127.0.0.1:9876".to_string())
                    .build();

                // send_oneway_batch is on impl, not accessible here
                // let messages_clone = messages.clone();
                // let _ = producer.send_oneway_batch(messages_clone).await;
                drop(producer);
            });
        });
    }

    group.finish();
}

/// Benchmark zero-copy overhead
///
/// Measures: Bytes::clone() performance vs deep copy
fn bench_zero_copy(c: &mut Criterion) {
    let data = Bytes::from(&b"Test message body data"[..]);

    c.bench_function("bytes_clone_reference_count", |b| {
        b.iter(|| {
            let _ = std::hint::black_box(data.clone());
        });
    });

    // Compare with deep copy
    let data_vec = data.to_vec();

    c.bench_function("bytes_deep_copy", |b| {
        b.iter(|| {
            let _ = std::hint::black_box(Bytes::from(data_vec.clone()));
        });
    });
}

/// Benchmark concurrent oneway sends
///
/// Target: Scales linearly with concurrent tasks
/// Measures: Performance under concurrent load
fn bench_concurrent_oneway(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent");
    let rt = tokio::runtime::Runtime::new().unwrap();

    for concurrency in [1, 10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("tasks", concurrency), concurrency, |b, &conc| {
            b.to_async(&rt).iter(|| async move {
                let mut handles = vec![];

                for i in 0..conc {
                    let handle = tokio::spawn(async move {
                        let mut producer = DefaultMQProducer::builder()
                            .producer_group("bench_group".to_string())
                            .name_server_addr("127.0.0.1:9876".to_string())
                            .build();

                        let msg = Message::builder()
                            .topic(CheetahString::from_static_str("BenchTopic"))
                            .tags(CheetahString::from_string(format!("Tag{}", i)))
                            .body(format!("Body{}", i).as_bytes().to_vec())
                            .build()
                            .unwrap();

                        let _ = producer.send_oneway(msg).await;
                    });
                    handles.push(handle);
                }

                for handle in handles {
                    let _ = handle.await;
                }
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_send_oneway_latency,
    // bench_send_oneway_batch_throughput, // Disabled: requires access to DefaultMQProducerImpl
    bench_zero_copy,
    bench_concurrent_oneway
);
criterion_main!(benches);
