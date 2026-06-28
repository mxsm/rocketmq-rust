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

//! Broker-free consumer cache pipeline baselines.
//!
//! These benchmarks exercise ProcessQueue hot operations through hidden probe
//! functions so later storage and lock optimizations have a stable baseline.

use std::hint::black_box;
use std::time::Duration;
use std::time::Instant;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_client_rust::consumer::consumer_impl::process_queue::run_process_queue_has_temp_message_probe;
use rocketmq_client_rust::consumer::consumer_impl::process_queue::run_process_queue_max_span_only_probe;
use rocketmq_client_rust::consumer::consumer_impl::process_queue::run_process_queue_put_probe;
use rocketmq_client_rust::consumer::consumer_impl::process_queue::run_process_queue_remove_probe;
use rocketmq_client_rust::consumer::consumer_impl::process_queue::run_process_queue_take_probe;
use rocketmq_client_rust::consumer::consumer_impl::process_queue::ProcessQueueOperationFixture;

fn bench_process_queue_put(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime should start");
    let mut group = c.benchmark_group("client_consume_pipeline/process_queue_put");

    for message_count in [32usize, 256, 1024] {
        group.throughput(Throughput::Elements(message_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(message_count),
            &message_count,
            |b, &message_count| {
                b.to_async(&runtime).iter_custom(|iters| async move {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let fixture = ProcessQueueOperationFixture::new(message_count, 1024);
                        let started_at = Instant::now();
                        black_box(run_process_queue_put_probe(fixture).await);
                        elapsed += started_at.elapsed();
                    }
                    elapsed
                });
            },
        );
    }

    group.finish();
}

fn bench_process_queue_take(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime should start");
    let mut group = c.benchmark_group("client_consume_pipeline/process_queue_take");

    for message_count in [32usize, 256, 1024] {
        group.throughput(Throughput::Elements(message_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(message_count),
            &message_count,
            |b, &message_count| {
                b.to_async(&runtime).iter_custom(|iters| async move {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let fixture = ProcessQueueOperationFixture::seeded(message_count, 1024).await;
                        let started_at = Instant::now();
                        black_box(run_process_queue_take_probe(fixture).await);
                        elapsed += started_at.elapsed();
                    }
                    elapsed
                });
            },
        );
    }

    group.finish();
}

fn bench_process_queue_remove(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime should start");
    let mut group = c.benchmark_group("client_consume_pipeline/process_queue_remove");

    for message_count in [32usize, 256, 1024] {
        group.throughput(Throughput::Elements(message_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(message_count),
            &message_count,
            |b, &message_count| {
                b.to_async(&runtime).iter_custom(|iters| async move {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let fixture = ProcessQueueOperationFixture::seeded(message_count, 1024).await;
                        let started_at = Instant::now();
                        black_box(run_process_queue_remove_probe(fixture).await);
                        elapsed += started_at.elapsed();
                    }
                    elapsed
                });
            },
        );
    }

    group.finish();
}

fn bench_process_queue_max_span(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime should start");
    let mut group = c.benchmark_group("client_consume_pipeline/process_queue_max_span");

    for message_count in [32usize, 256, 1024] {
        group.throughput(Throughput::Elements(message_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(message_count),
            &message_count,
            |b, &message_count| {
                b.to_async(&runtime).iter_custom(|iters| async move {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let fixture = ProcessQueueOperationFixture::seeded(message_count, 1024).await;
                        let started_at = Instant::now();
                        black_box(run_process_queue_max_span_only_probe(&fixture).await);
                        elapsed += started_at.elapsed();
                    }
                    elapsed
                });
            },
        );
    }

    group.finish();
}

fn bench_process_queue_has_temp_message(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime should start");
    let mut group = c.benchmark_group("client_consume_pipeline/process_queue_has_temp_message");

    for message_count in [0usize, 32, 256, 1024] {
        group.throughput(Throughput::Elements(message_count.max(1) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(message_count),
            &message_count,
            |b, &message_count| {
                b.to_async(&runtime).iter_custom(|iters| async move {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let fixture = if message_count == 0 {
                            ProcessQueueOperationFixture::new(0, 1024)
                        } else {
                            ProcessQueueOperationFixture::seeded(message_count, 1024).await
                        };
                        let started_at = Instant::now();
                        black_box(run_process_queue_has_temp_message_probe(&fixture).await);
                        elapsed += started_at.elapsed();
                    }
                    elapsed
                });
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1));
    targets = bench_process_queue_put,
        bench_process_queue_take,
        bench_process_queue_remove,
        bench_process_queue_max_span,
        bench_process_queue_has_temp_message
}
criterion_main!(benches);
