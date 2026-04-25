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

//! StoreStatsService hot-path and runtime-info performance baselines.
//!
//! Run with:
//! `cargo bench -p rocketmq-store --bench store_stats_service`

use std::hint::black_box;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_store::base::store_stats_service::StoreStatsService;

fn bench_hot_path_updates(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_stats_service_hot_path");

    for topic_count in [1usize, 16, 128] {
        let stats = StoreStatsService::new(None);
        let topics = (0..topic_count)
            .map(|index| format!("topic-{index}"))
            .collect::<Vec<_>>();

        group.bench_with_input(
            BenchmarkId::new("put_topic_times_and_size", topic_count),
            &topics,
            |b, topics| {
                let mut index = 0usize;
                b.iter(|| {
                    let topic = &topics[index % topics.len()];
                    stats.add_single_put_message_topic_times_total(black_box(topic), 1);
                    stats.add_single_put_message_topic_size_total(black_box(topic), 128);
                    index = index.wrapping_add(1);
                });
            },
        );
    }

    let stats = StoreStatsService::new(None);
    group.bench_function("put_latency_record", |b| {
        let mut value = 0u64;
        b.iter(|| {
            stats.set_put_message_entire_time_max(black_box(value % 10_000));
            value = value.wrapping_add(1);
        });
    });

    group.finish();
}

fn bench_runtime_info(c: &mut Criterion) {
    let stats = StoreStatsService::new(None);
    for topic_index in 0..256 {
        let topic = format!("topic-{topic_index}");
        stats.add_single_put_message_topic_times_total(&topic, 10);
        stats.add_single_put_message_topic_size_total(&topic, 1024);
    }
    for value in 1..=1_000 {
        stats.set_put_message_entire_time_max(value);
    }

    c.bench_function("store_stats_service_runtime_info_256_topics", |b| {
        b.iter(|| black_box(stats.get_runtime_info()));
    });
}

criterion_group!(benches, bench_hot_path_updates, bench_runtime_info);
criterion_main!(benches);
