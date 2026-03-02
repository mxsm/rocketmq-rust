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

//! Performance benchmarks for ConsumerManager optimizations
//!
//! This benchmark suite measures:
//! - query_topic_consume_by_who: Comparison of traversal vs. reverse index lookup
//! - DashMap shard optimization: Performance impact of different shard counts
//!
//! Note: do_channel_close_event benchmark is excluded due to Channel construction complexity.

use std::collections::HashSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use dashmap::DashMap;
use rocketmq_broker::bench_support::ConsumerGroupInfo;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;

/// Simulate the old O(n) traversal approach for query_topic_consume_by_who
struct OldQuerySimulation {
    consumer_table: Arc<DashMap<CheetahString, ConsumerGroupInfo>>,
}

impl OldQuerySimulation {
    fn new() -> Self {
        Self {
            consumer_table: Arc::new(DashMap::with_capacity_and_shard_amount(1024, 16)),
        }
    }

    /// Old O(n) implementation: traverse all consumer groups
    fn query_topic_consume_by_who_old(&self, topic: &CheetahString) -> HashSet<CheetahString> {
        let mut groups = HashSet::new();
        for entry in self.consumer_table.iter() {
            let group = entry.key();
            let consumer_group_info = entry.value();
            if consumer_group_info.find_subscription_data(topic).is_some() {
                groups.insert(group.clone());
            }
        }
        groups
    }
}

/// Simulate the new O(1) lookup approach
struct NewQuerySimulation {
    topic_group_table: Arc<DashMap<CheetahString, HashSet<CheetahString>>>,
}

impl NewQuerySimulation {
    fn new() -> Self {
        Self {
            topic_group_table: Arc::new(DashMap::with_capacity_and_shard_amount(1024, 64)),
        }
    }

    /// New O(1) implementation: direct lookup
    fn query_topic_consume_by_who_new(&self, topic: &CheetahString) -> HashSet<CheetahString> {
        self.topic_group_table
            .get(topic)
            .map(|groups| groups.clone())
            .unwrap_or_default()
    }
}

/// Setup test data for old O(n) simulation
fn setup_old_simulation(num_groups: usize, num_topics_per_group: usize) -> OldQuerySimulation {
    let simulation = OldQuerySimulation::new();

    for group_idx in 0..num_groups {
        let group = CheetahString::from_string(format!("GROUP_{}", group_idx));
        let mut consumer_group_info = ConsumerGroupInfo::new(
            group.clone(),
            ConsumeType::ConsumePassively,
            MessageModel::Clustering,
            ConsumeFromWhere::ConsumeFromLastOffset,
        );

        for topic_idx in 0..num_topics_per_group {
            let topic = CheetahString::from_string(format!("TOPIC_{}", topic_idx));
            let sub_data = SubscriptionData {
                topic,
                sub_string: CheetahString::from_static_str("*"),
                ..Default::default()
            };
            let mut sub_set = HashSet::new();
            sub_set.insert(sub_data);
            consumer_group_info.update_subscription(&sub_set);
        }

        simulation.consumer_table.insert(group, consumer_group_info);
    }

    simulation
}

/// Setup test data for new O(1) simulation
fn setup_new_simulation(num_groups: usize, num_topics_per_group: usize) -> NewQuerySimulation {
    let simulation = NewQuerySimulation::new();

    for group_idx in 0..num_groups {
        let group = CheetahString::from_string(format!("GROUP_{}", group_idx));

        for topic_idx in 0..num_topics_per_group {
            let topic = CheetahString::from_string(format!("TOPIC_{}", topic_idx));
            simulation
                .topic_group_table
                .entry(topic)
                .or_default()
                .insert(group.clone());
        }
    }

    simulation
}

/// Benchmark: query_topic_consume_by_who (O(n) vs O(1))
fn bench_query_topic_consume_by_who(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_topic_consume_by_who");

    for num_groups in [10, 50, 100, 500, 1000, 2000, 5000].iter() {
        let num_topics = 10;

        // Benchmark old O(n) approach
        group.throughput(Throughput::Elements(*num_groups as u64));
        group.bench_with_input(
            BenchmarkId::new("old_O(n)_traversal", num_groups),
            num_groups,
            |b, &size| {
                let simulation = setup_old_simulation(size, num_topics);
                let topic = CheetahString::from_static_str("TOPIC_5");
                b.iter(|| simulation.query_topic_consume_by_who_old(&topic));
            },
        );

        // Benchmark new O(1) approach
        group.throughput(Throughput::Elements(*num_groups as u64));
        group.bench_with_input(
            BenchmarkId::new("new_O(1)_lookup", num_groups),
            num_groups,
            |b, &size| {
                let simulation = setup_new_simulation(size, num_topics);
                let topic = CheetahString::from_static_str("TOPIC_5");
                b.iter(|| simulation.query_topic_consume_by_who_new(&topic));
            },
        );
    }

    group.finish();
}

/// Benchmark: Topic-group table insertion performance
fn bench_topic_group_table_insertion(c: &mut Criterion) {
    let mut group = c.benchmark_group("topic_group_table_insertion");

    for num_operations in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*num_operations as u64));
        group.bench_with_input(
            BenchmarkId::new("insertion", num_operations),
            num_operations,
            |b, &size| {
                b.iter(|| {
                    let table: DashMap<CheetahString, HashSet<CheetahString>> =
                        DashMap::with_capacity_and_shard_amount(1024, 64);

                    for i in 0..size {
                        let topic = CheetahString::from_string(format!("TOPIC_{}", i % 10));
                        let group = CheetahString::from_string(format!("GROUP_{}", i));
                        table.entry(topic).or_default().insert(group);
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: DashMap shard count comparison (16 vs 64 shards)
fn bench_dashmap_shard_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("dashmap_shard_comparison");

    let num_items = 10000;
    let num_operations = 10000;

    // Benchmark 16 shards (old)
    group.throughput(Throughput::Elements(num_operations as u64));
    group.bench_function("16_shards", |b| {
        let map: DashMap<CheetahString, CheetahString> = DashMap::with_capacity_and_shard_amount(num_items, 16);

        // Pre-populate
        for i in 0..num_items {
            let key = CheetahString::from_string(format!("KEY_{}", i));
            let value = CheetahString::from_string(format!("VALUE_{}", i));
            map.insert(key, value);
        }

        b.iter(|| {
            for i in 0..num_operations {
                let key = CheetahString::from_string(format!("KEY_{}", i % num_items));
                let _ = map.get(&key);
            }
        });
    });

    // Benchmark 64 shards (new)
    group.throughput(Throughput::Elements(num_operations as u64));
    group.bench_function("64_shards", |b| {
        let map: DashMap<CheetahString, CheetahString> = DashMap::with_capacity_and_shard_amount(num_items, 64);

        // Pre-populate
        for i in 0..num_items {
            let key = CheetahString::from_string(format!("KEY_{}", i));
            let value = CheetahString::from_string(format!("VALUE_{}", i));
            map.insert(key, value);
        }

        b.iter(|| {
            for i in 0..num_operations {
                let key = CheetahString::from_string(format!("KEY_{}", i % num_items));
                let _ = map.get(&key);
            }
        });
    });

    group.finish();
}

criterion_group! {
    name = consumer_manager_benches;
    config = Criterion::default()
        .sample_size(50);
    targets =
        bench_query_topic_consume_by_who,
        bench_topic_group_table_insertion,
        bench_dashmap_shard_comparison
}

criterion_main!(consumer_manager_benches);
