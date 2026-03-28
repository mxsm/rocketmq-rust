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

//! Manual benchmark for Phase 4 topic-table hot paths.
//!
//! This compares the old full-table scan style against the current broker-topic
//! reverse index helpers added in Phase 4.
//!
//! Run with:
//! `cargo bench -p rocketmq-namesrv --bench topic_table_hot_path_bench`

use std::collections::HashSet;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_namesrv::route::tables::TopicQueueTable;
use rocketmq_remoting::protocol::route::route_data_view::QueueData;

const TOPIC_COUNT: usize = 20_000;
const BROKER_COUNT: usize = 128;
const BROKERS_PER_TOPIC: usize = 4;
const CLUSTER_BROKER_SET_SIZE: usize = 16;
const ITERATIONS: usize = 30;

fn create_queue_data(broker_name: &CheetahString, queue_count: u32) -> QueueData {
    QueueData::new(broker_name.clone(), queue_count, queue_count, 6, 0)
}

fn build_fixture() -> (TopicQueueTable, CheetahString, HashSet<CheetahString>) {
    let table = TopicQueueTable::with_capacity(TOPIC_COUNT);
    let single_broker = CheetahString::from_string(format!("broker-{:03}", 7));
    let cluster_brokers = (0..CLUSTER_BROKER_SET_SIZE)
        .map(|index| CheetahString::from_string(format!("broker-{index:03}")))
        .collect::<HashSet<_>>();

    for topic_index in 0..TOPIC_COUNT {
        let topic = CheetahString::from_string(format!("topic-{topic_index:05}"));
        for broker_offset in 0..BROKERS_PER_TOPIC {
            let broker_index = (topic_index + broker_offset) % BROKER_COUNT;
            let broker_name = CheetahString::from_string(format!("broker-{broker_index:03}"));
            let queue_count = (broker_index % 8 + 1) as u32;
            table.insert(
                topic.clone(),
                broker_name.clone(),
                create_queue_data(&broker_name, queue_count),
            );
        }
    }

    (table, single_broker, cluster_brokers)
}

fn legacy_topics_for_broker(table: &TopicQueueTable, broker_name: &str) -> usize {
    table
        .get_all_topics()
        .into_iter()
        .filter(|topic| table.get(topic.as_str(), broker_name).is_some())
        .count()
}

fn legacy_topics_for_brokers_with_duplicates(table: &TopicQueueTable, broker_names: &HashSet<CheetahString>) -> usize {
    let mut topics = 0usize;

    for topic in table.get_all_topics() {
        for broker_name in broker_names {
            if table.get(topic.as_str(), broker_name.as_str()).is_some() {
                topics += 1;
            }
        }
    }

    topics
}

fn legacy_topic_broker_pairs_for_brokers(table: &TopicQueueTable, broker_names: &HashSet<CheetahString>) -> usize {
    let mut pairs = 0usize;

    for topic in table.get_all_topics() {
        for broker_name in broker_names {
            if table.get(topic.as_str(), broker_name.as_str()).is_some() {
                pairs += 1;
            }
        }
    }

    pairs
}

fn legacy_topic_queue_pairs_for_broker(table: &TopicQueueTable, broker_name: &str) -> usize {
    table
        .get_all_topics()
        .into_iter()
        .filter_map(|topic| table.get(topic.as_str(), broker_name))
        .count()
}

fn benchmark_operation<F>(iterations: usize, mut operation: F) -> Duration
where
    F: FnMut() -> usize,
{
    let start = Instant::now();
    for _ in 0..iterations {
        black_box(operation());
    }
    start.elapsed()
}

fn print_result(label: &str, legacy: Duration, optimized: Duration) {
    let legacy_ms = legacy.as_secs_f64() * 1000.0;
    let optimized_ms = optimized.as_secs_f64() * 1000.0;
    let speedup = legacy.as_secs_f64() / optimized.as_secs_f64();

    println!("{label}: legacy={legacy_ms:.2}ms optimized={optimized_ms:.2}ms speedup={speedup:.2}x");
}

fn main() {
    let (table, single_broker, cluster_brokers) = build_fixture();

    println!("Phase 4 topic-table hotspot benchmark");
    println!(
        "fixture: topics={TOPIC_COUNT} brokers={BROKER_COUNT} brokers_per_topic={BROKERS_PER_TOPIC} \
         iterations={ITERATIONS}"
    );

    let legacy_topics_for_broker_duration =
        benchmark_operation(ITERATIONS, || legacy_topics_for_broker(&table, single_broker.as_str()));
    let indexed_topics_for_broker_duration =
        benchmark_operation(ITERATIONS, || table.topics_for_broker(single_broker.as_str()).len());
    print_result(
        "topics_for_broker",
        legacy_topics_for_broker_duration,
        indexed_topics_for_broker_duration,
    );

    let legacy_topics_by_cluster_duration = benchmark_operation(ITERATIONS, || {
        legacy_topics_for_brokers_with_duplicates(&table, &cluster_brokers)
    });
    let indexed_topics_by_cluster_duration = benchmark_operation(ITERATIONS, || {
        table.topics_for_brokers_with_duplicates(&cluster_brokers).len()
    });
    print_result(
        "topics_for_brokers_with_duplicates",
        legacy_topics_by_cluster_duration,
        indexed_topics_by_cluster_duration,
    );

    let legacy_topic_broker_pairs_duration = benchmark_operation(ITERATIONS, || {
        legacy_topic_broker_pairs_for_brokers(&table, &cluster_brokers)
    });
    let indexed_topic_broker_pairs_duration = benchmark_operation(ITERATIONS, || {
        table.topic_broker_pairs_for_brokers(&cluster_brokers).len()
    });
    print_result(
        "topic_broker_pairs_for_brokers",
        legacy_topic_broker_pairs_duration,
        indexed_topic_broker_pairs_duration,
    );

    let legacy_topic_queue_pairs_duration = benchmark_operation(ITERATIONS, || {
        legacy_topic_queue_pairs_for_broker(&table, single_broker.as_str())
    });
    let indexed_topic_queue_pairs_duration = benchmark_operation(ITERATIONS, || {
        table.topic_queue_pairs_for_broker(single_broker.as_str()).len()
    });
    print_result(
        "topic_queue_pairs_for_broker",
        legacy_topic_queue_pairs_duration,
        indexed_topic_queue_pairs_duration,
    );

    let queue_pairs = table.topic_queue_pairs_for_broker(single_broker.as_str());
    let retained_queue_bytes = queue_pairs.iter().fold(0usize, |sum, (_, queue_data)| {
        sum + std::mem::size_of_val(queue_data.as_ref())
    });
    black_box(Arc::new(queue_pairs));
    println!("sample_queue_bytes={retained_queue_bytes}");
}
