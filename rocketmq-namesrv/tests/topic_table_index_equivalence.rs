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

use std::collections::HashSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_namesrv::route::tables::TopicQueueTable;
use rocketmq_remoting::protocol::route::route_data_view::QueueData;

fn create_queue_data(broker_name: &CheetahString, read_queue_nums: u32) -> QueueData {
    QueueData::new(broker_name.clone(), read_queue_nums, read_queue_nums, 6, 0)
}

fn build_fixture() -> TopicQueueTable {
    let table = TopicQueueTable::new();

    let broker_a = CheetahString::from_static_str("broker-a");
    let broker_b = CheetahString::from_static_str("broker-b");
    let broker_c = CheetahString::from_static_str("broker-c");

    table.insert(
        CheetahString::from_static_str("topic-a"),
        broker_a.clone(),
        create_queue_data(&broker_a, 8),
    );
    table.insert(
        CheetahString::from_static_str("topic-a"),
        broker_b.clone(),
        create_queue_data(&broker_b, 8),
    );
    table.insert(
        CheetahString::from_static_str("topic-b"),
        broker_a.clone(),
        create_queue_data(&broker_a, 4),
    );
    table.insert(
        CheetahString::from_static_str("topic-c"),
        broker_b.clone(),
        create_queue_data(&broker_b, 16),
    );
    table.insert(
        CheetahString::from_static_str("topic-d"),
        broker_c.clone(),
        create_queue_data(&broker_c, 32),
    );

    table
}

fn legacy_topics_for_broker(table: &TopicQueueTable, broker_name: &str) -> Vec<String> {
    let mut topics = table
        .get_all_topics()
        .into_iter()
        .filter(|topic| table.get(topic.as_str(), broker_name).is_some())
        .map(|topic| topic.to_string())
        .collect::<Vec<_>>();
    topics.sort();
    topics
}

fn legacy_topics_for_brokers_with_duplicates(
    table: &TopicQueueTable,
    broker_names: &HashSet<CheetahString>,
) -> Vec<String> {
    let mut topics = Vec::new();

    for topic in table.get_all_topics() {
        for broker_name in broker_names {
            if table.get(topic.as_str(), broker_name.as_str()).is_some() {
                topics.push(topic.to_string());
            }
        }
    }

    topics.sort();
    topics
}

fn legacy_topic_broker_pairs_for_brokers(
    table: &TopicQueueTable,
    broker_names: &HashSet<CheetahString>,
) -> Vec<(String, String)> {
    let mut pairs = Vec::new();

    for topic in table.get_all_topics() {
        for broker_name in broker_names {
            if table.get(topic.as_str(), broker_name.as_str()).is_some() {
                pairs.push((topic.to_string(), broker_name.to_string()));
            }
        }
    }

    pairs.sort();
    pairs
}

fn legacy_topic_queue_pairs_for_broker(table: &TopicQueueTable, broker_name: &str) -> Vec<(String, Arc<QueueData>)> {
    let mut pairs = table
        .get_all_topics()
        .into_iter()
        .filter_map(|topic| {
            table
                .get(topic.as_str(), broker_name)
                .map(|queue_data| (topic.to_string(), queue_data))
        })
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(&right.0));
    pairs
}

#[test]
fn indexed_topics_for_broker_matches_legacy_full_scan() {
    let table = build_fixture();

    let mut indexed = table
        .topics_for_broker("broker-a")
        .into_iter()
        .map(|topic| topic.to_string())
        .collect::<Vec<_>>();
    indexed.sort();

    assert_eq!(indexed, legacy_topics_for_broker(&table, "broker-a"));
}

#[test]
fn indexed_topics_for_brokers_with_duplicates_match_legacy_full_scan() {
    let table = build_fixture();
    let brokers = HashSet::from([
        CheetahString::from_static_str("broker-a"),
        CheetahString::from_static_str("broker-b"),
    ]);

    let mut indexed = table
        .topics_for_brokers_with_duplicates(&brokers)
        .into_iter()
        .map(|topic| topic.to_string())
        .collect::<Vec<_>>();
    indexed.sort();

    assert_eq!(indexed, legacy_topics_for_brokers_with_duplicates(&table, &brokers));
}

#[test]
fn indexed_topic_broker_pairs_match_legacy_full_scan() {
    let table = build_fixture();
    let brokers = HashSet::from([
        CheetahString::from_static_str("broker-a"),
        CheetahString::from_static_str("broker-b"),
    ]);

    let mut indexed = table
        .topic_broker_pairs_for_brokers(&brokers)
        .into_iter()
        .map(|(topic, broker)| (topic.to_string(), broker.to_string()))
        .collect::<Vec<_>>();
    indexed.sort();

    assert_eq!(indexed, legacy_topic_broker_pairs_for_brokers(&table, &brokers));
}

#[test]
fn indexed_topic_queue_pairs_match_legacy_full_scan() {
    let table = build_fixture();

    let mut indexed = table.topic_queue_pairs_for_broker("broker-b");
    indexed.sort_by(|left, right| left.0.cmp(&right.0));
    let legacy = legacy_topic_queue_pairs_for_broker(&table, "broker-b");

    assert_eq!(indexed.len(), legacy.len());
    for ((indexed_topic, indexed_queue), (legacy_topic, legacy_queue)) in indexed.iter().zip(legacy.iter()) {
        assert_eq!(indexed_topic.as_str(), legacy_topic);
        assert_eq!(indexed_queue.read_queue_nums(), legacy_queue.read_queue_nums());
        assert_eq!(indexed_queue.write_queue_nums(), legacy_queue.write_queue_nums());
    }
}
