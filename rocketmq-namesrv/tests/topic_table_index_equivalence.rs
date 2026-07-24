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

use std::collections::BTreeSet;
use std::collections::HashSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_namesrv::route::tables::TopicQueueTable;
use rocketmq_protocol::protocol::route::route_data_view::QueueData;

fn queue_data(broker_name: &str) -> QueueData {
    QueueData::new(CheetahString::from(broker_name), 4, 4, 6, 0)
}

fn topics_from_full_scan(table: &TopicQueueTable, broker_name: &str) -> BTreeSet<String> {
    table
        .iter_all_with_data()
        .into_iter()
        .filter_map(|(topic, queues)| {
            queues
                .iter()
                .any(|queue| queue.broker_name().as_str() == broker_name)
                .then_some(topic)
        })
        .collect()
}

fn topics_from_reverse_index(table: &TopicQueueTable, broker_name: &str) -> BTreeSet<String> {
    table
        .topics_for_broker(broker_name)
        .into_iter()
        .map(|topic| topic.to_string())
        .collect()
}

fn assert_reverse_index_equivalent(table: &TopicQueueTable, broker_names: &[&str]) {
    for broker_name in broker_names {
        assert_eq!(
            topics_from_reverse_index(table, broker_name),
            topics_from_full_scan(table, broker_name),
            "reverse topic index diverged for broker {broker_name}"
        );
    }
}

fn broker_pairs_from_full_scan(
    table: &TopicQueueTable,
    broker_names: &HashSet<CheetahString>,
) -> BTreeSet<(String, String)> {
    table
        .iter_all_with_data()
        .into_iter()
        .flat_map(|(topic, queues)| {
            queues
                .into_iter()
                .filter(|queue| broker_names.contains(queue.broker_name()))
                .map(move |queue| (topic.clone(), queue.broker_name().to_string()))
        })
        .collect()
}

#[test]
fn reverse_index_matches_full_scan_across_state_transitions() {
    let table = TopicQueueTable::new();
    table.insert("topic-a".into(), "broker-a".into(), queue_data("broker-a"));
    table.insert("topic-a".into(), "broker-b".into(), queue_data("broker-b"));
    table.insert("topic-b".into(), "broker-a".into(), queue_data("broker-a"));
    assert_reverse_index_equivalent(&table, &["broker-a", "broker-b"]);

    assert!(table.update_queue_data_perm("topic-a", "broker-a", 4));
    assert_reverse_index_equivalent(&table, &["broker-a", "broker-b"]);

    table.remove_broker("topic-a", "broker-a");
    assert_reverse_index_equivalent(&table, &["broker-a", "broker-b"]);

    assert!(table.remove_topic("topic-b"));
    assert_reverse_index_equivalent(&table, &["broker-a", "broker-b"]);

    table.clear();
    assert_reverse_index_equivalent(&table, &["broker-a", "broker-b"]);
}

#[test]
fn broker_set_index_preserves_full_scan_duplicate_semantics() {
    let table = TopicQueueTable::new();
    table.insert("shared".into(), "broker-a".into(), queue_data("broker-a"));
    table.insert("shared".into(), "broker-b".into(), queue_data("broker-b"));
    table.insert("only-a".into(), "broker-a".into(), queue_data("broker-a"));
    table.insert("outside".into(), "broker-c".into(), queue_data("broker-c"));

    let broker_names = HashSet::from([
        CheetahString::from_static_str("broker-a"),
        CheetahString::from_static_str("broker-b"),
    ]);
    let mut indexed = table
        .topics_for_brokers_with_duplicates(&broker_names)
        .into_iter()
        .map(|topic| topic.to_string())
        .collect::<Vec<_>>();
    indexed.sort();

    let mut scanned = table
        .iter_all_with_data()
        .into_iter()
        .flat_map(|(topic, queues)| {
            queues
                .into_iter()
                .filter(|queue| broker_names.contains(queue.broker_name()))
                .map(move |_| topic.clone())
        })
        .collect::<Vec<_>>();
    scanned.sort();

    assert_eq!(indexed, scanned);
    assert_eq!(indexed, vec!["only-a", "shared", "shared"]);

    let indexed_pairs = table
        .topic_broker_pairs_for_brokers(&broker_names)
        .into_iter()
        .map(|(topic, broker)| (topic.to_string(), broker.to_string()))
        .collect::<BTreeSet<_>>();
    assert_eq!(indexed_pairs, broker_pairs_from_full_scan(&table, &broker_names));

    let indexed_queue_pairs = table
        .topic_queue_pairs_for_broker("broker-a")
        .into_iter()
        .map(|(topic, queue)| (topic.to_string(), queue.broker_name().to_string()))
        .collect::<BTreeSet<_>>();
    let expected_queue_pairs =
        broker_pairs_from_full_scan(&table, &HashSet::from([CheetahString::from_static_str("broker-a")]));
    assert_eq!(indexed_queue_pairs, expected_queue_pairs);
}

#[test]
fn reverse_index_matches_full_scan_after_concurrent_disjoint_mutations() {
    let table = Arc::new(TopicQueueTable::new());
    let workers = (0..8)
        .map(|broker_index| {
            let table = Arc::clone(&table);
            std::thread::spawn(move || {
                let broker_name = format!("broker-{broker_index}");
                for topic_index in 0..64 {
                    let topic = format!("topic-{topic_index}");
                    table.insert(
                        CheetahString::from(topic.as_str()),
                        CheetahString::from(broker_name.as_str()),
                        queue_data(&broker_name),
                    );
                }
                for topic_index in (0..64).step_by(2) {
                    table.remove_broker(&format!("topic-{topic_index}"), &broker_name);
                }
            })
        })
        .collect::<Vec<_>>();

    for worker in workers {
        worker.join().expect("topic table worker should not panic");
    }

    let broker_names = (0..8).map(|index| format!("broker-{index}")).collect::<Vec<_>>();
    let broker_name_refs = broker_names.iter().map(String::as_str).collect::<Vec<_>>();
    assert_eq!(table.cleanup_empty_topics(), 32);
    assert_reverse_index_equivalent(&table, &broker_name_refs);
    assert_eq!(table.topic_count(), 32);
    assert_eq!(table.total_queue_count(), 32 * 8);
}
