// Copyright 2026 The RocketMQ Rust Authors
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
use std::collections::HashSet;
use std::sync::Mutex;

use rocketmq_model::message::MessageQueue;
use rocketmq_protocol::protocol::admin::offset_wrapper::OffsetWrapper;
use rocketmq_protocol::protocol::body::connection::Connection;
use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

use super::*;

const ADDRESS_A: &str = "127.0.0.1:10911";
const ADDRESS_B: &str = "127.0.0.2:10911";
const ADDRESS_C: &str = "127.0.0.3:10911";
const SLAVE_ADDRESS_A: &str = "127.0.0.1:20911";

#[derive(Clone)]
enum ConfigReply {
    Present(Box<SubscriptionGroupConfigVersioned>),
    Absent,
    Failure(&'static str),
}

#[derive(Clone)]
enum ConnectionReply {
    Online(rocketmq_protocol::protocol::body::consumer_connection::ConsumerConnection),
    Offline,
    Failure(&'static str),
}

enum ProgressReply {
    Observed(ConsumeStats),
    Absent,
    Failure(&'static str),
}

#[derive(Default)]
struct FakeSource {
    cluster_info: ClusterInfo,
    route: Option<TopicRouteData>,
    configs: BTreeMap<String, ConfigReply>,
    connections: BTreeMap<String, ConnectionReply>,
    progress: Mutex<BTreeMap<String, ProgressReply>>,
    config_calls: Mutex<Vec<String>>,
    connection_calls: Mutex<Vec<String>>,
    progress_calls: Mutex<Vec<String>>,
}

impl ConsumerObservationSource for FakeSource {
    async fn cluster_info(&self) -> Result<ClusterInfo, RocketMQError> {
        Ok(self.cluster_info.clone())
    }

    async fn consumer_route(&self, _consumer_group: &str) -> Result<Option<TopicRouteData>, RocketMQError> {
        Ok(self.route.clone())
    }

    async fn group_config(
        &self,
        broker_addr: CheetahString,
        _consumer_group: &str,
    ) -> Result<ConsumerGroupConfigRead, RocketMQError> {
        self.config_calls.lock().unwrap().push(broker_addr.to_string());
        match self.configs.get(broker_addr.as_str()) {
            Some(ConfigReply::Present(config)) => Ok(ConsumerGroupConfigRead::Present(config.clone())),
            Some(ConfigReply::Absent) | None => Ok(ConsumerGroupConfigRead::Absent),
            Some(ConfigReply::Failure(reason)) => Err(test_error(reason)),
        }
    }

    async fn connection(
        &self,
        broker_addr: CheetahString,
        _consumer_group: &str,
    ) -> Result<ConsumerConnectionRead, RocketMQError> {
        self.connection_calls.lock().unwrap().push(broker_addr.to_string());
        match self.connections.get(broker_addr.as_str()) {
            Some(ConnectionReply::Online(connection)) => Ok(ConsumerConnectionRead::Online(connection.clone())),
            Some(ConnectionReply::Offline) | None => Ok(ConsumerConnectionRead::Offline),
            Some(ConnectionReply::Failure(reason)) => Err(test_error(reason)),
        }
    }

    async fn progress(
        &self,
        broker_addr: CheetahString,
        _consumer_group: &str,
    ) -> Result<ConsumerProgressRead, RocketMQError> {
        self.progress_calls.lock().unwrap().push(broker_addr.to_string());
        match self.progress.lock().unwrap().remove(broker_addr.as_str()) {
            Some(ProgressReply::Observed(stats)) => Ok(ConsumerProgressRead::Observed(stats)),
            Some(ProgressReply::Absent) | None => Ok(ConsumerProgressRead::Absent),
            Some(ProgressReply::Failure(reason)) => Err(test_error(reason)),
        }
    }
}

fn topology(brokers: &[(&str, &str, &str)]) -> (ClusterInfo, TopicRouteData) {
    let mut broker_table = HashMap::new();
    let mut cluster_table = HashMap::<CheetahString, HashSet<CheetahString>>::new();
    let mut route_brokers = Vec::new();
    for (cluster, broker, address) in brokers {
        let data = BrokerData::new(
            (*cluster).into(),
            (*broker).into(),
            HashMap::from([(mix_all::MASTER_ID, (*address).into())]),
            None,
        );
        broker_table.insert((*broker).into(), data.clone());
        cluster_table
            .entry((*cluster).into())
            .or_default()
            .insert((*broker).into());
        route_brokers.push(data);
    }
    (
        ClusterInfo::new(Some(broker_table), Some(cluster_table)),
        TopicRouteData {
            broker_datas: route_brokers,
            ..Default::default()
        },
    )
}

fn source(brokers: &[(&str, &str, &str)]) -> FakeSource {
    let (cluster_info, route) = topology(brokers);
    FakeSource {
        cluster_info,
        route: Some(route),
        ..Default::default()
    }
}

#[derive(Clone, Copy)]
enum EmbeddedTopologyCorruption {
    RouteCluster,
    BrokerTableName,
}

fn corrupt_embedded_topology(source: &mut FakeSource, broker_name: &str, corruption: EmbeddedTopologyCorruption) {
    match corruption {
        EmbeddedTopologyCorruption::RouteCluster => source
            .route
            .as_mut()
            .unwrap()
            .broker_datas
            .iter_mut()
            .find(|broker| broker.broker_name().as_str() == broker_name)
            .unwrap()
            .set_cluster("cluster-b".into()),
        EmbeddedTopologyCorruption::BrokerTableName => source
            .cluster_info
            .broker_addr_table
            .as_mut()
            .unwrap()
            .get_mut(broker_name)
            .unwrap()
            .set_broker_name("forged-broker".into()),
    }
}

fn add_slave(source: &mut FakeSource, broker_name: &str, slave_address: &str) {
    source
        .cluster_info
        .broker_addr_table
        .as_mut()
        .unwrap()
        .get_mut(broker_name)
        .unwrap()
        .broker_addrs_mut()
        .insert(1, slave_address.into());
    source
        .route
        .as_mut()
        .unwrap()
        .broker_datas
        .iter_mut()
        .find(|broker| broker.broker_name().as_str() == broker_name)
        .unwrap()
        .broker_addrs_mut()
        .insert(1, slave_address.into());
}

fn config(version: u64) -> ConfigReply {
    let mut config = SubscriptionGroupConfig::new("group-a".into());
    config.set_retry_queue_nums(2);
    config.set_retry_max_times(17);
    config.set_consume_timeout_minute(16);
    ConfigReply::Present(Box::new(SubscriptionGroupConfigVersioned { version, config }))
}

fn connection(count: usize) -> ConnectionReply {
    let mut observation = rocketmq_protocol::protocol::body::consumer_connection::ConsumerConnection::new();
    observation.set_consume_type(ConsumeType::ConsumePassively);
    observation.set_message_model(MessageModel::Clustering);
    observation.set_consume_from_where(ConsumeFromWhere::ConsumeFromLastOffset);
    for index in 0..count {
        let mut client = Connection::new();
        client.set_client_id(format!("secret-client-{index}").into());
        client.set_client_addr(format!("10.0.0.{index}:12000").into());
        observation.insert_connection(client);
    }
    ConnectionReply::Online(observation)
}

fn stats(broker_name: &str, rows: &[(i32, i64, i64, i64, i64)], consume_tps: f64) -> ConsumeStats {
    let mut stats = ConsumeStats::new();
    stats.set_consume_tps(consume_tps);
    for (queue_id, broker_offset, consumer_offset, pull_offset, timestamp) in rows {
        insert_row(
            &mut stats,
            "orders",
            broker_name,
            *queue_id,
            *broker_offset,
            *consumer_offset,
            *pull_offset,
            *timestamp,
        );
    }
    stats
}

fn stats_with_count(broker_name: &str, row_count: usize, unique_topics: bool) -> ConsumeStats {
    let mut stats = ConsumeStats::new();
    for queue_id in 0..row_count {
        let topic = if unique_topics {
            format!("topic-{broker_name}-{queue_id}")
        } else {
            "orders".to_string()
        };
        insert_row(&mut stats, &topic, broker_name, queue_id as i32, 1, 0, 1, 1);
    }
    stats
}

fn progress_source_with_counts(row_counts: &[usize]) -> FakeSource {
    let owned = row_counts
        .iter()
        .enumerate()
        .map(|(index, _)| {
            (
                "cluster-a".to_string(),
                format!("broker-{index:02}"),
                format!("127.0.0.1:{}", 10_000 + index),
            )
        })
        .collect::<Vec<_>>();
    let borrowed = owned
        .iter()
        .map(|(cluster, broker, address)| (cluster.as_str(), broker.as_str(), address.as_str()))
        .collect::<Vec<_>>();
    let mut source = source(&borrowed);
    for ((_, broker, address), row_count) in owned.iter().zip(row_counts) {
        source.configs.insert(address.clone(), config(1));
        source.progress.lock().unwrap().insert(
            address.clone(),
            ProgressReply::Observed(stats_with_count(broker, *row_count, false)),
        );
    }
    source
}

#[allow(clippy::too_many_arguments)]
fn insert_row(
    stats: &mut ConsumeStats,
    topic: &str,
    broker_name: &str,
    queue_id: i32,
    broker_offset: i64,
    consumer_offset: i64,
    pull_offset: i64,
    timestamp: i64,
) {
    let mut offset = OffsetWrapper::new();
    offset.set_broker_offset(broker_offset);
    offset.set_consumer_offset(consumer_offset);
    offset.set_pull_offset(pull_offset);
    offset.set_last_timestamp(timestamp);
    stats
        .get_offset_table_mut()
        .insert(MessageQueue::from_parts(topic, broker_name, queue_id), offset);
}

fn details_request() -> QueryConsumerGroupDetailsRequest {
    QueryConsumerGroupDetailsRequest::try_new("cluster-a", "group-a").unwrap()
}

fn progress_request(max_rows: usize) -> QueryConsumerProgressRequest {
    QueryConsumerProgressRequest::try_new("cluster-a", "group-a", max_rows).unwrap()
}

#[tokio::test]
async fn details_are_sorted_typed_partial_and_address_free() {
    let mut source = source(&[
        ("cluster-a", "broker-b", ADDRESS_B),
        ("cluster-a", "broker-a", ADDRESS_A),
        ("cluster-b", "broker-c", ADDRESS_C),
    ]);
    source.configs.insert(ADDRESS_A.to_string(), config(7));
    source.configs.insert(ADDRESS_B.to_string(), config(8));
    source.connections.insert(ADDRESS_A.to_string(), connection(2));
    source
        .connections
        .insert(ADDRESS_B.to_string(), ConnectionReply::Failure("secret endpoint"));

    let result = query_consumer_group_details_from(&source, &details_request())
        .await
        .unwrap();

    assert!(result.partial);
    assert_eq!(result.data.total_connection_count, 2);
    assert_eq!(result.data.brokers[0].broker_name, "broker-a");
    assert_eq!(
        result.data.brokers[0].connection_state,
        Some(ConsumerConnectionState::Online)
    );
    assert_eq!(result.data.brokers[0].consume_type, Some(ConsumerConsumeType::Push));
    assert_eq!(result.data.brokers[1].connection_state, None);
    assert_eq!(result.source_failures[0].logical_target(), "broker-b");
    let wire = serde_json::to_string(&result).unwrap();
    for secret in [
        "127.0.0",
        "10.0.0",
        "secret-client",
        "secret endpoint",
        "subscription",
        "attributes",
    ] {
        assert!(!wire.contains(secret), "secret={secret}");
    }
}

#[tokio::test]
async fn details_map_offline_and_all_absent_or_no_valid_config_semantics() {
    let mut offline = source(&[("cluster-a", "broker-a", ADDRESS_A)]);
    offline.configs.insert(ADDRESS_A.to_string(), config(1));
    offline
        .connections
        .insert(ADDRESS_A.to_string(), ConnectionReply::Offline);
    let result = query_consumer_group_details_from(&offline, &details_request())
        .await
        .unwrap();
    assert!(!result.partial);
    assert_eq!(
        result.data.brokers[0].connection_state,
        Some(ConsumerConnectionState::Offline)
    );

    let mut absent = source(&[("cluster-a", "broker-a", ADDRESS_A)]);
    absent.configs.insert(ADDRESS_A.to_string(), ConfigReply::Absent);
    let error = query_consumer_group_details_from(&absent, &details_request())
        .await
        .unwrap_err();
    assert!(matches!(error, AdminError::NotFound { .. }));
    assert!(absent.connection_calls.lock().unwrap().is_empty());

    let mut mixed = source(&[
        ("cluster-a", "broker-a", ADDRESS_A),
        ("cluster-a", "broker-b", ADDRESS_B),
    ]);
    mixed.configs.insert(ADDRESS_A.to_string(), ConfigReply::Absent);
    mixed
        .configs
        .insert(ADDRESS_B.to_string(), ConfigReply::Failure("private"));
    let error = query_consumer_group_details_from(&mixed, &details_request())
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some("ADMIN_QUERY_ALL_SOURCES_FAILED"));
    assert!(mixed.connection_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn progress_distinguishes_empty_zero_and_nonzero_lag_and_preserves_partial() {
    let mut empty = source(&[("cluster-a", "broker-a", ADDRESS_A)]);
    empty.configs.insert(ADDRESS_A.to_string(), config(1));
    empty
        .progress
        .lock()
        .unwrap()
        .insert(ADDRESS_A.to_string(), ProgressReply::Observed(ConsumeStats::new()));
    let result = query_consumer_progress_from(&empty, &progress_request(10))
        .await
        .unwrap();
    assert_eq!(result.data.state, ConsumerProgressState::NoConsumption);
    assert_eq!(result.data.queue_count, 0);

    let mut zero = source(&[("cluster-a", "broker-a", ADDRESS_A)]);
    zero.configs.insert(ADDRESS_A.to_string(), config(1));
    zero.progress.lock().unwrap().insert(
        ADDRESS_A.to_string(),
        ProgressReply::Observed(stats("broker-a", &[(0, 10, 10, 10, 1)], 2.5)),
    );
    let result = query_consumer_progress_from(&zero, &progress_request(10))
        .await
        .unwrap();
    assert_eq!(result.data.state, ConsumerProgressState::Observed);
    assert_eq!(result.data.total_lag, 0);
    assert_eq!(result.data.consume_tps, 2.5);

    let mut partial = source(&[
        ("cluster-a", "broker-a", ADDRESS_A),
        ("cluster-a", "broker-b", ADDRESS_B),
    ]);
    partial.configs.insert(ADDRESS_A.to_string(), config(1));
    partial.configs.insert(ADDRESS_B.to_string(), config(1));
    partial.progress.lock().unwrap().insert(
        ADDRESS_A.to_string(),
        ProgressReply::Observed(stats("broker-a", &[(0, 20, 10, 15, 9)], 1.0)),
    );
    partial
        .progress
        .lock()
        .unwrap()
        .insert(ADDRESS_B.to_string(), ProgressReply::Failure("private"));
    let result = query_consumer_progress_from(&partial, &progress_request(10))
        .await
        .unwrap();
    assert!(result.partial);
    assert_eq!((result.data.total_lag, result.data.total_inflight), (10, 5));
    assert_eq!(result.data.queues[0].last_timestamp, 9);
    assert!(!serde_json::to_string(&result).unwrap().contains("private"));
}

#[tokio::test]
async fn all_progress_sources_failing_or_absent_is_total_failure() {
    for reply in [ProgressReply::Failure("private"), ProgressReply::Absent] {
        let mut source = source(&[("cluster-a", "broker-a", ADDRESS_A)]);
        source.configs.insert(ADDRESS_A.to_string(), config(1));
        source.progress.lock().unwrap().insert(ADDRESS_A.to_string(), reply);
        let error = query_consumer_progress_from(&source, &progress_request(10))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some("ADMIN_QUERY_ALL_SOURCES_FAILED"));
    }
}

#[test]
fn progress_wire_fields_are_fail_closed_and_valid_rows_are_not_rewritten() {
    let invalid_rows = [
        ("bad topic", "broker-a", 0, 10, 1, 2, 1),
        ("orders", "forged", 0, 10, 1, 2, 1),
        ("orders", "broker-a", -1, 10, 1, 2, 1),
        ("orders", "broker-a", 1, -1, 0, 0, 1),
        ("orders", "broker-a", 2, 1, -1, 0, 1),
        ("orders", "broker-a", 3, 1, 0, -1, 1),
        ("orders", "broker-a", 4, 1, 0, 0, -1),
        ("orders", "broker-a", 5, 1, 2, 2, 1),
        ("orders", "broker-a", 6, 2, 1, 0, 1),
    ];
    let mut malformed = ConsumeStats::new();
    for row in invalid_rows {
        insert_row(&mut malformed, row.0, row.1, row.2, row.3, row.4, row.5, row.6);
    }
    insert_row(&mut malformed, "orders", "broker-a", 7, 10, 4, 8, 9);
    let mut collector = BoundedProgressCollector::new(10);
    let observation = collector.observe_source("broker-a", malformed);
    assert!(observation.invalid);
    assert_eq!(observation.valid_rows, 1);
    assert_eq!(collector.rows.len(), 1);
    assert_eq!(collector.rows.values().next().unwrap().queue_id, 7);
    assert_eq!(
        (
            collector.rows.values().next().unwrap().lag,
            collector.rows.values().next().unwrap().inflight
        ),
        (6, 4)
    );
}

#[test]
fn exact_wire_keys_and_raw_table_uniqueness_prevent_normalized_collisions() {
    let mut observation = ConsumeStats::new();
    insert_row(&mut observation, "orders", "broker-a", 0, 20, 10, 15, 2);
    // An exact duplicate/conflict replaces the same decoded HashMap key before collection, so it
    // cannot be counted twice. No trim or case normalization is applied to otherwise distinct keys.
    insert_row(&mut observation, "orders", "broker-a", 0, 19, 10, 15, 3);
    insert_row(&mut observation, "orders", "broker-a", 1, 3, 2, 2, 4);
    insert_row(&mut observation, "orders-", "broker-a", 1, 5, 2, 4, 5);
    insert_row(&mut observation, "orders", "broker-a ", 2, 9, 1, 2, 6);
    assert_eq!(observation.get_offset_table().len(), 4);

    let mut collector = BoundedProgressCollector::new(10);
    let source = collector.observe_source("broker-a", observation);
    assert!(source.invalid);
    assert_eq!(source.valid_rows, 3);
    assert_eq!(collector.queue_count, 3);
    assert_eq!(collector.topics.len(), 2);
    assert_eq!(collector.rows.len(), 3);
    assert_eq!(collector.rows.values().next().unwrap().broker_offset, 19);

    let duplicate_source = collector.observe_source("broker-a", ConsumeStats::new());
    assert!(duplicate_source.invalid);
    assert_eq!(duplicate_source.valid_rows, 0);
    assert_eq!(collector.queue_count, 3);
    assert_eq!(collector.decoded_rows, 4);
}

#[tokio::test]
async fn retention_is_sorted_and_aggregates_every_unique_row_before_truncation() {
    let mut source = source(&[("cluster-a", "broker-a", ADDRESS_A)]);
    source.configs.insert(ADDRESS_A.to_string(), config(1));
    let mut observation = ConsumeStats::new();
    for queue_id in (0..=MAX_CONSUMER_PROGRESS_ROWS as i32).rev() {
        let (topic, broker_offset) = if queue_id == MAX_CONSUMER_PROGRESS_ROWS as i32 {
            ("returns", 2)
        } else {
            ("orders", 1)
        };
        insert_row(
            &mut observation,
            topic,
            "broker-a",
            queue_id,
            broker_offset,
            0,
            broker_offset,
            1,
        );
    }
    source
        .progress
        .lock()
        .unwrap()
        .insert(ADDRESS_A.to_string(), ProgressReply::Observed(observation));
    let result = query_consumer_progress_from(&source, &progress_request(MAX_CONSUMER_PROGRESS_ROWS))
        .await
        .unwrap();
    assert!(result.partial);
    assert!(result.data.truncated);
    assert_eq!(result.data.topic_count, 2);
    assert_eq!(result.data.queue_count, MAX_CONSUMER_PROGRESS_ROWS + 1);
    assert_eq!(result.data.total_lag, MAX_CONSUMER_PROGRESS_ROWS as u64 + 2);
    assert_eq!(result.data.max_queue_lag, 2);
    assert_eq!(result.data.queues.len(), MAX_CONSUMER_PROGRESS_ROWS);
    assert_eq!(result.data.queues.first().unwrap().queue_id, 0);
    assert_eq!(
        result.data.queues.last().unwrap().queue_id,
        MAX_CONSUMER_PROGRESS_ROWS as i32 - 1
    );
    assert!(result
        .warnings
        .iter()
        .any(|warning| warning == CONSUMER_PROGRESS_TRUNCATED_WARNING));
}

#[test]
fn fifty_thousand_wire_rows_keep_only_bounded_top_k_and_full_aggregates() {
    const WIRE_ROWS: i32 = 50_000;
    assert_eq!(WIRE_ROWS as usize, MAX_CONSUMER_PROGRESS_SOURCE_ROWS);
    assert_eq!(WIRE_ROWS as usize, MAX_CONSUMER_PROGRESS_QUERY_ROWS);
    let mut stats = ConsumeStats::new();
    for queue_id in (0..WIRE_ROWS).rev() {
        insert_row(
            &mut stats,
            "orders",
            "broker-a",
            queue_id,
            i64::from(queue_id) + 1,
            0,
            i64::from(queue_id) + 1,
            1,
        );
    }

    let mut collector = BoundedProgressCollector::new(MAX_CONSUMER_PROGRESS_ROWS);
    let observation = collector.observe_source("broker-a", stats);
    assert!(!observation.invalid);
    assert_eq!(observation.valid_rows, WIRE_ROWS as usize);
    assert_eq!(collector.max_retained_len, MAX_CONSUMER_PROGRESS_ROWS);
    assert_eq!(collector.rows.len(), MAX_CONSUMER_PROGRESS_ROWS);
    assert_eq!(collector.decoded_rows, MAX_CONSUMER_PROGRESS_QUERY_ROWS);
    assert_eq!(collector.topics.len(), 1);
    assert_eq!(collector.queue_count, WIRE_ROWS as usize);
    assert_eq!(
        collector.total_lag,
        u128::from(WIRE_ROWS as u64) * u128::from((WIRE_ROWS + 1) as u64) / 2
    );
    assert_eq!(collector.total_inflight, collector.total_lag);
    assert_eq!(collector.max_queue_lag, WIRE_ROWS as u64);
    assert_eq!(collector.rows.first_key_value().unwrap().1.queue_id, 0);
    assert_eq!(
        collector.rows.last_key_value().unwrap().1.queue_id,
        MAX_CONSUMER_PROGRESS_ROWS as i32 - 1
    );

    let mut oversized = ConsumeStats::new();
    for queue_id in 0..=WIRE_ROWS {
        insert_row(&mut oversized, "orders", "broker-b", queue_id, 1, 0, 1, 1);
    }
    let mut rejected = BoundedProgressCollector::new(MAX_CONSUMER_PROGRESS_ROWS);
    let observation = rejected.observe_source("broker-b", oversized);
    assert!(observation.invalid);
    assert_eq!(observation.valid_rows, 0);
    assert_eq!(rejected.queue_count, 0);
    assert_eq!(rejected.decoded_rows, 0);
    assert_eq!(rejected.total_lag, 0);
    assert_eq!(rejected.total_inflight, 0);
    assert_eq!(rejected.max_queue_lag, 0);
    assert!(rejected.topics.is_empty());
    assert!(rejected.rows.is_empty());
    assert_eq!(rejected.max_retained_len, 0);
}

#[test]
fn query_budget_is_exact_bounded_and_empty_sources_consume_zero() {
    let half = MAX_CONSUMER_PROGRESS_QUERY_ROWS / 2;
    let mut collector = BoundedProgressCollector::new(10);
    let first = collector.observe_source("broker-a", stats_with_count("broker-a", half, true));
    let second = collector.observe_source(
        "broker-b",
        stats_with_count("broker-b", MAX_CONSUMER_PROGRESS_QUERY_ROWS - half, true),
    );
    assert!(!first.invalid);
    assert!(!second.invalid);
    assert_eq!(collector.decoded_rows, MAX_CONSUMER_PROGRESS_QUERY_ROWS);
    assert_eq!(collector.queue_count, MAX_CONSUMER_PROGRESS_QUERY_ROWS);
    assert_eq!(collector.topics.len(), MAX_CONSUMER_PROGRESS_QUERY_ROWS);
    assert!(collector.topics.len() <= MAX_CONSUMER_PROGRESS_QUERY_ROWS);
    assert_eq!(collector.rows.len(), 10);
    assert_eq!(collector.max_retained_len, 10);

    let empty = collector.observe_source("broker-c", ConsumeStats::new());
    assert!(!empty.invalid);
    assert!(!empty.had_offsets);
    assert_eq!(collector.decoded_rows, MAX_CONSUMER_PROGRESS_QUERY_ROWS);

    let rejected = collector.observe_source("broker-d", stats_with_count("broker-d", 1, false));
    assert!(rejected.invalid);
    assert_eq!(rejected.valid_rows, 0);
    assert_eq!(collector.decoded_rows, MAX_CONSUMER_PROGRESS_QUERY_ROWS);
    assert_eq!(collector.queue_count, MAX_CONSUMER_PROGRESS_QUERY_ROWS);
    assert_eq!(collector.topics.len(), MAX_CONSUMER_PROGRESS_QUERY_ROWS);
    assert_eq!(collector.rows.len(), 10);
}

#[tokio::test]
async fn oversized_source_is_total_failure_or_partial_without_polluting_aggregates() {
    let oversized_stats = |broker_name: &str| {
        let mut observation = ConsumeStats::new();
        for queue_id in 0..=MAX_CONSUMER_PROGRESS_SOURCE_ROWS as i32 {
            insert_row(&mut observation, "orders", broker_name, queue_id, 1, 0, 1, 1);
        }
        observation
    };

    let mut only_oversized = source(&[("cluster-a", "broker-a", ADDRESS_A)]);
    only_oversized.configs.insert(ADDRESS_A.to_string(), config(1));
    only_oversized.progress.lock().unwrap().insert(
        ADDRESS_A.to_string(),
        ProgressReply::Observed(oversized_stats("broker-a")),
    );
    let error = query_consumer_progress_from(&only_oversized, &progress_request(10))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some("ADMIN_QUERY_ALL_SOURCES_FAILED"));

    let mut mixed = source(&[
        ("cluster-a", "broker-a", ADDRESS_A),
        ("cluster-a", "broker-b", ADDRESS_B),
    ]);
    mixed.configs.insert(ADDRESS_A.to_string(), config(1));
    mixed.configs.insert(ADDRESS_B.to_string(), config(1));
    mixed.progress.lock().unwrap().insert(
        ADDRESS_A.to_string(),
        ProgressReply::Observed(stats("broker-a", &[(0, 9, 4, 7, 1)], 2.0)),
    );
    mixed.progress.lock().unwrap().insert(
        ADDRESS_B.to_string(),
        ProgressReply::Observed(oversized_stats("broker-b")),
    );
    let result = query_consumer_progress_from(&mixed, &progress_request(10))
        .await
        .unwrap();
    assert!(result.partial);
    assert_eq!(result.data.topic_count, 1);
    assert_eq!(result.data.queue_count, 1);
    assert_eq!(result.data.total_lag, 5);
    assert_eq!(result.data.max_queue_lag, 5);
    assert_eq!(result.data.total_inflight, 3);
    assert_eq!(result.data.consume_tps, 2.0);
    assert_eq!(result.data.queues.len(), 1);
    assert_eq!(result.source_failures.len(), 1);
    assert_eq!(result.source_failures[0].logical_target(), "broker-b");
    assert_eq!(result.source_failures[0].code(), AdminQueryFailureCode::InvalidResponse);
}

#[tokio::test]
async fn query_budget_rejects_whole_later_source_and_preserves_partial_or_total_semantics() {
    {
        let mut partial = source(&[
            ("cluster-a", "broker-a", ADDRESS_A),
            ("cluster-a", "broker-b", ADDRESS_B),
        ]);
        partial.configs.insert(ADDRESS_A.to_string(), config(1));
        partial.configs.insert(ADDRESS_B.to_string(), config(1));
        partial.progress.lock().unwrap().insert(
            ADDRESS_A.to_string(),
            ProgressReply::Observed(stats_with_count("broker-a", 25_000, false)),
        );
        partial.progress.lock().unwrap().insert(
            ADDRESS_B.to_string(),
            ProgressReply::Observed(stats_with_count("broker-b", 25_001, false)),
        );
        let result = query_consumer_progress_from(&partial, &progress_request(10))
            .await
            .unwrap();
        assert!(result.partial);
        assert_eq!(result.data.topic_count, 1);
        assert_eq!(result.data.queue_count, 25_000);
        assert_eq!(result.data.total_lag, 25_000);
        assert_eq!(result.data.max_queue_lag, 1);
        assert_eq!(result.source_failures.len(), 1);
        assert_eq!(result.source_failures[0].logical_target(), "broker-b");
        assert_eq!(
            partial.progress_calls.lock().unwrap().as_slice(),
            [ADDRESS_A, ADDRESS_B]
        );
    }

    {
        let mut total = source(&[
            ("cluster-a", "broker-a", ADDRESS_A),
            ("cluster-a", "broker-b", ADDRESS_B),
        ]);
        total.configs.insert(ADDRESS_A.to_string(), config(1));
        total.configs.insert(ADDRESS_B.to_string(), config(1));
        total.progress.lock().unwrap().insert(
            ADDRESS_A.to_string(),
            ProgressReply::Observed(stats_with_count(
                "forged-broker",
                MAX_CONSUMER_PROGRESS_QUERY_ROWS,
                false,
            )),
        );
        total.progress.lock().unwrap().insert(
            ADDRESS_B.to_string(),
            ProgressReply::Observed(stats_with_count("broker-b", 1, false)),
        );
        let error = query_consumer_progress_from(&total, &progress_request(10))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some("ADMIN_QUERY_ALL_SOURCES_FAILED"));
        assert_eq!(total.progress_calls.lock().unwrap().as_slice(), [ADDRESS_A, ADDRESS_B]);
    }

    {
        let mut empty_first = source(&[
            ("cluster-a", "broker-a", ADDRESS_A),
            ("cluster-a", "broker-b", ADDRESS_B),
        ]);
        empty_first.configs.insert(ADDRESS_A.to_string(), config(1));
        empty_first.configs.insert(ADDRESS_B.to_string(), config(1));
        empty_first
            .progress
            .lock()
            .unwrap()
            .insert(ADDRESS_A.to_string(), ProgressReply::Observed(ConsumeStats::new()));
        empty_first.progress.lock().unwrap().insert(
            ADDRESS_B.to_string(),
            ProgressReply::Observed(stats_with_count("broker-b", MAX_CONSUMER_PROGRESS_QUERY_ROWS, false)),
        );
        let result = query_consumer_progress_from(&empty_first, &progress_request(10))
            .await
            .unwrap();
        assert!(result.source_failures.is_empty());
        assert_eq!(result.data.queue_count, MAX_CONSUMER_PROGRESS_QUERY_ROWS);
        assert_eq!(result.data.total_lag, MAX_CONSUMER_PROGRESS_QUERY_ROWS as u64);
        assert_eq!(
            empty_first.progress_calls.lock().unwrap().as_slice(),
            [ADDRESS_A, ADDRESS_B]
        );
    }
}

#[tokio::test]
async fn sixty_four_sources_have_deterministic_exact_and_over_budget_ownership() {
    let mut exact_counts = vec![781usize; MAX_CONSUMER_OBSERVATION_TARGETS];
    *exact_counts.last_mut().unwrap() = 797;
    assert_eq!(exact_counts.iter().sum::<usize>(), MAX_CONSUMER_PROGRESS_QUERY_ROWS);
    let exact = progress_source_with_counts(&exact_counts);
    let result = query_consumer_progress_from(&exact, &progress_request(10))
        .await
        .unwrap();
    assert!(result.source_failures.is_empty());
    assert_eq!(result.data.queue_count, MAX_CONSUMER_PROGRESS_QUERY_ROWS);
    assert_eq!(result.data.total_lag, MAX_CONSUMER_PROGRESS_QUERY_ROWS as u64);

    let mut over_counts = exact_counts;
    *over_counts.last_mut().unwrap() += 1;
    assert_eq!(over_counts.iter().sum::<usize>(), MAX_CONSUMER_PROGRESS_QUERY_ROWS + 1);
    let over = progress_source_with_counts(&over_counts);
    let result = query_consumer_progress_from(&over, &progress_request(10))
        .await
        .unwrap();
    let accepted_rows = 781usize * (MAX_CONSUMER_OBSERVATION_TARGETS - 1);
    assert!(result.partial);
    assert_eq!(result.data.queue_count, accepted_rows);
    assert_eq!(result.data.total_lag, accepted_rows as u64);
    assert_eq!(result.source_failures.len(), 1);
    assert_eq!(result.source_failures[0].logical_target(), "broker-63");
    assert_eq!(
        over.progress_calls.lock().unwrap().len(),
        MAX_CONSUMER_OBSERVATION_TARGETS
    );
}

#[tokio::test]
async fn totals_saturate_and_invalid_tps_is_excluded_with_stable_warnings() {
    let mut source = source(&[
        ("cluster-a", "broker-a", ADDRESS_A),
        ("cluster-a", "broker-b", ADDRESS_B),
        ("cluster-a", "broker-c", ADDRESS_C),
    ]);
    for (address, broker, tps) in [
        (ADDRESS_A, "broker-a", f64::MAX),
        (ADDRESS_B, "broker-b", f64::MAX),
        (ADDRESS_C, "broker-c", f64::NAN),
    ] {
        source.configs.insert(address.to_string(), config(1));
        source.progress.lock().unwrap().insert(
            address.to_string(),
            ProgressReply::Observed(stats(broker, &[(0, i64::MAX, 0, i64::MAX, 1)], tps)),
        );
    }
    let result = query_consumer_progress_from(&source, &progress_request(10))
        .await
        .unwrap();
    assert!(result.partial);
    assert_eq!(result.data.topic_count, 1);
    assert_eq!(result.data.total_lag, u64::MAX);
    assert_eq!(result.data.max_queue_lag, i64::MAX as u64);
    assert_eq!(result.data.total_inflight, u64::MAX);
    assert_eq!(result.data.consume_tps, f64::MAX);
    assert!(result
        .warnings
        .iter()
        .any(|warning| warning == CONSUMER_PROGRESS_TOTAL_SATURATED_WARNING));
    assert!(result
        .warnings
        .iter()
        .any(|warning| warning == CONSUMER_PROGRESS_INVALID_TPS_WARNING));
}

#[tokio::test]
async fn corrupt_topology_is_zero_rpc_or_partial_beside_valid_evidence() {
    let mut invalid = source(&[("cluster-a", "broker-a", ADDRESS_A)]);
    invalid
        .cluster_info
        .broker_addr_table
        .as_mut()
        .unwrap()
        .get_mut("broker-a")
        .unwrap()
        .set_cluster("cluster-b".into());
    let error = query_consumer_group_details_from(&invalid, &details_request())
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some("ADMIN_QUERY_ALL_SOURCES_FAILED"));
    assert!(invalid.config_calls.lock().unwrap().is_empty());

    let mut mixed = source(&[
        ("cluster-a", "broker-a", ADDRESS_A),
        ("cluster-a", "broker-b", ADDRESS_B),
    ]);
    mixed
        .cluster_info
        .broker_addr_table
        .as_mut()
        .unwrap()
        .get_mut("broker-b")
        .unwrap()
        .broker_addrs_mut()
        .insert(mix_all::MASTER_ID, "https://private.invalid/token".into());
    mixed.configs.insert(ADDRESS_A.to_string(), config(1));
    mixed
        .connections
        .insert(ADDRESS_A.to_string(), ConnectionReply::Offline);
    let result = query_consumer_group_details_from(&mixed, &details_request())
        .await
        .unwrap();
    assert!(result.partial);
    assert_eq!(result.data.brokers.len(), 1);
    assert_eq!(mixed.config_calls.lock().unwrap().as_slice(), [ADDRESS_A]);
}

#[tokio::test]
async fn embedded_cluster_and_broker_name_corruption_are_zero_rpc_for_both_tools() {
    for corruption in [
        EmbeddedTopologyCorruption::RouteCluster,
        EmbeddedTopologyCorruption::BrokerTableName,
    ] {
        let mut details = source(&[("cluster-a", "broker-a", ADDRESS_A)]);
        corrupt_embedded_topology(&mut details, "broker-a", corruption);
        let error = query_consumer_group_details_from(&details, &details_request())
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some("ADMIN_QUERY_ALL_SOURCES_FAILED"));
        assert!(details.config_calls.lock().unwrap().is_empty());
        assert!(details.connection_calls.lock().unwrap().is_empty());

        let mut progress = source(&[("cluster-a", "broker-a", ADDRESS_A)]);
        corrupt_embedded_topology(&mut progress, "broker-a", corruption);
        let error = query_consumer_progress_from(&progress, &progress_request(10))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some("ADMIN_QUERY_ALL_SOURCES_FAILED"));
        assert!(progress.config_calls.lock().unwrap().is_empty());
        assert!(progress.progress_calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn mixed_embedded_corruption_queries_only_valid_master_for_both_tools() {
    for corruption in [
        EmbeddedTopologyCorruption::RouteCluster,
        EmbeddedTopologyCorruption::BrokerTableName,
    ] {
        let brokers = [
            ("cluster-a", "broker-a", ADDRESS_A),
            ("cluster-a", "broker-b", ADDRESS_B),
        ];
        let mut details = source(&brokers);
        add_slave(&mut details, "broker-a", SLAVE_ADDRESS_A);
        corrupt_embedded_topology(&mut details, "broker-b", corruption);
        details.configs.insert(ADDRESS_A.to_string(), config(1));
        details
            .connections
            .insert(ADDRESS_A.to_string(), ConnectionReply::Offline);
        let result = query_consumer_group_details_from(&details, &details_request())
            .await
            .unwrap();
        assert!(result.partial);
        assert_eq!(result.data.brokers.len(), 1);
        assert_eq!(details.config_calls.lock().unwrap().as_slice(), [ADDRESS_A]);
        assert_eq!(details.connection_calls.lock().unwrap().as_slice(), [ADDRESS_A]);
        assert!(!details
            .config_calls
            .lock()
            .unwrap()
            .iter()
            .any(|address| address == SLAVE_ADDRESS_A));
        assert!(!details
            .connection_calls
            .lock()
            .unwrap()
            .iter()
            .any(|address| address == SLAVE_ADDRESS_A));

        let mut progress = source(&brokers);
        add_slave(&mut progress, "broker-a", SLAVE_ADDRESS_A);
        corrupt_embedded_topology(&mut progress, "broker-b", corruption);
        progress.configs.insert(ADDRESS_A.to_string(), config(1));
        progress.progress.lock().unwrap().insert(
            ADDRESS_A.to_string(),
            ProgressReply::Observed(stats("broker-a", &[(0, 3, 1, 2, 1)], 1.0)),
        );
        let result = query_consumer_progress_from(&progress, &progress_request(10))
            .await
            .unwrap();
        assert!(result.partial);
        assert_eq!(result.data.queue_count, 1);
        assert_eq!(progress.config_calls.lock().unwrap().as_slice(), [ADDRESS_A]);
        assert_eq!(progress.progress_calls.lock().unwrap().as_slice(), [ADDRESS_A]);
        assert!(!progress
            .config_calls
            .lock()
            .unwrap()
            .iter()
            .any(|address| address == SLAVE_ADDRESS_A));
        assert!(!progress
            .progress_calls
            .lock()
            .unwrap()
            .iter()
            .any(|address| address == SLAVE_ADDRESS_A));
    }
}

#[test]
fn target_cap_accepts_64_and_rejects_65() {
    for count in [MAX_CONSUMER_OBSERVATION_TARGETS, MAX_CONSUMER_OBSERVATION_TARGETS + 1] {
        let owned = (0..count)
            .map(|index| {
                (
                    "cluster-a".to_string(),
                    format!("broker-{index:02}"),
                    format!("127.0.0.1:{}", 10_000 + index),
                )
            })
            .collect::<Vec<_>>();
        let borrowed = owned
            .iter()
            .map(|(cluster, broker, address)| (cluster.as_str(), broker.as_str(), address.as_str()))
            .collect::<Vec<_>>();
        let (cluster_info, route) = topology(&borrowed);
        let resolved = selected_cluster_route_masters(
            &cluster_info,
            &route,
            "cluster-a",
            AdminQuerySource::ConsumerGroupConfig,
        );
        if count == MAX_CONSUMER_OBSERVATION_TARGETS {
            assert_eq!(resolved.unwrap().0.len(), count);
        } else {
            assert_eq!(
                resolved.unwrap_err().code(),
                Some("CONSUMER_OBSERVATION_TARGET_LIMIT_EXCEEDED")
            );
        }
    }
}

fn test_error(reason: &str) -> RocketMQError {
    RocketMQError::ResponseProcessFailed {
        operation: "consumer_observation_test",
        reason: reason.to_string(),
    }
}
