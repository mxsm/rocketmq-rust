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

use rocketmq_model::common::mix_all;
use rocketmq_protocol::protocol::route::route_data_view::BrokerData;

use super::*;

const ADDRESS_A: &str = "127.0.0.1:10911";
const ADDRESS_B: &str = "127.0.0.2:10911";
const HOUR: i64 = 60 * 60 * 1000;

#[derive(Default)]
struct FakeSource {
    cluster_info: ClusterInfo,
    /// The Topics that have a route, with the route.
    routes: HashMap<&'static str, TopicRouteData>,
    /// What each Broker address indexes; an address that is not listed fails.
    metadata: HashMap<&'static str, Vec<MessageMetadataRead>>,
    traces: HashMap<&'static str, Vec<MessageTraceRead>>,
    queries: Mutex<Vec<(String, MessageIndexQuery)>>,
}

impl FakeSource {
    fn answer<T: Clone>(
        &self,
        stored: &HashMap<&'static str, Vec<T>>,
        broker_addr: CheetahString,
        query: MessageIndexQuery,
    ) -> Result<Vec<T>, CanonicalError> {
        self.queries.lock().unwrap().push((broker_addr.to_string(), query));
        stored.get(broker_addr.as_str()).cloned().ok_or_else(|| {
            CanonicalError::new(&rocketmq_error::PROTOCOL_RESPONSE_FAILED).with_context(
                rocketmq_error::ErrorContext::new()
                    .with_text(rocketmq_error::fields::OPERATION_DIAGNOSTIC, "message_index_test")
                    .with_secret_presence(rocketmq_error::fields::REASON_PRESENT),
            )
        })
    }
}

impl MessageIndexSource for FakeSource {
    async fn cluster_info(&self) -> Result<ClusterInfo, CanonicalError> {
        Ok(self.cluster_info.clone())
    }

    async fn topic_route(&self, topic: &str) -> Result<Option<TopicRouteData>, CanonicalError> {
        Ok(self.routes.get(topic).cloned())
    }

    async fn metadata_by_key(
        &self,
        broker_addr: CheetahString,
        query: MessageIndexQuery,
    ) -> Result<Vec<MessageMetadataRead>, CanonicalError> {
        self.answer(&self.metadata, broker_addr, query)
    }

    async fn trace(
        &self,
        broker_addr: CheetahString,
        query: MessageIndexQuery,
    ) -> Result<Vec<MessageTraceRead>, CanonicalError> {
        self.answer(&self.traces, broker_addr, query)
    }
}

/// Two masters of `cluster-a`, both hosting every listed Topic.
fn source(topics: &[&'static str]) -> FakeSource {
    let brokers = [("broker-a", ADDRESS_A), ("broker-b", ADDRESS_B)].map(|(name, address)| {
        BrokerData::new(
            "cluster-a".into(),
            name.into(),
            HashMap::from([(mix_all::MASTER_ID, address.into())]),
            None,
        )
    });
    let route = TopicRouteData {
        broker_datas: brokers.to_vec(),
        ..Default::default()
    };
    FakeSource {
        cluster_info: ClusterInfo::new(
            Some(
                brokers
                    .iter()
                    .map(|broker| (broker.broker_name().clone(), broker.clone()))
                    .collect(),
            ),
            Some(HashMap::from([(
                CheetahString::from("cluster-a"),
                brokers
                    .iter()
                    .map(|broker| broker.broker_name().clone())
                    .collect::<HashSet<_>>(),
            )])),
        ),
        routes: topics.iter().map(|topic| (*topic, route.clone())).collect(),
        ..Default::default()
    }
}

fn message(queue_offset: i64, store_timestamp: i64) -> MessageMetadataRead {
    MessageMetadataRead {
        topic: "orders".to_owned(),
        message_id: format!("OFFSET-{queue_offset}"),
        unique_message_id: Some(format!("UNIQ-{queue_offset}")),
        born_timestamp: store_timestamp - 5,
        store_timestamp,
        queue_id: 0,
        queue_offset,
        store_size: 128,
        reconsume_times: 0,
        sys_flag: 0,
        flag: 0,
        prepared_transaction_offset: 0,
    }
}

fn search() -> FindMessagesByKeyRequest {
    FindMessagesByKeyRequest::try_new("cluster-a", "orders", "order-42", HOUR, 2 * HOUR).unwrap()
}

#[tokio::test]
async fn a_key_search_asks_every_master_and_returns_the_newest_matches_first() {
    let mut source = source(&["orders"]);
    source
        .metadata
        .insert(ADDRESS_A, vec![message(1, HOUR + 10), message(2, HOUR + 30)]);
    // The second Broker also returns a message its index dated inside the window and that was
    // stored after it.
    source
        .metadata
        .insert(ADDRESS_B, vec![message(7, HOUR + 20), message(8, 2 * HOUR + 1)]);

    let result = find_messages_by_key_from(&source, &search()).await.unwrap();

    assert_eq!(result.topic, "orders");
    assert_eq!(
        result
            .matches
            .iter()
            .map(|found| (found.broker_name.as_str(), found.metadata.queue_offset))
            .collect::<Vec<_>>(),
        [("broker-a", 2), ("broker-b", 7), ("broker-a", 1)]
    );
    assert!(!result.truncated);
    assert_eq!(result.queried_broker_count, 2);
    assert!(result.unanswered_brokers.is_empty());

    let queries = source.queries.lock().unwrap();
    assert_eq!(queries.len(), 2);
    for (_, query) in queries.iter() {
        assert_eq!((query.topic.as_str(), query.key.as_str()), ("orders", "order-42"));
        assert_eq!((query.begin_timestamp, query.end_timestamp), (HOUR, 2 * HOUR));
        // One more than is returned, which is how a Broker that holds more is noticed.
        assert_eq!(query.max_messages, 33);
    }
}

#[tokio::test]
async fn a_key_search_returns_a_bounded_number_of_matches_and_says_when_there_are_more() {
    let mut source = source(&["orders"]);
    source.metadata.insert(
        ADDRESS_A,
        (0..33).map(|offset| message(offset, HOUR + offset)).collect(),
    );
    source.metadata.insert(ADDRESS_B, Vec::new());

    let result = find_messages_by_key_from(&source, &search()).await.unwrap();

    assert_eq!(result.matches.len(), MAX_MESSAGE_KEY_MATCHES);
    assert!(result.truncated);
    assert_eq!(result.matches[0].metadata.queue_offset, 32);
}

#[tokio::test]
async fn a_broker_that_does_not_answer_is_named_and_no_answer_at_all_is_an_error() {
    let mut partial = source(&["orders"]);
    partial.metadata.insert(ADDRESS_B, vec![message(7, HOUR + 20)]);
    let result = find_messages_by_key_from(&partial, &search()).await.unwrap();
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.unanswered_brokers, ["broker-a"]);
    assert_eq!(result.queried_broker_count, 2);

    let down = source(&["orders"]);
    let error = find_messages_by_key_from(&down, &search()).await.unwrap_err();
    assert_eq!(error.failure(), AdminFailure::Backend);

    let unknown = source(&[]);
    let error = find_messages_by_key_from(&unknown, &search()).await.unwrap_err();
    assert_eq!(error.failure(), AdminFailure::NotFound);
    assert!(unknown.queries.lock().unwrap().is_empty());

    // An empty answer of every Broker is a complete answer: nothing matches.
    let mut empty = source(&["orders"]);
    empty.metadata.insert(ADDRESS_A, Vec::new());
    empty.metadata.insert(ADDRESS_B, Vec::new());
    let result = find_messages_by_key_from(&empty, &search()).await.unwrap();
    assert!(result.matches.is_empty() && !result.truncated && result.unanswered_brokers.is_empty());
}

fn trace(trace_type: TraceType, timestamp: i64, group: &str) -> MessageTraceRead {
    MessageTraceRead {
        trace_type,
        timestamp,
        group_name: group.to_owned(),
        cost_time: 12,
        success: true,
        topic: "orders".to_owned(),
        message_id: "UNIQ-A".to_owned(),
        offset_message_id: "OFFSET-A".to_owned(),
        tags: "TagA".to_owned(),
        keys: "order-42 user-7".to_owned(),
        store_host: ADDRESS_B.to_owned(),
        client_host: "10.0.0.7".to_owned(),
        retry_times: 0,
        body_length: 128,
        from_transaction_check: false,
    }
}

fn trace_request() -> QueryMessageTraceRequest {
    QueryMessageTraceRequest::try_new("cluster-a", "UNIQ-A").unwrap()
}

#[tokio::test]
async fn trace_records_are_joined_in_time_order_and_name_the_store_by_its_logical_name() {
    let mut source = source(&[DEFAULT_MESSAGE_TRACE_TOPIC]);
    source
        .traces
        .insert(ADDRESS_A, vec![trace(TraceType::SubAfter, 300, "workers")]);
    source.traces.insert(
        ADDRESS_B,
        vec![
            trace(TraceType::SubBefore, 200, "workers"),
            trace(TraceType::Pub, 100, "producers"),
            // The same record read twice is reported once.
            trace(TraceType::Pub, 100, "producers"),
        ],
    );

    let result = query_message_trace_from(&source, &trace_request()).await.unwrap();

    assert_eq!(result.trace_topic, DEFAULT_MESSAGE_TRACE_TOPIC);
    assert!(result.trace_topic_found && !result.truncated);
    assert_eq!(
        result
            .records
            .iter()
            .map(|record| (record.step, record.timestamp, record.group.as_deref()))
            .collect::<Vec<_>>(),
        [
            (MessageTraceStep::Publish, 100, Some("producers")),
            (MessageTraceStep::ConsumeStart, 200, Some("workers")),
            (MessageTraceStep::ConsumeEnd, 300, Some("workers")),
        ]
    );
    let published = &result.records[0];
    assert_eq!(published.store_broker_name.as_deref(), Some("broker-b"));
    assert_eq!(published.keys, ["order-42", "user-7"]);
    assert_eq!(published.tags.as_deref(), Some("TagA"));
    assert_eq!(published.client_host.as_deref(), Some("10.0.0.7"));
    assert_eq!(published.body_length, 128);
    // No Broker address leaves the adapter, as a member or inside one.
    let rendered = serde_json::to_string(&result).unwrap();
    assert!(!rendered.contains(ADDRESS_A) && !rendered.contains(ADDRESS_B));

    let queries = source.queries.lock().unwrap();
    assert!(queries
        .iter()
        .all(|(_, query)| query.topic.as_str() == DEFAULT_MESSAGE_TRACE_TOPIC && query.key.as_str() == "UNIQ-A"));
}

#[tokio::test]
async fn a_cluster_without_a_trace_topic_is_an_answer_and_a_custom_trace_topic_is_read() {
    let untraced = source(&["orders"]);
    let result = query_message_trace_from(&untraced, &trace_request()).await.unwrap();
    assert!(!result.trace_topic_found);
    assert!(result.records.is_empty());
    assert!(untraced.queries.lock().unwrap().is_empty());

    let mut custom = source(&["custom_trace"]);
    custom
        .traces
        .insert(ADDRESS_A, vec![trace(TraceType::Pub, 100, "producers")]);
    custom.traces.insert(ADDRESS_B, Vec::new());
    let request = trace_request().with_trace_topic("custom_trace").unwrap();
    let result = query_message_trace_from(&custom, &request).await.unwrap();
    assert_eq!(result.trace_topic, "custom_trace");
    assert_eq!(result.records.len(), 1);

    // A cluster that is not known stays an error: the answer would be about nothing.
    let other_cluster = QueryMessageTraceRequest::try_new("cluster-z", "UNIQ-A").unwrap();
    let error = query_message_trace_from(&source(&[DEFAULT_MESSAGE_TRACE_TOPIC]), &other_cluster)
        .await
        .unwrap_err();
    assert_eq!(error.failure(), AdminFailure::NotFound);
    assert_eq!(error.resource(), Some("cluster"));
}

#[tokio::test]
async fn what_a_client_wrote_into_a_trace_record_is_bounded_and_never_trusted() {
    let mut forged = trace(TraceType::Pub, 100, "producers\u{7}");
    forged.topic = "t".repeat(MAX_TRACE_TEXT_BYTES + 1);
    forged.tags = String::new();
    forged.keys = format!("ok {} bad\u{0}key", "k".repeat(MAX_MESSAGE_KEY_BYTES + 1));
    forged.store_host = "10.9.9.9:10911".to_owned();
    let mut source = source(&[DEFAULT_MESSAGE_TRACE_TOPIC]);
    source.traces.insert(ADDRESS_A, vec![forged]);

    let result = query_message_trace_from(&source, &trace_request()).await.unwrap();

    let record = &result.records[0];
    assert_eq!(
        (record.group.as_deref(), record.topic.as_deref(), record.tags.as_deref()),
        (None, None, None)
    );
    assert_eq!(record.keys, ["ok"]);
    // A store the cluster does not have is not named, and its address is not passed on.
    assert_eq!(record.store_broker_name, None);
    assert_eq!(result.unanswered_brokers, ["broker-b"]);
}
