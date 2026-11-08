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

use super::*;
use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
use std::{collections::HashMap, sync::Mutex};

const PHYSICAL_ID: &str = "7F00000100002A9F0000000000000001";
struct Fake {
    message: MessageRecord,
    route: AdminResult<Option<TopicRouteData>>,
    results: BTreeMap<String, AdminResult<MessageTraceRecordsRead>>,
    calls: Mutex<Vec<(String, MessageIndexQuery)>>,
    lookups: Mutex<Vec<MessageLookupRequest>>,
}
fn message() -> MessageRecord {
    MessageRecord {
        topic: "orders".into(),
        message_id: PHYSICAL_ID.into(),
        unique_message_id: Some("producer-id".into()),
        keys: None,
        tags: None,
        born_timestamp: 1,
        store_timestamp: 2,
        born_host: "client".into(),
        store_host: "broker".into(),
        queue_id: 0,
        queue_offset: 1,
        store_size: 1,
        reconsume_times: 0,
        body_crc: 0,
        sys_flag: 0,
        flag: 0,
        prepared_transaction_offset: 0,
        body: b"private business payload".to_vec(),
        properties: BTreeMap::new(),
    }
}
fn route() -> TopicRouteData {
    TopicRouteData {
        broker_datas: ["broker-a", "broker-b"]
            .into_iter()
            .map(|name| {
                BrokerData::new(
                    "cluster".into(),
                    name.into(),
                    HashMap::from([(0, format!("{name}:10911").into())]),
                    None,
                )
            })
            .collect(),
        ..Default::default()
    }
}
fn page(wire: &str) -> MessageTraceRecordsRead {
    MessageTraceRecordsRead {
        records: rocketmq_protocol::trace::decode_records(wire),
        limit_reached: false,
    }
}
fn fake() -> Fake {
    Fake {
        message: message(),
        route: Ok(Some(route())),
        results: BTreeMap::from([
            ("broker-a:10911".into(), Ok(page(""))),
            ("broker-b:10911".into(), Ok(page(""))),
        ]),
        calls: Mutex::new(Vec::new()),
        lookups: Mutex::new(Vec::new()),
    }
}
fn request() -> MessageTraceLookupRequest {
    MessageTraceLookupRequest {
        topic: "orders".into(),
        message_id: PHYSICAL_ID.into(),
        trace_topic: "custom-trace".into(),
    }
}
impl TraceSource for Fake {
    fn lookup<'a>(&'a self, request: &'a MessageLookupRequest) -> AdminFuture<'a, MessageRecord> {
        self.lookups.lock().unwrap().push(request.clone());
        Box::pin(std::future::ready(Ok(self.message.clone())))
    }
    fn route<'a>(&'a self, _: &'a str) -> AdminFuture<'a, Option<TopicRouteData>> {
        Box::pin(std::future::ready(self.route.clone()))
    }
    fn records<'a>(&'a self, broker: &'a str, query: MessageIndexQuery) -> AdminFuture<'a, MessageTraceRecordsRead> {
        self.calls.lock().unwrap().push((broker.into(), query));
        Box::pin(std::future::ready(self.results.get(broker).unwrap().clone()))
    }
}
fn publish(timestamp: i64, group: &str, offset: &str) -> String {
    format!("Pub\u{1}{timestamp}\u{1}region\u{1}{group}\u{1}orders\u{1}producer-id\u{1}tag\u{1}key\u{1}broker\u{1}10\u{1}5\u{1}0\u{1}{offset}\u{1}true\u{1}client\u{2}")
}

#[tokio::test]
async fn trace_read_resolves_selected_physical_identity_then_queries_explicit_topic_by_producer_id() {
    let mut source = fake();
    let data = format!(
        "{}{}{}{}",
        publish(20, "second", PHYSICAL_ID),
        publish(10, "first", PHYSICAL_ID),
        publish(10, "duplicate-producer-id-send", "other-offset"),
        "SubAfter\u{1}request\u{1}producer-id\u{1}8\u{1}false\u{1}key\u{2}"
    );
    source.results.insert("broker-a:10911".into(), Ok(page(&data)));
    let result = read_trace(&source, &request()).await.unwrap();
    assert_eq!(source.lookups.lock().unwrap()[0].message_id, PHYSICAL_ID);
    assert_eq!(result.message_id, PHYSICAL_ID);
    assert_eq!(result.producer_message_id.as_deref(), Some("producer-id"));
    for (_, query) in source.calls.lock().unwrap().iter() {
        assert_eq!(query.topic.as_str(), "custom-trace");
        assert_eq!(query.key.as_str(), "producer-id");
        assert_eq!(query.max_messages, 64);
    }
    assert_eq!(
        result
            .nodes
            .iter()
            .map(|node| node.group.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("first"), Some("duplicate-producer-id-send"), Some("second"), None]
    );
    assert_eq!(
        result.nodes[1].offset_message_id.as_deref(),
        Some("other-offset"),
        "reused producer IDs retain their real records without claiming one causal chain"
    );
    let old_consume = &result.nodes[3];
    assert_eq!(old_consume.timestamp, None);
    assert_eq!(old_consume.group, None);
    assert_eq!(old_consume.success, Some(false));
    assert_eq!(old_consume.cost_time_ms, Some(8));
    assert!(old_consume.client_host.is_none() && old_consume.store_host.is_none());
}

#[tokio::test]
async fn trace_empty_absent_topic_partial_and_all_failed_reads_remain_distinct() {
    let mut source = fake();
    assert!(read_trace(&source, &request()).await.unwrap().nodes.is_empty());
    source.route = Ok(None);
    let absent = read_trace(&source, &request()).await.unwrap();
    assert!(!absent.trace_topic_found);
    source.route = Ok(Some(route()));
    source.results.insert(
        "broker-b:10911".into(),
        Err(AdminError::unavailable("test", "private failure")),
    );
    let partial = read_trace(&source, &request()).await.unwrap();
    assert!(partial.trace_topic_found && partial.nodes.is_empty());
    assert_eq!(partial.unanswered_brokers, vec!["broker-b"]);
    source.results.insert(
        "broker-a:10911".into(),
        Err(AdminError::unavailable("test", "private failure")),
    );
    assert!(read_trace(&source, &request()).await.is_err());
    source.route = Err(AdminError::unavailable("route", "private route failure"));
    assert!(read_trace(&source, &request()).await.is_err());
}

#[tokio::test]
async fn trace_missing_producer_identity_does_not_query_physical_offset_as_trace_key() {
    let mut source = fake();
    source.message.unique_message_id = None;
    let result = read_trace(&source, &request()).await.unwrap();
    assert!(result.producer_message_id.is_none());
    assert!(result.nodes.is_empty());
    assert!(source.calls.lock().unwrap().is_empty());
    source.message.topic = "wrong-topic".into();
    assert!(read_trace(&source, &request()).await.is_err());
    assert!(source.calls.lock().unwrap().is_empty());
}

#[test]
fn trace_wire_mapping_preserves_unknown_fields_and_filters_other_ids_or_topics() {
    let before = page("SubBefore\u{1}10\u{1}region\u{1}workers\u{1}request\u{1}producer-id\u{1}2\u{1}key\u{2}");
    let node = observation(&before.records[0], "producer-id", "orders").unwrap();
    assert_eq!(node.timestamp, Some(10));
    assert_eq!(node.retry_times, Some(2));
    assert!(node.success.is_none() && node.cost_time_ms.is_none());
    let old_pub = page(
        "Pub\u{1}10\u{1}region\u{1}producers\u{1}orders\u{1}producer-id\u{1}tag\u{1}key\u{1}\u{1}10\u{1}\u{1}0\u{2}",
    );
    let node = observation(&old_pub.records[0], "producer-id", "orders").unwrap();
    assert!(node.success.is_none() && node.cost_time_ms.is_none() && node.store_host.is_none());
    assert!(observation(&old_pub.records[0], "other-id", "orders").is_none());
    assert!(observation(&old_pub.records[0], "producer-id", "other-topic").is_none());
}

#[tokio::test]
async fn trace_read_reports_node_and_broker_message_limits_instead_of_claiming_completeness() {
    let mut source = fake();
    let wire = (0..MESSAGE_TRACE_NODE_LIMIT + 1)
        .map(|i| publish(i as i64, "producers", PHYSICAL_ID))
        .collect::<String>();
    source.results.insert("broker-a:10911".into(), Ok(page(&wire)));
    let result = read_trace(&source, &request()).await.unwrap();
    assert_eq!(result.nodes.len(), MESSAGE_TRACE_NODE_LIMIT);
    assert!(result.limit_reached);
    let mut limited = page("");
    limited.limit_reached = true;
    source.results.insert("broker-a:10911".into(), Ok(limited));
    assert!(read_trace(&source, &request()).await.unwrap().limit_reached);
}
