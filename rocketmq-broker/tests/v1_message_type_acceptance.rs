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

use cheetah_string::CheetahString;
use rocketmq_broker::send_message_constants::apply_topic_delivery_properties;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::topic::TopicMessageType;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MessageTypeCorpus {
    schema_version: u32,
    result_id: String,
    topic_types: Vec<TopicTypeCase>,
    inference_cases: Vec<InferenceCase>,
    broker_delivery_cases: Vec<BrokerDeliveryCase>,
    main_request_surfaces: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TopicTypeCase {
    name: String,
    route_accept: Vec<String>,
    sendable: bool,
}

#[derive(Deserialize)]
struct InferenceCase {
    name: String,
    properties: HashMap<String, String>,
    expected: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BrokerDeliveryCase {
    name: String,
    properties: HashMap<String, String>,
    initial_queue_id: i32,
    expected_queue_id: i32,
    priority_retained: bool,
    expected_inner_dispatch: Option<String>,
}

fn corpus() -> MessageTypeCorpus {
    serde_json::from_str(include_str!("../../scripts/fixtures/v1-message-type-corpus.json"))
        .expect("valid v1 message type corpus")
}

#[test]
fn message_property_inference_matches_the_java_55_precedence() {
    for case in corpus().inference_cases {
        let actual = TopicMessageType::parse_from_message_property(&case.properties);
        assert_eq!(actual.as_str(), case.expected, "{}", case.name);
    }
}

#[test]
fn broker_delivery_projection_matches_direct_java_send_behavior() {
    let topic = TopicConfig::with_queues("parent", 8, 8);
    for case in corpus().broker_delivery_cases {
        let mut properties = case
            .properties
            .into_iter()
            .map(|(key, value)| (CheetahString::from_string(key), CheetahString::from_string(value)))
            .collect();
        let mut queue_id = case.initial_queue_id;

        apply_topic_delivery_properties(
            &topic,
            &CheetahString::from_static_str("parent"),
            &mut properties,
            &mut queue_id,
        );

        assert_eq!(queue_id, case.expected_queue_id, "{}", case.name);
        assert_eq!(
            properties.contains_key(MessageConst::PROPERTY_PRIORITY),
            case.priority_retained,
            "{}",
            case.name
        );
        assert_eq!(
            properties
                .get(MessageConst::PROPERTY_INNER_MULTI_DISPATCH)
                .map(CheetahString::as_str),
            case.expected_inner_dispatch.as_deref(),
            "{}",
            case.name
        );
    }
}

#[test]
fn m01_corpus_freezes_all_topic_types_and_primary_request_surfaces() {
    let corpus = corpus();
    assert_eq!(corpus.schema_version, 1);
    assert_eq!(corpus.result_id, "M01");

    let topic_types = corpus
        .topic_types
        .iter()
        .map(|case| case.name.as_str())
        .collect::<HashSet<_>>();
    assert_eq!(
        topic_types,
        HashSet::from([
            "UNSPECIFIED",
            "NORMAL",
            "FIFO",
            "DELAY",
            "TRANSACTION",
            "PRIORITY",
            "LITE",
            "MIXED",
        ])
    );
    assert_eq!(corpus.topic_types.iter().filter(|case| case.sendable).count(), 6);
    assert!(corpus.topic_types.iter().all(|case| !case.route_accept.is_empty()));

    let surfaces = corpus.main_request_surfaces.into_iter().collect::<HashSet<_>>();
    for required in [
        "send",
        "batch-send",
        "request-reply",
        "classic-pull",
        "lite-pull",
        "pop",
        "batch-ack",
        "transaction-check",
        "recall",
        "query-by-cursor",
        "rebalance",
        "topic-admin",
        "group-admin",
        "config-admin",
    ] {
        assert!(surfaces.contains(required), "missing M01 request surface {required}");
    }
}
