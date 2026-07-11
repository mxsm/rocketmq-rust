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

use cheetah_string::CheetahString;
use rocketmq_model::message::MessageQueue;
use rocketmq_model::result::PullOutcome;
use rocketmq_model::result::PullStatus;
use rocketmq_model::result::QueryResult;
use rocketmq_model::result::SendResult;
use rocketmq_model::result::SendStatus;
use rocketmq_model::topic::TopicConfig;
use rocketmq_model::topic::TopicFilterType;

#[test]
fn message_queue_contract_preserves_wire_order_and_display() {
    let first = MessageQueue::from_parts("TopicA", "BrokerA", 1);
    let second = MessageQueue::from_parts("TopicA", "BrokerA", 2);

    assert!(first < second);
    assert_eq!(
        serde_json::to_string(&first).expect("queue should serialize"),
        r#"{"topic":"TopicA","brokerName":"BrokerA","queueId":1}"#
    );
    assert_eq!(
        first.to_string(),
        "MessageQueue [topic=TopicA, brokerName=BrokerA, queueId=1]"
    );
}

#[test]
fn topic_config_contract_preserves_defaults_and_round_trip() {
    let mut config = TopicConfig::new("TopicA");
    config.topic_filter_type = TopicFilterType::MultiTag;
    config
        .attributes
        .insert(CheetahString::from("message.type"), CheetahString::from("LITE"));

    let json = serde_json::to_string(&config).expect("topic config should serialize");
    let decoded: TopicConfig = serde_json::from_str(&json).expect("topic config should deserialize");

    assert_eq!(decoded, config);
    assert_eq!(TopicConfig::default().read_queue_nums, 16);
    assert_eq!(TopicConfig::default().write_queue_nums, 16);
    assert_eq!(TopicConfig::default().perm, 6);
}

#[test]
fn result_contracts_preserve_status_and_owned_message_order() {
    assert_eq!(SendStatus::FlushDiskTimeout.to_string(), "FLUSH_DISK_TIMEOUT");
    assert_eq!(PullStatus::from(2), PullStatus::NoMatchedMsg);
    assert_eq!(i32::from(PullStatus::OffsetIllegal), 3);

    let send = SendResult::new(
        SendStatus::SendOk,
        Some(CheetahString::from("msg-a")),
        Some("offset-a".to_owned()),
        Some(MessageQueue::from_parts("TopicA", "BrokerA", 1)),
        7,
    );
    let decoded: SendResult =
        serde_json::from_str(&serde_json::to_string(&send).expect("send result should serialize"))
            .expect("send result should deserialize");
    assert_eq!(decoded.get_queue_offset(), 7);

    let query = QueryResult::new(42, vec!["first", "second"]);
    assert_eq!(query.message_list(), &["first", "second"]);

    let outcome = PullOutcome::new(PullStatus::Found, 12, 1, 20, vec!["first", "second"]);
    assert_eq!(outcome.messages(), Some(&["first", "second"][..]));
    assert_eq!(outcome.next_begin_offset(), 12);
}

#[test]
fn pull_outcome_distinguishes_absent_and_present_empty_messages() {
    let absent: PullOutcome<String> = serde_json::from_str(
        r#"{"pull_status":"Found","next_begin_offset":1,"min_offset":0,"max_offset":2,"messages":null}"#,
    )
    .expect("an absent message collection should deserialize");
    let present_empty: PullOutcome<String> = serde_json::from_str(
        r#"{"pull_status":"Found","next_begin_offset":1,"min_offset":0,"max_offset":2,"messages":[]}"#,
    )
    .expect("a present empty message collection should deserialize");

    assert_ne!(absent, present_empty);
    assert_eq!(
        serde_json::to_value(absent).expect("absent outcome should serialize")["messages"],
        serde_json::Value::Null
    );
    assert_eq!(
        serde_json::to_value(present_empty).expect("present empty outcome should serialize")["messages"],
        serde_json::json!([])
    );
}
