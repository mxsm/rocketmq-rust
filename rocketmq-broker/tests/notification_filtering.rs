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

use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_broker::test_support::run_notification_filter_probe;
use rocketmq_broker::test_support::NotificationFilterProbeMessage;
use rocketmq_model::common::filter::expression_type::ExpressionType;

fn message(tag: Option<&str>, color: &str) -> NotificationFilterProbeMessage {
    NotificationFilterProbeMessage {
        tag: tag.map(CheetahString::from),
        properties: HashMap::from([(CheetahString::from_static_str("color"), CheetahString::from(color))]),
    }
}

#[test]
fn tag_filter_scans_past_non_matching_messages() {
    let result = run_notification_filter_probe(
        ExpressionType::TAG,
        "blue",
        &[message(Some("red"), "red"), message(Some("blue"), "blue")],
        64,
    )
    .expect("valid TAG expression");

    assert!(result.has_message);
    assert_eq!(result.scanned_messages, 2);
    assert!(!result.optimistic);

    let miss = run_notification_filter_probe(ExpressionType::TAG, "blue", &[message(Some("red"), "red")], 64)
        .expect("valid TAG expression");
    assert!(!miss.has_message);
}

#[test]
fn sql92_filter_matches_properties_and_rejects_invalid_syntax() {
    let hit = run_notification_filter_probe(
        ExpressionType::SQL92,
        "color = 'blue'",
        &[message(None, "red"), message(None, "blue")],
        64,
    )
    .expect("valid SQL92 expression");
    assert!(hit.has_message);
    assert_eq!(hit.scanned_messages, 2);

    let miss = run_notification_filter_probe(
        ExpressionType::SQL92,
        "color = 'green'",
        &[message(None, "red"), message(None, "blue")],
        64,
    )
    .expect("valid SQL92 expression");
    assert!(!miss.has_message);

    assert!(run_notification_filter_probe(ExpressionType::SQL92, "color =", &[], 64).is_err());
    assert!(run_notification_filter_probe(ExpressionType::TAG, " || ", &[], 64).is_err());
}

#[test]
fn large_backlog_is_optimistic_and_empty_backlog_is_not() {
    let large = vec![message(Some("red"), "red"); 65];
    let result = run_notification_filter_probe(ExpressionType::TAG, "blue", &large, 64).expect("valid TAG expression");
    assert!(result.has_message);
    assert!(result.optimistic);
    assert_eq!(result.scanned_messages, 0);

    let empty = run_notification_filter_probe(ExpressionType::TAG, "blue", &[], 64).expect("valid TAG expression");
    assert!(!empty.has_message);
    assert!(!empty.optimistic);
}
