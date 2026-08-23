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

#![recursion_limit = "256"]

use rocketmq_client_rust::pop_retry_subscription_topics;
use rocketmq_client_rust::resolve_pop_retry_topic;
use rocketmq_model::common::pop_retry_policy::PopRetryTopicVersion;

#[test]
fn push_rebalance_subscription_set_covers_both_retry_versions() {
    let topics = pop_retry_subscription_topics("orders", "consumer-a");

    assert_eq!(topics[0].as_str(), "%RETRY%consumer-a_orders");
    assert_eq!(topics[1].as_str(), "%RETRY%consumer-a+orders");
    assert_eq!(
        resolve_pop_retry_topic(topics[0].as_str(), "consumer-a"),
        Some((PopRetryTopicVersion::V1, "orders"))
    );
    assert_eq!(
        resolve_pop_retry_topic(topics[1].as_str(), "consumer-a"),
        Some((PopRetryTopicVersion::V2, "orders"))
    );
}

#[test]
fn retry_codec_rejects_another_groups_or_plain_retry_topic() {
    assert_eq!(resolve_pop_retry_topic("%RETRY%consumer-b+orders", "consumer-a"), None);
    assert_eq!(resolve_pop_retry_topic("%RETRY%consumer-a", "consumer-a"), None);
}
