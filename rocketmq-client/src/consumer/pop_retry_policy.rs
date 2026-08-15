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
use rocketmq_model::common::key_builder::KeyBuilder;
use rocketmq_model::common::pop_retry_policy::PopRetryTopicVersion;

/// Returns both POP retry topics that a push-mode subscription may inherit
/// while a Broker is inside the supported v1/v2 migration window.
pub fn pop_retry_subscription_topics(topic: &str, consumer_group: &str) -> [CheetahString; 2] {
    [
        CheetahString::from_string(KeyBuilder::build_pop_retry_topic_for_version(
            topic,
            consumer_group,
            PopRetryTopicVersion::V1,
        )),
        CheetahString::from_string(KeyBuilder::build_pop_retry_topic_for_version(
            topic,
            consumer_group,
            PopRetryTopicVersion::V2,
        )),
    ]
}

/// Resolves an observed POP retry topic through the shared v1/v2 codec.
pub fn resolve_pop_retry_topic<'a>(
    retry_topic: &'a str,
    consumer_group: &str,
) -> Option<(PopRetryTopicVersion, &'a str)> {
    KeyBuilder::parse_pop_retry_topic(retry_topic, consumer_group)
}
