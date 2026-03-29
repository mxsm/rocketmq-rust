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
use rocketmq_common::common::lite::get_lite_topic;
use rocketmq_store::base::message_store::MessageStore;

use crate::broker_runtime::BrokerRuntimeInner;

pub(crate) struct LiteSharding;

impl LiteSharding {
    pub(crate) fn sharding_by_lmq_name<MS: MessageStore>(
        broker_runtime_inner: &BrokerRuntimeInner<MS>,
        parent_topic: &CheetahString,
        lmq_name: &CheetahString,
    ) -> CheetahString {
        let current_broker = broker_runtime_inner.broker_config().broker_name().clone();
        let Some(lite_topic) = get_lite_topic(lmq_name.as_str()) else {
            return current_broker;
        };

        let publish_info = broker_runtime_inner
            .topic_route_info_manager()
            .topic_publish_info_table
            .get(parent_topic)
            .cloned();
        let Some(publish_info) = publish_info.filter(|info| !info.message_queue_list.is_empty()) else {
            return current_broker;
        };

        let bucket = consistent_hash(
            java_string_hash_code(lite_topic.as_str()),
            publish_info.message_queue_list.len(),
        );
        publish_info
            .message_queue_list
            .get(bucket)
            .map(|queue| queue.broker_name().clone())
            .unwrap_or(current_broker)
    }
}

fn java_string_hash_code(value: &str) -> i32 {
    value
        .encode_utf16()
        .fold(0_i32, |hash, unit| hash.wrapping_mul(31).wrapping_add(unit as i32))
}

fn consistent_hash(input: i32, buckets: usize) -> usize {
    if buckets <= 1 {
        return 0;
    }

    let mut state = input as i64;
    let mut candidate = 0_i64;

    loop {
        state = state.wrapping_mul(2_862_933_555_777_941_757).wrapping_add(1);
        let generator = (((state as u64) >> 33) + 1) as f64 / (1_u64 << 31) as f64;
        let next = ((candidate + 1) as f64 / generator) as i64;
        if next < 0 || next >= buckets as i64 {
            return candidate as usize;
        }
        candidate = next;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn java_string_hash_code_matches_java_contract() {
        assert_eq!(java_string_hash_code(""), 0);
        assert_eq!(java_string_hash_code("abc"), 96_354);
        assert_eq!(java_string_hash_code("lite_topic"), 2_023_630_430);
    }

    #[test]
    fn consistent_hash_returns_single_bucket_for_single_queue() {
        assert_eq!(consistent_hash(java_string_hash_code("child-a"), 1), 0);
    }
}
