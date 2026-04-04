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

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::body::queue_time_span::QueueTimeSpan;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryConsumeTimeSpanBody {
    pub consume_time_span_set: Vec<QueueTimeSpan>,
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_queue::MessageQueue;

    use super::*;

    #[test]
    fn query_consume_time_span_body_uses_java_field_names() {
        let body = QueryConsumeTimeSpanBody {
            consume_time_span_set: vec![QueueTimeSpan {
                message_queue: Some(MessageQueue::from_parts(
                    CheetahString::from_static_str("topic-a"),
                    CheetahString::from_static_str("broker-a"),
                    0,
                )),
                min_time_stamp: 1,
                max_time_stamp: 2,
                consume_time_stamp: 3,
                delay_time: 4,
            }],
        };

        let serialized = serde_json::to_string(&body).expect("serialize query consume time span body");
        assert!(serialized.contains("\"consumeTimeSpanSet\""));
    }
}
