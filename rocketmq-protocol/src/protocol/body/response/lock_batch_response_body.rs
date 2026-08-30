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

use std::collections::HashSet;

use rocketmq_model::message::MessageQueue;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct LockBatchResponseBody {
    #[serde(rename = "lockOKMQSet")]
    pub lock_ok_mq_set: HashSet<MessageQueue>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn serde_contract_uses_the_java_field_name_and_round_trips_the_queue() {
        let queue = MessageQueue::from_parts("some_test_topic", "TEST_BROKER", 1);
        let body = LockBatchResponseBody {
            lock_ok_mq_set: HashSet::from([queue]),
        };

        let value = serde_json::to_value(&body).expect("serialize lock batch response");
        let entries = value["lockOKMQSet"].as_array().expect("lockOKMQSet should be an array");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["topic"], "some_test_topic");
        assert_eq!(entries[0]["brokerName"], "TEST_BROKER");
        assert_eq!(entries[0]["queueId"], 1);

        let decoded: LockBatchResponseBody = serde_json::from_value(value).expect("deserialize lock batch response");
        assert_eq!(decoded.lock_ok_mq_set.len(), 1);
        let mq = decoded.lock_ok_mq_set.iter().next().expect("decoded queue");
        assert_eq!(mq.topic(), "some_test_topic");
        assert_eq!(mq.broker_name(), "TEST_BROKER");
        assert_eq!(mq.queue_id(), 1);
    }
}
