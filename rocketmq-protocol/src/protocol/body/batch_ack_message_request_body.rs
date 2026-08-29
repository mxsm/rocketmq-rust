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
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::body::batch_ack::BatchAck;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchAckMessageRequestBody {
    pub broker_name: CheetahString,
    pub acks: Vec<BatchAck>,
}

#[cfg(test)]
mod tests {
    use bitvec::prelude::*;
    use cheetah_string::CheetahString;

    use super::*;
    use crate::protocol::body::batch_ack::SerializableBitVec;

    #[test]
    fn serde_round_trip_preserves_the_batch_ack_request() {
        let body = BatchAckMessageRequestBody {
            broker_name: CheetahString::from_static_str("broker1"),
            acks: vec![BatchAck {
                consumer_group: CheetahString::from_static_str("group1"),
                topic: CheetahString::from_static_str("topic1"),
                retry: CheetahString::from_static_str("1"),
                start_offset: 100,
                queue_id: 1,
                revive_queue_id: 2,
                pop_time: 123456789,
                invisible_time: 987654321,
                bit_set: SerializableBitVec(BitVec::from_element(8)),
            }],
        };

        let encoded = serde_json::to_vec(&body).expect("serialize batch ACK request");
        let decoded: BatchAckMessageRequestBody =
            serde_json::from_slice(&encoded).expect("deserialize batch ACK request");
        assert_eq!(decoded.broker_name, "broker1");
        assert_eq!(decoded.acks.len(), 1);
        assert_eq!(decoded.acks[0].consumer_group, "group1");
    }
}
