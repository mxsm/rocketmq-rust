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

use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::RequestOrdering;
use rocketmq_transport::RequestOrderingKey;

const CLIENT_LIFECYCLE_KEY: u64 = 0x434c_4945_4e54;
const SEND_NAMESPACE: u64 = 0x5345_4e44;
const OFFSET_NAMESPACE: u64 = 0x4f46_4653_4554;
const TRANSACTION_NAMESPACE: u64 = 0x0054_584e;
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

pub(super) fn broker_request_ordering(request: &RemotingCommand) -> RequestOrdering {
    match RequestCode::from(request.code()) {
        RequestCode::HeartBeat | RequestCode::UnregisterClient | RequestCode::CheckClientConfig => {
            RequestOrdering::Ordered(RequestOrderingKey::new(CLIENT_LIFECYCLE_KEY))
        }
        RequestCode::SendMessage
        | RequestCode::SendMessageV2
        | RequestCode::SendBatchMessage
        | RequestCode::SendReplyMessage
        | RequestCode::SendReplyMessageV2 => ordered_by_fields(
            request,
            SEND_NAMESPACE,
            &[&["producerGroup", "a"], &["topic", "b"], &["queueId", "e"]],
        ),
        RequestCode::UpdateConsumerOffset | RequestCode::QueryConsumerOffset => ordered_by_fields(
            request,
            OFFSET_NAMESPACE,
            &[&["consumerGroup"], &["topic"], &["queueId"]],
        ),
        RequestCode::EndTransaction => ordered_by_fields(
            request,
            TRANSACTION_NAMESPACE,
            &[
                &["producerGroup"],
                &["topic"],
                &["transactionId", "msgId"],
                &["commitLogOffset"],
            ],
        ),
        // Other broker operations already own their required domain locks or
        // are read-only. They remain concurrent within the transport session.
        _ => RequestOrdering::Concurrent,
    }
}

fn ordered_by_fields(request: &RemotingCommand, namespace: u64, fields: &[&[&str]]) -> RequestOrdering {
    let mut hash = FNV_OFFSET_BASIS;
    hash_u64(&mut hash, namespace);
    let ext_fields = request.ext_fields();
    for aliases in fields {
        let value = aliases
            .iter()
            .find_map(|name| ext_fields.and_then(|fields| fields.get(*name)));
        match value {
            Some(value) => hash_bytes(&mut hash, value.as_bytes()),
            None => hash_bytes(&mut hash, []),
        }
        hash_u64(&mut hash, u64::MAX);
    }
    RequestOrdering::Ordered(RequestOrderingKey::new(hash))
}

fn hash_u64(hash: &mut u64, value: u64) {
    hash_bytes(hash, value.to_le_bytes());
}

fn hash_bytes(hash: &mut u64, value: impl AsRef<[u8]>) {
    for byte in value.as_ref() {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(FNV_PRIME);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;

    fn command(code: RequestCode, fields: &[(&str, &str)]) -> RemotingCommand {
        let ext_fields = fields
            .iter()
            .map(|(key, value)| (CheetahString::from(*key), CheetahString::from(*value)))
            .collect::<HashMap<_, _>>();
        RemotingCommand::create_remoting_command(code).set_ext_fields(ext_fields)
    }

    #[test]
    fn client_lifecycle_requests_share_one_session_ordering_key() {
        let heartbeat = command(RequestCode::HeartBeat, &[]);
        let unregister = command(RequestCode::UnregisterClient, &[]);

        assert_eq!(
            broker_request_ordering(&heartbeat),
            broker_request_ordering(&unregister)
        );
    }

    #[test]
    fn sends_are_ordered_per_topic_queue_and_v2_aliases_match_v1_fields() {
        let v1 = command(
            RequestCode::SendMessage,
            &[("producerGroup", "producer-a"), ("topic", "topic-a"), ("queueId", "1")],
        );
        let v2 = command(
            RequestCode::SendMessageV2,
            &[("a", "producer-a"), ("b", "topic-a"), ("e", "1")],
        );
        let other_queue = command(
            RequestCode::SendMessage,
            &[("producerGroup", "producer-a"), ("topic", "topic-a"), ("queueId", "2")],
        );

        assert_eq!(broker_request_ordering(&v1), broker_request_ordering(&v2));
        assert_ne!(broker_request_ordering(&v1), broker_request_ordering(&other_queue));
    }

    #[test]
    fn offset_update_and_query_share_the_same_resource_key() {
        let fields = [("consumerGroup", "group-a"), ("topic", "topic-a"), ("queueId", "3")];
        let update = command(RequestCode::UpdateConsumerOffset, &fields);
        let query = command(RequestCode::QueryConsumerOffset, &fields);

        assert_eq!(broker_request_ordering(&update), broker_request_ordering(&query));
    }

    #[test]
    fn unrelated_read_requests_remain_concurrent() {
        let pull = command(RequestCode::PullMessage, &[]);
        assert_eq!(broker_request_ordering(&pull), RequestOrdering::Concurrent);
    }
}
