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
#[cfg(test)]
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::api::IngressRequestView;
use rocketmq_transport::api::RequestOrdering;
use rocketmq_transport::api::RequestOrderingKey;

const CLIENT_LIFECYCLE_KEY: u64 = 0x434c_4945_4e54;
const SEND_NAMESPACE: u64 = 0x5345_4e44;
const OFFSET_NAMESPACE: u64 = 0x4f46_4653_4554;
const TRANSACTION_NAMESPACE: u64 = 0x0054_584e;
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[cfg(test)]
pub(super) fn broker_command_ordering(request: &RemotingCommand) -> RequestOrdering {
    broker_request_ordering_parts(request.code(), |visitor| request.for_each_ext_field(visitor))
}

/// Runs on the connection reader for every request, so the key reads the header fields it hashes
/// in one walk over them instead of building the request's whole field map.
pub(super) fn broker_request_ordering(ingress: IngressRequestView<'_>) -> RequestOrdering {
    broker_request_ordering_parts(ingress.original_identity().original_code(), |visitor| {
        ingress.for_each_ext_field(visitor)
    })
}

/// Visits every header field of a request once.
type VisitFields<'v, 'a> = dyn FnMut(&'a str, &'a str) + 'v;

fn broker_request_ordering_parts<'a>(
    request_code: i32,
    visit_fields: impl FnOnce(&mut VisitFields<'_, 'a>),
) -> RequestOrdering {
    match RequestCode::from(request_code) {
        RequestCode::HeartBeat | RequestCode::UnregisterClient | RequestCode::CheckClientConfig => {
            RequestOrdering::Ordered(RequestOrderingKey::new(CLIENT_LIFECYCLE_KEY))
        }
        RequestCode::SendMessage
        | RequestCode::SendMessageV2
        | RequestCode::SendBatchMessage
        | RequestCode::SendReplyMessage
        | RequestCode::SendReplyMessageV2 => ordered_by_fields(
            visit_fields,
            SEND_NAMESPACE,
            &[&["producerGroup", "a"], &["topic", "b"], &["queueId", "e"]],
        ),
        RequestCode::UpdateConsumerOffset | RequestCode::QueryConsumerOffset => ordered_by_fields(
            visit_fields,
            OFFSET_NAMESPACE,
            &[&["consumerGroup"], &["topic"], &["queueId"]],
        ),
        RequestCode::EndTransaction => ordered_by_fields(
            visit_fields,
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

/// The most key fields, and names per field, that an ordering below reads.
const MAX_KEY_FIELDS: usize = 4;
const MAX_FIELD_NAMES: usize = 2;

/// Hashes the first named value of each key field. As in the field map, the last occurrence of a
/// repeated header field wins.
fn ordered_by_fields<'a>(
    visit_fields: impl FnOnce(&mut VisitFields<'_, 'a>),
    namespace: u64,
    fields: &[&[&str]],
) -> RequestOrdering {
    debug_assert!(fields.len() <= MAX_KEY_FIELDS && fields.iter().all(|names| names.len() <= MAX_FIELD_NAMES));
    let mut values = [[None::<&'a str>; MAX_FIELD_NAMES]; MAX_KEY_FIELDS];
    visit_fields(&mut |key, value| {
        for (names, slots) in fields.iter().zip(values.iter_mut()) {
            if let Some(slot) = names
                .iter()
                .position(|name| *name == key)
                .and_then(|index| slots.get_mut(index))
            {
                *slot = Some(value);
            }
        }
    });
    let mut hash = FNV_OFFSET_BASIS;
    hash_u64(&mut hash, namespace);
    for (names, slots) in fields.iter().zip(values.iter()) {
        let value = slots.iter().take(names.len()).find_map(|value| *value);
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
            broker_command_ordering(&heartbeat),
            broker_command_ordering(&unregister)
        );
    }

    #[test]
    fn compact_send_header_aliases_match_standard_fields() {
        let standard = command(
            RequestCode::SendMessage,
            &[("producerGroup", "producer-a"), ("topic", "topic-a"), ("queueId", "1")],
        );
        let compact = command(
            RequestCode::SendMessageV2,
            &[("a", "producer-a"), ("b", "topic-a"), ("e", "1")],
        );
        let other_queue = command(
            RequestCode::SendMessage,
            &[("producerGroup", "producer-a"), ("topic", "topic-a"), ("queueId", "2")],
        );

        assert_eq!(broker_command_ordering(&standard), broker_command_ordering(&compact));
        assert_ne!(
            broker_command_ordering(&standard),
            broker_command_ordering(&other_queue)
        );
    }

    #[test]
    fn offset_update_and_query_share_the_same_resource_key() {
        let fields = [("consumerGroup", "group-a"), ("topic", "topic-a"), ("queueId", "3")];
        let update = command(RequestCode::UpdateConsumerOffset, &fields);
        let query = command(RequestCode::QueryConsumerOffset, &fields);

        assert_eq!(broker_command_ordering(&update), broker_command_ordering(&query));
    }

    #[test]
    fn decoded_send_headers_order_like_their_field_maps() {
        use rocketmq_protocol::protocol::SerializeType;

        for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
            for fields in [
                [("a", "producer-a"), ("b", "topic-a"), ("e", "1")],
                [("a", "producer-a"), ("b", "topic-a"), ("e", "2")],
            ] {
                let mut sent = command(RequestCode::SendMessageV2, &fields).set_serialize_type(serialize_type);
                let mut encoded = bytes::BytesMut::new();
                sent.try_fast_header_encode(&mut encoded).unwrap();
                let received = RemotingCommand::decode(&mut encoded).unwrap().unwrap();

                assert_eq!(
                    broker_command_ordering(&received),
                    broker_command_ordering(&sent),
                    "{serialize_type:?} {fields:?}"
                );
            }
        }
    }

    #[test]
    fn standard_send_field_names_take_precedence_over_compact_aliases() {
        let both = command(
            RequestCode::SendMessage,
            &[
                ("a", "compact-producer"),
                ("producerGroup", "producer-a"),
                ("topic", "topic-a"),
                ("queueId", "1"),
            ],
        );
        let standard = command(
            RequestCode::SendMessage,
            &[("producerGroup", "producer-a"), ("topic", "topic-a"), ("queueId", "1")],
        );

        assert_eq!(broker_command_ordering(&both), broker_command_ordering(&standard));
    }

    #[test]
    fn unrelated_read_requests_remain_concurrent() {
        let pull = command(RequestCode::PullMessage, &[]);
        assert_eq!(broker_command_ordering(&pull), RequestOrdering::Concurrent);
    }
}
