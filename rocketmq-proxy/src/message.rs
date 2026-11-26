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
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_single::Message;
use rocketmq_proxy_core::ProxyMessage;
use rocketmq_proxy_core::ProxyMessageExt;

pub(crate) fn message_to_core(message: &Message) -> ProxyMessage {
    let mut core = ProxyMessage::default();
    core.set_topic(message.topic().clone());
    core.set_body_bytes(message.body());
    core.set_flag(message.flag());
    core.set_properties(message.properties().as_map().clone());
    core.set_transaction_id(message.transaction_id());
    core
}

pub(crate) fn message_ext_from_core(message: &ProxyMessageExt) -> MessageExt {
    MessageExt {
        message: message_from_core(&message.message),
        broker_name: CheetahString::from(message.broker_name.as_str()),
        queue_id: message.queue_id,
        store_size: message.store_size,
        queue_offset: message.queue_offset,
        sys_flag: message.sys_flag,
        born_timestamp: message.born_timestamp,
        born_host: message.born_host,
        store_timestamp: message.store_timestamp,
        store_host: message.store_host,
        msg_id: CheetahString::from(message.msg_id.as_str()),
        commit_log_offset: message.commit_log_offset,
        body_crc: message.body_crc,
        reconsume_times: message.reconsume_times,
        prepared_transaction_offset: message.prepared_transaction_offset,
    }
}

pub(crate) fn message_from_core(message: &ProxyMessage) -> Message {
    let mut facade = Message::default();
    facade.set_topic(message.topic().clone());
    facade.set_flag(message.flag());
    facade.set_properties(message.properties().clone());
    facade.set_body(message.body_bytes().cloned());
    *facade.transaction_id_mut() = message.transaction_id().map(CheetahString::from);
    facade
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_model::common::message::message_single::Message;

    use super::message_from_core;
    use super::message_to_core;

    #[test]
    fn remoting_round_trip_keeps_fields_and_shares_text() {
        let topic = CheetahString::from("a-topic-longer-than-twenty-three-bytes");
        let unique_key = CheetahString::from("0A0B0C0D1F2E3D4C5B6A00001234ABCD");
        let mut message = Message::default();
        message.set_topic(topic.clone());
        message.set_flag(7);
        message.set_body(Some(Bytes::from_static(b"body")));
        message.set_properties(HashMap::from([
            (CheetahString::from_static_str("UNIQ_KEY"), unique_key.clone()),
            (CheetahString::from_static_str("TAGS"), CheetahString::from("TagA")),
        ]));
        *message.transaction_id_mut() = Some(CheetahString::from("transaction-1"));

        let core = message_to_core(&message);
        let round_trip = message_from_core(&core);

        assert_eq!(round_trip.topic(), &topic);
        assert_eq!(round_trip.flag(), 7);
        assert_eq!(round_trip.body().as_deref(), Some(&b"body"[..]));
        assert_eq!(round_trip.properties().as_map(), message.properties().as_map());
        assert_eq!(round_trip.transaction_id(), Some("transaction-1"));
        // Long text is shared with the original message rather than copied.
        assert_eq!(core.topic().as_ptr(), topic.as_ptr());
        assert_eq!(round_trip.topic().as_ptr(), topic.as_ptr());
        assert_eq!(
            round_trip.properties().as_map()["UNIQ_KEY"].as_ptr(),
            unique_key.as_ptr()
        );
    }
}
