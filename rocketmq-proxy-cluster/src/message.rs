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

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_client_rust::proxy_adapter_compat::Message;
use rocketmq_client_rust::proxy_adapter_compat::MessageExt;
use rocketmq_proxy_core::ProxyMessage;
use rocketmq_proxy_core::ProxyMessageExt;

pub(crate) fn message_ext_to_core(message: &MessageExt) -> ProxyMessageExt {
    ProxyMessageExt {
        message: message_to_core(message.message_inner()),
        broker_name: message.broker_name().to_owned(),
        queue_id: message.queue_id(),
        store_size: message.store_size(),
        queue_offset: message.queue_offset(),
        sys_flag: message.sys_flag(),
        born_timestamp: message.born_timestamp(),
        born_host: message.born_host(),
        store_timestamp: message.store_timestamp(),
        store_host: message.store_host(),
        msg_id: message.msg_id().to_string(),
        commit_log_offset: message.commit_log_offset(),
        body_crc: message.body_crc(),
        reconsume_times: message.reconsume_times(),
        prepared_transaction_offset: message.prepared_transaction_offset(),
    }
}

pub(crate) fn message_from_core(message: &ProxyMessage) -> Message {
    let mut client = Message::default();
    client.set_topic(CheetahString::from(message.topic()));
    client.set_flag(message.flag());
    client.set_properties(
        message
            .properties()
            .iter()
            .map(|(key, value)| (CheetahString::from(key.as_str()), CheetahString::from(value.as_str())))
            .collect::<HashMap<_, _>>(),
    );
    client.set_body(message.body().map(Bytes::copy_from_slice));
    *client.transaction_id_mut() = message.transaction_id().map(CheetahString::from);
    client
}

fn message_to_core(message: &Message) -> ProxyMessage {
    let mut core = ProxyMessage::default();
    core.set_topic(message.topic().to_string());
    core.set_body(message.body().map(|body| body.to_vec()));
    core.set_flag(message.flag());
    core.set_properties(
        message
            .properties()
            .as_map()
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
    );
    core.set_transaction_id(message.transaction_id().map(str::to_owned));
    core
}
