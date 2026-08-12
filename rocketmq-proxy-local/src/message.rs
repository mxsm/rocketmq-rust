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

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_single::Message;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_proxy_core::ProxyMessage;
use rocketmq_proxy_core::ProxyMessageExt;

pub(crate) fn message_ext_to_core(message: &MessageExt) -> ProxyMessageExt {
    let inner = message.message_inner();
    let mut core = ProxyMessage::default();
    core.set_topic(inner.topic().to_string());
    core.set_body_bytes(inner.body());
    core.set_flag(inner.flag());
    core.set_properties(
        inner
            .properties()
            .as_map()
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
    );
    core.set_transaction_id(inner.transaction_id().map(str::to_owned));

    ProxyMessageExt {
        message: core,
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

pub(crate) fn message_properties_from_core(message: &ProxyMessage) -> HashMap<CheetahString, CheetahString> {
    message
        .properties()
        .iter()
        .map(|(key, value)| (CheetahString::from(key.as_str()), CheetahString::from(value.as_str())))
        .collect()
}

pub(crate) fn message_from_core(message: &ProxyMessage) -> Result<Message, RocketMQError> {
    let body = message.body_bytes().cloned().ok_or_else(|| {
        RocketMQError::request_body_invalid(
            "sendMessage",
            format!("message body is missing for topic '{}'", message.topic()),
        )
    })?;
    let mut model = Message::builder()
        .topic(message.topic())
        .body(body)
        .flag_bits(message.flag())
        .build()?;
    model.set_properties(message_properties_from_core(message));
    if let Some(transaction_id) = message.transaction_id() {
        model.set_transaction_id(CheetahString::from(transaction_id));
    }
    Ok(model)
}
