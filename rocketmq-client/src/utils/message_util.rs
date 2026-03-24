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

use std::sync::LazyLock;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::mix_all;
use rocketmq_common::MessageAccessor::MessageAccessor;

use crate::common::client_error_code::ClientErrorCode;

// Cached static strings to avoid repeated allocations
static PROPERTY_CLUSTER: LazyLock<CheetahString> =
    LazyLock::new(|| CheetahString::from_static_str(MessageConst::PROPERTY_CLUSTER));
static PROPERTY_MESSAGE_TYPE: LazyLock<CheetahString> =
    LazyLock::new(|| CheetahString::from_static_str(MessageConst::PROPERTY_MESSAGE_TYPE));
static REPLY_MESSAGE_FLAG: LazyLock<CheetahString> =
    LazyLock::new(|| CheetahString::from_static_str(mix_all::REPLY_MESSAGE_FLAG));
static PROPERTY_MESSAGE_REPLY_TO_CLIENT: LazyLock<CheetahString> =
    LazyLock::new(|| CheetahString::from_static_str(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT));
static PROPERTY_CORRELATION_ID: LazyLock<CheetahString> =
    LazyLock::new(|| CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID));
static PROPERTY_MESSAGE_TTL: LazyLock<CheetahString> =
    LazyLock::new(|| CheetahString::from_static_str(MessageConst::PROPERTY_MESSAGE_TTL));

pub struct MessageUtil;

impl MessageUtil {
    pub fn create_reply_message(request_message: &Message, body: &[u8]) -> rocketmq_error::RocketMQResult<Message> {
        // Early return: check required cluster property
        let Some(cluster) = request_message.property(&PROPERTY_CLUSTER) else {
            return Err(mq_client_err!(
                ClientErrorCode::CREATE_REPLY_MESSAGE_EXCEPTION,
                format!(
                    "create reply message fail, requestMessage error, property[{}] is null.",
                    MessageConst::PROPERTY_CLUSTER
                )
            ));
        };

        let mut reply_message = Message::default();
        reply_message.set_body(Some(Bytes::copy_from_slice(body)));

        let reply_topic = mix_all::get_retry_topic(cluster);
        reply_message.set_topic(CheetahString::from_string(reply_topic));

        // Set message type using cached static string
        MessageAccessor::put_property(
            &mut reply_message,
            PROPERTY_MESSAGE_TYPE.clone(),
            REPLY_MESSAGE_FLAG.clone(),
        );

        // Copy optional properties if present
        if let Some(reply_to) = request_message.property(&PROPERTY_MESSAGE_REPLY_TO_CLIENT) {
            MessageAccessor::put_property(
                &mut reply_message,
                PROPERTY_MESSAGE_REPLY_TO_CLIENT.clone(),
                CheetahString::from_slice(reply_to),
            );
        }

        if let Some(correlation_id) = request_message.property(&PROPERTY_CORRELATION_ID) {
            MessageAccessor::put_property(
                &mut reply_message,
                PROPERTY_CORRELATION_ID.clone(),
                CheetahString::from_slice(correlation_id),
            );
        }

        if let Some(ttl) = request_message.property(&PROPERTY_MESSAGE_TTL) {
            MessageAccessor::put_property(
                &mut reply_message,
                PROPERTY_MESSAGE_TTL.clone(),
                CheetahString::from_slice(ttl),
            );
        }

        Ok(reply_message)
    }

    pub fn get_reply_to_client(reply_message: &Message) -> Option<CheetahString> {
        reply_message
            .property(&PROPERTY_MESSAGE_REPLY_TO_CLIENT)
            .map(CheetahString::from_slice)
    }
}
