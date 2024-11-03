/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use bytes::Bytes;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::MessageAccessor::MessageAccessor;

use crate::common::client_error_code::ClientErrorCode;
use crate::error::MQClientError::MQClientErr;
use crate::Result;

pub struct MessageUtil;

impl MessageUtil {
    pub fn create_reply_message(request_message: &Message, body: &[u8]) -> Result<Message> {
        let mut reply_message = Message::default();
        let cluster = request_message.get_property(MessageConst::PROPERTY_CLUSTER);
        if let Some(cluster) = cluster {
            reply_message.set_body(Bytes::copy_from_slice(body));
            let reply_topic = mix_all::get_retry_topic(&cluster);
            reply_message.set_topic(reply_topic);
            MessageAccessor::put_property(
                &mut reply_message,
                MessageConst::PROPERTY_MESSAGE_TYPE.to_owned(),
                mix_all::REPLY_MESSAGE_FLAG.to_owned(),
            );
            if let Some(reply_to) =
                request_message.get_property(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT)
            {
                MessageAccessor::put_property(
                    &mut reply_message,
                    MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT.to_owned(),
                    reply_to,
                );
            }
            if let Some(correlation_id) =
                request_message.get_property(MessageConst::PROPERTY_CORRELATION_ID)
            {
                MessageAccessor::put_property(
                    &mut reply_message,
                    MessageConst::PROPERTY_CORRELATION_ID.to_owned(),
                    correlation_id,
                );
            }
            if let Some(ttl) = request_message.get_property(MessageConst::PROPERTY_MESSAGE_TTL) {
                MessageAccessor::put_property(
                    &mut reply_message,
                    MessageConst::PROPERTY_MESSAGE_TTL.to_owned(),
                    ttl,
                );
            }
            Ok(reply_message)
        } else {
            Err(MQClientErr(
                ClientErrorCode::CREATE_REPLY_MESSAGE_EXCEPTION,
                format!(
                    "create reply message fail, requestMessage error, property[{}] is null.",
                    MessageConst::PROPERTY_CLUSTER
                ),
            ))
        }
    }

    pub fn get_reply_to_client(reply_message: &Message) -> Option<String> {
        reply_message.get_property(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rocketmq_common::common::message::message_single::Message;
    use rocketmq_common::common::message::MessageConst;
    use rocketmq_common::common::mix_all;
    use rocketmq_common::MessageAccessor::MessageAccessor;

    use super::*;
    use crate::common::client_error_code::ClientErrorCode;
    use crate::error::MQClientError::MQClientErr;

    fn create_test_message(properties: HashMap<&str, &str>) -> Message {
        let mut message = Message::default();
        for (key, value) in properties {
            MessageAccessor::put_property(&mut message, key.to_owned(), value.to_owned());
        }
        message
    }

    #[test]
    fn create_reply_message_success() {
        let properties = HashMap::from([
            (MessageConst::PROPERTY_CLUSTER, "testCluster"),
            (MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT, "client1"),
            (MessageConst::PROPERTY_CORRELATION_ID, "correlation1"),
            (MessageConst::PROPERTY_MESSAGE_TTL, "1000"),
        ]);
        let request_message = create_test_message(properties);
        let body = b"reply body";

        let result = MessageUtil::create_reply_message(&request_message, body).unwrap();

        assert_eq!(result.get_topic(), mix_all::get_retry_topic("testCluster"));
        assert_eq!(
            result.get_property(MessageConst::PROPERTY_MESSAGE_TYPE),
            Some(mix_all::REPLY_MESSAGE_FLAG.to_string())
        );
        assert_eq!(
            result.get_property(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT),
            Some("client1".to_string())
        );
        assert_eq!(
            result.get_property(MessageConst::PROPERTY_CORRELATION_ID),
            Some("correlation1".to_string())
        );
        assert_eq!(
            result.get_property(MessageConst::PROPERTY_MESSAGE_TTL),
            Some("1000".to_string())
        );
    }

    #[test]
    fn create_reply_message_missing_cluster() {
        let properties = HashMap::new();
        let request_message = create_test_message(properties);
        let body = b"reply body";

        let result = MessageUtil::create_reply_message(&request_message, body);

        assert!(result.is_err());
        if let Err(MQClientErr(code, message)) = result {
            assert_eq!(code, ClientErrorCode::CREATE_REPLY_MESSAGE_EXCEPTION);
            assert!(message.contains("property[CLUSTER] is null"));
        }
    }

    #[test]
    fn get_reply_to_client_success() {
        let properties =
            HashMap::from([(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT, "client1")]);
        let reply_message = create_test_message(properties);

        let result = MessageUtil::get_reply_to_client(&reply_message);

        assert_eq!(result, Some("client1".to_string()));
    }

    #[test]
    fn get_reply_to_client_missing_property() {
        let properties = HashMap::new();
        let reply_message = create_test_message(properties);

        let result = MessageUtil::get_reply_to_client(&reply_message);

        assert_eq!(result, None);
    }
}
