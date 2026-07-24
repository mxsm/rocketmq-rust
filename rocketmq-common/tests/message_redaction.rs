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

use base64::Engine;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::message_single::Message;

const BODY_SECRET: &str = "super-secret-message-body";
const PROPERTY_SECRET: &str = "credential-property-value";
const TRANSACTION_SECRET: &str = "transaction-secret-id";

#[test]
fn message_text_and_json_formats_never_expose_sensitive_values_or_reversible_encodings() {
    let message = sensitive_message();
    let mut message_ext = MessageExt {
        message: message.clone(),
        queue_id: 7,
        queue_offset: 42,
        msg_id: CheetahString::from_static_str("controlled-message-id"),
        ..MessageExt::default()
    };
    message_ext.sys_flag = 1;

    let broker_inner = MessageExtBrokerInner {
        message_ext_inner: message_ext.clone(),
        properties_string: CheetahString::from(PROPERTY_SECRET),
        encoded_buff: Some(BytesMut::from(BODY_SECRET.as_bytes())),
        encode_completed: true,
        ..MessageExtBrokerInner::default()
    };

    let text_formats = [
        format!("{message}"),
        format!("{message:?}"),
        format!("{message_ext}"),
        format!("{message_ext:?}"),
        format!("{broker_inner}"),
        format!("{broker_inner:?}"),
    ];
    let json_format =
        serde_json::to_string(&serde_json::json!({ "messages": text_formats })).expect("serialize log record");

    for output in text_formats.iter().chain(std::iter::once(&json_format)) {
        assert_redacted(output);
    }

    assert!(text_formats[0].contains("bodyLen=25"));
    assert!(text_formats[0].contains("propertyCount=2"));
    assert!(text_formats[2].contains("queueId=7"));
    assert!(text_formats[4].contains("encodeCompleted=true"));
}

fn sensitive_message() -> Message {
    Message::builder()
        .topic("SafeTopic")
        .body_slice(BODY_SECRET.as_bytes())
        .raw_property("credential", PROPERTY_SECRET)
        .expect("custom property should be valid")
        .transaction_id(TRANSACTION_SECRET)
        .build_unchecked()
}

fn assert_redacted(output: &str) {
    for secret in [BODY_SECRET, PROPERTY_SECRET, TRANSACTION_SECRET] {
        let base64 = base64::engine::general_purpose::STANDARD.encode(secret);
        let hex = secret
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();

        assert!(!output.contains(secret), "raw sensitive value leaked: {output}");
        assert!(!output.contains(&base64), "base64 sensitive value leaked: {output}");
        assert!(
            !output.to_ascii_lowercase().contains(&hex),
            "hex sensitive value leaked: {output}"
        );
    }
}
