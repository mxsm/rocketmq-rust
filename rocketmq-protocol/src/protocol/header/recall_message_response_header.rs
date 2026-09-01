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
use rocketmq_macros::RequestHeaderCodecV3;
use serde::Deserialize;
use serde::Serialize;

/// Response header for message recall operation.
///
/// This header is returned by the broker after processing a recall message request.
/// It contains the message ID of the recalled message.
#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV3)]
#[serde(rename_all = "camelCase")]
#[header(
    type_id = "rocketmq_protocol::protocol::header::recall_message_response_header::RecallMessageResponseHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.RecallMessageResponseHeader"
)]
pub struct RecallMessageResponseHeader {
    /// Message ID of the recalled message (required).
    #[header(required)]
    pub msg_id: CheetahString,
}

impl RecallMessageResponseHeader {
    pub fn new(msg_id: impl Into<CheetahString>) -> Self {
        Self { msg_id: msg_id.into() }
    }

    pub fn msg_id(&self) -> &CheetahString {
        &self.msg_id
    }

    pub fn set_msg_id(&mut self, msg_id: impl Into<CheetahString>) {
        self.msg_id = msg_id.into();
    }
}

impl std::fmt::Display for RecallMessageResponseHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecallMessageResponseHeader {{ msg_id: {} }}", self.msg_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_includes_message_id() {
        let mut body = RecallMessageResponseHeader::new("initial_message");
        body.set_msg_id("some_message");
        let display_output = format!("{}", body);
        assert_eq!(display_output, "RecallMessageResponseHeader { msg_id: some_message }");
    }
    #[test]
    fn recall_message_serialisation() {
        let body = RecallMessageResponseHeader::new("some_message");

        let json = serde_json::to_string(&body).unwrap();
        assert!(json.contains("\"msgId\":\"some_message\""));
    }
    #[test]
    fn recall_message_deserialisation() {
        let json = r#"{"msgId": "some_message"}"#;

        let body: RecallMessageResponseHeader = serde_json::from_str(json).unwrap();
        assert_eq!(body.msg_id(), &CheetahString::from("some_message"));
    }
}
