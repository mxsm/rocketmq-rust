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
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::FastCodesHeader;

#[derive(Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct SendMessageResponseHeader {
    msg_id: CheetahString,
    queue_id: i32,
    queue_offset: i64,
    transaction_id: Option<CheetahString>,
    batch_uniq_id: Option<CheetahString>,
}

impl SendMessageResponseHeader {
    pub fn new(
        msg_id: CheetahString,
        queue_id: i32,
        queue_offset: i64,
        transaction_id: Option<CheetahString>,
        batch_uniq_id: Option<CheetahString>,
    ) -> Self {
        SendMessageResponseHeader {
            msg_id,
            queue_id,
            queue_offset,
            transaction_id,
            batch_uniq_id,
        }
    }

    pub fn msg_id(&self) -> &CheetahString {
        &self.msg_id
    }

    pub fn queue_id(&self) -> i32 {
        self.queue_id
    }

    pub fn queue_offset(&self) -> i64 {
        self.queue_offset
    }

    pub fn transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
    }

    pub fn batch_uniq_id(&self) -> Option<&str> {
        self.batch_uniq_id.as_deref()
    }

    pub fn set_msg_id(&mut self, msg_id: impl Into<CheetahString>) {
        self.msg_id = msg_id.into();
    }

    pub fn set_queue_id(&mut self, queue_id: i32) {
        self.queue_id = queue_id;
    }

    pub fn set_queue_offset(&mut self, queue_offset: i64) {
        self.queue_offset = queue_offset;
    }

    pub fn set_transaction_id(&mut self, transaction_id: Option<CheetahString>) {
        self.transaction_id = transaction_id;
    }

    pub fn set_batch_uniq_id(&mut self, batch_uniq_id: Option<CheetahString>) {
        self.batch_uniq_id = batch_uniq_id;
    }
}

impl FastCodesHeader for SendMessageResponseHeader {
    fn encode_fast(&mut self, out: &mut bytes::BytesMut) {
        Self::write_if_not_null(out, "msgId", self.msg_id.as_str());
        Self::write_if_not_null(out, "queueId", self.queue_id.to_string().as_str());
        Self::write_if_not_null(out, "queueOffset", self.queue_offset.to_string().as_str());
        if let Some(ref transaction_id) = self.transaction_id {
            Self::write_if_not_null(out, "transactionId", transaction_id.as_str());
        }
        if let Some(ref batch_uniq_id) = self.batch_uniq_id {
            Self::write_if_not_null(out, "batchUniqId", batch_uniq_id.as_str());
        }
    }

    fn decode_fast(&mut self, fields: &HashMap<CheetahString, CheetahString>) {
        if let Some(str) = fields.get(&CheetahString::from_slice("msgId")) {
            self.msg_id = str.clone();
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("queueId")) {
            self.queue_id = str.parse::<i32>().unwrap_or_default();
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("queueOffset")) {
            self.queue_offset = str.parse::<i64>().unwrap_or_default();
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("transactionId")) {
            self.transaction_id = Some(str.clone());
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("batchUniqId")) {
            self.batch_uniq_id = Some(str.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_message_response_header_new_and_default() {
        let header = SendMessageResponseHeader::new(
            CheetahString::from("msg123"),
            1,
            100,
            Some(CheetahString::from("tx456")),
            Some(CheetahString::from("batch789")),
        );

        assert_eq!(header.msg_id(), "msg123");
        assert_eq!(header.queue_id(), 1);
        assert_eq!(header.queue_offset(), 100);
        assert_eq!(header.transaction_id(), Some("tx456"));
        assert_eq!(header.batch_uniq_id(), Some("batch789"));

        let header = SendMessageResponseHeader::default();

        assert_eq!(header.msg_id(), "");
        assert_eq!(header.queue_id(), 0);
        assert_eq!(header.queue_offset(), 0);
        assert_eq!(header.transaction_id(), None);
        assert_eq!(header.batch_uniq_id(), None);
    }

    #[test]
    fn send_message_response_header_setters_and_getters() {
        let mut header = SendMessageResponseHeader::default();
        header.set_msg_id("newMsgId");
        header.set_queue_id(2);
        header.set_queue_offset(200);
        header.set_transaction_id(Some(CheetahString::from("newTxId")));
        header.set_batch_uniq_id(Some(CheetahString::from("newBatchId")));

        assert_eq!(header.msg_id(), "newMsgId");
        assert_eq!(header.queue_id(), 2);
        assert_eq!(header.queue_offset(), 200);
        assert_eq!(header.transaction_id(), Some("newTxId"));
        assert_eq!(header.batch_uniq_id(), Some("newBatchId"));
    }

    #[test]
    fn send_message_response_header_serialization_and_deserialization() {
        let header = SendMessageResponseHeader::new(
            CheetahString::from("msg123"),
            1,
            100,
            Some(CheetahString::from("tx456")),
            Some(CheetahString::from("batch789")),
        );

        let json = serde_json::to_string(&header).unwrap();
        assert_eq!(
            json,
            r#"{"msgId":"msg123","queueId":1,"queueOffset":100,"transactionId":"tx456","batchUniqId":"batch789"}"#
        );

        let header: SendMessageResponseHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(header.msg_id(), "msg123");
        assert_eq!(header.queue_id(), 1);
        assert_eq!(header.queue_offset(), 100);
        assert_eq!(header.transaction_id(), Some("tx456"));
        assert_eq!(header.batch_uniq_id(), Some("batch789"));
    }

    #[test]
    fn send_message_response_header_encode_decode_fast() {
        let mut header = SendMessageResponseHeader::new(
            CheetahString::from("msg123"),
            1,
            100,
            Some(CheetahString::from("tx456")),
            Some(CheetahString::from("batch789")),
        );

        let mut out = bytes::BytesMut::new();
        header.encode_fast(&mut out);
        assert!(!out.is_empty());

        let mut fields = std::collections::HashMap::new();
        fields.insert(CheetahString::from("msgId"), CheetahString::from("msg123"));
        fields.insert(CheetahString::from("queueId"), CheetahString::from("1"));
        fields.insert(CheetahString::from("queueOffset"), CheetahString::from("100"));
        fields.insert(CheetahString::from("transactionId"), CheetahString::from("tx456"));
        fields.insert(CheetahString::from("batchUniqId"), CheetahString::from("batch789"));

        let mut header = SendMessageResponseHeader::default();
        header.decode_fast(&fields);

        assert_eq!(header.msg_id(), "msg123");
        assert_eq!(header.queue_id(), 1);
        assert_eq!(header.queue_offset(), 100);
        assert_eq!(header.transaction_id(), Some("tx456"));
        assert_eq!(header.batch_uniq_id(), Some("batch789"));
    }
}
