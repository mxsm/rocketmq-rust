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
use rocketmq_common::common::message::message_queue::MessageQueue;
use serde::Deserialize;
use serde::Serialize;

use crate::producer::send_status::SendStatus;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendResult {
    pub send_status: SendStatus,
    pub msg_id: Option<CheetahString>,
    pub message_queue: Option<MessageQueue>,
    pub queue_offset: u64,
    pub transaction_id: Option<String>,
    pub offset_msg_id: Option<String>,
    pub region_id: Option<String>,
    pub trace_on: bool,
    pub raw_resp_body: Option<Vec<u8>>,
}

impl Default for SendResult {
    fn default() -> Self {
        SendResult {
            send_status: SendStatus::SendOk,
            msg_id: None,
            message_queue: None,
            queue_offset: 0,
            transaction_id: None,
            offset_msg_id: None,
            region_id: None,
            trace_on: true,
            raw_resp_body: None,
        }
    }
}

impl SendResult {
    pub fn new(
        send_status: SendStatus,
        msg_id: Option<CheetahString>,
        offset_msg_id: Option<String>,
        message_queue: Option<MessageQueue>,
        queue_offset: u64,
    ) -> Self {
        SendResult {
            send_status,
            msg_id,
            message_queue,
            queue_offset,
            transaction_id: None,
            offset_msg_id,
            region_id: None,
            trace_on: true,
            raw_resp_body: None,
        }
    }

    pub fn new_with_additional_fields(
        send_status: SendStatus,
        msg_id: Option<CheetahString>,
        message_queue: Option<MessageQueue>,
        queue_offset: u64,
        transaction_id: Option<String>,
        offset_msg_id: Option<String>,
        region_id: Option<String>,
    ) -> Self {
        SendResult {
            send_status,
            msg_id,
            message_queue,
            queue_offset,
            transaction_id,
            offset_msg_id,
            region_id,
            trace_on: true,
            raw_resp_body: None,
        }
    }

    #[inline]
    pub fn is_trace_on(&self) -> bool {
        self.trace_on
    }

    #[inline]
    pub fn set_trace_on(&mut self, trace_on: bool) {
        self.trace_on = trace_on;
    }

    #[inline]
    pub fn set_region_id(&mut self, region_id: String) {
        self.region_id = Some(region_id);
    }

    #[inline]
    pub fn set_msg_id(&mut self, msg_id: CheetahString) {
        self.msg_id = Some(msg_id);
    }

    #[inline]
    pub fn set_send_status(&mut self, send_status: SendStatus) {
        self.send_status = send_status;
    }

    #[inline]
    pub fn set_message_queue(&mut self, message_queue: MessageQueue) {
        self.message_queue = Some(message_queue);
    }

    #[inline]
    pub fn set_queue_offset(&mut self, queue_offset: u64) {
        self.queue_offset = queue_offset;
    }

    #[inline]
    pub fn set_transaction_id(&mut self, transaction_id: String) {
        self.transaction_id = Some(transaction_id);
    }

    #[inline]
    pub fn set_offset_msg_id(&mut self, offset_msg_id: String) {
        self.offset_msg_id = Some(offset_msg_id);
    }

    #[inline]
    pub fn set_raw_resp_body(&mut self, body: Vec<u8>) {
        self.raw_resp_body = Some(body);
    }

    #[inline]
    pub fn get_raw_resp_body(&self) -> Option<&[u8]> {
        self.raw_resp_body.as_deref()
    }
}

impl std::fmt::Display for SendResult {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "SendResult [sendStatus={:?}, msgId={:?}, offsetMsgId={:?}, messageQueue={:?}, queueOffset={}]",
            self.send_status, self.msg_id, self.offset_msg_id, self.message_queue, self.queue_offset,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_result_default() {
        let result = SendResult::default();
        assert_eq!(result.send_status, SendStatus::SendOk);
        assert!(result.msg_id.is_none());
        assert!(result.message_queue.is_none());
        assert_eq!(result.queue_offset, 0);
        assert!(result.transaction_id.is_none());
        assert!(result.offset_msg_id.is_none());
        assert!(result.region_id.is_none());
        assert!(result.is_trace_on());
        assert!(result.get_raw_resp_body().is_none());
    }

    #[test]
    fn test_send_result_new() {
        let send_status = SendStatus::SendOk;
        let msg_id = Some(CheetahString::from("msg_id"));
        let offset_msg_id = Some("offset_msg_id".to_string());
        let message_queue = Some(MessageQueue::default());
        let queue_offset = 123;

        let result = SendResult::new(
            send_status,
            msg_id.clone(),
            offset_msg_id.clone(),
            message_queue.clone(),
            queue_offset,
        );

        assert_eq!(result.send_status, send_status);
        assert_eq!(result.msg_id, msg_id);
        assert_eq!(result.message_queue, message_queue);
        assert_eq!(result.queue_offset, queue_offset);
        assert!(result.transaction_id.is_none());
        assert_eq!(result.offset_msg_id, offset_msg_id);
        assert!(result.region_id.is_none());
        assert!(result.is_trace_on());
        assert!(result.get_raw_resp_body().is_none());
    }

    #[test]
    fn test_send_result_new_with_additional_fields() {
        let send_status = SendStatus::SendOk;
        let msg_id = Some(CheetahString::from("msg_id"));
        let message_queue = Some(MessageQueue::default());
        let queue_offset = 123;
        let transaction_id = Some("transaction_id".to_string());
        let offset_msg_id = Some("offset_msg_id".to_string());
        let region_id = Some("region_id".to_string());

        let result = SendResult::new_with_additional_fields(
            send_status,
            msg_id.clone(),
            message_queue.clone(),
            queue_offset,
            transaction_id.clone(),
            offset_msg_id.clone(),
            region_id.clone(),
        );

        assert_eq!(result.send_status, send_status);
        assert_eq!(result.msg_id, msg_id);
        assert_eq!(result.message_queue, message_queue);
        assert_eq!(result.queue_offset, queue_offset);
        assert_eq!(result.transaction_id, transaction_id);
        assert_eq!(result.offset_msg_id, offset_msg_id);
        assert_eq!(result.region_id, region_id);
        assert!(result.is_trace_on());
        assert!(result.get_raw_resp_body().is_none());
    }

    #[test]
    fn test_send_result_setters_and_getters() {
        let mut result = SendResult::default();

        result.set_trace_on(false);
        assert!(!result.is_trace_on());

        result.set_region_id("region_id".to_string());
        assert_eq!(result.region_id, Some("region_id".to_string()));

        result.set_msg_id(CheetahString::from("msg_id"));
        assert_eq!(result.msg_id, Some(CheetahString::from("msg_id")));

        result.set_send_status(SendStatus::FlushDiskTimeout);
        assert_eq!(result.send_status, SendStatus::FlushDiskTimeout);

        let mq = MessageQueue::default();
        result.set_message_queue(mq.clone());
        assert_eq!(result.message_queue, Some(mq));

        result.set_queue_offset(456);
        assert_eq!(result.queue_offset, 456);

        result.set_transaction_id("transaction_id".to_string());
        assert_eq!(result.transaction_id, Some("transaction_id".to_string()));

        result.set_offset_msg_id("offset_msg_id".to_string());
        assert_eq!(result.offset_msg_id, Some("offset_msg_id".to_string()));

        let body = vec![1, 2, 3];
        result.set_raw_resp_body(body.clone());
        assert_eq!(result.get_raw_resp_body(), Some(body.as_slice()));
    }

    #[test]
    fn test_send_result_serialization_and_deserialization() {
        let send_status = SendStatus::SendOk;
        let msg_id = Some(CheetahString::from("msg_id"));
        let offset_msg_id = Some("offset_msg_id".to_string());
        let message_queue = Some(MessageQueue::default());
        let queue_offset = 123;

        let result = SendResult::new(
            send_status,
            msg_id.clone(),
            offset_msg_id.clone(),
            message_queue.clone(),
            queue_offset,
        );

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: SendResult = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.send_status, result.send_status);
        assert_eq!(deserialized.msg_id, result.msg_id);
        assert_eq!(deserialized.message_queue, result.message_queue);
        assert_eq!(deserialized.queue_offset, result.queue_offset);
        assert_eq!(deserialized.transaction_id, result.transaction_id);
        assert_eq!(deserialized.offset_msg_id, result.offset_msg_id);
        assert_eq!(deserialized.region_id, result.region_id);
        assert_eq!(deserialized.is_trace_on(), result.is_trace_on());
        assert_eq!(deserialized.get_raw_resp_body(), result.get_raw_resp_body());
    }

    #[test]
    fn test_send_result_display() {
        let result = SendResult::default();
        let display = format!("{}", result);
        assert_eq!(
            display,
            "SendResult [sendStatus=SendOk, msgId=None, offsetMsgId=None, messageQueue=None, queueOffset=0]"
        );
    }
}
