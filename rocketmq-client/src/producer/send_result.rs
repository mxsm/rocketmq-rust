//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

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
