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

use std::fmt;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use crate::message::MessageQueue;

/// Outcome reported by a producer send operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SendStatus {
    #[default]
    SendOk,
    FlushDiskTimeout,
    FlushSlaveTimeout,
    SlaveNotAvailable,
}

impl SendStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::SendOk => "SEND_OK",
            Self::FlushDiskTimeout => "FLUSH_DISK_TIMEOUT",
            Self::FlushSlaveTimeout => "FLUSH_SLAVE_TIMEOUT",
            Self::SlaveNotAvailable => "SLAVE_NOT_AVAILABLE",
        }
    }
}

impl Serialize for SendStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for SendStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        match value.as_str() {
            "SEND_OK" => Ok(Self::SendOk),
            "FLUSH_DISK_TIMEOUT" => Ok(Self::FlushDiskTimeout),
            "FLUSH_SLAVE_TIMEOUT" => Ok(Self::FlushSlaveTimeout),
            "SLAVE_NOT_AVAILABLE" => Ok(Self::SlaveNotAvailable),
            _ => Err(serde::de::Error::unknown_variant(
                &value,
                &[
                    "SEND_OK",
                    "FLUSH_DISK_TIMEOUT",
                    "FLUSH_SLAVE_TIMEOUT",
                    "SLAVE_NOT_AVAILABLE",
                ],
            )),
        }
    }
}

impl fmt::Display for SendStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Producer send result without client lifecycle state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SendResult {
    pub send_status: SendStatus,
    pub msg_id: Option<CheetahString>,
    pub message_queue: Option<MessageQueue>,
    pub queue_offset: u64,
    pub transaction_id: Option<String>,
    pub offset_msg_id: Option<String>,
    pub recall_handle: Option<String>,
    pub region_id: Option<String>,
    pub trace_on: bool,
    pub raw_resp_body: Option<Vec<u8>>,
}

impl Default for SendResult {
    fn default() -> Self {
        Self {
            send_status: SendStatus::SendOk,
            msg_id: None,
            message_queue: None,
            queue_offset: 0,
            transaction_id: None,
            offset_msg_id: None,
            recall_handle: None,
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
        Self {
            send_status,
            msg_id,
            message_queue,
            queue_offset,
            offset_msg_id,
            ..Self::default()
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
        Self {
            send_status,
            msg_id,
            message_queue,
            queue_offset,
            transaction_id,
            offset_msg_id,
            region_id,
            ..Self::default()
        }
    }

    pub fn encoder_send_result_to_json<T: Serialize>(value: &T) -> serde_json::Result<String> {
        serde_json::to_string(value)
    }

    pub fn decoder_send_result_from_json(json: &str) -> serde_json::Result<Self> {
        serde_json::from_str(json)
    }

    pub fn is_trace_on(&self) -> bool {
        self.trace_on
    }

    pub fn set_trace_on(&mut self, trace_on: bool) {
        self.trace_on = trace_on;
    }

    pub fn get_region_id(&self) -> Option<&str> {
        self.region_id.as_deref()
    }

    pub fn set_region_id(&mut self, region_id: String) {
        self.region_id = Some(region_id);
    }

    pub fn get_msg_id(&self) -> Option<&CheetahString> {
        self.msg_id.as_ref()
    }

    pub fn set_msg_id(&mut self, msg_id: CheetahString) {
        self.msg_id = Some(msg_id);
    }

    pub fn get_send_status(&self) -> SendStatus {
        self.send_status
    }

    pub fn set_send_status(&mut self, send_status: SendStatus) {
        self.send_status = send_status;
    }

    pub fn get_message_queue(&self) -> Option<&MessageQueue> {
        self.message_queue.as_ref()
    }

    pub fn set_message_queue(&mut self, message_queue: MessageQueue) {
        self.message_queue = Some(message_queue);
    }

    pub fn get_queue_offset(&self) -> u64 {
        self.queue_offset
    }

    pub fn set_queue_offset(&mut self, queue_offset: u64) {
        self.queue_offset = queue_offset;
    }

    pub fn get_transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
    }

    pub fn set_transaction_id(&mut self, transaction_id: String) {
        self.transaction_id = Some(transaction_id);
    }

    pub fn get_offset_msg_id(&self) -> Option<&str> {
        self.offset_msg_id.as_deref()
    }

    pub fn set_offset_msg_id(&mut self, offset_msg_id: String) {
        self.offset_msg_id = Some(offset_msg_id);
    }

    pub fn set_recall_handle(&mut self, recall_handle: String) {
        self.recall_handle = Some(recall_handle);
    }

    pub fn recall_handle(&self) -> Option<&str> {
        self.recall_handle.as_deref()
    }

    pub fn get_recall_handle(&self) -> Option<&str> {
        self.recall_handle()
    }

    pub fn set_raw_resp_body(&mut self, body: Vec<u8>) {
        self.raw_resp_body = Some(body);
    }

    pub fn get_raw_resp_body(&self) -> Option<&[u8]> {
        self.raw_resp_body.as_deref()
    }
}

impl fmt::Display for SendResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn java_option<T: fmt::Display>(value: Option<&T>) -> String {
            value.map_or_else(|| "null".to_owned(), ToString::to_string)
        }

        write!(
            f,
            "SendResult [sendStatus={}, msgId={}, offsetMsgId={}, messageQueue={}, queueOffset={}, recallHandle={}]",
            self.send_status,
            java_option(self.msg_id.as_ref()),
            java_option(self.offset_msg_id.as_ref()),
            java_option(self.message_queue.as_ref()),
            self.queue_offset,
            java_option(self.recall_handle.as_ref()),
        )
    }
}

/// Query result parameterized by its owned message representation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryResult<T> {
    index_last_update_timestamp: u64,
    message_list: Vec<T>,
}

impl<T> QueryResult<T> {
    pub fn new(index_last_update_timestamp: u64, message_list: Vec<T>) -> Self {
        Self {
            index_last_update_timestamp,
            message_list,
        }
    }

    pub fn index_last_update_timestamp(&self) -> u64 {
        self.index_last_update_timestamp
    }

    pub fn message_list(&self) -> &Vec<T> {
        &self.message_list
    }
}

impl<T: fmt::Display> fmt::Display for QueryResult<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let messages = self
            .message_list
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            f,
            "QueryResult [indexLastUpdateTimestamp={}, messageList=[{}]]",
            self.index_last_update_timestamp, messages
        )
    }
}

/// Consumer pull status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum PullStatus {
    #[default]
    Found,
    NoNewMsg,
    NoMatchedMsg,
    OffsetIllegal,
}

impl From<i32> for PullStatus {
    fn from(value: i32) -> Self {
        match value {
            1 => Self::NoNewMsg,
            2 => Self::NoMatchedMsg,
            3 => Self::OffsetIllegal,
            _ => Self::Found,
        }
    }
}

impl From<PullStatus> for i32 {
    fn from(value: PullStatus) -> Self {
        match value {
            PullStatus::Found => 0,
            PullStatus::NoNewMsg => 1,
            PullStatus::NoMatchedMsg => 2,
            PullStatus::OffsetIllegal => 3,
        }
    }
}

impl fmt::Display for PullStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Found => "FOUND",
            Self::NoNewMsg => "NO_NEW_MSG",
            Self::NoMatchedMsg => "NO_MATCHED_MSG",
            Self::OffsetIllegal => "OFFSET_ILLEGAL",
        })
    }
}

/// Immutable pull result that owns its message values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullOutcome<T> {
    pull_status: PullStatus,
    next_begin_offset: u64,
    min_offset: u64,
    max_offset: u64,
    messages: Option<Vec<T>>,
}

impl<T> PullOutcome<T> {
    pub fn new<M>(
        pull_status: PullStatus,
        next_begin_offset: u64,
        min_offset: u64,
        max_offset: u64,
        messages: M,
    ) -> Self
    where
        M: Into<Option<Vec<T>>>,
    {
        Self {
            pull_status,
            next_begin_offset,
            min_offset,
            max_offset,
            messages: messages.into(),
        }
    }

    pub fn pull_status(&self) -> PullStatus {
        self.pull_status
    }

    pub fn next_begin_offset(&self) -> u64 {
        self.next_begin_offset
    }

    pub fn min_offset(&self) -> u64 {
        self.min_offset
    }

    pub fn max_offset(&self) -> u64 {
        self.max_offset
    }

    pub fn messages(&self) -> Option<&[T]> {
        self.messages.as_deref()
    }

    pub fn into_messages(self) -> Option<Vec<T>> {
        self.messages
    }
}
