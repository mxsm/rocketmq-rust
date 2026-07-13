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

use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum AppendMessageStatus {
    #[default]
    PutOk,
    EndOfFile,
    MessageSizeExceeded,
    PropertiesSizeExceeded,
    UnknownError,
}

impl std::fmt::Display for AppendMessageStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Compatible with Java enums
        match self {
            AppendMessageStatus::PutOk => write!(f, "PUT_OK"),
            AppendMessageStatus::EndOfFile => write!(f, "END_OF_FILE"),
            AppendMessageStatus::MessageSizeExceeded => write!(f, "MESSAGE_SIZE_EXCEEDED"),
            AppendMessageStatus::PropertiesSizeExceeded => write!(f, "PROPERTIES_SIZE_EXCEEDED"),
            AppendMessageStatus::UnknownError => write!(f, "UNKNOWN_ERROR"),
        }
    }
}

type MessageIdSupplier = Arc<dyn Fn() -> String + Send + Sync>;

/// Represents the result of an append message operation.
#[derive(Clone)]
pub struct AppendMessageResult {
    /// Return code.
    pub status: AppendMessageStatus,
    /// Where to start writing.
    pub wrote_offset: i64,
    /// Write Bytes.
    pub wrote_bytes: i32,
    /// Message ID.
    pub msg_id: Option<String>,
    /// Message ID supplier.
    pub msg_id_supplier: Option<MessageIdSupplier>,
    /// Message storage timestamp.
    pub store_timestamp: i64,
    /// Consume queue's offset (step by one).
    pub logics_offset: i64,
    /// Page cache RT.
    pub page_cache_rt: i64,
    /// Message number.
    pub msg_num: i32,
}

impl Default for AppendMessageResult {
    fn default() -> Self {
        Self {
            status: AppendMessageStatus::UnknownError,
            wrote_offset: 0,
            wrote_bytes: 0,
            msg_id: None,
            msg_id_supplier: None,
            store_timestamp: 0,
            logics_offset: 0,
            page_cache_rt: 0,
            msg_num: 1,
        }
    }
}

impl Display for AppendMessageResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AppendMessageResult [status={:?}, wrote_offset={}, wrote_bytes={}, msg_id={:?}, store_timestamp={}, \
             logics_offset={}, page_cache_rt={}, msg_num={}]",
            self.status,
            self.wrote_offset,
            self.wrote_bytes,
            self.msg_id,
            self.store_timestamp,
            self.logics_offset,
            self.page_cache_rt,
            self.msg_num
        )
    }
}

impl AppendMessageResult {
    #[inline]
    pub fn is_ok(&self) -> bool {
        self.status == AppendMessageStatus::PutOk
    }

    pub fn get_message_id(&self) -> Option<String> {
        match self.msg_id_supplier {
            None => self.msg_id.clone(),
            Some(ref msg_id_supplier) => {
                let msg_id = msg_id_supplier();
                Some(msg_id)
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PutMessageContext {
    topic_queue_table_key: String,
    phy_pos: Vec<i64>,
    batch_size: i32,
}

impl PutMessageContext {
    #[inline]
    pub fn new(topic_queue_table_key: String) -> Self {
        PutMessageContext {
            topic_queue_table_key,
            phy_pos: Vec::new(),
            batch_size: 0,
        }
    }

    #[inline]
    pub fn get_topic_queue_table_key(&self) -> &str {
        &self.topic_queue_table_key
    }

    #[inline]
    pub fn get_phy_pos(&self) -> &[i64] {
        &self.phy_pos
    }

    #[inline]
    pub fn set_phy_pos(&mut self, phy_pos: Vec<i64>) {
        self.phy_pos = phy_pos;
    }

    #[inline]
    pub fn get_phy_pos_mut(&mut self) -> &mut [i64] {
        &mut self.phy_pos
    }

    #[inline]
    pub fn get_batch_size(&self) -> i32 {
        self.batch_size
    }

    #[inline]
    pub fn set_batch_size(&mut self, batch_size: i32) {
        self.batch_size = batch_size;
    }

    #[inline]
    pub fn set_topic_queue_table_key(&mut self, topic_queue_table_key: String) {
        self.topic_queue_table_key = topic_queue_table_key;
    }
}

/// Callback interface for compaction append message
pub trait CompactionAppendMsgCallback {
    /// Append messages during compaction
    ///
    /// # Arguments
    ///
    /// * `bb_dest` - The destination buffer to append to
    /// * `file_from_offset` - The offset of the file
    /// * `max_blank` - The maximum blank space
    /// * `bb_src` - The source buffer containing the message to be appended
    ///
    /// # Returns
    ///
    /// The result of the append operation
    fn do_append(
        &self,
        bb_dest: &mut bytes::Bytes,
        file_from_offset: i64,
        max_blank: i32,
        bb_src: &mut bytes::Bytes,
    ) -> AppendMessageResult;
}
