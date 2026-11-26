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
use std::net::SocketAddr;

use cheetah_string::CheetahString;

pub mod finalized_append;
pub mod micro_batch;
pub mod prepared_payload;
pub mod sequencer;

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

/// What an appended message's offset message ID is built from.
///
/// The append records these values under the CommitLog writer lock without allocating; the ID
/// text, uppercase hexadecimal of the store host followed by the physical offset, is built only
/// when a caller asks for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageIdSource {
    /// One message written by `store_host` at physical `offset`.
    Single { store_host: SocketAddr, offset: i64 },
    /// A batch: one ID per frame offset, separated by commas, and empty when no frame was written.
    Batch { store_host: SocketAddr, offsets: Vec<i64> },
}

impl MessageIdSource {
    /// The longest ID: a 16-byte IPv6 address, a 4-byte port and an 8-byte offset, in hexadecimal.
    const MAX_ID_LEN: usize = (16 + 4 + 8) * 2;

    /// Builds the message ID text.
    pub fn to_message_id(&self) -> String {
        match self {
            Self::Single { store_host, offset } => {
                let mut text = [0; Self::MAX_ID_LEN];
                Self::id_text(*store_host, *offset, &mut text).to_owned()
            }
            Self::Batch { store_host, offsets } => {
                let mut ids = String::with_capacity(offsets.len() * (Self::MAX_ID_LEN + 1));
                let mut text = [0; Self::MAX_ID_LEN];
                for (index, offset) in offsets.iter().enumerate() {
                    if index > 0 {
                        ids.push(',');
                    }
                    ids.push_str(Self::id_text(*store_host, *offset, &mut text));
                }
                ids
            }
        }
    }

    /// Builds the message ID text as a [`CheetahString`]; a single message takes one allocation.
    pub fn to_message_id_cheetah(&self) -> CheetahString {
        match self {
            Self::Single { store_host, offset } => {
                let mut text = [0; Self::MAX_ID_LEN];
                CheetahString::from_slice(Self::id_text(*store_host, *offset, &mut text))
            }
            Self::Batch { .. } => CheetahString::from_string(self.to_message_id()),
        }
    }

    /// Writes the uppercase hexadecimal of the store host address, its port as a big-endian
    /// `i32`, and `offset` as a big-endian `i64`, the layout of a stored message's host fields.
    fn id_text(store_host: SocketAddr, offset: i64, text: &mut [u8; Self::MAX_ID_LEN]) -> &str {
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        let mut raw = [0u8; 28];
        let ip_len = match store_host {
            SocketAddr::V4(address) => {
                raw[..4].copy_from_slice(&address.ip().octets());
                4
            }
            SocketAddr::V6(address) => {
                raw[..16].copy_from_slice(&address.ip().octets());
                16
            }
        };
        raw[ip_len..ip_len + 4].copy_from_slice(&i32::from(store_host.port()).to_be_bytes());
        raw[ip_len + 4..ip_len + 12].copy_from_slice(&offset.to_be_bytes());
        let raw = &raw[..ip_len + 12];
        let text = &mut text[..raw.len() * 2];
        for (pair, byte) in text.chunks_exact_mut(2).zip(raw) {
            pair[0] = HEX[usize::from(byte >> 4)];
            pair[1] = HEX[usize::from(byte & 0x0f)];
        }
        // Hexadecimal digits are ASCII.
        std::str::from_utf8(text).unwrap_or_default()
    }
}

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
    /// The values the message ID is built from on request.
    pub msg_id_source: Option<MessageIdSource>,
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
            msg_id_source: None,
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

    /// Builds the message ID from its source, or copies the stored ID, as a [`CheetahString`].
    pub fn get_message_id_cheetah(&self) -> Option<CheetahString> {
        match &self.msg_id_source {
            Some(source) => Some(source.to_message_id_cheetah()),
            None => self.msg_id.as_deref().map(CheetahString::from_slice),
        }
    }

    /// Builds the message ID from its source, or clones the stored ID.
    pub fn get_message_id(&self) -> Option<String> {
        match &self.msg_id_source {
            Some(source) => Some(source.to_message_id()),
            None => self.msg_id.clone(),
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
