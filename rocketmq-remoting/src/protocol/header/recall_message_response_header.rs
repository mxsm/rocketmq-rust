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
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

/// Response header for message recall operation.
///
/// This header is returned by the broker after processing a recall message request.
/// It contains the message ID of the recalled message.
#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct RecallMessageResponseHeader {
    /// Message ID of the recalled message (required).
    #[required]
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
