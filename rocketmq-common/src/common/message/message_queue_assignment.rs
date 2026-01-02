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
use std::hash::Hash;
use std::hash::Hasher;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::common::message::message_enum::MessageRequestMode;
use crate::common::message::message_queue::MessageQueue;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MessageQueueAssignment {
    pub message_queue: Option<MessageQueue>,
    pub mode: MessageRequestMode,
    pub attachments: Option<HashMap<CheetahString, CheetahString>>,
}

impl Hash for MessageQueueAssignment {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.message_queue.hash(state);
        self.mode.hash(state);
        if let Some(ref attachments) = self.attachments {
            for (key, value) in attachments {
                key.hash(state);
                value.hash(state);
            }
        }
    }
}

impl Default for MessageQueueAssignment {
    fn default() -> Self {
        MessageQueueAssignment {
            message_queue: None,
            mode: MessageRequestMode::Pull,
            attachments: None,
        }
    }
}
