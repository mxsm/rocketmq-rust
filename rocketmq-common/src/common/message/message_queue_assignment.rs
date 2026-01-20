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
            let mut sorted_attachments: Vec<_> = attachments.iter().collect();
            sorted_attachments.sort_by_key(|(k, _)| k.as_str());
            for (key, value) in sorted_attachments {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_queue_assignment() {
        let msg_queue_assignment = MessageQueueAssignment::default();

        assert_eq!(msg_queue_assignment.message_queue, None);
        assert_eq!(msg_queue_assignment.mode, MessageRequestMode::Pull);
        assert_eq!(msg_queue_assignment.attachments, None);
    }

    #[test]
    fn test_message_queue_assignment_hash() {
        let mut first_msg_queue = MessageQueue::default();
        first_msg_queue.set_broker_name(CheetahString::from("defaultBroker"));
        first_msg_queue.set_queue_id(123);
        first_msg_queue.set_topic(CheetahString::from("testTopic"));
        let mut first_props = HashMap::new();
        first_props.insert(CheetahString::from("rocketmqHome"), CheetahString::from("/new/path"));
        first_props.insert(
            CheetahString::from("kvConfigPath"),
            CheetahString::from("/new/kvConfigPath"),
        );
        let first = MessageQueueAssignment {
            message_queue: Some(first_msg_queue),
            mode: MessageRequestMode::Pull,
            attachments: Some(first_props),
        };

        let mut second_msg_queue = MessageQueue::default();
        second_msg_queue.set_broker_name(CheetahString::from("defaultBroker"));
        second_msg_queue.set_queue_id(123);
        second_msg_queue.set_topic(CheetahString::from("testTopic"));
        let mut second_props = HashMap::new();
        second_props.insert(CheetahString::from("rocketmqHome"), CheetahString::from("/new/path"));
        second_props.insert(
            CheetahString::from("kvConfigPath"),
            CheetahString::from("/new/kvConfigPath"),
        );
        let second = MessageQueueAssignment {
            message_queue: Some(second_msg_queue),
            mode: MessageRequestMode::Pull,
            attachments: Some(second_props),
        };

        let mut first_hasher = std::collections::hash_map::DefaultHasher::new();
        let mut second_hasher = std::collections::hash_map::DefaultHasher::new();

        first.hash(&mut first_hasher);
        let hash_first = first_hasher.finish();
        second.hash(&mut second_hasher);
        let hash_second = second_hasher.finish();

        assert_eq!(hash_first, hash_second);
    }

    #[test]
    fn test_message_queue_assignment_hash_differences() {
        let mut first_props = HashMap::new();
        first_props.insert(CheetahString::from("rocketmqHome"), CheetahString::from("/new/path"));
        let mut second_props = HashMap::new();
        second_props.insert(
            CheetahString::from("rocketmqHome"),
            CheetahString::from("/some/other/path"),
        );

        let first = MessageQueueAssignment {
            message_queue: None,
            mode: MessageRequestMode::Pull,
            attachments: Some(first_props),
        };

        let second = MessageQueueAssignment {
            message_queue: None,
            mode: MessageRequestMode::Pull,
            attachments: Some(second_props),
        };

        let mut first_hasher = std::collections::hash_map::DefaultHasher::new();
        let mut second_hasher = std::collections::hash_map::DefaultHasher::new();

        first.hash(&mut first_hasher);
        let hash_first = first_hasher.finish();
        second.hash(&mut second_hasher);
        let hash_second = second_hasher.finish();

        assert_ne!(hash_first, hash_second);

        let mut first_msg_queue = MessageQueue::default();
        first_msg_queue.set_broker_name(CheetahString::from("defaultBroker"));
        let mut second_msg_queue = MessageQueue::default();
        second_msg_queue.set_broker_name(CheetahString::from("nonDefaultBroker"));

        let first = MessageQueueAssignment {
            message_queue: Some(first_msg_queue),
            mode: MessageRequestMode::Pull,
            attachments: None,
        };
        let second = MessageQueueAssignment {
            message_queue: Some(second_msg_queue),
            mode: MessageRequestMode::Pull,
            attachments: None,
        };

        let mut first_hasher = std::collections::hash_map::DefaultHasher::new();
        let mut second_hasher = std::collections::hash_map::DefaultHasher::new();

        first.hash(&mut first_hasher);
        let hash_first = first_hasher.finish();
        second.hash(&mut second_hasher);
        let hash_second = second_hasher.finish();

        assert_ne!(hash_first, hash_second);

        let first = MessageQueueAssignment {
            message_queue: None,
            mode: MessageRequestMode::Pop,
            attachments: None,
        };
        let second = MessageQueueAssignment {
            message_queue: None,
            mode: MessageRequestMode::Pull,
            attachments: None,
        };

        let mut first_hasher = std::collections::hash_map::DefaultHasher::new();
        let mut second_hasher = std::collections::hash_map::DefaultHasher::new();

        first.hash(&mut first_hasher);
        let hash_first = first_hasher.finish();
        second.hash(&mut second_hasher);
        let hash_second = second_hasher.finish();

        assert_ne!(hash_first, hash_second);
    }
}
