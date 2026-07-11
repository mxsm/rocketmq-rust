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

use crate::protocol::body::set_message_request_mode_request_body::SetMessageRequestModeRequestBody;

pub type MessageRequestModeMap =
    HashMap<CheetahString /* Topic */, HashMap<CheetahString /* Group */, SetMessageRequestModeRequestBody>>;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct MessageRequestModeSerializeWrapper {
    message_request_mode_map: MessageRequestModeMap,
}

impl MessageRequestModeSerializeWrapper {
    pub fn new(message_request_mode_map: MessageRequestModeMap) -> Self {
        Self {
            message_request_mode_map,
        }
    }

    pub fn message_request_mode_map(&self) -> &MessageRequestModeMap {
        &self.message_request_mode_map
    }

    pub fn into_inner(self) -> MessageRequestModeMap {
        self.message_request_mode_map
    }

    pub fn from_inner(message_request_mode_map: MessageRequestModeMap) -> Self {
        Self {
            message_request_mode_map,
        }
    }

    pub fn set_message_request_mode_map(&mut self, map: MessageRequestModeMap) {
        self.message_request_mode_map = map;
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::body::set_message_request_mode_request_body::SetMessageRequestModeRequestBody;

    #[test]
    fn default_creates_wrapper_with_empty_map() {
        let wrapper = MessageRequestModeSerializeWrapper::default();
        assert!(wrapper.message_request_mode_map().is_empty());
    }

    #[test]
    fn new_creates_wrapper_with_provided_map() {
        let mut map = MessageRequestModeMap::new();
        let mut group_map = HashMap::new();
        group_map.insert(
            CheetahString::from("group1"),
            SetMessageRequestModeRequestBody::default(),
        );
        map.insert(CheetahString::from("topic1"), group_map);

        let wrapper = MessageRequestModeSerializeWrapper::new(map);
        assert_eq!(wrapper.message_request_mode_map().len(), 1);
        assert!(wrapper
            .message_request_mode_map()
            .contains_key(&CheetahString::from("topic1")));
    }

    #[test]
    fn new_creates_wrapper_with_empty_map() {
        let map = MessageRequestModeMap::new();
        let wrapper = MessageRequestModeSerializeWrapper::new(map);
        assert!(wrapper.message_request_mode_map().is_empty());
    }
}
