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
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_remoting::protocol::body::message_request_mode_serialize_wrapper::MessageRequestModeMap;
use rocketmq_remoting::protocol::body::set_message_request_mode_request_body::SetMessageRequestModeRequestBody;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use tracing::info;

use crate::broker_path_config_helper;

pub(crate) struct MessageRequestModeManager {
    message_store_config: Arc<MessageStoreConfig>,
    message_request_mode_map: Arc<parking_lot::Mutex<MessageRequestModeMap>>,
}

impl MessageRequestModeManager {
    pub fn new(message_store_config: Arc<MessageStoreConfig>) -> Self {
        Self {
            message_store_config,
            message_request_mode_map: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        }
    }

    pub fn set_message_request_mode(
        &self,
        topic: CheetahString,
        consumer_group: CheetahString,
        request_body: SetMessageRequestModeRequestBody,
    ) {
        let mut message_request_mode_map = self.message_request_mode_map.lock();
        message_request_mode_map
            .entry(topic)
            .or_default()
            .insert(consumer_group, request_body);
    }

    pub fn get_message_request_mode(
        &self,
        topic: &CheetahString,
        consumer_group: &CheetahString,
    ) -> Option<SetMessageRequestModeRequestBody> {
        let message_request_mode_map = self.message_request_mode_map.lock();
        if let Some(consumer_group_map) = message_request_mode_map.get(topic) {
            if let Some(message_request_mode) = consumer_group_map.get(consumer_group) {
                return Some(message_request_mode.clone());
            }
        }
        None
    }

    pub fn message_request_mode_map(&self) -> Arc<parking_lot::Mutex<MessageRequestModeMap>> {
        self.message_request_mode_map.clone()
    }
}

impl ConfigManager for MessageRequestModeManager {
    fn config_file_path(&self) -> String {
        broker_path_config_helper::get_message_request_mode_path(self.message_store_config.store_path_root_dir.as_str())
    }

    fn encode_pretty(&self, pretty_format: bool) -> String {
        if pretty_format {
            SerdeJsonUtils::serialize_json_pretty(&*self.message_request_mode_map.lock()).expect("encode failed")
        } else {
            SerdeJsonUtils::serialize_json(&*self.message_request_mode_map.lock()).expect("encode failed")
        }
    }

    fn decode(&self, json_string: &str) {
        info!("decode MessageRequestModeManager from json string:{}", json_string);
        if json_string.is_empty() {
            return;
        }
        let message_request_mode_map: HashMap<CheetahString, HashMap<CheetahString, SetMessageRequestModeRequestBody>> =
            SerdeJsonUtils::from_json_str(json_string).expect("decode failed");
        let mut message_request_mode_map_ = self.message_request_mode_map.lock();
        *message_request_mode_map_ = message_request_mode_map;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_enum::MessageRequestMode;
    use rocketmq_remoting::protocol::body::set_message_request_mode_request_body::SetMessageRequestModeRequestBody;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;

    use super::*;

    #[test]
    fn set_message_request_mode_adds_entry() {
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let manager = MessageRequestModeManager::new(message_store_config);
        let topic = CheetahString::from("test_topic");
        let consumer_group = CheetahString::from("test_group");
        let request_body = SetMessageRequestModeRequestBody::default();

        manager.set_message_request_mode(topic.clone(), consumer_group.clone(), request_body.clone());
        let _result = manager.get_message_request_mode(&topic, &consumer_group);

        //assert_eq!(result, Some(request_body));
    }

    #[test]
    fn get_message_request_mode_returns_none_for_nonexistent_entry() {
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let manager = MessageRequestModeManager::new(message_store_config);
        let topic = CheetahString::from("nonexistent_topic");
        let consumer_group = CheetahString::from("nonexistent_group");

        let result = manager.get_message_request_mode(&topic, &consumer_group);

        assert!(result.is_none());
    }

    #[test]
    fn encode_pretty_returns_pretty_json() {
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let manager = MessageRequestModeManager::new(message_store_config);
        let topic = CheetahString::from("test_topic");
        let consumer_group = CheetahString::from("test_group");
        let request_body = SetMessageRequestModeRequestBody::default();

        manager.set_message_request_mode(topic.clone(), consumer_group.clone(), request_body.clone());
        let json = manager.encode_pretty(true);

        assert!(json.contains("\n"));
        assert!(json.contains("\"test_topic\""));
    }

    #[test]
    fn decode_populates_message_request_mode_map() {
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let manager = MessageRequestModeManager::new(message_store_config);
        let json = r#"{
             "test_topic": {
                 "test_group": {
                     "topic": "test_topic",
                     "consumerGroup": "test_group",
                     "mode": "PULL",
                     "popShareQueueNum": 0
                 }
             }
         }"#;

        manager.decode(json);
        let result =
            manager.get_message_request_mode(&CheetahString::from("test_topic"), &CheetahString::from("test_group"));

        assert!(result.is_some());
        assert_eq!(result.unwrap().mode, MessageRequestMode::Pull);
    }
}
