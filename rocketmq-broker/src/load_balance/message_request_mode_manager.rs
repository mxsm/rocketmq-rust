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
use std::collections::HashSet;
use std::sync::Arc;

use crate::config::config_manager::ConfigManager;
use cheetah_string::CheetahString;
use rocketmq_model::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_protocol::protocol::body::message_request_mode_serialize_wrapper::MessageRequestModeMap;
use rocketmq_protocol::protocol::body::set_message_request_mode_request_body::SetMessageRequestModeRequestBody;
use rocketmq_store::MessageStoreConfig;
use tracing::info;

use crate::broker_path_config_helper;

#[derive(Clone)]
pub(crate) struct MessageRequestModeManager {
    message_store_config: Arc<MessageStoreConfig>,
    message_request_mode_map: Arc<parking_lot::Mutex<MessageRequestModeMap>>,
    supervised_dirty: Arc<parking_lot::Mutex<HashSet<(CheetahString, CheetahString)>>>,
}

#[derive(Debug)]
pub(crate) struct MessageRequestModeUpdate {
    pub(crate) value: SetMessageRequestModeRequestBody,
    pub(crate) changed: bool,
}

#[derive(Debug)]
pub(crate) enum MessageRequestModeCasError {
    Conflict(Option<SetMessageRequestModeRequestBody>),
    PersistenceDirty(Option<SetMessageRequestModeRequestBody>),
}

impl MessageRequestModeManager {
    pub fn new(message_store_config: Arc<MessageStoreConfig>) -> Self {
        Self {
            message_store_config,
            message_request_mode_map: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            supervised_dirty: Arc::new(parking_lot::Mutex::new(HashSet::new())),
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

    /// Conditionally replaces one exact Topic/group entry under the manager lock.
    pub fn set_message_request_mode_if_current(
        &self,
        topic: CheetahString,
        consumer_group: CheetahString,
        expected: Option<&SetMessageRequestModeRequestBody>,
        replacement: SetMessageRequestModeRequestBody,
    ) -> Result<MessageRequestModeUpdate, MessageRequestModeCasError> {
        let mut dirty = self.supervised_dirty.lock();
        let mut map = self.message_request_mode_map.lock();
        let current = map.get(&topic).and_then(|groups| groups.get(&consumer_group)).cloned();
        if dirty.contains(&(topic.clone(), consumer_group.clone())) {
            return Err(MessageRequestModeCasError::PersistenceDirty(current));
        }
        let expected_matches = match (expected, current.as_ref()) {
            (None, None) => true,
            (Some(expected), Some(current)) => {
                expected.mode == current.mode && expected.pop_share_queue_num == current.pop_share_queue_num
            }
            _ => false,
        };
        if !expected_matches {
            return Err(MessageRequestModeCasError::Conflict(current));
        }
        if current.as_ref().is_some_and(|current| {
            current.mode == replacement.mode && current.pop_share_queue_num == replacement.pop_share_queue_num
        }) {
            return Ok(MessageRequestModeUpdate {
                value: replacement,
                changed: false,
            });
        }
        map.entry(topic.clone())
            .or_default()
            .insert(consumer_group.clone(), replacement.clone());
        dirty.insert((topic, consumer_group));
        Ok(MessageRequestModeUpdate {
            value: replacement,
            changed: true,
        })
    }

    pub(crate) fn complete_supervised_persistence(
        &self,
        topic: &CheetahString,
        consumer_group: &CheetahString,
        persisted: bool,
    ) {
        if persisted {
            self.supervised_dirty
                .lock()
                .remove(&(topic.clone(), consumer_group.clone()));
        }
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
    use rocketmq_model::common::message::message_enum::MessageRequestMode;
    use rocketmq_protocol::protocol::body::set_message_request_mode_request_body::SetMessageRequestModeRequestBody;
    use rocketmq_store::MessageStoreConfig;

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
    fn conditional_request_mode_set_is_exact_and_conflicts_without_overwrite() {
        let manager = MessageRequestModeManager::new(Arc::new(MessageStoreConfig::default()));
        let topic = CheetahString::from("test_topic");
        let consumer_group = CheetahString::from("test_group");
        let pull = SetMessageRequestModeRequestBody {
            mode: MessageRequestMode::Pull,
            pop_share_queue_num: 0,
            ..SetMessageRequestModeRequestBody::default()
        };
        let pop = SetMessageRequestModeRequestBody {
            mode: MessageRequestMode::Pop,
            pop_share_queue_num: 4,
            ..pull.clone()
        };

        let created = manager
            .set_message_request_mode_if_current(topic.clone(), consumer_group.clone(), None, pull.clone())
            .expect("absent mode should be created");
        assert!(created.changed);
        assert_eq!(created.value.mode, MessageRequestMode::Pull);
        assert_eq!(created.value.pop_share_queue_num, 0);

        let conflict = manager
            .set_message_request_mode_if_current(topic.clone(), consumer_group.clone(), None, pop.clone())
            .expect_err("second absent create should conflict");
        let MessageRequestModeCasError::PersistenceDirty(Some(conflict)) = conflict else {
            panic!("changed supervised state remains dirty until persistence completes");
        };
        assert_eq!(conflict.mode, MessageRequestMode::Pull);
        assert_eq!(conflict.pop_share_queue_num, 0);

        manager.complete_supervised_persistence(&topic, &consumer_group, true);

        let updated = manager
            .set_message_request_mode_if_current(topic.clone(), consumer_group.clone(), Some(&pull), pop.clone())
            .expect("matching exact mode should update");
        assert!(updated.changed);
        assert_eq!(updated.value.mode, MessageRequestMode::Pop);
        assert_eq!(updated.value.pop_share_queue_num, 4);
        for replacement in [pop.clone(), pull.clone()] {
            assert!(matches!(
                manager.set_message_request_mode_if_current(
                    topic.clone(),
                    consumer_group.clone(),
                    Some(&pop),
                    replacement,
                ),
                Err(MessageRequestModeCasError::PersistenceDirty(Some(_)))
            ));
        }
        manager.complete_supervised_persistence(&topic, &consumer_group, true);
        let unchanged = manager
            .set_message_request_mode_if_current(topic.clone(), consumer_group.clone(), Some(&pop), pop.clone())
            .expect("identical replacement should be an accepted no-op");
        assert!(!unchanged.changed);
        let current = manager
            .get_message_request_mode(&topic, &consumer_group)
            .expect("updated mode should remain present");
        assert_eq!(current.mode, MessageRequestMode::Pop);
        assert_eq!(current.pop_share_queue_num, 4);
    }

    #[test]
    fn dirty_request_mode_is_not_visible_after_restart_from_last_durable_snapshot() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let config = Arc::new(MessageStoreConfig {
            store_path_root_dir: temp_dir.path().to_string_lossy().into_owned().into(),
            ..MessageStoreConfig::default()
        });
        let manager = MessageRequestModeManager::new(Arc::clone(&config));
        manager.persist().expect("persist empty baseline");
        let topic = CheetahString::from("test_topic");
        let consumer_group = CheetahString::from("test_group");
        let pull = SetMessageRequestModeRequestBody {
            mode: MessageRequestMode::Pull,
            pop_share_queue_num: 0,
            ..SetMessageRequestModeRequestBody::default()
        };
        let applied = manager
            .set_message_request_mode_if_current(topic.clone(), consumer_group.clone(), None, pull)
            .expect("in-memory replacement");
        assert!(applied.changed);
        manager.complete_supervised_persistence(&topic, &consumer_group, false);

        let restarted = MessageRequestModeManager::new(config);
        assert!(restarted.load());
        assert!(restarted.get_message_request_mode(&topic, &consumer_group).is_none());
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
