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

use std::collections::HashSet;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::event::event_message::EventMessage;
use crate::event::event_type::EventType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlterSyncStateSetEvent {
    broker_name: CheetahString,
    new_sync_state_set: HashSet<u64>, // BrokerId
}

impl AlterSyncStateSetEvent {
    pub fn new(broker_name: impl Into<CheetahString>, new_sync_state_set: impl IntoIterator<Item = u64>) -> Self {
        Self {
            broker_name: broker_name.into(),
            new_sync_state_set: new_sync_state_set.into_iter().collect(),
        }
    }

    #[inline]
    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    #[inline]
    pub fn new_sync_state_set(&self) -> &HashSet<u64> {
        &self.new_sync_state_set
    }
}

impl EventMessage for AlterSyncStateSetEvent {
    fn get_event_type(&self) -> EventType {
        EventType::AlterSyncStateSet
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alter_sync_state_set_event_new_and_getters() {
        let broker_name = "test_broker";
        let sync_state_set = vec![1, 2, 3];
        let event = AlterSyncStateSetEvent::new(broker_name, sync_state_set.clone());

        assert_eq!(event.broker_name(), broker_name);
        assert_eq!(event.new_sync_state_set().len(), 3);
        for id in sync_state_set {
            assert!(event.new_sync_state_set().contains(&id));
        }
    }

    #[test]
    fn alter_sync_state_set_event_get_event_type() {
        let event = AlterSyncStateSetEvent::new("broker", vec![1]);
        assert_eq!(event.get_event_type(), EventType::AlterSyncStateSet);
    }

    #[test]
    fn alter_sync_state_set_event_as_any() {
        let event = AlterSyncStateSetEvent::new("broker", vec![1]);
        let any = event.as_any();
        assert!(any.is::<AlterSyncStateSetEvent>());
        let downcast = any.downcast_ref::<AlterSyncStateSetEvent>().unwrap();
        assert_eq!(downcast.broker_name(), "broker");
    }

    #[test]
    fn alter_sync_state_set_event_serialization_and_deserialization() {
        let event = AlterSyncStateSetEvent::new("broker", vec![1, 2]);
        let json = serde_json::to_string(&event).unwrap();
        let deserialized: AlterSyncStateSetEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.broker_name(), event.broker_name());
        assert_eq!(deserialized.new_sync_state_set(), event.new_sync_state_set());
    }
}
