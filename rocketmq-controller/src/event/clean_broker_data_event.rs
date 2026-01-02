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
pub struct CleanBrokerDataEvent {
    broker_name: CheetahString,
    broker_id_set_to_clean: Option<HashSet<u64>>,
}

impl CleanBrokerDataEvent {
    pub fn new(broker_name: impl Into<CheetahString>, broker_id_set_to_clean: Option<HashSet<u64>>) -> Self {
        Self {
            broker_name: broker_name.into(),
            broker_id_set_to_clean,
        }
    }

    #[inline]
    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    #[inline]
    pub fn broker_id_set_to_clean(&self) -> Option<&HashSet<u64>> {
        self.broker_id_set_to_clean.as_ref()
    }
}

impl EventMessage for CleanBrokerDataEvent {
    fn get_event_type(&self) -> EventType {
        EventType::CleanBrokerData
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_broker_data_event_new_and_getters() {
        let broker_name = "test_broker";
        let mut ids = HashSet::new();
        ids.insert(1);
        ids.insert(2);

        let event = CleanBrokerDataEvent::new(broker_name, Some(ids.clone()));
        assert_eq!(event.broker_name(), broker_name);
        assert_eq!(event.broker_id_set_to_clean(), Some(&ids));

        let event = CleanBrokerDataEvent::new(broker_name, None);
        assert_eq!(event.broker_name(), broker_name);
        assert_eq!(event.broker_id_set_to_clean(), None);
    }

    #[test]
    fn clean_broker_data_event_get_event_type() {
        let event = CleanBrokerDataEvent::new("broker", None);
        assert_eq!(event.get_event_type(), EventType::CleanBrokerData);
    }

    #[test]
    fn clean_broker_data_event_as_any() {
        let event = CleanBrokerDataEvent::new("broker", None);
        let any = event.as_any();
        assert!(any.is::<CleanBrokerDataEvent>());
        let downcast = any.downcast_ref::<CleanBrokerDataEvent>().unwrap();
        assert_eq!(downcast.broker_name(), "broker");
    }

    #[test]
    fn clean_broker_data_event_serialization_and_deserialization() {
        let mut ids = HashSet::new();
        ids.insert(1);
        let event = CleanBrokerDataEvent::new("broker", Some(ids));
        let json = serde_json::to_string(&event).unwrap();
        let deserialized: CleanBrokerDataEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.broker_name(), event.broker_name());
        assert_eq!(deserialized.broker_id_set_to_clean(), event.broker_id_set_to_clean());
    }
}
