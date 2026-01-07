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
use serde::Deserialize;
use serde::Serialize;

use crate::event::event_message::EventMessage;
use crate::event::event_type::EventType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElectMasterEvent {
    /// Whether a new master was elected
    new_master_elected: bool,
    broker_name: CheetahString,
    new_master_broker_id: Option<u64>,
}

impl ElectMasterEvent {
    pub fn without_new_master(new_master_elected: bool, broker_name: impl Into<CheetahString>) -> Self {
        Self {
            new_master_elected,
            broker_name: broker_name.into(),
            new_master_broker_id: None,
        }
    }

    pub fn with_new_master(broker_name: impl Into<CheetahString>, new_master_broker_id: u64) -> Self {
        Self {
            new_master_elected: true,
            broker_name: broker_name.into(),
            new_master_broker_id: Some(new_master_broker_id),
        }
    }

    pub fn new(
        new_master_elected: bool,
        broker_name: impl Into<CheetahString>,
        new_master_broker_id: Option<u64>,
    ) -> Self {
        Self {
            new_master_elected,
            broker_name: broker_name.into(),
            new_master_broker_id,
        }
    }

    #[inline]
    pub fn new_master_elected(&self) -> bool {
        self.new_master_elected
    }

    #[inline]
    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    #[inline]
    pub fn new_master_broker_id(&self) -> Option<u64> {
        self.new_master_broker_id
    }
}

impl EventMessage for ElectMasterEvent {
    fn get_event_type(&self) -> EventType {
        EventType::ElectMaster
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn elect_master_event_new_and_getters() {
        let event = ElectMasterEvent::new(true, "test_broker", Some(100));

        assert!(event.new_master_elected());
        assert_eq!(event.broker_name(), "test_broker");
        assert_eq!(event.new_master_broker_id(), Some(100));
    }

    #[test]
    fn elect_master_event_with_new_master() {
        let event = ElectMasterEvent::with_new_master("test_broker", 100);

        assert!(event.new_master_elected());
        assert_eq!(event.broker_name(), "test_broker");
        assert_eq!(event.new_master_broker_id(), Some(100));
    }

    #[test]
    fn elect_master_event_without_new_master() {
        let event = ElectMasterEvent::without_new_master(false, "test_broker");

        assert!(!event.new_master_elected());
        assert_eq!(event.broker_name(), "test_broker");
        assert_eq!(event.new_master_broker_id(), None);
    }

    #[test]
    fn elect_master_event_get_event_type() {
        let event = ElectMasterEvent::without_new_master(false, "broker");
        assert_eq!(event.get_event_type(), EventType::ElectMaster);
    }

    #[test]
    fn elect_master_event_as_any() {
        let event = ElectMasterEvent::without_new_master(false, "broker");
        let any = event.as_any();
        assert!(any.is::<ElectMasterEvent>());
        let downcast = any.downcast_ref::<ElectMasterEvent>().unwrap();
        assert_eq!(downcast.broker_name(), "broker");
    }

    #[test]
    fn elect_master_event_serialization_and_deserialization() {
        let event = ElectMasterEvent::with_new_master("broker", 100);
        let json = serde_json::to_string(&event).unwrap();
        let deserialized: ElectMasterEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.new_master_elected(), event.new_master_elected());
        assert_eq!(deserialized.broker_name(), event.broker_name());
        assert_eq!(deserialized.new_master_broker_id(), event.new_master_broker_id());
    }
}
