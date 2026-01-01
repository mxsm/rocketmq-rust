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
pub struct ApplyBrokerIdEvent {
    cluster_name: CheetahString,
    broker_name: CheetahString,
    broker_address: CheetahString,
    register_check_code: CheetahString,
    new_broker_id: u64,
}

impl ApplyBrokerIdEvent {
    pub fn new(
        cluster_name: impl Into<CheetahString>,
        broker_name: impl Into<CheetahString>,
        broker_address: impl Into<CheetahString>,
        new_broker_id: u64,
        register_check_code: impl Into<CheetahString>,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_name: broker_name.into(),
            broker_address: broker_address.into(),
            register_check_code: register_check_code.into(),
            new_broker_id,
        }
    }

    #[inline]
    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    #[inline]
    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    #[inline]
    pub fn broker_address(&self) -> &str {
        &self.broker_address
    }

    #[inline]
    pub fn register_check_code(&self) -> &str {
        &self.register_check_code
    }

    #[inline]
    pub fn new_broker_id(&self) -> u64 {
        self.new_broker_id
    }
}

impl EventMessage for ApplyBrokerIdEvent {
    fn get_event_type(&self) -> EventType {
        EventType::ApplyBrokerId
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_broker_id_event_new_and_getters() {
        let cluster_name = "test_cluster";
        let broker_name = "test_broker";
        let broker_address = "127.0.0.1:10911";
        let new_broker_id = 100;
        let register_check_code = "check_code";

        let event = ApplyBrokerIdEvent::new(
            cluster_name,
            broker_name,
            broker_address,
            new_broker_id,
            register_check_code,
        );

        assert_eq!(event.cluster_name(), cluster_name);
        assert_eq!(event.broker_name(), broker_name);
        assert_eq!(event.broker_address(), broker_address);
        assert_eq!(event.new_broker_id(), new_broker_id);
        assert_eq!(event.register_check_code(), register_check_code);
    }

    #[test]
    fn apply_broker_id_event_get_event_type() {
        let event = ApplyBrokerIdEvent::new("cluster", "broker", "addr", 1, "code");
        assert_eq!(event.get_event_type(), EventType::ApplyBrokerId);
    }

    #[test]
    fn apply_broker_id_event_as_any() {
        let event = ApplyBrokerIdEvent::new("cluster", "broker", "addr", 1, "code");
        let any = event.as_any();
        assert!(any.is::<ApplyBrokerIdEvent>());
        let downcast = any.downcast_ref::<ApplyBrokerIdEvent>().unwrap();
        assert_eq!(downcast.cluster_name(), "cluster");
    }

    #[test]
    fn apply_broker_id_event_serialization_and_deserialization() {
        let event = ApplyBrokerIdEvent::new("cluster", "broker", "addr", 1, "code");
        let json = serde_json::to_string(&event).unwrap();
        let deserialized: ApplyBrokerIdEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.cluster_name(), event.cluster_name());
        assert_eq!(deserialized.broker_name(), event.broker_name());
        assert_eq!(deserialized.broker_address(), event.broker_address());
        assert_eq!(deserialized.new_broker_id(), event.new_broker_id());
        assert_eq!(deserialized.register_check_code(), event.register_check_code());
    }
}
