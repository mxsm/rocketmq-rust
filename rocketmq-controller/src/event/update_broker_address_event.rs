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
pub struct UpdateBrokerAddressEvent {
    cluster_name: CheetahString,
    broker_name: CheetahString,
    broker_address: CheetahString,
    broker_id: u64,
}

impl UpdateBrokerAddressEvent {
    pub fn new(
        cluster_name: impl Into<CheetahString>,
        broker_name: impl Into<CheetahString>,
        broker_address: impl Into<CheetahString>,
        broker_id: u64,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_name: broker_name.into(),
            broker_address: broker_address.into(),
            broker_id,
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
    pub fn broker_id(&self) -> u64 {
        self.broker_id
    }
}

impl EventMessage for UpdateBrokerAddressEvent {
    fn get_event_type(&self) -> EventType {
        EventType::UpdateBrokerAddress
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_CLUSTER_NAME: &str = "test_cluster";
    const TEST_BROKER_NAME: &str = "test_broker";
    const TEST_BROKER_ADDRESS: &str = "192.168.10.30:8989";
    const BROKER_ID: Option<u64> = Some(u64::MAX);

    #[test]
    fn new_method() {
        let event = UpdateBrokerAddressEvent::new(TEST_CLUSTER_NAME, TEST_BROKER_NAME, TEST_BROKER_ADDRESS, 0);

        assert_eq!(event.cluster_name(), TEST_CLUSTER_NAME);
        assert_eq!(event.broker_name(), TEST_BROKER_NAME);
        assert_eq!(event.broker_address(), TEST_BROKER_ADDRESS);
        assert_eq!(event.broker_id(), 0);
    }

    #[test]
    fn new_without_broker_id() {
        let event = UpdateBrokerAddressEvent::new(TEST_CLUSTER_NAME, TEST_BROKER_NAME, TEST_BROKER_ADDRESS, 1);

        assert_eq!(event.broker_id(), 1);
    }

    #[test]
    fn accepts_mixed_string_types() {
        let event = UpdateBrokerAddressEvent::new(
            TEST_CLUSTER_NAME,
            String::from(TEST_BROKER_NAME),
            CheetahString::from(TEST_BROKER_ADDRESS),
            0,
        );

        assert_eq!(event.cluster_name(), TEST_CLUSTER_NAME);
        assert_eq!(event.broker_name(), TEST_BROKER_NAME);
        assert_eq!(event.broker_address(), TEST_BROKER_ADDRESS);
    }

    #[test]
    fn correct_event_type() {
        let event = UpdateBrokerAddressEvent::new(TEST_CLUSTER_NAME, TEST_BROKER_NAME, TEST_BROKER_ADDRESS, 0);

        assert_eq!(event.get_event_type(), EventType::UpdateBrokerAddress);
    }
}
