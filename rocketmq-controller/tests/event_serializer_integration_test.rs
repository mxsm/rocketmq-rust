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

//! Integration tests for EventSerializer
//!
//! These tests verify the complete serialization/deserialization workflow
//! for all event types.

use std::collections::HashSet;

use rocketmq_controller::event::alter_sync_state_set_event::AlterSyncStateSetEvent;
use rocketmq_controller::event::apply_broker_id_event::ApplyBrokerIdEvent;
use rocketmq_controller::event::clean_broker_data_event::CleanBrokerDataEvent;
use rocketmq_controller::event::elect_master_event::ElectMasterEvent;
use rocketmq_controller::event::event_serializer::Event;
use rocketmq_controller::event::event_serializer::EventSerializer;
use rocketmq_controller::event::event_type::EventType;
use rocketmq_controller::event::update_broker_address_event::UpdateBrokerAddressEvent;

#[test]
fn test_serialize_deserialize_alter_sync_state_set_event() {
    let serializer = EventSerializer::new();

    let mut sync_state_set = HashSet::new();
    sync_state_set.insert(1u64);
    sync_state_set.insert(2u64);
    sync_state_set.insert(3u64);

    let event = AlterSyncStateSetEvent::new("test-broker", sync_state_set.clone());
    let event_enum = Event::AlterSyncStateSet(event.clone());

    // Serialize
    let bytes = serializer.serialize(&event_enum).unwrap().unwrap();

    // Verify event type in header
    let event_type_id = i16::from_be_bytes([bytes[0], bytes[1]]);
    assert_eq!(event_type_id, EventType::AlterSyncStateSet.id());

    // Deserialize
    let deserialized = serializer.deserialize(&bytes).unwrap().unwrap();

    // Verify
    if let Event::AlterSyncStateSet(deserialized_event) = deserialized {
        assert_eq!(deserialized_event.broker_name(), event.broker_name());
        assert_eq!(deserialized_event.new_sync_state_set(), event.new_sync_state_set());
    } else {
        panic!("Expected AlterSyncStateSetEvent");
    }
}

#[test]
fn test_serialize_deserialize_apply_broker_id_event() {
    let serializer = EventSerializer::new();

    let event = ApplyBrokerIdEvent::new(
        "test-cluster",
        "test-broker",
        "192.168.1.100:10911",
        123,
        "check-code-456",
    );

    let bytes = serializer.serialize_event(event.clone()).unwrap().unwrap();

    // Verify event type
    let event_type_id = i16::from_be_bytes([bytes[0], bytes[1]]);
    assert_eq!(event_type_id, EventType::ApplyBrokerId.id());

    // Deserialize
    let deserialized = serializer.deserialize(&bytes).unwrap().unwrap();

    // Verify
    if let Event::ApplyBrokerId(deserialized_event) = deserialized {
        assert_eq!(deserialized_event.cluster_name(), event.cluster_name());
        assert_eq!(deserialized_event.broker_name(), event.broker_name());
        assert_eq!(deserialized_event.broker_address(), event.broker_address());
        assert_eq!(deserialized_event.new_broker_id(), event.new_broker_id());
        assert_eq!(deserialized_event.register_check_code(), event.register_check_code());
    } else {
        panic!("Expected ApplyBrokerIdEvent");
    }
}

#[test]
fn test_serialize_deserialize_elect_master_event_with_new_master() {
    let serializer = EventSerializer::new();

    let event = ElectMasterEvent::with_new_master("test-broker", 1);
    let bytes = serializer.serialize_event(event.clone()).unwrap().unwrap();

    // Verify event type
    let event_type_id = i16::from_be_bytes([bytes[0], bytes[1]]);
    assert_eq!(event_type_id, EventType::ElectMaster.id());

    // Deserialize
    let deserialized = serializer.deserialize(&bytes).unwrap().unwrap();

    // Verify
    if let Event::ElectMaster(deserialized_event) = deserialized {
        assert_eq!(deserialized_event.broker_name(), event.broker_name());
        assert_eq!(deserialized_event.new_master_broker_id(), event.new_master_broker_id());
        assert!(deserialized_event.new_master_elected());
    } else {
        panic!("Expected ElectMasterEvent");
    }
}

#[test]
fn test_serialize_deserialize_elect_master_event_without_new_master() {
    let serializer = EventSerializer::new();

    let event = ElectMasterEvent::without_new_master(false, "test-broker");
    let bytes = serializer.serialize_event(event.clone()).unwrap().unwrap();

    // Deserialize
    let deserialized = serializer.deserialize(&bytes).unwrap().unwrap();

    // Verify
    if let Event::ElectMaster(deserialized_event) = deserialized {
        assert_eq!(deserialized_event.broker_name(), event.broker_name());
        assert_eq!(deserialized_event.new_master_broker_id(), event.new_master_broker_id());
        assert!(!deserialized_event.new_master_elected());
    } else {
        panic!("Expected ElectMasterEvent");
    }
}

#[test]
fn test_serialize_deserialize_clean_broker_data_event() {
    let serializer = EventSerializer::new();

    let mut broker_ids = HashSet::new();
    broker_ids.insert(1u64);
    broker_ids.insert(2u64);
    broker_ids.insert(3u64);
    broker_ids.insert(4u64);
    broker_ids.insert(5u64);
    let event = CleanBrokerDataEvent::new("test-broker", Some(broker_ids));
    let bytes = serializer.serialize_event(event.clone()).unwrap().unwrap();

    // Verify event type
    let event_type_id = i16::from_be_bytes([bytes[0], bytes[1]]);
    assert_eq!(event_type_id, EventType::CleanBrokerData.id());

    // Deserialize
    let deserialized = serializer.deserialize(&bytes).unwrap().unwrap();

    // Verify
    if let Event::CleanBrokerData(deserialized_event) = deserialized {
        assert_eq!(deserialized_event.broker_name(), event.broker_name());
        assert_eq!(
            deserialized_event.broker_id_set_to_clean(),
            event.broker_id_set_to_clean()
        );
    } else {
        panic!("Expected CleanBrokerDataEvent");
    }
}

#[test]
fn test_serialize_deserialize_update_broker_address_event_with_id() {
    let serializer = EventSerializer::new();

    let event = UpdateBrokerAddressEvent::new("test-cluster", "test-broker", "192.168.1.200:10911", 99);
    let bytes = serializer.serialize_event(event.clone()).unwrap().unwrap();

    // Verify event type
    let event_type_id = i16::from_be_bytes([bytes[0], bytes[1]]);
    assert_eq!(event_type_id, EventType::UpdateBrokerAddress.id());

    // Deserialize
    let deserialized = serializer.deserialize(&bytes).unwrap().unwrap();

    // Verify
    if let Event::UpdateBrokerAddress(deserialized_event) = deserialized {
        assert_eq!(deserialized_event.cluster_name(), event.cluster_name());
        assert_eq!(deserialized_event.broker_name(), event.broker_name());
        assert_eq!(deserialized_event.broker_address(), event.broker_address());
        assert_eq!(deserialized_event.broker_id(), event.broker_id());
    } else {
        panic!("Expected UpdateBrokerAddressEvent");
    }
}

#[test]
fn test_serialize_deserialize_update_broker_address_event_without_id() {
    let serializer = EventSerializer::new();

    let event = UpdateBrokerAddressEvent::new("test-cluster", "test-broker", "192.168.1.200:10911", 0);
    let bytes = serializer.serialize_event(event.clone()).unwrap().unwrap();

    // Deserialize
    let deserialized = serializer.deserialize(&bytes).unwrap().unwrap();

    // Verify
    if let Event::UpdateBrokerAddress(deserialized_event) = deserialized {
        assert_eq!(deserialized_event.cluster_name(), event.cluster_name());
        assert_eq!(deserialized_event.broker_name(), event.broker_name());
        assert_eq!(deserialized_event.broker_address(), event.broker_address());
        assert_eq!(deserialized_event.broker_id(), 0);
    } else {
        panic!("Expected UpdateBrokerAddressEvent");
    }
}

#[test]
fn test_deserialize_empty_data() {
    let serializer = EventSerializer::new();

    // Empty bytes - Java returns null, so we return Ok(None)
    let result = serializer.deserialize(&[]);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    // Only one byte - Java returns null, so we return Ok(None)
    let result = serializer.deserialize(&[1]);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_deserialize_invalid_event_type() {
    let serializer = EventSerializer::new();

    // Event type 0 (invalid) - Java returns null, so we return Ok(None)
    let result = serializer.deserialize(&[0, 0, 123]);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    // Negative event type - Java returns null, so we return Ok(None)
    let result = serializer.deserialize(&[255, 255, 123]);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    // Unknown event type - Java returns null, so we return Ok(None)
    let result = serializer.deserialize(&[0, 99, 123]);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_deserialize_empty_event_data() {
    let serializer = EventSerializer::new();

    // Valid event type but no data - Java returns null, so we return Ok(None)
    let mut bytes = vec![];
    bytes.extend_from_slice(&1i16.to_be_bytes()); // AlterSyncStateSetEvent
                                                  // No JSON data
    let result = serializer.deserialize(&bytes);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_deserialize_invalid_json() {
    let serializer = EventSerializer::new();

    let mut bytes = vec![];
    bytes.extend_from_slice(&1i16.to_be_bytes()); // AlterSyncStateSetEvent
    bytes.extend_from_slice(b"not valid json");

    let result = serializer.deserialize(&bytes);
    assert!(result.is_err());
}

#[test]
fn test_multiple_events_round_trip() {
    let serializer = EventSerializer::new();

    // Create multiple events
    let mut broker_ids = HashSet::new();
    broker_ids.insert(1u64);
    broker_ids.insert(2u64);
    broker_ids.insert(3u64);
    let events = [
        Event::ElectMaster(ElectMasterEvent::with_new_master("broker1", 1)),
        Event::ApplyBrokerId(ApplyBrokerIdEvent::new(
            "cluster1",
            "broker2",
            "192.168.1.1:10911",
            2,
            "code1",
        )),
        Event::CleanBrokerData(CleanBrokerDataEvent::new("broker3", Some(broker_ids))),
    ];

    // Serialize all events
    let serialized: Vec<_> = events
        .iter()
        .map(|e| serializer.serialize(e).unwrap().unwrap())
        .collect();

    // Deserialize all events
    let deserialized: Vec<_> = serialized
        .iter()
        .map(|bytes| serializer.deserialize(bytes).unwrap().unwrap())
        .collect();

    // Verify count
    assert_eq!(deserialized.len(), events.len());

    // Verify types
    assert!(matches!(deserialized[0], Event::ElectMaster(_)));
    assert!(matches!(deserialized[1], Event::ApplyBrokerId(_)));
    assert!(matches!(deserialized[2], Event::CleanBrokerData(_)));
}

#[test]
fn test_event_type_extraction() {
    let elect_event = Event::ElectMaster(ElectMasterEvent::with_new_master("broker", 1));
    assert_eq!(elect_event.event_type(), EventType::ElectMaster);

    let apply_event = Event::ApplyBrokerId(ApplyBrokerIdEvent::new("cluster", "broker", "addr", 1, "code"));
    assert_eq!(apply_event.event_type(), EventType::ApplyBrokerId);
}

#[test]
fn test_typed_deserialization() {
    let serializer = EventSerializer::new();

    let event = ElectMasterEvent::with_new_master("test-broker", 1);
    let bytes = serializer.serialize_event(event.clone()).unwrap().unwrap();

    // Deserialize with type inference
    let deserialized: ElectMasterEvent = serializer.deserialize_typed(&bytes).unwrap().unwrap();

    assert_eq!(deserialized.broker_name(), event.broker_name());
    assert_eq!(deserialized.new_master_broker_id(), event.new_master_broker_id());
}

#[test]
fn test_typed_deserialization_wrong_type() {
    let serializer = EventSerializer::new();

    // Serialize an ElectMasterEvent
    let event = ElectMasterEvent::with_new_master("test-broker", 1);
    let bytes = serializer.serialize_event(event).unwrap().unwrap();

    // Try to deserialize as wrong type - Java would return null, Rust returns Ok(None)
    let result: Result<Option<ApplyBrokerIdEvent>, _> = serializer.deserialize_typed(&bytes);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}
