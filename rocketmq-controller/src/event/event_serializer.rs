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

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use rocketmq_error::RocketMQResult;
use rocketmq_error::SerializationError;
use serde::Deserialize;
use serde::Serialize;

use crate::event::alter_sync_state_set_event::AlterSyncStateSetEvent;
use crate::event::apply_broker_id_event::ApplyBrokerIdEvent;
use crate::event::clean_broker_data_event::CleanBrokerDataEvent;
use crate::event::elect_master_event::ElectMasterEvent;
use crate::event::event_type::EventType;
use crate::event::update_broker_address_event::UpdateBrokerAddressEvent;

/// Unified event enum that can hold any event type
///
/// This enum represents all possible event types that can be serialized
/// and deserialized by the EventSerializer.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Event {
    AlterSyncStateSet(AlterSyncStateSetEvent),
    ApplyBrokerId(ApplyBrokerIdEvent),
    ElectMaster(ElectMasterEvent),
    CleanBrokerData(CleanBrokerDataEvent),
    UpdateBrokerAddress(UpdateBrokerAddressEvent),
}

impl Event {
    /// Get the event type of this event
    pub fn event_type(&self) -> EventType {
        match self {
            Event::AlterSyncStateSet(_) => EventType::AlterSyncStateSet,
            Event::ApplyBrokerId(_) => EventType::ApplyBrokerId,
            Event::ElectMaster(_) => EventType::ElectMaster,
            Event::CleanBrokerData(_) => EventType::CleanBrokerData,
            Event::UpdateBrokerAddress(_) => EventType::UpdateBrokerAddress,
        }
    }
}

/// Event serializer that handles serialization and deserialization of events
///
/// The serialization format is:
/// - 2 bytes: event type ID (big-endian i16)
/// - N bytes: JSON serialized event data
///
/// This format matches the Java implementation for compatibility.
pub struct EventSerializer;

impl EventSerializer {
    /// Create a new EventSerializer instance
    pub fn new() -> Self {
        Self
    }

    /// Serialize an event to bytes
    ///
    /// The format is: [event_type_id: 2 bytes][json_data: N bytes]
    ///
    /// # Arguments
    /// * `event` - The event to serialize
    ///
    /// # Returns
    /// * `Ok(Some(Bytes))` - The serialized bytes
    /// * `Ok(None)` - If serialization produces empty data (matches Java behavior)
    /// * `Err(RocketMQError)` - If serialization fails
    ///
    /// # Example
    /// ```ignore
    /// let serializer = EventSerializer::new();
    /// let event = Event::ElectMaster(elect_master_event);
    /// let bytes = serializer.serialize(&event)?;
    /// ```
    pub fn serialize(&self, event: &Event) -> RocketMQResult<Option<Bytes>> {
        // Serialize the event data to JSON
        let json_data = serde_json::to_vec(event)
            .map_err(|e| SerializationError::event_serialization_failed(format!("JSON serialization failed: {}", e)))?;

        // If data is empty, return None (matches Java behavior: return null)
        if json_data.is_empty() {
            return Ok(None);
        }

        // Get the event type ID
        let event_type_id = event.event_type().id();

        // Create result buffer: 2 bytes for event type + JSON data
        let mut result = BytesMut::with_capacity(2 + json_data.len());

        // Write event type (big-endian i16)
        result.put_i16(event_type_id);

        // Write JSON data
        result.put_slice(&json_data);

        Ok(Some(result.freeze()))
    }

    /// Serialize a specific event type to bytes
    ///
    /// This is a helper method that automatically wraps specific event types
    /// into the unified Event enum.
    ///
    /// # Type Parameters
    /// * `T` - The specific event type (must be convertible to Event)
    ///
    /// # Arguments
    /// * `event` - The specific event to serialize
    ///
    /// # Returns
    /// * `Ok(Some(Bytes))` - The serialized bytes
    /// * `Ok(None)` - If serialization produces empty data
    /// * `Err(RocketMQError)` - If serialization fails
    pub fn serialize_event<T>(&self, event: T) -> RocketMQResult<Option<Bytes>>
    where
        T: Into<Event>,
    {
        self.serialize(&event.into())
    }

    /// Deserialize bytes to an event
    ///
    /// The format is: [event_type_id: 2 bytes][json_data: N bytes]
    ///
    /// # Arguments
    /// * `bytes` - The bytes to deserialize
    ///
    /// # Returns
    /// * `Ok(Some(Event))` - The deserialized event
    /// * `Ok(None)` - If data is invalid but not an error (matches Java null behavior)
    /// * `Err(RocketMQError)` - If deserialization fails with a critical error
    ///
    /// # Example
    /// ```ignore
    /// let serializer = EventSerializer::new();
    /// let event = serializer.deserialize(&bytes)?;
    /// ```
    pub fn deserialize(&self, bytes: &[u8]) -> RocketMQResult<Option<Event>> {
        // Check minimum length (at least 2 bytes for event type)
        // Java: if (bytes.length < 2) return null;
        if bytes.len() < 2 {
            return Ok(None);
        }

        // Read event type ID (big-endian i16)
        let mut buf = bytes;
        let event_type_id = buf.get_i16();

        // Validate event type
        // Java: if (eventId > 0)
        if event_type_id <= 0 {
            return Ok(None);
        }

        // Get event type from ID
        // Java: final EventType eventType = EventType.from(eventId);
        // Java: if (eventType != null)
        let event_type = match EventType::from_id(event_type_id) {
            Some(et) => et,
            None => return Ok(None), // Java returns null for unknown event types
        };

        // Extract JSON data (remaining bytes after the 2-byte header)
        let json_data = &bytes[2..];

        // If no data, return None (though Java doesn't explicitly check this)
        if json_data.is_empty() {
            return Ok(None);
        }

        // Deserialize based on event type
        // Java: switch (eventType) with individual deserialize calls
        let event = match event_type {
            EventType::AlterSyncStateSet => {
                let alter_event: AlterSyncStateSetEvent = serde_json::from_slice(json_data).map_err(|e| {
                    SerializationError::event_deserialization_failed(format!(
                        "Failed to deserialize AlterSyncStateSetEvent: {}",
                        e
                    ))
                })?;
                Event::AlterSyncStateSet(alter_event)
            }
            EventType::ApplyBrokerId => {
                let apply_event: ApplyBrokerIdEvent = serde_json::from_slice(json_data).map_err(|e| {
                    SerializationError::event_deserialization_failed(format!(
                        "Failed to deserialize ApplyBrokerIdEvent: {}",
                        e
                    ))
                })?;
                Event::ApplyBrokerId(apply_event)
            }
            EventType::ElectMaster => {
                let elect_event: ElectMasterEvent = serde_json::from_slice(json_data).map_err(|e| {
                    SerializationError::event_deserialization_failed(format!(
                        "Failed to deserialize ElectMasterEvent: {}",
                        e
                    ))
                })?;
                Event::ElectMaster(elect_event)
            }
            EventType::CleanBrokerData => {
                let clean_event: CleanBrokerDataEvent = serde_json::from_slice(json_data).map_err(|e| {
                    SerializationError::event_deserialization_failed(format!(
                        "Failed to deserialize CleanBrokerDataEvent: {}",
                        e
                    ))
                })?;
                Event::CleanBrokerData(clean_event)
            }
            EventType::UpdateBrokerAddress => {
                let update_event: UpdateBrokerAddressEvent = serde_json::from_slice(json_data).map_err(|e| {
                    SerializationError::event_deserialization_failed(format!(
                        "Failed to deserialize UpdateBrokerAddressEvent: {}",
                        e
                    ))
                })?;
                Event::UpdateBrokerAddress(update_event)
            }
            EventType::ReadEvent => {
                // Java: default: break; (returns null)
                return Ok(None);
            }
        };

        Ok(Some(event))
    }

    /// Deserialize bytes to a specific event type
    ///
    /// This is a type-safe helper that attempts to deserialize and extract
    /// a specific event type.
    ///
    /// # Type Parameters
    /// * `T` - The expected event type
    ///
    /// # Arguments
    /// * `bytes` - The bytes to deserialize
    ///
    /// # Returns
    /// * `Ok(Some(T))` - The deserialized event of the expected type
    /// * `Ok(None)` - If deserialization returns None or type mismatch
    /// * `Err(RocketMQError)` - If deserialization fails
    pub fn deserialize_typed<T>(&self, bytes: &[u8]) -> RocketMQResult<Option<T>>
    where
        T: TryFrom<Event>,
    {
        match self.deserialize(bytes)? {
            Some(event) => match T::try_from(event) {
                Ok(typed_event) => Ok(Some(typed_event)),
                Err(_) => Ok(None), // Type mismatch, return None like Java
            },
            None => Ok(None),
        }
    }
}

impl Default for EventSerializer {
    fn default() -> Self {
        Self::new()
    }
}

// Conversion implementations for convenience
impl From<AlterSyncStateSetEvent> for Event {
    fn from(event: AlterSyncStateSetEvent) -> Self {
        Event::AlterSyncStateSet(event)
    }
}

impl From<ApplyBrokerIdEvent> for Event {
    fn from(event: ApplyBrokerIdEvent) -> Self {
        Event::ApplyBrokerId(event)
    }
}

impl From<ElectMasterEvent> for Event {
    fn from(event: ElectMasterEvent) -> Self {
        Event::ElectMaster(event)
    }
}

impl From<CleanBrokerDataEvent> for Event {
    fn from(event: CleanBrokerDataEvent) -> Self {
        Event::CleanBrokerData(event)
    }
}

impl From<UpdateBrokerAddressEvent> for Event {
    fn from(event: UpdateBrokerAddressEvent) -> Self {
        Event::UpdateBrokerAddress(event)
    }
}

// TryFrom implementations for extracting specific types
impl TryFrom<Event> for AlterSyncStateSetEvent {
    type Error = SerializationError;

    fn try_from(event: Event) -> std::result::Result<Self, Self::Error> {
        match event {
            Event::AlterSyncStateSet(e) => Ok(e),
            _ => Err(SerializationError::invalid_format(
                "AlterSyncStateSetEvent",
                "other event type",
            )),
        }
    }
}

impl TryFrom<Event> for ApplyBrokerIdEvent {
    type Error = SerializationError;

    fn try_from(event: Event) -> std::result::Result<Self, Self::Error> {
        match event {
            Event::ApplyBrokerId(e) => Ok(e),
            _ => Err(SerializationError::invalid_format(
                "ApplyBrokerIdEvent",
                "other event type",
            )),
        }
    }
}

impl TryFrom<Event> for ElectMasterEvent {
    type Error = SerializationError;

    fn try_from(event: Event) -> std::result::Result<Self, Self::Error> {
        match event {
            Event::ElectMaster(e) => Ok(e),
            _ => Err(SerializationError::invalid_format(
                "ElectMasterEvent",
                "other event type",
            )),
        }
    }
}

impl TryFrom<Event> for CleanBrokerDataEvent {
    type Error = SerializationError;

    fn try_from(event: Event) -> std::result::Result<Self, Self::Error> {
        match event {
            Event::CleanBrokerData(e) => Ok(e),
            _ => Err(SerializationError::invalid_format(
                "CleanBrokerDataEvent",
                "other event type",
            )),
        }
    }
}

impl TryFrom<Event> for UpdateBrokerAddressEvent {
    type Error = SerializationError;

    fn try_from(event: Event) -> std::result::Result<Self, Self::Error> {
        match event {
            Event::UpdateBrokerAddress(e) => Ok(e),
            _ => Err(SerializationError::invalid_format(
                "UpdateBrokerAddressEvent",
                "other event type",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_elect_master_event() {
        let serializer = EventSerializer::new();

        let event = ElectMasterEvent::with_new_master("test-broker", 1);
        let event_enum = Event::ElectMaster(event.clone());

        // Serialize
        let bytes = serializer.serialize(&event_enum).unwrap().unwrap();

        // Should have at least 2 bytes for event type
        assert!(bytes.len() >= 2);

        // First 2 bytes should be event type ID
        let event_type_id = i16::from_be_bytes([bytes[0], bytes[1]]);
        assert_eq!(event_type_id, EventType::ElectMaster.id());

        // Deserialize
        let deserialized = serializer.deserialize(&bytes).unwrap().unwrap();

        // Check event type
        assert_eq!(deserialized.event_type(), EventType::ElectMaster);

        // Extract specific event
        if let Event::ElectMaster(deserialized_event) = deserialized {
            assert_eq!(deserialized_event.broker_name(), event.broker_name());
            assert_eq!(deserialized_event.new_master_broker_id(), event.new_master_broker_id());
        } else {
            panic!("Expected ElectMasterEvent");
        }
    }

    #[test]
    fn test_deserialize_invalid_data() {
        let serializer = EventSerializer::new();

        // Too short - Java returns null
        let result = serializer.deserialize(&[0]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Invalid event type (0) - Java returns null
        let result = serializer.deserialize(&[0, 0, 123]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Invalid event type (negative) - Java returns null
        let result = serializer.deserialize(&[255, 255, 123]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_serialize_event_helper() {
        let serializer = EventSerializer::new();
        let event = ElectMasterEvent::with_new_master("test-broker", 1);

        // Use helper method
        let bytes = serializer.serialize_event(event).unwrap().unwrap();
        assert!(bytes.len() >= 2);

        // Verify it can be deserialized
        let deserialized = serializer.deserialize(&bytes).unwrap().unwrap();
        assert_eq!(deserialized.event_type(), EventType::ElectMaster);
    }
}
