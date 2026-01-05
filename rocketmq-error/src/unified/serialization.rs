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

//! Serialization and deserialization errors

use thiserror::Error;

/// Serialization/Deserialization errors
#[derive(Debug, Error)]
pub enum SerializationError {
    /// Encoding failed
    #[error("Encoding failed ({format}): {message}")]
    EncodeFailed { format: &'static str, message: String },

    /// Decoding failed
    #[error("Decoding failed ({format}): {message}")]
    DecodeFailed { format: &'static str, message: String },

    /// Invalid data format
    #[error("Invalid format: expected {expected}, got {got}")]
    InvalidFormat { expected: &'static str, got: String },

    /// Missing required field
    #[error("Missing required field: {field}")]
    MissingField { field: &'static str },

    /// Invalid field value
    #[error("Invalid value for field '{field}': {reason}")]
    InvalidValue { field: &'static str, reason: String },

    /// UTF-8 encoding error
    #[error("UTF-8 encoding error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),

    /// JSON serialization error
    #[cfg(feature = "with_serde")]
    #[error("JSON error: {0}")]
    JsonError(String),

    /// Protobuf serialization error
    #[error("Protobuf error: {0}")]
    ProtobufError(String),

    /// Event serialization failed
    #[error("Event serialization failed: {0}")]
    EventSerializationFailed(String),

    /// Event deserialization failed
    #[error("Event deserialization failed: {0}")]
    EventDeserializationFailed(String),

    /// Invalid event type
    #[error("Invalid event type: {0}")]
    InvalidEventType(i16),

    /// Unknown event type
    #[error("Unknown event type: {0}")]
    UnknownEventType(i16),
}

impl SerializationError {
    /// Create an encode failed error
    #[inline]
    pub fn encode_failed(format: &'static str, message: impl Into<String>) -> Self {
        Self::EncodeFailed {
            format,
            message: message.into(),
        }
    }

    /// Create a decode failed error
    #[inline]
    pub fn decode_failed(format: &'static str, message: impl Into<String>) -> Self {
        Self::DecodeFailed {
            format,
            message: message.into(),
        }
    }

    /// Create an invalid format error
    #[inline]
    pub fn invalid_format(expected: &'static str, got: impl Into<String>) -> Self {
        Self::InvalidFormat {
            expected,
            got: got.into(),
        }
    }

    /// Create a missing field error
    #[inline]
    pub fn missing_field(field: &'static str) -> Self {
        Self::MissingField { field }
    }

    /// Create an event serialization failed error
    #[inline]
    pub fn event_serialization_failed(message: impl Into<String>) -> Self {
        Self::EventSerializationFailed(message.into())
    }

    /// Create an event deserialization failed error
    #[inline]
    pub fn event_deserialization_failed(message: impl Into<String>) -> Self {
        Self::EventDeserializationFailed(message.into())
    }

    /// Create an invalid event type error
    #[inline]
    pub fn invalid_event_type(type_id: i16) -> Self {
        Self::InvalidEventType(type_id)
    }

    /// Create an unknown event type error
    #[inline]
    pub fn unknown_event_type(type_id: i16) -> Self {
        Self::UnknownEventType(type_id)
    }
}

#[cfg(feature = "with_serde")]
impl From<serde_json::Error> for SerializationError {
    fn from(e: serde_json::Error) -> Self {
        Self::JsonError(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialization_error() {
        let err = SerializationError::encode_failed("JSON", "unexpected token");
        assert_eq!(err.to_string(), "Encoding failed (JSON): unexpected token");

        let err = SerializationError::decode_failed("Protobuf", "invalid length");
        assert_eq!(err.to_string(), "Decoding failed (Protobuf): invalid length");

        let err = SerializationError::invalid_format("u32", "string".to_string());
        assert_eq!(err.to_string(), "Invalid format: expected u32, got string");

        let err = SerializationError::missing_field("broker_name");
        assert_eq!(err.to_string(), "Missing required field: broker_name");

        let err = SerializationError::InvalidValue {
            field: "timeout",
            reason: "negative".to_string(),
        };
        assert_eq!(err.to_string(), "Invalid value for field 'timeout': negative");

        let err = SerializationError::ProtobufError("missing tag".to_string());
        assert_eq!(err.to_string(), "Protobuf error: missing tag");

        let err = SerializationError::event_serialization_failed("error");
        assert_eq!(err.to_string(), "Event serialization failed: error");

        let err = SerializationError::event_deserialization_failed("error");
        assert_eq!(err.to_string(), "Event deserialization failed: error");

        let err = SerializationError::invalid_event_type(1);
        assert_eq!(err.to_string(), "Invalid event type: 1");

        let err = SerializationError::unknown_event_type(1);
        assert_eq!(err.to_string(), "Unknown event type: 1");
    }

    #[test]
    fn test_utf8_error() {
        let invalid_utf8 = vec![0, 159, 146, 150];
        let result = std::str::from_utf8(&invalid_utf8);
        let utf8_error = result.err().unwrap();
        let err = SerializationError::from(utf8_error);
        assert_eq!(
            err.to_string(),
            "UTF-8 encoding error: invalid utf-8 sequence of 1 bytes from index 1"
        );
    }

    #[cfg(feature = "with_serde")]
    #[test]
    fn test_json_error() {
        let json_result: Result<serde_json::Value, _> = serde_json::from_str("{ invalid }");
        let json_error = json_result.err().unwrap();
        let err = SerializationError::from(json_error);
        assert!(err.to_string().contains("JSON error"));
    }
}
