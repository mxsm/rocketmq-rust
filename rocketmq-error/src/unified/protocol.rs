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

//! RocketMQ protocol-specific errors

use thiserror::Error;

/// Protocol validation and processing errors
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// Invalid command code
    #[error("Invalid command code: {code}")]
    InvalidCommand { code: i32 },

    /// Unsupported protocol version
    #[error("Unsupported protocol version: {version}")]
    UnsupportedVersion { version: i32 },

    /// Required header field is missing
    #[error("Missing required header field: {field}")]
    HeaderMissing { field: &'static str },

    /// Required body is missing
    #[error("Missing required message body")]
    BodyMissing,

    /// Checksum mismatch
    #[error("Checksum mismatch: expected {expected:x}, got {actual:x}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    /// Invalid message format
    #[error("Invalid message format: {reason}")]
    InvalidMessage { reason: String },

    /// Protocol decode error
    #[error("Protocol decode error: ext_fields_length={ext_fields_len}, header_length={header_len}")]
    DecodeError { ext_fields_len: usize, header_len: usize },

    /// Unsupported serialization type
    #[error("Unsupported serialization type: {serialize_type}")]
    UnsupportedSerializationType { serialize_type: u8 },
}

impl ProtocolError {
    /// Create an invalid command error
    #[inline]
    pub fn invalid_command(code: i32) -> Self {
        Self::InvalidCommand { code }
    }

    /// Create a header missing error
    #[inline]
    pub fn header_missing(field: &'static str) -> Self {
        Self::HeaderMissing { field }
    }

    /// Create a checksum mismatch error
    #[inline]
    pub fn checksum_mismatch(expected: u32, actual: u32) -> Self {
        Self::ChecksumMismatch { expected, actual }
    }

    /// Create an invalid message error
    #[inline]
    pub fn invalid_message(reason: impl Into<String>) -> Self {
        Self::InvalidMessage { reason: reason.into() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_error() {
        let err = ProtocolError::invalid_command(999);
        assert_eq!(err.to_string(), "Invalid command code: 999");

        let err = ProtocolError::UnsupportedVersion { version: 1 };
        assert_eq!(err.to_string(), "Unsupported protocol version: 1");

        let err = ProtocolError::header_missing("topic");
        assert_eq!(err.to_string(), "Missing required header field: topic");

        let err = ProtocolError::BodyMissing;
        assert_eq!(err.to_string(), "Missing required message body");

        let err = ProtocolError::checksum_mismatch(0xABCD, 0x1234);
        assert!(err.to_string().contains("abcd"));
        assert!(err.to_string().contains("1234"));

        let err = ProtocolError::invalid_message("too long");
        assert_eq!(err.to_string(), "Invalid message format: too long");

        let err = ProtocolError::DecodeError {
            ext_fields_len: 10,
            header_len: 20,
        };
        assert_eq!(
            err.to_string(),
            "Protocol decode error: ext_fields_length=10, header_length=20"
        );

        let err = ProtocolError::UnsupportedSerializationType { serialize_type: 2 };
        assert_eq!(err.to_string(), "Unsupported serialization type: 2");
    }
}
