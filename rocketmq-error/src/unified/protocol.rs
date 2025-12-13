//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

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
    #[error(
        "Protocol decode error: ext_fields_length={ext_fields_len}, header_length={header_len}"
    )]
    DecodeError {
        ext_fields_len: usize,
        header_len: usize,
    },

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
        Self::InvalidMessage {
            reason: reason.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_error() {
        let err = ProtocolError::invalid_command(999);
        assert_eq!(err.to_string(), "Invalid command code: 999");
    }

    #[test]
    fn test_checksum_mismatch() {
        let err = ProtocolError::checksum_mismatch(0xABCD, 0x1234);
        assert!(err.to_string().contains("abcd"));
        assert!(err.to_string().contains("1234"));
    }
}
