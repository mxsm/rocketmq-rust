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

use crate::fields;
use crate::ErrorContext;
use crate::ErrorDescriptor;
use crate::PROTOCOL_ENCODING_UNSUPPORTED;
use crate::PROTOCOL_REQUEST_UNSUPPORTED;

/// Protocol validation and processing errors
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// Invalid command code
    #[error("Invalid command code: {code}")]
    /// The invalid command value.
    InvalidCommand {
        /// The code value.
        code: i32,
    },

    /// Unsupported serialization type
    #[error("Unsupported serialization type: {serialize_type}")]
    /// The unsupported serialization type value.
    UnsupportedSerializationType {
        /// The serialize type value.
        serialize_type: u8,
    },
}

impl ProtocolError {
    /// Returns the canonical descriptor for this protocol failure.
    pub const fn descriptor(&self) -> &'static ErrorDescriptor {
        match self {
            Self::InvalidCommand { .. } => &PROTOCOL_REQUEST_UNSUPPORTED,
            Self::UnsupportedSerializationType { .. } => &PROTOCOL_ENCODING_UNSUPPORTED,
        }
    }

    /// Returns descriptor-valid protocol context.
    pub fn context(&self) -> ErrorContext {
        match self {
            Self::InvalidCommand { code } => ErrorContext::new().with_i64(fields::REQUEST_CODE, i64::from(*code)),
            Self::UnsupportedSerializationType { serialize_type } => {
                ErrorContext::new().with_u64(fields::SERIALIZATION_TYPE, u64::from(*serialize_type))
            }
        }
    }

    /// Create an invalid command error
    #[inline]
    pub fn invalid_command(code: i32) -> Self {
        Self::InvalidCommand { code }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_error() {
        let err = ProtocolError::invalid_command(999);
        assert_eq!(err.to_string(), "Invalid command code: 999");

        let err = ProtocolError::UnsupportedSerializationType { serialize_type: 2 };
        assert_eq!(err.to_string(), "Unsupported serialization type: 2");
    }
}
