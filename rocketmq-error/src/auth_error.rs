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

//! Authentication error types.

use thiserror::Error;

/// Authentication error types.
///
/// This enum represents various authentication errors that can occur during
/// the authentication process, including context building, credential validation,
/// and signature verification.
#[derive(Debug, Clone, Error)]
pub enum AuthError {
    /// Missing DateTime header in gRPC metadata
    #[error("Missing DateTime header: {0}")]
    MissingDateTime(String),

    /// Invalid Authorization header format
    #[error("Invalid Authorization header: {0}")]
    InvalidAuthorizationHeader(String),

    /// Invalid credential format or content
    #[error("Invalid credential: {0}")]
    InvalidCredential(String),

    /// Invalid hex-encoded signature
    #[error("Invalid hex signature: {0}")]
    InvalidHexSignature(String),

    /// Generic authentication context creation error
    #[error("Failed to create authentication context: {0}")]
    ContextCreationError(String),

    /// User authentication failed
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    /// User not found
    #[error("User not found: {0}")]
    UserNotFound(String),

    /// Invalid signature
    #[error("Invalid signature: {0}")]
    InvalidSignature(String),

    /// User status is not valid (disabled, deleted, etc.)
    #[error("Invalid user status: {0}")]
    InvalidUserStatus(String),

    /// Generic error with custom message
    #[error("Authentication error: {0}")]
    Other(String),
}

impl From<String> for AuthError {
    fn from(msg: String) -> Self {
        AuthError::Other(msg)
    }
}

impl From<&str> for AuthError {
    fn from(msg: &str) -> Self {
        AuthError::Other(msg.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_error_variants() {
        let errors = vec![
            AuthError::AuthenticationFailed("could not authenticate".to_string()),
            AuthError::ContextCreationError("could not create context".to_string()),
            AuthError::InvalidAuthorizationHeader("invalid authorization header".to_string()),
            AuthError::InvalidCredential("invalid credential".to_string()),
            AuthError::InvalidHexSignature("invalid hex signature".to_string()),
            AuthError::InvalidSignature("invalid signature".to_string()),
            AuthError::InvalidUserStatus("invalid user status".to_string()),
            AuthError::MissingDateTime("missing date time".to_string()),
            AuthError::Other("other error".to_string()),
            AuthError::UserNotFound("user not found".to_string()),
        ];

        for error in errors {
            let _msg = format!("{}", error);
            let _debug = format!("{:?}", error);
        }
    }

    #[test]
    fn test_from_string_creates_other() {
        let error: AuthError = String::from("custom error").into();

        match error {
            AuthError::Other(msg) => assert_eq!(msg, "custom error"),
            _ => panic!("Expected AuthError::Other"),
        }
    }

    #[test]
    fn test_from_str_creates_other() {
        let error: AuthError = "custom error".into();

        match error {
            AuthError::Other(msg) => assert_eq!(msg, "custom error"),
            _ => panic!("Expected AuthError::Other"),
        }
    }
}
