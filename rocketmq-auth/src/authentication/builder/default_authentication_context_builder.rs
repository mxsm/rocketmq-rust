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

//! Default implementation of authentication context builder.
//!
//! This module provides the standard implementation for building authentication contexts
//! from both gRPC and Remoting protocol requests.

use std::collections::BTreeMap;

use base64::Engine;
use cheetah_string::CheetahString;
use rocketmq_common::common::mix_all::UNIQUE_MSG_QUERY_FLAG;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_error::AuthError;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
#[cfg(feature = "grpc")]
use tracing::warn;

use crate::authentication::builder::AuthenticationContextBuilder;
use crate::authentication::context::default_authentication_context::DefaultAuthenticationContext;

/// Constants for parsing authentication headers
const CREDENTIAL: &str = "Credential";
const SIGNATURE: &str = "Signature";
const ACCESS_KEY: &str = "AccessKey";
const SIGNATURE_FIELD: &str = "Signature";

/// Separator constants
const SPACE: char = ' ';
const COMMA: char = ',';
const EQUAL: char = '=';
const SLASH: char = '/';

/// Default authentication context builder.
///
/// This builder implements the standard RocketMQ authentication context construction
/// logic, supporting both gRPC (with Authorization header parsing) and Remoting protocol
/// (with extFields parsing) request types.
///
/// # Authentication Format
///
/// ## gRPC Authorization Header Format:
/// ```text
/// MQv2-HMAC-SHA1 Credential=<username>[/<additional>], SignedHeaders=x-mq-date-time, Signature=<hex-signature>
/// ```
///
/// ## Remoting Protocol Format:
/// ExtFields containing:
/// - `AccessKey`: The username/access key
/// - `Signature`: Base64-encoded signature
/// - Other fields: Combined and sorted for signature verification
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_auth::authentication::builder::DefaultAuthenticationContextBuilder;
/// use rocketmq_auth::authentication::builder::AuthenticationContextBuilder;
///
/// let builder = DefaultAuthenticationContextBuilder::new();
///
/// // From remoting command
/// let context = builder.build_from_remoting(&command, Some("channel-123"))?;
/// assert_eq!(context.username(), Some(&CheetahString::from("testuser")));
/// ```
#[derive(Debug, Default)]
pub struct DefaultAuthenticationContextBuilder;

impl DefaultAuthenticationContextBuilder {
    /// Create a new default authentication context builder.
    pub fn new() -> Self {
        Self
    }

    /// Convert hex-encoded string to base64-encoded string.
    ///
    /// This is used to convert the hex signature from gRPC Authorization header
    /// to the base64 format expected by the authentication system.
    ///
    /// # Arguments
    ///
    /// * `hex_input` - Hex-encoded string
    ///
    /// # Returns
    ///
    /// * `Ok(String)` - Base64-encoded string
    /// * `Err(AuthError)` - Invalid hex input
    fn hex_to_base64(hex_input: &str) -> Result<String, AuthError> {
        let bytes = hex::decode(hex_input)
            .map_err(|e| AuthError::InvalidHexSignature(format!("Failed to decode hex: {}", e)))?;
        Ok(base64::engine::general_purpose::STANDARD.encode(&bytes))
    }

    /// Combine request content for signature verification (Remoting protocol).
    ///
    /// This method combines the RemotingCommand fields into a single byte array
    /// for signature verification.
    ///
    /// # Arguments
    ///
    /// * `request` - The remoting command
    /// * `sorted_fields` - Sorted field map (excluding Signature)
    ///
    /// # Returns
    ///
    /// Byte vector containing the combined content
    fn combine_request_content(
        request: &RemotingCommand,
        sorted_fields: &BTreeMap<CheetahString, CheetahString>,
    ) -> Vec<u8> {
        let mut content = Vec::new();

        // Add all sorted field values
        for (key, value) in sorted_fields {
            content.extend_from_slice(key.as_bytes());
            content.extend_from_slice(value.as_bytes());
        }

        // Add request body if present
        if let Some(body) = request.body() {
            content.extend_from_slice(body);
        }

        content
    }
}

impl AuthenticationContextBuilder<DefaultAuthenticationContext> for DefaultAuthenticationContextBuilder {
    #[cfg(feature = "grpc")]
    fn build_from_grpc(
        &self,
        metadata: &tonic::metadata::MetadataMap,
        request: &dyn std::any::Any,
    ) -> Result<DefaultAuthenticationContext, AuthError> {
        let mut context = DefaultAuthenticationContext::new();

        // Extract channel ID from metadata
        if let Some(channel_id) = metadata.get("channel-id") {
            if let Ok(channel_id_str) = channel_id.to_str() {
                context.base.set_channel_id(Some(CheetahString::from(channel_id_str)));
            }
        }

        // Set RPC code from request type name
        let rpc_code = std::any::type_name_of_val(request);
        context.base.set_rpc_code(Some(CheetahString::from(rpc_code)));

        // Extract Authorization header
        let authorization = metadata
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if authorization.is_empty() {
            return Ok(context);
        }

        // Extract DateTime header (required if authorization present)
        let datetime = metadata
            .get("x-mq-date-time")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| AuthError::MissingDateTime("datetime header is required".into()))?;

        // Parse authorization header: "MQv2-HMAC-SHA1 Credential=xxx, Signature=yyy"
        let parts: Vec<&str> = authorization.splitn(2, SPACE).collect();
        if parts.len() != 2 {
            return Err(AuthError::InvalidAuthorizationHeader(
                "authentication header format is incorrect".into(),
            ));
        }

        // Parse key-value pairs: "Credential=xxx, Signature=yyy"
        let key_values: Vec<&str> = parts[1].split(COMMA).collect();
        for key_value in key_values {
            let kv: Vec<&str> = key_value.trim().splitn(2, EQUAL).collect();
            if kv.len() != 2 {
                warn!("Skipping invalid authentication key-value pair: {}", key_value);
                continue;
            }

            let auth_item = kv[0];
            let value = kv[1];

            match auth_item {
                CREDENTIAL => {
                    // Extract username from credential: "username/region/service" or just
                    // "username"
                    let credential_parts: Vec<&str> = value.split(SLASH).collect();
                    if credential_parts.is_empty() {
                        return Err(AuthError::InvalidCredential("credential is empty".into()));
                    }
                    context.set_username(CheetahString::from(credential_parts[0]));
                }
                SIGNATURE => {
                    // Convert hex signature to base64
                    let base64_signature = Self::hex_to_base64(value)?;
                    context.set_signature(CheetahString::from(base64_signature));
                }
                _ => {
                    // Ignore other fields like "SignedHeaders"
                }
            }
        }

        // Set content to datetime bytes for signature verification
        context.set_content(datetime.as_bytes().to_vec());

        Ok(context)
    }

    fn build_from_remoting(
        &self,
        request: &RemotingCommand,
        channel_id: Option<&str>,
    ) -> Result<DefaultAuthenticationContext, AuthError> {
        let mut context = DefaultAuthenticationContext::new();

        // Set channel ID
        if let Some(id) = channel_id {
            context.base.set_channel_id(Some(CheetahString::from(id)));
        }

        // Set RPC code from request code
        context
            .base
            .set_rpc_code(Some(CheetahString::from(request.code().to_string())));

        // Get extFields
        let fields = match request.ext_fields() {
            Some(fields) => fields,
            None => return Ok(context), // No auth fields, return empty context
        };

        // Check for AccessKey
        let access_key = match fields.get(&CheetahString::from(ACCESS_KEY)) {
            Some(key) => key,
            None => return Ok(context), // No AccessKey, return empty context
        };

        // Set username
        context.set_username(access_key.clone());

        // Set signature if present
        if let Some(signature) = fields.get(&CheetahString::from(SIGNATURE_FIELD)) {
            context.set_signature(signature.clone());
        }

        // Build sorted content map for signature verification
        let mut sorted_map = BTreeMap::new();

        for (key, value) in fields.iter() {
            // Filter out UNIQUE_MSG_QUERY_FLAG for old versions
            if request.version() <= RocketMqVersion::V4_9_3.ordinal() as i32 && key.as_str() == UNIQUE_MSG_QUERY_FLAG {
                continue;
            }

            // Exclude Signature field from content
            if key.as_str() != SIGNATURE_FIELD {
                sorted_map.insert(key.clone(), value.clone());
            }
        }

        // Combine request content for signature verification
        let content = Self::combine_request_content(request, &sorted_map);
        context.set_content(content);

        Ok(context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hex_to_base64() {
        let hex = "48656C6C6F";
        let result = DefaultAuthenticationContextBuilder::hex_to_base64(hex).unwrap();
        assert_eq!(result, "SGVsbG8=");
    }

    #[test]
    fn test_hex_to_base64_invalid() {
        let hex = "ZZZZ";
        let result = DefaultAuthenticationContextBuilder::hex_to_base64(hex);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_from_remoting_no_ext_fields() {
        use rocketmq_remoting::protocol::command_custom_header::CommandCustomHeader;

        #[derive(Debug, Clone)]
        struct EmptyHeader;
        impl CommandCustomHeader for EmptyHeader {
            fn to_map(
                &self,
            ) -> Option<std::collections::HashMap<cheetah_string::CheetahString, cheetah_string::CheetahString>>
            {
                None
            }
        }

        let builder = DefaultAuthenticationContextBuilder::new();
        let request = RemotingCommand::create_request_command(1, EmptyHeader);
        let result = builder.build_from_remoting(&request, Some("test-channel")).unwrap();

        assert_eq!(result.base.channel_id(), Some(&CheetahString::from("test-channel")));
        assert!(result.username().is_none());
    }
}
