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
use std::collections::HashMap;

use base64::Engine;
use cheetah_string::CheetahString;
use chrono::DateTime;
use chrono::NaiveDateTime;
use rocketmq_common::common::mix_all::UNIQUE_MSG_QUERY_FLAG;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_error::AuthError;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use tracing::warn;

use crate::authentication::builder::AuthenticationContextBuilder;
use crate::authentication::context::default_authentication_context::DefaultAuthenticationContext;

/// Constants for parsing authentication headers
const CREDENTIAL: &str = "Credential";
const SIGNATURE: &str = "Signature";
const ACCESS_KEY: &str = "AccessKey";
const SIGNATURE_FIELD: &str = "Signature";
const X_MQ_DATE_TIME: &str = "x-mq-date-time";
const DATE_TIME: &str = "DateTime";
const TIMESTAMP: &str = "Timestamp";
const TIMESTAMP_LOWERCASE: &str = "timestamp";

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
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultAuthenticationContextBuilder;

impl DefaultAuthenticationContextBuilder {
    /// Create a new default authentication context builder.
    pub fn new() -> Self {
        Self
    }

    /// Build a gRPC authentication context from plain metadata values.
    ///
    /// This keeps provider-level metadata parsing available without requiring callers
    /// to construct a tonic `MetadataMap`.
    pub fn build_from_grpc_metadata_map(
        &self,
        metadata: &HashMap<String, String>,
        request: &dyn std::any::Any,
    ) -> Result<DefaultAuthenticationContext, AuthError> {
        Self::build_from_grpc_values(
            request,
            Self::metadata_value(metadata, "authorization"),
            Self::metadata_value(metadata, X_MQ_DATE_TIME),
            Self::metadata_value(metadata, "channel-id"),
        )
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

        // Match Java AclUtils.combineRequestContent: concatenate sorted ext field
        // values only, then append the request body. Field names are not signed.
        for value in sorted_fields.values() {
            content.extend_from_slice(value.as_bytes());
        }

        // Add request body if present
        if let Some(body) = request.body() {
            content.extend_from_slice(body);
        }

        content
    }

    fn parse_request_timestamp_millis(input: &str) -> Option<i64> {
        let value = input.trim();
        if value.is_empty() {
            return None;
        }

        if value.bytes().all(|byte| byte.is_ascii_digit()) {
            let timestamp = value.parse::<i64>().ok()?;
            return if value.len() == 10 {
                timestamp.checked_mul(1000)
            } else {
                Some(timestamp)
            };
        }

        if let Ok(datetime) = NaiveDateTime::parse_from_str(value, "%Y%m%dT%H%M%SZ") {
            return Some(datetime.and_utc().timestamp_millis());
        }

        DateTime::parse_from_rfc3339(value)
            .ok()
            .map(|datetime| datetime.timestamp_millis())
    }

    fn set_request_timestamp(context: &mut DefaultAuthenticationContext, value: &str) {
        context.set_request_timestamp(CheetahString::from(value));
        if let Some(timestamp_millis) = Self::parse_request_timestamp_millis(value) {
            context.set_request_timestamp_millis(timestamp_millis);
        }
    }

    fn metadata_value<'a>(metadata: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
        metadata.get(key).map(String::as_str).or_else(|| {
            metadata
                .iter()
                .find_map(|(candidate, value)| candidate.eq_ignore_ascii_case(key).then_some(value.as_str()))
        })
    }

    fn build_from_grpc_values(
        request: &dyn std::any::Any,
        authorization: Option<&str>,
        datetime: Option<&str>,
        channel_id: Option<&str>,
    ) -> Result<DefaultAuthenticationContext, AuthError> {
        let mut context = DefaultAuthenticationContext::new();

        if let Some(channel_id) = channel_id {
            context.base.set_channel_id(Some(CheetahString::from(channel_id)));
        }

        let rpc_code = std::any::type_name_of_val(request);
        context.base.set_rpc_code(Some(CheetahString::from(rpc_code)));

        let authorization = authorization.unwrap_or("");
        if authorization.is_empty() {
            return Ok(context);
        }

        let datetime = datetime.ok_or_else(|| AuthError::MissingDateTime("datetime header is required".into()))?;
        Self::set_request_timestamp(&mut context, datetime);

        let parts: Vec<&str> = authorization.splitn(2, SPACE).collect();
        if parts.len() != 2 {
            return Err(AuthError::InvalidAuthorizationHeader(
                "authentication header format is incorrect".into(),
            ));
        }

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
                    let credential_parts: Vec<&str> = value.split(SLASH).collect();
                    if credential_parts.is_empty() {
                        return Err(AuthError::InvalidCredential("credential is empty".into()));
                    }
                    context.set_username(CheetahString::from(credential_parts[0]));
                }
                SIGNATURE => {
                    let base64_signature = Self::hex_to_base64(value)?;
                    context.set_signature(CheetahString::from(base64_signature));
                }
                _ => {}
            }
        }

        context.set_content(datetime.as_bytes().to_vec());

        Ok(context)
    }

    fn remoting_request_timestamp(fields: &std::collections::HashMap<CheetahString, CheetahString>) -> Option<&str> {
        fields
            .get(&CheetahString::from(TIMESTAMP))
            .or_else(|| fields.get(&CheetahString::from(TIMESTAMP_LOWERCASE)))
            .or_else(|| fields.get(&CheetahString::from(DATE_TIME)))
            .or_else(|| fields.get(&CheetahString::from(X_MQ_DATE_TIME)))
            .map(CheetahString::as_str)
    }
}

impl AuthenticationContextBuilder<DefaultAuthenticationContext> for DefaultAuthenticationContextBuilder {
    #[cfg(feature = "grpc")]
    fn build_from_grpc(
        &self,
        metadata: &tonic::metadata::MetadataMap,
        request: &dyn std::any::Any,
    ) -> Result<DefaultAuthenticationContext, AuthError> {
        Self::build_from_grpc_values(
            request,
            metadata.get("authorization").and_then(|v| v.to_str().ok()),
            metadata.get(X_MQ_DATE_TIME).and_then(|v| v.to_str().ok()),
            metadata.get("channel-id").and_then(|v| v.to_str().ok()),
        )
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

        if let Some(request_timestamp) = Self::remoting_request_timestamp(fields) {
            Self::set_request_timestamp(&mut context, request_timestamp);
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
    use std::collections::HashMap;

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
    fn test_combine_request_content_matches_java_value_only_order() {
        let request = RemotingCommand::create_remoting_command(10).set_body(Vec::from("body"));
        let mut sorted_fields = BTreeMap::new();
        sorted_fields.insert(CheetahString::from("AccessKey"), CheetahString::from("alice"));
        sorted_fields.insert(CheetahString::from("topic"), CheetahString::from("topic-a"));

        let content = DefaultAuthenticationContextBuilder::combine_request_content(&request, &sorted_fields);

        assert_eq!(content, b"alicetopic-abody");
    }

    #[test]
    fn parse_request_timestamp_millis_supports_java_grpc_format() {
        let timestamp = DefaultAuthenticationContextBuilder::parse_request_timestamp_millis("20231227T194619Z");

        assert_eq!(timestamp, Some(1_703_706_379_000));
    }

    #[test]
    fn parse_request_timestamp_millis_supports_epoch_seconds_and_millis() {
        assert_eq!(
            DefaultAuthenticationContextBuilder::parse_request_timestamp_millis("1703706379"),
            Some(1_703_706_379_000)
        );
        assert_eq!(
            DefaultAuthenticationContextBuilder::parse_request_timestamp_millis("1703706379000"),
            Some(1_703_706_379_000)
        );
    }

    #[test]
    fn parse_request_timestamp_millis_supports_rfc3339() {
        let timestamp = DefaultAuthenticationContextBuilder::parse_request_timestamp_millis("2023-12-27T19:46:19Z");

        assert_eq!(timestamp, Some(1_703_706_379_000));
    }

    #[test]
    fn build_from_grpc_metadata_map_parses_authorization_header() {
        let builder = DefaultAuthenticationContextBuilder::new();
        let mut metadata = HashMap::new();
        metadata.insert(
            "Authorization".to_owned(),
            "MQv2-HMAC-SHA1 Credential=alice/us-east/service, SignedHeaders=x-mq-date-time, Signature=48656C6C6F"
                .to_owned(),
        );
        metadata.insert("X-Mq-Date-Time".to_owned(), "20231227T194619Z".to_owned());
        metadata.insert("Channel-Id".to_owned(), "channel-a".to_owned());

        let context = builder.build_from_grpc_metadata_map(&metadata, &()).unwrap();

        assert_eq!(context.username(), Some(&CheetahString::from("alice")));
        assert_eq!(context.signature(), Some(&CheetahString::from("SGVsbG8=")));
        assert_eq!(context.content(), Some(b"20231227T194619Z".as_slice()));
        assert_eq!(context.base.channel_id(), Some(&CheetahString::from("channel-a")));
        assert_eq!(context.request_timestamp_millis(), Some(1_703_706_379_000));
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

    #[test]
    fn test_build_from_remoting_uses_java_signature_content() {
        let mut ext_fields = HashMap::new();
        ext_fields.insert(CheetahString::from("topic"), CheetahString::from("topic-a"));
        ext_fields.insert(CheetahString::from("AccessKey"), CheetahString::from("alice"));
        ext_fields.insert(CheetahString::from("Signature"), CheetahString::from("sig"));

        let request = RemotingCommand::create_remoting_command(10)
            .set_ext_fields(ext_fields)
            .set_body(Vec::from("body"));
        let builder = DefaultAuthenticationContextBuilder::new();

        let context = builder.build_from_remoting(&request, Some("test-channel")).unwrap();

        assert_eq!(context.username(), Some(&CheetahString::from("alice")));
        assert_eq!(context.signature(), Some(&CheetahString::from("sig")));
        assert_eq!(context.content(), Some(b"alicetopic-abody".as_slice()));
    }

    #[test]
    fn test_build_from_remoting_extracts_request_timestamp() {
        let mut ext_fields = HashMap::new();
        ext_fields.insert(CheetahString::from("AccessKey"), CheetahString::from("alice"));
        ext_fields.insert(CheetahString::from("Signature"), CheetahString::from("sig"));
        ext_fields.insert(CheetahString::from("Timestamp"), CheetahString::from("1703706379000"));

        let request = RemotingCommand::create_remoting_command(10).set_ext_fields(ext_fields);
        let builder = DefaultAuthenticationContextBuilder::new();

        let context = builder.build_from_remoting(&request, Some("test-channel")).unwrap();

        assert_eq!(context.request_timestamp(), Some(&CheetahString::from("1703706379000")));
        assert_eq!(context.request_timestamp_millis(), Some(1_703_706_379_000));
    }
}
