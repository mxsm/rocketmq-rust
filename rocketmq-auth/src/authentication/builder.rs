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

//! Authentication context builder module.
//!
//! This module provides traits and implementations for building authentication contexts
//! from various request sources (gRPC metadata, Remoting commands, etc.).

pub mod default_authentication_context_builder;

pub use default_authentication_context_builder::DefaultAuthenticationContextBuilder;
use rocketmq_error::AuthError;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;

/// Authentication context builder trait.
///
/// This trait defines the interface for building authentication contexts from different
/// request sources. Implementations should extract authentication credentials (AccessKey,
/// Signature, etc.) from the request and construct an appropriate authentication context.
///
/// # Type Parameters
///
/// * `Context` - The type of authentication context to build
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_auth::authentication::builder::AuthenticationContextBuilder;
///
/// let builder = DefaultAuthenticationContextBuilder::new();
/// let context = builder.build_from_remoting(channel_context, &request)?;
/// ```
pub trait AuthenticationContextBuilder<Context> {
    /// Build an authentication context from a gRPC request.
    ///
    /// Extracts authentication information from gRPC metadata headers including:
    /// - Authorization header (contains Credential and Signature)
    /// - DateTime header (used as signed content)
    /// - Channel ID
    ///
    /// # Arguments
    ///
    /// * `metadata` - gRPC metadata containing authentication headers
    /// * `request` - The gRPC request message
    ///
    /// # Returns
    ///
    /// * `Ok(Context)` - Successfully built authentication context
    /// * `Err(AuthError)` - Failed to parse or validate authentication data
    ///
    /// # Errors
    ///
    /// - Missing or invalid DateTime header
    /// - Malformed Authorization header
    /// - Invalid credential format
    #[cfg(feature = "grpc")]
    fn build_from_grpc(
        &self,
        metadata: &tonic::metadata::MetadataMap,
        request: &dyn std::any::Any,
    ) -> Result<Context, AuthError>;

    /// Build an authentication context from a Remoting command.
    ///
    /// Extracts authentication information from RemotingCommand extFields including:
    /// - AccessKey field
    /// - Signature field
    /// - All other fields (combined as signed content)
    ///
    /// # Arguments
    ///
    /// * `channel_context` - The channel handler context (provides channel ID)
    /// * `request` - The remoting command request
    ///
    /// # Returns
    ///
    /// * `Ok(Context)` - Successfully built authentication context
    /// * `Err(AuthError)` - Failed to extract authentication data
    ///
    /// # Security Notes
    ///
    /// - Filters out UNIQUE_MSG_QUERY_FLAG for versions <= 4.9.3
    /// - Excludes Signature field from signed content
    /// - Sorts fields for consistent signature verification
    fn build_from_remoting(&self, request: &RemotingCommand, channel_id: Option<&str>) -> Result<Context, AuthError>;
}
