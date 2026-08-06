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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::adapters::build_chat_transport_request;
use crate::adapters::parse_chat_transport_response;
use crate::error::ProviderError;
use crate::error::ProviderErrorCode;
use crate::ir::CanonicalModelRequest;
use crate::ir::CanonicalModelResponse;
use crate::profile::ProviderCapabilities;
use crate::profile::ProviderFamily;
use crate::profile::ProviderHealth;
use crate::profile::ProviderProfile;
use crate::provider::InvocationContext;
use crate::secret::SecretMaterial;
use crate::secret::SecretReferenceKind;
use crate::stream::AsyncBoundedModelStream;
use crate::transport::AsyncModelTransport;

/// Async end-to-end client for the four built-in provider protocol families.
///
/// Credential resolution is deliberately outside this type. A service should
/// use an asynchronous secret manager, or place a legacy synchronous
/// [`crate::SecretProvider`] lookup on its owned
/// `ServiceContext::metadata_io().spawn_io(...)` lane, then pass only the
/// resulting [`SecretMaterial`] into [`Self::invoke`]. This prevents secret
/// filesystem or SDK work from blocking the async runtime.
pub struct AsyncBuiltinProviderClient {
    profile: ProviderProfile,
    transport: Arc<dyn AsyncModelTransport>,
}

impl AsyncBuiltinProviderClient {
    /// Creates an async client for an OpenAI-compatible, Anthropic, Gemini, or
    /// Bedrock profile.
    ///
    /// # Errors
    ///
    /// Returns a profile error for invalid profiles, Provider SPI profiles, or
    /// adapter-owned credential references.
    pub fn new(profile: ProviderProfile, transport: Arc<dyn AsyncModelTransport>) -> Result<Self, ProviderError> {
        profile.validate()?;
        if profile.provider_family == ProviderFamily::ProviderSpi {
            return Err(ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "provider SPI profiles require ProviderSpiClient",
            ));
        }
        if profile
            .credential_ref
            .as_ref()
            .is_some_and(|reference| reference.kind() == SecretReferenceKind::Adapter)
        {
            return Err(ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "built-in provider credentials must be gateway-owned",
            ));
        }
        Ok(Self { profile, transport })
    }

    /// Returns the stable configured profile identifier.
    #[must_use]
    pub fn profile_id(&self) -> &str {
        &self.profile.id
    }

    /// Returns the profile's declared capabilities.
    #[must_use]
    pub fn capabilities(&self) -> &ProviderCapabilities {
        &self.profile.capabilities
    }

    /// Returns the profile's routing health.
    #[must_use]
    pub const fn health(&self) -> ProviderHealth {
        self.profile.health
    }

    /// Invokes the configured provider without blocking the async runtime.
    ///
    /// `credential` must correspond to the profile's reference and must be
    /// resolved immediately before the invocation when rotation is possible.
    ///
    /// # Errors
    ///
    /// Returns stable validation, secret, deadline, transport, provider, or
    /// protocol errors. Errors never contain endpoint, payload, or credential
    /// material.
    pub async fn invoke(
        &self,
        context: &InvocationContext,
        request: &CanonicalModelRequest,
        credential: Option<SecretMaterial>,
    ) -> Result<CanonicalModelResponse, ProviderError> {
        self.validate_credential_presence(credential.as_ref())?;
        let transport_request = build_chat_transport_request(&self.profile, context, request, credential)?;
        let response = self.transport.invoke(transport_request).await?;
        parse_chat_transport_response(&self.profile, response, context.max_response_bytes)
    }

    /// Starts a bounded provider stream through the canonical event model.
    ///
    /// The returned stream is pull-based. Cancelling or dropping it stops
    /// provider body reads without an unowned background task.
    ///
    /// # Errors
    ///
    /// Returns stable validation, secret, deadline, transport, provider,
    /// protocol, cancellation, or output-bound errors.
    pub async fn invoke_stream(
        &self,
        context: &InvocationContext,
        request: &CanonicalModelRequest,
        credential: Option<SecretMaterial>,
    ) -> Result<AsyncBoundedModelStream, ProviderError> {
        self.validate_credential_presence(credential.as_ref())?;
        let mut streaming_request = request.clone();
        streaming_request.stream = true;
        let transport_request = build_chat_transport_request(&self.profile, context, &streaming_request, credential)?;
        self.transport
            .invoke_stream(transport_request, context.stream_bounds, context.cancellation.clone())
            .await
    }

    fn validate_credential_presence(&self, credential: Option<&SecretMaterial>) -> Result<(), ProviderError> {
        match (self.profile.credential_ref.is_some(), credential.is_some()) {
            (true, false) => {
                return Err(ProviderError::new(
                    ProviderErrorCode::SecretUnavailable,
                    "model provider credential is unavailable",
                ));
            }
            (false, true) => {
                return Err(ProviderError::policy_denied(
                    "credential was supplied to a profile without a credential reference",
                ));
            }
            _ => {}
        }
        Ok(())
    }
}

impl Debug for AsyncBuiltinProviderClient {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AsyncBuiltinProviderClient")
            .field("profile_id", &self.profile.id)
            .field("provider_family", &self.profile.provider_family)
            .field("dialect", &self.profile.dialect)
            .field("endpoint", &"[ENDPOINT REDACTED]")
            .finish_non_exhaustive()
    }
}
