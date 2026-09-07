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

use rocketmq_sre_contracts::CorrelationId;

use crate::error::ProviderError;
use crate::error::ProviderOperationalFailure;
use crate::error::ProviderRejection;
use crate::error::ProviderStatusOutcome;
use crate::ir::CanonicalModelRequest;
use crate::ir::CanonicalModelResponse;
use crate::profile::ProviderCapabilities;
use crate::profile::ProviderFamily;
use crate::profile::ProviderHealth;
use crate::profile::ProviderProfile;
use crate::provider::ChatModelProvider;
use crate::provider::InvocationContext;
use crate::secret::SecretReferenceKind;
use crate::stream::BoundedModelStream;
use crate::stream::StreamBounds;

/// Current process-external provider SPI wire version.
pub const PROVIDER_SPI_WIRE_VERSION: &str = "rocketmq-sre.provider-spi.v1";

/// Credential ownership declared during SPI handshake.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialOwner {
    Gateway,
    Adapter,
}

/// Version and mTLS identity handshake request.
#[derive(Clone, Debug)]
pub struct SpiHandshakeRequest {
    pub wire_version: String,
    pub gateway_identity: String,
    pub correlation_id: CorrelationId,
    pub max_payload_bytes: usize,
}

/// Version, capability, and credential-ownership handshake response.
#[derive(Clone, Debug)]
pub struct SpiHandshakeResponse {
    pub wire_version: String,
    pub adapter_identity: String,
    pub credential_owner: CredentialOwner,
    pub capabilities: ProviderCapabilities,
    pub credential_version_fingerprint: Option<String>,
}

/// Unary SPI invocation request. It contains no credential material.
#[derive(Clone, Debug)]
pub struct SpiInvokeRequest {
    pub invocation_id: String,
    pub correlation_id: CorrelationId,
    pub deadline_unix_ms: Option<u64>,
    pub request: CanonicalModelRequest,
}

/// Streaming SPI invocation request.
#[derive(Clone, Debug)]
pub struct SpiStreamRequest {
    pub invocation_id: String,
    pub correlation_id: CorrelationId,
    pub deadline_unix_ms: Option<u64>,
    pub bounds: StreamBounds,
    pub request: CanonicalModelRequest,
}

/// Cancellation request for one SPI invocation.
#[derive(Clone, Debug)]
pub struct SpiCancelRequest {
    pub invocation_id: String,
    pub correlation_id: CorrelationId,
}

/// Redacted SPI health response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpiHealth {
    pub status: ProviderHealth,
    pub credential_version_fingerprint: Option<String>,
}

/// Process-external provider adapter contract.
///
/// A production transport maps these methods to the versioned gRPC contract in
/// `proto/provider/v1/provider.proto`. The adapter has its own workload
/// identity and its own `SecretProvider`; no method carries plaintext
/// credentials or RocketMQ/MCP/executor authority.
pub trait ProviderSpi: Send + Sync {
    /// Negotiates wire version, capabilities, identity, and credential owner.
    fn handshake(&self, request: &SpiHandshakeRequest) -> Result<SpiHandshakeResponse, ProviderStatusOutcome>;

    /// Executes a bounded unary invocation.
    fn invoke(&self, request: &SpiInvokeRequest) -> Result<CanonicalModelResponse, ProviderStatusOutcome>;

    /// Starts a bounded stream over the process-external transport.
    fn invoke_stream(&self, request: &SpiStreamRequest) -> Result<BoundedModelStream, ProviderStatusOutcome>;

    /// Cancels one invocation.
    fn cancel(&self, request: &SpiCancelRequest) -> Result<(), ProviderStatusOutcome>;

    /// Returns current adapter and credential-version health.
    fn health(&self) -> Result<SpiHealth, ProviderStatusOutcome>;
}

/// Fail-closed provider SPI client configuration.
#[derive(Clone, Debug)]
pub struct SpiClientConfig {
    pub wire_version: String,
    pub gateway_identity: String,
    pub expected_adapter_identity: String,
    pub max_payload_bytes: usize,
}

impl SpiClientConfig {
    /// Creates a mutual-TLS identity configuration with the current wire
    /// version and a four-MiB payload bound.
    #[must_use]
    pub fn mutual_tls(gateway_identity: impl Into<String>, expected_adapter_identity: impl Into<String>) -> Self {
        Self {
            wire_version: PROVIDER_SPI_WIRE_VERSION.to_owned(),
            gateway_identity: gateway_identity.into(),
            expected_adapter_identity: expected_adapter_identity.into(),
            max_payload_bytes: 4 * 1024 * 1024,
        }
    }

    fn validate(&self) -> Result<(), ProviderStatusOutcome> {
        if !self.gateway_identity.starts_with("spiffe://")
            || !self.expected_adapter_identity.starts_with("spiffe://")
            || self.gateway_identity == self.expected_adapter_identity
        {
            return Err(ProviderStatusOutcome::rejected(ProviderRejection::MutualTlsFailed));
        }
        if self.max_payload_bytes == 0 {
            return Err(ProviderStatusOutcome::rejected(ProviderRejection::ProfileInvalid));
        }
        Ok(())
    }
}

/// Verified client for one process-external adapter.
pub struct ProviderSpiClient {
    adapter: Arc<dyn ProviderSpi>,
    config: SpiClientConfig,
    handshake: SpiHandshakeResponse,
}

impl Debug for ProviderSpiClient {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderSpiClient")
            .field("wire_version", &self.handshake.wire_version)
            .field("adapter_identity", &self.handshake.adapter_identity)
            .field("credential_owner", &self.handshake.credential_owner)
            .field("max_payload_bytes", &self.config.max_payload_bytes)
            .finish_non_exhaustive()
    }
}

impl ProviderSpiClient {
    /// Connects and validates wire version, mutual identity, and adapter-owned
    /// credentials.
    ///
    /// # Errors
    ///
    /// Fails closed on version drift, identity mismatch, gateway-owned
    /// credentials, or an invalid payload bound.
    pub fn connect(adapter: Arc<dyn ProviderSpi>, config: SpiClientConfig) -> Result<Self, ProviderStatusOutcome> {
        config.validate()?;
        let request = SpiHandshakeRequest {
            wire_version: config.wire_version.clone(),
            gateway_identity: config.gateway_identity.clone(),
            correlation_id: CorrelationId::new(),
            max_payload_bytes: config.max_payload_bytes,
        };
        let handshake = adapter.handshake(&request)?;
        if handshake.wire_version != config.wire_version {
            return Err(ProviderStatusOutcome::rejected(
                ProviderRejection::UnsupportedWireVersion,
            ));
        }
        if handshake.adapter_identity != config.expected_adapter_identity {
            return Err(ProviderStatusOutcome::rejected(ProviderRejection::MutualTlsFailed));
        }
        if handshake.credential_owner != CredentialOwner::Adapter {
            return Err(ProviderStatusOutcome::rejected(ProviderRejection::AuthorizationFailed));
        }
        Ok(Self {
            adapter,
            config,
            handshake,
        })
    }

    /// Returns the negotiated credential owner.
    #[must_use]
    pub const fn credential_owner(&self) -> CredentialOwner {
        self.handshake.credential_owner
    }

    /// Returns negotiated provider capabilities.
    #[must_use]
    pub fn capabilities(&self) -> ProviderCapabilities {
        self.handshake.capabilities.clone()
    }

    /// Invokes the adapter without passing credentials.
    ///
    /// # Errors
    ///
    /// Returns deadline, cancellation, payload-bound, adapter, or
    /// response-bound failures.
    pub fn invoke(
        &self,
        context: &InvocationContext,
        request: &CanonicalModelRequest,
    ) -> Result<CanonicalModelResponse, ProviderStatusOutcome> {
        context.ensure_active()?;
        self.ensure_payload_bound(request)?;
        let response = self.adapter.invoke(&SpiInvokeRequest {
            invocation_id: format!("spi-{}", context.correlation_id),
            correlation_id: context.correlation_id,
            deadline_unix_ms: context.deadline_unix_ms,
            request: request.clone(),
        })?;
        let response_bytes = serde_json::to_vec(&response)
            .map_err(|source| ProviderError::from_source(ProviderOperationalFailure::ProtocolError, source))?;
        if response_bytes.len() > context.max_response_bytes {
            return Err(ProviderError::new(
                ProviderOperationalFailure::OutputTooLarge,
                "provider SPI response exceeded configured bounds",
            )
            .into());
        }
        Ok(response)
    }

    /// Starts an adapter-owned bounded stream.
    ///
    /// # Errors
    ///
    /// Returns a stable SPI error when bounds, deadline, or adapter startup
    /// fail. Stream events are carried by the production gRPC transport.
    pub fn invoke_stream(
        &self,
        context: &InvocationContext,
        request: &CanonicalModelRequest,
    ) -> Result<BoundedModelStream, ProviderStatusOutcome> {
        context.ensure_active()?;
        self.ensure_payload_bound(request)?;
        self.adapter.invoke_stream(&SpiStreamRequest {
            invocation_id: format!("spi-{}", context.correlation_id),
            correlation_id: context.correlation_id,
            deadline_unix_ms: context.deadline_unix_ms,
            bounds: context.stream_bounds,
            request: request.clone(),
        })
    }

    /// Cancels one adapter invocation.
    ///
    /// # Errors
    ///
    /// Returns the adapter's stable cancellation error.
    pub fn cancel(
        &self,
        invocation_id: impl Into<String>,
        correlation_id: CorrelationId,
    ) -> Result<(), ProviderStatusOutcome> {
        self.adapter.cancel(&SpiCancelRequest {
            invocation_id: invocation_id.into(),
            correlation_id,
        })
    }

    /// Queries redacted adapter health.
    ///
    /// # Errors
    ///
    /// Returns a stable adapter error.
    pub fn health(&self) -> Result<SpiHealth, ProviderStatusOutcome> {
        self.adapter.health()
    }

    fn ensure_payload_bound(&self, request: &CanonicalModelRequest) -> Result<(), ProviderError> {
        let bytes = serde_json::to_vec(request)
            .map_err(|source| ProviderError::from_source(ProviderOperationalFailure::ProtocolError, source))?;
        if bytes.len() > self.config.max_payload_bytes {
            Err(ProviderError::new(
                ProviderOperationalFailure::OutputTooLarge,
                "provider SPI request exceeded configured bounds",
            ))
        } else {
            Ok(())
        }
    }
}

/// Chat provider facade over a verified process-external SPI client.
pub struct ProviderSpiChatAdapter {
    profile: ProviderProfile,
    client: ProviderSpiClient,
}

impl ProviderSpiChatAdapter {
    /// Creates an SPI chat adapter.
    ///
    /// # Errors
    ///
    /// Returns a profile error unless the profile uses `provider_spi` and its
    /// declared capabilities are a subset of the negotiated capabilities.
    pub fn new(profile: ProviderProfile, client: ProviderSpiClient) -> Result<Self, ProviderStatusOutcome> {
        profile.validate()?;
        if profile.provider_family != ProviderFamily::ProviderSpi
            || profile
                .credential_ref
                .as_ref()
                .is_none_or(|reference| reference.kind() != SecretReferenceKind::Adapter)
        {
            return Err(ProviderStatusOutcome::rejected(ProviderRejection::ProfileInvalid));
        }
        if !client.capabilities().supports_all(&profile.capabilities.supported) {
            return Err(ProviderStatusOutcome::rejected(ProviderRejection::ProfileInvalid));
        }
        Ok(Self { profile, client })
    }
}

impl ChatModelProvider for ProviderSpiChatAdapter {
    fn profile_id(&self) -> &str {
        &self.profile.id
    }

    fn capabilities(&self) -> ProviderCapabilities {
        self.client.capabilities()
    }

    fn health(&self) -> ProviderHealth {
        self.client
            .health()
            .map_or(ProviderHealth::Unavailable, |health| health.status)
    }

    fn invoke(
        &self,
        context: &InvocationContext,
        request: &CanonicalModelRequest,
    ) -> Result<CanonicalModelResponse, ProviderStatusOutcome> {
        self.profile.capabilities.ensure_request_supported(request)?;
        self.client.invoke(context, request)
    }

    fn invoke_stream(
        &self,
        context: &InvocationContext,
        request: &CanonicalModelRequest,
    ) -> Result<BoundedModelStream, ProviderStatusOutcome> {
        self.client.invoke_stream(context, request)
    }
}
