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
use std::time::Duration;

use rocketmq_sre_contracts::CorrelationId;
use tonic::Code;
use tonic::Request;
use tonic::transport::Certificate;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Endpoint;
use tonic::transport::Identity;

use crate::CanonicalModelRequest;
use crate::CanonicalModelResponse;
use crate::InvocationContext;
use crate::ModelStreamEvent;
use crate::ProviderCapabilities;
use crate::ProviderError;
use crate::ProviderFailure;
use crate::ProviderHealth;
use crate::ProviderOperationalFailure as OperationalFailure;
use crate::ProviderRejection;
use crate::ProviderStatusOutcome;
use crate::SpiHealth;
use crate::current_unix_ms;

/// Generated gRPC wire contract for process-external provider adapters.
///
/// Adapter processes implement the generated
/// `provider_adapter_server::ProviderAdapter`
/// and serve it with `tonic::transport::Server` configured with a server
/// identity and a client CA root.
pub mod wire {
    tonic::include_proto!("rocketmq.sre.provider.v1");
}

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
const MAX_IDENTITY_CHARS: usize = 512;

/// Client-side mTLS material and application identity for one SPI adapter.
///
/// PEM values are held in memory only. Debug output never includes them.
#[derive(Clone)]
pub struct GrpcSpiClientTlsConfig {
    ca_certificate_pem: Vec<u8>,
    client_certificate_pem: Vec<u8>,
    client_private_key_pem: Vec<u8>,
    server_domain_name: String,
    gateway_identity: String,
    expected_adapter_identity: String,
    connect_timeout: Duration,
    request_timeout: Duration,
    max_payload_bytes: usize,
}

impl GrpcSpiClientTlsConfig {
    /// Creates a fail-closed mutual-TLS configuration.
    #[must_use]
    pub fn mutual_tls(
        ca_certificate_pem: impl Into<Vec<u8>>,
        client_certificate_pem: impl Into<Vec<u8>>,
        client_private_key_pem: impl Into<Vec<u8>>,
        server_domain_name: impl Into<String>,
        gateway_identity: impl Into<String>,
        expected_adapter_identity: impl Into<String>,
    ) -> Self {
        Self {
            ca_certificate_pem: ca_certificate_pem.into(),
            client_certificate_pem: client_certificate_pem.into(),
            client_private_key_pem: client_private_key_pem.into(),
            server_domain_name: server_domain_name.into(),
            gateway_identity: gateway_identity.into(),
            expected_adapter_identity: expected_adapter_identity.into(),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
        }
    }

    /// Applies finite transport timeouts.
    #[must_use]
    pub const fn with_timeouts(mut self, connect_timeout: Duration, request_timeout: Duration) -> Self {
        self.connect_timeout = connect_timeout;
        self.request_timeout = request_timeout;
        self
    }

    /// Applies the canonical JSON payload limit.
    #[must_use]
    pub const fn with_max_payload_bytes(mut self, max_payload_bytes: usize) -> Self {
        self.max_payload_bytes = max_payload_bytes;
        self
    }

    fn validate(&self) -> Result<(), ProviderStatusOutcome> {
        if self.ca_certificate_pem.is_empty()
            || self.client_certificate_pem.is_empty()
            || self.client_private_key_pem.is_empty()
        {
            return Err(ProviderStatusOutcome::rejected(ProviderRejection::MutualTlsFailed));
        }
        if self.server_domain_name.trim().is_empty()
            || self.server_domain_name.chars().count() > 253
            || self.server_domain_name.chars().any(char::is_control)
            || !self.gateway_identity.starts_with("spiffe://")
            || !self.expected_adapter_identity.starts_with("spiffe://")
            || self.gateway_identity == self.expected_adapter_identity
            || self.gateway_identity.chars().count() > MAX_IDENTITY_CHARS
            || self.expected_adapter_identity.chars().count() > MAX_IDENTITY_CHARS
            || self.gateway_identity.chars().any(char::is_control)
            || self.expected_adapter_identity.chars().any(char::is_control)
        {
            return Err(ProviderStatusOutcome::rejected(ProviderRejection::MutualTlsFailed));
        }
        if self.connect_timeout.is_zero()
            || self.request_timeout.is_zero()
            || self.max_payload_bytes == 0
            || self.max_payload_bytes > MAX_PAYLOAD_BYTES
        {
            return Err(ProviderStatusOutcome::rejected(ProviderRejection::ProfileInvalid));
        }
        Ok(())
    }
}

impl Debug for GrpcSpiClientTlsConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GrpcSpiClientTlsConfig")
            .field("ca_certificate_pem", &"[TLS MATERIAL REDACTED]")
            .field("client_certificate_pem", &"[TLS MATERIAL REDACTED]")
            .field("client_private_key_pem", &"[TLS MATERIAL REDACTED]")
            .field("server_domain_name", &"[SERVER NAME REDACTED]")
            .field("gateway_identity", &"[WORKLOAD IDENTITY REDACTED]")
            .field("expected_adapter_identity", &"[WORKLOAD IDENTITY REDACTED]")
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("max_payload_bytes", &self.max_payload_bytes)
            .finish()
    }
}

/// Negotiated asynchronous gRPC client for one process-external provider.
#[derive(Clone)]
pub struct GrpcProviderSpiClient {
    client: wire::provider_adapter_client::ProviderAdapterClient<Channel>,
    gateway_identity: String,
    adapter_identity: String,
    capabilities: ProviderCapabilities,
    credential_version_fingerprint: Option<String>,
    request_timeout: Duration,
    max_payload_bytes: usize,
}

impl GrpcProviderSpiClient {
    /// Establishes mTLS and verifies wire version, SPIFFE application
    /// identities, adapter-owned credentials, and capability JSON.
    ///
    /// `endpoint` must use `https://` or the equivalent `grpcs://` spelling.
    ///
    /// # Errors
    ///
    /// Returns a stable redacted transport, mTLS, handshake, identity, version,
    /// credential-owner, or capability error.
    pub async fn connect(endpoint: &str, config: GrpcSpiClientTlsConfig) -> Result<Self, ProviderStatusOutcome> {
        config.validate()?;
        let _ = rustls::crypto::ring::default_provider().install_default();
        let endpoint = normalize_grpc_endpoint(endpoint)?;
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&config.ca_certificate_pem))
            .identity(Identity::from_pem(
                &config.client_certificate_pem,
                &config.client_private_key_pem,
            ))
            .domain_name(config.server_domain_name.clone());
        let channel = Endpoint::from_shared(endpoint)
            .map_err(|source| ProviderError::from_source(OperationalFailure::TransportFailed, source))?
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .tls_config(tls)
            .map_err(|source| ProviderError::from_source(OperationalFailure::TransportFailed, source))?
            .connect()
            .await
            .map_err(|source| ProviderError::from_source(OperationalFailure::TransportFailed, source))?;
        let mut client = wire::provider_adapter_client::ProviderAdapterClient::new(channel)
            .max_decoding_message_size(config.max_payload_bytes)
            .max_encoding_message_size(config.max_payload_bytes);
        let correlation_id = CorrelationId::new();
        let response = match client
            .handshake(wire::HandshakeRequest {
                wire_version: crate::PROVIDER_SPI_WIRE_VERSION.to_owned(),
                gateway_identity: config.gateway_identity.clone(),
                correlation_id: correlation_id.to_string(),
                max_payload_bytes: config.max_payload_bytes as u64,
            })
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status) => return Err(map_handshake_status(status)),
        };
        if response.wire_version != crate::PROVIDER_SPI_WIRE_VERSION {
            return Err(ProviderStatusOutcome::rejected(
                ProviderRejection::UnsupportedWireVersion,
            ));
        }
        if response.adapter_identity != config.expected_adapter_identity {
            return Err(ProviderStatusOutcome::rejected(ProviderRejection::MutualTlsFailed));
        }
        if response.credential_owner != "adapter" {
            return Err(ProviderStatusOutcome::rejected(ProviderRejection::AuthorizationFailed));
        }
        if response.adapter_identity.chars().count() > MAX_IDENTITY_CHARS
            || response.adapter_identity.chars().any(char::is_control)
        {
            return Err(ProviderError::new(
                OperationalFailure::ProtocolError,
                "provider SPI handshake metadata exceeded configured bounds",
            )
            .into());
        }
        let capabilities = decode_json(&response.capabilities_json, config.max_payload_bytes)?;
        let credential_version_fingerprint = bounded_fingerprint(response.credential_version_fingerprint)?;
        Ok(Self {
            client,
            gateway_identity: config.gateway_identity,
            adapter_identity: response.adapter_identity,
            capabilities,
            credential_version_fingerprint,
            request_timeout: config.request_timeout,
            max_payload_bytes: config.max_payload_bytes,
        })
    }

    /// Returns the authenticated application identity sent by the gateway.
    #[must_use]
    pub fn gateway_identity(&self) -> &str {
        &self.gateway_identity
    }

    /// Returns the negotiated adapter application identity.
    #[must_use]
    pub fn adapter_identity(&self) -> &str {
        &self.adapter_identity
    }

    /// Returns the negotiated adapter capability set.
    #[must_use]
    pub fn capabilities(&self) -> &ProviderCapabilities {
        &self.capabilities
    }

    /// Returns the adapter's non-secret credential version fingerprint.
    #[must_use]
    pub fn credential_version_fingerprint(&self) -> Option<&str> {
        self.credential_version_fingerprint.as_deref()
    }

    /// Invokes the external adapter with canonical JSON and no credential
    /// material.
    ///
    /// # Errors
    ///
    /// Returns stable deadline, cancellation, transport, adapter, payload, or
    /// canonical response errors.
    pub async fn invoke(
        &mut self,
        context: &InvocationContext,
        request: &CanonicalModelRequest,
    ) -> Result<CanonicalModelResponse, ProviderStatusOutcome> {
        context.ensure_active()?;
        ensure_request_correlation(context, request)?;
        let payload = encode_json(request, self.max_payload_bytes)?;
        let response = match self.client.invoke(self.invoke_request(context, payload)).await {
            Ok(response) => response.into_inner(),
            Err(status) => return Err(map_status(status)),
        };
        decode_invoke_response(response, context.max_response_bytes.min(self.max_payload_bytes))
    }

    /// Starts a bounded external provider stream.
    ///
    /// # Errors
    ///
    /// Returns stable deadline, cancellation, transport, adapter, or payload
    /// errors. Each event is decoded and bounded again by
    /// [`GrpcProviderSpiStream::message`].
    pub async fn invoke_stream(
        &mut self,
        context: &InvocationContext,
        request: &CanonicalModelRequest,
    ) -> Result<GrpcProviderSpiStream, ProviderStatusOutcome> {
        context.ensure_active()?;
        ensure_request_correlation(context, request)?;
        if context.stream_bounds.channel_capacity == 0
            || context.stream_bounds.max_events == 0
            || context.stream_bounds.max_bytes == 0
        {
            return Err(ProviderStatusOutcome::rejected(ProviderRejection::InvalidRequest));
        }
        let payload = encode_json(request, self.max_payload_bytes)?;
        let stream = match self.client.invoke_stream(self.invoke_request(context, payload)).await {
            Ok(response) => response.into_inner(),
            Err(status) => return Err(map_status(status)),
        };
        Ok(GrpcProviderSpiStream {
            inner: stream,
            events: 0,
            bytes: 0,
            max_events: context.stream_bounds.max_events,
            max_bytes: context.stream_bounds.max_bytes.min(self.max_payload_bytes),
        })
    }

    /// Cancels an adapter invocation by its opaque ID.
    ///
    /// # Errors
    ///
    /// Returns a stable transport or adapter error.
    pub async fn cancel(
        &mut self,
        invocation_id: impl Into<String>,
        correlation_id: CorrelationId,
    ) -> Result<(), ProviderStatusOutcome> {
        let invocation_id = invocation_id.into();
        if invocation_id.is_empty()
            || invocation_id.chars().count() > MAX_IDENTITY_CHARS
            || invocation_id.chars().any(char::is_control)
        {
            return Err(ProviderStatusOutcome::rejected(ProviderRejection::InvalidRequest));
        }
        match self
            .client
            .cancel(wire::CancelRequest {
                invocation_id,
                correlation_id: correlation_id.to_string(),
            })
            .await
        {
            Ok(_) => {}
            Err(status) => return Err(map_status(status)),
        }
        Ok(())
    }

    /// Queries redacted adapter health.
    ///
    /// # Errors
    ///
    /// Returns a stable transport, protocol, or adapter error.
    pub async fn health(&mut self) -> Result<SpiHealth, ProviderStatusOutcome> {
        let response = match self.client.health(wire::HealthRequest {}).await {
            Ok(response) => response.into_inner(),
            Err(status) => return Err(map_status(status)),
        };
        Ok(SpiHealth {
            status: parse_health(&response.status)?,
            credential_version_fingerprint: bounded_fingerprint(response.credential_version_fingerprint)?,
        })
    }

    fn invoke_request(&self, context: &InvocationContext, payload: Vec<u8>) -> Request<wire::InvokeRequest> {
        let mut request = Request::new(wire::InvokeRequest {
            invocation_id: format!("spi-{}", context.correlation_id),
            correlation_id: context.correlation_id.to_string(),
            deadline_unix_ms: context.deadline_unix_ms.unwrap_or_default(),
            canonical_request_json: payload,
            stream_channel_capacity: context.stream_bounds.channel_capacity as u64,
            max_stream_events: context.stream_bounds.max_events as u64,
            max_stream_bytes: context.stream_bounds.max_bytes as u64,
        });
        request.set_timeout(remaining_timeout(context.deadline_unix_ms, self.request_timeout));
        request
    }
}

impl Debug for GrpcProviderSpiClient {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GrpcProviderSpiClient")
            .field("gateway_identity", &"[WORKLOAD IDENTITY REDACTED]")
            .field("adapter_identity", &"[WORKLOAD IDENTITY REDACTED]")
            .field("capabilities", &self.capabilities)
            .field("credential_version_fingerprint", &self.credential_version_fingerprint)
            .field("request_timeout", &self.request_timeout)
            .field("max_payload_bytes", &self.max_payload_bytes)
            .finish_non_exhaustive()
    }
}

/// Bounded decoder over a gRPC server stream.
pub struct GrpcProviderSpiStream {
    inner: tonic::Streaming<wire::StreamEvent>,
    events: usize,
    bytes: usize,
    max_events: usize,
    max_bytes: usize,
}

impl GrpcProviderSpiStream {
    /// Receives and decodes one canonical event.
    ///
    /// # Errors
    ///
    /// Fails closed on gRPC status, adapter error, invalid JSON, or cumulative
    /// event/byte overflow.
    pub async fn message(&mut self) -> Result<Option<ModelStreamEvent>, ProviderStatusOutcome> {
        let event = match self.inner.message().await {
            Ok(event) => event,
            Err(status) => return Err(map_status(status)),
        };
        let Some(event) = event else {
            return Ok(None);
        };
        if let Some(error) = event.error {
            return Err(decode_wire_error(error));
        }
        self.events = self.events.saturating_add(1);
        self.bytes = self.bytes.saturating_add(event.canonical_event_json.len());
        if self.events > self.max_events || self.bytes > self.max_bytes {
            return Err(ProviderError::new(
                OperationalFailure::OutputTooLarge,
                "provider SPI stream exceeded configured bounds",
            )
            .into());
        }
        decode_json(&event.canonical_event_json, self.max_bytes)
            .map(Some)
            .map_err(Into::into)
    }
}

impl Debug for GrpcProviderSpiStream {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GrpcProviderSpiStream")
            .field("events", &self.events)
            .field("bytes", &self.bytes)
            .field("max_events", &self.max_events)
            .field("max_bytes", &self.max_bytes)
            .finish_non_exhaustive()
    }
}

/// Wraps an adapter implementation with the generated service and hard
/// message-size limits. The caller must configure `tonic::transport::Server`
/// with server identity and client CA root before serving it.
#[must_use]
pub fn bounded_provider_adapter_service<T>(
    adapter: T,
    max_payload_bytes: usize,
) -> wire::provider_adapter_server::ProviderAdapterServer<T>
where
    T: wire::provider_adapter_server::ProviderAdapter,
{
    let max_payload_bytes = max_payload_bytes.clamp(1, MAX_PAYLOAD_BYTES);
    wire::provider_adapter_server::ProviderAdapterServer::new(adapter)
        .max_decoding_message_size(max_payload_bytes)
        .max_encoding_message_size(max_payload_bytes)
}

fn normalize_grpc_endpoint(endpoint: &str) -> Result<String, ProviderStatusOutcome> {
    if let Some(rest) = endpoint.strip_prefix("grpcs://") {
        return Ok(format!("https://{rest}"));
    }
    if endpoint.starts_with("https://") {
        return Ok(endpoint.to_owned());
    }
    Err(ProviderStatusOutcome::rejected(ProviderRejection::MutualTlsFailed))
}

fn remaining_timeout(deadline_unix_ms: Option<u64>, configured: Duration) -> Duration {
    deadline_unix_ms.map_or(configured, |deadline| {
        Duration::from_millis(deadline.saturating_sub(current_unix_ms()).max(1)).min(configured)
    })
}

fn encode_json(value: &impl serde::Serialize, max_bytes: usize) -> Result<Vec<u8>, ProviderError> {
    let payload = serde_json::to_vec(value)
        .map_err(|source| ProviderError::from_source(OperationalFailure::ProtocolError, source))?;
    if payload.len() > max_bytes {
        return Err(ProviderError::new(
            OperationalFailure::OutputTooLarge,
            "provider SPI canonical payload exceeded configured bounds",
        ));
    }
    Ok(payload)
}

fn decode_json<T>(payload: &[u8], max_bytes: usize) -> Result<T, ProviderError>
where
    T: serde::de::DeserializeOwned,
{
    if payload.is_empty() || payload.len() > max_bytes {
        return Err(ProviderError::new(
            OperationalFailure::OutputTooLarge,
            "provider SPI canonical payload was empty or exceeded configured bounds",
        ));
    }
    serde_json::from_slice(payload)
        .map_err(|source| ProviderError::from_source(OperationalFailure::ProtocolError, source))
}

fn decode_invoke_response(
    response: wire::InvokeResponse,
    max_bytes: usize,
) -> Result<CanonicalModelResponse, ProviderStatusOutcome> {
    if let Some(error) = response.error {
        return Err(decode_wire_error(error));
    }
    decode_json(&response.canonical_response_json, max_bytes).map_err(Into::into)
}

fn decode_wire_error(error: wire::ProviderError) -> ProviderStatusOutcome {
    let code = parse_error_code(&error.code).unwrap_or(ProviderFailure::ProtocolError);
    match OperationalFailure::try_from(code) {
        Ok(failure) => ProviderError::from_remote(failure, error.retryable).into(),
        Err(rejection) => ProviderStatusOutcome::rejected(rejection),
    }
}

fn parse_error_code(value: &str) -> Option<ProviderFailure> {
    serde_json::from_value(serde_json::Value::String(value.to_owned())).ok()
}

fn parse_health(value: &str) -> Result<ProviderHealth, ProviderError> {
    match value {
        "unknown" => Ok(ProviderHealth::Unknown),
        "healthy" => Ok(ProviderHealth::Healthy),
        "degraded" => Ok(ProviderHealth::Degraded),
        "unavailable" => Ok(ProviderHealth::Unavailable),
        "quarantined" => Ok(ProviderHealth::Quarantined),
        _ => Err(ProviderError::new(
            OperationalFailure::ProtocolError,
            "provider SPI health status is invalid",
        )),
    }
}

fn map_status(status: tonic::Status) -> ProviderStatusOutcome {
    match status.code() {
        Code::Unauthenticated => ProviderStatusOutcome::rejected(ProviderRejection::AuthenticationFailed),
        Code::PermissionDenied => ProviderStatusOutcome::rejected(ProviderRejection::AuthorizationFailed),
        Code::Cancelled => ProviderStatusOutcome::rejected(ProviderRejection::Cancelled),
        Code::InvalidArgument | Code::FailedPrecondition | Code::OutOfRange => {
            ProviderStatusOutcome::rejected(ProviderRejection::InvalidRequest)
        }
        Code::DeadlineExceeded => ProviderError::from_source(OperationalFailure::Timeout, status).into(),
        Code::ResourceExhausted => ProviderError::from_source(OperationalFailure::RateLimited, status).into(),
        Code::Unavailable => ProviderError::from_source(OperationalFailure::ServiceUnavailable, status).into(),
        _ => ProviderError::from_source(OperationalFailure::TransportFailed, status).into(),
    }
}

fn map_handshake_status(status: tonic::Status) -> ProviderStatusOutcome {
    match status.code() {
        Code::Unknown | Code::Internal | Code::Unavailable => {
            ProviderError::from_source(OperationalFailure::TransportFailed, status).into()
        }
        _ => map_status(status),
    }
}

fn ensure_request_correlation(
    context: &InvocationContext,
    request: &CanonicalModelRequest,
) -> Result<(), ProviderStatusOutcome> {
    if context.correlation_id != request.correlation_id {
        return Err(ProviderStatusOutcome::rejected(ProviderRejection::InvalidRequest));
    }
    Ok(())
}

fn bounded_fingerprint(value: String) -> Result<Option<String>, ProviderError> {
    if value.chars().count() > MAX_IDENTITY_CHARS || value.chars().any(char::is_control) {
        return Err(ProviderError::new(
            OperationalFailure::ProtocolError,
            "provider SPI credential fingerprint exceeded configured bounds",
        ));
    }
    Ok((!value.is_empty()).then_some(value))
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    #[test]
    fn endpoint_and_tls_configuration_fail_closed() {
        assert_eq!(
            normalize_grpc_endpoint("grpc://adapter.internal")
                .expect_err("plaintext must fail")
                .failure(),
            ProviderFailure::MutualTlsFailed
        );
        assert_eq!(
            normalize_grpc_endpoint("grpcs://adapter.internal").expect("TLS endpoint"),
            "https://adapter.internal"
        );

        let config = GrpcSpiClientTlsConfig::mutual_tls(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            "adapter.internal",
            "spiffe://sre/gateway",
            "spiffe://sre/adapter",
        );
        assert_eq!(
            config.validate().expect_err("empty TLS material").failure(),
            ProviderFailure::MutualTlsFailed
        );

        let debug = format!("{config:?}");
        assert!(!debug.contains("adapter.internal"));
        assert!(!debug.contains("spiffe://sre/gateway"));
        assert!(!debug.contains("spiffe://sre/adapter"));
    }

    #[test]
    fn handshake_metadata_and_request_correlation_are_bounded() {
        assert_eq!(
            bounded_fingerprint("valid\nsecret".to_owned())
                .expect_err("control characters must fail")
                .failure(),
            ProviderFailure::ProtocolError
        );

        let context = InvocationContext::new(CorrelationId::new());
        let request = CanonicalModelRequest::new(CorrelationId::new(), "model", Vec::new());
        assert_eq!(
            ensure_request_correlation(&context, &request)
                .expect_err("different correlations must fail")
                .failure(),
            ProviderFailure::InvalidRequest
        );
    }

    #[test]
    fn wire_errors_do_not_expose_adapter_messages() {
        let outcome = decode_wire_error(wire::ProviderError {
            code: "rate_limited".to_owned(),
            message: "secret provider detail".to_owned(),
            retryable: true,
        });
        assert_eq!(outcome.failure(), ProviderFailure::RateLimited);
        assert!(outcome.operational_error().is_some());
        assert!(!outcome.message().contains("secret"));
    }

    #[test]
    fn grpc_request_refusals_are_closed_and_operational_statuses_keep_sources() {
        for (status, rejection) in [
            (
                tonic::Status::unauthenticated("credential detail"),
                ProviderRejection::AuthenticationFailed,
            ),
            (
                tonic::Status::permission_denied("policy detail"),
                ProviderRejection::AuthorizationFailed,
            ),
            (
                tonic::Status::invalid_argument("payload detail"),
                ProviderRejection::InvalidRequest,
            ),
        ] {
            let outcome = map_status(status);
            assert_eq!(outcome.rejection(), Some(rejection));
            assert!(outcome.operational_error().is_none());
            assert!(!format!("{outcome:?} {outcome}").contains("detail"));
        }

        let outcome = map_status(tonic::Status::deadline_exceeded("upstream detail"));
        let error = outcome.operational_error().expect("deadline failures are operational");
        assert_eq!(error.failure(), ProviderFailure::Timeout);
        assert!(
            error
                .source()
                .and_then(|source| source.downcast_ref::<tonic::Status>())
                .is_some()
        );
        assert!(!format!("{outcome:?} {outcome}").contains("upstream detail"));
    }
}
