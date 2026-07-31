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
use crate::ProviderErrorCode;
use crate::ProviderHealth;
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

    fn validate(&self) -> Result<(), ProviderError> {
        if self.ca_certificate_pem.is_empty()
            || self.client_certificate_pem.is_empty()
            || self.client_private_key_pem.is_empty()
        {
            return Err(ProviderError::new(
                ProviderErrorCode::MutualTlsFailed,
                "provider SPI requires CA, client certificate, and client key material",
            ));
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
            return Err(ProviderError::new(
                ProviderErrorCode::MutualTlsFailed,
                "provider SPI mutual-TLS identities are invalid",
            ));
        }
        if self.connect_timeout.is_zero()
            || self.request_timeout.is_zero()
            || self.max_payload_bytes == 0
            || self.max_payload_bytes > MAX_PAYLOAD_BYTES
        {
            return Err(ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "provider SPI timeout and payload bounds must be non-zero",
            ));
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
    pub async fn connect(endpoint: &str, config: GrpcSpiClientTlsConfig) -> Result<Self, ProviderError> {
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
            .map_err(|_| mutual_tls_error())?
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .tls_config(tls)
            .map_err(|_| mutual_tls_error())?
            .connect()
            .await
            .map_err(|_| mutual_tls_error())?;
        let mut client = wire::provider_adapter_client::ProviderAdapterClient::new(channel)
            .max_decoding_message_size(config.max_payload_bytes)
            .max_encoding_message_size(config.max_payload_bytes);
        let correlation_id = CorrelationId::new();
        let response = client
            .handshake(wire::HandshakeRequest {
                wire_version: crate::PROVIDER_SPI_WIRE_VERSION.to_owned(),
                gateway_identity: config.gateway_identity.clone(),
                correlation_id: correlation_id.to_string(),
                max_payload_bytes: config.max_payload_bytes as u64,
            })
            .await
            .map_err(map_handshake_status)?
            .into_inner();
        if response.wire_version != crate::PROVIDER_SPI_WIRE_VERSION {
            return Err(ProviderError::new(
                ProviderErrorCode::UnsupportedWireVersion,
                "provider SPI wire version is incompatible",
            ));
        }
        if response.adapter_identity != config.expected_adapter_identity {
            return Err(ProviderError::new(
                ProviderErrorCode::MutualTlsFailed,
                "provider SPI adapter identity did not match the trusted identity",
            ));
        }
        if response.credential_owner != "adapter" {
            return Err(ProviderError::new(
                ProviderErrorCode::AuthorizationFailed,
                "provider SPI adapter must own its model credential",
            ));
        }
        if response.adapter_identity.chars().count() > MAX_IDENTITY_CHARS
            || response.adapter_identity.chars().any(char::is_control)
        {
            return Err(ProviderError::new(
                ProviderErrorCode::ProtocolError,
                "provider SPI handshake metadata exceeded configured bounds",
            ));
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
    ) -> Result<CanonicalModelResponse, ProviderError> {
        context.ensure_active()?;
        ensure_request_correlation(context, request)?;
        let payload = encode_json(request, self.max_payload_bytes)?;
        let response = self
            .client
            .invoke(self.invoke_request(context, payload))
            .await
            .map_err(map_status)?
            .into_inner();
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
    ) -> Result<GrpcProviderSpiStream, ProviderError> {
        context.ensure_active()?;
        ensure_request_correlation(context, request)?;
        if context.stream_bounds.channel_capacity == 0
            || context.stream_bounds.max_events == 0
            || context.stream_bounds.max_bytes == 0
        {
            return Err(ProviderError::new(
                ProviderErrorCode::InvalidRequest,
                "provider SPI stream bounds must be non-zero",
            ));
        }
        let payload = encode_json(request, self.max_payload_bytes)?;
        let stream = self
            .client
            .invoke_stream(self.invoke_request(context, payload))
            .await
            .map_err(map_status)?
            .into_inner();
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
    ) -> Result<(), ProviderError> {
        let invocation_id = invocation_id.into();
        if invocation_id.is_empty()
            || invocation_id.chars().count() > MAX_IDENTITY_CHARS
            || invocation_id.chars().any(char::is_control)
        {
            return Err(ProviderError::new(
                ProviderErrorCode::InvalidRequest,
                "provider SPI cancellation identifier is invalid",
            ));
        }
        self.client
            .cancel(wire::CancelRequest {
                invocation_id,
                correlation_id: correlation_id.to_string(),
            })
            .await
            .map_err(map_status)?;
        Ok(())
    }

    /// Queries redacted adapter health.
    ///
    /// # Errors
    ///
    /// Returns a stable transport, protocol, or adapter error.
    pub async fn health(&mut self) -> Result<SpiHealth, ProviderError> {
        let response = self
            .client
            .health(wire::HealthRequest {})
            .await
            .map_err(map_status)?
            .into_inner();
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
    pub async fn message(&mut self) -> Result<Option<ModelStreamEvent>, ProviderError> {
        let Some(event) = self.inner.message().await.map_err(map_status)? else {
            return Ok(None);
        };
        if let Some(error) = event.error {
            return Err(decode_wire_error(error));
        }
        self.events = self.events.saturating_add(1);
        self.bytes = self.bytes.saturating_add(event.canonical_event_json.len());
        if self.events > self.max_events || self.bytes > self.max_bytes {
            return Err(ProviderError::new(
                ProviderErrorCode::OutputTooLarge,
                "provider SPI stream exceeded configured bounds",
            ));
        }
        decode_json(&event.canonical_event_json, self.max_bytes).map(Some)
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

fn normalize_grpc_endpoint(endpoint: &str) -> Result<String, ProviderError> {
    if let Some(rest) = endpoint.strip_prefix("grpcs://") {
        return Ok(format!("https://{rest}"));
    }
    if endpoint.starts_with("https://") {
        return Ok(endpoint.to_owned());
    }
    Err(ProviderError::new(
        ProviderErrorCode::MutualTlsFailed,
        "provider SPI endpoint must use authenticated TLS",
    ))
}

fn remaining_timeout(deadline_unix_ms: Option<u64>, configured: Duration) -> Duration {
    deadline_unix_ms.map_or(configured, |deadline| {
        Duration::from_millis(deadline.saturating_sub(current_unix_ms()).max(1)).min(configured)
    })
}

fn encode_json(value: &impl serde::Serialize, max_bytes: usize) -> Result<Vec<u8>, ProviderError> {
    let payload = serde_json::to_vec(value).map_err(|_| {
        ProviderError::new(
            ProviderErrorCode::ProtocolError,
            "provider SPI canonical payload could not be encoded",
        )
    })?;
    if payload.len() > max_bytes {
        return Err(ProviderError::new(
            ProviderErrorCode::OutputTooLarge,
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
            ProviderErrorCode::OutputTooLarge,
            "provider SPI canonical payload was empty or exceeded configured bounds",
        ));
    }
    serde_json::from_slice(payload).map_err(|_| {
        ProviderError::new(
            ProviderErrorCode::ProtocolError,
            "provider SPI canonical payload could not be decoded",
        )
    })
}

fn decode_invoke_response(
    response: wire::InvokeResponse,
    max_bytes: usize,
) -> Result<CanonicalModelResponse, ProviderError> {
    if let Some(error) = response.error {
        return Err(decode_wire_error(error));
    }
    decode_json(&response.canonical_response_json, max_bytes)
}

fn decode_wire_error(error: wire::ProviderError) -> ProviderError {
    let code = parse_error_code(&error.code).unwrap_or(ProviderErrorCode::ProtocolError);
    ProviderError {
        code,
        message: stable_error_message(code).to_owned(),
        retryable: code.retryable() && error.retryable,
        provider_status: None,
    }
}

fn parse_error_code(value: &str) -> Option<ProviderErrorCode> {
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
            ProviderErrorCode::ProtocolError,
            "provider SPI health status is invalid",
        )),
    }
}

fn map_status(status: tonic::Status) -> ProviderError {
    let code = match status.code() {
        Code::Unauthenticated => ProviderErrorCode::AuthenticationFailed,
        Code::PermissionDenied => ProviderErrorCode::AuthorizationFailed,
        Code::DeadlineExceeded => ProviderErrorCode::Timeout,
        Code::ResourceExhausted => ProviderErrorCode::RateLimited,
        Code::Unavailable => ProviderErrorCode::ServiceUnavailable,
        Code::Cancelled => ProviderErrorCode::Cancelled,
        Code::InvalidArgument | Code::FailedPrecondition | Code::OutOfRange => ProviderErrorCode::InvalidRequest,
        _ => ProviderErrorCode::TransportFailed,
    };
    ProviderError::new(code, stable_error_message(code))
}

fn map_handshake_status(status: tonic::Status) -> ProviderError {
    match status.code() {
        Code::Unknown | Code::Internal | Code::Unavailable => mutual_tls_error(),
        _ => map_status(status),
    }
}

const fn stable_error_message(code: ProviderErrorCode) -> &'static str {
    match code {
        ProviderErrorCode::AuthenticationFailed => "provider SPI authentication failed",
        ProviderErrorCode::AuthorizationFailed => "provider SPI authorization failed",
        ProviderErrorCode::Timeout => "provider SPI request timed out",
        ProviderErrorCode::RateLimited => "provider SPI rate limit exceeded",
        ProviderErrorCode::ServiceUnavailable => "provider SPI service unavailable",
        ProviderErrorCode::Cancelled => "provider SPI request was cancelled",
        ProviderErrorCode::InvalidRequest => "provider SPI rejected the request",
        ProviderErrorCode::ProtocolError => "provider SPI protocol response was invalid",
        _ => "provider SPI transport failed",
    }
}

fn mutual_tls_error() -> ProviderError {
    ProviderError::new(
        ProviderErrorCode::MutualTlsFailed,
        "provider SPI mutual-TLS connection failed",
    )
}

fn ensure_request_correlation(
    context: &InvocationContext,
    request: &CanonicalModelRequest,
) -> Result<(), ProviderError> {
    if context.correlation_id != request.correlation_id {
        return Err(ProviderError::new(
            ProviderErrorCode::InvalidRequest,
            "provider SPI request correlation did not match the invocation context",
        ));
    }
    Ok(())
}

fn bounded_fingerprint(value: String) -> Result<Option<String>, ProviderError> {
    if value.chars().count() > MAX_IDENTITY_CHARS || value.chars().any(char::is_control) {
        return Err(ProviderError::new(
            ProviderErrorCode::ProtocolError,
            "provider SPI credential fingerprint exceeded configured bounds",
        ));
    }
    Ok((!value.is_empty()).then_some(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_and_tls_configuration_fail_closed() {
        assert_eq!(
            normalize_grpc_endpoint("grpc://adapter.internal")
                .expect_err("plaintext must fail")
                .code,
            ProviderErrorCode::MutualTlsFailed
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
            config.validate().expect_err("empty TLS material").code,
            ProviderErrorCode::MutualTlsFailed
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
                .code,
            ProviderErrorCode::ProtocolError
        );

        let context = InvocationContext::new(CorrelationId::new());
        let request = CanonicalModelRequest::new(CorrelationId::new(), "model", Vec::new());
        assert_eq!(
            ensure_request_correlation(&context, &request)
                .expect_err("different correlations must fail")
                .code,
            ProviderErrorCode::InvalidRequest
        );
    }

    #[test]
    fn wire_errors_do_not_expose_adapter_messages() {
        let error = decode_wire_error(wire::ProviderError {
            code: "rate_limited".to_owned(),
            message: "secret provider detail".to_owned(),
            retryable: true,
        });
        assert_eq!(error.code, ProviderErrorCode::RateLimited);
        assert!(!error.message.contains("secret"));
    }
}
