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
use std::net::IpAddr;
use std::time::Duration;

use chrono::Utc;
use reqwest::Certificate;
use reqwest::Client;
use reqwest::Identity;
use reqwest::RequestBuilder;
use reqwest::header::ACCEPT;
use reqwest::header::CONTENT_TYPE;
use reqwest::header::HeaderValue;
use reqwest::redirect::Policy;
use serde_json::Value;
use url::Host;
use url::Url;

use crate::aws_sigv4::sign_bedrock_request;
use crate::error::ProviderError;
use crate::error::ProviderErrorCode;
use crate::profile::ProviderDialect;
use crate::secret::SecretMaterial;
use crate::secret::current_unix_ms;
use crate::transport::AsyncModelTransport;
use crate::transport::TransportFuture;
use crate::transport::TransportRequest;
use crate::transport::TransportResponse;

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const DEFAULT_MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

/// PEM client certificate chain and private key for optional mutual TLS.
///
/// Debug output never exposes the certificate or private-key bytes.
pub struct TlsClientIdentity {
    combined_pem: Vec<u8>,
}

impl TlsClientIdentity {
    /// Combines a PEM certificate chain and private key for rustls.
    ///
    /// # Errors
    ///
    /// Returns a profile error for empty input. Certificate parsing is
    /// completed when [`HttpModelTransport`] builds the TLS client.
    pub fn from_pem(certificate_chain_pem: &[u8], private_key_pem: &[u8]) -> Result<Self, ProviderError> {
        if certificate_chain_pem.is_empty() || private_key_pem.is_empty() {
            return Err(ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "TLS client identity requires a certificate chain and private key",
            ));
        }
        let mut combined_pem = Vec::with_capacity(certificate_chain_pem.len() + private_key_pem.len() + 1);
        combined_pem.extend_from_slice(certificate_chain_pem);
        if !certificate_chain_pem.ends_with(b"\n") {
            combined_pem.push(b'\n');
        }
        combined_pem.extend_from_slice(private_key_pem);
        Ok(Self { combined_pem })
    }
}

impl Debug for TlsClientIdentity {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TlsClientIdentity([CLIENT IDENTITY REDACTED])")
    }
}

/// TLS trust and optional mTLS configuration for the provider HTTP client.
#[derive(Default)]
pub struct HttpTlsConfig {
    root_certificate_pem_bundles: Vec<Vec<u8>>,
    only_custom_roots: bool,
    client_identity: Option<TlsClientIdentity>,
}

impl HttpTlsConfig {
    /// Adds one PEM root-certificate bundle.
    #[must_use]
    pub fn with_root_certificate_pem(mut self, pem_bundle: impl Into<Vec<u8>>) -> Self {
        self.root_certificate_pem_bundles.push(pem_bundle.into());
        self
    }

    /// Replaces platform roots with only the configured PEM roots.
    #[must_use]
    pub const fn with_only_custom_roots(mut self, enabled: bool) -> Self {
        self.only_custom_roots = enabled;
        self
    }

    /// Configures an optional client certificate and private key.
    #[must_use]
    pub fn with_client_identity(mut self, identity: TlsClientIdentity) -> Self {
        self.client_identity = Some(identity);
        self
    }
}

impl Debug for HttpTlsConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpTlsConfig")
            .field(
                "root_certificate_bundle_count",
                &self.root_certificate_pem_bundles.len(),
            )
            .field("only_custom_roots", &self.only_custom_roots)
            .field(
                "client_identity",
                &self.client_identity.as_ref().map(|_| "[CLIENT IDENTITY REDACTED]"),
            )
            .finish()
    }
}

/// Hard bounds and endpoint policy for [`HttpModelTransport`].
pub struct HttpTransportConfig {
    connect_timeout: Duration,
    request_timeout: Duration,
    pool_idle_timeout: Duration,
    max_request_bytes: usize,
    max_response_bytes: usize,
    allow_loopback_http: bool,
    allow_insecure_non_loopback_http: bool,
    use_system_proxy: bool,
    tls: HttpTlsConfig,
}

impl Default for HttpTransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            pool_idle_timeout: DEFAULT_POOL_IDLE_TIMEOUT,
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            allow_loopback_http: true,
            allow_insecure_non_loopback_http: false,
            use_system_proxy: false,
            tls: HttpTlsConfig::default(),
        }
    }
}

impl HttpTransportConfig {
    /// Sets finite connect and whole-request timeouts.
    #[must_use]
    pub const fn with_timeouts(mut self, connect_timeout: Duration, request_timeout: Duration) -> Self {
        self.connect_timeout = connect_timeout;
        self.request_timeout = request_timeout;
        self
    }

    /// Sets hard serialized request and received response bounds.
    #[must_use]
    pub const fn with_body_limits(mut self, max_request_bytes: usize, max_response_bytes: usize) -> Self {
        self.max_request_bytes = max_request_bytes;
        self.max_response_bytes = max_response_bytes;
        self
    }

    /// Controls plaintext loopback HTTP, which is enabled only for local
    /// development and deterministic mock tests by default.
    #[must_use]
    pub const fn with_loopback_http(mut self, enabled: bool) -> Self {
        self.allow_loopback_http = enabled;
        self
    }

    /// Explicitly opts into plaintext non-loopback HTTP.
    ///
    /// Production profiles should terminate TLS instead. This setting exists
    /// only for isolated private development networks and defaults to false.
    #[must_use]
    pub const fn with_insecure_non_loopback_http(mut self, enabled: bool) -> Self {
        self.allow_insecure_non_loopback_http = enabled;
        self
    }

    /// Allows ambient system proxy configuration.
    ///
    /// This defaults to false so model credentials cannot be silently routed
    /// through an inherited proxy.
    #[must_use]
    pub const fn with_system_proxy(mut self, enabled: bool) -> Self {
        self.use_system_proxy = enabled;
        self
    }

    /// Applies TLS trust and optional mTLS identity configuration.
    #[must_use]
    pub fn with_tls(mut self, tls: HttpTlsConfig) -> Self {
        self.tls = tls;
        self
    }

    fn validate(&self) -> Result<(), ProviderError> {
        if self.connect_timeout.is_zero() || self.request_timeout.is_zero() {
            return Err(ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "model HTTP timeouts must be non-zero",
            ));
        }
        if self.max_request_bytes == 0 || self.max_response_bytes == 0 {
            return Err(ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "model HTTP body limits must be non-zero",
            ));
        }
        if self.tls.only_custom_roots && self.tls.root_certificate_pem_bundles.is_empty() {
            return Err(ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "custom-only TLS trust requires at least one root certificate",
            ));
        }
        Ok(())
    }
}

impl Debug for HttpTransportConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpTransportConfig")
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("pool_idle_timeout", &self.pool_idle_timeout)
            .field("max_request_bytes", &self.max_request_bytes)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("allow_loopback_http", &self.allow_loopback_http)
            .field(
                "allow_insecure_non_loopback_http",
                &self.allow_insecure_non_loopback_http,
            )
            .field("use_system_proxy", &self.use_system_proxy)
            .field("tls", &self.tls)
            .finish()
    }
}

/// Reusable rustls-backed, bounded asynchronous provider transport.
pub struct HttpModelTransport {
    client: Client,
    request_timeout: Duration,
    max_request_bytes: usize,
    max_response_bytes: usize,
    allow_loopback_http: bool,
    allow_insecure_non_loopback_http: bool,
}

impl HttpModelTransport {
    /// Builds a redirect-free HTTP client with explicit trust and resource
    /// bounds.
    ///
    /// # Errors
    ///
    /// Returns a redacted profile error for invalid bounds, certificates,
    /// client identity, or HTTP client configuration.
    pub fn new(config: HttpTransportConfig) -> Result<Self, ProviderError> {
        config.validate()?;
        let mut builder = Client::builder()
            .redirect(Policy::none())
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .pool_idle_timeout(config.pool_idle_timeout)
            .user_agent(concat!("rocketmq-sre-model-gateway/", env!("CARGO_PKG_VERSION")));
        if !config.use_system_proxy {
            builder = builder.no_proxy();
        }

        let mut certificates = Vec::new();
        for pem_bundle in &config.tls.root_certificate_pem_bundles {
            let parsed = Certificate::from_pem_bundle(pem_bundle).map_err(|_| {
                ProviderError::new(
                    ProviderErrorCode::ProfileInvalid,
                    "model TLS root certificate bundle is invalid",
                )
            })?;
            if parsed.is_empty() {
                return Err(ProviderError::new(
                    ProviderErrorCode::ProfileInvalid,
                    "model TLS root certificate bundle is empty",
                ));
            }
            certificates.extend(parsed);
        }
        if config.tls.only_custom_roots {
            builder = builder.tls_certs_only(certificates);
        } else {
            for certificate in certificates {
                builder = builder.add_root_certificate(certificate);
            }
        }
        if let Some(identity) = config.tls.client_identity {
            let identity = Identity::from_pem(&identity.combined_pem).map_err(|_| {
                ProviderError::new(
                    ProviderErrorCode::ProfileInvalid,
                    "model TLS client identity is invalid",
                )
            })?;
            builder = builder.identity(identity);
        }
        let client = builder.build().map_err(|_| {
            ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "model HTTP client configuration is invalid",
            )
        })?;
        Ok(Self {
            client,
            request_timeout: config.request_timeout,
            max_request_bytes: config.max_request_bytes,
            max_response_bytes: config.max_response_bytes,
            allow_loopback_http: config.allow_loopback_http,
            allow_insecure_non_loopback_http: config.allow_insecure_non_loopback_http,
        })
    }

    async fn invoke_http(&self, request: TransportRequest) -> Result<TransportResponse, ProviderError> {
        let timeout = effective_timeout(request.deadline_unix_ms, self.request_timeout)?;
        let url = self.provider_url(&request.endpoint, &request.path)?;
        let body = serde_json::to_vec(&request.body).map_err(|_| {
            ProviderError::new(
                ProviderErrorCode::InvalidRequest,
                "model request JSON could not be encoded",
            )
        })?;
        if body.len() > self.max_request_bytes {
            return Err(ProviderError::new(
                ProviderErrorCode::OutputTooLarge,
                "model request exceeded the configured transport bound",
            ));
        }
        let response_bound = request.max_response_bytes.min(self.max_response_bytes);
        if response_bound == 0 {
            return Err(ProviderError::new(
                ProviderErrorCode::InvalidRequest,
                "model response bound must be non-zero",
            ));
        }

        validate_credential_expiry(request.credential.as_ref())?;
        let mut builder = self
            .client
            .post(url.clone())
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .header("x-rocketmq-correlation-id", request.correlation_id.to_string())
            .body(body.clone())
            .timeout(timeout);
        builder = apply_provider_auth(builder, &url, request.dialect, request.credential.as_ref(), &body)?;

        match tokio::time::timeout(timeout, receive_bounded_json(builder, response_bound)).await {
            Ok(result) => result,
            Err(_) => Err(ProviderError::timeout("model provider request exceeded its deadline")),
        }
    }

    fn provider_url(&self, endpoint: &str, path: &str) -> Result<Url, ProviderError> {
        let base = Url::parse(endpoint).map_err(|_| invalid_endpoint())?;
        if !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || base.host().is_none()
        {
            return Err(invalid_endpoint());
        }
        match base.scheme() {
            "https" => {}
            "http" if is_loopback(&base) && self.allow_loopback_http => {}
            "http" if self.allow_insecure_non_loopback_http => {}
            "http" => {
                return Err(ProviderError::policy_denied(
                    "plaintext non-loopback model endpoints are disabled",
                ));
            }
            _ => return Err(invalid_endpoint()),
        }
        if !path.starts_with('/') || path.starts_with("//") || path.contains('#') || path.chars().any(char::is_control)
        {
            return Err(ProviderError::new(
                ProviderErrorCode::InvalidRequest,
                "model provider path is invalid",
            ));
        }

        let full =
            Url::parse(&format!("{}{}", endpoint.trim_end_matches('/'), path)).map_err(|_| invalid_endpoint())?;
        if base.scheme() != full.scheme()
            || base.host() != full.host()
            || base.port_or_known_default() != full.port_or_known_default()
            || !full.username().is_empty()
            || full.password().is_some()
            || full.fragment().is_some()
        {
            return Err(ProviderError::policy_denied(
                "model provider path attempted to change endpoint authority",
            ));
        }
        let base_path = base.path().trim_end_matches('/');
        if !base_path.is_empty()
            && base_path != "/"
            && full.path() != base_path
            && !full.path().starts_with(&format!("{base_path}/"))
        {
            return Err(ProviderError::policy_denied(
                "model provider path escaped its configured base path",
            ));
        }
        Ok(full)
    }
}

impl Debug for HttpModelTransport {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpModelTransport")
            .field("request_timeout", &self.request_timeout)
            .field("max_request_bytes", &self.max_request_bytes)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("allow_loopback_http", &self.allow_loopback_http)
            .field(
                "allow_insecure_non_loopback_http",
                &self.allow_insecure_non_loopback_http,
            )
            .finish_non_exhaustive()
    }
}

impl AsyncModelTransport for HttpModelTransport {
    fn invoke(&self, request: TransportRequest) -> TransportFuture<'_> {
        Box::pin(async move { self.invoke_http(request).await })
    }
}

async fn receive_bounded_json(
    builder: RequestBuilder,
    max_response_bytes: usize,
) -> Result<TransportResponse, ProviderError> {
    let mut response = builder.send().await.map_err(map_reqwest_error)?;
    let status = response.status();
    if status.is_redirection() {
        return Err(
            ProviderError::policy_denied("model provider redirects are disabled").with_provider_status(status.as_u16())
        );
    }
    if response
        .content_length()
        .is_some_and(|length| length > max_response_bytes as u64)
    {
        return Err(ProviderError::new(
            ProviderErrorCode::OutputTooLarge,
            "model provider response exceeded the configured bound",
        )
        .with_provider_status(status.as_u16()));
    }

    let mut body = Vec::with_capacity(
        response
            .content_length()
            .map_or(0, |length| usize::try_from(length).unwrap_or(max_response_bytes))
            .min(max_response_bytes),
    );
    while let Some(chunk) = response.chunk().await.map_err(map_reqwest_error)? {
        if chunk.len() > max_response_bytes.saturating_sub(body.len()) {
            return Err(ProviderError::new(
                ProviderErrorCode::OutputTooLarge,
                "model provider response exceeded the configured bound",
            )
            .with_provider_status(status.as_u16()));
        }
        body.extend_from_slice(&chunk);
    }
    let body: Value = serde_json::from_slice(&body).map_err(|_| {
        ProviderError::new(ProviderErrorCode::ProtocolError, "model provider returned invalid JSON")
            .with_provider_status(status.as_u16())
    })?;
    Ok(TransportResponse {
        status: status.as_u16(),
        body,
    })
}

fn apply_provider_auth(
    builder: RequestBuilder,
    url: &Url,
    dialect: ProviderDialect,
    credential: Option<&SecretMaterial>,
    body: &[u8],
) -> Result<RequestBuilder, ProviderError> {
    let required = credential_required(dialect);
    let credential = match credential {
        Some(credential) => Some(credential),
        None if required => {
            return Err(ProviderError::new(
                ProviderErrorCode::AuthenticationFailed,
                "model provider credential is unavailable",
            ));
        }
        None => None,
    };
    let Some(credential) = credential else {
        return Ok(builder);
    };
    let secret = credential.expose_to_transport();
    match dialect {
        ProviderDialect::AzureOpenAi => Ok(builder.header("api-key", secret_header(secret)?)),
        ProviderDialect::Anthropic | ProviderDialect::DeepSeekAnthropic => Ok(builder
            .header("x-api-key", secret_header(secret)?)
            .header("anthropic-version", "2023-06-01")),
        ProviderDialect::Gemini => Ok(builder.header("x-goog-api-key", secret_header(secret)?)),
        ProviderDialect::Bedrock => sign_bedrock_request(builder, url, body, credential, Utc::now()),
        ProviderDialect::OpenAi
        | ProviderDialect::DeepSeekOpenAi
        | ProviderDialect::ZhipuGlm
        | ProviderDialect::Kimi
        | ProviderDialect::Vllm
        | ProviderDialect::Ollama
        | ProviderDialect::LlamaCpp
        | ProviderDialect::Sglang
        | ProviderDialect::EnterpriseProxy => {
            let mut value = secret_header(&format!("Bearer {secret}"))?;
            value.set_sensitive(true);
            Ok(builder.header("authorization", value))
        }
        ProviderDialect::ProprietarySpi => Err(ProviderError::capability_unsupported(
            "provider SPI does not use the built-in HTTP transport",
        )),
    }
}

fn credential_required(dialect: ProviderDialect) -> bool {
    matches!(
        dialect,
        ProviderDialect::OpenAi
            | ProviderDialect::AzureOpenAi
            | ProviderDialect::Anthropic
            | ProviderDialect::Gemini
            | ProviderDialect::Bedrock
            | ProviderDialect::DeepSeekOpenAi
            | ProviderDialect::DeepSeekAnthropic
            | ProviderDialect::ZhipuGlm
            | ProviderDialect::Kimi
            | ProviderDialect::EnterpriseProxy
    )
}

fn validate_credential_expiry(credential: Option<&SecretMaterial>) -> Result<(), ProviderError> {
    if credential
        .and_then(SecretMaterial::expires_at_unix_ms)
        .is_some_and(|expires_at| expires_at <= current_unix_ms())
    {
        return Err(ProviderError::new(
            ProviderErrorCode::SecretUnavailable,
            "model provider credential has expired",
        ));
    }
    Ok(())
}

fn effective_timeout(deadline_unix_ms: Option<u64>, configured: Duration) -> Result<Duration, ProviderError> {
    let Some(deadline) = deadline_unix_ms else {
        return Ok(configured);
    };
    let now = current_unix_ms();
    if deadline <= now {
        return Err(ProviderError::timeout("model invocation deadline has expired"));
    }
    Ok(configured.min(Duration::from_millis(deadline - now)))
}

fn is_loopback(url: &Url) -> bool {
    match url.host() {
        Some(Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(address)) => IpAddr::V4(address).is_loopback(),
        Some(Host::Ipv6(address)) => IpAddr::V6(address).is_loopback(),
        None => false,
    }
}

fn secret_header(value: &str) -> Result<HeaderValue, ProviderError> {
    let mut header = HeaderValue::from_str(value).map_err(|_| {
        ProviderError::new(
            ProviderErrorCode::AuthenticationFailed,
            "provider credential contains invalid header material",
        )
    })?;
    header.set_sensitive(true);
    Ok(header)
}

fn invalid_endpoint() -> ProviderError {
    ProviderError::new(ProviderErrorCode::ProfileInvalid, "model provider endpoint is invalid")
}

fn map_reqwest_error(error: reqwest::Error) -> ProviderError {
    if error.is_timeout() {
        ProviderError::timeout("model provider request timed out")
    } else if error.is_connect() {
        ProviderError::service_unavailable("model provider connection failed")
    } else {
        ProviderError::new(ProviderErrorCode::TransportFailed, "model provider transport failed")
    }
}
