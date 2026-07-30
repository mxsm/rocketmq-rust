// Copyright 2026 The RocketMQ Rust Authors
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

//! Authenticated, bounded transport for the sanitized runtime diagnostics view.
//!
//! This endpoint is deliberately separate from the anonymous lifecycle probe.
//! It is disabled unless both a bind address and a mounted token-file reference
//! are configured. Plain HTTP on a non-loopback listener additionally requires
//! an explicit development-only opt-in.

use std::env;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeComponent;
use rocketmq_runtime::RuntimeDiagnosticsViewV1;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use crate::metrics::runtime::RuntimeLifecycleReason;
use crate::metrics::runtime::RuntimeLifecycleState;
use crate::ObservabilityError;

pub const RUNTIME_DIAGNOSTICS_BIND_ADDR_ENV: &str = "ROCKETMQ_RUNTIME_DIAGNOSTICS_BIND_ADDR";
pub const RUNTIME_DIAGNOSTICS_TOKEN_FILE_ENV: &str = "ROCKETMQ_RUNTIME_DIAGNOSTICS_TOKEN_FILE";
pub const RUNTIME_DIAGNOSTICS_ALLOW_INSECURE_HTTP_ENV: &str = "ROCKETMQ_RUNTIME_DIAGNOSTICS_ALLOW_INSECURE_HTTP";
pub const RUNTIME_DIAGNOSTICS_SAMPLE_INTERVAL_SECONDS_ENV: &str =
    "ROCKETMQ_RUNTIME_DIAGNOSTICS_SAMPLE_INTERVAL_SECONDS";
pub const RUNTIME_DIAGNOSTICS_SCOPE: &str = "rocketmq:diagnose";
pub const RUNTIME_DIAGNOSTICS_PATH: &str = "/internal/v1/runtime/diagnostics";
pub const RUNTIME_DIAGNOSTICS_ENDPOINT_SCHEMA: &str = "rocketmq.runtime-diagnostics-endpoint.v1";

const DEFAULT_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_REQUEST_BYTES: usize = 8 * 1024;
const MAX_TOKEN_BYTES: u64 = 64 * 1024;

/// Fail-closed configuration for the protected runtime diagnostics listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeDiagnosticsEndpointConfig {
    bind_addr: SocketAddr,
    token_file: PathBuf,
    sample_interval: Duration,
}

impl RuntimeDiagnosticsEndpointConfig {
    /// Creates and validates an explicit endpoint configuration.
    ///
    /// Non-loopback plain HTTP is allowed only when
    /// `allow_insecure_http` is true. This flag exists solely for bounded
    /// development clusters where a NetworkPolicy protects the listener.
    ///
    /// # Errors
    ///
    /// Returns an error for a relative or non-normalized token path, a zero or
    /// excessive sampling interval, or a non-loopback listener without the
    /// explicit development opt-in.
    pub fn try_new(
        bind_addr: SocketAddr,
        token_file: PathBuf,
        sample_interval: Duration,
        allow_insecure_http: bool,
    ) -> Result<Self, ObservabilityError> {
        validate_token_path(&token_file)?;
        if sample_interval.is_zero() || sample_interval > Duration::from_secs(300) {
            return Err(ObservabilityError::invalid_config(
                "runtime diagnostics sampling interval must be between 1 and 300 seconds",
            ));
        }
        if !is_loopback(bind_addr.ip()) && !allow_insecure_http {
            return Err(ObservabilityError::invalid_config(
                "non-loopback runtime diagnostics HTTP requires the explicit development-only opt-in",
            ));
        }
        Ok(Self {
            bind_addr,
            token_file,
            sample_interval,
        })
    }

    /// Loads the optional endpoint configuration from process environment.
    ///
    /// Both the bind address and token-file reference must be present. Leaving
    /// both absent disables the endpoint. A partial configuration fails closed.
    ///
    /// # Errors
    ///
    /// Returns a sanitized configuration error when the environment is
    /// incomplete, malformed, or violates a listener security invariant.
    pub fn from_env() -> Result<Option<Self>, ObservabilityError> {
        let bind_addr = optional_env(RUNTIME_DIAGNOSTICS_BIND_ADDR_ENV)?;
        let token_file = optional_env(RUNTIME_DIAGNOSTICS_TOKEN_FILE_ENV)?;
        let (Some(bind_addr), Some(token_file)) = (bind_addr, token_file) else {
            if env::var_os(RUNTIME_DIAGNOSTICS_BIND_ADDR_ENV).is_some()
                || env::var_os(RUNTIME_DIAGNOSTICS_TOKEN_FILE_ENV).is_some()
            {
                return Err(ObservabilityError::invalid_config(
                    "runtime diagnostics bind address and token-file reference must be configured together",
                ));
            }
            return Ok(None);
        };
        let bind_addr = bind_addr.parse::<SocketAddr>().map_err(|_| {
            ObservabilityError::invalid_config("runtime diagnostics bind address must be a socket address")
        })?;
        let allow_insecure_http = parse_bool_env(RUNTIME_DIAGNOSTICS_ALLOW_INSECURE_HTTP_ENV, false)?;
        let sample_interval = parse_sample_interval()?;
        Self::try_new(
            bind_addr,
            PathBuf::from(token_file),
            sample_interval,
            allow_insecure_http,
        )
        .map(Some)
    }

    pub fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }
}

/// Bound address of the lifecycle-owned diagnostics listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeDiagnosticsEndpointHandle {
    local_addr: SocketAddr,
}

impl RuntimeDiagnosticsEndpointHandle {
    pub fn local_addr(self) -> SocketAddr {
        self.local_addr
    }
}

/// Starts the optional protected endpoint and its owned metrics sampler.
///
/// The token is read through the runtime metadata-I/O lane for every request,
/// so mounted-secret rotation takes effect without process restart.
///
/// # Errors
///
/// Returns a sanitized error when configuration, the initial token read, the
/// listener bind, or lifecycle task registration fails.
pub async fn start_runtime_diagnostics_endpoint_from_env(
    service_context: &ChildServiceContext,
    component: RuntimeComponent,
) -> Result<Option<RuntimeDiagnosticsEndpointHandle>, ObservabilityError> {
    let Some(config) = RuntimeDiagnosticsEndpointConfig::from_env()? else {
        return Ok(None);
    };
    start_runtime_diagnostics_endpoint(service_context, component, config)
        .await
        .map(Some)
}

/// Starts a protected diagnostics listener using an explicit configuration.
///
/// # Errors
///
/// Returns a sanitized error when the initial token cannot be validated, the
/// listener cannot bind, or lifecycle task registration fails.
pub async fn start_runtime_diagnostics_endpoint(
    service_context: &ChildServiceContext,
    component: RuntimeComponent,
    config: RuntimeDiagnosticsEndpointConfig,
) -> Result<RuntimeDiagnosticsEndpointHandle, ObservabilityError> {
    read_token(service_context, config.token_file.clone()).await?;
    let listener = TcpListener::bind(config.bind_addr)
        .await
        .map_err(|_| ObservabilityError::invalid_config("runtime diagnostics listener cannot bind"))?;
    let local_addr = listener
        .local_addr()
        .map_err(|_| ObservabilityError::invalid_config("runtime diagnostics listener address is unavailable"))?;

    let sampler_context = service_context.clone();
    let sampler_cancellation = service_context.task_group().cancellation_token();
    let sample_interval = config.sample_interval;
    service_context
        .spawn_service("runtime-diagnostics.sampler", async move {
            let mut interval = tokio::time::interval(sample_interval);
            loop {
                tokio::select! {
                    _ = sampler_cancellation.cancelled() => break,
                    _ = interval.tick() => {
                        let view = sampler_context.diagnostics_view_v1(component);
                        crate::metrics::runtime::record_snapshot(&view);
                    }
                }
            }
        })
        .map_err(|_| ObservabilityError::invalid_config("runtime diagnostics sampler cannot start"))?;

    crate::metrics::runtime::record_lifecycle(
        component,
        RuntimeLifecycleState::Starting,
        RuntimeLifecycleReason::Startup,
    );

    let server_context = service_context.clone();
    let server_cancellation = service_context.task_group().cancellation_token();
    service_context
        .spawn_service("runtime-diagnostics.endpoint", async move {
            serve(
                listener,
                server_context,
                component,
                config.token_file,
                server_cancellation,
            )
            .await;
        })
        .map_err(|_| ObservabilityError::invalid_config("runtime diagnostics endpoint cannot start"))?;

    tracing::info!(
        component = component_name(component),
        bind = %local_addr,
        scope = RUNTIME_DIAGNOSTICS_SCOPE,
        "protected runtime diagnostics endpoint listening"
    );
    Ok(RuntimeDiagnosticsEndpointHandle { local_addr })
}

async fn serve(
    listener: TcpListener,
    service_context: ChildServiceContext,
    component: RuntimeComponent,
    token_file: PathBuf,
    cancellation: tokio_util::sync::CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => break,
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _peer)) => {
                        handle_connection(stream, &service_context, component, &token_file).await;
                    }
                    Err(error) => {
                        tracing::warn!(
                            component = component_name(component),
                            error_kind = ?error.kind(),
                            "runtime diagnostics accept failed"
                        );
                        break;
                    }
                }
            }
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    service_context: &ChildServiceContext,
    component: RuntimeComponent,
    token_file: &Path,
) {
    let response = match tokio::time::timeout(REQUEST_TIMEOUT, read_request(&mut stream)).await {
        Ok(Ok(request)) => route_request(&request, service_context, component, token_file).await,
        Ok(Err(status)) => error_response(status),
        Err(_) => error_response(HttpStatus::RequestTimeout),
    };
    let _ = stream.write_all(&response).await;
    let _ = stream.shutdown().await;
}

async fn route_request(
    request: &[u8],
    service_context: &ChildServiceContext,
    component: RuntimeComponent,
    token_file: &Path,
) -> Vec<u8> {
    let Ok(request) = ParsedRequest::parse(request) else {
        return error_response(HttpStatus::BadRequest);
    };
    if request.method != "GET" {
        return error_response(HttpStatus::MethodNotAllowed);
    }
    if request.path != RUNTIME_DIAGNOSTICS_PATH {
        return error_response(HttpStatus::NotFound);
    }
    let Some(candidate) = request.bearer_token else {
        return error_response(HttpStatus::Unauthorized);
    };
    if request.scope != Some(RUNTIME_DIAGNOSTICS_SCOPE) {
        return error_response(HttpStatus::Forbidden);
    }
    let expected = match read_token(service_context, token_file.to_path_buf()).await {
        Ok(token) => token,
        Err(_) => return error_response(HttpStatus::ServiceUnavailable),
    };
    if !constant_time_equal(candidate.as_bytes(), expected.as_bytes()) {
        return error_response(HttpStatus::Unauthorized);
    }

    let view = service_context.diagnostics_view_v1(component);
    crate::metrics::runtime::record_snapshot(&view);
    let envelope = RuntimeDiagnosticsEndpointEnvelopeV1 {
        schema_version: RUNTIME_DIAGNOSTICS_ENDPOINT_SCHEMA,
        source: "rocketmq_process",
        data: view,
    };
    match serde_json::to_vec(&envelope) {
        Ok(body) => response(HttpStatus::Ok, &body),
        Err(_) => error_response(HttpStatus::InternalServerError),
    }
}

async fn read_request(stream: &mut TcpStream) -> Result<Vec<u8>, HttpStatus> {
    let mut request = Vec::with_capacity(1024);
    loop {
        if request.len() == MAX_REQUEST_BYTES {
            return Err(HttpStatus::PayloadTooLarge);
        }
        let remaining = MAX_REQUEST_BYTES - request.len();
        let mut buffer = [0_u8; 1024];
        let read = stream
            .read(&mut buffer[..remaining.min(buffer.len())])
            .await
            .map_err(|_| HttpStatus::BadRequest)?;
        if read == 0 {
            return Err(HttpStatus::BadRequest);
        }
        request.extend_from_slice(&buffer[..read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            return Ok(request);
        }
    }
}

async fn read_token(service_context: &ChildServiceContext, token_file: PathBuf) -> Result<String, ObservabilityError> {
    service_context
        .metadata_io()
        .spawn_io("runtime-diagnostics.read-token", move || read_token_file(&token_file))
        .await
        .map_err(|_| ObservabilityError::invalid_config("runtime diagnostics token read was not admitted"))?
}

fn read_token_file(path: &Path) -> Result<String, ObservabilityError> {
    let parent = path
        .parent()
        .ok_or_else(|| ObservabilityError::invalid_config("runtime diagnostics token path has no parent"))?;
    let mount_root = std::fs::canonicalize(parent)
        .map_err(|_| ObservabilityError::invalid_config("runtime diagnostics token mount cannot be resolved"))?;
    let resolved = std::fs::canonicalize(path)
        .map_err(|_| ObservabilityError::invalid_config("runtime diagnostics token cannot be resolved"))?;
    if !resolved.starts_with(&mount_root) || resolved == mount_root {
        return Err(ObservabilityError::invalid_config(
            "runtime diagnostics token escaped its configured mount",
        ));
    }
    let metadata = std::fs::metadata(&resolved)
        .map_err(|_| ObservabilityError::invalid_config("runtime diagnostics token cannot be inspected"))?;
    if !metadata.is_file() || metadata.len() > MAX_TOKEN_BYTES {
        return Err(ObservabilityError::invalid_config(
            "runtime diagnostics token must be a bounded regular file",
        ));
    }
    let token = std::fs::read_to_string(resolved)
        .map_err(|_| ObservabilityError::invalid_config("runtime diagnostics token cannot be read"))?;
    let token = token.trim().to_owned();
    if token.is_empty() || u64::try_from(token.len()).map_or(true, |length| length > MAX_TOKEN_BYTES) {
        return Err(ObservabilityError::invalid_config(
            "runtime diagnostics token contains an invalid value",
        ));
    }
    Ok(token)
}

fn validate_token_path(path: &Path) -> Result<(), ObservabilityError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(ObservabilityError::invalid_config(
            "runtime diagnostics token path must be absolute and normalized",
        ));
    }
    Ok(())
}

fn optional_env(name: &'static str) -> Result<Option<String>, ObservabilityError> {
    let Some(value) = env::var_os(name) else {
        return Ok(None);
    };
    value
        .into_string()
        .map(Some)
        .map_err(|_| ObservabilityError::invalid_config(format!("{name} must contain valid UTF-8")))
}

fn parse_bool_env(name: &'static str, default: bool) -> Result<bool, ObservabilityError> {
    let Some(raw) = optional_env(name)? else {
        return Ok(default);
    };
    match raw.as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(ObservabilityError::invalid_config(format!(
            "{name} must be true, false, 1, or 0"
        ))),
    }
}

fn parse_sample_interval() -> Result<Duration, ObservabilityError> {
    let Some(raw) = optional_env(RUNTIME_DIAGNOSTICS_SAMPLE_INTERVAL_SECONDS_ENV)? else {
        return Ok(DEFAULT_SAMPLE_INTERVAL);
    };
    let seconds = raw.parse::<u64>().map_err(|_| {
        ObservabilityError::invalid_config("runtime diagnostics sampling interval must be an integer number of seconds")
    })?;
    Ok(Duration::from_secs(seconds))
}

fn is_loopback(ip: IpAddr) -> bool {
    ip.is_loopback()
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    let maximum = left.len().max(right.len());
    let mut difference = left.len() ^ right.len();
    for index in 0..maximum {
        let left = left.get(index).copied().unwrap_or_default();
        let right = right.get(index).copied().unwrap_or_default();
        difference |= usize::from(left ^ right);
    }
    difference == 0
}

fn component_name(component: RuntimeComponent) -> &'static str {
    match component {
        RuntimeComponent::Broker => "broker",
        RuntimeComponent::NameServer => "name_server",
        RuntimeComponent::Controller => "controller",
        RuntimeComponent::Proxy => "proxy",
        RuntimeComponent::Mcp => "mcp",
        RuntimeComponent::SreControlPlane => "sre_control_plane",
        RuntimeComponent::SreConnector => "sre_connector",
        RuntimeComponent::Other => "other",
    }
}

#[derive(Serialize)]
struct RuntimeDiagnosticsEndpointEnvelopeV1 {
    schema_version: &'static str,
    source: &'static str,
    data: RuntimeDiagnosticsViewV1,
}

struct ParsedRequest<'a> {
    method: &'a str,
    path: &'a str,
    bearer_token: Option<&'a str>,
    scope: Option<&'a str>,
}

impl<'a> ParsedRequest<'a> {
    fn parse(request: &'a [u8]) -> Result<Self, ()> {
        let request = std::str::from_utf8(request).map_err(|_| ())?;
        let mut lines = request.split("\r\n");
        let first_line = lines.next().ok_or(())?;
        let mut fields = first_line.split_ascii_whitespace();
        let method = fields.next().ok_or(())?;
        let path = fields.next().ok_or(())?;
        if fields.next().is_none() || fields.next().is_some() {
            return Err(());
        }

        let mut bearer_token = None;
        let mut scope = None;
        for line in lines {
            if line.is_empty() {
                break;
            }
            let (name, value) = line.split_once(':').ok_or(())?;
            let value = value.trim();
            if name.eq_ignore_ascii_case("authorization") {
                if bearer_token.is_some() {
                    return Err(());
                }
                bearer_token = value.strip_prefix("Bearer ");
                if bearer_token.is_none() {
                    return Err(());
                }
            } else if name.eq_ignore_ascii_case("x-rocketmq-sre-scope") {
                if scope.replace(value).is_some() {
                    return Err(());
                }
            }
        }
        Ok(Self {
            method,
            path,
            bearer_token,
            scope,
        })
    }
}

#[derive(Clone, Copy)]
enum HttpStatus {
    Ok,
    BadRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    MethodNotAllowed,
    RequestTimeout,
    PayloadTooLarge,
    InternalServerError,
    ServiceUnavailable,
}

impl HttpStatus {
    const fn code(self) -> u16 {
        match self {
            Self::Ok => 200,
            Self::BadRequest => 400,
            Self::Unauthorized => 401,
            Self::Forbidden => 403,
            Self::NotFound => 404,
            Self::MethodNotAllowed => 405,
            Self::RequestTimeout => 408,
            Self::PayloadTooLarge => 413,
            Self::InternalServerError => 500,
            Self::ServiceUnavailable => 503,
        }
    }

    const fn reason(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::BadRequest => "Bad Request",
            Self::Unauthorized => "Unauthorized",
            Self::Forbidden => "Forbidden",
            Self::NotFound => "Not Found",
            Self::MethodNotAllowed => "Method Not Allowed",
            Self::RequestTimeout => "Request Timeout",
            Self::PayloadTooLarge => "Payload Too Large",
            Self::InternalServerError => "Internal Server Error",
            Self::ServiceUnavailable => "Service Unavailable",
        }
    }

    const fn error_code(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::BadRequest => "bad_request",
            Self::Unauthorized => "unauthorized_scope",
            Self::Forbidden => "unauthorized_scope",
            Self::NotFound => "not_found",
            Self::MethodNotAllowed => "method_not_allowed",
            Self::RequestTimeout => "request_timeout",
            Self::PayloadTooLarge => "request_too_large",
            Self::InternalServerError => "internal_error",
            Self::ServiceUnavailable => "source_unavailable",
        }
    }
}

fn error_response(status: HttpStatus) -> Vec<u8> {
    let body = format!(r#"{{"code":"{}"}}"#, status.error_code());
    response(status, body.as_bytes())
}

fn response(status: HttpStatus, body: &[u8]) -> Vec<u8> {
    let headers = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: \
         close\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\n\r\n",
        status.code(),
        status.reason(),
        body.len()
    );
    let mut response = Vec::with_capacity(headers.len() + body.len());
    response.extend_from_slice(headers.as_bytes());
    response.extend_from_slice(body);
    response
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use rocketmq_runtime::RuntimeContext;

    use super::*;

    static NEXT_TEST_FILE: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn endpoint_config_rejects_implicit_non_loopback_plaintext() {
        let token_file = test_token_path();
        assert!(RuntimeDiagnosticsEndpointConfig::try_new(
            "0.0.0.0:8087".parse().expect("address"),
            token_file,
            Duration::from_secs(10),
            false,
        )
        .is_err());
    }

    #[tokio::test]
    async fn endpoint_requires_scope_and_observes_token_rotation() {
        let token_file = test_token_path();
        fs::write(&token_file, "first-token").expect("write token fixture");
        let context = RuntimeContext::from_current("runtime-diagnostics-endpoint-test");
        let service_context = context.service_context("runtime-diagnostics-endpoint-test");
        let config = RuntimeDiagnosticsEndpointConfig::try_new(
            "127.0.0.1:0".parse().expect("address"),
            token_file.clone(),
            Duration::from_secs(1),
            false,
        )
        .expect("config");
        let endpoint = start_runtime_diagnostics_endpoint(&service_context, RuntimeComponent::Broker, config)
            .await
            .expect("endpoint");

        let missing_scope = request(endpoint.local_addr(), "first-token", None).await;
        assert!(missing_scope.starts_with("HTTP/1.1 403 Forbidden"));

        let accepted = request(endpoint.local_addr(), "first-token", Some(RUNTIME_DIAGNOSTICS_SCOPE)).await;
        assert!(accepted.starts_with("HTTP/1.1 200 OK"), "{accepted}");
        assert!(accepted.contains(RUNTIME_DIAGNOSTICS_ENDPOINT_SCHEMA));
        assert!(accepted.contains(r#""component":"broker""#));
        assert!(!accepted.contains("first-token"));
        assert!(!accepted.contains("runtime-diagnostics.endpoint"));

        fs::write(&token_file, "second-token").expect("rotate token fixture");
        let rejected = request(endpoint.local_addr(), "first-token", Some(RUNTIME_DIAGNOSTICS_SCOPE)).await;
        assert!(rejected.starts_with("HTTP/1.1 401 Unauthorized"));
        let rotated = request(endpoint.local_addr(), "second-token", Some(RUNTIME_DIAGNOSTICS_SCOPE)).await;
        assert!(rotated.starts_with("HTTP/1.1 200 OK"));

        let report = context.shutdown_tasks(Duration::from_secs(2)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
        fs::remove_file(token_file).expect("remove token fixture");
    }

    fn test_token_path() -> PathBuf {
        let sequence = NEXT_TEST_FILE.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "rocketmq-runtime-diagnostics-{}-{sequence}.token",
            std::process::id()
        ))
    }

    async fn request(addr: SocketAddr, token: &str, scope: Option<&str>) -> String {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let scope = scope
            .map(|scope| format!("X-RocketMQ-SRE-Scope: {scope}\r\n"))
            .unwrap_or_default();
        let request = format!(
            "GET {RUNTIME_DIAGNOSTICS_PATH} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer \
             {token}\r\n{scope}Connection: close\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).await.expect("write request");
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.expect("read response");
        String::from_utf8(response).expect("UTF-8 response")
    }
}
