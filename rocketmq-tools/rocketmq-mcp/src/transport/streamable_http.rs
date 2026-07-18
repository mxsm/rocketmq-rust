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

use std::net::IpAddr;
use std::net::SocketAddr;
use std::time::Duration;

use axum::http::StatusCode;
use axum::middleware;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::serve::Listener;
use axum::Router;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::tower::StreamableHttpService;
use rmcp::transport::StreamableHttpServerConfig;
use serde_json::json;
use tokio_util::sync::CancellationToken;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::app::McpApp;
use crate::config::HttpConfig;
use crate::error::McpError;
use crate::guard::http_auth::http_auth_middleware;
use crate::guard::http_auth::HttpAuthState;
use crate::protocol::server::RocketmqMcpServer;
use rocketmq_transport::config::TlsConfig;
use rocketmq_transport::config::TlsMode;
use rocketmq_transport::config::TlsServerConfig;
use rocketmq_transport::tls::TlsServerRuntime;

const MAX_HTTP_BODY_BYTES: usize = 1024 * 1024;
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn serve_typed(app: McpApp) -> Result<(), McpError> {
    let bind = parse_bind_addr(&app.config().server.http.bind)?;
    let tcp_listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|source| McpError::infrastructure("bind MCP HTTP listener", source))?;
    let service_context = app.service_context("rocketmq-mcp-https")?;
    let tls_runtime =
        TlsServerRuntime::initialize_with_service_context(tls_config(&app.config().server.http), &service_context)
            .await
            .map_err(|source| McpError::infrastructure("initialize MCP HTTPS listener", source))?;
    if tls_runtime.active_generation() == 0 {
        return Err(McpError::InvalidConfig(
            "MCP HTTPS listener has no verified certificate generation".to_string(),
        ));
    }
    let listener = HttpsListener {
        tcp: tcp_listener,
        tls: tls_runtime,
    };
    let endpoint = app.config().server.http.endpoint.clone();
    let cancellation_token = CancellationToken::new();
    let auth_state = auth_state(&app)?;
    auth_state
        .warm_up()
        .await
        .map_err(|source| McpError::infrastructure("initialize MCP JWKS verifier", source))?;
    let router = build_router_with_auth(app, cancellation_token.clone(), auth_state);

    tracing::info!(
        bind = %bind,
        endpoint = %endpoint,
        tls_generation = listener.tls.active_generation(),
        "rocketmq-mcp streamable HTTPS transport listening"
    );

    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            cancellation_token.cancel();
        })
        .await
        .map_err(|source| McpError::infrastructure("serve MCP HTTP requests", source))?;
    Ok(())
}

#[deprecated(since = "1.0.0", note = "use serve_typed")]
pub async fn serve(app: McpApp) -> anyhow::Result<()> {
    serve_typed(app).await.map_err(anyhow::Error::new)
}

pub fn build_router_typed(app: McpApp, cancellation_token: CancellationToken) -> Result<Router, McpError> {
    let auth_state = auth_state(&app)?;
    Ok(build_router_with_auth(app, cancellation_token, auth_state))
}

fn auth_state(app: &McpApp) -> Result<HttpAuthState, McpError> {
    HttpAuthState::from_http_config(&app.config().server.http, app.guard().clone())
        .map_err(|source| McpError::infrastructure("configure MCP HTTP authentication", source))
}

fn build_router_with_auth(app: McpApp, cancellation_token: CancellationToken, auth_state: HttpAuthState) -> Router {
    let endpoint = app.config().server.http.endpoint.clone();
    let service = streamable_service(app.clone(), cancellation_token);
    let metadata_path = app.config().server.http.auth.protected_resource_metadata_path.clone();
    let metadata = protected_resource_metadata(&app.config().server.http);
    let mcp_router = Router::new()
        .nest_service(&endpoint, service)
        .layer(middleware::from_fn_with_state(auth_state, http_auth_middleware));

    Router::new()
        .route(
            &metadata_path,
            get(move || {
                let metadata = metadata.clone();
                async move { axum::Json(metadata).into_response() }
            }),
        )
        .merge(mcp_router)
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            HTTP_REQUEST_TIMEOUT,
        ))
        .layer(RequestBodyLimitLayer::new(MAX_HTTP_BODY_BYTES))
}

#[deprecated(since = "1.0.0", note = "use build_router_typed")]
pub fn build_router(app: McpApp, cancellation_token: CancellationToken) -> anyhow::Result<Router> {
    build_router_typed(app, cancellation_token).map_err(anyhow::Error::new)
}

fn protected_resource_metadata(http_config: &HttpConfig) -> serde_json::Value {
    let resource = format!(
        "{}{}",
        http_config.public_base_url.trim_end_matches('/'),
        http_config.endpoint
    );
    let authorization_servers = if !http_config.auth.issuer.trim().is_empty() {
        vec![http_config.auth.issuer.clone()]
    } else {
        Vec::new()
    };
    json!({
        "resource": resource,
        "authorization_servers": authorization_servers,
        "scopes_supported": http_config.auth.required_scopes,
    })
}

fn streamable_service(
    app: McpApp,
    cancellation_token: CancellationToken,
) -> StreamableHttpService<RocketmqMcpServer, LocalSessionManager> {
    let server_config = streamable_server_config(app.config().server.http.clone(), cancellation_token);
    StreamableHttpService::new(
        move || Ok(RocketmqMcpServer::new(app.clone())),
        Default::default(),
        server_config,
    )
}

fn streamable_server_config(
    http_config: HttpConfig,
    cancellation_token: CancellationToken,
) -> StreamableHttpServerConfig {
    let mut server_config = StreamableHttpServerConfig::default()
        .with_allowed_hosts(allowed_hosts(&http_config))
        .with_stateful_mode(false)
        .with_json_response(true)
        .with_cancellation_token(cancellation_token);

    if http_config.validate_origin {
        server_config = server_config.with_allowed_origins(http_config.allowed_origins);
    } else {
        server_config = server_config.disable_allowed_origins();
    }

    server_config
}

fn parse_bind_addr(bind: &str) -> Result<SocketAddr, McpError> {
    bind.parse::<SocketAddr>()
        .map_err(|source| McpError::infrastructure("parse server.http.bind socket address", source))
}

fn allowed_hosts(http_config: &HttpConfig) -> Vec<String> {
    let mut hosts = vec!["localhost".to_string(), "127.0.0.1".to_string(), "::1".to_string()];

    if let Ok(addr) = http_config.bind.parse::<SocketAddr>() {
        match addr.ip() {
            IpAddr::V4(ip) if !ip.is_unspecified() => hosts.push(ip.to_string()),
            IpAddr::V6(ip) if !ip.is_unspecified() => hosts.push(ip.to_string()),
            _ => {}
        }
    }
    if let Ok(public_base_url) = url::Url::parse(&http_config.public_base_url) {
        if let Some(host) = public_base_url.host_str() {
            hosts.push(host.to_string());
        }
    }

    hosts.sort();
    hosts.dedup();
    hosts
}

fn tls_config(http_config: &HttpConfig) -> TlsConfig {
    TlsConfig {
        enable: true,
        server: TlsServerConfig {
            mode: TlsMode::Enforcing,
            cert_path: Some(http_config.tls.cert_path.clone()),
            key_path: Some(http_config.tls.key_path.clone()),
            ..TlsServerConfig::default()
        },
        ..TlsConfig::default()
    }
}

struct HttpsListener {
    tcp: tokio::net::TcpListener,
    tls: TlsServerRuntime,
}

impl Listener for HttpsListener {
    type Io = tokio_rustls::server::TlsStream<tokio::net::TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (stream, remote_addr) = match self.tcp.accept().await {
                Ok(accepted) => accepted,
                Err(error) => {
                    tracing::warn!(%error, "failed to accept MCP HTTPS TCP connection");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            if let Some(stream) = self.tls.accept_stream(stream, remote_addr).await {
                return (stream, remote_addr);
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.tcp.local_addr()
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::header::AUTHORIZATION;
    use axum::http::header::HOST;
    use axum::http::header::ORIGIN;
    use axum::http::Request;
    use tower::ServiceExt;

    use super::*;
    use crate::app::McpApp;
    use crate::config::McpConfig;

    #[test]
    fn example_http_bind_uses_loopback_by_default() {
        let config = McpConfig::load(example_config_path()).unwrap();
        let bind = parse_bind_addr(&config.server.http.bind).unwrap();

        assert!(bind.ip().is_loopback());
        assert_ne!(bind.ip().to_string(), "0.0.0.0");
    }

    #[test]
    fn allowed_hosts_include_loopback_and_configured_bind_host() {
        let config = McpConfig::load(example_config_path()).unwrap();
        let hosts = allowed_hosts(&config.server.http);

        assert!(hosts.contains(&"127.0.0.1".to_string()));
        assert!(hosts.contains(&"localhost".to_string()));
        assert!(!hosts.contains(&"0.0.0.0".to_string()));
    }

    #[test]
    fn streamable_server_config_applies_allowed_origins() {
        let config = McpConfig::load(example_config_path()).unwrap();
        let server_config = streamable_server_config(config.server.http, CancellationToken::new());

        assert!(server_config.allowed_origins.contains(&"https://localhost".to_string()));
        assert!(server_config.json_response);
        assert!(!server_config.stateful_mode);
    }

    #[test]
    fn protected_resource_metadata_advertises_resource_and_scopes() {
        let config = McpConfig::load(example_config_path()).unwrap();
        let metadata = protected_resource_metadata(&config.server.http);

        assert_eq!(metadata["resource"], "https://127.0.0.1:8089/mcp");
        assert_eq!(metadata["scopes_supported"], serde_json::json!(["rocketmq:read"]));
    }

    #[tokio::test]
    async fn router_exposes_metadata_and_enforces_http_security_boundaries() {
        let _environment = development_token_environment_lock().lock().await;
        std::env::set_var("ROCKETMQ_MCP_HTTP_TOKEN", "router-test-token");
        let mut config = McpConfig::load(example_config_path()).unwrap();
        config.audit.enabled = true;
        config.audit.sink = "memory".to_string();
        let app = McpApp::new(config).unwrap();
        let router = build_router_typed(app.clone(), CancellationToken::new()).unwrap();

        let metadata = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/.well-known/oauth-protected-resource")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(metadata.status(), StatusCode::OK);

        let unauthorized = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert!(unauthorized
            .headers()
            .get("www-authenticate")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value.contains("resource_metadata=\"https://127.0.0.1:8089/.well-known/oauth-protected-resource\"")
            }));

        let forbidden_origin = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header(AUTHORIZATION, "Bearer router-test-token")
                    .header(HOST, "localhost")
                    .header(ORIGIN, "https://untrusted.example.test")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(forbidden_origin.status(), StatusCode::FORBIDDEN);

        let tool_call = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header(AUTHORIZATION, "Bearer router-test-token")
                    .header(HOST, "localhost")
                    .header(ORIGIN, "https://localhost")
                    .header("content-type", "application/json")
                    .header("accept", "application/json, text/event-stream")
                    .header("mcp-protocol-version", "2025-11-25")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"rocketmq_get_cluster_overview","arguments":{"cluster":"missing-cluster"}}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(tool_call.status(), StatusCode::OK);
        let audit_records = app.guard().audit_log().records();
        let tool_record = audit_records
            .iter()
            .find(|record| record.tool == "rocketmq_get_cluster_overview")
            .expect("real MCP handler should emit a tool audit record");
        assert_eq!(tool_record.operator, "development-http-client");
        assert_eq!(tool_record.client.as_deref(), Some("development-token"));
        assert_ne!(tool_record.operator, "local-stdio");

        std::env::remove_var("ROCKETMQ_MCP_HTTP_TOKEN");
    }

    #[tokio::test]
    async fn https_listener_accepts_tls_and_rejects_plaintext() {
        let temp_dir = tempfile::tempdir().unwrap();
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_path = temp_dir.path().join("server-cert.pem");
        let key_path = temp_dir.path().join("server-key.pem");
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, signing_key.serialize_pem()).unwrap();

        let mut config = McpConfig::load(example_config_path()).unwrap();
        config.server.http.tls.cert_path = cert_path.to_string_lossy().into_owned();
        config.server.http.tls.key_path = key_path.to_string_lossy().into_owned();
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();
        let listener = HttpsListener {
            tcp,
            tls: TlsServerRuntime::new(tls_config(&config.server.http)),
        };
        assert_eq!(listener.tls.active_generation(), 1);

        let cancellation = CancellationToken::new();
        let server_cancellation = cancellation.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, Router::new().route("/health", get(|| async { "ok" })))
                .with_graceful_shutdown(server_cancellation.cancelled_owned())
                .await
        });
        let https_client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let response = https_client
            .get(format!("https://127.0.0.1:{}/health", addr.port()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(response.text().await.unwrap(), "ok");

        let plaintext = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap()
            .get(format!("http://127.0.0.1:{}/health", addr.port()))
            .send()
            .await;
        assert!(plaintext.is_err(), "plaintext must fail closed on the HTTPS listener");

        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    fn development_token_environment_lock() -> &'static tokio::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(tokio::sync::Mutex::default)
    }

    fn example_config_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("conf")
            .join("mcp.example.toml")
    }
}
