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

use axum::extract::Request;
use axum::extract::State;
use axum::http::header::HOST;
use axum::http::header::ORIGIN;
use axum::http::StatusCode;
use axum::middleware;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::serve::Listener;
use axum::Router;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::tower::StreamableHttpService;
use rmcp::transport::StreamableHttpServerConfig;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_transport::api::TlsConfig as RocketmqTlsConfig;
use rocketmq_transport::api::TlsMode;
use rocketmq_transport::api::TlsServerConfig;
use rocketmq_transport::api::TlsServerRuntime;
use serde_json::json;
use tokio_util::sync::CancellationToken;
use tower_http::timeout::TimeoutLayer;

use crate::auth::oauth_middleware;
use crate::auth::AuthState;
use crate::auth::HttpJwksSource;
use crate::auth::JwksSource;
use crate::config::ControlConfig;
use crate::config::REQUIRED_WRITE_SCOPE;
use crate::error::ControlError;
use crate::server::ControlServer;

pub const MAX_HTTP_BODY_BYTES: usize = 1024 * 1024;
pub const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn serve<F>(
    config: ControlConfig,
    service_context: ChildServiceContext,
    shutdown: F,
) -> Result<(), ControlError>
where
    F: FutureShutdown,
{
    config.validate()?;
    let metadata_url = resource_metadata_url(&config);
    let auth = AuthState::<HttpJwksSource>::initialize(&config.oauth, metadata_url)
        .await
        .map_err(|_| ControlError::invalid_config())?;
    let tls = TlsServerRuntime::initialize_with_service_context(tls_config(&config), &service_context)
        .await
        .map_err(|_| ControlError::invalid_config())?;
    if tls.active_generation() == 0 {
        return Err(ControlError::invalid_config());
    }
    let bind = config
        .server
        .bind
        .parse::<SocketAddr>()
        .map_err(|_| ControlError::invalid_config())?;
    let tcp = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|_| ControlError::invalid_config())?;
    let listener = HttpsListener { tcp, tls };
    let cancellation = CancellationToken::new();
    let server = ControlServer::new(config.mutations.mutations_enabled);
    let router = build_router_with_auth(&config, server, cancellation.clone(), auth);

    tracing::info!("rocketmq-mcp-control authenticated HTTPS transport is ready");
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            shutdown.await;
            cancellation.cancel();
        })
        .await
        .map_err(|_| ControlError::execution_failed())
}

pub trait FutureShutdown: std::future::Future<Output = ()> + Send + 'static {}

impl<T> FutureShutdown for T where T: std::future::Future<Output = ()> + Send + 'static {}

pub(crate) fn build_router_with_auth<S: JwksSource + 'static>(
    config: &ControlConfig,
    server: ControlServer,
    cancellation: CancellationToken,
    auth: AuthState<S>,
) -> Router {
    let endpoint = config.server.endpoint.clone();
    let service = streamable_service(config, server, cancellation);
    let metadata_path = "/.well-known/oauth-protected-resource";
    let metadata = protected_resource_metadata(config);
    let mcp_router = Router::new()
        .nest_service(&endpoint, service)
        .layer(middleware::from_fn_with_state(auth, oauth_middleware::<S>));

    let policy = RequestOriginPolicy {
        host: config.server.public_base_url.host().to_string(),
        origin: config.server.public_base_url.as_str().to_string(),
    };
    apply_http_limits(
        Router::new()
            .route(
                metadata_path,
                get(move || {
                    let metadata = metadata.clone();
                    async move { axum::Json(metadata).into_response() }
                }),
            )
            .merge(mcp_router)
            .layer(middleware::from_fn_with_state(policy, validate_host_origin)),
    )
}

fn apply_http_limits(router: Router) -> Router {
    router.layer(TimeoutLayer::with_status_code(
        StatusCode::REQUEST_TIMEOUT,
        HTTP_REQUEST_TIMEOUT,
    ))
}

#[derive(Clone)]
struct RequestOriginPolicy {
    host: String,
    origin: String,
}

async fn validate_host_origin(State(policy): State<RequestOriginPolicy>, request: Request, next: Next) -> Response {
    let host_matches = request
        .headers()
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == policy.host);
    let origin_matches = request
        .headers()
        .get(ORIGIN)
        .map(|value| value.to_str().is_ok_and(|value| value == policy.origin))
        .unwrap_or(true);
    if !host_matches || !origin_matches {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(ControlError::request_rejected().envelope()),
        )
            .into_response();
    }
    next.run(request).await
}

fn streamable_service(
    config: &ControlConfig,
    server: ControlServer,
    cancellation: CancellationToken,
) -> StreamableHttpService<ControlServer, LocalSessionManager> {
    let server_config = StreamableHttpServerConfig::default()
        .with_allowed_hosts(allowed_hosts(config))
        .with_allowed_origins(vec![config.server.public_base_url.as_str().to_string()])
        .with_legacy_session_mode(false)
        .with_json_response(true)
        .with_max_request_body_bytes(MAX_HTTP_BODY_BYTES)
        .with_cancellation_token(cancellation);
    StreamableHttpService::new(move || Ok(server.clone()), Default::default(), server_config)
}

fn protected_resource_metadata(config: &ControlConfig) -> serde_json::Value {
    json!({
        "resource": format!("{}{}", config.server.public_base_url.as_str(), config.server.endpoint),
        "authorization_servers": [config.oauth.issuer],
        "scopes_supported": [REQUIRED_WRITE_SCOPE],
    })
}

fn resource_metadata_url(config: &ControlConfig) -> String {
    format!(
        "{}/.well-known/oauth-protected-resource",
        config.server.public_base_url.as_str()
    )
}

fn allowed_hosts(config: &ControlConfig) -> Vec<String> {
    let mut hosts = vec!["localhost".to_string(), "127.0.0.1".to_string(), "::1".to_string()];
    if let Ok(address) = config.server.bind.parse::<SocketAddr>() {
        match address.ip() {
            IpAddr::V4(ip) if !ip.is_unspecified() => hosts.push(ip.to_string()),
            IpAddr::V6(ip) if !ip.is_unspecified() => hosts.push(ip.to_string()),
            _ => {}
        }
    }
    hosts.push(config.server.public_base_url.host().to_string());
    hosts.sort();
    hosts.dedup();
    hosts
}

fn tls_config(config: &ControlConfig) -> RocketmqTlsConfig {
    RocketmqTlsConfig {
        enable: true,
        server: TlsServerConfig {
            mode: TlsMode::Enforcing,
            cert_path: Some(config.server.tls.cert_path.clone()),
            key_path: Some(config.server.tls.key_path.clone()),
            ..TlsServerConfig::default()
        },
        ..RocketmqTlsConfig::default()
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
            let (stream, remote_address) = match self.tcp.accept().await {
                Ok(accepted) => accepted,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            if let Some(stream) = self.tls.accept_stream(stream, remote_address).await {
                return (stream, remote_address);
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.tcp.local_addr()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;

    use axum::body::to_bytes;
    use axum::body::Body;
    use axum::http::header::AUTHORIZATION;
    use axum::http::header::HOST;
    use axum::http::header::ORIGIN;
    use axum::http::Request;
    use jsonwebtoken::encode;
    use jsonwebtoken::Algorithm;
    use jsonwebtoken::EncodingKey;
    use jsonwebtoken::Header;
    use serde::Serialize;
    use tower::ServiceExt;
    use tracing_subscriber::fmt::MakeWriter;

    use super::*;
    use crate::auth::AuthError;
    use crate::config::AuditConfig;
    use crate::config::MutationPolicyConfig;
    use crate::config::OAuthConfig;
    use crate::config::ServerConfig;
    use crate::config::TlsConfig;

    const RSA_N: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";

    struct StaticSource;

    impl JwksSource for StaticSource {
        async fn fetch(&self) -> Result<Vec<u8>, AuthError> {
            Ok(serde_json::to_vec(&serde_json::json!({"keys": [{
                "kty": "RSA", "kid": "test-key", "alg": "RS256", "use": "sig",
                "key_ops": ["verify"], "n": RSA_N, "e": "AQAB"
            }]}))
            .unwrap())
        }
    }

    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<StdMutex<Vec<u8>>>);

    impl Write for CapturedLogs {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> MakeWriter<'writer> for CapturedLogs {
        type Writer = Self;

        fn make_writer(&'writer self) -> Self::Writer {
            self.clone()
        }
    }

    #[derive(Serialize)]
    struct TestClaims<'a> {
        sub: &'a str,
        iss: &'a str,
        aud: &'a str,
        exp: usize,
        scope: &'a str,
        rocketmq_operations: Vec<&'a str>,
        rocketmq_clusters: Vec<&'a str>,
    }

    fn config() -> ControlConfig {
        ControlConfig {
            server: ServerConfig {
                bind: "127.0.0.1:8090".to_string(),
                endpoint: "/mcp".to_string(),
                public_base_url: crate::config::HttpsOrigin::try_new("https://control.example.test").unwrap(),
                tls: TlsConfig {
                    cert_path: "server.pem".to_string(),
                    key_path: "server-key.pem".to_string(),
                },
            },
            oauth: OAuthConfig {
                issuer: "https://issuer.example.test".to_string(),
                audience: "rocketmq-mcp-control".to_string(),
                jwks_url: "https://issuer.example.test/jwks".to_string(),
                jwks_ca_path: None,
            },
            mutations: MutationPolicyConfig::default(),
            audit: AuditConfig {
                path: "audit.jsonl".to_string(),
                capacity: 64,
                max_record_bytes: 4096,
            },
        }
    }

    fn token(scope: &str) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key".to_string());
        encode(
            &header,
            &TestClaims {
                sub: "operator@example.test",
                iss: "https://issuer.example.test",
                aud: "rocketmq-mcp-control",
                exp: 4_102_444_800,
                scope,
                rocketmq_operations: vec![],
                rocketmq_clusters: vec![],
            },
            &EncodingKey::from_rsa_pem(include_bytes!("../tests/fixtures/oauth-private-key.pem")).unwrap(),
        )
        .unwrap()
    }

    async fn router() -> Router {
        router_with_server(ControlServer::new(false)).await
    }

    async fn router_with_server(server: ControlServer) -> Router {
        let config = config();
        let auth = AuthState::from_source(&config.oauth, resource_metadata_url(&config), StaticSource)
            .await
            .unwrap();
        build_router_with_auth(&config, server, CancellationToken::new(), auth)
    }

    fn request(path: &str, body: Body, token: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder()
            .method("POST")
            .uri(path)
            .header(HOST, "control.example.test")
            .header(ORIGIN, "https://control.example.test")
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2025-11-25");
        if let Some(token) = token {
            builder = builder.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        builder.body(body).unwrap()
    }

    #[tokio::test]
    async fn oauth_runs_before_mcp_schema_and_body_limits_are_enforced() {
        let router = router().await;
        let missing = router
            .clone()
            .oneshot(request("/mcp", Body::from("not-json"), None))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
        assert!(!missing
            .headers()
            .get(axum::http::header::WWW_AUTHENTICATE)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("127.0.0.1"));

        let read_token = token("rocketmq:read");
        let missing_scope = router
            .clone()
            .oneshot(request("/mcp", Body::from("not-json"), Some(&read_token)))
            .await
            .unwrap();
        assert_eq!(missing_scope.status(), StatusCode::FORBIDDEN);

        let write_token = token(REQUIRED_WRITE_SCOPE);
        let oversized = router
            .oneshot(request(
                "/mcp",
                Body::from(vec![b'x'; MAX_HTTP_BODY_BYTES + 1]),
                Some(&write_token),
            ))
            .await
            .unwrap();
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn protected_metadata_and_authenticated_tools_list_are_real_protocol_surfaces() {
        let router = router().await;
        let metadata = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/.well-known/oauth-protected-resource")
                    .header(HOST, "control.example.test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(metadata.status(), StatusCode::OK);
        let metadata_body = to_bytes(metadata.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
        let metadata_text = String::from_utf8(metadata_body.to_vec()).unwrap();
        assert!(!metadata_text.contains("127.0.0.1"));
        assert!(!metadata_text.contains(":8090"));

        let write_token = token(REQUIRED_WRITE_SCOPE);
        let initialize = router
            .clone()
            .oneshot(request(
                "/mcp",
                Body::from(r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"control-test","version":"1.0"}}}"#),
                Some(&write_token),
            ))
            .await
            .unwrap();
        assert_eq!(initialize.status(), StatusCode::OK);
        let tools = router
            .oneshot(request(
                "/mcp",
                Body::from(r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#),
                Some(&write_token),
            ))
            .await
            .unwrap();
        assert_eq!(tools.status(), StatusCode::OK);
        let body = to_bytes(tools.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["result"]["tools"], serde_json::json!([]));
    }

    #[tokio::test(start_paused = true)]
    async fn authenticated_mcp_request_timeout_does_not_use_a_dummy_route() {
        let router = router_with_server(
            ControlServer::new(false).with_response_delay(HTTP_REQUEST_TIMEOUT + Duration::from_secs(1)),
        )
        .await;
        let write_token = token(REQUIRED_WRITE_SCOPE);
        let response = router
            .oneshot(request(
                "/mcp",
                Body::from(r#"{"jsonrpc":"2.0","id":3,"method":"tools/list","params":{}}"#),
                Some(&write_token),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
    }

    #[tokio::test]
    async fn hostile_host_and_origin_are_rejected_before_oauth_without_echo_or_logs() {
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(logs.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let router = router().await;
        let hostile_host = "token%3Dsecret.example.test";
        let hostile_origin = "https://evil.example.test/%31%32%37%2e%30%2e%30%2e%31";
        let make_request = |authorization: Option<&str>| {
            let mut builder = Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(HOST, hostile_host)
                .header(ORIGIN, hostile_origin)
                .header("content-type", "application/json");
            if let Some(authorization) = authorization {
                builder = builder.header(AUTHORIZATION, authorization);
            }
            builder.body(Body::from("not-json")).unwrap()
        };
        let first = router.clone().oneshot(make_request(None)).await.unwrap();
        let second = router
            .oneshot(make_request(Some("Bearer attacker-controlled")))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::BAD_REQUEST);
        assert_eq!(second.status(), StatusCode::BAD_REQUEST);
        let first_body = to_bytes(first.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
        let second_body = to_bytes(second.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
        assert_eq!(first_body, second_body);
        let body = String::from_utf8(first_body.to_vec()).unwrap();
        let captured = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        for forbidden in [hostile_host, hostile_origin, "token=secret", "127.0.0.1"] {
            assert!(!body.contains(forbidden));
            assert!(!captured.contains(forbidden));
        }
    }

    #[tokio::test]
    async fn tls_listener_accepts_https_and_rejects_plaintext() {
        let directory = tempfile::tempdir().unwrap();
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_path = directory.path().join("server.pem");
        let key_path = directory.path().join("server-key.pem");
        tokio::fs::write(&cert_path, cert.pem()).await.unwrap();
        tokio::fs::write(&key_path, signing_key.serialize_pem()).await.unwrap();
        let mut config = config();
        config.server.tls.cert_path = cert_path.to_string_lossy().into_owned();
        config.server.tls.key_path = key_path.to_string_lossy().into_owned();
        let runtime = rocketmq_runtime::RuntimeContext::from_current("mcp-control-tls-test");
        let context = runtime.service_context("mcp-control-tls-test");
        let tls = TlsServerRuntime::initialize_with_service_context(tls_config(&config), &context)
            .await
            .unwrap();
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = tcp.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let server_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            axum::serve(
                HttpsListener { tcp, tls },
                Router::new().route("/health", get(|| async { "ok" })),
            )
            .with_graceful_shutdown(server_cancellation.cancelled_owned())
            .await
        });
        let https = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap()
            .get(format!("https://127.0.0.1:{}/health", address.port()))
            .send()
            .await
            .unwrap();
        assert_eq!(https.status(), reqwest::StatusCode::OK);
        let plaintext = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap()
            .get(format!("http://127.0.0.1:{}/health", address.port()))
            .send()
            .await;
        assert!(plaintext.is_err());
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
