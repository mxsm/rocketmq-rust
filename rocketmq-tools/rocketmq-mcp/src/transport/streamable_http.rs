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
use crate::guard::http_auth::http_auth_middleware;
use crate::guard::http_auth::HttpAuthState;
use crate::protocol::server::RocketmqMcpServer;

const MAX_HTTP_BODY_BYTES: usize = 1024 * 1024;
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn serve(app: McpApp) -> anyhow::Result<()> {
    let bind = parse_bind_addr(&app.config().server.http.bind)?;
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let endpoint = app.config().server.http.endpoint.clone();
    let cancellation_token = CancellationToken::new();
    let router = build_router(app, cancellation_token.clone())?;

    tracing::info!(
        bind = %bind,
        endpoint = %endpoint,
        "rocketmq-mcp streamable HTTP transport listening"
    );

    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            cancellation_token.cancel();
        })
        .await?;
    Ok(())
}

pub fn build_router(app: McpApp, cancellation_token: CancellationToken) -> anyhow::Result<Router> {
    let endpoint = app.config().server.http.endpoint.clone();
    let service = streamable_service(app.clone(), cancellation_token);
    let auth_state = HttpAuthState::from_config(&app.config().server.http.auth, app.guard().clone())?;
    let metadata_path = app.config().server.http.auth.protected_resource_metadata_path.clone();
    let metadata = protected_resource_metadata(&app.config().server.http);
    let mcp_router = Router::new()
        .nest_service(&endpoint, service)
        .layer(middleware::from_fn_with_state(auth_state, http_auth_middleware));

    Ok(Router::new()
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
        .layer(RequestBodyLimitLayer::new(MAX_HTTP_BODY_BYTES)))
}

fn protected_resource_metadata(http_config: &HttpConfig) -> serde_json::Value {
    let resource = format!("http://{}{}", http_config.bind, http_config.endpoint);
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
        .with_allowed_hosts(allowed_hosts(&http_config.bind))
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

fn parse_bind_addr(bind: &str) -> anyhow::Result<SocketAddr> {
    bind.parse::<SocketAddr>()
        .map_err(|error| anyhow::anyhow!("server.http.bind must be a socket address: {error}"))
}

fn allowed_hosts(bind: &str) -> Vec<String> {
    let mut hosts = vec!["localhost".to_string(), "127.0.0.1".to_string(), "::1".to_string()];

    if let Ok(addr) = bind.parse::<SocketAddr>() {
        match addr.ip() {
            IpAddr::V4(ip) if !ip.is_unspecified() => hosts.push(ip.to_string()),
            IpAddr::V6(ip) if !ip.is_unspecified() => hosts.push(ip.to_string()),
            _ => {}
        }
    }

    hosts.sort();
    hosts.dedup();
    hosts
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let hosts = allowed_hosts("127.0.0.1:8089");

        assert!(hosts.contains(&"127.0.0.1".to_string()));
        assert!(hosts.contains(&"localhost".to_string()));
        assert!(!hosts.contains(&"0.0.0.0".to_string()));
    }

    #[test]
    fn streamable_server_config_applies_allowed_origins() {
        let config = McpConfig::load(example_config_path()).unwrap();
        let server_config = streamable_server_config(config.server.http, CancellationToken::new());

        assert!(server_config.allowed_origins.contains(&"http://localhost".to_string()));
        assert!(server_config.json_response);
        assert!(!server_config.stateful_mode);
    }

    #[test]
    fn protected_resource_metadata_advertises_resource_and_scopes() {
        let config = McpConfig::load(example_config_path()).unwrap();
        let metadata = protected_resource_metadata(&config.server.http);

        assert_eq!(metadata["resource"], "http://127.0.0.1:8089/mcp");
        assert_eq!(metadata["scopes_supported"], serde_json::json!(["rocketmq:read"]));
    }

    fn example_config_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("conf")
            .join("mcp.example.toml")
    }
}
