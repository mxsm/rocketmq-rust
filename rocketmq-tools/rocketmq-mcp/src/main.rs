#![recursion_limit = "256"]

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

use clap::Parser;
use rocketmq_mcp::app::McpApp;
use rocketmq_mcp::config::Args;
use rocketmq_mcp::config::McpConfig;
use rocketmq_mcp::config::TransportKind;
use rocketmq_mcp::error::McpError;
use rocketmq_mcp::transport;
use rocketmq_runtime::ServiceLifecycle;
use rocketmq_runtime::ServiceLifecycleState;
use rocketmq_runtime::ShutdownReason;
use rocketmq_security_api::SecurityBootstrapConfig;
use rocketmq_security_api::SecurityBootstrapProfile;
use rocketmq_security_api::ValidatedSecurityBootstrap;

const RUNTIME_TEARDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

fn main() -> Result<(), McpError> {
    let runtime = build_runtime()?;
    let result = runtime.block_on(run());
    shutdown_runtime(runtime, RUNTIME_TEARDOWN_TIMEOUT);
    result
}

fn build_runtime() -> Result<tokio::runtime::Runtime, McpError> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("rocketmq-mcp")
        .build()
        .map_err(|source| McpError::Infrastructure {
            operation: "create MCP Tokio runtime",
            source: Box::new(source),
        })
}

fn shutdown_runtime(runtime: tokio::runtime::Runtime, timeout: std::time::Duration) {
    runtime.shutdown_timeout(timeout);
}

async fn run() -> Result<(), McpError> {
    let args = Args::parse();
    let lifecycle = ServiceLifecycle::from_env("rocketmq-mcp")
        .map_err(|error| McpError::InvalidConfig(format!("invalid MCP lifecycle configuration: {error}")))?;
    let config = McpConfig::load_with_overrides(&args)?;
    let security_config = SecurityBootstrapConfig::from_env()
        .map_err(|error| McpError::InvalidConfig(format!("MCP security bootstrap configuration failed: {error}")))?;
    let validated_security = validate_mcp_security(
        &security_config,
        config.server.transport,
        &config.server.http.bind,
        lifecycle.config().probe_bind_addr,
    )?;
    let app = McpApp::bootstrap_typed(config, validated_security).await?;
    log_security_bootstrap(validated_security);
    if let Err(error) = app.start_lifecycle(&lifecycle).await {
        lifecycle.mark_failed();
        let request = lifecycle.request_shutdown(ShutdownReason::Internal);
        app.shutdown_with_deadline(request.deadline).await.log_if_unhealthy();
        return Err(error);
    }

    tracing::info!(
        server = %app.config().server.name,
        transport = app.transport().as_str(),
        cluster_count = app.config().clusters.len(),
        "{} startup initialized",
        app.config().server.name,
    );

    let result = match app.transport() {
        TransportKind::Stdio => serve_stdio(app.clone(), lifecycle.clone()).await,
        TransportKind::StreamableHttp => serve_streamable_http(app.clone(), lifecycle.clone()).await,
    };
    if result.is_err() {
        lifecycle.mark_failed();
    }
    let lifecycle_failed = lifecycle.state() == ServiceLifecycleState::Failed;
    let shutdown_request = lifecycle
        .shutdown_request()
        .unwrap_or_else(|| lifecycle.request_shutdown(ShutdownReason::Internal));
    let shutdown_report = app.shutdown_with_deadline(shutdown_request.deadline).await;
    shutdown_report.log_if_unhealthy();
    if result.is_ok() && shutdown_report.is_healthy() && !lifecycle_failed {
        lifecycle.mark_stopped();
    } else {
        lifecycle.mark_failed();
    }
    result?;
    if lifecycle_failed {
        return Err(McpError::Infrastructure {
            operation: "complete MCP lifecycle shutdown",
            source: Box::new(std::io::Error::other(
                "MCP lifecycle failed while observing or completing shutdown",
            )),
        });
    }
    if !shutdown_report.is_healthy() {
        return Err(McpError::Infrastructure {
            operation: "shutdown MCP within the shared lifecycle deadline",
            source: Box::new(std::io::Error::other("MCP shutdown report is unhealthy")),
        });
    }

    Ok(())
}

fn validate_mcp_security(
    security_config: &SecurityBootstrapConfig,
    transport: TransportKind,
    http_bind: &str,
    probe_bind_addr: Option<std::net::SocketAddr>,
) -> Result<ValidatedSecurityBootstrap, McpError> {
    let mut listeners = Vec::with_capacity(2);
    if transport == TransportKind::StreamableHttp {
        listeners.push(
            http_bind
                .parse::<std::net::SocketAddr>()
                .map_err(|_| McpError::InvalidConfig("server.http.bind must be a socket address".to_string()))?,
        );
    }
    if let Some(probe_bind_addr) = probe_bind_addr {
        listeners.push(probe_bind_addr);
    }
    security_config.validate(&listeners).map_err(|error| {
        McpError::InvalidConfig(format!("MCP security bootstrap failed before listener bind: {error}"))
    })
}

fn log_security_bootstrap(validated: ValidatedSecurityBootstrap) {
    match validated.profile() {
        SecurityBootstrapProfile::DevelopmentInsecureLoopback => tracing::warn!(
            profile = validated.profile().as_str(),
            listener_count = validated.listener_count(),
            "MCP development-insecure security profile is active; every listener is restricted to loopback"
        ),
        SecurityBootstrapProfile::SecureEnforced => tracing::info!(
            profile = validated.profile().as_str(),
            listener_count = validated.listener_count(),
            "MCP secure bootstrap completed before listener bind"
        ),
    }
}

async fn serve_stdio(app: McpApp, lifecycle: ServiceLifecycle) -> Result<(), McpError> {
    lifecycle
        .mark_ready()
        .map_err(|error| McpError::InvalidConfig(format!("failed to publish MCP readiness: {error}")))?;
    transport::stdio::serve_typed_with_lifecycle(app, lifecycle).await
}

async fn serve_streamable_http(app: McpApp, lifecycle: ServiceLifecycle) -> Result<(), McpError> {
    #[cfg(feature = "streamable-http")]
    {
        transport::streamable_http::serve_typed_with_lifecycle(app, lifecycle).await
    }

    #[cfg(not(feature = "streamable-http"))]
    {
        let _ = (app, lifecycle);
        Err(McpError::FeatureDisabled {
            transport: "streamable-http",
            feature: "streamable-http",
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;
    use std::time::Instant;

    use super::build_runtime;
    use super::shutdown_runtime;
    use super::validate_mcp_security;
    use super::TransportKind;
    use rocketmq_security_api::SecurityBootstrapConfig;
    use rocketmq_security_api::SecurityBootstrapProfile;

    #[test]
    fn security_bootstrap_precedes_mcp_listener_bind() {
        let security = SecurityBootstrapConfig::new(SecurityBootstrapProfile::DevelopmentInsecureLoopback);

        validate_mcp_security(
            &security,
            TransportKind::StreamableHttp,
            "127.0.0.1:8089",
            Some("127.0.0.1:8088".parse().expect("probe address")),
        )
        .expect("loopback-only MCP bootstrap should pass");

        assert!(validate_mcp_security(&security, TransportKind::StreamableHttp, "0.0.0.0:8089", None,).is_err());
    }

    #[test]
    fn runtime_teardown_is_bounded_when_blocking_work_is_still_open() {
        let runtime = build_runtime().expect("test runtime should build");
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        runtime.spawn_blocking(move || {
            started_tx.send(()).expect("test should observe blocking work");
            let _ = release_rx.recv();
        });
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocking work should start");

        let started_at = Instant::now();
        shutdown_runtime(runtime, Duration::from_millis(10));
        assert!(started_at.elapsed() < Duration::from_secs(1));
        release_tx.send(()).expect("blocking work should still be releasable");
    }
}
