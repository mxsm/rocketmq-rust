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
use rocketmq_mcp::app::prepare_mcp_bootstrap;
use rocketmq_mcp::app::McpApp;
use rocketmq_mcp::config::Args;
use rocketmq_mcp::config::McpConfig;
use rocketmq_mcp::config::TransportKind;
use rocketmq_mcp::error::McpError;
use rocketmq_mcp::transport;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ServiceLifecycle;
use rocketmq_runtime::ServiceLifecycleState;
use rocketmq_runtime::ShutdownReason;
use rocketmq_security_api::SecurityBootstrapConfig;
use rocketmq_security_api::SecurityBootstrapOutcome;
use rocketmq_security_api::SecurityBootstrapProfile;

const RUNTIME_TEARDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
fn main() -> Result<(), McpError> {
    let owner = RuntimeOwner::new(mcp_runtime_config()).map_err(|source| McpError::Infrastructure {
        operation: "create MCP runtime owner",
        source: Box::new(source),
    })?;
    let service_context = owner.root_context().component("rocketmq-mcp");
    let lifecycle = ServiceLifecycle::from_env("rocketmq-mcp")
        .map_err(|error| McpError::InvalidConfig(format!("invalid MCP lifecycle configuration: {error}")))?;

    let run_result = owner.block_on(run(service_context, lifecycle.clone()));
    if run_result.is_err() {
        lifecycle.mark_failed();
    }
    let shutdown_request = lifecycle
        .shutdown_request()
        .unwrap_or_else(|| lifecycle.request_shutdown(ShutdownReason::Internal));
    let shutdown_result = owner
        .shutdown_runtime_blocking_until(shutdown_request.deadline)
        .map_err(|source| McpError::Infrastructure {
            operation: "shutdown MCP runtime owner",
            source: Box::new(source),
        });

    match (run_result, shutdown_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(report)) if !report.is_healthy() => {
            lifecycle.mark_failed();
            Err(McpError::Infrastructure {
                operation: "complete MCP runtime shutdown without task leaks",
                source: Box::new(std::io::Error::other(report.to_json())),
            })
        }
        (Ok(()), Ok(_report)) => Ok(()),
    }
}

fn mcp_runtime_config() -> RuntimeConfig {
    let mut config = RuntimeConfig::server_default("rocketmq-mcp");
    config.shutdown_timeout = RUNTIME_TEARDOWN_TIMEOUT;
    config
}

async fn run(service_context: ChildServiceContext, lifecycle: ServiceLifecycle) -> Result<(), McpError> {
    let args = Args::parse();
    let config = McpConfig::load_with_overrides(&args)?;
    let security_bootstrap = SecurityBootstrapConfig::from_env()
        .map_err(|error| McpError::InvalidConfig(format!("MCP security bootstrap configuration failed: {error}")))?;
    let bootstrap_handoff = prepare_mcp_bootstrap(config, &security_bootstrap, lifecycle.config().probe_bind_addr)?;
    let validated_security = bootstrap_handoff.security_outcome();
    let app = McpApp::bootstrap_validated_typed(bootstrap_handoff, service_context).await?;
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

fn log_security_bootstrap(outcome: SecurityBootstrapOutcome) {
    match outcome {
        SecurityBootstrapOutcome::Disabled => {
            tracing::warn!("MCP security bootstrap is disabled because no security profile is configured")
        }
        SecurityBootstrapOutcome::Validated(validated) => match validated.profile() {
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
        },
    }
}

async fn serve_stdio(app: McpApp, lifecycle: ServiceLifecycle) -> Result<(), McpError> {
    lifecycle
        .mark_ready()
        .map_err(|error| McpError::InvalidConfig(format!("failed to publish MCP readiness: {error}")))?;
    rocketmq_observability::metrics::runtime::record_lifecycle(
        rocketmq_runtime::RuntimeComponent::Mcp,
        rocketmq_observability::metrics::runtime::RuntimeLifecycleState::Ready,
        rocketmq_observability::metrics::runtime::RuntimeLifecycleReason::Startup,
    );
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
    use super::mcp_runtime_config;
    use super::RuntimeConfig;
    use super::TransportKind;
    use rocketmq_mcp::app::validate_mcp_security;
    use rocketmq_security_api::SecurityBootstrap;
    use rocketmq_security_api::SecurityBootstrapConfig;
    use rocketmq_security_api::SecurityBootstrapOutcome;
    use rocketmq_security_api::SecurityBootstrapProfile;

    #[test]
    fn disabled_security_bootstrap_skips_mcp_listener_validation() {
        let outcome = validate_mcp_security(
            &SecurityBootstrap::Disabled,
            TransportKind::StreamableHttp,
            "not-a-socket-address",
            None,
            None,
        )
        .expect("disabled security bootstrap should not inspect MCP listeners");

        assert_eq!(outcome, SecurityBootstrapOutcome::Disabled);
    }

    #[test]
    fn security_bootstrap_precedes_mcp_listener_bind() {
        let security = SecurityBootstrap::Enabled(SecurityBootstrapConfig::new(
            SecurityBootstrapProfile::DevelopmentInsecureLoopback,
        ));

        validate_mcp_security(
            &security,
            TransportKind::StreamableHttp,
            "127.0.0.1:8089",
            None,
            Some("127.0.0.1:8088".parse().expect("probe address")),
        )
        .expect("loopback-only MCP bootstrap should pass");

        assert!(validate_mcp_security(&security, TransportKind::StreamableHttp, "0.0.0.0:8089", None, None,).is_err());
    }

    #[test]
    fn development_security_rejects_public_prometheus_listener() {
        let security = SecurityBootstrap::Enabled(SecurityBootstrapConfig::new(
            SecurityBootstrapProfile::DevelopmentInsecureLoopback,
        ));

        assert!(validate_mcp_security(
            &security,
            TransportKind::Stdio,
            "127.0.0.1:8089",
            Some("0.0.0.0:5557".parse().expect("metrics address")),
            None,
        )
        .is_err());
    }

    #[test]
    fn file_prometheus_listener_resolution_feeds_pre_bind_security_validation() {
        let mut config = rocketmq_mcp::config::McpConfig::load(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("conf")
                .join("mcp.example.toml"),
        )
        .expect("example MCP config should load");
        config.observability.metrics.exporter = Some(rocketmq_observability::MetricsExporter::Prometheus);
        config.observability.prometheus.host = Some("0.0.0.0".to_string());
        config.observability.prometheus.port = Some(5557);
        config.observability.prometheus.path = Some("/metrics".to_string());
        let mut bootstrap = rocketmq_observability::TelemetryBootstrapConfig::default();
        bootstrap.observability.service_name = "rocketmq-mcp".to_string();
        bootstrap.observability.node_type = "mcp".to_string();
        bootstrap.observability.node_id = config.server.name.clone();
        let resolution = rocketmq_observability::resolve_telemetry_values(
            "rocketmq-mcp",
            bootstrap,
            &config.observability,
            &rocketmq_observability::TelemetryEnvironmentValues::default(),
            rocketmq_observability::TelemetryEnvironmentSpec {
                trace_sample_ratio_env: Some("ROCKETMQ_MCP_TRACE_SAMPLE_RATIO"),
            },
        )
        .expect("file Prometheus telemetry should resolve");
        let security = SecurityBootstrap::Enabled(SecurityBootstrapConfig::new(
            SecurityBootstrapProfile::DevelopmentInsecureLoopback,
        ));

        let error = validate_mcp_security(
            &security,
            TransportKind::Stdio,
            "127.0.0.1:8089",
            resolution.prometheus_listener_addr,
            None,
        )
        .expect_err("public file Prometheus listener must fail before bind")
        .to_string();

        assert!(error.contains("loopback"));
        assert!(!error.contains("0.0.0.0"));
        assert!(!error.contains("5557"));
    }

    #[test]
    fn runtime_owner_configuration_is_bounded() {
        let config = mcp_runtime_config();

        assert_eq!(config.thread_name, "rocketmq-mcp");
        assert_eq!(
            config.max_blocking_threads,
            RuntimeConfig::server_default("comparison").max_blocking_threads
        );
        assert_eq!(config.shutdown_timeout, super::RUNTIME_TEARDOWN_TIMEOUT);
        assert!(config.blocking_lane_policies.storage_io.max_queue_depth > 0);
        assert!(config.blocking_lane_policies.metadata_io.max_queue_depth > 0);
        assert!(config.blocking_lane_policies.cpu_crypto.max_queue_depth > 0);
    }
}
